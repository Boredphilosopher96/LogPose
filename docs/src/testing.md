# Testing

LogPose treats testing as part of system architecture, not as a final verification step.

Our long-term testing structure is explicitly inspired by TigerBeetle's layered strategy. The goal is not to copy TigerBeetle's exact infrastructure or claim parity with its maturity. The goal is to follow the same discipline: use multiple complementary harnesses, make failures reproducible, and keep production behavior under test whenever practical.

## Current State

LogPose already has a real testing stack, but it is not a full-system simulator yet.

What exists today:

- inline unit tests across storage, WAL, API, and supporting crates
- crate-level integration and regression suites for storage, service, control-plane behavior, CLI workflows, and query planning
- seeded randomized harnesses at the storage and service boundaries with replayable seeds and explicit models or oracles
- deterministic service-boundary simulation scenarios for runtime status, placement, restart, recovery, and wrong-plane rejection
- process-boundary CLI contract tests with snapshot baselines for operator-visible JSON output
- exact-versus-ANN regressions, recall checks, and reproducible ANN benchmarks

What does not exist yet:

- a deterministic multi-node event-loop simulator
- virtual time, simulated network faults, or simulated disk-fault scheduling
- liveness campaigns that freeze faults outside a healthy core and require convergence
- broader fuzz and property harnesses for WAL, manifests, sidecars, and operator input surfaces

## What We Are Basing This On

The testing doctrine for LogPose is based on the public testing approach TigerBeetle has described across its engineering material:

- deterministic simulation testing for fault, recovery, and time-sensitive system behavior
- generative full-system testing around real binaries and real process boundaries
- targeted fuzzing and property-style testing for codecs, protocol surfaces, and subsystem invariants
- conventional unit, integration, and regression testing
- snapshot-style assertions when the observable contract is text output or structured rendered output

That layered structure is the important part. Different layers catch different classes of bugs, and no single test style is enough on its own.

## The LogPose Testing Ladder

LogPose organizes testing as a ladder from tight local checks to broader system validation.

### 1. Unit Tests

Use unit tests for local pure behavior, validation logic, codec rules, and private helper behavior that benefits from tight encapsulation.

- keep these close to the code under test with `#[cfg(test)]`
- use them for small, direct, non-workflow behavior
- prefer them when private internals matter more than shared harness reuse

### 2. Integration and Regression Tests

Use integration tests for filesystem-backed workflows, async behavior, cross-module behavior, and regressions that should read like operator-visible scenarios.

- place them under crate-level `tests/`
- use real public interfaces whenever possible
- keep regression tests explicit even when the same behavior is also covered by a randomized harness

### 3. Generative Harnesses

This is the first major TigerBeetle-inspired layer LogPose implemented, and it now exists at more than one boundary.

Generative harnesses run seeded, replayable sequences of operations against real LogPose components and compare the observed results to an explicit oracle or model. These are not "random tests" in the loose sense. They are bounded, deterministic scenario generators with correctness checks.

The pattern exists today at both the storage boundary and the service boundary:

- generated actions drive the storage `Engine` and its `CollectionHandle` (storage harness v2, `crates/logpose-storage/tests/harness`) or the shared `AppState` with its REST and gRPC views (the service harness)
- a model tracks expected logical visibility
- checks run after writes, snapshots, flushes, compaction, stats reads, and reopen/recovery steps
- failures must report the exact seed and action trace needed for replay

This layer is the bridge between ordinary integration tests and future simulation-style system testing.

### 4. Process-Boundary Operator Tests

Process-boundary tests sit between local harnesses and true full-system simulation.

- use real binaries and real transport boundaries when the operator contract matters
- keep snapshot-style baselines for CLI output and other rendered operator surfaces
- preserve transport parity checks for REST and gRPC where the same workflow must stay semantically aligned

This layer already exists in LogPose through CLI server fixtures and snapshot contracts. It should keep growing as operator-facing surfaces become more important.

The local Podman chaos workflow documented in [Podman Chaos](./podman-chaos.md)
belongs in this layer. It uses real etcd-backed runtimes, real readiness and
placement surfaces, and explicit failover invariants. Because LogPose still
lacks a public shard-promotion API, ownership moves in that workflow remain
helper-driven rather than endpoint-driven.

### 5. Targeted Fuzzing and Property Tests

The next deepening layer after those generative harnesses is subsystem fuzzing and property-style verification.

Near-term candidates include:

- WAL frame parsing and replay behavior
- manifest parsing and storage metadata loading
- segment decoding and corruption handling
- CLI text and JSON output surfaces where structured inputs can be generated cheaply

These tests should focus on invariants and malformed-input behavior, not only on coverage volume.

### 6. Full-System Simulation

The long-term target is a deterministic full-system layer for multi-component and eventually multi-node behavior. This is where LogPose moves closest to the TigerBeetle mindset.

Over time, this layer should cover scenarios such as:

- process restart and recovery sequences
- background maintenance interacting with foreground reads and writes
- network delay, loss, reordering, duplication, and partition once remote runtime boundaries exist
- time-sensitive visibility and durability transitions under virtual time
- crash and restart behavior across multiple nodes
- cluster or service-level orchestration behavior

The intended shape is a seeded event loop with replayable faults, explicit safety invariants, and later healthy-core liveness checks. The existing deterministic control-plane harness is a precursor to that system, not the finished version.

## Current Adoption Plan

We are adopting the TigerBeetle-inspired structure incrementally.

### Now

- storage harness v2 (`crates/logpose-storage/tests/harness`, run with `cargo test -p logpose-storage --test harness`), which checks the engine against a model of the logical state:
  - randomized model checking over the full action table (upserts, partial updates, deletes, filter writes, schema changes, get, count, scroll, order by, search, snapshot tokens and a manual clock, flushes, compactions, job phases stepped by hand or through the paused scheduler, crashes in every tear mode including inside recovery, failed file and directory syncs, and in-process reopens), on `FaultVfs` and on the real filesystem; a failure prints its seed and a replay command and is shrunk to a short trace
  - exhaustive crash enumeration of the engine design's scenarios (a group commit, a flush, a compaction with concurrent deletes, a flush during a compaction, a checkpoint-only flush, a schema change and a flush, a failed manifest publish and its retry, GC after a token release): the recovered state is a prefix holding every acknowledged request, never behind what a concurrent reader saw, and the same as a clean recovery of the same disk image when that recovery is itself crashed at every operation
  - every interleaving of job phases with a short write sequence, and a time-bounded stress test of concurrent writers with invariant-checking readers
  - golden storage files (WAL, manifests, segments, a deletion-vector file) under `crates/logpose-storage/tests/golden`, written by a deterministic workload (fixed collection id, boot id, and directory name; no wall-clock or random bytes); after a deliberate format change, regenerate them with `LOGPOSE_UPDATE_GOLDEN=1 cargo test -p logpose-storage --test harness golden` and commit the result
- crash tests on `FaultVfs` (`crates/logpose-storage/tests/crash_recovery.rs`) for creating and dropping a collection, one test per named crash point, and failed WAL fsyncs
- seeded service and transport harnesses that exercise planner-controlled ANN, hybrid merge, and profile diagnostics paths
- deterministic service-boundary simulation scenarios for control-plane/runtime status, placement diagnostics, persistence/recovery behavior, recorded placement, and wrong-plane rejection, with REST and gRPC parity checks focused on the same read-side operator contracts
- continued explicit regression coverage for storage atomicity and corruption cases
- checkpoint-aware recovery regressions for stale rolled WAL corruption and crash-window leftovers that would otherwise re-enter the mutable delta after reopen
- deterministic exact-vs-ANN regression suites and recall checks for immutable HNSW units
- reproducible Criterion benchmarks that pair exact baselines with planner-selected unfiltered ANN, filtered ANN, and tiny exact-fallback queries on fixed corpora
- snapshot-style CLI contract tests for runtime status, placement diagnostics, query explain/profile output, and selected inspect surfaces (`wal`, `manifest`, and `segment`)
- dedicated CI execution for randomized storage (time-bounded, with new seeds every run and a longer nightly run), randomized service, and CLI operator-contract suites so runtime failures stay attributable even though workspace compilation is still shared
- clearer separation between inline unit tests and external integration/harness tests

### Near-Term

- deeper fuzz/property harnesses for WAL, manifests, HNSW sidecars, storage metadata, and CLI input surfaces
- broader process-boundary validation beyond the current CLI and service operators
- deterministic simulator seams for time, transport, and failure injection instead of only filesystem or service-local orchestration

### Later

- a seeded multi-node simulator with virtual time, crash scheduling, and network-fault injection
- healthy-core liveness campaigns inspired by TigerBeetle's public simulation work
- deeper restart and recovery orchestration tests across metadata, storage, and remote serving boundaries
- fault-injection around disk, transport, and background maintenance behavior once those seams are explicit

## Benchmark Harness

`crates/logpose-bench` is the scoreboard for engine work. It drives a pluggable `BenchTarget` (today the in-process storage `Engine` with the staged `logpose_query::query` search) through bulk ingest, flush, unfiltered and filtered top-k search, and write-to-searchable probes, and it scores every answer against a brute-force oracle computed in the harness.

- datasets: a seeded clustered Gaussian generator, or SIFT-format `.fvecs` files with optional `.ivecs` ground truth to validate the oracle
- filters: exact selectivities (0.1, 1, 10, 50, and 99 percent by default), uncorrelated or anti-correlated with the queries, expressed with the `Predicate` AST as equality flags (default) or ranges
- metrics: ingest rows per second, write-to-searchable latency, single-client and multi-thread QPS, p50, p95, and p99 latency, recall@k, resident memory, and bytes read per query

Run it from the workspace root, always in release mode:

```bash
cargo run --release -p logpose-bench -- --preset smoke
cargo run --release -p logpose-bench -- --preset small --threads 4 --output target/logpose-bench/small.json
cargo run --release -p logpose-bench -- --base-fvecs sift_base.fvecs --query-fvecs sift_query.fvecs --ground-truth-ivecs sift_groundtruth.ivecs --queries 1000
cargo run --release -p logpose-bench -- --help
```

Without `--data-dir`, the harness runs the engine in a fresh `logpose-bench-<pid>-<nanos>` directory under the system temp directory and deletes it when the run ends, after the engine closes, also when the run fails with an error. A directory passed with `--data-dir` is created if missing and never deleted.

The JSON report records the machine, configuration, and per-case results. Committed baselines live in `benches/baselines/`; see its README for how to reproduce them. Compare new engine work against the latest baseline at the same preset and seed.

### Comparing With Milvus

Two subcommands run VectorDBBench-style workloads against a running server over the `logpose.v2` gRPC API, so LogPose can be compared with other vector databases on the same data and ground truth:

- `vdb-prepare --shape <shape> --data-dir <dir>` writes a dataset directory: base and query vectors (`.fvecs`), an integer `rank` column, and exact top-k ground truth (`.ivecs`) for unfiltered search and for `rank < t` filters at 1 and 99 percent selectivity. Shapes mirror VectorDBBench datasets (`cohere-100k`, `openai-50k`, `cohere-1m`, `openai-500k`) with synthetic embedding-like vectors, plus `tiny` for smoke tests. A matching directory is reused.
- `vdb-run --dataset <dir> --endpoint <grpc>` loads the dataset (one `BulkUpsertRecords` stream, then flush and compact), sweeps the query `ef` for each case until mean recall@10 reaches 0.95, and measures QPS and p50 and p99 latency at that `ef` with 1, 4, and 8 concurrent clients, each on its own connection.

`scripts/bench/milvus_vdb.py` runs the same cases against Milvus with `pymilvus` and writes the same report shape, and `scripts/bench-milvus.sh` runs the whole comparison end to end: it starts a fresh `logpose-server`, then a fresh Milvus standalone container (never both at once), and writes `benches/baselines/phase5-milvus-<shape>.{json,md}`:

```bash
LOGPOSE_BENCH_DATA=$HOME/.cache/logpose-bench scripts/bench-milvus.sh cohere-100k openai-50k
```

It runs on Linux and needs `flock` from util-linux, Docker for Milvus (`SKIP_MILVUS=1` runs LogPose alone), and Python 3 with `venv`; it creates a venv with `pymilvus` (pinned to 3.0.2) and `numpy` under `LOGPOSE_BENCH_DATA`.

Each run keeps its server data, Milvus volume, binaries, raw reports, and logs in `LOGPOSE_BENCH_DATA/runs/<run id>` and holds a lock on that directory while it runs. It refuses to start when the lock is taken or a port it needs is already in use, and after starting `logpose-server` it checks that the server is alive and owns both listening ports. `LOGPOSE_BENCH_RUN_ID` (default `default`), `LOGPOSE_BENCH_GRPC_PORT` (15051), `LOGPOSE_BENCH_REST_PORT` (18080), `LOGPOSE_BENCH_MILVUS_PORT` (19530), and `LOGPOSE_BENCH_MILVUS_HEALTH_PORT` (9091) select the run and its ports, and the Milvus container is `logpose-bench-milvus-<run id>`. A run stops the `logpose-server` and Milvus container that a killed (SIGKILL) earlier run with the same run id left behind. To run two benchmarks on one host at the same time, for example the base and the change of an A/B comparison, give each a distinct run id, distinct ports, and its own `OUTPUT_DIR`; `benches/baselines/README.md` has the recipe.

## Non-Negotiable Harness Rules

Every new generative, fuzzing, or simulation harness in LogPose should satisfy these rules:

1. Deterministic seeds and reproducible replay are required.
2. Every harness must have an explicit oracle, model, or invariant set. "It did not panic" is not enough.
3. Failures must print the seed and scenario trace needed to replay the case.
4. Prefer testing production codepaths and real binaries over test-only alternate implementations.
5. Generators and harness support code must be reusable. Avoid one-off ad hoc random loops.
6. Keep scenarios bounded so CI runtime remains predictable.
7. Preserve focused regression tests for bugs and contracts that deserve named coverage even if a generative harness also reaches them.
8. ANN-capable harnesses must compare approximate paths against an exact oracle or a documented recall envelope.
9. Full-system simulation failures must save enough context to replay the failing seed and fault schedule.

## Test Placement Policy

LogPose uses one consistency rule for test organization:

- keep tight unit tests inline with the module they protect
- place async, filesystem, workflow, harness, snapshot, and future simulation tests in crate-level `tests/`

This keeps production files focused while still allowing private units to stay close to the code they exercise.

## Temporary Directories

Every directory or file a test, benchmark, or harness creates on the real filesystem is removed when the test ends, also when it fails or panics. A test run must leave nothing behind in the system temp directory.

- Create it with the `tempfile` crate (a dev-dependency of every crate that needs one), through the crate's helper such as `unique_temp_dir(label)`. The helpers name it `logpose-<label>-<random>` so a directory seen while a test runs can be traced to its test. Never hand-roll `std::env::temp_dir().join(...)`, and do not remove directories by hand at the end of a test: the guard does it, including on panic.
- Hold the `TempDir` guard for as long as anything uses the directory, including across a simulated crash and reopen. Helpers that build a node configuration return the guard beside it, as in `let (config, _root) = test_config("label");`. Bind it to a named variable such as `_root`, never to `_`, which drops it at once.
- Declare the guard before the engine or node that uses the directory, or as the last field of a fixture struct, so the engine closes before the directory is removed.
- A harness that keeps a failing case's directory for debugging keeps it only on failure and prints its path. The storage and service harnesses replay a failure from its seed instead, so they remove the directory either way.

Tests honor `TMPDIR`. To check that a run leaks nothing, point it at an empty directory and check that the directory is still empty afterwards:

```bash
export TMPDIR="$(mktemp -d)"
cargo test --workspace
ls -A "$TMPDIR"
```

## CI Philosophy

CI should reflect the same layered strategy.

Unrelated checks should not be serialized into one long job when they can run independently. Rust formatting and linting, conventional tests, generative harnesses, repository hygiene checks, docs, and supply-chain checks each provide different signal and should be allowed to fail independently.

This matters for two reasons:

- it shortens feedback loops for contributors
- it creates a durable home for future fuzzing and simulation harnesses without redesigning CI every time a new layer is added

## Practical Standard For Future Work

When a new subsystem or boundary is introduced, the testing question is not only "what unit tests should we add?"

It is:

1. What are the local invariants?
2. What regression scenarios need names?
3. What model or oracle could drive a seeded generative harness?
4. Does this subsystem eventually need fuzzing, snapshots, or simulation coverage?
5. Where does this harness fit on the testing ladder above?

If future work follows that checklist, LogPose can expand toward TigerBeetle-style fuzzing and simulation discipline without losing consistency from one iteration to the next.
