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

- generated actions drive `LocalStorageEngine`
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

- a seeded, replayable state-machine harness at the storage boundary
- seeded service and transport harnesses that exercise planner-controlled ANN, hybrid merge, and profile diagnostics paths
- deterministic service-boundary simulation scenarios for control-plane/runtime status, placement diagnostics, persistence/recovery behavior, recorded placement, and wrong-plane rejection, with REST and gRPC parity checks focused on the same read-side operator contracts
- continued explicit regression coverage for storage atomicity and corruption cases
- checkpoint-aware recovery regressions for stale rolled WAL corruption and crash-window leftovers that would otherwise re-enter the mutable delta after reopen
- deterministic exact-vs-ANN regression suites and recall checks for immutable HNSW units
- reproducible Criterion benchmarks that pair exact baselines with planner-selected unfiltered ANN, filtered ANN, and tiny exact-fallback queries on fixed corpora
- snapshot-style CLI contract tests for runtime status, placement diagnostics, query explain/profile output, and selected inspect surfaces (`wal`, `manifest`, and `segment`)
- dedicated CI execution for randomized storage, randomized service, and CLI operator-contract suites so runtime failures stay attributable even though workspace compilation is still shared
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
