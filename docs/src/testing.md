# Testing

LogPose treats testing as part of system architecture, not as a final verification step.

Our long-term testing structure is explicitly inspired by TigerBeetle's layered strategy. The goal is not to copy TigerBeetle's exact infrastructure or claim parity with its maturity. The goal is to follow the same discipline: use multiple complementary harnesses, make failures reproducible, and keep production behavior under test whenever practical.

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

Use unit tests for local pure behavior, validation logic, and private helper behavior that benefits from tight encapsulation.

- keep these close to the code under test with `#[cfg(test)]`
- use them for small, direct, non-workflow behavior
- prefer them when private internals matter more than shared harness reuse

### 2. Integration and Regression Tests

Use integration tests for filesystem-backed workflows, async behavior, cross-module behavior, and regressions that should read like operator-visible scenarios.

- place them under crate-level `tests/`
- use real public interfaces whenever possible
- keep regression tests explicit even when the same behavior is also covered by a randomized harness

### 3. Generative Harnesses

This is the first major TigerBeetle-inspired layer we are implementing.

Generative harnesses run seeded, replayable sequences of operations against real LogPose components and compare the observed results to an explicit oracle or model. These are not "random tests" in the loose sense. They are bounded, deterministic scenario generators with correctness checks.

For the first Phase 2 slice, the initial harness targets the storage boundary:

- generated actions drive `LocalStorageEngine`
- a model tracks expected logical visibility
- checks run after writes, snapshots, flushes, compaction, stats reads, and reopen/recovery steps
- failures must report the exact seed and action trace needed for replay

This layer is the bridge between ordinary integration tests and future simulation-style system testing.

### 4. Targeted Fuzzing and Property Tests

The next layer after the first storage harness is subsystem fuzzing and property-style verification.

Near-term candidates include:

- WAL frame parsing and replay behavior
- manifest parsing and storage metadata loading
- segment decoding and corruption handling
- CLI text and JSON output surfaces where structured inputs can be generated cheaply

These tests should focus on invariants and malformed-input behavior, not only on coverage volume.

### 5. Simulation-Oriented System Testing

The long-term target is a simulation-oriented layer for multi-component behavior. This is where LogPose moves closest to the TigerBeetle mindset for deterministic simulation testing.

Over time, this layer should cover scenarios such as:

- process restart and recovery sequences
- background maintenance interacting with foreground reads and writes
- network or transport faults once distributed/runtime components exist
- time-sensitive visibility and durability transitions
- cluster or service-level orchestration behavior

We do not need every component to have this layer immediately. We do need every new harness to fit into this structure so the repository evolves consistently toward it.

## Current Adoption Plan

We are adopting the TigerBeetle-inspired structure incrementally.

### Now

- a seeded, replayable state-machine harness at the storage boundary
- continued explicit regression coverage for storage atomicity and corruption cases
- clearer separation between inline unit tests and external integration/harness tests

### Near-Term

- targeted fuzz/property harnesses for WAL, manifests, storage metadata, and CLI surfaces
- snapshot-style assertions for stable textual and JSON operator output where that increases clarity
- dedicated CI execution for generative suites so they can evolve independently from general workspace tests

### Later

- simulation-style harnesses for multi-component workflows
- restart and recovery orchestration tests
- fault-injection around process, disk, transport, and time behavior where LogPose gains those boundaries

## Non-Negotiable Harness Rules

Every new generative, fuzzing, or simulation harness in LogPose should satisfy these rules:

1. Deterministic seeds and reproducible replay are required.
2. Every harness must have an explicit oracle, model, or invariant set. "It did not panic" is not enough.
3. Failures must print the seed and scenario trace needed to replay the case.
4. Prefer testing production codepaths and real binaries over test-only alternate implementations.
5. Generators and harness support code must be reusable. Avoid one-off ad hoc random loops.
6. Keep scenarios bounded so CI runtime remains predictable.
7. Preserve focused regression tests for bugs and contracts that deserve named coverage even if a generative harness also reaches them.

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
