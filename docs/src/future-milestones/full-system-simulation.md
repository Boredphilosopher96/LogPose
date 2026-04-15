# Full-System Simulation

## Goal

Turn LogPose's current mix of deterministic service scenarios, randomized harnesses, and recovery tests into a TigerBeetle-style full-system simulation layer with replayable seeds, virtual time, controlled faults, and explicit safety and liveness checks.

## Current State

LogPose already has strong foundations:

- integration and regression suites across storage, service, query, transport, and CLI layers
- seeded randomized storage and service harnesses with replayable seeds
- deterministic service-boundary simulation for placement, restart, recovery, and wrong-plane rejection
- CLI contract tests that exercise real process boundaries

What is missing today:

- no deterministic multi-node event loop
- no simulated network, virtual clock, or storage-fault scheduler
- no crash and restart campaigns across more than one node in one harness
- no healthy-core liveness mode after arbitrary fault injection

## Why This Matters

The next class of bugs for LogPose will not be simple codec bugs. They will be ordering bugs, convergence bugs, failover bugs, and recovery bugs that only show up when multiple subsystems interact under failure.

That is exactly where TigerBeetle-style simulation becomes valuable: it exercises production logic under deterministic but adversarial schedules, then makes failures reproducible from a seed and trace instead of leaving them as intermittent CI noise.

## Research Anchors

- [TigerBeetle architecture](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/ARCHITECTURE.md)
- [TigerBeetle VOPR internals](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md)
- [TigerBeetle VOPR source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/vopr.zig)
- [Simulation testing for liveness](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness)
- [Random fuzzy thoughts](https://tigerbeetle.com/blog/2023-03-28-random-fuzzy-thoughts)
- [FoundationDB testing](https://apple.github.io/foundationdb/testing.html)

## Direction For LogPose

Build a deterministic runtime harness rather than just adding more integration tests.

The likely shape is a dedicated simulator crate that runs multiple LogPose nodes in one process while replacing real time, sockets, and selected storage behavior with deterministic shims.

### Core Simulator Pieces

- an event scheduler that owns virtual time, timer delivery, and event ordering
- a simulated network with delay, drop, duplication, reordering, and partition support
- a simulated storage layer that can model crash windows, delayed persistence, and corruption or missing-artifact faults
- crash and restart control that drops in-memory state but preserves only simulated durable state
- a workload generator for mixed create, write, delete, query, flush, compact, inspect, and routing operations
- safety and liveness checkers with replayable traces

### Safety Before Liveness

Follow TigerBeetle's broad pattern:

- first inject arbitrary faults and validate safety invariants
- then freeze faults outside a healthy core and require that the healthy core converges and makes progress

LogPose should start with semantic convergence checks rather than byte-identical state replication. Once true replica and metadata coordination exist, the simulator can tighten its physical-state assertions.

## Main Work Streams

### 1. Deterministic Seams

- inject a clock abstraction instead of relying on wall time
- separate transport behavior from real listeners where simulation needs control
- add storage fault hooks below the current corruption-focused tests

### 2. Multi-Node Harness

- run multiple `AppState` instances under one deterministic controller
- support scripted scenarios first, then seeded randomized workloads
- keep current service and transport parity checks as reusable checkers inside the simulator

### 3. Replayability

- save seed, simulator config, and scenario trace on failure
- make replay a documented developer workflow
- build a corpus of previously found failures and replay them in CI

### 4. Invariant Library

- acknowledged writes remain visible after restart and recovery
- routing never serves from a node that lacks authority
- maintenance eventually reaches a quiescent state under healthy conditions
- future replica and metadata work converges without double ownership

## Testing And Validation

- keep existing unit, integration, randomized, and CLI tests as the base layer
- add scripted simulator scenarios before broad randomized campaigns
- validate safety invariants on every simulator run
- add liveness campaigns once a multi-node metadata plane exists
- run many simulator seeds in CI and save the failing replay artifacts automatically

## Exit Criteria

- LogPose has a deterministic multi-node simulator that runs from a single seed
- the simulator controls time, failure scheduling, and at least a first version of network and storage faults
- failures are replayable from saved seed and scenario details
- both scripted and randomized simulator campaigns exist
- safety invariants are checked automatically during simulation
- healthy-core liveness checks exist for the runtime behaviors that claim resilience
