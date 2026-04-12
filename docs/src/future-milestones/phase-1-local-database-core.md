# Phase 1: Local Database Core

## Goal

Turn the current exact-search prototype into a crisp single-node database core with explicit contracts, operator-ready workflows, and testing discipline strong enough to carry later storage and planner work.

## Architectural Shift

This phase moves LogPose from "a promising local vector service" to "a small but principled database core."

The emphasis is not on ANN or distribution yet. It is on correctness, visibility, contract clarity, and transport parity.

## Component Changes

| Component | Change Needed |
| --- | --- |
| Storage and durability | Tighten collection lifecycle, snapshots, delete visibility, flush, compact, inspect, and reopen behavior so the local engine has a stable contract |
| Query layer | Keep exact search as the default execution path, define metadata filter semantics clearly, and make ranking plus snapshot behavior explicit |
| Shared application layer | Centralize orchestration, validation, and transport-neutral error mapping so REST and gRPC stay thin |
| REST, gRPC, and CLI | Reach parity for collection management, writes, queries, stats, maintenance, inspect, and diagnostics |
| Runtime and telemetry | Expose health, build metadata, structured tracing, and enough metrics to reason about local operator workflows |
| Docs and contracts | Keep OpenAPI, proto, and operator docs aligned with actual behavior and validation rules |
| Testing and CI | Preserve focused unit and integration coverage while expanding seeded randomized harnesses at storage and service layers |

## What "Done" Looks Like

- a local operator can manage collections and data through REST, gRPC, and CLI without semantic drift
- exact query semantics, visibility rules, and maintenance commands are documented and test-backed
- shared service orchestration is the default integration path for new transport work
- contract drift between implementation, OpenAPI, and proto becomes the exception instead of the norm
- the seeded generative harnesses are normal infrastructure rather than one-off experiments

## Testing Direction

This phase should complete the first rung of the long-term testing roadmap in [Testing](../testing.md):

- strong local unit tests for validation and helper behavior
- crate-level integration tests for storage and service workflows
- seeded, replayable generative harnesses for real storage and service codepaths
- regression tests for every bug with operator-visible impact
- CI separation for linting, conventional tests, generative suites, docs, and supply-chain checks

This is also the phase where snapshot-style assertions for stable CLI and JSON output can start paying off.

## What This Unlocks

Phase 1 is the prerequisite for every later phase.

Without a crisp single-node contract, later work on mutable storage tiers, planning, ANN, and distribution would only scale ambiguity. Phase 2 should build on a storage core whose visibility and maintenance behavior is already trustworthy.
