# Future Milestones

This section turns LogPose's architecture direction into a long-term delivery roadmap.

It is meant to be used alongside:

- [Architecture](./architecture.md) for the current workspace structure
- [Better Vector DB Architecture](./better-vector-db.md) for the target system shape
- [Testing](./testing.md) for the long-term testing ladder

This roadmap is intentionally two-layered:

- this page stays high-level and explains the whole program arc
- the phase pages go lower-level and describe what each component should change over time

## Destination

The end-state for LogPose is not "an ANN service with metadata support."

It is a database-shaped retrieval engine with:

- write-friendly mutable plus immutable storage
- explicit visibility, freshness, and delete semantics
- a planner that treats vector search as a first-class physical operator
- ANN as one operator family inside that planner, not the center of the architecture
- adaptive memory and observability instead of operator-managed residency by default
- a testing ladder that grows from unit and integration coverage into generative, fuzzing, and simulation-style system testing

## Starting Point

Today, LogPose already has the beginnings of the local database core:

- a layered Rust workspace with focused crates
- REST and gRPC service surfaces
- planner-led exact query with structured predicates and explain/profile diagnostics
- durable local storage with mutable and immutable units, background maintenance, and recovery behavior
- shared service orchestration across transports
- seeded randomized harnesses at storage and service boundaries

This section records how that base was turned into the architecture described in [Better Vector DB Architecture](./better-vector-db.md).

## Component Map

The phase pages refer to logical components rather than only one specific file or crate, but the current code homes are already visible in the workspace:

| Component Area | Current Homes | Long-Term Role |
| --- | --- | --- |
| Storage and durability | `crates/logpose-storage`, `crates/logpose-catalog`, `crates/logpose-wal` | Mutable and immutable storage, manifests, compaction, recovery, layout statistics |
| Query and planning | `crates/logpose-query` and future planner-oriented crates if needed | Exact execution today, then plan selection and hybrid operator orchestration |
| Indexing | `crates/logpose-index` | ANN and vector indexing families under planner control |
| Shared application layer | `crates/logpose-service`, `crates/logpose-core` | Request orchestration, validation, error mapping, shared runtime state |
| Transport and operator surfaces | `crates/logpose-api-rest`, `crates/logpose-api-grpc`, `apps/logpose-cli`, `crates/logpose-client` | Stable external APIs, diagnostics, explain surfaces, admin workflows |
| Runtime and operations | `apps/logpose-server`, `crates/logpose-config`, `crates/logpose-telemetry`, `crates/logpose-auth` | Service runtime, observability, policy, control-plane behaviors |
| Testing and CI | crate-level `tests/`, workspace scripts, CI workflows, future harness directories | The full testing ladder from unit checks to simulation-oriented system testing |

## Phase Map

| Phase | Program Shift | Primary Outcome | Testing Shift | Details |
| --- | --- | --- | --- | --- |
| ~~1. Local Database Core~~ | ~~Finish the single-node exact-search database contract~~ | ~~Crisp APIs, explicit visibility rules, operator-ready workflows~~ | ~~Broaden unit, integration, and seeded generative harnesses~~ | ~~[Phase 1](./future-milestones/phase-1-local-database-core.md)~~ |
| ~~2. Write-Friendly Storage~~ | ~~Move from simple local durability to a real mutable plus immutable storage model~~ | ~~Delta tier, tombstone-aware merge, compaction planning, richer storage stats~~ | ~~Seeded storage harnesses and corruption-focused recovery coverage establish the next testing layer, with deeper fuzz/property work left for later~~ | ~~[Phase 2](./future-milestones/phase-2-write-friendly-storage.md)~~ |
| ~~3. Query Planning~~ | ~~Move from handcrafted exact query execution to plan-directed retrieval~~ | ~~Planner statistics, explain surfaces, scalar plus vector path selection~~ | ~~Planner oracles, explain snapshots, richer generative query scenarios~~ | ~~[Phase 3](./future-milestones/phase-3-query-planning.md)~~ |
| ~~4. ANN and Hybrid Execution~~ | ~~Add ANN as a physical operator family under planner control~~ | ~~Exact and ANN hybrid execution with filtered retrieval strategies~~ | ~~Exact-vs-ANN correctness checks, ANN codec hardening, and benchmark discipline~~ | ~~[Phase 4](./future-milestones/phase-4-ann-and-hybrid-execution.md)~~ |
| ~~5. Distribution and Operations~~ | ~~Grow from single-node engine to service platform~~ | ~~Explicit control-plane/data-plane boundaries, placement diagnostics, operational control~~ | ~~Deterministic multi-component, restart, and simulation-oriented harnesses~~ | ~~[Phase 5](./future-milestones/phase-5-distribution-and-operations.md)~~ |
| ~~6. Mature Platform~~ | ~~Finish the system and harden the end-state architecture~~ | ~~Planner-led retrieval database with mature operations and disciplined research extensions~~ | ~~Testing ladder established across core boundaries, with room to deepen fuzzing and simulation later~~ | ~~[Phase 6](./future-milestones/phase-6-mature-platform.md)~~ |

## Cross-Cutting Rules

The roadmap is phase-based, but a few rules should stay constant across all phases:

1. Storage and visibility rules come before clever ANN work.
2. Planner quality comes before large-scale distribution of hybrid query behavior.
3. Testing evolves with the architecture. Every new subsystem should advance the testing ladder described in [Testing](./testing.md).
4. Operator ergonomics matter as much as raw query speed. Explainability, diagnostics, and maintenance controls are part of the product.
5. Later research-heavy optimizations should land only after the storage, planning, and observability contracts are already solid.

## How To Use This Section

Use the roadmap in two passes:

- start on this page to decide where a proposal fits in the overall program
- then use the matching phase page to understand the intended component changes, testing direction, and exit criteria

If future design work changes the end-state architecture, update [Better Vector DB Architecture](./better-vector-db.md) first, then realign these milestones to match.

**Done marker:** Phases 1, 2, 3, 4, 5, and 6 are complete and the documents on this page now reflect that status.
