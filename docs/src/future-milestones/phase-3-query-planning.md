# Phase 3: Query Planning

## Goal

Introduce a planner-led query architecture so LogPose can choose among scalar and vector execution strategies instead of hardcoding one retrieval path per request type.

## Architectural Shift

This phase moves LogPose from "query execution" to "query planning plus execution."

The planner does not need full relational optimizer complexity immediately. It does need enough structure to:

- inspect storage and segment statistics
- estimate filter selectivity
- choose exact and hybrid access paths intentionally
- explain why it made those choices

## Component Changes

| Component | Change Needed |
| --- | --- |
| Query subsystem | Evolve `logpose-query` into a planning-aware subsystem, or split planner responsibilities into a dedicated crate if the boundary becomes clearer that way |
| Metadata filtering | Expand from basic equality semantics toward broader scalar predicates and reusable predicate evaluation infrastructure |
| Storage statistics | Surface the per-tier and per-unit stats the planner needs for selectivity, cost heuristics, and candidate sizing |
| Explainability | Add explain or profile output for vector and hybrid queries so operators can see chosen plans, candidate counts, and major cost centers |
| Transport and CLI surfaces | Expose planner-aware diagnostics through REST, gRPC, and CLI without duplicating business logic in each transport |
| Observability | Track plan choice, scan scope, filter selectivity, rerank count, and slow-query diagnostics in structured telemetry |
| Docs | Document hybrid visibility, predicate semantics, and explain outputs as first-class operator concepts |

## What "Done" Looks Like

- LogPose chooses among multiple exact and hybrid paths based on storage state and predicates
- planner statistics exist and are stable enough to reason about operator choice
- explain output makes query behavior inspectable without reading internal code
- scalar and vector work are no longer separate conceptual subsystems glued together ad hoc
- planner decisions can evolve without rewriting external APIs

## Testing Direction

Phase 3 should add planner-focused verification layers:

- planner unit tests for cost heuristics, predicate classification, and plan assembly
- regression tests for known selectivity edge cases and explain output stability
- seeded generative query harnesses with explicit model or oracle expectations for selected plans and visible results
- snapshot-style assertions for explain output where stable rendered text improves clarity
- property-style checks that different plan choices preserve the same visibility and correctness contract

This is the phase where the testing ladder starts validating "why that plan was chosen," not only "whether the query returned rows."

## What This Unlocks

Once planning is real, ANN can become an operator family instead of a special mode.

That makes Phase 4 cleaner: LogPose can add ANN under the planner instead of restructuring the whole engine around one index type.
