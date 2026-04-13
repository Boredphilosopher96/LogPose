# ~~Phase 4: ANN and Hybrid Execution~~

**Done marker:** Phase 4 is complete.

## Goal

Add ANN and richer hybrid retrieval as planner-controlled execution options while preserving the database-like storage and visibility rules established in earlier phases.

## Architectural Shift

This is the phase where LogPose becomes more than an exact-search engine, but it should do so without reverting to "ANN-first, everything else later."

ANN belongs here as a physical operator family alongside:

- exact scan
- scalar prefilter plus vector search
- vector-first candidate generation plus postfilter
- cooperative filtered ANN traversal
- rerank and late materialization stages

## Component Changes

| Component | Change Needed |
| --- | --- |
| `logpose-index` | Define ANN index families behind a stable operator-facing interface rather than exposing them as the architecture's main abstraction |
| Storage layout | Store the vector, routing, and metadata structures each physical operator needs, including compressed payloads and rerank-friendly raw data where appropriate |
| Planner | Choose among exact, ANN, and hybrid strategies using selectivity, top-k, tier state, and memory temperature |
| Query execution | Add candidate generation, postfilter, rerank, and merge stages that preserve snapshot semantics even when ANN is approximate |
| Memory management | Track graph topology, routing summaries, compressed payloads, raw vectors, and metadata state separately so residency decisions become intelligent |
| Operator surfaces | Add explain and profile details specific to ANN, filtered ANN, rerank, and memory behavior without requiring operator-managed load and release workflows |
| Benchmarking and docs | Define repeatable benchmark methodology and document where ANN is expected to help, where exact fallback should still win, and what correctness contract remains non-negotiable |

## What "Done" Looks Like

- ANN is available as one operator family inside the planner
- filtered retrieval quality no longer depends on one rigid execution recipe
- exact fallback remains available and intentional for tiny filtered populations or safety-critical cases
- memory accounting is good enough to explain where vector execution cost lives
- operator-facing explain output shows how filters, candidate generation, rerank, and merge contributed to query cost

## Status

Phase 4 is done.

## Testing Direction

This phase added verification layers that keep ANN honest:

- exact-vs-ANN comparison tests for correctness envelopes and recall guardrails
- targeted regression suites for filtered ANN behavior at different selectivity ranges
- corruption and property-style tests for index codecs, metadata payloads, and candidate-merging logic
- benchmark suites that are reproducible enough to detect planner or layout regressions
- service-level harness scenarios where planner choices move between exact and ANN paths under controlled inputs

The goal is disciplined hybrid retrieval, not just faster demos.

## What This Unlocks

Hybrid execution is now planner-directed and observable, with HNSW sidecars published for immutable units, ANN-aware diagnostics exposed through REST, gRPC, client, and CLI surfaces, and deterministic benchmarks living beside the query crate.

That puts LogPose in position to scale the runtime outward.

That makes Phase 5 about service architecture, orchestration, and fault handling rather than still trying to settle the storage and execution contract.
