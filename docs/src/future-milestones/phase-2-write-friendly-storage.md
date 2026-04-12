# Phase 2: Write-Friendly Storage

## Goal

Reshape LogPose's local durability story around a real mutable plus immutable storage model so writes, deletes, compaction, and recovery behave more like a database and less like an offline segment pipeline.

## Architectural Shift

This phase is where the storage engine starts moving toward the target described in [Better Vector DB Architecture](../better-vector-db.md):

- a mutable delta tier for fresh writes
- immutable compacted units for durable historical data
- explicit version and tombstone behavior across both tiers

The system should still prefer correctness and predictable maintenance over index sophistication.

## Component Changes

| Component | Change Needed |
| --- | --- |
| `logpose-storage`, `logpose-catalog`, `logpose-wal` | Introduce a clearer delta tier, stronger manifest evolution, tier-aware compaction behavior, and richer segment or slab metadata |
| Storage visibility model | Make sequence numbers, snapshots, tombstones, and compaction visibility fully explicit across mutable and immutable data |
| Query execution | Merge mutable and immutable tiers under one snapshot-aware read path and keep deletes enforced before final ranking |
| Indexing | Support fast-build strategies for fresh units and higher-quality strategies for compacted units without making ANN the center of the design yet |
| Runtime and maintenance | Add background scheduling for flush and compaction with operator-visible progress, backpressure, and failure reporting |
| Operator surfaces | Expose tier-aware stats, inspect output, and maintenance diagnostics through the CLI and service APIs |
| Docs and contracts | Document durability, freshness, and maintenance semantics in a way that later planner work can depend on |

## What "Done" Looks Like

- fresh writes land in a clearly defined mutable tier
- compacted units are immutable, atomically visible, and rich enough to drive planning later
- deletes and updates remain correct across reopen, flush, compaction, and recovery
- storage stats are informative enough to explain backlog, tier sizes, and maintenance cost
- the query path no longer depends on a monolithic storage view that hides tier differences

## Testing Direction

Phase 2 should move the testing ladder upward, not just widen the unit suite:

- extend seeded generative harnesses to cover tier transitions, maintenance, reopen, and recovery flows
- add targeted fuzzing and property-style tests for WAL frames, manifest parsing, segment decoding, and storage metadata loading
- preserve named regression tests for corruption handling, atomicity, and recovery edge cases
- keep CI jobs separate for generative suites and new fuzz/property coverage so failures stay attributable

This is the bridge from today's generative harnesses toward future simulation-oriented storage testing.

## What This Unlocks

Planner quality depends on real storage structure.

Once the engine has mutable and immutable tiers, tombstones, and per-unit statistics, LogPose can begin choosing query strategies based on actual data layout instead of only fixed exact-query behavior. That is the foundation for Phase 3.
