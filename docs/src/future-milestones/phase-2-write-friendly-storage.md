# ~~Phase 2: Write-Friendly Storage~~

**Done marker:** Phase 2 is complete.

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

## Status

Phase 2 is done.

## Testing Direction

Phase 2 moved the testing ladder upward, not just widening the unit suite:

- seeded generative harnesses now cover tier transitions, maintenance, reopen, and recovery flows at the storage boundary
- named regression tests cover corruption handling, atomicity, and recovery edge cases that operators can actually hit
- the repository gained a dedicated randomized storage CI lane so those failures stay attributable

Deeper fuzz/property harnesses for WAL frames, manifest parsing, segment decoding, and storage metadata remained later work after the storage model itself was in place.

## What This Unlocks

Planner quality depends on real storage structure.

The engine now has mutable and immutable tiers, tombstones, per-unit statistics, background maintenance, and exact flat sidecars for immutable units. That foundation is in place, and Phase 3 can build planner behavior on top of real storage structure instead of a monolithic exact-scan view.
