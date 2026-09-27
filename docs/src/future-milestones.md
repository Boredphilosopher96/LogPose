# Future Milestones

The [Engine V2 Plan](./engine-v2-plan.md) supersedes the original phase roadmap. It holds the current audit, the design decisions, and the phase order. Start there to decide what to work on next.

The original phase roadmap is not complete in any production sense. Every phase has a code path and tests, but most of those paths are not engineered beyond test-fixture scale. The main gaps, from the engine-v2 audit:

- the storage engine is stateless per call: every operation reloads the descriptor and manifest and replays the unflushed WAL, so queries and writes do O(collection size) metadata work
- durability has known holes, including a torn WAL tail that is never truncated, missing directory and sidecar fsyncs, write batches without a commit record, and no exclusive lock on a storage root
- there are no scalar indexes; every predicate walks JSON per record
- filtered ANN post-filters and restarts with a larger `ef`, HNSW parameters are fixed and minimal, and distance kernels are scalar
- file I/O is blocking inside async code
- distribution fences metadata but not data: the etcd epoch never reaches storage, nothing is replicated, and failover drills copy directories by hand
- there is no benchmark baseline to measure any of this against

This page and the milestone chapters below remain as longer-range design notes. Where they disagree with the engine-v2 plan, the plan wins. They are meant to be read alongside:

- [Architecture](./architecture.md) for the current workspace structure
- [Better Vector DB Architecture](./better-vector-db.md) for the target system shape
- [Testing](./testing.md) for the long-term testing ladder

## Remaining Milestone Map

| Milestone | Program Shift | Primary Outcome | Testing Shift | Details |
| --- | --- | --- | --- | --- |
| Multi-Cluster Metadata and Consistency | Move from local placement metadata to an authoritative distributed control plane | etcd-backed membership, controller elections, shard or replica ownership, failover, resilience, and explicit consistency modes | Multi-process metadata tests, lease loss, election handoff, failover simulation, and fault-injection around metadata outages | [Details](./future-milestones/multicluster-metadata-and-consistency.md) |
| Additional Vector Index Families | Move from one ANN family to planner-selected index families | IVF-based and compression-aware operators alongside HNSW, with better workload fit and richer explain surfaces | Exact-oracle validation, filtered-selectivity regressions, codec corruption tests, and family-specific benchmarks | [Details](./future-milestones/additional-vector-index-families.md) |
| Full-System Simulation | Move from local and service-boundary harnesses to deterministic system simulation | TigerBeetle-style seeded simulation with virtual time, network and crash faults, replayability, safety checks, and liveness checks | Multi-node simulator campaigns, replayable failures, and healthy-core convergence testing in CI | [Details](./future-milestones/full-system-simulation.md) |
| Web GUI | Move from CLI plus raw API surfaces to a real operator and developer console | Browser-based runtime, collection, query, inspect, and maintenance workflows | Browser end-to-end coverage plus API contract tests for all UI-backed operations | [Details](./future-milestones/web-gui.md) |
| Blob Storage Integration | Move immutable artifacts from local-only files to real object storage | MinIO and S3-backed segment and index bundles, remote sync, recovery, and operator-visible durability state | MinIO-backed integration suites, remote failure injection, restart reconciliation, and GC correctness tests | [Details](./future-milestones/blob-storage-integration.md) |
| Endgoal Convergence and Missing Capabilities | Close the remaining gap between the current milestone set and the `better-vector-db.md` endgoal | Adaptive residency, memory-aware planning, SIMD vector kernels, disk-native serving, and broader filtered-search strategy work are explicitly owned | Memory-sensitive benchmarks, kernel correctness checks, cold-versus-warm plan validation, and broader filtered-search strategy coverage | [Details](./future-milestones/endgoal-convergence-and-missing-capabilities.md) |

## Cross-Cutting Rules

The remaining work should still follow a few fixed rules:

1. Metadata authority must come before real multi-node serving.
2. New vector indexes must fit the planner model instead of bypassing it.
3. Object storage and multi-cluster work should share one immutable-artifact contract rather than inventing competing durability paths.
4. Simulation should deepen before chaos-style experimentation becomes the default systems test.
5. Operator ergonomics, auth, and observability must grow with the runtime instead of arriving after it.

## Additional Gaps Folded Into These Milestones

Some missing work does not need its own chapter yet because it is part of the milestone set above:

- auth, RBAC, and auditability belong inside the Web GUI and multi-cluster/operator stories
- richer database-scoped policy objects, auditability, and operator ergonomics should continue to grow out of the new database catalog surface rather than being bolted onto collections later
- richer metrics and readiness belong inside the Web GUI and simulation stories
- deeper fuzz/property work remains part of the testing ladder and should advance alongside new storage and index artifacts
- adaptive memory management, SIMD kernels, and broader filtered-search strategy work are captured in the endgoal convergence chapter so the roadmap stays aligned with `better-vector-db.md`

## How To Use This Section

Use the roadmap in two passes:

- start with the [Engine V2 Plan](./engine-v2-plan.md) to decide where a proposal fits in the phase order
- then use the matching milestone page here for longer-range component, research, and testing notes

If future design work changes the end-state architecture, update [Better Vector DB Architecture](./better-vector-db.md) first, then realign these milestones to match.
