# Future Milestones

This section now tracks only the major capabilities that are still missing from LogPose.

It is meant to be used alongside:

- [Architecture](./architecture.md) for the current workspace structure
- [Better Vector DB Architecture](./better-vector-db.md) for the target system shape
- [Testing](./testing.md) for the long-term testing ladder

The original phase roadmap is complete. LogPose already has:

- local filesystem durability with WAL, manifests, immutable segments, and maintenance recovery
- planner-led exact, ANN, and hybrid query execution
- operator-visible runtime status, placement diagnostics, stats, and inspect surfaces
- layered integration, randomized, process-boundary, and deterministic service-boundary testing

What remains is the next layer of system work: turning those local contracts into resilient multi-node behavior, broadening the vector operator family, deepening the testing model, and adding the missing product surfaces around storage and operations.

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
- richer metrics and readiness belong inside the Web GUI and simulation stories
- deeper fuzz/property work remains part of the testing ladder and should advance alongside new storage and index artifacts
- adaptive memory management, SIMD kernels, and broader filtered-search strategy work are captured in the endgoal convergence chapter so the roadmap stays aligned with `better-vector-db.md`

## How To Use This Section

Use the roadmap in two passes:

- start on this page to decide where a proposal fits in the overall program
- then use the matching milestone page to understand the intended component changes, research direction, testing strategy, and exit criteria

If future design work changes the end-state architecture, update [Better Vector DB Architecture](./better-vector-db.md) first, then realign these milestones to match.
