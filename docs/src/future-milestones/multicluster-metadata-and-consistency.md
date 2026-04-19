# Multi-Cluster Metadata And Consistency

## Goal

Replace LogPose's local per-collection placement files with an authoritative metadata plane that can support multi-node and eventually multi-cluster serving, failover, and explicit consistency modes without losing the clean operator contracts the system already has today.

## Current State

LogPose has the beginnings of a control plane, but it is still local:

- node roles are explicit: `combined`, `control`, and `data`
- placement is persisted as local metadata per collection and surfaced through runtime-status and placement-diagnostics APIs
- wrong-plane requests are rejected based on that recorded local placement
- restart-aware service-boundary tests already cover status, placement, and wrong-plane rejection

What is missing today:

- no metadata quorum or authoritative metadata service
- no cluster membership, watch-driven routing cache, or replica map
- no failover state machine, ownership epoch, or lease-backed liveness
- no client-visible consistency-mode contract beyond local serving behavior

## Why This Matters

Metadata is the control plane for correctness in a distributed vector database.

If metadata diverges, the system can route writes to the wrong owner, miss fresh immutable units during query execution, rebuild indexes on the wrong node, or promote stale state after failure. LogPose already has useful local boundaries; this milestone is about making those boundaries authoritative across more than one process.

## Research Anchors

- [etcd API guarantees](https://etcd.io/docs/v3.5/learning/api_guarantees/) for strict-serializable writes plus watch semantics
- [etcd watches](https://etcd.io/docs/v3.5/tutorials/how-to-watch-keys/) and [leases](https://etcd.io/docs/v3.5/tutorials/how-to-create-lease/) for membership and change propagation
- [etcd failure handling](https://etcd.io/docs/v3.5/op-guide/failures/) and [recovery](https://etcd.io/docs/v3.5/op-guide/recovery/) for operational expectations
- [Milvus architecture overview](https://milvus.io/docs/architecture_overview.md), which uses etcd for metadata, service registration, and health-oriented coordination
- [Weaviate replication and consistency](https://docs.weaviate.io/weaviate/concepts/replication-architecture/consistency) as a reference for separating metadata coordination from replica consistency choices
- [Qdrant distributed deployment](https://qdrant.tech/documentation/operations/distributed_deployment/) as a reference for shard movement and recovery outside the consensus path
- [Azure Cosmos DB consistency levels](https://learn.microsoft.com/en-us/azure/cosmos-db/consistency-levels) and [CockroachDB follower reads](https://www.cockroachlabs.com/docs/stable/follower-reads) for practical consistency-mode vocabulary

## Direction For LogPose

Use etcd as the authoritative metadata plane for small transactional state, not for vectors or segment payloads.

Store persistent metadata there:

- database descriptors and database-scoped policy objects
- collection specifications
- shard and replica assignments
- ownership epochs
- segment and index artifact pointers
- replay or checkpoint progress needed for recovery and promotion

Store ephemeral metadata there through leases:

- node membership
- controller ownership
- active task claims
- health and liveness markers

Keep LogPose data where it belongs:

- WAL for fresh local durability
- local immutable artifacts for immediate serving
- object storage for remote immutable durability once that milestone lands

Use watch-driven local caches rather than polling. Every service should load a snapshot, start watching from the returned revision, and resynchronize cleanly after disconnect or compaction.

Use epoch-based ownership instead of implicit trust in node names. A node should only serve writes when it owns the relevant shard or replica at the current metadata epoch.

Start with one authoritative metadata control domain, not active-active global writes. Multi-cluster should first mean shared authoritative metadata plus remote consumers and replicas. Global multi-writer metadata is a later problem.

## What etcd Gives And What LogPose Must Still Build

etcd is the metadata substrate, not the distributed database itself.

What etcd gives LogPose directly:

- strongly consistent metadata writes
- watchable metadata revisions for routing and cache invalidation
- leases for liveness and membership heartbeats
- compare-and-swap transactions for ownership epochs and promotions
- building blocks for controller leader election

What LogPose still has to build on top of etcd:

- node membership semantics and health rules
- controller election policy and fencing for old leaders
- shard and replica placement policy
- failover and promotion state machines
- replica catch-up and repair logic
- query and write consistency-mode contracts
- data-plane split-brain prevention and ownership enforcement

## Consistency Modes To Add

Keep metadata writes strongly consistent by default. Placement, failover, and lifecycle changes should be linearizable.

Add read-side consistency modes intentionally:

- strong: authoritative metadata and ownership reads that drive routing or failover
- session: read-your-writes behavior after DDL or placement updates
- bounded staleness: lower-latency diagnostic or remote read paths that can tolerate lag behind the latest metadata revision
- eventual: non-authoritative observability caches only

The system should document which APIs allow which modes. Consistency should be a product contract, not a side effect of which node answered.

## Main Work Streams

## Implementation Checkpoint (April 17, 2026)

The repository now includes a first production-oriented metadata backend switch with an etcd option:

- `metadata.backend = "etcd"` enables etcd-backed authoritative collection assignment metadata
- collection create writes use create-if-absent transactions for both the authoritative assignment and descriptor metadata
- read paths fail closed if authoritative etcd metadata is unavailable instead of falling back to stale local placement files
- local placement files are still written for recovery/bootstrap diagnostics, but they are no longer consulted as an authority once the etcd backend is selected
- single-shard ownership records are now seeded in etcd, surfaced through placement diagnostics, and used to fence stale owners after an ownership promotion
- query and stats requests now accept lower-bound read barriers derived from prior write or read snapshots, which gives clients a concrete current-node monotonic-read primitive without breaking exact historical snapshot reads; once ownership is promoted, the system now fails those barriers closed until replica freshness metadata exists

This is still not full distributed control-plane completion. Remaining work from the streams below still applies, especially watch-driven caches, replica-aware placement, failover control loops, stronger leader fencing, and multi-node failover simulations.

### 1. Metadata Model

- add first-class shard, replica, and epoch types
- distinguish desired placement from current ownership and health
- track replica freshness and recovery progress explicitly

### 2. Metadata Service Layer

- add an etcd-backed metadata crate for transactions, watches, leases, and recovery
- add config for endpoints, TLS, auth, timeouts, and lease tuning
- replace `placement.json` as the authoritative source of truth

### 3. Control Loops

- membership registration through leases plus watch-driven cache updates
- controller leader election, leader handoff, and fencing
- placement and rebalance control
- failover and promotion control
- repair and catch-up control for lagging replicas

### 4. Data-Plane Enforcement

- gate writes on current ownership epoch
- surface serving consistency and replica freshness in query diagnostics
- make wrong-plane rejection metadata-driven instead of purely local-file driven

### 5. Operator Surfaces

- show metadata revision, ownership epoch, replica health, and watch lag
- explain why a collection or shard is placed where it is
- expose failover history and rebuild progress

## Testing And Validation

This milestone should extend the current testing ladder upward, not replace it.

- unit-test revision handling, CAS failures, lease expiry, and watch replay logic
- integration-test real etcd snapshot plus watch catch-up, watch compaction, lease loss, election handoff, and transactional placement updates
- add deterministic failover simulations for owner loss during write, flush, and index publication
- add multi-process tests with several metadata members and multiple LogPose nodes
- add metadata fault-injection inspired by Milvus' etcd chaos tests
- validate that strong, session, bounded-staleness, and eventual modes behave exactly as documented

## Exit Criteria

- etcd-backed metadata is authoritative for catalog, placement, and failover-critical state
- nodes register membership through leases and lose liveness by lease expiry
- controller leader election and fencing are explicit and tested
- placement is shard and replica-aware, not just collection-to-node local metadata
- ownership is epoch-based and prevents double serving
- failover and promotion behavior are deterministic and operator-visible
- at least one client-visible consistency contract exists beyond local-node behavior
- operators can inspect metadata revision, ownership epoch, replica health, and failover reasons
- deterministic simulation and multi-process tests cover metadata loss, failover, and recovery
