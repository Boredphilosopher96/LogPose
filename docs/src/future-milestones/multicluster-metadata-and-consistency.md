# Multi-Cluster Metadata And Consistency

## Goal

Replace LogPose's local per-collection placement files with an authoritative metadata plane that can support multi-node and eventually multi-cluster serving, failover, and explicit read-barrier contracts without losing the clean operator contracts the system already has today.

## Status

This milestone is **in progress** as of April 20, 2026.

This branch lands a substantial control-plane slice for one authoritative etcd
metadata domain, but it does **not** close every exit criterion in this
milestone yet. Remote multi-cluster replication, discovery, and routing are
still later work that build on this control plane, and some same-domain
consistency and validation work remains open too.

LogPose now has:

- authoritative etcd-backed metadata for database descriptors, collection
  descriptors, assignments, and failover-critical owner records
- membership leases, explicit controller leadership, and watch-driven metadata
  caches, with direct authoritative etcd reads used when serving decisions
  need to stay safe during cache loss or lag
- epoch-fenced shard ownership plus public drain, undrain, promote, and
  rebalance controls over REST, gRPC, and CLI
- replica-aware placement diagnostics with desired replicas, metadata revision,
  ownership epoch, the current `watch_lag` field, and operator-visible
  failover reasons
- automatic leader-side owner failover when a desired replica already has
  materialized local state, while stale replicas stay fenced instead of
  auto-promoting
- concrete client-visible consistency contracts through exact historical
  snapshots and lower-bound read barriers, including fail-closed behavior after
  ownership promotion
- seeded multi-process Podman chaos coverage as the required local
  control-plane gate for failover, lease loss, partitions, metadata outages,
  and stale-replica non-promotion

What is still pending before this milestone can be called complete:

- the product still exposes exact snapshots plus lower-bound read barriers,
  not the fuller named consistency surface originally sketched for this
  milestone such as session tokens or bounded-staleness reads
- replica repair and catch-up exist for the current archive-transfer path, but
  the broader replica-read serving contract and the richer freshness model are
  not fully closed yet
- the branch has targeted etcd, API, config, and docs verification, but the
  full local completion gate (`scripts/check.sh`, `scripts/pre-push-checks.sh`)
  was still in progress when this snapshot was pushed
- the requested five-agent holistic review swarm and the follow-up fix loop are
  still pending on top of this branch state

The separate [Full-System Simulation](./full-system-simulation.md) milestone is
still open. That later milestone owns TigerBeetle-style deterministic
whole-system replay rather than the real-process chaos gate used here.

## Starting State

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

## What etcd Gives And What LogPose Built On Top

etcd is the metadata substrate, not the distributed database itself.

What etcd gives LogPose directly:

- strongly consistent metadata writes
- watchable metadata revisions for routing and cache invalidation
- leases for liveness and membership heartbeats
- compare-and-swap transactions for ownership epochs and promotions
- building blocks for controller leader election

What LogPose built on top of etcd for this milestone:

- node membership semantics and health rules
- controller election policy and fencing for old leaders
- shard and replica placement policy
- failover and promotion state machines
- stale-replica fencing and fail-closed read-barrier behavior
- exact snapshot plus lower-bound read-barrier contracts
- data-plane split-brain prevention and ownership enforcement

## Consistency Contracts Added

Metadata writes remain strongly consistent by default. Placement, failover, and
lifecycle changes are linearizable through authoritative etcd metadata.

The current client-visible read contracts are:

- exact snapshots for historical reads against a specific visible state
- lower-bound read barriers for current-owner monotonic reads after prior
  writes or reads

Promoted owners fail read barriers closed after ownership promotion, so
consistency remains explicit instead of becoming a side effect of whichever
node answered. Read-barrier continuity across ownership promotion is still a
later feature.
There are not multiple named consistency levels yet; the product exposes the
exact snapshot and lower-bound barrier contract directly.

## Delivered Work Streams

### 1. Metadata Model

- first-class ownership epochs, desired replicas, failover reasons, and
  metadata revisions are now surfaced through placement diagnostics
- desired placement is distinguished from current ownership and membership
  readiness
- collection creation persists explicit `replication_factor` intent in
  authoritative metadata

### 2. Metadata Service Layer

- the etcd-backed metadata layer now owns authoritative descriptors,
  assignments, membership records, leadership records, owner epochs, and
  failover reasons
- services load a point-in-time snapshot, watch for changes from that
  revision, and fall back to direct authoritative etcd reads when the watch
  cache is unavailable so serving decisions stay fenced by current metadata
- `placement.json` remains a local recovery artifact only; it is not the
  authority once the etcd backend is selected

### 3. Control Loops

- membership registration is lease-backed and operator-visible through runtime
  status
- controller leader election and handoff are explicit and fenced by leased
  leadership metadata
- placement, drain, undrain, rebalance, and promotion controls are now public
  server surfaces
- automatic owner failover promotes a desired local replica through
  compare-and-swap when the leader sees the old owner lose readiness and the
  replacement already has materialized state

### 4. Data-Plane Enforcement

- writes are gated on the current ownership epoch
- wrong-plane reads and writes are rejected from authoritative metadata rather
  than local placement files
- exact snapshots and lower-bound read barriers form the current client-visible
  consistency contract beyond local-node behavior
- promoted owners fail lower-bound read barriers closed until freshness
  metadata exists

### 5. Operator Surfaces

- runtime status and placement diagnostics now expose metadata revision,
  ownership epoch, replica targets, the current `watch_lag` field, and
  failover reasons
- public CLI, REST, and gRPC controls exist for drain, undrain, promote, and
  rebalance workflows
- the seeded Podman chaos harness is now the checked local gate for metadata
  outage, failover, membership churn, partition recovery, and stale-replica
  non-promotion

## Current Validation

This branch extends the current testing ladder upward instead of replacing it.
What has been verified on this branch so far:

- unit and service-boundary tests cover watch-state handling, CAS conflicts,
  lease expiry, read-barrier fencing, and control-plane routing decisions
- real etcd integration tests cover snapshot plus watch catch-up, membership
  leases, leadership handoff, public promotion, automatic failover, stale
  replica fencing, and fail-closed behavior on metadata loss
- mdBook documentation still builds cleanly
- seeded Podman chaos campaigns are the intended required local PR gate for the
  multi-node control plane, but the full end-to-end `scripts/check.sh` run was
  interrupted before this branch snapshot was published
- the later full-system simulation milestone still owns deterministic
  whole-system event replay and virtual-time campaigns

## Remaining Exit Criteria

The milestone should only be marked complete once all of these are true:

- the current exact-snapshot and read-barrier contract is either accepted as
  the final consistency surface for this milestone or is expanded into the
  richer named consistency and session-token model originally planned
- replica repair, catch-up, and freshness semantics are documented and exposed
  clearly enough that replica serving behavior is operator-visible instead of
  inferred from current implementation details
- the full local completion gate finishes cleanly on the branch, including the
  repo-wide `scripts/check.sh` path and `scripts/pre-push-checks.sh`
- the requested five-agent holistic review swarm is completed on the current
  branch state, and any serious findings from that review loop are fixed
