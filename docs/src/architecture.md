# Architecture

## Workspace Shape

LogPose uses a layered Cargo workspace:

- `apps/logpose-server` hosts the main runtime
- `apps/logpose-cli` provides operator tooling
- `crates/logpose-*` isolate core concerns such as config, storage, indexing, query execution, auth, telemetry, and transport layers

## Runtime Shape Today

`logpose-server` is one process. It builds a single shared `AppState` and serves both REST and gRPC from that same runtime state.

- `crates/logpose-core` bootstraps the runtime and local storage engine
- `crates/logpose-service` contains the shared control-plane and data-plane services
- `crates/logpose-api-rest` and `crates/logpose-api-grpc` expose transport-parity views over those services

The control-plane and data-plane split is real, but it is still in-process rather than distributed.

## Control Plane And Data Plane Today

- the control plane owns collection lifecycle, runtime status, and placement diagnostics
- the data plane owns writes, queries, maintenance execution, and storage inspection
- wrong-plane requests are rejected based on node role and recorded collection placement
- collection creation is currently accepted on `combined` nodes, not on `control`-only nodes

## Storage And Query Path

LogPose is still a local filesystem engine.

- mutable writes land in WAL-backed local state under `storage_root`
- flush and compaction publish immutable segment files plus planner-visible index sidecars
- collection state persists through `descriptor.json`, `placement.json`, `maintenance.json`, `CURRENT`, `manifests/`, `wal/`, `segments/`, and `indexes/`
- the planner can choose exact execution, HNSW-backed ANN over immutable units, or hybrid exact-plus-ANN merge
- mutable data remains on the exact path; ANN is currently limited to immutable HNSW units

## Node Roles And Placement

The runtime exposes three node roles:

- `combined` serves both control-plane and data-plane workflows
- `control` serves only control-plane workflows
- `data` serves only data-plane workflows

Placement is currently persisted local metadata per collection: an assigned node name plus assigned role, surfaced through runtime-status and placement-diagnostics APIs. It is useful and operator-visible, but it is not yet a remote scheduler or shard-management layer.

## Current Limits

LogPose does not yet have:

- a metadata service such as etcd
- dynamic cluster membership or watch-driven placement updates
- shard maps, replica sets, or failover controllers
- remote query dispatch or replication
- real MinIO or S3-backed immutable artifact storage

Those capabilities are future work, not hidden behavior in the current runtime.
