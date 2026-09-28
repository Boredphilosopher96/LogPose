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

- the control plane owns database and collection lifecycle, runtime status, and placement diagnostics
- the control plane also owns database policy persistence and operator-gated namespace administration
- the data plane owns writes, queries, maintenance execution, and storage inspection
- wrong-plane requests are rejected based on node role and recorded collection placement
- collection creation is currently accepted on `combined` nodes, not on `control`-only nodes

## Storage And Query Path

LogPose is still a local filesystem engine.

- mutable writes land in WAL-backed local state under `storage_root`; each collection has one writer task that applies write batches and schema changes in order
- each write batch is one CRC-framed WAL frame; concurrent batches share one fsync (group commit), a batch is acknowledged only after its fsync returned and the state that includes it is published, and recovery replays a batch entirely or not at all
- the WAL is a directory of `wal/<first sequence number>.wal` files (the WAL v2 format); a torn tail left by a crash is truncated when the collection is recovered, while damage followed by a later durable group, any damage in an older file, and a sequence gap are reported as corruption instead
- a failed append or fsync truncates the active WAL file back to its last synced group, fails the group's writes with an outcome (`not applied`, or `unknown` when that rollback failed too), and makes the collection read-only until the engine is reopened; when the rollback failed, a `wal/FSYNC_FAILED` marker refuses a reopen in the same boot
- storage roots written by earlier builds (a `wal/active.wal` file) are not readable and fail to open as corrupt; there is no migration
- writes land in an in-memory memtable; an upsert, update, or delete of an existing key sets a bit in the deletion vector of the unit (memtable or segment) holding its previous row, so every key has at most one live row
- flush writes the memtable's live rows as one immutable segment file (`segments/<unit>.seg`, the segment v2 format) and each grown deletion vector as a new `segments/<unit>.dv.<generation>` file; compaction rewrites segments without their deleted rows; segments carry no vector index sections yet, so ANN over a segment is served by an exact scan of its live rows
- storage roots written by builds before segment v2 (v1 `.lps` segments and `indexes/` sidecars) do not open; there is no migration
- a default database descriptor is now persisted under `storage_root/databases/default/descriptor.json`
- operator-facing namespaces are database-first: collection identities are `database/collection` outside the default database and just `collection` inside it
- collection state persists through `descriptor.json`, `placement.json`, `maintenance.json`, `CURRENT`, `manifests/`, `wal/`, and `segments/`
- every durable file is published by writing a temp file, fsyncing it, renaming it into place, and fsyncing the parent directory, except that a manifest generation is created under its final name and fsynced with its directory before `CURRENT` is renamed to name it; new segment and deletion-vector files fsync their directories before the manifest that references them is published, and a new WAL file is synced, with its directory, before any write lands in it
- one engine owns a `storage_root` at a time: opening the storage engine takes an exclusive lock on `storage_root/LOCK` and keeps every collection's state resident, so the server opens one engine and shares it between the data plane and the catalog, and a second engine (in another process or the same one) fails at startup with an "already in use by another engine" error
- a flush rotates the WAL to a new file before it writes its segment, so its checkpoint falls on a file boundary; once the manifest with that checkpoint is durable, the WAL files it covers are deleted, since no read goes back to the WAL
- segment files are deleted once no durable manifest and no in-memory version (including one a snapshot token pins) references them; opening a collection first syncs its directories, then removes every file the durable manifest does not reference
- all storage and WAL file I/O goes through the `Vfs` trait in `crates/logpose-vfs`: `StdVfs` in production, and `FaultVfs`, an in-memory filesystem that models lost unsynced data, torn writes, failed fsyncs and volatile directory entries, in crash tests
- the planner can choose exact execution, ANN over immutable units, or hybrid exact-plus-ANN merge
- mutable data remains on the exact path; until segments carry vector index sections, an immutable unit's ANN candidates come from an exact scan of its live rows

## Node Roles And Placement

The runtime exposes three node roles:

- `combined` serves both control-plane and data-plane workflows
- `control` serves only control-plane workflows
- `data` serves only data-plane workflows

Placement is currently persisted local metadata per collection: an assigned node name plus assigned role, surfaced through runtime-status and placement-diagnostics APIs. It is useful and operator-visible, but it is not yet a remote scheduler or shard-management layer.

## Current Limits

LogPose does not yet have:

- fully wired etcd control loops, even though an etcd-backed assignment metadata foundation exists
- dynamic cluster membership or watch-driven placement updates
- shard maps, replica sets, or failover controllers
- principal lifecycle management beyond bootstrap tokens, richer database policy enumeration/delete workflows, or non-bootstrap identity providers
- remote query dispatch or replication
- real MinIO or S3-backed immutable artifact storage

Those capabilities are future work, not hidden behavior in the current runtime.
