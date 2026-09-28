# Architecture

## Workspace Shape

LogPose uses a layered Cargo workspace:

- `apps/logpose-server` hosts the main runtime
- `apps/logpose-cli` provides operator tooling
- `crates/logpose-*` isolate core concerns such as config, storage, indexing, query execution, auth, telemetry, and transport layers

The storage and query crates stack as follows (each depends on the ones below it):

- `logpose-service` and `logpose-core` wire the runtime: they hold one `Engine` and call it directly
- `logpose-query` compiles filters and runs vector search, get, count, scroll, and order-by over a storage `ReadView`, and implements the `RowSetResolver` the engine uses for filter writes
- `logpose-storage` owns every file: the `Engine`, each collection's `CollectionHandle`, the writer, memtables, segments, manifests, deletion vectors, the buffer cache, compaction, and garbage collection
- `logpose-index` builds and reads vector and scalar indexes over byte buffers and does no I/O
- `logpose-wal` is the write-ahead log format, reader, and writer
- `logpose-vfs` is the filesystem seam every storage and WAL file access goes through
- `logpose-types` holds the shared types: schemas, records, values, filters, and the typed error

The engine's design, with its invariants and file formats, is in [Engine Core Design](./engine-core-design.md).

## Runtime Shape Today

`logpose-server` is one process. It builds a single shared `AppState` and serves both REST and gRPC from that same runtime state.

- `crates/logpose-core` bootstraps the runtime: it opens one storage `Engine` on `storage_root` and shares it between the data plane and the local catalog of databases and principals; the catalog's descriptor files are read and written on the engine's I/O pool, never on a request handler's runtime worker
- `crates/logpose-service` contains the shared control-plane and data-plane services; the data plane resolves a collection's descriptor from its collection catalog, then calls the engine's `CollectionHandle` for writes, schema changes, flushes, compactions, statistics, and inspection, and `logpose-query` for reads
- `crates/logpose-api-rest` and `crates/logpose-api-grpc` expose transport-parity views over those services

The collection catalog is local or etcd-backed. With local metadata the engine's own descriptors are authoritative. With `metadata.backend = "etcd"`, etcd is authoritative for collection descriptors, placement assignments, and shard ownership (`EtcdCollectionCatalog` in `logpose-storage-etcd`), while the engine still holds the data of the collections this node serves: a create writes pending metadata to etcd, then the local collection, then marks the metadata ready, and a drop removes the local collection first and the metadata last, both fenced by the control-plane leader's lease. Each create and drop runs as a task of its own, so a caller that stops waiting (a client that hangs up, a request timeout) never leaves it half done. A create that still stops between its steps, because its process died or etcd failed after the local create, leaves pending metadata that the leader resolves when it gains leadership and every 30 seconds while it leads: it marks the metadata ready when its own engine holds the collection, opened, and otherwise removes the metadata (and a local copy of the collection that failed to open); pending metadata placed on another node waits for that node to lead.

The control-plane and data-plane split is real, but it is still in-process rather than distributed.

## Control Plane And Data Plane Today

- the control plane owns database and collection lifecycle, runtime status, and placement diagnostics
- the control plane also owns database policy persistence and operator-gated namespace administration
- the data plane owns writes, queries, maintenance execution, and storage inspection
- wrong-plane requests are rejected based on node role and recorded collection placement
- collection creation is currently accepted on `combined` nodes, not on `control`-only nodes

## Storage And Query Path

LogPose is a local filesystem engine. One `Engine` owns a `storage_root` and keeps every collection's metadata resident: a read never touches a metadata file.

- one engine owns a `storage_root` at a time: opening it takes an exclusive lock on `storage_root/LOCK`, so the server opens one engine and shares it between the data plane and the catalog, and a second engine (in another process or the same one) fails at startup with an "already in use by another engine" error
- collections are recovered when the engine opens, in parallel; a collection whose recovery fails is reported by every call on it, and the engine still opens
- each collection has one writer task that applies write batches, filter writes, and schema changes in order; a write batch is one CRC-framed WAL frame, concurrent batches share one fsync (group commit), and a batch is acknowledged only after its fsync returned and the state that includes it is published, so an acknowledged write is visible to every later read and recovery replays a batch entirely or not at all
- the WAL is a directory of `wal/<first sequence number>.wal` files; a torn tail left by a crash is truncated when the collection is recovered, while damage followed by a later durable group, any damage in an older file, and a sequence gap are reported as corruption instead
- a failed append or fsync truncates the active WAL file back to its last synced group, fails the group's writes with an outcome (`not applied`, or `unknown` when that rollback failed too), and makes the collection read-only until the engine is reopened; when the rollback failed, a `wal/FSYNC_FAILED` marker refuses a reopen in the same boot
- writes land in an in-memory memtable; an upsert, update, or delete of an existing key sets a bit in the deletion vector of the unit (memtable or segment) holding its previous row, so every key has at most one live row
- delete-by-filter and update-by-filter resolve their filter once, against the writer's latest state, and commit the matching keys as one ordinary batch
- a memtable freezes once it reaches a flush trigger (operation count, bytes, rows, or age) and a background flush writes it; with two memtables frozen and a third full, writes wait for a flush, and a write that waits longer than 30 seconds fails with `WRITE_STALLED`; a flush that keeps failing poisons the collection so writes fail fast
- flush writes the memtable's live rows as one immutable segment file (`segments/<unit>.seg`, the segment v2 format) with its index sections (scalar inverted and sorted indexes, SQ8 codes for a vector field with at least 1,024 rows, and an HNSW graph for one with at least 20,000), and each grown deletion vector as a new `segments/<unit>.dv.<generation>` file
- background compaction is size-tiered: once a collection has `compaction_threshold_segments` segments of one size tier (tiers of 32,768 live rows times powers of four), they are merged into one, and a segment of at least 32,768 rows with a fifth of them deleted is rewritten; merges are sized to the maintenance memory (a fifth of the memory limit, each job at most half of it), so each row is rewritten about once per tier it climbs; an explicit compaction merges every segment one job can hold
- a collection's background maintenance starts once it is first used (a read, write, statistics, or inspection) after the engine opens, so a node that only reports status for a collection never runs its jobs
- a flush rotates the WAL to a new file before it writes its segment, so its checkpoint falls on a file boundary; once the manifest with that checkpoint is durable, the WAL files it covers are deleted, since no read goes back to the WAL
- segment files are deleted once no durable manifest and no in-memory version (including one a snapshot token pins) references them; opening a collection first syncs its directories, then removes every file the durable manifest does not reference
- every durable file is published by writing a temp file, fsyncing it, renaming it into place, and fsyncing the parent directory, except that a manifest generation is created under its final name and fsynced with its directory before `CURRENT` is renamed to name it; new segment and deletion-vector files fsync their directories before the manifest that references them is published, and a new WAL file is synced, with its directory, before any write lands in it
- segment sections are read through one engine-wide buffer cache with a memory budget, loaded on demand in two stages per query (index and filter sections first, then the vector pages of the candidates)
- a read pins one published state (a `ReadView`) for its whole duration, so every get, count, scroll page, and search is consistent with one sequence number; a snapshot token pins a state across requests until it is released or expires
- vector search runs per unit: memtables and small segments are scanned exactly, a segment with SQ8 codes is scanned over its codes, and a larger segment with a graph is walked (admitting only filter matches, or ACORN-style for selective filters); every candidate is reranked with exact distances and one top-k is taken across units
- all storage and WAL file I/O goes through the `Vfs` trait in `crates/logpose-vfs`: `StdVfs` in production, and `FaultVfs`, an in-memory filesystem that models lost unsynced data, torn writes, failed fsyncs and volatile directory entries, in crash tests
- storage roots written by earlier builds (a `wal/active.wal` file, v1 `.lps` segments, or `indexes/` sidecars) do not open; there is no migration
- a default database descriptor is persisted under `storage_root/databases/default/descriptor.json`
- operator-facing namespaces are database-first: collection identities are `database/collection` outside the default database and just `collection` inside it
- collection state persists through `descriptor.json`, `placement.json`, `CURRENT`, `manifests/`, `wal/`, and `segments/`; maintenance status is runtime state, and a restarted node plans whatever maintenance its recovered state is due once the collection is next used

## Node Roles And Placement

The runtime exposes three node roles:

- `combined` serves both control-plane and data-plane workflows
- `control` serves only control-plane workflows
- `data` serves only data-plane workflows

Placement is persisted metadata per collection: an assigned node name plus assigned role, in the collection's `placement.json` with local metadata and in etcd with etcd metadata, surfaced through runtime-status and placement-diagnostics APIs. It is useful and operator-visible, but it is not yet a remote scheduler or shard-management layer.

## Current Limits

LogPose does not yet have:

- fully wired etcd control loops, even though etcd-backed collection metadata, membership, and leadership exist
- dynamic cluster membership or watch-driven placement updates
- shard maps, replica sets, or failover controllers
- principal lifecycle management beyond bootstrap tokens, richer database policy enumeration/delete workflows, or non-bootstrap identity providers
- remote query dispatch or replication
- real MinIO or S3-backed immutable artifact storage

Those capabilities are future work, not hidden behavior in the current runtime.
