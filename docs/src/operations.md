# Operations

LogPose is currently operated as one `logpose-server` process configured through `LOGPOSE_CONFIG`.

## Current Operator Model

Operational workflows are centered around:

- the `logpose-server` runtime
- the `logpose-cli` CLI as a server-first wrapper around the same control-plane and data-plane workflows
- structured logging and tracing
- repeatable CI/CD quality gates

Use the server as the source of truth for service behavior, and treat the CLI as the preferred operator entrypoint for configuration inspection, query diagnostics, and maintenance. The CLI now has two explicit modes:

- `logpose-cli interactive` for a guided dashboard with concern-based navigation, searchable workflow pickers, persistent result tabs, clipboard-friendly views, and a shortcut bar that stays visible while you work
- direct commands such as `status`, `config`, `database`, `collection`, `record`, `query`, and `inspect` for fast operator and scripting workflows

Direct commands default to concise human-readable summaries. Use `--json` or `--output json` when you need the exact machine-readable contract. REST and gRPC should remain transport-parity views over the same shared workflows, with no semantic drift between them.

Namespace handling is now database-first. `database list/show/put` sit at the top level, and collection, record, query, and inspect flows default to the `default` database unless you pass `--database <name>`. Human-facing collection labels use `database/collection` outside the default database and collapse to just `collection` inside it.

In interactive mode, the flow is intentionally layered: start from a broad concern area, narrow to a workflow, then fill a guided form with fuzzy-searchable selectors where appropriate. Results stay open instead of ending the session, which makes it practical to copy output, compare summary and json views, or jump straight back into repeat operations such as adding multiple files to the same collection.

## Runtime Boundaries

The runtime boundary is explicit today:

- control-plane workflows now expose runtime status and collection placement reasoning
- data-plane workflows remain responsible for writes, queries, maintenance actions, and storage inspection
- role-specific nodes now reject wrong-plane requests instead of silently serving them through the local filesystem path
- the CLI `status` and `collection placement` surfaces reflect server-reported runtime status and routing instead of synthesizing local guesses

## Operator-Facing Diagnostics

Operator-facing query diagnostics now include ANN-aware plan kinds, candidate generation and rerank timings, merge accounting, and fallback reasons. Query-unit artifact and component statistics are surfaced through collection stats and inspect outputs. Together, those surfaces make explain/profile and storage introspection part of the normal operational workflow rather than debugging-only escape hatches.

## Storage Engine

Each server process opens one storage engine on its `storage_root`, recovers every collection there at startup, and holds the root locked until it exits (see [Configuration](./configuration.md)). The engine's layout and guarantees are summarized in [Architecture](./architecture.md#storage-and-query-path).

- **Flush, compaction, and index builds.** All run in the background on their own triggers. `flush` makes every acknowledged write part of a segment and moves the WAL checkpoint past it; the segment's HNSW graph follows from a background index build (maintenance status `index`), and until it lands the segment is searched over its SQ8 codes. `compact` merges a collection's segments, smallest first, job after job until one is left or no two fit the maintenance memory, reclaims deleted rows, and builds every missing graph before it returns. Both return the snapshot they published. A client that loads data and wants it fully indexed calls `compact` (or waits for the maintenance status to show nothing pending or in progress).
- **Statistics.** Collection stats report the manifest generation, visible sequence number, live and deleted row counts, operations above the checkpoint, and one query unit per segment plus one for the memtables. A segment's `index_kind` is `hnsw` (graph and SQ8 codes), `sq8` (codes only), or `flat`; the memtables are `raw`. The stats' `maintenance` field shows jobs waiting for a permit, the job running, completed jobs, and the last failure with its count of consecutive failures.
- **Inspection.** `inspect` targets are `manifest` (the current manifest), `wal` (the rows written since the checkpoint: each with its sequence number, primary key `pk`, the record as the current schema reads it, its memtable, and whether a later write deleted it), `segment` (one segment's manifest entry, section table, and rows, each with its `pk` and whether it is deleted), and `maintenance`. Inspect payloads are diagnostics, not a stable contract.
- **Failures.** A WAL fsync failure makes the collection read-only (writes fail with `COLLECTION_POISONED`) until the server restarts; reads keep serving the last published state. A flush that fails five times in a row poisons the collection the same way, and one that fails because the device is full or read-only poisons it at once. A `wal/FSYNC_FAILED` marker refuses to reopen a collection in the same boot, because the page cache may still hold frames the disk never received. A collection whose recovery fails reports that error on every call, and the rest of the server keeps serving.
- **Background maintenance after a restart** starts for a collection on its first use (a read, write, stats call, or inspection), so a node that only reports status for a collection never runs its jobs.

## Local Podman Chaos

PR4's local multi-node chaos workflow is documented in [Podman
Chaos](./podman-chaos.md). That page covers the three-node Podman topology, the
repo-owned shell contract checks, the etcd helper used for ownership races, and
the exact invariants each scenario must preserve.

One rule matters for every failover drill today: LogPose still has no public
REST, gRPC, or CLI API for shard promotion. Ownership moves in the Podman chaos
lab must use a helper or example instead of a normal operator endpoint.

## Current Limits

Operationally, LogPose is still earlier than a distributed database:

- etcd-backed collection-assignment metadata can now be enabled, but metadata quorum, membership leases, and replica controllers are not complete
- bootstrap bearer authentication, operator-gated database admin, and database-scoped read/write/owner policies now exist, but principal lifecycle and richer policy listing/delete workflows are not complete
- health and readiness are still simple role-oriented signals, not dependency-aware distributed probes
- database descriptors are now explicit control-plane objects, but etcd-backed catalog replication and distributed namespace controllers are not implemented yet
- tracing is initialized, but a metrics endpoint and richer telemetry surfaces do not exist yet
- remote blob synchronization to MinIO or S3 is not implemented yet

Testing and CI are intentionally layered. The repository-level doctrine for generative harnesses, future simulation work, and concern-based CI decomposition lives in [Testing](./testing.md).

To enable etcd-backed assignment metadata, set explicit endpoints:

```toml
[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://127.0.0.1:2379"]
key_prefix = "/logpose/metadata"
timeout_ms = 1500
membership_ttl_secs = 15
leadership_ttl_secs = 10
cluster_name = "default"
```

With `metadata.backend = "etcd"`, LogPose now treats etcd as the authoritative
source for collection descriptors and assignments; each node's storage engine
holds the data of the collections it serves, and collection creates and drops
are fenced by the control-plane leader's lease. Collections created before
the etcd metadata path is enabled are not auto-backfilled from local
`placement.json` files; migrate them by recreating them through the control
plane or by explicitly backfilling metadata before flipping an existing storage
root to the etcd backend.

The coordination loop runs every third of the shorter TTL. Each tick it
refreshes the membership and leadership leases and checks that etcd still holds
the matching member and leader keys. If a lease has expired or been revoked, or
its key is missing, the node reports the claim as lost at once, then registers
or campaigns again in the same tick. Losing membership also gives up
leadership, so a node that is not a registered member never keeps leading.
`timeout_ms` also bounds each keep-alive round trip.
