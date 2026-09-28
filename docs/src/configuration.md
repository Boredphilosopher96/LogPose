# Configuration

LogPose currently loads default bootstrap settings or a TOML string from `LOGPOSE_CONFIG`.

Default endpoints:

- REST: `127.0.0.1:8080`
- gRPC: `127.0.0.1:50051`

Example:

```bash
export LOGPOSE_CONFIG='node_name = "edge-a"
node_role = "combined"
rest_host = "0.0.0.0"
rest_port = 8080
grpc_host = "0.0.0.0"
grpc_port = 50051
log_filter = "info,logpose=debug"
storage_root = ".logpose-edge-a"'
```

When `node_role` is omitted it defaults to `combined`. When `LOGPOSE_CONFIG` is provided, the remaining fields should still be present in the TOML payload.

`storage_root` belongs to one server process at a time. On startup the server takes an exclusive lock on `storage_root/LOCK` and holds it until exit; a second server pointed at the same directory exits with an error naming the directory and the pid holding it. The lock is released automatically when the owning process exits, so a stale `LOCK` file needs no cleanup. The lock is an advisory `flock`, so `storage_root` must live on a filesystem that supports it: startup fails with an error naming `LOCK` on filesystems that reject file locks, and network mounts that only lock locally (for example NFS mounted with `nolock`) cannot keep servers on different hosts apart.

## Request Size Limits

The optional `[limits]` table caps request and response sizes on both API
listeners:

```toml
[limits]
max_rest_body_bytes = 16777216     # 16 MiB, the default
max_grpc_message_bytes = 16777216  # 16 MiB, the default
```

- `max_rest_body_bytes` caps a REST request body. A larger body gets HTTP
  `413` with code `RESOURCE_EXHAUSTED` and reason `TOO_LARGE`; the error's
  metadata reports `limit_bytes` and, when the request declared a
  `Content-Length`, `size_bytes`.
- `max_grpc_message_bytes` caps one decoded gRPC request message. A larger
  message gets `RESOURCE_EXHAUSTED` with reason `TOO_LARGE`. Each message of a
  `BulkUpsertRecords` stream is one batch and is checked on its own, so
  bulk ingest is bounded per batch, not per stream.

The same limits bound the responses of record reads: a query, scroll page, or
get whose REST response or gRPC reply would be larger fails with `TOO_LARGE`
(HTTP `413` in REST) and is not sent.

Both values must be greater than 0. Either key may be omitted to keep its
default.

## Snapshot Tokens

The optional `[snapshots]` table sets how long a pinned state lives. A query or
count with `pin` returns a snapshot token, and every scroll cursor carries one;
reads through a token see exactly the state it pins.

```toml
[snapshots]
token_ttl_ms = 300000            # 5 minutes, the default
max_tokens_per_collection = 64   # the default
```

- `token_ttl_ms` is how long a token lives after its last use; every read
  through it extends it. Reads through an expired token, and scroll pages after
  their cursor's token expired, fail with `FAILED_PRECONDITION` and reason
  `SNAPSHOT_EXPIRED`. A scroll's last page releases the pin the scroll made, so
  only unfinished scrolls hold pins.
- `max_tokens_per_collection` caps the pinned states per collection, since each
  keeps the files of its state on disk. A pin past it fails with
  `RESOURCE_EXHAUSTED` and reason `TOO_MANY_SNAPSHOTS`.

Both values must be greater than 0. Either key may be omitted to keep its
default.

## Vector Index

The optional `[index]` table sets the HNSW graph parameters that the background
index build uses when it builds a segment's vector graph, for every collection
on the node:

```toml
[index]
hnsw_m = 16                  # links per node on upper layers, 2x on layer 0; the default
hnsw_ef_construction = 128   # build-time beam width; the default
```

- `hnsw_m` must be 2 to 1024. Larger values raise recall at a given search
  `ef` and cost memory and build time.
- `hnsw_ef_construction` must be 1 to 4096. Larger values build a better graph
  more slowly.

A change applies to graphs built after it; existing segments keep their graphs
until compaction rewrites them. Flush and compaction write a segment without a
graph; a background index build adds it once the segment has 20,000 vectors, or
at any size once the collection took no write for 10 seconds (an explicit
compaction builds every missing graph before it answers). Until then the segment
is searched over its SQ8 codes. The search-time beam width is per query (`ef` on
the query request).

## Storage Engine

The storage engine's settings are built in; `LOGPOSE_CONFIG` sets only `storage_root`, the `[snapshots]` table, and the `[index]` graph parameters above. The defaults the server runs with:

| Setting | Default |
| --- | --- |
| Engine memory budget (memtables, primary-key indexes, maintenance, and the buffer cache of segment sections) | 4 GiB |
| Memtable reservation, engine-wide | an eighth of the budget |
| Maintenance memory for flush, compaction, and index builds | a fifth of the budget |
| Memtable flush triggers | 64 MiB, 1,000,000 rows, or 10 minutes |
| Frozen memtables before writes wait | 2; a write that waits 10 seconds fails with `WRITE_STALLED` |
| WAL file size before rotation | 64 MiB |
| Group commit | up to 256 requests or 16 MiB per group, no added delay |
| Compaction tiers | 32,768 live rows times powers of four; merges of 4 to 10 segments, at most 2,000,000 rows or 8 GiB, and small enough for the output's graph build to fit the maintenance memory; after 10 seconds without writes, segments below 20,000 rows merge together |
| Index sections | SQ8 codes from 1,024 rows and the scalar indexes each typed field declares, written with the segment; an HNSW graph (built with the `[index]` parameters) in an index sidecar that a background index build writes from 20,000 vectors, or from any number once the collection is quiet |
| Threads | 8 I/O threads, one query thread per core, a graph-building maintenance thread per two cores, 2 writer threads |

Each collection's descriptor also carries its own flush and compaction thresholds, set when it is created: `flush_threshold_ops` (10,000 operations), `flush_threshold_bytes` (64 MiB, capped by the engine's memtable size), and `compaction_threshold_segments` (4 segments of one tier).

`node_name` must not be `local`. That name is reserved for the placement of collections created without an explicit assignment.
