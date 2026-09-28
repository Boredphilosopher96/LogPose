# API Overview

LogPose exposes two integration surfaces that share the same core application
layer:

| Transport | Default Endpoint        | Use Case                                     |
|-----------|-------------------------|----------------------------------------------|
| REST      | `http://127.0.0.1:8080` | HTTP-based control-plane and data-plane ops  |
| gRPC      | `127.0.0.1:50051`       | Strongly typed, high-performance integrations|

Both transports cover the same workflows and stay aligned with the shared
application layer, even when a given transport exposes slightly different
ergonomics.

---

<!-- toc -->

---

## Authentication

LogPose now supports bootstrap bearer-token authentication and database-scoped
authorization.

- Configure accepted bootstrap tokens under `auth.bootstrap_tokens` in `LOGPOSE_CONFIG`.
- Send `Authorization: Bearer <token>` on REST requests.
- Send `authorization: Bearer <token>` gRPC metadata on gRPC requests.
- `/health` remains open. Operator-only endpoints such as runtime status and
  database management require an operator-tier principal.
- Database-scoped policies gate collection reads, writes, flushes, compactions,
  and policy changes.

Example bootstrap config:

```toml
[[auth.bootstrap_tokens]]
token = "operator-secret"

[auth.bootstrap_tokens.principal]
name = "ops-admin"
kind = "user"
access_tier = "operator"
```

## Base URL and Versioning

All REST endpoints are prefixed with `/v1`. The gRPC service is defined
under the `logpose.v1` package.

```text
REST  :  http://127.0.0.1:8080/v1/...
gRPC  :  logpose.v1.LogPoseService
```

## Errors

Errors are typed from storage to the wire. Every error has a canonical code,
a stable machine-readable `reason`, and structured details, and both
transports report the same values. Nothing is classified by message text.

REST returns the code's HTTP status and this body:

```json
{
  "code": "UNAVAILABLE",
  "message": "collection 'default/embeddings' is not locally served by node 'node-a'; it is served by node 'node-b'",
  "details": {
    "reason": "NOT_OWNER",
    "metadata": {
      "collection": "default/embeddings",
      "node": "node-a",
      "owner_node": "node-b"
    },
    "field_violations": [],
    "retry_after_ms": 1000
  }
}
```

- `code` is one of the codes below, named like the gRPC status code.
- `details.reason` is stable and more specific than `code`.
- `details.metadata` holds the error's structured fields as strings.
- `details.field_violations` names the offending request fields, such as
  `operations[2].vector`, for `INVALID_ARGUMENT` errors that know them.
- `details.retry_after_ms` is present when retrying is expected to help. REST
  also sends it as a `Retry-After` header in whole seconds, rounded up.

gRPC returns the same code as the status code and the message as the status
message. The `grpc-status-details-bin` trailer holds a `google.rpc.Status`
with a `google.rpc.ErrorInfo` (the `reason`, domain `logpose`, and the
`metadata`), a `google.rpc.BadRequest` with the field violations, and a
`google.rpc.RetryInfo` with the retry hint. The retry hint is also sent as the
ASCII trailer `retry-after-ms` for clients that do not decode rich details.

| Code                  | HTTP  | gRPC                  | Meaning                                                          |
|-----------------------|-------|-----------------------|------------------------------------------------------------------|
| `INVALID_ARGUMENT`    | `400` | `INVALID_ARGUMENT`    | The request is malformed or fails validation                     |
| `NOT_FOUND`           | `404` | `NOT_FOUND`           | A named resource, or a route (path and method), does not exist   |
| `ALREADY_EXISTS`      | `409` | `ALREADY_EXISTS`      | A resource the request creates already exists                    |
| `FAILED_PRECONDITION` | `409` | `FAILED_PRECONDITION` | The node or collection is not in the state the request needs     |
| `UNAUTHENTICATED`     | `401` | `UNAUTHENTICATED`     | Missing or invalid bearer token                                  |
| `PERMISSION_DENIED`   | `403` | `PERMISSION_DENIED`   | The principal may not do this                                    |
| `RESOURCE_EXHAUSTED`  | `413` | `RESOURCE_EXHAUSTED`  | The request exceeds a size limit (`429` for other exhaustion)    |
| `UNAVAILABLE`         | `503` | `UNAVAILABLE`         | This node cannot serve the request now; retry, maybe elsewhere   |
| `DATA_LOSS`           | `500` | `DATA_LOSS`           | Stored data is corrupt                                           |
| `INTERNAL`            | `500` | `INTERNAL`            | An unexpected server failure                                     |

<!-- markdownlint-disable MD060 -->
| Reason                       | Code                  | Metadata                                                        | Retry hint |
|------------------------------|-----------------------|-----------------------------------------------------------------|------------|
| `INVALID_ARGUMENT`           | `INVALID_ARGUMENT`    | field violation when the field is known                         | no         |
| `DIMENSION_MISMATCH`         | `INVALID_ARGUMENT`    | `expected_dimensions`, `actual_dimensions`, `record_id`         | no         |
| `INVALID_CONFIG`             | `INVALID_ARGUMENT`    |                                                                 | no         |
| `TOO_LARGE`                  | `RESOURCE_EXHAUSTED`  | `what`, `limit_bytes`, `size_bytes` when known                  | no         |
| `RESOURCE_NOT_FOUND`         | `NOT_FOUND`           | `resource_type`, `resource_name`                                | no         |
| `RESOURCE_ALREADY_EXISTS`    | `ALREADY_EXISTS`      | `resource_type`, `resource_name`                                | no         |
| `FAILED_PRECONDITION`        | `FAILED_PRECONDITION` |                                                                 | no         |
| `WRONG_NODE_ROLE`            | `FAILED_PRECONDITION` | `node`, `node_role`                                             | no         |
| `RECONCILIATION_REQUIRED`    | `FAILED_PRECONDITION` | `collection`                                                    | no         |
| `STORAGE_ROOT_LOCKED`        | `FAILED_PRECONDITION` | `storage_root`, `holder_pid`                                    | no         |
| `UNAUTHENTICATED`            | `UNAUTHENTICATED`     |                                                                 | no         |
| `PERMISSION_DENIED`          | `PERMISSION_DENIED`   |                                                                 | no         |
| `NOT_OWNER`                  | `UNAVAILABLE`         | `collection`, `node`, `owner_node` when known                   | 1 s        |
| `NOT_LEADER`                 | `UNAVAILABLE`         | `node`, `leader_node` when known                                | 1 s        |
| `READ_BARRIER_NOT_SATISFIED` | `FAILED_PRECONDITION` | `collection`, `required_manifest_generation`, `required_seq_no`, `visible_manifest_generation`, `visible_seq_no` | no |
| `UNAVAILABLE`                | `UNAVAILABLE`         |                                                                 | sometimes  |
| `COLLECTION_POISONED`        | `FAILED_PRECONDITION` | `collection`                                                    | no         |
| `WAL_WRITE_FAILED`           | `UNAVAILABLE` (`not_applied`) or `INTERNAL` (`unknown_*`) | `collection`, `outcome` (`not_applied`, `unknown_fenced`, `unknown_unfenced`) | no |
| `DATA_CORRUPTION`            | `DATA_LOSS`           | `corruption_kind` (`wal`, `segment`, `manifest`, `index`, `descriptor`, `metadata`), `location` | no |
| `IO_ERROR`                   | `INTERNAL`            | `io_error_kind`                                                 | no         |
| `INTERNAL`                   | `INTERNAL`            |                                                                 | no         |
<!-- markdownlint-enable MD060 -->

`NOT_OWNER` and `NOT_LEADER` mean the request reached the wrong node: send it
to `owner_node` or `leader_node` when the error names one, or retry after the
hint. `COLLECTION_POISONED` means a storage failure made the collection
read-only until an operator reopens the engine; reads keep working. Do not
retry it automatically. `WAL_WRITE_FAILED` is the error of the writes whose WAL
group could not be made durable, which also poisons the collection: with
`outcome` `not_applied` the write is definitely absent (`UNAVAILABLE`); with
`unknown_fenced` or `unknown_unfenced` it may still appear after recovery, so
treat it like a timeout (`INTERNAL`).

### Rust Client Errors, Retries, and Redirects

The `logpose-client` crate decodes every error status into
`ClientError::Server(ServerError)`. A `ServerError` carries the `ErrorReason`,
the `ErrorCode`, the message, the metadata, the field violations, and the retry
hint (from `RetryInfo`, or the `retry-after-ms` trailer), and keeps the raw
`tonic::Status`. A status the client cannot classify (a reason from a newer
server, another `ErrorInfo` domain, or no `ErrorInfo`, as from a proxy or the
transport) decodes to the generic `ServerErrorKind::Unknown` with its gRPC
code, raw reason, and message.

By default the client sends each request once and returns the typed error. Two
opt-in policies change that:

- `RetryPolicy` retries on the same node only errors the server marks
  retryable: `UNAVAILABLE` or `RESOURCE_EXHAUSTED` with a retry hint. It never
  retries `INVALID_ARGUMENT`, `FAILED_PRECONDITION` (including
  `COLLECTION_POISONED` and `READ_BARRIER_NOT_SATISFIED`), or any error without
  a hint. It waits the larger of the hint and its own exponential backoff, and
  returns the error instead when the hint is longer than `max_backoff`. Reads
  are retried; writes only with `retry_writes`. Upserts and deletes by id and
  the database and policy puts are idempotent; a repeated collection create can
  report `RESOURCE_ALREADY_EXISTS`. Bulk streams (`BulkWriteCollection`) are
  never retried automatically; resume from `failed_batch_index`.
- `RedirectPolicy` follows `NOT_OWNER` to `owner_node` and `NOT_LEADER` to
  `leader_node` immediately, through a `NodeResolver` that maps node ids to
  gRPC endpoints, up to `max_redirects` times per request (2 by default). Both
  errors refuse the request before applying it, so redirects apply to writes
  too. Without a resolver, or when it does not know the node, the typed error is
  returned (or retried after its hint by a `RetryPolicy`). Each request starts
  at the configured endpoint again.

The CLI prints a typed error with its reason and message, followed by its code,
field violations, metadata, and where or when to retry:

```text
[error] failed to write records; the failing batch may have partially committed, so verify collection state before retrying
  [cause] DIMENSION_MISMATCH: record 'alpha' expected 2 dimensions but found 3
    code: INVALID_ARGUMENT
    field operations[0].vector: record 'alpha' expected 2 dimensions but found 3
    metadata: actual_dimensions=3, expected_dimensions=2, record_id=alpha
```

## Request Size Limits

| Setting                         | Default | Over the limit                                                |
| `limits.max_rest_body_bytes`    | 16 MiB  | HTTP `413`, `RESOURCE_EXHAUSTED`, reason `TOO_LARGE`          |
| `limits.max_grpc_message_bytes` | 16 MiB  | gRPC `RESOURCE_EXHAUSTED`, reason `TOO_LARGE`                 |
| `limits.max_grpc_message_bytes` | 16 MiB  | gRPC `RESOURCE_EXHAUSTED`, reason `TOO_LARGE`                  |

The gRPC limit applies to each request message, so each batch of a
`BulkWriteCollection` stream is checked on its own. See
[Configuration](configuration.md).

## Common Response Schemas

Snapshot references are used across writes, queries, flushes, and compactions:

```json
{
  "manifest_generation": 4,
  "visible_seq_no": 1023
}
```

Collection-scoped write/query/flush/compact/inspect responses flatten
`database_name` and `collection_name` into the top-level JSON
payload so operators can tell which namespace produced the response without
reconstructing it from the request path.

## Endpoints

### Health Check

A lightweight liveness probe with no request body.

```bash
curl http://127.0.0.1:8080/health
```

| Detail   | Value            |
|----------|------------------|
| Method   | `GET`            |
| Path     | `/health`        |
| Success  | `200 OK`         |

**Response** (`200`):

```json
{
  "status": "ok"
}
```

### Node Metadata

Returns normalized build and identity information for the running node.

```bash
curl http://127.0.0.1:8080/v1/metadata
```

**Response** (`200`):

```json
{
  "product": "logpose",
  "node_name": "node-alpha",
  "version": "0.1.0",
  "git_sha": "abc1234",
  "profile": "release"
}
```

gRPC equivalent:

```protobuf
rpc GetMetadata(GetMetadataRequest) returns (GetMetadataReply);
```

### Runtime Status

Returns a control-plane summary including node role, listener endpoints,
collection placements, maintenance backlog, and cluster coordination status
when etcd-backed metadata is enabled.

```bash
curl http://127.0.0.1:8080/v1/runtime/status \
  -H "Authorization: Bearer operator-secret"
```

**Response** (`200`):

```json
{
  "metadata": { "product": "LogPose", "node_name": "node-alpha", "version": "0.1.0", "git_sha": "abc1234", "profile": "release" },
  "role": "combined",
  "rest_endpoint": "http://127.0.0.1:8080",
  "grpc_endpoint": "http://127.0.0.1:50051",
  "storage_engine": "local+etcd-metadata",
  "control_plane_ready": true,
  "data_plane_ready": true,
  "collection_count": 1,
  "collections": [
    {
      "collection_id": "550e8400-e29b-41d4-a716-446655440000",
      "database_name": "default",
      "collection_name": "embeddings",
      "assigned_node": "node-alpha",
      "assigned_role": "data",
      "owner_node": "node-alpha",
      "ownership_epoch": 1,
      "route_kind": "local",
      "route_reason": "ownership epoch 1 is active on this runtime"
    }
  ],
  "maintenance": {
    "collections_with_pending": 0,
    "pending_operations": 0,
    "collections_in_progress": 0,
    "collections_with_errors": 0
  },
  "coordination": {
    "cluster_name": "prod-cluster",
    "membership_registered": true,
    "membership_lease_id": 17,
    "registered_members": ["node-alpha", "node-beta"],
    "is_local_leader": true,
    "leadership_lease_id": 23,
    "leader_node": "node-alpha",
    "last_error": null
  }
}
```

gRPC equivalent:

```protobuf
rpc GetRuntimeStatus(GetRuntimeStatusRequest) returns (GetRuntimeStatusReply);
```

### Database Management

Operator principals can provision database descriptors explicitly instead of
relying on collection or policy side effects. Operator UX is database-scoped:
collections live under `database/collection` namespaces, and the default
database is used when no database is selected.

```bash
curl -X PUT http://127.0.0.1:8080/v1/databases/analytics \
  -H "Authorization: Bearer operator-secret" \
  -H "Content-Type: application/json" \
  -d '{"database_id":"550e8400-e29b-41d4-a716-446655440001","name":"analytics","is_default":false}'
```

### Create Collection

Creates a new vector collection. Control-plane lifecycle changes are only
accepted on nodes with the `combined` role. Request and response bodies are
namespace-aware: omit `database_name` to use the bootstrap `default`
database, or set it explicitly for non-default scopes.

```bash
curl -X POST http://127.0.0.1:8080/v1/collections \
  -H "Authorization: Bearer operator-secret" \
  -H "Content-Type: application/json" \
  -d '{
    "database_name": "analytics",
    "name": "embeddings",
    "dimensions": 768,
    "metric": "cosine"
  }'
```

**Request body**:

| Field           | Type    | Required | Description                                              |
|-----------------|---------|----------|----------------------------------------------------------|
| `database_name` | string  | no       | Target database; defaults to `default` when omitted      |
| `name`          | string  | yes      | Unique collection name inside the selected database      |
| `dimensions`    | integer | yes      | Vector dimensionality, from 1 to 65,536                  |
| `metric`        | string  | yes      | Distance metric: `cosine`, `dot`, `l2`                   |

**Response** (`201`):

```json
{
  "database_name": "default",
  "collection_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "embeddings",
  "dimensions": 768,
  "metric": "cosine",
  "root_path": "/data/collections/embeddings",
  "remote_blob": null,
  "flush_threshold_ops": 10000,
  "flush_threshold_bytes": 67108864,
  "compaction_threshold_segments": 4
}
```

| Status | Meaning                                       |
|--------|-----------------------------------------------|
| `201`  | Collection created                            |
| `400`  | Invalid request                               |
| `409`  | Collection already exists, or wrong node role |
| `413`  | Request body too large                        |

gRPC equivalent:

```protobuf
rpc CreateCollection(CreateCollectionRequest) returns (CollectionDescriptorReply);
```

Collection routes target the `default` database unless you select another one.
For non-default collections:

- use `?database=analytics` on read-style routes such as `GET /v1/collections/{name}`,
  `.../placement`, `.../stats`, `.../flush`, `.../compact`, and `.../inspect`
- include `"database_name": "analytics"` in write/query request bodies
- include `Authorization: Bearer <token>` on collection control-plane requests, and on
  data-plane requests whenever auth is enabled

### Get Collection

Retrieves metadata for an existing collection by name. Use the `database`
query parameter for non-default namespaces.

```bash
curl http://127.0.0.1:8080/v1/collections/embeddings
```

| Status | Meaning                            |
|--------|------------------------------------|
| `200`  | Collection descriptor              |
| `400`  | Invalid namespace                  |
| `404`  | Collection not found               |
| `409`  | Reconciliation required            |

### Get Collection Placement

Returns placement routing information for a collection. Use the `database`
query parameter for non-default namespaces. When etcd-backed ownership fencing
is active, the reply also surfaces the current owner node and ownership epoch.

```bash
curl http://127.0.0.1:8080/v1/collections/embeddings/placement
```

**Response** (`200`):

```json
{
  "collection_id": "550e8400-e29b-41d4-a716-446655440000",
  "database_name": "default",
  "collection_name": "embeddings",
  "assigned_node": "node-alpha",
  "assigned_role": "data",
  "owner_node": "node-alpha",
  "ownership_epoch": 1,
  "route_kind": "local",
  "route_reason": "ownership epoch 1 is active on this runtime"
}
```

gRPC equivalent:

```protobuf
rpc GetCollectionPlacement(GetCollectionPlacementRequest) returns (CollectionPlacementReply);
```

### Write Batch

Submits a mixed batch of `put` and `delete` operations to a collection.
Data-plane calls are rejected when the runtime cannot serve the collection
locally.
For non-default namespaces, include `database_name` in the request body.

```bash
curl -X POST http://127.0.0.1:8080/v1/collections/embeddings/writes \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [
      {
        "op": "put",
        "id": "doc-001",
        "vector": [0.12, 0.45, 0.78],
        "metadata": { "source": "arxiv", "year": 2025 }
      },
      {
        "op": "put",
        "id": "doc-002",
        "vector": [0.33, 0.66, 0.99],
        "metadata": { "source": "wiki", "year": 2024 }
      },
      {
        "op": "delete",
        "id": "doc-old"
      }
    ]
  }'
```

**Response** (`200`):

```json
{
  "database_name": "default",
  "collection_name": "embeddings",
  "last_seq_no": 1023,
  "applied_ops": 3,
  "snapshot": {
    "manifest_generation": 0,
    "visible_seq_no": 1023
  }
}
```

Every operation is validated against the collection's schema before
anything is written, and one invalid operation rejects the whole batch:

- `id` must be a non-empty string of at most 1,024 bytes.
- `vector` must have exactly `dimensions` components, all finite.
- `metadata` must be a JSON object or `null`. Its keys are stored as dynamic
  fields, so they cannot be named `id` or `vector`, the collection's
  declared key and vector fields, or `$extra`, which is reserved.

| Status | Meaning                                                                                       |
|--------|-----------------------------------------------------------------------------------------------|
| `200`  | Write committed                                                                               |
| `400`  | Invalid request; see `field_violations`                                                       |
| `404`  | Collection not found                                                                          |
| `409`  | Wrong node role, or collection read-only until the engine is reopened (`COLLECTION_POISONED`) |
| `413`  | Request body too large                                                                        |
| `503`  | Not the owner (`NOT_OWNER`)                                                                   |

gRPC equivalent:

```protobuf
rpc WriteCollection(WriteCollectionRequest) returns (CommitAckReply);
```

### Bulk Write (gRPC only)

`BulkWriteCollection` is a client-streaming RPC for bulk ingest. REST has no
equivalent; REST clients send batches to `POST /v1/collections/{name}/writes`.

```protobuf
rpc BulkWriteCollection(stream BulkWriteCollectionRequest) returns (BulkWriteCollectionReply);
```

- The first message names the collection (`collection_name`, and
  `database_name` for non-default databases). Later messages may leave both
  empty, or must repeat the same values.
- Each message is one batch, validated and committed atomically exactly like
  one `WriteCollection` call.
- Batches commit in stream order, one at a time. The server reads the next
  message only after the previous batch commits, so HTTP/2 flow control
  pushes back on a client that sends faster than the collection commits.
- Each message must fit `limits.max_grpc_message_bytes`.
- When every batch commits, the reply reports `committed_batches`,
  `applied_ops`, the `last_seq_no` of the last batch, and a `snapshot` that
  includes every batch.
- The first failing batch fails the RPC with that batch's error code and
  reason, and the server stops reading. Every earlier batch is committed; the
  failed batch and everything after it are not. The error's `ErrorInfo`
  metadata reports `failed_batch_index` (zero-based), `committed_batches`,
  `committed_operations`, and `last_committed_seq_no` when a batch was
  committed, so a client can resume from `failed_batch_index`.
- If the client cancels or disconnects, a batch that is being committed still
  commits or fails as a whole, and once the server sees the cancellation it
  starts no further batch, even one the client sent before cancelling. Resume
  after checking what is visible.
- An empty stream is `INVALID_ARGUMENT`.

### Query Collection

Executes a planner-controlled vector query with optional metadata filtering
and explain diagnostics.
For non-default namespaces, include `database_name` in the request body.

```bash
curl -X POST http://127.0.0.1:8080/v1/collections/embeddings/query \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.12, 0.45, 0.78],
    "top_k": 5,
    "explain": "profile"
  }'
```

**Request body**:

<!-- markdownlint-disable MD060 -->
| Field           | Type    | Required | Description                                                                    |
|-----------------|---------|----------|--------------------------------------------------------------------------------|
| `database_name` | string  | no       | Database namespace; defaults to `default`                                      |
| `vector`        | float[] | yes      | Query vector                                                                   |
| `top_k`         | integer | yes      | Maximum results to return (>= 1)                                               |
| `snapshot`      | object  | no       | Pin query to a specific snapshot                                               |
| `read_barrier`  | object  | no       | Require a lower-bound previously observed snapshot on the current owner; cannot be combined with `snapshot` |
| `filters`       | object  | no       | Legacy AND-only equality filters over scalar metadata                          |
| `predicate`     | object  | no       | Structured predicate tree (see below)                                          |
| `explain`       | string  | no       | `"none"`, `"plan"`, or `"profile"`                                             |
<!-- markdownlint-enable MD060 -->

**Response** (`200`):

```json
{
  "database_name": "default",
  "collection_name": "embeddings",
  "metric": "cosine",
  "top_k": 5,
  "returned": 2,
  "snapshot": { "manifest_generation": 4, "visible_seq_no": 1023 },
  "matches": [
    { "id": "doc-001", "value": 0.98, "metadata": { "source": "arxiv", "year": 2025 } },
    { "id": "doc-002", "value": 0.87, "metadata": { "source": "wiki", "year": 2024 } }
  ],
  "diagnostics": {
    "chosen_plan": "hybrid_exact_ann_merge",
    "planner_reason": "mutable delta present alongside HNSW sidecar",
    "estimated_selectivity": 1.0,
    "units_considered": 2,
    "units_pruned": 0,
    "units_scanned": 2,
    "candidates_before_filter": 50,
    "candidates_after_filter": 50,
    "candidates_reranked": 5,
    "candidates_merged": 2,
    "rerank_count": 5,
    "fallback_reason": null,
    "unit_scan_mix": { "mutable": 1, "immutable": 1 },
    "stage_timings": {
      "planning_micros": 12,
      "prefilter_micros": 0,
      "candidate_generation_micros": 340,
      "postfilter_micros": 0,
      "rerank_micros": 45,
      "merge_micros": 8
    }
  }
}
```

<!-- markdownlint-disable MD060 -->
| Status | Meaning                                  |
|--------|------------------------------------------|
| `200`  | Query returned                           |
| `400`  | Invalid request                          |
| `404`  | Collection not found                     |
| `409`  | Wrong node role, read barrier not visible (`READ_BARRIER_NOT_SATISFIED`), or read barriers rejected after ownership promotion until freshness metadata exists |
| `413`  | Request body too large                   |
| `503`  | Not the owner (`NOT_OWNER`)              |
<!-- markdownlint-enable MD060 -->

gRPC equivalent:

```protobuf
rpc QueryCollection(QueryCollectionRequest) returns (QueryCollectionReply);
```

#### Structured Predicates

The `predicate` field accepts a tree of boolean and comparison nodes for
rich metadata filtering beyond the legacy equality-only `filters` map.

**Comparison predicate**:

```json
{
  "kind": "comparison",
  "field": "year",
  "operator": "gte",
  "value": 2024
}
```

Available operators: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `exists`, `is_null`.

**Boolean combinators** (`and`, `or`, `not`):

```json
{
  "kind": "and",
  "children": [
    { "kind": "comparison", "field": "source", "operator": "eq", "value": "arxiv" },
    { "kind": "not", "child": { "kind": "comparison", "field": "year", "operator": "lt", "value": 2023 } }
  ]
}
```

#### Query Plan Kinds

The planner selects an execution strategy based on collection state and
filter selectivity:

| Plan Kind                          | Description                                             |
|------------------------------------|---------------------------------------------------------|
| `unfiltered_exact_scan`            | Full exact scan, no filters applied                     |
| `predicate_first_exact`            | Filter first, then exact distance on survivors          |
| `vector_first_exact`               | Exact scan first, then post-filter                      |
| `tiny_population_exact_fallback`   | Population too small for ANN, falls back to exact       |
| `vector_first_ann`                 | ANN index scan, then post-filter                        |
| `cooperative_filtered_ann`         | Cooperative ANN with inline predicate evaluation        |
| `hybrid_exact_ann_merge`           | Merge exact (mutable) and ANN (immutable) results       |

### Collection Stats

Returns storage statistics, maintenance state, and per-query-unit breakdowns.
Use the `database` query parameter for non-default namespaces.
Use `snapshot_manifest_generation` and `snapshot_visible_seq_no` together to inspect
stats at one exact historical snapshot. Use
`read_barrier_manifest_generation` and `read_barrier_visible_seq_no`
together to require the current serving node to expose stats from a snapshot at
or beyond one previously observed write or read boundary. Exact snapshots and
read barriers are mutually exclusive. After ownership promotion, promoted
owners fail read barriers closed until replica freshness metadata exists.

```bash
curl http://127.0.0.1:8080/v1/collections/embeddings/stats
```

**Response** (`200`):

```json
{
  "database_name": "default",
  "collection_id": "550e8400-e29b-41d4-a716-446655440000",
  "collection_name": "embeddings",
  "manifest_generation": 4,
  "visible_seq_no": 1023,
  "mutable_op_count": 150,
  "segment_count": 3,
  "live_record_count": 9500,
  "deleted_record_count": 500,
  "maintenance": {
    "pending": [],
    "in_progress": null,
    "last_error": null,
    "completed_runs": 12
  },
  "query_units": [
    {
      "unit_id": "mutable",
      "tier": "mutable",
      "index_kind": "flat",
      "min_seq_no": 1001,
      "max_seq_no": 1023,
      "put_count": 150,
      "delete_count": 10,
      "approx_bytes": 245760,
      "scalar_fields": {},
      "artifact_stats": [],
      "component_bytes": { "vectors": 184320, "metadata": 61440 }
    }
  ]
}
```

<!-- markdownlint-disable MD060 -->
| Status | Meaning                                  |
|--------|------------------------------------------|
| `200`  | Collection stats returned                |
| `400`  | Invalid request                          |
| `404`  | Collection not found                     |
| `409`  | Wrong node role, read barrier not visible (`READ_BARRIER_NOT_SATISFIED`), or read barriers rejected after ownership promotion until freshness metadata exists |
| `503`  | Not the owner (`NOT_OWNER`)              |
<!-- markdownlint-enable MD060 -->

gRPC equivalent:

```protobuf
rpc GetCollectionStats(GetCollectionStatsRequest) returns (CollectionStatsReply);
```

### Flush Collection

Flushes the mutable delta into a new immutable segment.
Use the `database` query parameter for non-default namespaces.

```bash
curl -X POST http://127.0.0.1:8080/v1/collections/embeddings/flush
```

**Response** (`200`):

```json
{
  "database_name": "default",
  "collection_name": "embeddings",
  "manifest_generation": 5,
  "visible_seq_no": 1023
}
```

gRPC equivalent:

```protobuf
rpc FlushCollection(FlushCollectionRequest) returns (SnapshotReply);
```

### Compact Collection

Merges immutable segments to reduce segment count and reclaim space from
tombstoned deletes.
Use the `database` query parameter for non-default namespaces.

```bash
curl -X POST http://127.0.0.1:8080/v1/collections/embeddings/compact
```

**Response** (`200`):

```json
{
  "database_name": "default",
  "collection_name": "embeddings",
  "manifest_generation": 6,
  "visible_seq_no": 1023
}
```

gRPC equivalent:

```protobuf
rpc CompactCollection(CompactCollectionRequest) returns (SnapshotReply);
```

### Inspect Collection

Returns low-level storage inspection reports for debugging and diagnostics.
Use the `database` query parameter for non-default namespaces.

```bash
# Inspect manifest
curl "http://127.0.0.1:8080/v1/collections/embeddings/inspect?target=manifest"

# Inspect WAL
curl "http://127.0.0.1:8080/v1/collections/embeddings/inspect?target=wal"

# Inspect a specific segment
curl "http://127.0.0.1:8080/v1/collections/embeddings/inspect?target=segment&segment_id=seg-001"

# Inspect maintenance state
curl "http://127.0.0.1:8080/v1/collections/embeddings/inspect?target=maintenance"
```

| Parameter    | Type   | Required | Values                                       |
|--------------|--------|----------|----------------------------------------------|
| `target`     | string | no       | `manifest`, `wal`, `segment`, `maintenance`  |
| `segment_id` | string | no       | Required when `target=segment`               |

**Response** (`200`):

```json
{
  "database_name": "default",
  "collection_name": "embeddings",
  "target": "manifest",
  "payload": { "...": "target-specific JSON" }
}
```

gRPC equivalent:

```protobuf
rpc InspectCollection(InspectCollectionRequest) returns (InspectCollectionReply);
```

## gRPC Service Definition

The full gRPC contract is defined in `proto/logpose/v1/logpose.proto`:

```protobuf
service LogPoseService {
  rpc GetMetadata(GetMetadataRequest) returns (GetMetadataReply);
  rpc GetRuntimeStatus(GetRuntimeStatusRequest) returns (GetRuntimeStatusReply);
  rpc PutDatabase(PutDatabaseRequest) returns (DatabaseDescriptorReply);
  rpc GetDatabase(GetDatabaseRequest) returns (DatabaseDescriptorReply);
  rpc ListDatabases(ListDatabasesRequest) returns (ListDatabasesReply);
  rpc CreateCollection(CreateCollectionRequest) returns (CollectionDescriptorReply);
  rpc GetCollection(GetCollectionRequest) returns (CollectionDescriptorReply);
  rpc GetCollectionPlacement(GetCollectionPlacementRequest) returns (CollectionPlacementReply);
  rpc WriteCollection(WriteCollectionRequest) returns (CommitAckReply);
  rpc BulkWriteCollection(stream BulkWriteCollectionRequest) returns (BulkWriteCollectionReply);
  rpc QueryCollection(QueryCollectionRequest) returns (QueryCollectionReply);
  rpc GetCollectionStats(GetCollectionStatsRequest) returns (CollectionStatsReply);
  rpc FlushCollection(FlushCollectionRequest) returns (SnapshotReply);
  rpc CompactCollection(CompactCollectionRequest) returns (SnapshotReply);
  rpc InspectCollection(InspectCollectionRequest) returns (InspectCollectionReply);
  rpc PutDatabasePolicy(PutDatabasePolicyRequest) returns (DatabaseAccessPolicyReply);
  rpc GetDatabasePolicy(GetDatabasePolicyRequest) returns (DatabaseAccessPolicyReply);
}
```

Every RPC has a REST operation whose `operationId` is the RPC name in
lower camel case, except `BulkWriteCollection` (gRPC only). `GET /health` is
REST only; gRPC serves the standard `grpc.health.v1.Health` service. The
`api_contract` test in `crates/logpose-api-rest/tests` enforces this, checks
that the router serves exactly the documented routes and methods, and
validates the OpenAPI document.

## Distance Metrics

| Metric | Description                          | REST value | Proto enum                 |
|--------|--------------------------------------|------------|----------------------------|
| Cosine | Cosine similarity (1 - cosine dist)  | `cosine`   | `DISTANCE_METRIC_COSINE`   |
| Dot    | Dot-product similarity               | `dot`      | `DISTANCE_METRIC_DOT`      |
| L2     | Euclidean (L2) distance              | `l2`       | `DISTANCE_METRIC_L2`       |

## Contract Sources

| Surface | File                                |
|---------|-------------------------------------|
| REST    | `openapi/logpose.v1.yaml`           |
| gRPC    | `proto/logpose/v1/logpose.proto`    |
| Errors  | `crates/logpose-types/src/error.rs` |

## Current Limits

The public APIs do not yet provide:

- multiple named consistency levels beyond exact snapshots and lower-bound read barriers, including read-barrier continuity across ownership promotion
- multi-node data-plane failover orchestration and chaos-tested recovery workflows
- collection listing or delete/drop lifecycle endpoints
- record-browse or scroll-style inspection endpoints
- browser-ready authentication or RBAC enforcement
- remote blob-storage configuration for collection creation
