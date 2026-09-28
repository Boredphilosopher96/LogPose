# API Overview

LogPose exposes two integration surfaces that share the same core application
layer:

| Transport | Default Endpoint        | Use Case                                      |
|-----------|-------------------------|-----------------------------------------------|
| REST      | `http://127.0.0.1:8080` | HTTP-based control-plane and data-plane ops   |
| gRPC      | `127.0.0.1:50051`       | Strongly typed, high-performance integrations |

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

All REST endpoints except `/health` are prefixed with `/v2`. The gRPC service
is defined under the `logpose.v2` package. Version 2 replaced version 1
outright; the `/v1` routes and the `logpose.v1` package no longer exist.

```text
REST  :  http://127.0.0.1:8080/v2/...
gRPC  :  logpose.v2.LogPoseService
```

Every database-scoped REST route names its database in the path, and every
collection-scoped route names its collection below it:

```text
/v2/databases/{database}
/v2/databases/{database}/policy
/v2/databases/{database}/collections
/v2/databases/{database}/collections/{collection}
/v2/databases/{database}/collections/{collection}/records/{upsert,update,delete,get,count,scroll}
/v2/databases/{database}/collections/{collection}/{placement,query,stats,flush,compact,inspect}
```

There is no implicit database: use `default` for the default database, and
percent-encode names that need it. gRPC requests carry the same selection in
`database_name` and `collection_name`, and both are required on every
collection-scoped request.

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
  `records[2].price`, `keys[0]`, or `vectors[0].dimensions`, for
  `INVALID_ARGUMENT` errors that know them.
- `details.retry_after_ms` is present when retrying is expected to help. REST
  also sends it as a `Retry-After` header in whole seconds, rounded up.

gRPC returns the same code as the status code and the message as the status
message. The `grpc-status-details-bin` trailer holds a `google.rpc.Status`
with a `google.rpc.ErrorInfo` (the `reason`, domain `logpose`, and the
`metadata`), a `google.rpc.BadRequest` with the field violations, and a
`google.rpc.RetryInfo` with the retry hint. The retry hint is also sent as the
ASCII trailer `retry-after-ms` for clients that do not decode rich details.

| Code                  | HTTP  | gRPC                  | Meaning                                                        |
|-----------------------|-------|-----------------------|----------------------------------------------------------------|
| `INVALID_ARGUMENT`    | `400` | `INVALID_ARGUMENT`    | The request is malformed or fails validation                   |
| `NOT_FOUND`           | `404` | `NOT_FOUND`           | A named resource, or a route (path and method), does not exist |
| `ALREADY_EXISTS`      | `409` | `ALREADY_EXISTS`      | A resource the request creates already exists                  |
| `FAILED_PRECONDITION` | `409` | `FAILED_PRECONDITION` | The node or collection is not in the state the request needs   |
| `UNAUTHENTICATED`     | `401` | `UNAUTHENTICATED`     | Missing or invalid bearer token                                |
| `PERMISSION_DENIED`   | `403` | `PERMISSION_DENIED`   | The principal may not do this                                  |
| `RESOURCE_EXHAUSTED`  | `413` | `RESOURCE_EXHAUSTED`  | The request exceeds a size limit (`429` for other exhaustion)  |
| `UNAVAILABLE`         | `503` | `UNAVAILABLE`         | This node cannot serve the request now; retry, maybe elsewhere |
| `DATA_LOSS`           | `500` | `DATA_LOSS`           | Stored data is corrupt                                         |
| `INTERNAL`            | `500` | `INTERNAL`            | An unexpected server failure                                   |

<!-- markdownlint-disable MD060 -->
| Reason                       | Code                                                      | Metadata                                                                                                           | Retry hint |
|------------------------------|-----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|------------|
| `INVALID_ARGUMENT`           | `INVALID_ARGUMENT`                                        | field violation when the field is known                                                                            | no         |
| `DIMENSION_MISMATCH`         | `INVALID_ARGUMENT`                                        | `expected_dimensions`, `actual_dimensions`, `record_id`                                                            | no         |
| `INVALID_CONFIG`             | `INVALID_ARGUMENT`                                        |                                                                                                                    | no         |
| `TOO_LARGE`                  | `RESOURCE_EXHAUSTED`                                      | `what`, `limit_bytes`, `size_bytes` when known                                                                     | no         |
| `RESOURCE_NOT_FOUND`         | `NOT_FOUND`                                               | `resource_type`, `resource_name`                                                                                   | no         |
| `RESOURCE_ALREADY_EXISTS`    | `ALREADY_EXISTS`                                          | `resource_type`, `resource_name`                                                                                   | no         |
| `FAILED_PRECONDITION`        | `FAILED_PRECONDITION`                                     |                                                                                                                    | no         |
| `WRONG_NODE_ROLE`            | `FAILED_PRECONDITION`                                     | `node`, `node_role`                                                                                                | no         |
| `RECONCILIATION_REQUIRED`    | `FAILED_PRECONDITION`                                     | `collection`                                                                                                       | no         |
| `STORAGE_ROOT_LOCKED`        | `FAILED_PRECONDITION`                                     | `storage_root`, `holder_pid`                                                                                       | no         |
| `SNAPSHOT_EXPIRED`           | `FAILED_PRECONDITION`                                     | `collection`                                                                                                       | no         |
| `TOO_MANY_SNAPSHOTS`         | `RESOURCE_EXHAUSTED`                                      | `collection`                                                                                                       | no         |
| `WRITE_STALLED`              | `UNAVAILABLE`                                             | `collection`                                                                                                       | 1 s        |
| `UNAUTHENTICATED`            | `UNAUTHENTICATED`                                         |                                                                                                                    | no         |
| `PERMISSION_DENIED`          | `PERMISSION_DENIED`                                       |                                                                                                                    | no         |
| `NOT_OWNER`                  | `UNAVAILABLE`                                             | `collection`, `node`, `owner_node` when known                                                                      | 1 s        |
| `NOT_LEADER`                 | `UNAVAILABLE`                                             | `node`, `leader_node` when known                                                                                   | 1 s        |
| `READ_BARRIER_NOT_SATISFIED` | `FAILED_PRECONDITION`                                     | `collection`, `required_manifest_generation`, `required_seq_no`, `visible_manifest_generation`, `visible_seq_no`   | no         |
| `UNAVAILABLE`                | `UNAVAILABLE`                                             |                                                                                                                    | sometimes  |
| `COLLECTION_POISONED`        | `FAILED_PRECONDITION`                                     | `collection`                                                                                                       | no         |
| `WAL_WRITE_FAILED`           | `UNAVAILABLE` (`not_applied`) or `INTERNAL` (`unknown_*`) | `collection`, `outcome` (`not_applied`, `unknown_fenced`, `unknown_unfenced`)                                      | no         |
| `DATA_CORRUPTION`            | `DATA_LOSS`                                               | `corruption_kind` (`wal`, `segment`, `deletion_vector`, `manifest`, `index`, `descriptor`, `metadata`), `location` | no         |
| `IO_ERROR`                   | `INTERNAL`                                                | `io_error_kind`                                                                                                    | no         |
| `INTERNAL`                   | `INTERNAL`                                                |                                                                                                                    | no         |
<!-- markdownlint-enable MD060 -->

`NOT_OWNER` and `NOT_LEADER` mean the request reached the wrong node: send it
to `owner_node` or `leader_node` when the error names one, or retry after the
hint. `COLLECTION_POISONED` means a storage failure made the collection
read-only until an operator reopens the engine; reads keep working. Do not
retry it automatically. Flushes that keep failing (five in a row by default,
or one on a full or read-only device) poison the collection too, and the
collection's stats then show the failure in `maintenance.last_error`.
`WAL_WRITE_FAILED` is the error of the writes whose WAL
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
  are retried; writes only with `retry_writes`. A lost reply carries no hint
  and is never retried, but an etcd failure during a database, policy, or
  collection create is hinted and may have committed. Upserts and deletes by
  key and the database and policy puts are idempotent; a repeated collection
  create can report `RESOURCE_ALREADY_EXISTS`. Bulk streams (`BulkUpsertRecords`)
  are never retried automatically; resume from `failed_batch_index`.
- `RedirectPolicy` follows `NOT_OWNER` to `owner_node` and `NOT_LEADER` to
  `leader_node` immediately, through a `NodeResolver` that maps node ids to
  gRPC endpoints, up to `max_redirects` times per request (2 by default). Both
  errors refuse the request before applying it, so redirects apply to writes
  too. Without a resolver, or when it does not know the node, the typed error is
  returned (or retried after its hint by a `RetryPolicy`). Each request starts
  at the configured endpoint again. The server names only a node id; the
  endpoint, and so where the client sends its bearer token, comes from the
  resolver, so it should map only known node ids.

The CLI prints a typed error with its reason and message, followed by its code,
field violations, metadata, and where or when to retry:

```text
[error] failed to write records; each batch commits atomically, so the failing batch was applied in full or not at all; verify collection state before retrying it
  [cause] INVALID_ARGUMENT: write batch includes primary key "alpha" more than once
    code: INVALID_ARGUMENT
    field records[1]: write batch includes primary key "alpha" more than once
```

`logpose record put` reads the collection schema first and checks each JSONL
line against it, so a record that does not fit the schema (a missing field, a
value of the wrong type, a vector of the wrong length) fails before anything
is sent, naming the line.

## Request Size Limits

| Setting                         | Default | Over the limit                                       |
|---------------------------------|---------|------------------------------------------------------|
| `limits.max_rest_body_bytes`    | 16 MiB  | HTTP `413`, `RESOURCE_EXHAUSTED`, reason `TOO_LARGE` |
| `limits.max_grpc_message_bytes` | 16 MiB  | gRPC `RESOURCE_EXHAUSTED`, reason `TOO_LARGE`        |

The gRPC limit applies to each request message, so each batch of a
`BulkUpsertRecords` stream is checked on its own. See
[Configuration](configuration.md).

## Common Response Schemas

Snapshot references are used across writes, queries, flushes, and compactions:

```json
{
  "manifest_generation": 4,
  "visible_seq_no": 1023
}
```

An exact snapshot is readable only while it is the current state, one of the
latest states of the current manifest generation, or a state a snapshot token
pins. Every flush and compaction publishes a new generation; after that, a read
of an unpinned snapshot from an older generation fails with
`FAILED_PRECONDITION` (reason `SNAPSHOT_EXPIRED`, HTTP 409).

For repeatable reads, pin the state: a query or count with `"pin": true`
returns a `snapshot_token` (an opaque 48-character string), and later queries,
counts, and first scroll pages that pass it as `snapshot_token` read exactly
that state, whatever was written, flushed, or compacted since. Each use extends
the token's expiry (`snapshots.token_ttl_ms`, five minutes by default); an
expired, released, or unknown token fails with `SNAPSHOT_EXPIRED`. Tokens do
not survive a restart. Pinning more snapshots than a collection allows
(`snapshots.max_tokens_per_collection`, 64 by default), or more retired memory
than the engine allows, fails with `RESOURCE_EXHAUSTED` (reason
`TOO_MANY_SNAPSHOTS`, HTTP 429). A scroll cursor carries a token of its own.

A read that names no snapshot reads one consistent state from its first stage
to its last: a flush or compaction that publishes while it runs never fails it.

Collection-scoped record, query, flush, compact, and inspect responses flatten
`database_name` and `collection_name` into the top-level JSON
payload so operators can tell which namespace produced the response without
reconstructing it from the request path.

## Endpoints

### Health Check

A lightweight liveness probe with no request body.

```bash
curl http://127.0.0.1:8080/health
```

| Detail  | Value     |
|---------|-----------|
| Method  | `GET`     |
| Path    | `/health` |
| Success | `200 OK`  |

**Response** (`200`):

```json
{
  "status": "ok"
}
```

### Node Metadata

Returns normalized build and identity information for the running node.

```bash
curl http://127.0.0.1:8080/v2/metadata
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
curl http://127.0.0.1:8080/v2/runtime/status \
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

### Databases

Operator principals manage databases. A database holds collections; the
`default` database always exists and is created lazily.

| Operation       | REST                              | gRPC            |
|-----------------|-----------------------------------|-----------------|
| List databases  | `GET /v2/databases`               | `ListDatabases` |
| Get a database  | `GET /v2/databases/{database}`    | `GetDatabase`   |
| Create database | `PUT /v2/databases/{database}`    | `PutDatabase`   |
| Drop a database | `DELETE /v2/databases/{database}` | `DropDatabase`  |

```bash
curl -X PUT http://127.0.0.1:8080/v2/databases/analytics \
  -H "Authorization: Bearer operator-secret"
```

`PUT` takes no body and is idempotent. `GET /v2/databases` returns
`{"databases": [...]}`. `DELETE` drops an empty database and returns
`{"database_name": "analytics"}`; it fails with `FAILED_PRECONDITION` for the
`default` database and for a database that still has collections, so drop
those first. Dropping a database also removes its access policy.

The access policy of a database lives at `/v2/databases/{database}/policy`
(`GET` and `PUT`; gRPC `GetDatabasePolicy` and `PutDatabasePolicy`). The
`PUT` body holds `authentication_mode` and `role_bindings` (each a
`principal_name` and a `role`); the path names the database.

### Collection Schemas

A collection has a typed schema (engine plan decision D4): one primary key,
one or more vector fields, typed scalar fields, and the `$extra` dynamic
field that keeps undeclared keys when dynamic fields are on.

| Part          | Rules                                                                                                            |
|---------------|------------------------------------------------------------------------------------------------------------------|
| Names         | `[A-Za-z_][A-Za-z0-9_]{0,63}`, unique across the collection; `$extra` is reserved                                |
| Primary key   | `string` (1 to 1,024 bytes) or `int64`                                                                           |
| Vector fields | `dimensions` from 1 to 65,536; `metric` `cosine` (the default), `dot`, or `l2`                                   |
| Scalar fields | a type from the table below, `nullable` (default `true`), and an `index`                                         |
| Dynamic       | `dynamic_fields` (default `true`): keep undeclared keys in `$extra`; when `false`, an undeclared key is rejected |

Scalar field types, and how a value looks in REST JSON and in the gRPC
`Value` message:

| Type        | REST JSON                                                                                   | gRPC `Value`       |
|-------------|---------------------------------------------------------------------------------------------|--------------------|
| `bool`      | `true`, `false`                                                                             | `bool_value`       |
| `int64`     | an integer                                                                                  | `int64_value`      |
| `float64`   | a finite number                                                                             | `float64_value`    |
| `string`    | a string                                                                                    | `string_value`     |
| `timestamp` | RFC 3339 string, or integer microseconds since the Unix epoch; reads return RFC 3339 in UTC | `timestamp_micros` |
| `json`      | any JSON value                                                                              | `json_value`       |
| `array<T>`  | a flat array of `T` (`bool`, `int64`, `float64`, `string`, `timestamp`), no nulls           | `array_value`      |
| null        | `null`                                                                                      | `null_value`       |

Nothing is coerced: a string never becomes a number. An integer is accepted
for a `float64` field when it converts exactly. The
`index` is `auto` (the default), `none`, `inverted`, `sorted`, or
`inverted_and_sorted`; `auto` resolves to `inverted` for `bool`, `string`,
and arrays, `inverted_and_sorted` for numbers and timestamps, and `none` for
`json`, so a stored schema never holds `auto`.

Every field has a stable id that is never reused. A dropped or renamed-away
name becomes *retired*: `$extra` keys under a retired name stay hidden until a
field with that name is declared again, and a declared field shadows a dynamic
key of the same name.

### Create Collection

```bash
curl -X POST http://127.0.0.1:8080/v2/databases/analytics/collections \
  -H "Authorization: Bearer operator-secret" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products",
    "primary_key": { "name": "sku", "type": "int64" },
    "vectors": [{ "name": "embedding", "dimensions": 768, "metric": "cosine" }],
    "fields": [
      { "name": "title", "type": "string", "nullable": false },
      { "name": "price", "type": "float64" },
      { "name": "tags", "type": "array<string>" },
      { "name": "released", "type": "timestamp", "index": "sorted" }
    ],
    "dynamic_fields": true
  }'
```

Unknown keys are rejected. Creating a collection creates its database when
needed. Lifecycle changes are accepted only on nodes with the `combined` role. The reply (`201`) is the collection with its live
schema:

```json
{
  "collection_id": "550e8400-e29b-41d4-a716-446655440000",
  "database_name": "analytics",
  "name": "products",
  "schema": {
    "schema_version": 1,
    "next_field_id": 7,
    "primary_key": { "id": 1, "name": "sku", "type": "int64" },
    "vectors": [{ "id": 2, "name": "embedding", "dimensions": 768, "metric": "cosine" }],
    "fields": [
      { "id": 3, "name": "title", "type": "string", "index": "inverted", "nullable": false },
      { "id": 4, "name": "price", "type": "float64", "index": "inverted_and_sorted", "nullable": true },
      { "id": 5, "name": "tags", "type": "array<string>", "index": "inverted", "nullable": true },
      { "id": 6, "name": "released", "type": "timestamp", "index": "sorted", "nullable": true }
    ],
    "dynamic_fields": true,
    "retired_names": []
  },
  "root_path": "/data/collections/550e8400-e29b-41d4-a716-446655440000",
  "remote_blob": null,
  "flush_threshold_ops": 10000,
  "flush_threshold_bytes": 67108864,
  "compaction_threshold_segments": 4
}
```

An invalid schema fails with `INVALID_ARGUMENT` and a field violation naming
the part of the request, such as `vectors[0].dimensions`, `fields[1].name`, or
`fields[2].index`.

| Status | Meaning                                       |
|--------|-----------------------------------------------|
| `201`  | Collection created                            |
| `400`  | Invalid schema; see `field_violations`        |
| `409`  | Collection already exists, or wrong node role |
| `413`  | Request body too large                        |

gRPC equivalent:

```protobuf
rpc CreateCollection(CreateCollectionRequest) returns (CollectionReply);
```

### List, Get, and Drop Collections

| Operation         | REST                                                       | gRPC                     |
|-------------------|------------------------------------------------------------|--------------------------|
| List collections  | `GET /v2/databases/{database}/collections`                 | `ListCollections`        |
| Get a collection  | `GET /v2/databases/{database}/collections/{collection}`    | `GetCollection`          |
| Drop a collection | `DELETE /v2/databases/{database}/collections/{collection}` | `DropCollection`         |
| Placement         | `GET .../collections/{collection}/placement`               | `GetCollectionPlacement` |

List returns `{"collections": [...]}` and get returns one collection, each
with its live schema. Drop removes the collection's metadata and data and
returns `{"database_name": ..., "collection_name": ...}`; it needs write access
to the database and a `combined` node.

Placement returns routing information. When etcd-backed ownership fencing is
active, the reply also names the current owner node and ownership epoch:

```json
{
  "collection_id": "550e8400-e29b-41d4-a716-446655440000",
  "database_name": "analytics",
  "collection_name": "products",
  "assigned_node": "node-alpha",
  "assigned_role": "data",
  "owner_node": "node-alpha",
  "ownership_epoch": 1,
  "route_kind": "local",
  "route_reason": "ownership epoch 1 is active on this runtime"
}
```

### Alter Collection

`PATCH /v2/databases/{database}/collections/{collection}` (gRPC
`AlterCollection`) applies one online schema change. The body is an object
with exactly one key:

```bash
curl -X PATCH http://127.0.0.1:8080/v2/databases/analytics/collections/products \
  -H "Content-Type: application/json" \
  -d '{ "add_field": { "name": "color", "type": "string" } }'
```

| Change                                              | Effect                                                                                                 |
|-----------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `{"add_field": {<scalar field>}}`                   | Add a scalar field; it must be nullable. Records written before read null                              |
| `{"drop_field": {"name": "price"}}`                 | Drop a scalar or vector field (not the primary key or the last vector field); the name becomes retired |
| `{"rename_field": {"from": "title", "to": "name"}}` | Rename any field, including the primary key; the old name becomes retired                              |

A change is ordered with the writes around it: a write committed before it is
validated against the old schema, and every later write against the new one.
Each change increments `schema_version`, survives a restart, and is replayed
with the schema each record was written with. The reply is the collection with
its new schema. An invalid change fails with a field violation such as
`add_field.nullable`, `drop_field.name`, or `rename_field.to`.

### Records

Records are addressed by primary key. In REST a record is a natural JSON
document keyed by field name, typed by the schema:

```json
{
  "sku": 7,
  "embedding": [0.12, 0.45, 0.78],
  "title": "desk lamp",
  "price": 24.5,
  "tags": ["lighting", "office"],
  "released": "2025-03-01T12:00:00Z",
  "color": "red"
}
```

The primary key, each vector field, and each scalar field appear under their
names. With dynamic fields on, any other key (here `color`) is kept in `$extra`
as JSON and returned at the top level on reads. In gRPC a `Record` holds `pk`,
`vectors` (a map of `Vector`), `fields` (a map of typed `Value`), and `extra`
(a `JsonObject`).

<!-- markdownlint-disable MD060 -->
| Operation            | REST (`POST .../collections/{collection}` +) | gRPC            | Body                                      |
|----------------------|----------------------------------------------|-----------------|-------------------------------------------|
| Insert or replace    | `/records/upsert`                            | `UpsertRecords` | `{"records": [...]}`                      |
| Change some fields   | `/records/update`                            | `UpdateRecords` | `{"records": [...]}` (partial)            |
| Change by filter     | `/records/update`                            | `UpdateRecords` | `{"filter": {...}, "patch": {...}}`       |
| Delete by key        | `/records/delete`                            | `DeleteRecords` | `{"keys": [...]}`                         |
| Delete by filter     | `/records/delete`                            | `DeleteRecords` | `{"filter": {...}}`                       |
| Read by key          | `/records/get`                               | `GetRecords`    | `{"keys": [...], "output_fields": [...]}` |
| Count matches        | `/records/count`                             | `CountRecords`  | `{"filter": {...}}`                       |
| Page through matches | `/records/scroll`                            | `ScrollRecords` | `{"filter": {...}, "cursor": "..."}`      |
<!-- markdownlint-enable MD060 -->

Each write request is one batch that is validated in full and commits
atomically: one invalid record rejects the whole batch, and a key may appear
only once per batch. A write reply is the commit acknowledgement:

```json
{
  "database_name": "analytics",
  "collection_name": "products",
  "last_seq_no": 1023,
  "applied_ops": 2,
  "snapshot": { "manifest_generation": 0, "visible_seq_no": 1023 }
}
```

- **Upsert** writes whole records. Each needs its primary key, every vector
  field, and every non-nullable field; a missing nullable field is null.
- **Update** takes partial documents: the primary key and the fields to
  change. A value replaces the field, `null` clears a nullable field (or
  removes a dynamic key), and a vector replaces the vector. An update of a key
  without a live record fails the batch with `NOT_FOUND`
  (`RESOURCE_NOT_FOUND`); an update that changes nothing is invalid.
- **Delete** takes primary keys. A key without a live record is a no-op.
- **Get** returns the live records found, in request order, the requested keys
  without one in `missing_keys`, and the snapshot it read:

```bash
curl -X POST http://127.0.0.1:8080/v2/databases/analytics/collections/products/records/get \
  -H "Content-Type: application/json" \
  -d '{ "keys": [7, 8], "output_fields": ["title", "price"] }'
```

```json
{
  "database_name": "analytics",
  "collection_name": "products",
  "records": [{ "sku": 7, "title": "desk lamp", "price": 24.5 }],
  "missing_keys": [8],
  "snapshot": { "manifest_generation": 0, "visible_seq_no": 1023 }
}
```

`output_fields` projects the returned records: declared field names, `$extra`
for every visible dynamic key, or (with dynamic fields on) single dynamic keys.
The primary key is always returned, and an empty list returns every field.

Vectors of a `cosine` field are normalized to unit length when they are
written, so reads return the normalized vector (`[3, 4]` reads back as
`[0.6, 0.8]`) and scoring compares unit vectors. A vector must have exactly the
field's dimensions and finite components.

Validation errors name the field with its position in the request:
`records[2].price`, `records[0].embedding[3]`, `records[1]` for a repeated key,
or `keys[0]` for a key of the wrong type. A vector of the wrong length is
`DIMENSION_MISMATCH` with the record's key in `record_id`.

| Status | Meaning                                                                                       |
|--------|-----------------------------------------------------------------------------------------------|
| `200`  | Write committed, or records read                                                              |
| `400`  | Invalid request; see `field_violations`                                                       |
| `404`  | Collection not found, or (update) a key without a live record                                 |
| `409`  | Wrong node role, or collection read-only until the engine is reopened (`COLLECTION_POISONED`) |
| `413`  | Request body too large                                                                        |
| `503`  | Not the owner (`NOT_OWNER`), or writes stalled behind maintenance (`WRITE_STALLED`)           |

### Filters

Queries, counts, scrolls, and deletes and updates by filter take one filter
language (engine plan decision D11). In REST a filter is natural JSON: an
object with exactly one operator key.

```json
{ "and": [
  { "eq": { "tenant": "acme" } },
  { "range": { "price": { "gte": 10, "lt": 50 } } },
  { "contains": { "tags": "outdoor" } },
  { "not": { "exists": "$extra.archived" } }
] }
```

<!-- markdownlint-disable MD060 -->
| Operator       | Form                                  | Matches a record when                                                 |
|----------------|---------------------------------------|-----------------------------------------------------------------------|
| `and`, `or`    | `{"and": [filter, ...]}`              | every (some) child matches; at least one child                        |
| `not`          | `{"not": filter}`                     | the child does not match, records where its field is null included    |
| `eq`           | `{"eq": {field: value}}`              | the field equals the value (an array field: some element does)        |
| `ne`           | `{"ne": {field: value}}`              | the field has a value and it is not the value (no element is)         |
| `range`        | `{"range": {field: {"gte": 1, ...}}}` | the field (some element) lies within every bound: `gt`, `gte`, `lt`, `lte` |
| `in`           | `{"in": {field: [value, ...]}}`       | the field (some element) is one of the values                         |
| `not_in`       | `{"not_in": {field: [value, ...]}}`   | the field has a value and it (every element) is none of the values    |
| `contains`     | `{"contains": {field: value}}`        | an array field has an element equal to the value                      |
| `contains_any` | `{"contains_any": {field: [...]}}`    | an array field has an element equal to one of the values              |
| `exists`       | `{"exists": field}`                   | the field has a value; a dynamic key is present                       |
| `is_null`      | `{"is_null": field}`                  | the field has no value; a dynamic key is present and JSON `null`     |
<!-- markdownlint-enable MD060 -->

A field is named by a path, resolved with the schema of the state the request
reads:

- a declared name: the primary key or a scalar field (vector fields cannot be
  filtered);
- `$extra.<key>`: the dynamic key `<key>`, everything after the first dot;
- with dynamic fields on, any other name: the dynamic key of that name.

A dynamic key the schema declares or retired is hidden, so filtering it is an
error, as is an undeclared name when dynamic fields are off and a dropped or
renamed field's old name. Nested JSON paths are not supported: a dynamic key
or `json` field compares as a whole.

Values are typed like record values of the field they compare, and an array
field's like one element: a string for a `string` field, an RFC 3339 string or
integer microseconds for a `timestamp`, and so on. Numbers convert between
`int64` and `float64` when exact, and a fractional bound of an `int64` or
`timestamp` range rounds to the matching integer range (`{"lt": 3.5}` is
`{"lte": 3}`). A dynamic key or `json` field compares by JSON scalar equality,
and its range bounds compare two strings or two numbers. A value is never
`null`: use `exists` and `is_null`. `range` needs an ordered field (not
`bool`), and not both `gt` and `gte` (or `lt` and `lte`); `contains` and
`contains_any` need an array field.

A filter nests at most 32 levels (`{"not": {"eq": ...}}` is two) and has at
most 10,000 terms: its nodes plus the values of its `in`, `not_in`, and
`contains_any` lists. A filter past either limit is `INVALID_ARGUMENT`, at the
first node too deep or at `filter`.

In gRPC a filter is the `Filter` message, a `oneof` with the same operators:
`FilterList` for `and` and `or`, `FieldValue` (`field` and a typed `Value`) for
`eq`, `ne`, and `contains`, `FieldValues` for `in`, `not_in`, and
`contains_any`, `FieldRange` for `range`, and the field path string for
`exists` and `is_null`.

Filter errors are `INVALID_ARGUMENT` naming the offending node by its path in
the REST form, the same in both transports: `filter.and[1].range.price.gte`,
`filter.in.tags[2]`, `filter.not.exists`, or `filter.eq.colour` for a field
the schema does not have.

Declared fields are served by their scalar indexes (inverted for equality and
`exists`, sorted for ranges) and zone maps prune segments; dynamic keys and
`json` fields are scanned.

### Delete and Update by Filter

`POST .../records/delete` with `{"filter": {...}}` deletes every live record
the filter matches, and `POST .../records/update` with
`{"filter": {...}, "patch": {...}}` applies the patch to each of them. A patch
is a partial document without the primary key: a value replaces a field,
`null` clears it (or removes a dynamic key), and a vector replaces the vector.
Patch errors name `patch.<field>`.

The filter is resolved once, against the latest state (every earlier write
included), to a fixed set of keys that commits as one atomic batch in one WAL
frame: all of the matches change or none do, and the reply's `applied_ops` is
the number changed. A filter that matches nothing commits nothing and returns
`applied_ops: 0`. The keys (and, for an update, the rewritten records) must
fit one WAL frame of 64 MiB, a few million keys; a larger match fails with
`RESOURCE_EXHAUSTED` (reason `TOO_LARGE`, HTTP 413) and changes nothing.
Narrow the filter, for example by a range of keys, and repeat. The server does
not split a large match into several batches, because a failure between them
would leave it half applied.

```bash
curl -X POST http://127.0.0.1:8080/v2/databases/analytics/collections/products/records/delete \
  -H "Content-Type: application/json" \
  -d '{ "filter": { "and": [ { "eq": { "tenant": "acme" } }, { "range": { "price": { "lt": 1 } } } ] } }'
```

### Count Records

`POST .../records/count` (gRPC `CountRecords`) counts the live records
matching `filter`, or every live record without one. It takes the snapshot
fields of a query (`snapshot`, `read_barrier`, `snapshot_token`, `pin`).

```json
{ "filter": { "eq": { "tenant": "acme" } }, "pin": true }
```

```json
{
  "database_name": "analytics",
  "collection_name": "products",
  "count": 1284,
  "snapshot": { "manifest_generation": 3, "visible_seq_no": 5120 },
  "snapshot_token": "AbC..."
}
```

A count with `pin` returns the token of the state it counted; scrolling that
token returns exactly `count` records.

### Scroll Records

`POST .../records/scroll` (gRPC `ScrollRecords`) returns one page of the live
records matching `filter`, in `order_by` order:

| Field            | Type     | Description                                                             |
|------------------|----------|-------------------------------------------------------------------------|
| `filter`         | object   | Only matching records                                                   |
| `order_by`       | array    | At most one `{"field": "price", "direction": "desc"}`; key by default   |
| `page_size`      | integer  | Records per page, 1 to 10,000; 100 by default                           |
| `output_fields`  | string[] | Fields to return, as `records/get` projects them                        |
| `cursor`         | string   | Continue after the page that returned it                                |
| `snapshot_token` | string   | A first page reads the state this token pins                            |

```json
{
  "database_name": "analytics",
  "collection_name": "products",
  "records": [{ "sku": 7, "price": 24.5 }],
  "next_cursor": "AQ...",
  "snapshot": { "manifest_generation": 3, "visible_seq_no": 5120 }
}
```

A first page reads the current state, or the state `snapshot_token` pins. When
records are left after it, the server pins the state it read and returns a
`next_cursor` that carries the pin; send the same request with that cursor for
the next page. Every page reads exactly that state, so every record matching
the filter appears exactly once across the pages, whatever is written,
flushed, or compacted meanwhile, and `snapshot` is the same on every page. The
last page has `next_cursor: null`, and a scroll that fits in one page pins
nothing.

- A cursor is opaque. It must be sent with the same `filter` and `order_by` as
  the first page, and without `snapshot_token`; anything else is
  `INVALID_ARGUMENT` at `cursor` (or `snapshot_token`), as is a cursor that is
  not one.
- Its pin expires `snapshots.token_ttl_ms` (five minutes by default) after the
  last page. A page after that fails with `FAILED_PRECONDITION` (reason
  `SNAPSHOT_EXPIRED`, HTTP 409): start the scroll again.
- Ordering by a field puts records without a value last in both directions and
  breaks ties by primary key. `order_by` takes a declared scalar field that is
  not an array or JSON; primary key order (ascending) is what an empty
  `order_by` gives, and naming the primary key is `INVALID_ARGUMENT`.
- Each open scroll holds one of the collection's snapshot pins, so a client
  that abandons many scrolls can exhaust them until they expire
  (`TOO_MANY_SNAPSHOTS`).

### Bulk Upsert (gRPC only)

`BulkUpsertRecords` is a client-streaming RPC for bulk ingest. REST has no
equivalent; REST clients send batches to `.../records/upsert`.

```protobuf
rpc BulkUpsertRecords(stream BulkUpsertRecordsRequest) returns (BulkUpsertRecordsReply);
```

- The first message names the database and the collection. Later messages
  may leave both empty, or must repeat the same values.
- Each message is one batch of records, validated and committed atomically
  exactly like one `UpsertRecords` call.
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

One search request (engine plan decision D11) covers vector search, filters,
an order, a limit, and a projection. With a vector it returns the `top_k`
records nearest the query vector in the named vector field that match the
filter, best first; without one it returns the first `top_k` matching records
in `order_by` order, a filtered scan (use [Scroll Records](#scroll-records) to
page through all of them).

```bash
curl -X POST http://127.0.0.1:8080/v2/databases/analytics/collections/products/query \
  -H "Content-Type: application/json" \
  -d '{
    "vector": { "field": "embedding", "values": [0.12, 0.45, 0.78] },
    "filter": { "and": [
      { "eq": { "tenant": "acme" } },
      { "range": { "price": { "gte": 10, "lt": 50 } } }
    ] },
    "top_k": 5,
    "output_fields": ["sku", "price"],
    "explain": "profile"
  }'
```

**Request body**:

<!-- markdownlint-disable MD060 -->
| Field            | Type     | Required | Description                                                                                                        |
|------------------|----------|----------|--------------------------------------------------------------------------------------------------------------------|
| `vector`         | object   | no       | `{"field": ..., "values": [...]}`; `field` may be omitted when the collection has one vector field                 |
| `filter`         | object   | no       | A [filter](#filters)                                                                                               |
| `order_by`       | array    | no       | At most one `{"field": ..., "direction": "asc" or "desc"}`: reorders the vector hits, or orders the scan (key by default) |
| `top_k`          | integer  | yes      | Results to return, 1 to 10,000                                                                                     |
| `output_fields`  | string[] | no       | Fields each hit returns, as `records/get` projects them; empty returns every field, vectors included              |
| `ef`             | integer  | no       | Beam width of graph walks, 1 to 4,096 (default 64, and at least four candidates per result); vector search only    |
| `explain`        | string   | no       | `"none"`, `"plan"`, or `"profile"`                                                                                 |
| `snapshot`       | object   | no       | Read one exact snapshot; see snapshot retention above                                                              |
| `snapshot_token` | string   | no       | Read exactly the state this token pins; cannot be combined with `snapshot`                                        |
| `pin`            | boolean  | no       | Pin the state read and return its `snapshot_token`                                                                 |
| `read_barrier`   | object   | no       | Fail unless the state read is at or past this snapshot; cannot be combined with `snapshot`                         |
<!-- markdownlint-enable MD060 -->

Name only the primary key in `output_fields` to get keys and scores alone;
the default returns whole records. A collection with several vector fields
needs `vector.field`. Validation errors name the request field: `top_k`,
`vector.field`, `vector.values` (`DIMENSION_MISMATCH` for the wrong length),
`order_by[0].field`, `ef`, `output_fields[2]`, or the filter node.

**Response** (`200`):

```json
{
  "database_name": "analytics",
  "collection_name": "products",
  "vector_field": "embedding",
  "metric": "cosine",
  "top_k": 5,
  "returned": 2,
  "snapshot": { "manifest_generation": 4, "visible_seq_no": 1023 },
  "hits": [
    { "score": 0.98, "record": { "sku": 7, "price": 24.5 } },
    { "score": 0.87, "record": { "sku": 12, "price": 31.0 } }
  ],
  "diagnostics": {
    "chosen_plan": "hybrid_exact_ann_merge",
    "planner_reason": "unit 00000003 graph_admit: 48000 matching rows exceed the exact-scan limit 2048; selectivity 1.000; unit 00000007 memtable_scan: memtables are scanned exactly",
    "estimated_selectivity": 1.0,
    "units_considered": 2,
    "units_pruned": 0,
    "units_scanned": 2,
    "candidates_before_filter": 48050,
    "candidates_after_filter": 48050,
    "candidates_reranked": 20,
    "candidates_merged": 40,
    "rerank_count": 1,
    "unit_scan_mix": { "graph_admit": 1, "memtable_scan": 1 },
    "stage_timings": {
      "planning_micros": 90,
      "prefilter_micros": 0,
      "candidate_generation_micros": 340,
      "postfilter_micros": 35,
      "rerank_micros": 45,
      "merge_micros": 0
    }
  }
}
```

<!-- markdownlint-disable MD060 -->
| Status | Meaning                                                                                                                                                       |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `200`  | Query returned                                                                                                                                                |
| `400`  | Invalid request                                                                                                                                               |
| `404`  | Collection not found                                                                                                                                          |
| `409`  | Wrong node role, read barrier not visible (`READ_BARRIER_NOT_SATISFIED`), or read barriers rejected after ownership promotion until freshness metadata exists |
| `409`  | Expired snapshot or token (`SNAPSHOT_EXPIRED`)                                                                                                                |
| `413`  | Request body too large                                                                                                                                        |
| `429`  | Too many pinned snapshots (`TOO_MANY_SNAPSHOTS`)                                                                                                              |
| `503`  | Not the owner (`NOT_OWNER`)                                                                                                                                   |
<!-- markdownlint-enable MD060 -->

gRPC equivalent:

```protobuf
rpc QueryCollection(QueryCollectionRequest) returns (QueryCollectionReply);
```

The gRPC request holds the same fields: `VectorQuery vector`, `Filter filter`,
`repeated OrderBy order_by`, `top_k`, `output_fields`, `ef` (0 for the
default), and the snapshot fields. Each `QueryHit` holds the projected
`Record` and an optional `score`, and `metric` is unspecified for a scan.

#### Query Plan Kinds

The planner chooses a strategy per unit from the exact number of live rows
the filter matches there (`n`) and the unit's live rows (`N`). Memtables are
scanned exactly. A segment without a graph, or whose `n` is at most 2,048 (or
the candidate budget, four candidates per result), is scanned exactly over its
filter bitmap: over SQ8 codes when it has them, then reranked in f32. Otherwise
it walks its HNSW graph over SQ8 codes: ACORN-1 style when `n / N` is below
0.3, else a walk that admits only matching rows; a walk that comes back short,
or whose visited rows match the filter far less often than `n / N` predicts,
widens its beam and continues. Every unit's candidates are reranked in f32 and
merged into one top-k. `unit_scan_mix` counts units per strategy
(`memtable_scan`, `exact_sq8`, `exact_f32`, `graph_admit`, `graph_acorn`,
`pruned`, `empty`); `chosen_plan` summarizes them:

| Plan Kind                          | Description                                             |
|------------------------------------|---------------------------------------------------------|
| `unfiltered_exact_scan`            | Every unit scanned exactly, no filter                   |
| `predicate_first_exact`            | Every unit scanned exactly over its filter bitmap       |
| `vector_first_ann`                 | Segment graph walks, no filter, no memtable rows        |
| `cooperative_filtered_ann`         | Filtered segment graph walks (ACORN-1 or admit-only)    |
| `hybrid_exact_ann_merge`           | Segment graph walks merged with memtable scans          |
| `ordered_scan`                     | No vector: rows read in `order_by` order                |
| `vector_first_exact`               | No longer produced                                      |
| `tiny_population_exact_fallback`   | No longer produced                                      |

With a vector, `order_by` sorts the `top_k` hits the search found; it does not
search for the best records by that field. The diagnostics of a scan report
only `ordered_scan`, the units considered, and (with `profile`) the scan's
time.

### Collection Stats

Returns storage statistics, maintenance state, and per-query-unit breakdowns.
Use `snapshot_manifest_generation` and `snapshot_visible_seq_no` together to inspect
stats at one exact snapshot, retained as described above. Use
`read_barrier_manifest_generation` and `read_barrier_visible_seq_no`
together to require the current serving node to expose stats from a snapshot at
or beyond one previously observed write or read boundary. Exact snapshots and
read barriers are mutually exclusive. After ownership promotion, promoted
owners fail read barriers closed until replica freshness metadata exists.

```bash
curl http://127.0.0.1:8080/v2/databases/default/collections/embeddings/stats
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
| Status | Meaning                                                                                                                                                       |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `200`  | Collection stats returned                                                                                                                                     |
| `400`  | Invalid request                                                                                                                                               |
| `404`  | Collection not found                                                                                                                                          |
| `409`  | Wrong node role, read barrier not visible (`READ_BARRIER_NOT_SATISFIED`), or read barriers rejected after ownership promotion until freshness metadata exists |
| `503`  | Not the owner (`NOT_OWNER`)                                                                                                                                   |
<!-- markdownlint-enable MD060 -->

gRPC equivalent:

```protobuf
rpc GetCollectionStats(GetCollectionStatsRequest) returns (CollectionStatsReply);
```

### Flush Collection

Flushes the mutable delta into a new immutable segment.

```bash
curl -X POST http://127.0.0.1:8080/v2/databases/default/collections/embeddings/flush
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

```bash
curl -X POST http://127.0.0.1:8080/v2/databases/default/collections/embeddings/compact
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

```bash
# Inspect manifest
curl "http://127.0.0.1:8080/v2/databases/default/collections/embeddings/inspect?target=manifest"

# Inspect WAL
curl "http://127.0.0.1:8080/v2/databases/default/collections/embeddings/inspect?target=wal"

# Inspect a specific segment
curl "http://127.0.0.1:8080/v2/databases/default/collections/embeddings/inspect?target=segment&segment_id=seg-001"

# Inspect maintenance state
curl "http://127.0.0.1:8080/v2/databases/default/collections/embeddings/inspect?target=maintenance"
```

| Parameter    | Type   | Required | Values                                      |
|--------------|--------|----------|---------------------------------------------|
| `target`     | string | no       | `manifest`, `wal`, `segment`, `maintenance` |
| `segment_id` | string | no       | Required when `target=segment`              |

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

In gRPC the payload is a `JsonValue` document. Its shape depends on the
target and is an operator diagnostic, not a stable contract, so it has no
typed message.

## gRPC Service Definition

The full gRPC contract is defined in `proto/logpose/v2/logpose.proto`:

```protobuf
service LogPoseService {
  rpc GetMetadata(GetMetadataRequest) returns (GetMetadataReply);
  rpc GetRuntimeStatus(GetRuntimeStatusRequest) returns (GetRuntimeStatusReply);

  rpc PutDatabase(PutDatabaseRequest) returns (DatabaseDescriptorReply);
  rpc GetDatabase(GetDatabaseRequest) returns (DatabaseDescriptorReply);
  rpc ListDatabases(ListDatabasesRequest) returns (ListDatabasesReply);
  rpc DropDatabase(DropDatabaseRequest) returns (DropDatabaseReply);
  rpc PutDatabasePolicy(PutDatabasePolicyRequest) returns (DatabaseAccessPolicyReply);
  rpc GetDatabasePolicy(GetDatabasePolicyRequest) returns (DatabaseAccessPolicyReply);

  rpc CreateCollection(CreateCollectionRequest) returns (CollectionReply);
  rpc GetCollection(GetCollectionRequest) returns (CollectionReply);
  rpc ListCollections(ListCollectionsRequest) returns (ListCollectionsReply);
  rpc AlterCollection(AlterCollectionRequest) returns (CollectionReply);
  rpc DropCollection(DropCollectionRequest) returns (DropCollectionReply);
  rpc GetCollectionPlacement(GetCollectionPlacementRequest) returns (CollectionPlacementReply);

  rpc UpsertRecords(UpsertRecordsRequest) returns (CommitAckReply);
  rpc UpdateRecords(UpdateRecordsRequest) returns (CommitAckReply);
  rpc DeleteRecords(DeleteRecordsRequest) returns (CommitAckReply);
  rpc GetRecords(GetRecordsRequest) returns (GetRecordsReply);
  rpc CountRecords(CountRecordsRequest) returns (CountRecordsReply);
  rpc ScrollRecords(ScrollRecordsRequest) returns (ScrollRecordsReply);
  rpc BulkUpsertRecords(stream BulkUpsertRecordsRequest) returns (BulkUpsertRecordsReply);

  rpc QueryCollection(QueryCollectionRequest) returns (QueryCollectionReply);
  rpc GetCollectionStats(GetCollectionStatsRequest) returns (CollectionStatsReply);
  rpc FlushCollection(FlushCollectionRequest) returns (SnapshotReply);
  rpc CompactCollection(CompactCollectionRequest) returns (SnapshotReply);
  rpc InspectCollection(InspectCollectionRequest) returns (InspectCollectionReply);
}
```

Scalar values travel as the `Value` message: a `oneof` of `null_value`,
`bool_value`, `int64_value`, `float64_value`, `string_value`,
`timestamp_micros`, `array_value`, and `json_value`. JSON documents (`json`
fields and `$extra`, statistics bounds, and inspect payloads) travel as
`JsonValue`, `JsonArray`, and `JsonObject`, which keep 64-bit integers exact;
no message carries JSON text. Filters are the `Filter` message (see
[Filters](#filters)). The schema is the `CollectionSchema` message,
and a schema change is `AlterCollectionRequest` with one of `add_field`,
`drop_field`, or `rename_field`.

Every RPC has a REST operation whose `operationId` is the RPC name in
lower camel case, except `BulkUpsertRecords` (gRPC only). `GET /health` is
REST only; gRPC serves the standard `grpc.health.v1.Health` service. The
`api_contract` test in `crates/logpose-api-rest/tests` enforces this, checks
that the router serves exactly the documented routes and methods, and
validates the OpenAPI document.

## Distance Metrics

| Metric | Description                         | REST value | Proto enum               |
|--------|-------------------------------------|------------|--------------------------|
| Cosine | Cosine similarity (1 - cosine dist) | `cosine`   | `DISTANCE_METRIC_COSINE` |
| Dot    | Dot-product similarity              | `dot`      | `DISTANCE_METRIC_DOT`    |
| L2     | Euclidean (L2) distance             | `l2`       | `DISTANCE_METRIC_L2`     |

`cosine` vectors are normalized to unit length when written, so stored and
returned vectors of a cosine field have length 1.

## Contract Sources

| Surface | File                                |
|---------|-------------------------------------|
| REST    | `openapi/logpose.v2.yaml`           |
| gRPC    | `proto/logpose/v2/logpose.proto`    |
| Errors  | `crates/logpose-types/src/error.rs` |

## Current Limits

The public APIs do not yet provide:

- multiple named consistency levels beyond exact snapshots and lower-bound read barriers, including read-barrier continuity across ownership promotion
- multi-node data-plane failover orchestration and chaos-tested recovery workflows
- deletes and updates by filter larger than one WAL frame (64 MiB of keys and records); such a request fails with `TOO_LARGE`
- nested JSON paths, prefix matching, or full-text matching in filters
- searches over several vector fields at once, or paging through vector search results
- browser-ready authentication or RBAC enforcement
- remote blob-storage configuration for collection creation
