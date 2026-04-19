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

## Common Response Schemas

Every error from the REST surface returns the same envelope:

```json
{
  "error": "human-readable description of the failure"
}
```

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
| `dimensions`    | integer | yes      | Vector dimensionality (>= 1)                             |
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
| `400`  | Invalid request or wrong node role            |
| `409`  | Collection already exists                     |

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
| `400`  | Reconciliation or namespace error  |
| `404`  | Collection not found               |

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

| Status | Meaning                                     |
|--------|---------------------------------------------|
| `200`  | Write committed                             |
| `400`  | Invalid request or collection not servable  |
| `404`  | Collection not found                        |

gRPC equivalent:

```protobuf
rpc WriteCollection(WriteCollectionRequest) returns (CommitAckReply);
```

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
| `400`  | Invalid request or collection not servable |
| `404`  | Collection not found                     |
| `412`  | Read barrier not yet visible on this node, or rejected after ownership promotion until freshness metadata exists |
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
| `400`  | Invalid request or collection not locally servable |
| `404`  | Collection not found                     |
| `412`  | Read barrier not yet visible on this node, or rejected after ownership promotion until freshness metadata exists |
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
  rpc CreateCollection(CreateCollectionRequest) returns (CollectionDescriptorReply);
  rpc GetCollection(GetCollectionRequest) returns (CollectionDescriptorReply);
  rpc GetCollectionPlacement(GetCollectionPlacementRequest) returns (CollectionPlacementReply);
  rpc WriteCollection(WriteCollectionRequest) returns (CommitAckReply);
  rpc QueryCollection(QueryCollectionRequest) returns (QueryCollectionReply);
  rpc GetCollectionStats(GetCollectionStatsRequest) returns (CollectionStatsReply);
  rpc FlushCollection(FlushCollectionRequest) returns (SnapshotReply);
  rpc CompactCollection(CompactCollectionRequest) returns (SnapshotReply);
  rpc InspectCollection(InspectCollectionRequest) returns (InspectCollectionReply);
}
```

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

## Current Limits

The public APIs do not yet provide:

- multiple named consistency levels beyond exact snapshots and lower-bound read barriers, including read-barrier continuity across ownership promotion
- multi-node data-plane failover orchestration and chaos-tested recovery workflows
- collection listing or delete/drop lifecycle endpoints
- record-browse or scroll-style inspection endpoints
- browser-ready authentication or RBAC enforcement
- remote blob-storage configuration for collection creation
