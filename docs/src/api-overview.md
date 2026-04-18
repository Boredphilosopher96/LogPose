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

The public API surface does not currently enforce authentication or RBAC.
All endpoints are open once the server is reachable. Browser-ready auth
and role-based access control are tracked as future work. The runtime now
persists collections inside a default database catalog entry, but database-
scoped policy enforcement is not implemented yet.

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
collection placements, and maintenance backlog.

```bash
curl http://127.0.0.1:8080/v1/runtime/status
```

**Response** (`200`):

```json
{
  "metadata": { "product": "logpose", "node_name": "node-alpha", "version": "0.1.0", "git_sha": "abc1234", "profile": "release" },
  "role": "combined",
  "rest_endpoint": "127.0.0.1:8080",
  "grpc_endpoint": "127.0.0.1:50051",
  "storage_engine": "local",
  "control_plane_ready": true,
  "data_plane_ready": true,
  "collection_count": 2,
  "collections": [
    {
      "collection_id": "550e8400-e29b-41d4-a716-446655440000",
      "collection_name": "embeddings",
      "assigned_node": "node-alpha",
      "assigned_role": "combined",
      "route_kind": "local",
      "route_reason": "single-node placement"
    }
  ],
  "maintenance": {
    "collections_with_pending": 0,
    "pending_operations": 0,
    "collections_in_progress": 0,
    "collections_with_errors": 0
  }
}
```

gRPC equivalent:

```protobuf
rpc GetRuntimeStatus(GetRuntimeStatusRequest) returns (GetRuntimeStatusReply);
```

### Create Collection

Creates a new vector collection. Control-plane lifecycle changes are only
accepted on nodes with the `combined` role.

```bash
curl -X POST http://127.0.0.1:8080/v1/collections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "embeddings",
    "dimensions": 768,
    "metric": "cosine"
  }'
```

**Request body**:

| Field        | Type     | Required | Description                            |
|--------------|----------|----------|----------------------------------------|
| `name`       | string   | yes      | Unique collection name                 |
| `dimensions` | integer  | yes      | Vector dimensionality (>= 1)           |
| `metric`     | string   | yes      | Distance metric: `cosine`, `dot`, `l2` |

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

### Get Collection

Retrieves metadata for an existing collection by name.

```bash
curl http://127.0.0.1:8080/v1/collections/embeddings
```

| Status | Meaning              |
|--------|----------------------|
| `200`  | Collection descriptor|
| `404`  | Collection not found |

### Get Collection Placement

Returns placement routing information for a collection.

```bash
curl http://127.0.0.1:8080/v1/collections/embeddings/placement
```

**Response** (`200`):

```json
{
  "collection_id": "550e8400-e29b-41d4-a716-446655440000",
  "collection_name": "embeddings",
  "assigned_node": "node-alpha",
  "assigned_role": "combined",
  "route_kind": "local",
  "route_reason": "single-node placement"
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
  "last_seq_no": 1023,
  "applied_ops": 3
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

| Field       | Type     | Required | Description                                              |
|-------------|----------|----------|----------------------------------------------------------|
| `vector`    | float[]  | yes      | Query vector                                             |
| `top_k`     | integer  | yes      | Maximum results to return (>= 1)                         |
| `snapshot`  | object   | no       | Pin query to a specific snapshot                         |
| `filters`   | object   | no       | Legacy AND-only equality filters over scalar metadata    |
| `predicate` | object   | no       | Structured predicate tree (see below)                    |
| `explain`   | string   | no       | `"none"`, `"plan"`, or `"profile"`                       |

**Response** (`200`):

```json
{
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

```bash
curl http://127.0.0.1:8080/v1/collections/embeddings/stats
```

**Response** (`200`):

```json
{
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

| Status | Meaning                                    |
|--------|--------------------------------------------|
| `200`  | Collection stats returned                  |
| `400`  | Collection not locally servable            |
| `404`  | Collection not found                       |

gRPC equivalent:

```protobuf
rpc GetCollectionStats(GetCollectionStatsRequest) returns (CollectionStatsReply);
```

### Flush Collection

Flushes the mutable delta into a new immutable segment.

```bash
curl -X POST http://127.0.0.1:8080/v1/collections/embeddings/flush
```

**Response** (`200`):

```json
{
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
curl -X POST http://127.0.0.1:8080/v1/collections/embeddings/compact
```

**Response** (`200`):

```json
{
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

- multi-node control-plane coordination or consistency-mode selection
- collection listing or delete/drop lifecycle endpoints
- record-browse or scroll-style inspection endpoints
- browser-ready authentication or RBAC enforcement
- remote blob-storage configuration for collection creation
