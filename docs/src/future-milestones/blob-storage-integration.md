# Blob Storage Integration

## Goal

Turn LogPose's current remote-blob metadata stubs into real MinIO and S3-backed immutable artifact storage, with explicit durability semantics, background sync, recovery, and operator visibility.

## Current State

LogPose already has the right local shape for this work:

- WAL-backed local durability for fresh writes
- immutable segment and index publication during flush and compaction
- persisted maintenance state and restart-aware recovery
- a `BlobStore` trait plus `remote_blob` metadata scaffolding

What is missing today:

- no real S3 or MinIO client implementation
- no multipart upload support
- no remote reads, listing, reconciliation, or garbage collection
- no public API to configure remote blob storage for a collection
- no remote durability state beyond placeholder metadata

## Why This Matters

Immutable segments and index sidecars are natural object-store artifacts.

Real blob integration would:

- separate fast local WAL durability from longer-term immutable durability
- make future failover and multi-node recovery practical
- reduce pressure on local disks for compacted history
- align LogPose with the object-storage patterns that systems like Milvus already use

## Research Anchors

- [Amazon S3 overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)
- [Amazon S3 multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [MinIO erasure coding and healing](https://docs.min.io/enterprise/aistor-object-store/operations/core-concepts/erasure-coding/)
- [Milvus architecture overview](https://milvus.io/docs/architecture_overview.md)
- [Milvus standalone advanced configuration](https://raw.githubusercontent.com/milvus-io/milvus-docs/master/site/en/getstarted/standalone/configuration_standalone-advanced.md)
- [Milvus cluster advanced configuration](https://raw.githubusercontent.com/milvus-io/milvus-docs/master/site/en/getstarted/cluster/configuration_cluster-advanced.md)

## Direction For LogPose

Keep the write path local-first.

- WAL remains the low-latency durability boundary for fresh writes
- flush and compaction still publish local immutable artifacts first
- background remote sync then moves complete immutable bundles into object storage

Treat a segment as a bundle, not as one object.

- segment payload
- flat sidecar
- HNSW or later ANN sidecar
- manifest information needed to make the bundle recoverable

Use manifests as the visibility and recovery binding point. Because S3 is only atomic per key, not across several keys, LogPose should only treat a remote bundle as usable after every object in the bundle is uploaded and validated.

Add explicit durability states:

- local_only
- remote_pending
- remote_confirmed
- remote_failed

Operators should be able to see those states in stats and inspect output.

## Main Work Streams

### 1. Object Store Contract

- expand `BlobStore` beyond one `put_object` method
- support reads, metadata checks, deletes, prefix reconciliation, and multipart lifecycle operations
- add real S3-compatible implementations and runtime configuration

### 2. Remote Artifact Model

- store full remote bundle metadata, not just one object key
- record checksums, lengths, status, and timestamps
- decide how manifest generations map to remote visibility and recovery

### 3. Background Sync And Recovery

- add a persisted remote-sync queue
- resume or reconcile pending uploads on restart
- rebuild local immutable artifacts from remote state when that becomes supported
- add remote garbage collection tied to manifest reachability

### 4. Operator Surfaces

- show remote backlog, pending bytes, last upload error, and oldest unsynced artifact age
- support explicit retry and reconcile commands
- document what local durability means versus remote durability

## Testing And Validation

- unit-test object naming, manifest binding rules, multipart state, and GC reachability
- add fake-store integration tests for upload success and failure paths
- add real MinIO-backed integration tests in CI
- inject network, timeout, checksum, and partial-upload failures
- test restart reconciliation and remote-backed recovery behavior

## Exit Criteria

- LogPose can configure real S3-compatible blob storage for immutable artifacts
- flush and compaction publish complete remote artifact bundles, not just placeholder metadata
- multipart upload is used where needed and can recover across restarts
- manifests only treat remote bundles as usable after verification
- operator surfaces expose remote sync progress and failure state
- MinIO-backed integration and failure-injection tests pass in CI
