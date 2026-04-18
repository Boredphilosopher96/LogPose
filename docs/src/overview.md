# Overview

LogPose is a Rust retrieval database prototype shaped more like a storage engine plus query planner than a standalone ANN service.

## What It Does Today

- `logpose-server` exposes REST and gRPC from one shared runtime
- `logpose-cli` provides operator-facing diagnostics and workflow commands over the same core services
- collections support create, get, and mixed put/delete batches
- collection descriptors now belong to a persisted default database catalog entry, which is the first control-plane step toward real tenancy and policy
- queries can run exact, HNSW-backed ANN, or hybrid exact-plus-ANN execution with explain and profile diagnostics
- operators can inspect runtime status, collection placement, stats, maintenance state, WAL, manifests, and segments

## What It Is Today

- one local process with role-aware control-plane and data-plane boundaries
- local filesystem durability with WAL, manifests, immutable segments, and index sidecars
- persisted database descriptors, bootstrap bearer authentication, and database-scoped access policies for read, write, and owner control
- immutable HNSW sidecars for ANN, with exact execution still covering mutable state and acting as the correctness oracle
- layered integration, randomized, process-boundary, and deterministic service-boundary tests

## What Is Not Here Yet

- fully wired etcd-backed multi-node or multi-cluster metadata management
- replica management, failover, or consistency-mode routing
- vector index families beyond HNSW
- a TigerBeetle-style full-system simulator
- a web GUI
- real MinIO or S3-backed blob storage

Those remaining capabilities are tracked in [Future Milestones](./future-milestones.md).
