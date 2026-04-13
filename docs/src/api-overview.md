# API Overview

LogPose exposes two integration surfaces:

- REST for straightforward HTTP-based control-plane and data-plane operations
- gRPC for strongly typed, high-performance service integrations over the same core workflows

Current public workflows include:

- create and inspect collections
- write mixed put/delete batches
- planner-controlled exact, ANN, and hybrid vector query with legacy equality filters or structured predicate trees over top-level scalar metadata, including lossless 64-bit integer matching
- optional query plan and profile diagnostics that expose planner choice, selectivity estimates, unit pruning, candidate generation, postfilter, rerank, merge, fallback reasons, and per-stage timings
- collection stats, flush, compact, and inspect operations, including maintenance state plus planner-visible query unit artifact and component-byte summaries
- normalized node metadata exposed through the `MetadataResponse` schema, including `product`, `node_name`, `version`, `git_sha`, and `profile`

The REST and gRPC surfaces are expected to describe the same core workflows and to stay aligned with the shared application layer, even when a given transport exposes slightly different ergonomics.

Contract sources live in:

- `openapi/logpose.v1.yaml`
- `proto/logpose/v1/logpose.proto`
