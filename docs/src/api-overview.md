# API Overview

LogPose exposes two integration surfaces:

- REST for straightforward HTTP-based control-plane and data-plane operations
- gRPC for strongly typed, high-performance service integrations over the same core workflows

Current public workflows include:

- create and inspect collections
- write mixed put/delete batches
- exact vector query with optional top-level metadata equality filters, including lossless 64-bit integer matching
- collection stats, flush, compact, and inspect operations
- normalized node metadata exposed through the `MetadataResponse` schema, including `product`, `node_name`, `version`, `git_sha`, and `profile`

The REST and gRPC surfaces are expected to describe the same core workflows and to stay aligned with the shared application layer, even when a given transport exposes slightly different ergonomics.

Contract sources live in:

- `openapi/logpose.v1.yaml`
- `proto/logpose/v1/logpose.proto`
