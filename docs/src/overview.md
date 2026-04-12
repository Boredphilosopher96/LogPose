# Overview

LogPose is built to deliver fast vector retrieval, durable service behavior, and clean operational ergonomics for modern AI and search platforms.

The current platform surface now includes:

- exact vector retrieval over REST and gRPC
- collection creation plus mixed put/delete writes
- operator-visible stats, flush, compact, and inspect APIs
- top-level metadata equality filters for exact queries

The workspace is organized for long-term maintainability:

- focused Rust crates for core subsystems
- separate server and CLI applications
- REST and gRPC integration surfaces
- documentation, security, and quality automation embedded into the repository
