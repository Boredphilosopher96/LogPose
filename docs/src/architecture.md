# Architecture

LogPose uses a layered Cargo workspace:

- `apps/logpose-server` hosts the main runtime
- `apps/logpose-cli` provides operator tooling
- `crates/logpose-*` isolate core concerns such as config, storage, indexing, query execution, auth, telemetry, and transport layers

This layout is designed to scale cleanly as collection management, replication, durability, query planning, and observability grow in depth.

