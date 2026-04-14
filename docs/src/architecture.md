# Architecture

LogPose uses a layered Cargo workspace:

- `apps/logpose-server` hosts the main runtime
- `apps/logpose-cli` provides operator tooling
- `crates/logpose-*` isolate core concerns such as config, storage, indexing, query execution, auth, telemetry, and transport layers

The runtime now treats control-plane and data-plane responsibilities as explicit peers instead of one undifferentiated service surface:

- the control plane owns collection lifecycle, runtime status, and placement diagnostics
- the data plane owns writes, queries, maintenance execution, and storage inspection
- REST and gRPC stay transport-parity views over those same shared services

This layout is designed to scale cleanly as collection management, replication, durability, query planning, and observability grow in depth.
