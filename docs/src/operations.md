# Operations

Operational workflows are centered around:

- the `logpose-server` runtime
- the `logpose` CLI as a server-first wrapper around the same control-plane and diagnostics workflows
- structured logging and tracing
- repeatable CI/CD quality gates

Use the server as the source of truth for service behavior, and treat the CLI as the preferred operator entrypoint for configuration inspection and diagnostics. REST and gRPC should remain transport-parity views over the same shared workflows, with no semantic drift between them.

Testing and CI are intentionally layered. The repository-level doctrine for generative harnesses, future simulation work, and concern-based CI decomposition lives in [Testing](./testing.md).
