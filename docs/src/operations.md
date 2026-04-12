# Operations

Operational workflows are centered around:

- the `logpose-server` runtime
- the `logpose` CLI
- structured logging and tracing
- repeatable CI/CD quality gates

Use the CLI for configuration inspection and diagnostics, and use the server process for service hosting and API exposure.

Testing and CI are intentionally layered. The repository-level doctrine for generative harnesses, future simulation work, and concern-based CI decomposition lives in [Testing](./testing.md).
