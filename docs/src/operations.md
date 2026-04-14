# Operations

Operational workflows are centered around:

- the `logpose-server` runtime
- the `logpose` CLI as a server-first wrapper around the same control-plane and diagnostics workflows
- structured logging and tracing
- repeatable CI/CD quality gates

Use the server as the source of truth for service behavior, and treat the CLI as the preferred operator entrypoint for configuration inspection and diagnostics. REST and gRPC should remain transport-parity views over the same shared workflows, with no semantic drift between them.

Phase 5 made the runtime boundary explicit:

- control-plane workflows now expose runtime status and collection placement reasoning
- data-plane workflows remain responsible for writes, queries, maintenance actions, and storage inspection
- role-specific nodes now reject wrong-plane requests instead of silently serving them through the local filesystem path
- the CLI `diagnostics` surface now reflects server-reported runtime status instead of synthesizing local guesses

Operator-facing query diagnostics now include ANN-aware plan kinds, candidate generation and rerank timings, merge accounting, and fallback reasons. Query-unit artifact and component statistics are surfaced through collection stats and inspect outputs. Together, those surfaces make explain/profile and storage introspection part of the normal operational workflow rather than debugging-only escape hatches.

Testing and CI are intentionally layered. The repository-level doctrine for generative harnesses, future simulation work, and concern-based CI decomposition lives in [Testing](./testing.md).
