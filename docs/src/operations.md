# Operations

LogPose is currently operated as one `logpose-server` process configured through `LOGPOSE_CONFIG`.

## Current Operator Model

Operational workflows are centered around:

- the `logpose-server` runtime
- the `logpose` CLI as a server-first wrapper around the same control-plane and diagnostics workflows
- structured logging and tracing
- repeatable CI/CD quality gates

Use the server as the source of truth for service behavior, and treat the CLI as the preferred operator entrypoint for configuration inspection and diagnostics. REST and gRPC should remain transport-parity views over the same shared workflows, with no semantic drift between them.

## Runtime Boundaries

The runtime boundary is explicit today:

- control-plane workflows now expose runtime status and collection placement reasoning
- data-plane workflows remain responsible for writes, queries, maintenance actions, and storage inspection
- role-specific nodes now reject wrong-plane requests instead of silently serving them through the local filesystem path
- the CLI `diagnostics` surface now reflects server-reported runtime status instead of synthesizing local guesses

## Operator-Facing Diagnostics

Operator-facing query diagnostics now include ANN-aware plan kinds, candidate generation and rerank timings, merge accounting, and fallback reasons. Query-unit artifact and component statistics are surfaced through collection stats and inspect outputs. Together, those surfaces make explain/profile and storage introspection part of the normal operational workflow rather than debugging-only escape hatches.

## Current Limits

Operationally, LogPose is still earlier than a distributed database:

- there is no metadata quorum, cluster membership service, or replica controller
- health and readiness are still simple role-oriented signals, not dependency-aware distributed probes
- authentication and authorization are scaffolds, not full operator policy enforcement
- tracing is initialized, but a metrics endpoint and richer telemetry surfaces do not exist yet
- remote blob synchronization to MinIO or S3 is not implemented yet

Testing and CI are intentionally layered. The repository-level doctrine for generative harnesses, future simulation work, and concern-based CI decomposition lives in [Testing](./testing.md).
