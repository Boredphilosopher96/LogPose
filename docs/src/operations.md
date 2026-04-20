# Operations

LogPose is currently operated as one `logpose-server` process configured through `LOGPOSE_CONFIG`.

## Current Operator Model

Operational workflows are centered around:

- the `logpose-server` runtime
- the `logpose-cli` CLI as a server-first wrapper around the same control-plane and data-plane workflows
- structured logging and tracing
- repeatable CI/CD quality gates

Use the server as the source of truth for service behavior, and treat the CLI as the preferred operator entrypoint for configuration inspection, query diagnostics, and maintenance. The CLI now has two explicit modes:

- `logpose-cli interactive` for a guided dashboard with concern-based navigation, searchable workflow pickers, persistent result tabs, clipboard-friendly views, and a shortcut bar that stays visible while you work
- direct commands such as `status`, `config`, `database`, `collection`, `record`, `query`, and `inspect` for fast operator and scripting workflows

Direct commands default to concise human-readable summaries. Use `--json` or `--output json` when you need the exact machine-readable contract. REST and gRPC should remain transport-parity views over the same shared workflows, with no semantic drift between them.

Namespace handling is now database-first. `database list/show/put` sit at the top level, and collection, record, query, and inspect flows default to the `default` database unless you pass `--database <name>`. Human-facing collection labels use `database/collection` outside the default database and collapse to just `collection` inside it.

In interactive mode, the flow is intentionally layered: start from a broad concern area, narrow to a workflow, then fill a guided form with fuzzy-searchable selectors where appropriate. Results stay open instead of ending the session, which makes it practical to copy output, compare summary and json views, or jump straight back into repeat operations such as adding multiple files to the same collection.

## Runtime Boundaries

The runtime boundary is explicit today:

- control-plane workflows now expose runtime status and collection placement reasoning
- data-plane workflows remain responsible for writes, queries, maintenance actions, and storage inspection
- role-specific nodes now reject wrong-plane requests instead of silently serving them through the local filesystem path
- the CLI `status` and `collection placement` surfaces reflect server-reported runtime status and routing instead of synthesizing local guesses

## Operator-Facing Diagnostics

Operator-facing query diagnostics now include ANN-aware plan kinds, candidate generation and rerank timings, merge accounting, and fallback reasons. Query-unit artifact and component statistics are surfaced through collection stats and inspect outputs. Together, those surfaces make explain/profile and storage introspection part of the normal operational workflow rather than debugging-only escape hatches.

## Local Podman Chaos

PR4's local multi-node chaos workflow is documented in [Podman
Chaos](./podman-chaos.md). That page covers the three-node Podman topology, the
repo-owned shell contract checks, the public drain and ownership-control
surfaces, and the exact invariants each scenario must preserve.

## Current Limits

Operationally, LogPose is still earlier than a distributed database:

- authoritative etcd-backed database and collection metadata, membership
  leases, public drain, undrain, promote, and rebalance controls,
  replica-aware placement diagnostics, and seeded Podman chaos validation are
  now in place, but automatic owner failover still promotes only a desired
  replica that already has local materialized state; remote artifact transfer
  remains a later storage concern
- bootstrap bearer authentication, operator-gated database admin, and database-scoped read/write/owner policies now exist, but principal lifecycle and richer policy listing/delete workflows are not complete
- health and readiness are still simple role-oriented signals, not dependency-aware distributed probes
- richer namespace controllers, auditability, and deeper operator workflows are
  still incomplete even though descriptors and policies now live in
  authoritative metadata
- tracing is initialized, but a metrics endpoint and richer telemetry surfaces do not exist yet
- remote blob synchronization to MinIO or S3 is not implemented yet

Testing and CI are intentionally layered. The repository-level doctrine for generative harnesses, future simulation work, and concern-based CI decomposition lives in [Testing](./testing.md).

To enable etcd-backed assignment metadata, set explicit endpoints:

```toml
[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://127.0.0.1:2379"]
key_prefix = "/logpose/metadata"
timeout_ms = 1500
membership_ttl_secs = 15
leadership_ttl_secs = 10
cluster_name = "default"
```

With `metadata.backend = "etcd"`, LogPose now treats etcd as the authoritative
source for collection descriptors and assignments. Collections created before
the etcd metadata path is enabled are not auto-backfilled from local
`placement.json` files; migrate them by recreating them through the control
plane or by explicitly backfilling metadata before flipping an existing storage
root to the etcd backend.
