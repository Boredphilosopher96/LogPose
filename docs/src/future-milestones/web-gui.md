# Web GUI

## Goal

Add a browser-based operator and developer console for LogPose so the system's runtime status, placement reasoning, query diagnostics, maintenance state, and storage inspection surfaces are usable without dropping straight into raw JSON or CLI output.

## Current State

LogPose already exposes many UI-worthy surfaces:

- runtime status with role, listeners, collection inventory, and maintenance totals
- collection placement diagnostics with route kind and route reason
- create and inspect collection workflows
- writes, queries, stats, flush, compact, and inspect operations
- query explain and profile diagnostics with planner, candidate, rerank, merge, and timing data

What is missing today:

- no browser UI
- no public collection listing endpoint even though the service layer can enumerate collections internally
- no delete or drop collection endpoint
- no record-browse or scroll-style inspection API
- no browser-ready auth flow, metrics endpoint, or audit trail

## Why This Matters

LogPose already spends engineering effort on operator-visible contracts. A GUI makes those contracts actually usable.

The highest-value workflows are not generic CRUD screens. They are:

- seeing whether a node is healthy and what it can serve
- understanding where a collection is placed and why
- running a query and immediately seeing planner choice and timing breakdown
- inspecting WAL, manifests, segments, and maintenance state without hand-reading raw responses

## Research Anchors

- [Attu for Milvus](https://github.com/zilliztech/attu)
- [pgAdmin features](https://www.pgadmin.org/features/)
- [Redis Insight](https://redis.io/insight/)
- [Qdrant Web UI](https://qdrant.tech/documentation/web-ui/)
- [Qdrant monitoring](https://qdrant.tech/documentation/operations/monitoring/)
- [Qdrant security](https://qdrant.tech/documentation/operations/security/)

## Direction For LogPose

Build the first GUI as a practical embedded console served by `logpose-server`.

Start narrow:

- an overview page for node status and readiness
- a collections page for inventory, placement, and quick health
- a collection detail page for stats, maintenance, and inspect data
- a query workbench for vector input, filters, explain, and profile
- safe operator actions such as create, flush, and compact

Do not try to build a full cloud console in v1. The first value is turning the existing operator contracts into fast visual workflows.

## Main Work Streams

### 1. API Gaps

- add public collection listing
- add delete or drop collection with explicit safety rules
- add record-browse or sample-record inspection APIs if the UI needs data-level visibility
- add capability discovery so the UI can adapt to server feature level

### 2. Auth And Policy

- turn the current auth scaffold into real browser-usable authn and authz
- support at least operator and read-only roles
- add audit logging for admin actions that the UI can trigger

### 3. Observability

- add a metrics endpoint
- add richer readiness and health detail than the current basic status response
- expose enough telemetry to power dashboards without scraping raw logs only

### 4. Frontend Application

- decide how assets are built and served
- generate or hand-maintain a stable API client layer from the OpenAPI or proto contracts
- keep UI views tightly coupled to existing operator language rather than inventing a second vocabulary

## Testing And Validation

- extend API contract tests for every new UI-backed endpoint
- keep transport parity for shared workflows
- add browser end-to-end tests against a real server fixture
- validate auth and role restrictions through real UI and HTTP flows
- keep snapshot coverage for important rendered diagnostics where text or JSON stability matters

## Exit Criteria

- a browser can connect to `logpose-server` and show runtime status, collection inventory, placement reasoning, stats, and inspect outputs
- operators can safely create collections and run flush or compact from the UI
- developers can run queries with explain and profile and understand the plan visually
- browser-usable auth exists with at least operator versus read-only separation
- API, auth, and browser workflows are covered by automated tests
