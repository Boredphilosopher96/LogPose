# LogPose

LogPose is a high-performance, reliable vector database built for low-latency retrieval, operational clarity, and production-scale deployments. It is designed to give engineering teams a durable foundation for semantic search, recommendation systems, retrieval-augmented generation, and real-time similarity workloads.

## Why LogPose

- High-throughput architecture designed for predictable performance
- Reliable service boundaries with durability, observability, and operational discipline
- Dual REST and gRPC APIs for flexible integrations
- First-class CLI for administration, diagnostics, and data workflows
- Strong quality gates with linting, tests, supply-chain checks, and CI/CD automation
- Documentation built with mdBook for operators, contributors, and integrators

## Project Layout

- `apps/logpose-server`: main server process with REST and gRPC listeners
- `apps/logpose-cli`: CLI for local administration, diagnostics, and data operations
- `crates/`: shared libraries for configuration, transport, storage, indexing, query execution, telemetry, and domain types
- `proto/`: protobuf contracts for gRPC
- `openapi/`: versioned REST API descriptions
- `docs/`: mdBook documentation source
- `.github/`: CI/CD workflows, templates, and repository automation

## Getting Started

### Prerequisites

- Rust `1.94.1` via `rustup`
- Cargo `1.94.1`

No system `protoc` is needed. `.cargo/config.toml` points `PROTOC` at `scripts/protoc.sh`, which runs the vendored `protoc-bin-vendored` binary; set `PROTOC` yourself to override it (required on Windows).

Optional:

- etcd, only for the etcd integration tests. They are skipped unless `LOGPOSE_TEST_ETCD_ENDPOINTS` is set, for example `LOGPOSE_TEST_ETCD_ENDPOINTS=http://127.0.0.1:2379`, and fail if etcd is then unreachable.
- `mdbook` and `mdbook-toc`, only to build the documentation site; `scripts/check.sh` skips that step without them.

### Bootstrap

```bash
cargo metadata --format-version 1 > /dev/null
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

### Run The CLI

```bash
cargo run -p logpose-cli -- status
```

### Run The Server

```bash
cargo run -p logpose-server
```

The default bootstrap exposes:

- REST on `127.0.0.1:8080`
- gRPC on `127.0.0.1:50051`

## Documentation

Published documentation is available at:

- [Hosted documentation](https://boredphilosopher96.github.io/LogPose/overview.html)

Build the documentation site locally with:

```bash
mdbook build docs
```

Primary guides live in `docs/src/` and cover architecture, setup, operations, APIs, security, and contribution workflows.

The repository testing doctrine, including the TigerBeetle-inspired harness strategy and CI layering, is documented in `docs/src/testing.md`.

## Developer Workflow

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace`
- `cargo doc --workspace --no-deps`

Use `scripts/check.sh` for the standard local verification flow.

To run supply-chain checks automatically before every push, enable the tracked Git hook once per clone:

```bash
git config core.hooksPath .githooks
```

That hook runs `cargo deny check`, `cargo audit`, and `cargo machete` through `scripts/pre-push-checks.sh`.

## License

LogPose is available under the MIT License. See `LICENSE` for details.
