# Repository Guidelines

## Project Structure & Module Organization

LogPose is a Rust workspace. Application entrypoints live in `apps/logpose-server` and `apps/logpose-cli`. Shared libraries live under `crates/` by domain, such as `logpose-storage`, `logpose-index`, and `logpose-api-rest`. The root `Cargo.toml` is a virtual manifest, so there is no root `tests/` or `benches/` directory: integration tests live in each crate's or app's `tests/` directory, and benchmarks live in each crate's `benches/` directory, such as `crates/logpose-query/benches/`. API contracts are in `proto/` and `openapi/`. Contributor docs for mdBook live in `docs/src/`.

## Build, Test, and Development Commands

Use the workspace root for all commands.

- `cargo run -p logpose-server` starts the server on the default REST and gRPC ports.
- `cargo run -p logpose-cli -- status` runs a basic CLI diagnostic.
- `cargo fmt --all --check` enforces formatting.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` treats lint warnings as errors.
- `cargo test --workspace` runs unit and integration tests. The etcd integration tests run only when `LOGPOSE_TEST_ETCD_ENDPOINTS` is set (for example `http://127.0.0.1:2379`) and fail if etcd is then unreachable.
- `cargo doc --workspace --no-deps` builds local API docs.
- `./scripts/check.sh` runs the standard local verification flow used before PRs; it builds the mdBook only when `mdbook` and `mdbook-toc` are installed.

No system `protoc` is needed: `.cargo/config.toml` points `PROTOC` at `scripts/protoc.sh`, which runs the vendored `protoc-bin-vendored` binary.

## Coding Style & Naming Conventions

This repository uses Rust 2024. Follow `.editorconfig`: 4 spaces for Rust, 2 spaces for Markdown, TOML, YAML, and Proto. Let `rustfmt` handle layout. Keep crate and module names snake_case and align filenames with the primary type or responsibility. Workspace lints forbid `unsafe_code` and deny `dbg!`, `todo!`, `unimplemented!`, and `unwrap()`.

## Testing Guidelines

Prefer focused unit tests next to the code under test with `#[cfg(test)]`, and use a crate's `tests/` directory for cross-crate or smoke coverage such as `apps/logpose-cli/tests/help.rs`. Name tests for observable behavior, for example `rejects_invalid_config`. Run `cargo test --workspace` before opening a PR; add targeted tests for any bug fix or new behavior.

## Commit & Pull Request Guidelines

Keep commits small and imperative. Existing history favors short subjects like `Stop tracking docs/superpowers` and scoped prefixes when useful, such as `docs: add production scaffold design spec`. PRs should explain the change, describe operator or user impact, and complete the checklist in `.github/pull_request_template.md`, including `fmt`, `clippy`, `test`, and `doc` verification.

## Security & Configuration Tips

Do not commit generated secrets, local state, or ignored planning material. Keep configuration changes explicit and update `docs/src/`, `proto/`, or `openapi/` when public behavior or contracts change.

## Agent workflow rules

Always prefer using subagents and using multiple parallel agents as much as possible. Especially for exploration and discovery.
Even when implementing plans, parallel implementation using subagents if possible

## Backwards compatibility

Do not care about backwards compatibility. Care only about the best design decisions. The product is still being built and has no users
