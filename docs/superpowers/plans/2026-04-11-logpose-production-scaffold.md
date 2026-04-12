# LogPose Production Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade Rust workspace scaffold for LogPose with server and CLI applications, subsystem crates, documentation, CI/CD, quality gates, and security tooling.

**Architecture:** Use a layered Cargo workspace with focused crates under `crates/` and application entrypoints under `apps/`. Keep initial runtime behavior limited to configuration loading, telemetry bootstrap, REST/gRPC wiring, and CLI command structure while ensuring the workspace compiles and the repo presents as a polished production system.

**Tech Stack:** Rust 1.94.1, Cargo workspace, Tokio, Axum, Tonic, Clap, Serde, Tracing, mdBook, GitHub Actions, cargo-deny, cargo-audit, cargo-machete, typos

---

### Task 1: Create workspace and repository metadata

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `rustfmt.toml`
- Create: `.clippy.toml`
- Create: `.editorconfig`
- Create: `.gitignore`
- Create: `.gitattributes`

- [ ] **Step 1: Add root workspace manifest and shared lint settings**
- [ ] **Step 2: Pin Rust toolchain to 1.94.1**
- [ ] **Step 3: Add formatting, editor, and git metadata defaults**
- [ ] **Step 4: Verify manifest parses with `cargo metadata`**

### Task 2: Scaffold crates and applications

**Files:**
- Create: `apps/logpose-server/**`
- Create: `apps/logpose-cli/**`
- Create: `crates/logpose-*/**`
- Create: `proto/logpose/v1/logpose.proto`
- Create: `openapi/logpose.v1.yaml`

- [ ] **Step 1: Create focused library crates for shared domains**
- [ ] **Step 2: Add server and CLI binaries with compile-safe startup flow**
- [ ] **Step 3: Add REST and gRPC scaffold wiring**
- [ ] **Step 4: Verify `cargo test` reaches a green baseline**

### Task 3: Add tests, benchmarks, and scripts

**Files:**
- Create: `tests/smoke_workspace.rs`
- Create: `benches/bootstrap.rs`
- Create: `scripts/check.sh`
- Create: `scripts/docs.sh`

- [ ] **Step 1: Add smoke tests for CLI and configuration bootstrap**
- [ ] **Step 2: Add benchmark harness placeholder**
- [ ] **Step 3: Add helper scripts for local quality runs**
- [ ] **Step 4: Verify tests and benches build**

### Task 4: Add docs and governance

**Files:**
- Create: `README.md`
- Create: `LICENSE`
- Create: `CONTRIBUTING.md`
- Create: `CODE_OF_CONDUCT.md`
- Create: `SECURITY.md`
- Create: `docs/book.toml`
- Create: `docs/src/**`
- Create: `.github/**`

- [ ] **Step 1: Write marketing-forward README and contributor docs**
- [ ] **Step 2: Create mdBook documentation structure**
- [ ] **Step 3: Add issue templates, PR template, and CODEOWNERS**
- [ ] **Step 4: Verify docs build with `mdbook build docs`**

### Task 5: Add CI/CD and supply-chain tooling

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/release.yml`
- Create: `.github/dependabot.yml`
- Create: `deny.toml`
- Create: `.cargo/audit.toml`
- Create: `.config/nextest.toml`
- Create: `.markdownlint.json`
- Create: `.typos.toml`

- [ ] **Step 1: Add CI workflow for format, lint, test, docs, and security checks**
- [ ] **Step 2: Add release workflow for tagged artifacts**
- [ ] **Step 3: Add repository quality and dependency scanning configuration**
- [ ] **Step 4: Verify workflow YAML and local commands are internally consistent**

### Task 6: Final verification

**Files:**
- Modify: all created files as needed

- [ ] **Step 1: Run `cargo fmt --all --check`**
- [ ] **Step 2: Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`**
- [ ] **Step 3: Run `cargo test --workspace`**
- [ ] **Step 4: Run `cargo doc --workspace --no-deps`**
- [ ] **Step 5: Run `cargo metadata --format-version 1`**
- [ ] **Step 6: Fix any issues until all required checks pass**
