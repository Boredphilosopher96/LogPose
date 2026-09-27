#!/usr/bin/env bash
# Standard local verification flow used before opening a pull request.
#
# Optional pieces run only when their dependencies are available:
# - the etcd integration tests run when LOGPOSE_TEST_ETCD_ENDPOINTS is set,
#   for example to http://127.0.0.1:2379, and then fail if etcd is unreachable
# - the mdBook build runs when mdbook and mdbook-toc are installed
set -euo pipefail

if [[ -n "${LOGPOSE_TEST_ETCD_ENDPOINTS:-}" ]]; then
  echo "Running etcd integration tests against ${LOGPOSE_TEST_ETCD_ENDPOINTS}"
else
  echo "Skipping etcd integration tests: LOGPOSE_TEST_ETCD_ENDPOINTS is not set" >&2
fi

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps

if command -v mdbook >/dev/null 2>&1 && command -v mdbook-toc >/dev/null 2>&1; then
  mdbook build docs
else
  echo "Skipping mdbook build: install mdbook and mdbook-toc to build the docs" >&2
fi
