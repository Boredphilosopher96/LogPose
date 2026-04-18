#!/usr/bin/env bash
set -euo pipefail

ETCD_ENDPOINT="${LOGPOSE_TEST_ETCD_ENDPOINTS%%,*}"
ETCD_ENDPOINT="${ETCD_ENDPOINT:-http://127.0.0.1:2379}"

if ! curl -fsS "${ETCD_ENDPOINT%/}/health" >/dev/null 2>&1; then
  echo "scripts/check.sh expects etcd to be reachable at ${ETCD_ENDPOINT} for the workspace metadata tests" >&2
  echo "Set LOGPOSE_TEST_ETCD_ENDPOINTS or start a local etcd before running the full verification flow." >&2
  exit 1
fi

for tool in mdbook mdbook-toc; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "scripts/check.sh expects ${tool} to be installed for the docs verification flow" >&2
    exit 1
  fi
done

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
mdbook build docs
