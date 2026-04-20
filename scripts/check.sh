#!/usr/bin/env bash
set -euo pipefail

for tool in mdbook mdbook-toc; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "scripts/check.sh expects ${tool} to be installed for the docs verification flow" >&2
    exit 1
  fi
done

if ! command -v podman >/dev/null 2>&1; then
  echo "scripts/check.sh expects podman to be installed for the chaos verification flow" >&2
  exit 1
fi

repo_root="$(git rev-parse --show-toplevel)"
chaos_script="$repo_root/scripts/podman-chaos.sh"
chaos_cluster="${LOGPOSE_CHAOS_CLUSTER:-check-sh}"
export LOGPOSE_TEST_ETCD_ENDPOINTS="${LOGPOSE_TEST_ETCD_ENDPOINTS:-http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379}"
trap '"$chaos_script" teardown --cluster "$chaos_cluster" >/dev/null 2>&1 || true' EXIT
LOGPOSE_PODMAN_CHAOS_REBUILD_IMAGE="${LOGPOSE_PODMAN_CHAOS_REBUILD_IMAGE:-0}" \
  "$chaos_script" bootstrap --cluster "$chaos_cluster"

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
mdbook build docs
./scripts/check-chaos.sh --integration --cluster "$chaos_cluster" --seed "${LOGPOSE_CHAOS_SEED:-424242}"
