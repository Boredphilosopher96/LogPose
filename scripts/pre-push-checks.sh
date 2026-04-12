#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

missing_tools=()
for tool in cargo-deny cargo-audit cargo-machete; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        missing_tools+=("$tool")
    fi
done

if ((${#missing_tools[@]} > 0)); then
    printf 'Missing required pre-push tool(s): %s\n' "${missing_tools[*]}" >&2
    printf 'Install them with:\n' >&2
    printf '  cargo install cargo-deny --locked\n' >&2
    printf '  cargo install cargo-audit --locked\n' >&2
    printf '  cargo install cargo-machete --locked\n' >&2
    exit 1
fi

echo "Running pre-push supply-chain checks..."
cargo deny check
cargo audit
cargo machete
