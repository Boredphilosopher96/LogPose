#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
chaos_script="$repo_root/scripts/podman-chaos.sh"
self_script="$repo_root/scripts/check-chaos.sh"
integration=0
cluster="check-chaos"

while (($# > 0)); do
    case "$1" in
        --integration)
            integration=1
            ;;
        --cluster)
            shift
            cluster="${1:?--cluster requires a value}"
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 1
            ;;
    esac
    shift
done

assert_contains() {
    local haystack="$1"
    local needle="$2"
    if [[ "$haystack" != *"$needle"* ]]; then
        echo "expected output to contain '$needle'" >&2
        exit 1
    fi
}

assert_line() {
    local haystack="$1"
    local needle="$2"
    if ! grep -Fxq "$needle" <<<"$haystack"; then
        echo "expected line '$needle'" >&2
        exit 1
    fi
}

bash -n "$chaos_script" "$self_script"

help_output="$("$chaos_script" help)"
assert_contains "$help_output" "bootstrap"
assert_contains "$help_output" "teardown"
assert_contains "$help_output" "scenario"

node_output="$("$chaos_script" list-nodes)"
assert_line "$node_output" "node-a"
assert_line "$node_output" "node-b"
assert_line "$node_output" "node-c"

scenario_output="$("$chaos_script" list-scenarios)"
assert_line "$scenario_output" "smoke"
assert_line "$scenario_output" "leader-failover"
assert_line "$scenario_output" "etcd-outage"

config_output="$("$chaos_script" render-config node-a --cluster "$cluster")"
assert_contains "$config_output" "node_name = \"node-a\""
assert_contains "$config_output" "backend = \"etcd\""
assert_contains "$config_output" "cluster_name = \"$cluster\""

"$chaos_script" self-test

if ((integration)); then
    trap '"$chaos_script" reset --cluster "$cluster" >/dev/null 2>&1 || true' EXIT
    "$chaos_script" scenario smoke --cluster "$cluster"
fi

echo "chaos script checks passed"
