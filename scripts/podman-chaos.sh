#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
state_root_default="$repo_root/.logpose/podman-chaos"
image_tag_default="localhost/logpose-chaos:dev"
etcd_image_default="quay.io/coreos/etcd:v3.5.18"
key_prefix_default="/logpose/chaos"
machine_name_default="podman-machine-default"
logpose_server_container_port=8080
logpose_server_grpc_port=50051
readonly nodes=("node-a" "node-b" "node-c")
readonly scenarios=(
    "smoke"
    "new-node-registration"
    "concurrent-writers"
    "owner-failover"
    "leader-failover"
    "lagging-node-rejoin"
    "etcd-outage"
    "partition-heal"
)

usage() {
    cat <<'EOF'
Usage: scripts/podman-chaos.sh <command> [options]

Commands:
  help
  list-nodes
  list-scenarios
  render-config <node> [--cluster <name>]
  bootstrap [--cluster <name>]
  teardown [--cluster <name>]
  reset [--cluster <name>]
  status [--cluster <name>] [collection]
  self-test
  scenario <name> [--cluster <name>]

Scenarios:
  smoke
  new-node-registration
  concurrent-writers
  owner-failover
  leader-failover
  lagging-node-rejoin
  etcd-outage
  partition-heal

Environment:
  LOGPOSE_PODMAN_CHAOS_STATE_DIR  Base state directory (default: .logpose/podman-chaos)
  LOGPOSE_PODMAN_CHAOS_IMAGE      Server image tag (default: localhost/logpose-chaos:dev)
  LOGPOSE_PODMAN_CHAOS_REBUILD_IMAGE  Rebuild the server image even if the tag already exists (default: 0)
  LOGPOSE_PODMAN_CHAOS_ETCD_IMAGE Etcd image tag (default: quay.io/coreos/etcd:v3.5.18)
  LOGPOSE_PODMAN_CHAOS_KEY_PREFIX Etcd key prefix (default: /logpose/chaos)
  LOGPOSE_PODMAN_MACHINE_NAME     Podman machine name (default: podman-machine-default)
EOF
}

cluster_name="${LOGPOSE_PODMAN_CHAOS_CLUSTER:-podman-chaos}"
state_root="${LOGPOSE_PODMAN_CHAOS_STATE_DIR:-$state_root_default}"
image_tag="${LOGPOSE_PODMAN_CHAOS_IMAGE:-$image_tag_default}"
rebuild_image="${LOGPOSE_PODMAN_CHAOS_REBUILD_IMAGE:-0}"
etcd_image="${LOGPOSE_PODMAN_CHAOS_ETCD_IMAGE:-$etcd_image_default}"
key_prefix="${LOGPOSE_PODMAN_CHAOS_KEY_PREFIX:-$key_prefix_default}"
machine_name="${LOGPOSE_PODMAN_MACHINE_NAME:-$machine_name_default}"

parse_cluster_flag() {
    local args=()
    remaining_args=()
    while (($# > 0)); do
        case "$1" in
            --cluster)
                shift
                cluster_name="${1:?--cluster requires a value}"
                ;;
            *)
                args+=("$1")
                ;;
        esac
        shift || true
    done
    if ((${#args[@]} > 0)); then
        remaining_args=("${args[@]}")
    fi
}

log() {
    printf '[podman-chaos] %s\n' "$*" >&2
}

die() {
    printf '[podman-chaos] %s\n' "$*" >&2
    exit 1
}

cluster_slug() {
    printf '%s' "$cluster_name" | tr -c '[:alnum:]' '-'
}

cluster_state_dir() {
    printf '%s/%s\n' "$state_root" "$(cluster_slug)"
}

network_name() {
    printf 'logpose-chaos-%s\n' "$(cluster_slug)"
}

logpose_container_name() {
    printf 'logpose-%s-%s\n' "$(cluster_slug)" "$1"
}

etcd_container_name() {
    printf 'logpose-%s-etcd-%s\n' "$(cluster_slug)" "$1"
}

node_role() {
    case "$1" in
        node-a | node-b)
            printf 'combined\n'
            ;;
        node-c)
            printf 'data\n'
            ;;
        *)
            die "unknown node '$1'"
            ;;
    esac
}

node_rest_port() {
    case "$1" in
        node-a) printf '18080\n' ;;
        node-b) printf '18081\n' ;;
        node-c) printf '18082\n' ;;
        *) die "unknown node '$1'" ;;
    esac
}

node_grpc_port() {
    case "$1" in
        node-a) printf '15051\n' ;;
        node-b) printf '15052\n' ;;
        node-c) printf '15053\n' ;;
        *) die "unknown node '$1'" ;;
    esac
}

node_http_url() {
    printf 'http://127.0.0.1:%s\n' "$(node_rest_port "$1")"
}

node_data_dir() {
    printf '%s/data/%s\n' "$(cluster_state_dir)" "$1"
}

node_cli_storage_dir() {
    printf '%s/cli/%s\n' "$(cluster_state_dir)" "$1"
}

node_config_path() {
    printf '%s/config/%s.toml\n' "$(cluster_state_dir)" "$1"
}

etcd_data_dir() {
    printf '%s/etcd/%s\n' "$(cluster_state_dir)" "$1"
}

etcd_host_client_port() {
    case "$1" in
        etcd-1) printf '12379\n' ;;
        etcd-2) printf '22379\n' ;;
        etcd-3) printf '32379\n' ;;
        *) die "unknown etcd member '$1'" ;;
    esac
}

etcd_host_endpoints_csv() {
    printf 'http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379\n'
}

etcd_container_endpoints_toml() {
    printf '["http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"]\n'
}

ensure_dir() {
    mkdir -p "$1"
}

toml_basic_string() {
    python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

run_with_timeout() {
    local timeout_secs="$1"
    shift
    python3 - "$timeout_secs" "$@" <<'PY'
import subprocess
import sys

timeout_secs = float(sys.argv[1])
command = sys.argv[2:]
try:
    completed = subprocess.run(command, timeout=timeout_secs, check=False)
except subprocess.TimeoutExpired:
    print(
        f"command timed out after {timeout_secs:g}s: {' '.join(command)}",
        file=sys.stderr,
    )
    sys.exit(124)
sys.exit(completed.returncode)
PY
}

json_query() {
    local expression="$1"
    python3 -c '
import json
import sys

expr = sys.argv[1]
value = json.load(sys.stdin)
current = value
for part in expr.split("."):
    if not part:
        continue
    if isinstance(current, list):
        current = current[int(part)]
    else:
        current = current[part]
if isinstance(current, (dict, list)):
    json.dump(current, sys.stdout)
    sys.stdout.write("\n")
elif current is None:
    sys.stdout.write("null\n")
elif isinstance(current, bool):
    sys.stdout.write("true\n" if current else "false\n")
else:
    sys.stdout.write(f"{current}\n")
' "$expression"
}

python_json_assert() {
    local script="$1"
    python3 - <<PY
import json
import pathlib
import sys
$script
PY
}

cli_config_toml() {
    local node="$1"
    cat <<EOF
node_name = "cli-${node}"
node_role = "combined"
rest_host = "127.0.0.1"
rest_port = $(node_rest_port "$node")
grpc_host = "127.0.0.1"
grpc_port = $(node_grpc_port "$node")
log_filter = "info"
storage_root = "$(node_cli_storage_dir "$node")"
EOF
}

render_node_config() {
    local node="$1"
    cat <<EOF
node_name = $(toml_basic_string "$node")
node_role = $(toml_basic_string "$(node_role "$node")")
rest_host = "0.0.0.0"
rest_port = ${logpose_server_container_port}
grpc_host = "0.0.0.0"
grpc_port = ${logpose_server_grpc_port}
log_filter = "info,logpose=debug"
storage_root = "/var/lib/logpose"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = $(etcd_container_endpoints_toml)
key_prefix = $(toml_basic_string "$key_prefix")
timeout_ms = 1500
membership_ttl_secs = 4
leadership_ttl_secs = 3
cluster_name = $(toml_basic_string "$cluster_name")
EOF
}

build_host_binaries() {
    log "building host CLI and etcd admin helper"
    cargo build -p logpose-cli >/dev/null
    cargo build -p logpose-storage-etcd --example etcd_coordination_admin >/dev/null
}

cli_bin() {
    printf '%s/target/debug/logpose-cli\n' "$repo_root"
}

admin_bin() {
    printf '%s/target/debug/examples/etcd_coordination_admin\n' "$repo_root"
}

cli_json() {
    local node="$1"
    shift
    ensure_dir "$(node_cli_storage_dir "$node")"
    LOGPOSE_CONFIG="$(cli_config_toml "$node")" "$(cli_bin)" --json "$@"
}

admin_json() {
    LOGPOSE_ETCD_ENDPOINTS="$(etcd_host_endpoints_csv)" \
    LOGPOSE_ETCD_CLUSTER="$cluster_name" \
    LOGPOSE_ETCD_KEY_PREFIX="$key_prefix" \
        "$(admin_bin)" "$@"
}

ensure_podman_ready() {
    if podman info >/dev/null 2>&1; then
        return
    fi
    if [[ "$(uname -s)" != "Darwin" ]]; then
        die "podman info is unavailable. Start the local Podman service manually before running this harness."
    fi
    log "podman info not ready; checking machine '${machine_name}'"
    if ! podman machine list | awk 'NR>1 {print $1}' | sed 's/\*$//' | grep -Fxq "$machine_name"; then
        log "creating podman machine '${machine_name}' with applehv provider"
        CONTAINERS_MACHINE_PROVIDER="${CONTAINERS_MACHINE_PROVIDER:-applehv}" \
            podman machine init "$machine_name" --cpus 5 --memory 4096 --disk-size 30 >/dev/null
    else
        local machine_state
        machine_state="$(
            podman machine inspect "$machine_name" 2>/dev/null | \
                python3 -c 'import json, sys; print(json.load(sys.stdin)[0]["State"])'
        )"
        if [[ "$machine_state" == "running" ]]; then
            for _ in $(seq 1 15); do
                if podman info >/dev/null 2>&1; then
                    return
                fi
                sleep 2
            done
            log "machine '${machine_name}' is running but its API is unavailable; recycling it"
            podman machine stop "$machine_name" >/dev/null || true
        fi
    fi
    local start_output=""
    if start_output="$(run_with_timeout 120 podman machine start "$machine_name" 2>&1 >/dev/null)"; then
        :
    else
        local start_exit=$?
        if [[ "$start_exit" -eq 124 ]]; then
            die "timed out while starting podman machine '${machine_name}'"
        fi
        if [[ "$start_output" != *"already running"* && "$start_output" != *"proxy already running"* ]]; then
            die "failed to start podman machine '${machine_name}'. If macOS is configured for libkrun without krunkit, recreate the machine with CONTAINERS_MACHINE_PROVIDER=applehv and vfkit installed."
        fi
    fi
    for _ in $(seq 1 30); do
        if podman info >/dev/null 2>&1; then
            return
        fi
        sleep 2
    done
    die "podman machine '${machine_name}' did not become ready"
}

reject_remaining_args() {
    if ((${#remaining_args[@]} > 0)); then
        die "unexpected arguments: ${remaining_args[*]}"
    fi
}

build_server_image() {
    ensure_podman_ready
    if [[ "$rebuild_image" != "1" ]] && podman image exists "$image_tag" >/dev/null 2>&1; then
        log "reusing existing server image ${image_tag}"
        return
    fi
    log "building server image ${image_tag}"
    podman build -t "$image_tag" -f "$repo_root/deploy/Dockerfile" "$repo_root" >/dev/null
}

ensure_network() {
    if ! podman network exists "$(network_name)" >/dev/null 2>&1; then
        podman network create "$(network_name)" >/dev/null
    fi
}

container_exists() {
    podman container exists "$1" >/dev/null 2>&1
}

remove_container_if_present() {
    if container_exists "$1"; then
        podman rm -f "$1" >/dev/null
    fi
}

start_etcd_member() {
    local member="$1"
    local host_client_port
    host_client_port="$(etcd_host_client_port "$member")"
    local name
    name="$(etcd_container_name "$member")"
    remove_container_if_present "$name"
    ensure_dir "$(etcd_data_dir "$member")"
    podman run -d \
        --name "$name" \
        --network "$(network_name)" \
        --network-alias "$member" \
        -p "${host_client_port}:2379" \
        -v "$(etcd_data_dir "$member"):/etcd-data" \
        "$etcd_image" \
        /usr/local/bin/etcd \
        --name "$member" \
        --data-dir /etcd-data \
        --listen-client-urls http://0.0.0.0:2379 \
        --advertise-client-urls "http://${member}:2379" \
        --listen-peer-urls http://0.0.0.0:2380 \
        --initial-advertise-peer-urls "http://${member}:2380" \
        --initial-cluster "etcd-1=http://etcd-1:2380,etcd-2=http://etcd-2:2380,etcd-3=http://etcd-3:2380" \
        --initial-cluster-state new \
        --initial-cluster-token "$cluster_name" \
        >/dev/null
}

wait_for_etcd() {
    local url="$1"
    for _ in $(seq 1 30); do
        if curl -fsS "${url}/health" >/dev/null 2>&1; then
            return
        fi
        sleep 1
    done
    die "etcd endpoint ${url} did not become healthy"
}

start_etcd_cluster() {
    ensure_network
    start_etcd_member etcd-1
    start_etcd_member etcd-2
    start_etcd_member etcd-3
    wait_for_etcd "http://127.0.0.1:12379"
    wait_for_etcd "http://127.0.0.1:22379"
    wait_for_etcd "http://127.0.0.1:32379"
}

write_node_config_file() {
    local node="$1"
    local path
    path="$(node_config_path "$node")"
    ensure_dir "$(dirname "$path")"
    render_node_config "$node" >"$path"
}

start_node() {
    local node="$1"
    local name
    name="$(logpose_container_name "$node")"
    write_node_config_file "$node"
    ensure_dir "$(node_data_dir "$node")"
    remove_container_if_present "$name"
    podman run -d \
        --name "$name" \
        --network "$(network_name)" \
        --network-alias "$node" \
        -p "$(node_rest_port "$node"):8080" \
        -p "$(node_grpc_port "$node"):50051" \
        -v "$(node_config_path "$node"):/etc/logpose/config.toml:ro" \
        -v "$(node_data_dir "$node"):/var/lib/logpose" \
        --entrypoint /bin/sh \
        "$image_tag" \
        -lc 'export LOGPOSE_CONFIG="$(cat /etc/logpose/config.toml)"; exec logpose-server' \
        >/dev/null
    wait_for_node_ready "$node"
}

wait_for_node_ready() {
    local node="$1"
    for _ in $(seq 1 60); do
        if cli_json "$node" status >/dev/null 2>&1; then
            return
        fi
        sleep 1
    done
    die "node '${node}' did not become ready"
}

bootstrap_cluster() {
    local requested_nodes=("$@")
    ensure_podman_ready
    build_host_binaries
    build_server_image
    teardown_cluster
    rm -rf "$(cluster_state_dir)"
    ensure_dir "$(cluster_state_dir)"
    start_etcd_cluster
    admin_json wipe-cluster --yes >/dev/null
    for node in "${requested_nodes[@]}"; do
        start_node "$node"
    done
}

teardown_cluster() {
    for node in "${nodes[@]}"; do
        remove_container_if_present "$(logpose_container_name "$node")"
    done
    for member in etcd-1 etcd-2 etcd-3; do
        remove_container_if_present "$(etcd_container_name "$member")"
    done
    podman network rm "$(network_name)" >/dev/null 2>&1 || true
}

reset_cluster() {
    teardown_cluster
    rm -rf "$(cluster_state_dir)"
}

status_json() {
    cli_json "$1" status
}

placement_json() {
    local node="$1"
    local collection="$2"
    cli_json "$node" collection placement "$collection"
}

collection_show_json() {
    local node="$1"
    local collection="$2"
    cli_json "$node" collection show "$collection"
}

stats_json() {
    local node="$1"
    local collection="$2"
    shift 2
    cli_json "$node" collection stats "$collection" "$@"
}

query_json() {
    local node="$1"
    local collection="$2"
    shift 2
    cli_json "$node" query "$collection" --top-k 4 --vector 1.0,0.0 "$@"
}

owner_node() {
    local probe="$1"
    local collection="$2"
    placement_json "$probe" "$collection" | python3 -c '
import json
import sys
placement = json.load(sys.stdin)
print(placement.get("owner_node") or placement["assigned_node"])
'
}

owner_epoch() {
    local probe="$1"
    local collection="$2"
    placement_json "$probe" "$collection" | python3 -c '
import json
import sys
placement = json.load(sys.stdin)
epoch = placement.get("ownership_epoch")
print("" if epoch is None else epoch)
'
}

wait_for_membership_count() {
    local probe="$1"
    local expected="$2"
    for _ in $(seq 1 40); do
        local count
        count="$(status_json "$probe" | python3 -c '
import json
import sys
status = json.load(sys.stdin)
coord = status["coordination"]
print(len(coord["registered_members"]))
')"
        if [[ "$count" == "$expected" ]]; then
            return
        fi
        sleep 1
    done
    die "timed out waiting for membership count ${expected} on ${probe}"
}

wait_for_member_present() {
    local probe="$1"
    local member="$2"
    for _ in $(seq 1 40); do
        if status_json "$probe" | python3 -c '
import json
import sys
member = sys.argv[1]
status = json.load(sys.stdin)
members = status["coordination"]["registered_members"]
sys.exit(0 if member in members else 1)
' "$member"
        then
            return
        fi
        sleep 1
    done
    die "timed out waiting for member '${member}' on ${probe}"
}

wait_for_member_absent() {
    local probe="$1"
    local member="$2"
    for _ in $(seq 1 40); do
        if status_json "$probe" | python3 -c '
import json
import sys
member = sys.argv[1]
status = json.load(sys.stdin)
members = status["coordination"]["registered_members"]
sys.exit(0 if member not in members else 1)
' "$member"
        then
            return
        fi
        sleep 1
    done
    die "timed out waiting for member '${member}' to disappear from ${probe}"
}

wait_for_owner() {
    local probe="$1"
    local collection="$2"
    local expected_owner="$3"
    local expected_epoch="$4"
    for _ in $(seq 1 40); do
        local owner epoch
        owner="$(owner_node "$probe" "$collection")"
        epoch="$(owner_epoch "$probe" "$collection")"
        if [[ "$owner" == "$expected_owner" && "$epoch" == "$expected_epoch" ]]; then
            return
        fi
        sleep 1
    done
    die "timed out waiting for owner ${expected_owner} epoch ${expected_epoch} on ${collection}"
}

wait_for_local_leader() {
    local probe="$1"
    local expected_leader="$2"
    for _ in $(seq 1 40); do
        if status_json "$probe" | python3 -c '
import json
import sys
expected_leader = sys.argv[1]
status = json.load(sys.stdin)
coord = status["coordination"]
sys.exit(0 if coord["is_local_leader"] and coord["leader_node"] == expected_leader else 1)
' "$expected_leader"
        then
            return
        fi
        sleep 1
    done
    die "timed out waiting for ${probe} to report itself as leader ${expected_leader}"
}

wait_for_follower_of_leader() {
    local probe="$1"
    local expected_leader="$2"
    for _ in $(seq 1 40); do
        if status_json "$probe" | python3 -c '
import json
import sys
expected_leader = sys.argv[1]
status = json.load(sys.stdin)
coord = status["coordination"]
sys.exit(0 if (not coord["is_local_leader"]) and coord["leader_node"] == expected_leader else 1)
' "$expected_leader"
        then
            return
        fi
        sleep 1
    done
    die "timed out waiting for ${probe} to follow leader ${expected_leader}"
}

wait_for_etcd_quorum_loss() {
    local probe="$1"
    for _ in $(seq 1 60); do
        if status_json "$probe" | python3 -c '
import json
import sys
status = json.load(sys.stdin)
coord = status["coordination"]
healthy = (
    coord["membership_registered"]
    or bool(coord["registered_members"])
    or coord["leader_node"] is not None
    or status["control_plane_ready"]
    or status["data_plane_ready"]
)
sys.exit(0 if (not healthy) and coord.get("last_error") else 1)
'
        then
            return
        fi
        sleep 1
    done
    die "timed out waiting for ${probe} to report fail-closed status after etcd quorum loss"
}

wait_for_pid_running() {
    local pid="$1"
    local label="$2"
    for _ in $(seq 1 40); do
        if kill -0 "$pid" >/dev/null 2>&1; then
            return
        fi
        sleep 0.25
    done
    die "${label} exited before fault injection"
}

write_batch_file() {
    local path="$1"
    local prefix="$2"
    local count="$3"
    local x="$4"
    local y="$5"
    python3 - "$path" "$prefix" "$count" "$x" "$y" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
prefix = sys.argv[2]
count = int(sys.argv[3])
x = float(sys.argv[4])
y = float(sys.argv[5])
lines = []
for index in range(count):
    lines.append(json.dumps({
        "id": f"{prefix}-{index}",
        "vector": [x, y],
        "metadata": {"kind": prefix, "index": index},
    }))
path.write_text("\n".join(lines))
PY
}

extract_snapshot_arg() {
    local json_file="$1"
    local field="$2"
    python3 - "$json_file" "$field" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
snapshot = payload["snapshot"]
print(snapshot[sys.argv[2]])
PY
}

assert_live_record_count() {
    local json_payload="$1"
    local expected="$2"
    python3 -c '
import json
import sys

payload = json.loads(sys.argv[1])
expected = int(sys.argv[2])
actual = int(payload["live_record_count"])
if actual != expected:
    raise SystemExit(f"expected live_record_count={expected}, got {actual}")
' "$json_payload" "$expected"
}

assert_failure_contains() {
    local stderr_file="$1"
    local needle="$2"
    grep -Fq "$needle" "$stderr_file" || die "expected failure output to contain '${needle}'"
}

translate_collection_root_to_host() {
    local node="$1"
    local container_root="$2"
    printf '%s%s\n' "$(node_data_dir "$node")" "${container_root#/var/lib/logpose}"
}

mirror_collection_root() {
    local from_node="$1"
    local collection="$2"
    local to_node="$3"
    local descriptor_json
    descriptor_json="$(collection_show_json "$from_node" "$collection")"
    local container_root
    container_root="$(printf '%s' "$descriptor_json" | json_query root_path)"
    local source_root target_root
    source_root="$(translate_collection_root_to_host "$from_node" "$container_root")"
    target_root="$(translate_collection_root_to_host "$to_node" "$container_root")"
    python3 - "$source_root" "$target_root" <<'PY'
import pathlib
import shutil
import sys

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
if not source.exists():
    raise SystemExit(f"source collection root does not exist: {source}")
target.parent.mkdir(parents=True, exist_ok=True)
if target.exists():
    shutil.rmtree(target)
shutil.copytree(source, target)
PY
}

stop_node() {
    podman stop -t 0 "$(logpose_container_name "$1")" >/dev/null
}

start_node_again() {
    start_node "$1"
}

stop_etcd_member() {
    podman stop -t 0 "$(etcd_container_name "$1")" >/dev/null
}

disconnect_node() {
    podman network disconnect "$(network_name)" "$(logpose_container_name "$1")" >/dev/null
}

reconnect_node() {
    podman network connect --alias "$1" "$(network_name)" "$(logpose_container_name "$1")" >/dev/null
}

create_collection() {
    local node="$1"
    local collection="$2"
    cli_json "$node" collection create "$collection" --dimensions 2 --metric dot >/dev/null
}

run_write() {
    local node="$1"
    local collection="$2"
    local input="$3"
    local output="$4"
    cli_json "$node" record put "$collection" --input "$input" >"$output"
}

run_stats_with_barrier() {
    local node="$1"
    local collection="$2"
    local manifest="$3"
    local visible="$4"
    stats_json "$node" "$collection" \
        --read-barrier-manifest-generation "$manifest" \
        --read-barrier-visible-seq-no "$visible"
}

run_query_with_snapshot() {
    local node="$1"
    local collection="$2"
    local manifest="$3"
    local visible="$4"
    query_json "$node" "$collection" \
        --snapshot-manifest-generation "$manifest" \
        --snapshot-visible-seq-no "$visible"
}

run_query_with_barrier_expect_failure() {
    local node="$1"
    local collection="$2"
    local manifest="$3"
    local visible="$4"
    local stdout_file="$5"
    local stderr_file="$6"
    if query_json "$node" "$collection" \
        --read-barrier-manifest-generation "$manifest" \
        --read-barrier-visible-seq-no "$visible" \
        >"$stdout_file" 2>"$stderr_file"
    then
        die "expected query with read barrier to fail after promotion"
    fi
}

run_stats_with_barrier_expect_failure() {
    local node="$1"
    local collection="$2"
    local manifest="$3"
    local visible="$4"
    local stdout_file="$5"
    local stderr_file="$6"
    if stats_json "$node" "$collection" \
        --read-barrier-manifest-generation "$manifest" \
        --read-barrier-visible-seq-no "$visible" \
        >"$stdout_file" 2>"$stderr_file"
    then
        die "expected stats with read barrier to fail after promotion"
    fi
}

scenario_smoke() {
    bootstrap_cluster "${nodes[@]}"
    wait_for_membership_count node-a 3
    wait_for_local_leader node-a node-a
    wait_for_follower_of_leader node-b node-a
    wait_for_follower_of_leader node-c node-a
    local collection="smoke"
    local batch_dir
    batch_dir="$(cluster_state_dir)/tmp"
    ensure_dir "$batch_dir"
    local input_file output_file
    input_file="${batch_dir}/smoke.jsonl"
    output_file="${batch_dir}/smoke-write.json"
    write_batch_file "$input_file" "smoke" 2 1.0 0.0
    create_collection node-a "$collection"
    run_write node-a "$collection" "$input_file" "$output_file"
    local manifest visible stats_payload
    manifest="$(extract_snapshot_arg "$output_file" manifest_generation)"
    visible="$(extract_snapshot_arg "$output_file" visible_seq_no)"
    stats_payload="$(run_stats_with_barrier node-a "$collection" "$manifest" "$visible")"
    assert_live_record_count "$stats_payload" 2
}

scenario_new_node_registration() {
    bootstrap_cluster node-a node-b
    wait_for_membership_count node-a 2
    wait_for_local_leader node-a node-a
    wait_for_follower_of_leader node-b node-a
    start_node node-c
    wait_for_membership_count node-a 3
    wait_for_local_leader node-a node-a
    wait_for_member_present node-a node-c
    wait_for_member_present node-b node-c
}

scenario_concurrent_writers() {
    bootstrap_cluster "${nodes[@]}"
    local collection="writers"
    local tmp_dir
    tmp_dir="$(cluster_state_dir)/tmp"
    ensure_dir "$tmp_dir"
    local input_a input_b out_a out_b
    input_a="${tmp_dir}/writers-a.jsonl"
    input_b="${tmp_dir}/writers-b.jsonl"
    out_a="${tmp_dir}/writers-a.json"
    out_b="${tmp_dir}/writers-b.json"
    write_batch_file "$input_a" "writer-a" 20 1.0 0.0
    write_batch_file "$input_b" "writer-b" 20 0.0 1.0
    create_collection node-a "$collection"
    run_write node-a "$collection" "$input_a" "$out_a" &
    local pid_a=$!
    run_write node-b "$collection" "$input_b" "$out_b" &
    local pid_b
    pid_b=$!
    local success_count=0
    wait "$pid_a" && success_count=$((success_count + 1)) || true
    wait "$pid_b" && success_count=$((success_count + 1)) || true
    [[ "$success_count" -gt 0 ]] || die "expected at least one concurrent writer to succeed"
    local owner stats_payload
    owner="$(owner_node node-a "$collection")"
    stats_payload="$(stats_json "$owner" "$collection")"
    assert_live_record_count "$stats_payload" $((20 * success_count))
}

scenario_owner_failover() {
    bootstrap_cluster "${nodes[@]}"
    local collection="owner-failover"
    local tmp_dir
    tmp_dir="$(cluster_state_dir)/tmp"
    ensure_dir "$tmp_dir"
    local seed_input seed_output flood_input flood_output flood_err
    seed_input="${tmp_dir}/owner-seed.jsonl"
    seed_output="${tmp_dir}/owner-seed.json"
    flood_input="${tmp_dir}/owner-flood.jsonl"
    flood_output="${tmp_dir}/owner-flood.json"
    flood_err="${tmp_dir}/owner-flood.err"
    write_batch_file "$seed_input" "seed" 1 1.0 0.0
    write_batch_file "$flood_input" "flood" 5000 0.5 0.5
    create_collection node-a "$collection"
    run_write node-a "$collection" "$seed_input" "$seed_output"
    mirror_collection_root node-a "$collection" node-b
    run_write node-a "$collection" "$flood_input" "$flood_output" 2>"$flood_err" &
    local flood_pid=$!
    wait_for_pid_running "$flood_pid" "owner failover flood write"
    stop_node node-a
    if wait "$flood_pid"; then
        die "expected in-flight write to fail when stopping the owner"
    fi
    admin_json promote-shard-owner "$collection" node-b >/dev/null
    wait_for_owner node-b "$collection" node-b 2
    local manifest visible query_err stats_err query_out stats_out
    manifest="$(extract_snapshot_arg "$seed_output" manifest_generation)"
    visible="$(extract_snapshot_arg "$seed_output" visible_seq_no)"
    query_out="${tmp_dir}/owner-query.out"
    query_err="${tmp_dir}/owner-query.err"
    stats_out="${tmp_dir}/owner-stats.out"
    stats_err="${tmp_dir}/owner-stats.err"
    run_query_with_barrier_expect_failure node-b "$collection" "$manifest" "$visible" "$query_out" "$query_err"
    run_stats_with_barrier_expect_failure node-b "$collection" "$manifest" "$visible" "$stats_out" "$stats_err"
    assert_failure_contains "$query_err" "cannot safely satisfy read barriers after promotion"
    assert_failure_contains "$stats_err" "cannot safely satisfy read barriers after promotion"
}

scenario_leader_failover() {
    bootstrap_cluster "${nodes[@]}"
    local collection="leader-failover"
    local tmp_dir
    tmp_dir="$(cluster_state_dir)/tmp"
    ensure_dir "$tmp_dir"
    local seed_input seed_output
    seed_input="${tmp_dir}/leader-seed.jsonl"
    seed_output="${tmp_dir}/leader-seed.json"
    write_batch_file "$seed_input" "leader" 2 1.0 0.0
    create_collection node-a "$collection"
    run_write node-a "$collection" "$seed_input" "$seed_output"
    mirror_collection_root node-a "$collection" node-b
    stop_node node-a
    wait_for_member_absent node-b node-a
    wait_for_local_leader node-b node-b
    wait_for_follower_of_leader node-c node-b
    admin_json promote-shard-owner "$collection" node-b >/dev/null
    wait_for_owner node-b "$collection" node-b 2
    local post_input post_output
    post_input="${tmp_dir}/leader-post.jsonl"
    post_output="${tmp_dir}/leader-post.json"
    write_batch_file "$post_input" "post" 1 0.0 1.0
    run_write node-b "$collection" "$post_input" "$post_output"
}

scenario_lagging_node_rejoin() {
    bootstrap_cluster "${nodes[@]}"
    stop_node node-c
    wait_for_member_absent node-a node-c
    start_node_again node-c
    wait_for_member_present node-a node-c
    wait_for_membership_count node-a 3
}

scenario_etcd_outage() {
    bootstrap_cluster "${nodes[@]}"
    local tmp_dir create_err create_out
    tmp_dir="$(cluster_state_dir)/tmp"
    ensure_dir "$tmp_dir"
    create_out="${tmp_dir}/etcd-outage-create.out"
    create_err="${tmp_dir}/etcd-outage-create.err"
    stop_etcd_member etcd-2
    stop_etcd_member etcd-3
    wait_for_etcd_quorum_loss node-a
    if cli_json node-a collection create "etcd-outage" --dimensions 2 --metric dot >"$create_out" 2>"$create_err"; then
        die "expected collection creation to fail after etcd quorum loss"
    fi
    assert_failure_contains "$create_err" "etcd metadata operation failed"
}

scenario_partition_heal() {
    bootstrap_cluster "${nodes[@]}"
    disconnect_node node-c
    wait_for_member_absent node-a node-c
    reconnect_node node-c
    wait_for_member_present node-a node-c
}

run_named_scenario() {
    case "$1" in
        smoke) scenario_smoke ;;
        new-node-registration) scenario_new_node_registration ;;
        concurrent-writers) scenario_concurrent_writers ;;
        owner-failover) scenario_owner_failover ;;
        leader-failover) scenario_leader_failover ;;
        lagging-node-rejoin) scenario_lagging_node_rejoin ;;
        etcd-outage) scenario_etcd_outage ;;
        partition-heal) scenario_partition_heal ;;
        *) die "unknown scenario '$1'" ;;
    esac
}

self_test() {
    [[ "$(printf '%s\n' "${nodes[@]}" | wc -l | tr -d ' ')" == "3" ]] || die "expected three nodes"
    [[ "$(printf '%s\n' "${scenarios[@]}" | wc -l | tr -d ' ')" == "8" ]] || die "expected eight scenarios"
    [[ "$(etcd_host_client_port etcd-1)" == "12379" ]] || die "expected dedicated host port for etcd-1"
    [[ "$(etcd_host_endpoints_csv)" == "http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379" ]] || die "unexpected etcd host endpoint set"
    render_node_config node-a | grep -Fq 'backend = "etcd"' || die "rendered config missing etcd backend"
    render_node_config node-a | grep -Fq "cluster_name = $(toml_basic_string "$cluster_name")" || die "rendered config missing cluster name"
}

status_command() {
    if (($# == 0)); then
        for node in "${nodes[@]}"; do
            printf '== %s ==\n' "$node"
            status_json "$node"
        done
        return
    fi
    if (($# > 1)); then
        die "status accepts at most one collection argument"
    fi
    local collection="$1"
    for node in "${nodes[@]}"; do
        printf '== %s ==\n' "$node"
        status_json "$node"
        printf '== %s placement ==\n' "$node"
        placement_json "$node" "$collection"
    done
}

command="${1:-help}"
shift || true

case "$command" in
    help)
        usage
        ;;
    list-nodes)
        printf '%s\n' "${nodes[@]}"
        ;;
    list-scenarios)
        printf '%s\n' "${scenarios[@]}"
        ;;
    render-config)
        node="${1:?render-config requires a node name}"
        shift
        parse_cluster_flag "$@"
        reject_remaining_args
        render_node_config "$node"
        ;;
    bootstrap)
        parse_cluster_flag "$@"
        reject_remaining_args
        bootstrap_cluster "${nodes[@]}"
        ;;
    teardown)
        parse_cluster_flag "$@"
        reject_remaining_args
        teardown_cluster
        ;;
    reset)
        parse_cluster_flag "$@"
        reject_remaining_args
        reset_cluster
        ;;
    status)
        parse_cluster_flag "$@"
        status_command "${remaining_args[@]}"
        ;;
    self-test)
        self_test
        ;;
    scenario)
        scenario_name="${1:?scenario requires a name}"
        shift
        parse_cluster_flag "$@"
        reject_remaining_args
        run_named_scenario "$scenario_name"
        ;;
    *)
        usage
        die "unknown command '$command'"
        ;;
esac
