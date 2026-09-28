#!/usr/bin/env bash
# Reproduce the Phase 5 LogPose versus Milvus comparison end to end.
#
# For each dataset shape it:
#   1. builds logpose-server and logpose-bench in release mode
#   2. prepares the dataset and its exact ground truth (cached between runs)
#   3. starts a fresh logpose-server, loads the data over gRPC, runs every case,
#      and stops the server
#   4. starts a fresh Milvus standalone container, runs the same cases with
#      scripts/bench/milvus_vdb.py, and removes the container
#   5. writes benches/baselines/phase5-milvus-<shape>.json and .md
#
# The two systems never run at the same time. Usage:
#
#   scripts/bench-milvus.sh [shape ...]      # default: cohere-100k openai-50k
#
# Shapes: tiny, cohere-100k, openai-50k, cohere-1m, openai-500k.
#
# Every run holds an exclusive lock on its run directory
# (LOGPOSE_BENCH_DATA/runs/<run id>) and refuses to start a server on a port
# that is already in use, so two runs on one host need distinct run ids and
# ports (see benches/baselines/README.md for an A/B recipe). A run stops the
# logpose-server and Milvus container that a killed earlier run with the same
# run id left behind. Needs Linux (/proc) and flock from util-linux.
#
# Environment:
#   LOGPOSE_BENCH_DATA   work directory outside the repository for datasets,
#                        runs, and the Python venv
#                        (default: $HOME/.cache/logpose-bench)
#   LOGPOSE_BENCH_RUN_ID name of this run (letters, digits, '.', '_', '-';
#                        default: default). Server data, the Milvus volume,
#                        the binaries of this run, raw reports, and server
#                        logs go under LOGPOSE_BENCH_DATA/runs/<run id>; the
#                        Milvus container is logpose-bench-milvus-<run id>
#   LOGPOSE_BENCH_GRPC_PORT, LOGPOSE_BENCH_REST_PORT
#                        logpose-server ports on 127.0.0.1 (15051, 18080)
#   LOGPOSE_BENCH_MILVUS_PORT, LOGPOSE_BENCH_MILVUS_HEALTH_PORT
#                        Milvus gRPC and health ports published on 127.0.0.1
#                        (19530, 9091)
#   MILVUS_IMAGE         Milvus image (default: milvusdb/milvus:v2.6.24)
#   SKIP_MILVUS=1        run only LogPose and write a LogPose-only summary;
#                        MILVUS_MISSING_REASON says why in the summary
#   BENCH_DURATION       seconds per throughput run (default: 20)
#   BENCH_CONCURRENCY    client counts (default: 1,4,8)
#   BENCH_TARGET_RECALL  recall the ef sweep looks for (default: 0.95)
#   HNSW_M, HNSW_EF_CONSTRUCTION   graph parameters for both systems (16, 200)
#   OUTPUT_DIR           where summaries go (default: benches/baselines)
#   PYTHON               Python with pymilvus and numpy; by default a venv is
#                        created under LOGPOSE_BENCH_DATA with the pymilvus
#                        version pinned below
#   CARGO_BUILD_JOBS     passed through to cargo
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data_root="${LOGPOSE_BENCH_DATA:-${HOME}/.cache/logpose-bench}"
run_id="${LOGPOSE_BENCH_RUN_ID:-default}"
milvus_image="${MILVUS_IMAGE:-milvusdb/milvus:v2.6.24}"
pymilvus_version="3.0.2"
duration="${BENCH_DURATION:-20}"
concurrency="${BENCH_CONCURRENCY:-1,4,8}"
target_recall="${BENCH_TARGET_RECALL:-0.95}"
hnsw_m="${HNSW_M:-16}"
hnsw_ef_construction="${HNSW_EF_CONSTRUCTION:-200}"
output_dir="${OUTPUT_DIR:-${repo_root}/benches/baselines}"
grpc_port="${LOGPOSE_BENCH_GRPC_PORT:-15051}"
rest_port="${LOGPOSE_BENCH_REST_PORT:-18080}"
milvus_port="${LOGPOSE_BENCH_MILVUS_PORT:-19530}"
milvus_health_port="${LOGPOSE_BENCH_MILVUS_HEALTH_PORT:-9091}"
skip_milvus="${SKIP_MILVUS:-0}"

if [[ $# -eq 0 ]]; then
  set -- cohere-100k openai-50k
fi

log() {
  echo "[bench-milvus] $*" >&2
}

die() {
  log "error: $*"
  exit 1
}

if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  die "LOGPOSE_BENCH_RUN_ID '${run_id}' must start with a letter or digit and contain only letters, digits, '.', '_', and '-'"
fi
for port_var in grpc_port rest_port milvus_port milvus_health_port; do
  if [[ ! "${!port_var}" =~ ^[1-9][0-9]{0,4}$ ]] || ((${!port_var} > 65535)); then
    die "${port_var} '${!port_var}' is not a TCP port"
  fi
done
used_ports=("${grpc_port}" "${rest_port}")
if [[ "${skip_milvus}" != "1" ]]; then
  used_ports+=("${milvus_port}" "${milvus_health_port}")
fi
if [[ "$(printf '%s\n' "${used_ports[@]}" | sort -u | wc -l)" -ne "${#used_ports[@]}" ]]; then
  die "the ports of this run must differ: ${used_ports[*]}"
fi

run_dir="${data_root}/runs/${run_id}"
results_dir="${run_dir}/results"
milvus_container="logpose-bench-milvus-${run_id}"
server_pid_file="${run_dir}/server.pid"
mkdir -p "${data_root}/datasets" "${results_dir}"

command -v flock >/dev/null 2>&1 || die "flock (from util-linux) is required"

# One run per run directory. The lock is released when this shell exits; the
# long-lived processes it starts (cargo, logpose-server) do not inherit it, so
# a run killed with SIGKILL leaves the lock held at most until its short-lived
# bench clients finish.
lock_file="${run_dir}/lock"
exec {lock_fd}<>"${lock_file}"
if ! flock -n "${lock_fd}"; then
  die "run '${run_id}' is already in progress in ${run_dir} (started by pid $(cat "${lock_file}" 2>/dev/null || echo unknown)); set LOGPOSE_BENCH_RUN_ID and distinct ports to run another benchmark at the same time"
fi
echo "$$" >"${lock_file}"

# Removes the Milvus container of this run, if there is one. The container
# carries the run directory as a label, so a container of the same name that
# a run with another LOGPOSE_BENCH_DATA started is left alone (and fails).
remove_milvus_container() {
  local owner
  if ! owner="$(docker container inspect -f '{{index .Config.Labels "logpose.bench.run-dir"}}' "${milvus_container}" 2>/dev/null)"; then
    return 0
  fi
  if [[ "${owner}" != "${run_dir}" ]]; then
    log "container ${milvus_container} belongs to run directory '${owner}'; not removing it"
    return 1
  fi
  docker rm -f "${milvus_container}" >/dev/null
}

server_pid=""
milvus_started=0
cleanup() {
  if [[ -n "${server_pid}" ]]; then
    kill "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
    rm -f "${server_pid_file}"
  fi
  if [[ "${milvus_started}" == "1" ]]; then
    remove_milvus_container || true
  fi
}
trap cleanup EXIT
# Exit on a signal too, so the EXIT trap stops the server and removes the container.
trap 'exit 130' INT
trap 'exit 143' TERM

# Prints the socket inodes listening on a TCP port, on any local address.
listener_inodes() {
  local port_hex
  port_hex="$(printf '%04X' "$1")"
  awk -v port="${port_hex}" \
    'FNR > 1 && $4 == "0A" && substr($2, length($2) - 3) == port { print $10 }' \
    /proc/net/tcp /proc/net/tcp6 2>/dev/null || true
}

port_in_use() {
  local port="$1"
  if [[ -n "$(listener_inodes "${port}")" ]]; then
    return 0
  fi
  (echo >"/dev/tcp/127.0.0.1/${port}") 2>/dev/null
}

require_free_ports() {
  local port
  for port in "$@"; do
    if port_in_use "${port}"; then
      die "port ${port} is already in use (another benchmark run or server?); pick free ports with LOGPOSE_BENCH_GRPC_PORT, LOGPOSE_BENCH_REST_PORT, LOGPOSE_BENCH_MILVUS_PORT, and LOGPOSE_BENCH_MILVUS_HEALTH_PORT"
    fi
  done
}

# Succeeds when process $1 holds a socket listening on port $2.
pid_owns_listener() {
  local pid="$1" port="$2" inode fd
  for inode in $(listener_inodes "${port}"); do
    for fd in "/proc/${pid}/fd/"*; do
      if [[ "$(readlink "${fd}" 2>/dev/null)" == "socket:[${inode}]" ]]; then
        return 0
      fi
    done
  done
  return 1
}

# Waits until the server process listens on every port given after the pid and
# log file. Fails as soon as the process exits, and fails if another process
# owns one of the ports.
wait_for_server() {
  local pid="$1" server_log="$2" port
  shift 2
  for port in "$@"; do
    for _ in $(seq 1 120); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        tail -n 20 "${server_log}" >&2 || true
        die "logpose-server (pid ${pid}) exited before listening on port ${port}; log: ${server_log}"
      fi
      if (echo >"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
        break
      fi
      sleep 0.5
    done
    if ! kill -0 "${pid}" 2>/dev/null; then
      tail -n 20 "${server_log}" >&2 || true
      die "logpose-server (pid ${pid}) exited; log: ${server_log}"
    fi
    if ! (echo >"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
      die "port ${port} did not open; log: ${server_log}"
    fi
    if [[ -r /proc/net/tcp ]]; then
      if ! pid_owns_listener "${pid}" "${port}"; then
        die "port ${port} is served by a process other than logpose-server (pid ${pid})"
      fi
    else
      log "cannot read /proc/net/tcp; not checking which process listens on port ${port}"
    fi
  done
}

rss_bytes() {
  local kib
  kib="$(awk '/^VmRSS:/ { print $2 }' "/proc/$1/status" 2>/dev/null || true)"
  echo "$(( ${kib:-0} * 1024 ))"
}

# This run holds the lock, so a logpose-server or Milvus container of this run
# directory that is still up was left behind by an earlier run that was killed
# (SIGKILL) before its cleanup ran. Stop them so they do not hold the ports.
if [[ -f "${server_pid_file}" ]]; then
  stale_pid="$(cat "${server_pid_file}")"
  stale_exe="$(readlink "/proc/${stale_pid}/exe" 2>/dev/null || true)"
  if [[ "${stale_pid}" =~ ^[0-9]+$ && "${stale_exe% (deleted)}" == "${run_dir}/bin/logpose-server" ]]; then
    log "stopping logpose-server (pid ${stale_pid}) left running by an earlier run '${run_id}'"
    kill "${stale_pid}" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "${stale_pid}" 2>/dev/null || break
      sleep 0.5
    done
    kill -9 "${stale_pid}" 2>/dev/null || true
  fi
  rm -f "${server_pid_file}"
fi
if [[ "${skip_milvus}" != "1" ]]; then
  remove_milvus_container || die "the Milvus container name ${milvus_container} is taken by a run in another LOGPOSE_BENCH_DATA; use another LOGPOSE_BENCH_RUN_ID"
fi

# Fail before the build when the ports are taken; each server start checks again.
require_free_ports "${used_ports[@]}"

target_dir="${CARGO_TARGET_DIR:-${repo_root}/target}"
# cargo resolves a relative CARGO_TARGET_DIR against the directory it runs in.
if [[ "${target_dir}" != /* ]]; then
  target_dir="${repo_root}/${target_dir}"
fi
log "building logpose-server and logpose-bench (release)"
(cd "${repo_root}" && cargo build --release -p logpose-server -p logpose-bench {lock_fd}>&-)
# Run private copies, so a build in another checkout that shares the target
# directory cannot swap the binaries between shapes.
mkdir -p "${run_dir}/bin"
rm -f "${run_dir}/bin/logpose-bench" "${run_dir}/bin/logpose-server"
cp "${target_dir}/release/logpose-bench" "${target_dir}/release/logpose-server" "${run_dir}/bin/"
bench_bin="${run_dir}/bin/logpose-bench"
server_bin="${run_dir}/bin/logpose-server"

python_bin="${PYTHON:-}"
if [[ -z "${python_bin}" && "${skip_milvus}" != "1" ]]; then
  venv="${data_root}/venv"
  # Runs share the venv; set it up one at a time.
  exec {venv_lock_fd}<>"${data_root}/venv.lock"
  flock "${venv_lock_fd}"
  if [[ ! -x "${venv}/bin/python" ]]; then
    log "creating Python venv in ${venv}"
    python3 -m venv "${venv}"
  fi
  # A no-op when the pinned version is already installed.
  "${venv}/bin/pip" install --quiet "pymilvus==${pymilvus_version}" numpy
  exec {venv_lock_fd}<&-
  python_bin="${venv}/bin/python"
fi
summary_python="${python_bin:-python3}"

run_logpose() {
  local shape="$1" dataset="$2" report="$3"
  local storage="${run_dir}/logpose-server-${shape}"
  local server_log="${results_dir}/${shape}-logpose-server.log"
  require_free_ports "${grpc_port}" "${rest_port}"
  rm -rf "${storage}"
  # The path goes into a TOML basic string.
  local storage_toml="${storage//\\/\\\\}"
  storage_toml="${storage_toml//\"/\\\"}"
  log "starting logpose-server for ${shape} (gRPC ${grpc_port}, REST ${rest_port}, data ${storage})"
  LOGPOSE_CONFIG="node_name = \"bench\"
rest_host = \"127.0.0.1\"
rest_port = ${rest_port}
grpc_host = \"127.0.0.1\"
grpc_port = ${grpc_port}
log_filter = \"warn\"
storage_root = \"${storage_toml}\"

[index]
hnsw_m = ${hnsw_m}
hnsw_ef_construction = ${hnsw_ef_construction}" \
    "${server_bin}" >"${server_log}" 2>&1 {lock_fd}>&- &
  server_pid=$!
  echo "${server_pid}" >"${server_pid_file}"
  wait_for_server "${server_pid}" "${server_log}" "${grpc_port}" "${rest_port}"
  logpose_loadavg="$(cut -d' ' -f1-3 /proc/loadavg)"
  "${bench_bin}" vdb-run \
    --dataset "${dataset}" \
    --endpoint "http://127.0.0.1:${grpc_port}" \
    --duration "${duration}" \
    --concurrency "${concurrency}" \
    --target-recall "${target_recall}" \
    --hnsw-m "${hnsw_m}" \
    --hnsw-ef-construction "${hnsw_ef_construction}" \
    --output "${report}"
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    die "logpose-server (pid ${server_pid}) exited during the run; log: ${server_log}"
  fi
  logpose_rss="$(rss_bytes "${server_pid}")"
  logpose_disk="$(du -sb "${storage}" | cut -f1)"
  kill "${server_pid}"
  wait "${server_pid}" 2>/dev/null || true
  server_pid=""
  rm -f "${server_pid_file}"
  rm -rf "${storage}"
}

run_milvus() {
  local shape="$1" dataset="$2" report="$3" published
  local volume="${run_dir}/milvus-${shape}"
  remove_milvus_container || die "the Milvus container name ${milvus_container} is taken by a run in another LOGPOSE_BENCH_DATA; use another LOGPOSE_BENCH_RUN_ID"
  require_free_ports "${milvus_port}" "${milvus_health_port}"
  rm -rf "${volume}"
  mkdir -p "${volume}/volumes"
  cat >"${volume}/embedEtcd.yaml" <<'EOF'
listen-client-urls: http://0.0.0.0:2379
advertise-client-urls: http://0.0.0.0:2379
quota-backend-bytes: 4294967296
auto-compaction-mode: revision
auto-compaction-retention: '1000'
EOF
  echo "# no overrides" >"${volume}/user.yaml"
  log "starting ${milvus_image} for ${shape} as ${milvus_container} (port ${milvus_port}, health ${milvus_health_port})"
  milvus_started=1
  # The same single-container setup as Milvus's standalone_embed.sh: embedded
  # etcd and local storage.
  docker run -d --name "${milvus_container}" \
    --label "logpose.bench.run-dir=${run_dir}" \
    --security-opt seccomp:unconfined \
    -e ETCD_USE_EMBED=true \
    -e ETCD_DATA_DIR=/var/lib/milvus/etcd \
    -e ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
    -e COMMON_STORAGETYPE=local \
    -e DEPLOY_MODE=STANDALONE \
    -v "${volume}/volumes:/var/lib/milvus" \
    -v "${volume}/embedEtcd.yaml:/milvus/configs/embedEtcd.yaml" \
    -v "${volume}/user.yaml:/milvus/configs/user.yaml" \
    -p "127.0.0.1:${milvus_port}:19530" -p "127.0.0.1:${milvus_health_port}:9091" \
    "${milvus_image}" milvus run standalone >/dev/null
  for _ in $(seq 1 180); do
    if [[ "$(docker inspect -f '{{.State.Running}}' "${milvus_container}" 2>/dev/null)" != "true" ]]; then
      docker logs --tail 20 "${milvus_container}" >&2 || true
      die "Milvus container ${milvus_container} stopped before it became healthy"
    fi
    if curl -sf "http://127.0.0.1:${milvus_health_port}/healthz" >/dev/null; then
      break
    fi
    sleep 1
  done
  curl -sf "http://127.0.0.1:${milvus_health_port}/healthz" >/dev/null ||
    die "Milvus did not become healthy on port ${milvus_health_port}"
  published="$(docker port "${milvus_container}" 19530/tcp)"
  if ! grep -qx "127.0.0.1:${milvus_port}" <<<"${published}"; then
    die "port ${milvus_port} is not published by ${milvus_container}"
  fi
  milvus_loadavg="$(cut -d' ' -f1-3 /proc/loadavg)"
  "${python_bin}" "${repo_root}/scripts/bench/milvus_vdb.py" \
    --dataset "${dataset}" \
    --uri "http://127.0.0.1:${milvus_port}" \
    --duration "${duration}" \
    --concurrency "${concurrency}" \
    --target-recall "${target_recall}" \
    --hnsw-m "${hnsw_m}" \
    --hnsw-ef-construction "${hnsw_ef_construction}" \
    --output "${report}"
  milvus_mem="$(docker stats --no-stream --format '{{.MemUsage}}' "${milvus_container}")"
  milvus_disk="$(du -sb "${volume}/volumes" | cut -f1)"
  remove_milvus_container
  milvus_started=0
  rm -rf "${volume}"
}

log "run '${run_id}' in ${run_dir}"
for shape in "$@"; do
  log "preparing ${shape}"
  # Runs share the dataset cache; prepare one shape at a time.
  dataset="$(flock "${data_root}/datasets/.prepare.lock" \
    "${bench_bin}" vdb-prepare --shape "${shape}" --data-dir "${data_root}/datasets")"
  logpose_report="${results_dir}/${shape}-logpose.json"
  milvus_report="${results_dir}/${shape}-milvus.json"

  run_logpose "${shape}" "${dataset}" "${logpose_report}"
  summary_args=(
    --logpose "${logpose_report}"
    --resource "logpose_server_rss_bytes=${logpose_rss}"
    --resource "logpose_disk_bytes=${logpose_disk}"
    --resource "logpose_loadavg_at_start=${logpose_loadavg}"
  )
  if [[ "${skip_milvus}" != "1" ]]; then
    run_milvus "${shape}" "${dataset}" "${milvus_report}"
    summary_args+=(
      --milvus "${milvus_report}"
      --resource "milvus_container_memory=${milvus_mem}"
      --resource "milvus_disk_bytes=${milvus_disk}"
      --resource "milvus_loadavg_at_start=${milvus_loadavg}"
      --resource "milvus_image=${milvus_image}"
    )
  else
    summary_args+=(--milvus-missing-reason "${MILVUS_MISSING_REASON:-Milvus was skipped (SKIP_MILVUS=1).}")
  fi
  mkdir -p "${output_dir}"
  "${summary_python}" "${repo_root}/scripts/bench/vdb_summary.py" "${summary_args[@]}" \
    --json "${output_dir}/phase5-milvus-${shape}.json" \
    --md "${output_dir}/phase5-milvus-${shape}.md"
  log "wrote ${output_dir}/phase5-milvus-${shape}.{json,md}"
done
