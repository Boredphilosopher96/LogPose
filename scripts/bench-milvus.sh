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
# Environment:
#   LOGPOSE_BENCH_DATA   work directory outside the repository for datasets,
#                        server data, the Python venv, and raw reports
#                        (default: $HOME/.cache/logpose-bench)
#   MILVUS_IMAGE         Milvus image (default: milvusdb/milvus:v2.6.24)
#   SKIP_MILVUS=1        run only LogPose and write a LogPose-only summary;
#                        MILVUS_MISSING_REASON says why in the summary
#   BENCH_DURATION       seconds per throughput run (default: 20)
#   BENCH_CONCURRENCY    client counts (default: 1,4,8)
#   BENCH_TARGET_RECALL  recall the ef sweep looks for (default: 0.95)
#   HNSW_M, HNSW_EF_CONSTRUCTION   graph parameters for both systems (16, 200)
#   OUTPUT_DIR           where summaries go (default: benches/baselines)
#   PYTHON               Python with pymilvus and numpy; by default a venv is
#                        created under LOGPOSE_BENCH_DATA
#   CARGO_BUILD_JOBS     passed through to cargo
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data_root="${LOGPOSE_BENCH_DATA:-${HOME}/.cache/logpose-bench}"
milvus_image="${MILVUS_IMAGE:-milvusdb/milvus:v2.6.24}"
duration="${BENCH_DURATION:-20}"
concurrency="${BENCH_CONCURRENCY:-1,4,8}"
target_recall="${BENCH_TARGET_RECALL:-0.95}"
hnsw_m="${HNSW_M:-16}"
hnsw_ef_construction="${HNSW_EF_CONSTRUCTION:-200}"
output_dir="${OUTPUT_DIR:-${repo_root}/benches/baselines}"
grpc_port="${LOGPOSE_BENCH_GRPC_PORT:-15051}"
rest_port="${LOGPOSE_BENCH_REST_PORT:-18080}"
milvus_container="logpose-bench-milvus"

if [[ $# -eq 0 ]]; then
  set -- cohere-100k openai-50k
fi

log() {
  echo "[bench-milvus] $*" >&2
}

mkdir -p "${data_root}/datasets" "${data_root}/results"

target_dir="${CARGO_TARGET_DIR:-${repo_root}/target}"
log "building logpose-server and logpose-bench (release)"
(cd "${repo_root}" && cargo build --release -p logpose-server -p logpose-bench)
bench_bin="${target_dir}/release/logpose-bench"
server_bin="${target_dir}/release/logpose-server"

python_bin="${PYTHON:-}"
if [[ -z "${python_bin}" && "${SKIP_MILVUS:-0}" != "1" ]]; then
  venv="${data_root}/venv"
  if [[ ! -x "${venv}/bin/python" ]]; then
    log "creating Python venv with pymilvus and numpy in ${venv}"
    python3 -m venv "${venv}"
    "${venv}/bin/pip" install --quiet pymilvus numpy
  fi
  python_bin="${venv}/bin/python"
fi
summary_python="${python_bin:-python3}"

server_pid=""
cleanup() {
  if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" 2>/dev/null; then
    kill "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
  if command -v docker >/dev/null 2>&1; then
    docker rm -f "${milvus_container}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_port() {
  local port="$1"
  for _ in $(seq 1 120); do
    if (echo >"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  log "port ${port} did not open"
  return 1
}

rss_bytes() {
  local kib
  kib="$(awk '/^VmRSS:/ { print $2 }' "/proc/$1/status" 2>/dev/null || true)"
  echo "$(( ${kib:-0} * 1024 ))"
}

run_logpose() {
  local shape="$1" dataset="$2" report="$3"
  local storage="${data_root}/logpose-server-${shape}"
  rm -rf "${storage}"
  log "starting logpose-server for ${shape}"
  LOGPOSE_CONFIG="node_name = \"bench\"
rest_host = \"127.0.0.1\"
rest_port = ${rest_port}
grpc_host = \"127.0.0.1\"
grpc_port = ${grpc_port}
log_filter = \"warn\"
storage_root = \"${storage}\"

[index]
hnsw_m = ${hnsw_m}
hnsw_ef_construction = ${hnsw_ef_construction}" \
    "${server_bin}" >"${data_root}/results/${shape}-logpose-server.log" 2>&1 &
  server_pid=$!
  wait_for_port "${grpc_port}"
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
  logpose_rss="$(rss_bytes "${server_pid}")"
  logpose_disk="$(du -sb "${storage}" | cut -f1)"
  kill "${server_pid}"
  wait "${server_pid}" 2>/dev/null || true
  server_pid=""
  rm -rf "${storage}"
}

run_milvus() {
  local shape="$1" dataset="$2" report="$3"
  local volume="${data_root}/milvus-${shape}"
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
  docker rm -f "${milvus_container}" >/dev/null 2>&1 || true
  log "starting ${milvus_image} for ${shape}"
  # The same single-container setup as Milvus's standalone_embed.sh: embedded
  # etcd and local storage.
  docker run -d --name "${milvus_container}" \
    --security-opt seccomp:unconfined \
    -e ETCD_USE_EMBED=true \
    -e ETCD_DATA_DIR=/var/lib/milvus/etcd \
    -e ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
    -e COMMON_STORAGETYPE=local \
    -e DEPLOY_MODE=STANDALONE \
    -v "${volume}/volumes:/var/lib/milvus" \
    -v "${volume}/embedEtcd.yaml:/milvus/configs/embedEtcd.yaml" \
    -v "${volume}/user.yaml:/milvus/configs/user.yaml" \
    -p 127.0.0.1:19530:19530 -p 127.0.0.1:9091:9091 \
    "${milvus_image}" milvus run standalone >/dev/null
  for _ in $(seq 1 180); do
    if curl -sf http://127.0.0.1:9091/healthz >/dev/null; then
      break
    fi
    sleep 1
  done
  curl -sf http://127.0.0.1:9091/healthz >/dev/null
  milvus_loadavg="$(cut -d' ' -f1-3 /proc/loadavg)"
  "${python_bin}" "${repo_root}/scripts/bench/milvus_vdb.py" \
    --dataset "${dataset}" \
    --duration "${duration}" \
    --concurrency "${concurrency}" \
    --target-recall "${target_recall}" \
    --hnsw-m "${hnsw_m}" \
    --hnsw-ef-construction "${hnsw_ef_construction}" \
    --output "${report}"
  milvus_mem="$(docker stats --no-stream --format '{{.MemUsage}}' "${milvus_container}")"
  milvus_disk="$(du -sb "${volume}/volumes" | cut -f1)"
  docker rm -f "${milvus_container}" >/dev/null
  rm -rf "${volume}"
}

for shape in "$@"; do
  log "preparing ${shape}"
  dataset="$("${bench_bin}" vdb-prepare --shape "${shape}" --data-dir "${data_root}/datasets")"
  logpose_report="${data_root}/results/${shape}-logpose.json"
  milvus_report="${data_root}/results/${shape}-milvus.json"

  run_logpose "${shape}" "${dataset}" "${logpose_report}"
  summary_args=(
    --logpose "${logpose_report}"
    --resource "logpose_server_rss_bytes=${logpose_rss}"
    --resource "logpose_disk_bytes=${logpose_disk}"
    --resource "logpose_loadavg_at_start=${logpose_loadavg}"
  )
  if [[ "${SKIP_MILVUS:-0}" != "1" ]]; then
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
  "${summary_python}" "${repo_root}/scripts/bench/vdb_summary.py" "${summary_args[@]}" \
    --json "${output_dir}/phase5-milvus-${shape}.json" \
    --md "${output_dir}/phase5-milvus-${shape}.md"
  log "wrote ${output_dir}/phase5-milvus-${shape}.{json,md}"
done
