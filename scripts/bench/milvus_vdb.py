#!/usr/bin/env python3
"""Milvus driver for the VectorDBBench-style comparison.

Reads a dataset directory prepared by `logpose-bench vdb-prepare`, loads it into
a running Milvus standalone server with an HNSW index, and runs the same cases
as `logpose-bench vdb-run`: for each case it sweeps `ef` until mean recall@k
reaches the target, then measures throughput and latency at that `ef` with
several concurrent clients. It writes the same report shape
(`logpose-vdb-report/1`), scored against the same ground truth files.

Requires `pymilvus` and `numpy`.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
from pymilvus import DataType, MilvusClient

REPORT_SCHEMA = "logpose-vdb-report/1"
VECTOR_FIELD = "embedding"
PK_FIELD = "id"


def log(message: str) -> None:
    print(f"[milvus-vdb] {message}", file=sys.stderr, flush=True)


# Dataset files -------------------------------------------------------------


def read_vecs(path: Path, dtype: str) -> np.ndarray:
    """Read an .fvecs or .ivecs file into a 2-D array."""
    raw = np.fromfile(path, dtype="<i4")
    if raw.size == 0:
        raise ValueError(f"{path} is empty")
    dims = int(raw[0])
    rows = raw.reshape(-1, dims + 1)
    if not np.all(rows[:, 0] == dims):
        raise ValueError(f"{path} has rows of different lengths")
    return rows[:, 1:].copy().view(dtype)


class Dataset:
    """A prepared dataset directory."""

    def __init__(self, directory: Path) -> None:
        self.dir = directory
        self.manifest = json.loads((directory / "dataset.json").read_text())
        if self.manifest["format"] != 1:
            raise ValueError(f"unsupported dataset format {self.manifest['format']}")
        self.k = int(self.manifest["k"])
        self.metric = self.manifest["metric"]
        self.scalar_field = self.manifest["scalar_field"]
        self.queries = read_vecs(directory / self.manifest["queries_file"], "<f4")
        self.truths = []
        for case in self.manifest["cases"]:
            ids = read_vecs(directory / case["ground_truth_file"], "<i4")
            self.truths.append([[int(i) for i in row if i >= 0] for row in ids])

    def base(self) -> np.ndarray:
        return read_vecs(self.dir / self.manifest["base_file"], "<f4")

    def ranks(self) -> np.ndarray:
        return np.fromfile(self.dir / self.manifest["scalar_file"], dtype="<i8")


def recall_at_k(truth: list[int], returned: list[int], k: int) -> float:
    """Same definition as the Rust harness: |truth[:k] & returned[:k]| / len(truth[:k])."""
    truth = truth[:k]
    if not truth:
        return 1.0
    got = set(returned[:k])
    return sum(1 for i in truth if i in got) / len(truth)


def latency_summary(seconds: list[float]) -> dict:
    """Nearest-rank percentiles in milliseconds, like the Rust harness."""
    if not seconds:
        return {"count": 0, "mean_ms": 0.0, "min_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}
    millis = sorted(s * 1000.0 for s in seconds)

    def pct(p: float) -> float:
        rank = int(np.ceil(p / 100.0 * len(millis)))
        return millis[min(max(rank, 1), len(millis)) - 1]

    return {
        "count": len(millis),
        "mean_ms": sum(millis) / len(millis),
        "min_ms": millis[0],
        "p50_ms": pct(50),
        "p95_ms": pct(95),
        "p99_ms": pct(99),
        "max_ms": millis[-1],
    }


def case_filter(dataset: Dataset, case: dict) -> str:
    limit = case.get("filter_lt")
    return "" if limit is None else f"{dataset.scalar_field} < {int(limit)}"


def metric_type(metric: str) -> str:
    return {"cosine": "COSINE", "dot": "IP", "l2": "L2"}[metric]


# Machine information ------------------------------------------------------


def machine_info() -> dict:
    def read(path: str) -> str | None:
        try:
            return Path(path).read_text()
        except OSError:
            return None

    cpu_model = None
    for line in (read("/proc/cpuinfo") or "").splitlines():
        key, _, value = line.partition(":")
        if key.strip() == "model name":
            cpu_model = value.strip()
            break
    total_memory = None
    for line in (read("/proc/meminfo") or "").splitlines():
        if line.startswith("MemTotal:"):
            total_memory = int(line.split()[1]) * 1024
            break

    def git(*args: str) -> str | None:
        try:
            out = subprocess.run(["git", *args], capture_output=True, text=True, check=True)
        except (OSError, subprocess.CalledProcessError):
            return None
        return out.stdout.strip()

    status = git("status", "--porcelain", "--untracked-files=no")
    return {
        "os": platform.system().lower(),
        "arch": platform.machine(),
        "kernel": (read("/proc/sys/kernel/osrelease") or "").strip() or None,
        "cpu_model": cpu_model,
        "logical_cpus": os.cpu_count(),
        "total_memory_bytes": total_memory,
        "git_commit": git("rev-parse", "HEAD"),
        "git_dirty": None if status is None else bool(status),
    }


# Load ---------------------------------------------------------------------


def load(client: MilvusClient, args: argparse.Namespace, dataset: Dataset) -> dict:
    name = args.collection
    if client.has_collection(name):
        log(f"dropping existing collection {name}")
        client.drop_collection(name)
    dims = int(dataset.manifest["dims"])
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(PK_FIELD, DataType.INT64, is_primary=True)
    schema.add_field(VECTOR_FIELD, DataType.FLOAT_VECTOR, dim=dims)
    schema.add_field(dataset.scalar_field, DataType.INT64)
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name=VECTOR_FIELD,
        index_type="HNSW",
        metric_type=metric_type(dataset.metric),
        params={"M": args.hnsw_m, "efConstruction": args.hnsw_ef_construction},
    )
    index_params.add_index(field_name=dataset.scalar_field, index_type=args.scalar_index)
    # Creating the collection with its indexes also loads it, as VectorDBBench does
    # before inserting.
    client.create_collection(name, schema=schema, index_params=index_params)

    base = dataset.base()
    ranks = dataset.ranks()
    n = base.shape[0]
    log(f"inserting {n} rows in batches of {args.batch_size}")
    started = time.monotonic()
    for start in range(0, n, args.batch_size):
        end = min(start + args.batch_size, n)
        rows = [
            {PK_FIELD: i, VECTOR_FIELD: base[i].tolist(), dataset.scalar_field: int(ranks[i])}
            for i in range(start, end)
        ]
        client.insert(name, rows)
    insert_seconds = time.monotonic() - started

    log("flushing, compacting, and waiting for the index")
    started = time.monotonic()
    client.flush(name)
    job = client.compact(name)
    while client.get_compaction_state(job) != "Completed":
        time.sleep(0.5)
    deadline = time.monotonic() + args.index_timeout
    while True:
        info = client.describe_index(name, VECTOR_FIELD)
        if int(info.get("pending_index_rows", 0)) == 0 and int(info.get("indexed_rows", 0)) >= n:
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"index not built within {args.index_timeout} s: {info}")
        time.sleep(0.5)
    client.refresh_load(name)
    optimize_seconds = time.monotonic() - started
    log(f"loaded in {insert_seconds:.1f} s, optimized in {optimize_seconds:.1f} s")
    return {
        "rows": n,
        "batch_size": args.batch_size,
        "insert_seconds": insert_seconds,
        "optimize_seconds": optimize_seconds,
        "total_seconds": insert_seconds + optimize_seconds,
        "insert_rows_per_sec": n / max(insert_seconds, 1e-9),
    }


# Search -------------------------------------------------------------------


def search_ids(client: MilvusClient, args: argparse.Namespace, dataset: Dataset, query: np.ndarray, expr: str, ef: int) -> list[int]:
    result = client.search(
        args.collection,
        data=[query.tolist()],
        filter=expr,
        limit=dataset.k,
        anns_field=VECTOR_FIELD,
        output_fields=[],
        search_params={"metric_type": metric_type(dataset.metric), "params": {"ef": ef}},
    )
    return [int(hit["id"]) for hit in result[0]]


def throughput_worker(conn, args, case_index, clients, worker, ef, barrier):
    """One concurrent client: its own process and connection."""
    try:
        dataset = Dataset(Path(args.dataset))
        client = MilvusClient(uri=args.uri)
        case = dataset.manifest["cases"][case_index]
        expr = case_filter(dataset, case)
        truth = dataset.truths[case_index]
        queries = len(dataset.queries)
        # One untimed search so connection setup is not measured.
        search_ids(client, args, dataset, dataset.queries[worker % queries], expr, ef)
        barrier.wait()
        started = time.monotonic()
        deadline = started + args.duration
        latencies = []
        recall_sum = 0.0
        query = worker % queries
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            ids = search_ids(client, args, dataset, dataset.queries[query], expr, ef)
            latencies.append(time.monotonic() - t0)
            recall_sum += recall_at_k(truth[query], ids, dataset.k)
            query = (query + clients) % queries
        conn.send((started, time.monotonic(), latencies, recall_sum, None))
    except Exception as error:  # noqa: BLE001 - reported to the parent
        conn.send((0.0, 0.0, [], 0.0, repr(error)))
    finally:
        conn.close()


def throughput(args: argparse.Namespace, case_index: int, clients: int, ef: int) -> dict:
    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(clients)
    pipes = []
    processes = []
    for worker in range(clients):
        parent, child = ctx.Pipe(duplex=False)
        process = ctx.Process(target=throughput_worker, args=(child, args, case_index, clients, worker, ef, barrier))
        process.start()
        pipes.append(parent)
        processes.append(process)
    results = [pipe.recv() for pipe in pipes]
    for process in processes:
        process.join()
    errors = [r[4] for r in results if r[4]]
    if errors:
        raise RuntimeError(f"throughput worker failed: {errors[0]}")
    wall = max(r[1] for r in results) - min(r[0] for r in results)
    latencies = [s for r in results for s in r[2]]
    recall_sum = sum(r[3] for r in results)
    return {
        "clients": clients,
        "duration_seconds": wall,
        "queries": len(latencies),
        "qps": len(latencies) / max(wall, 1e-9),
        "latency": latency_summary(latencies),
        "recall": recall_sum / max(len(latencies), 1),
    }


def run_case(client: MilvusClient, args: argparse.Namespace, dataset: Dataset, index: int) -> dict:
    case = dataset.manifest["cases"][index]
    expr = case_filter(dataset, case)
    truth = dataset.truths[index]
    queries = dataset.queries
    for warm in range(min(args.warmup, len(queries))):
        search_ids(client, args, dataset, queries[warm], expr, args.ef[0])

    sweep = []
    for ef in args.ef:
        latencies = []
        recalls = []
        started = time.monotonic()
        for q, expected in enumerate(truth):
            t0 = time.monotonic()
            ids = search_ids(client, args, dataset, queries[q], expr, ef)
            latencies.append(time.monotonic() - t0)
            recalls.append(recall_at_k(expected, ids, dataset.k))
        wall = time.monotonic() - started
        point = {
            "ef": ef,
            "recall": float(np.mean(recalls)),
            "min_recall": float(min(recalls)),
            "queries": len(truth),
            "qps": len(truth) / max(wall, 1e-9),
            "latency": latency_summary(latencies),
        }
        log(f"{case['name']} ef {ef}: recall {point['recall']:.4f}, {point['qps']:.1f} qps, p99 {point['latency']['p99_ms']:.2f} ms")
        sweep.append(point)
        if point["recall"] >= args.target_recall:
            break

    met = [p for p in sweep if p["recall"] >= args.target_recall]
    chosen = met[0] if met else max(sweep, key=lambda p: p["recall"])
    concurrency = [throughput(args, index, clients, chosen["ef"]) for clients in args.concurrency]
    summary = ", ".join(f"{r['clients']} clients {r['qps']:.1f} qps" for r in concurrency)
    log(f"{case['name']}: ef {chosen['ef']} recall {chosen['recall']:.4f}; {summary}")
    return {
        "name": case["name"],
        "selectivity": case.get("selectivity"),
        "filter": expr or None,
        "matching_rows": case["matching_rows"],
        "sweep": sweep,
        "chosen_ef": chosen["ef"],
        "chosen_recall": chosen["recall"],
        "met_target": bool(met),
        "plan": None,
        "concurrency": concurrency,
    }


def server_stats(client: MilvusClient, args: argparse.Namespace, dataset: Dataset) -> dict:
    stats = {}
    try:
        stats["collection"] = client.get_collection_stats(args.collection)
        stats["vector_index"] = client.describe_index(args.collection, VECTOR_FIELD)
        stats["scalar_index"] = client.describe_index(args.collection, dataset.scalar_field)
    except Exception as error:  # noqa: BLE001 - statistics are best effort
        stats["error"] = repr(error)
    return json.loads(json.dumps(stats, default=str))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", required=True, help="prepared dataset directory")
    parser.add_argument("--uri", default="http://127.0.0.1:19530", help="Milvus endpoint")
    parser.add_argument("--collection", default="vdb_bench")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--hnsw-m", type=int, default=16)
    parser.add_argument("--hnsw-ef-construction", type=int, default=200)
    parser.add_argument("--scalar-index", default="STL_SORT", help="Milvus index type for the rank field")
    parser.add_argument("--ef", default="16,24,32,48,64,96,128,192,256,384,512,768,1024")
    parser.add_argument("--target-recall", type=float, default=0.95)
    parser.add_argument("--concurrency", default="1,4,8")
    parser.add_argument("--duration", type=float, default=20.0, help="seconds per throughput run")
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--index-timeout", type=float, default=3600.0)
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    args.ef = [int(v) for v in args.ef.split(",")]
    args.concurrency = [int(v) for v in args.concurrency.split(",")]
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    started_at = int(time.time())
    dataset = Dataset(Path(args.dataset))
    client = MilvusClient(uri=args.uri)
    version = client.get_server_version()
    log(f"connected to Milvus {version}")
    load_report = (
        {"rows": 0, "batch_size": args.batch_size, "insert_seconds": 0.0, "optimize_seconds": 0.0, "total_seconds": 0.0, "insert_rows_per_sec": 0.0}
        if args.skip_load
        else load(client, args, dataset)
    )
    stats = server_stats(client, args, dataset)
    cases = [run_case(client, args, dataset, index) for index in range(len(dataset.manifest["cases"]))]
    notes = [
        "recall is mean recall@k against exact ground truth computed by logpose-bench vdb-prepare",
        "latency is measured by the client around each pymilvus search call, so it includes Python serialization and loopback transport",
        "each concurrent client is its own Python process with its own connection, on the same machine as the server",
        f"searches use the collection's default consistency level; the scalar field has a {args.scalar_index} index",
    ]
    if dataset.manifest.get("synthetic"):
        notes.append("the dataset is synthetic (embedding-like, see dataset.generator); it mirrors only the shape of the public dataset named in dataset.mirrors")
    report = {
        "schema": REPORT_SCHEMA,
        "system": "milvus",
        "system_version": version,
        "endpoint": args.uri,
        "driver": "scripts/bench/milvus_vdb.py",
        "started_at_unix": started_at,
        "total_seconds": time.monotonic() - started,
        "machine": machine_info(),
        "dataset": dataset.manifest,
        "index": {
            "type": "hnsw",
            "m": args.hnsw_m,
            "ef_construction": args.hnsw_ef_construction,
            "quantization": "none (HNSW over f32)",
            "scalar_index": args.scalar_index,
        },
        "target_recall": args.target_recall,
        "load": load_report,
        "cases": cases,
        "server_stats": stats,
        "notes": notes,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"report written to {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
