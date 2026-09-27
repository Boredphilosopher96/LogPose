# Benchmark Baselines

Committed reports from `logpose-bench` (`crates/logpose-bench`). Each file is the JSON report of one run. It records the machine, the harness and engine commit, the full configuration, and per-case results. Compare new engine work against the latest baseline at the same preset, seed, and filter style.

Numbers are only comparable on similar hardware. Each report's `machine` block says where it ran. Treat differences under about 10 percent as noise unless they repeat.

## phase0-current-engine.json

The pre-rewrite engine (`LocalStorageEngine` plus the `query_exact` planner), measured before Phase 1 of the engine v2 plan and after the legacy HNSW recall fix.

Reproduce it from the workspace root:

```bash
cargo run --release -p logpose-bench -- \
  --preset small --queries 100 --threads 4 \
  --label phase0-current-engine \
  --output benches/baselines/phase0-current-engine.json
```

The run uses 20,000 synthetic vectors of 128 dimensions in 32 Gaussian clusters, with seed 42 and the L2 metric. There are 100 held-out queries drawn from a quarter of the clusters, and k is 10. Rows are ingested in batches of 1,000, then flushed so searches use the HNSW path. Filters use equality flags at 0.1, 1, 10, 50, and 99 percent selectivity, both uncorrelated and anti-correlated. Every case also gets a 4-thread QPS pass, and 20 write-to-searchable probes run at the end.

The small preset uses 200 queries. This baseline uses 100 to keep the run short, because every query reloads collection state from disk.

The recorded run took 8.6 minutes on a 4-vCPU Xeon VM with 16 GB of RAM, on a clean tree at the commit in the report. It measures the engine after the legacy HNSW recall fix. The "Before the HNSW fix" column is the previous committed run of this baseline (commit `ddc3380`, same machine type, same flags), which measured the engine as of the WAL-batch and storage-durability changes on main (#49, #54). Headline results:

| Metric | Result | Before the HNSW fix |
| --- | --- | --- |
| Ingest | 2,990 rows/s (batch p50 99 ms, p99 4.8 s) | 3,256 rows/s (batch p50 125 ms, p99 3.7 s) |
| Flush (explicit, after waiting for automatic flushes) | 5.9 s, leaving 2 segments of 10,000 rows and 68 MB on disk | 6.1 s, 2 segments, 67 MB |
| Write-to-searchable p50 | 265 ms (always visible on the first search; the cost is one query) | 277 ms |
| Unfiltered search | 3.8 QPS, p50 255 ms, p99 310 ms, recall@10 1.000 | 3.2 QPS, p50 312 ms, p99 497 ms, recall@10 0.055 |
| Filtered search | 2.6 to 3.8 QPS, recall@10 between 0.992 and 1.000 at every selectivity, never short of k | 3.4 to 5.3 QPS, recall@10 between 0.00 and 0.06, selective filters short of k on every query |
| 4-thread QPS | 6.1 to 10.6 | 3.7 to 13.2 |
| Bytes read per query (`rchar`) | 50 MB, about 74 percent of the on-disk collection | 31 to 49 MB |
| Peak RSS | 260 MB, of which 15 MB was resident before the engine opened (dataset, oracle, harness) | 260 MB |

What the baseline shows:

- ANN recall now matches the oracle on clustered data. After the flush every query takes an HNSW plan (`vector_first_ann` or `cooperative_filtered_ann`) and returns the true neighbors; the one miss is a single anti-correlated 50 percent query at 0.8. The exact path agrees with the oracle exactly; the harness tests check this.
- Selective filters (0.1 and 1 percent) are the slowest cases, at about 2.6 QPS. Filtered ANN still post-filters and restarts with a doubled `ef` until k rows survive, and a connected graph now lets those restarts walk most of each segment. Before the fix they were faster only because the search was trapped in one cluster and gave up short of k.
- Every query reads most of the collection from disk, whatever the plan or filter.
- Extra client threads add little throughput.
- Range predicates (`--filter-style range`) always get a fixed 0.6 selectivity estimate from the planner, so selective range filters get `vector_first_ann` with post-filtering. That is why equality flags are the default style.

### Why ANN Recall Used To Collapse

Before the fix, the legacy HNSW graph in `crates/logpose-index/src/lib.rs` split clustered data into one island per cluster, so a search could only reach the cluster that held the segment's entry point. Recall@10 was 0.055 unfiltered and at most 0.06 filtered. Three construction choices caused it:

- Levels came from counting trailing zero bits of a hash, capped at 4. That gave each level half the nodes of the one below, instead of the usual 1/M, so the top layer held 1/16 of the segment. For a 10,000-row segment that is about 20 nodes per cluster, far more than M, so even the top layer had no edges between clusters.
- Neighbor selection kept the M closest candidates, with no diversity heuristic. Once a cluster had more than M members, pruning dropped every edge that left it.
- Layer 0 kept M = 8 neighbors instead of the usual 2M.

Evidence at one bench-shaped segment (6,700 rows of 128 dimensions, 32 clusters, spread 1.0, 60 candidates):

| Graph construction | Recall@10 | Nodes reachable from the entry point on layer 0 |
| --- | --- | --- |
| Old builder | 0.07 | 152 of 6,700 (one cluster averages 209) |
| Levels at 1/M only | 0.43 | 168 |
| Layer 0 at 2M only | 0.48 | 6,574 |
| Selection heuristic only | 0.44 | 5,386 |
| All three, M = 8 and `ef_construction` = 32 | 0.88 | 6,576 |

No single change reaches high recall. The fix applies all three: levels are drawn from the same deterministic hash but mapped to a geometric distribution with `mL = 1 / ln(M)`, layer 0 holds up to 2M neighbors, and both linking and pruning use the diversity heuristic with pruned connections kept (Malkov and Yashunin, Algorithm 4). The defaults also rose to M = 16, `ef_construction` = 128, and `ef_search` = 64. The sidecar version moved to 2, so sidecars written by the old builder are rejected. The HNSW v2 work in the engine v2 plan still replaces this index wholesale.

## Reading The Numbers

- `recall` compares each answer with a brute-force oracle that the harness computes itself, independent of the engine.
- `memory` is the harness process. For the in-process engine it includes the dataset and the oracle. Subtract `memory.before_target` to estimate what the engine added.
- `io_per_query.rchar` counts bytes returned by read syscalls, including page-cache hits. `read_bytes` counts only device reads, so it stays near zero when the data fits in the page cache.
- `plans` shows which physical plan the planner chose for each query.
