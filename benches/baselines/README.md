# Benchmark Baselines

Committed reports from `logpose-bench` (`crates/logpose-bench`). Each file is the JSON report of one run. It records the machine, the harness and engine commit, the full configuration, and per-case results. Compare new engine work against the latest baseline at the same preset, seed, and filter style.

Numbers are only comparable on similar hardware. Each report's `machine` block says where it ran. Treat differences under about 10 percent as noise unless they repeat.

## phase0-current-engine.json

The pre-rewrite engine (`LocalStorageEngine` plus the `query_exact` planner), measured before Phase 1 of the engine v2 plan.

Reproduce it from the workspace root:

```bash
cargo run --release -p logpose-bench -- \
  --preset small --queries 100 --threads 4 \
  --label phase0-current-engine \
  --output benches/baselines/phase0-current-engine.json
```

The run uses 20,000 synthetic vectors of 128 dimensions in 32 Gaussian clusters, with seed 42 and the L2 metric. There are 100 held-out queries drawn from a quarter of the clusters, and k is 10. Rows are ingested in batches of 1,000, then flushed so searches use the HNSW path. Filters use equality flags at 0.1, 1, 10, 50, and 99 percent selectivity, both uncorrelated and anti-correlated. Every case also gets a 4-thread QPS pass, and 20 write-to-searchable probes run at the end.

The small preset uses 200 queries. This baseline uses 100 to keep the run short, because every query reloads collection state from disk.

The recorded run took 8.6 minutes on a 4-vCPU Xeon VM with 16 GB of RAM, on a clean tree at the commit in the report. It measures the engine as of the WAL-batch and storage-durability changes on main (#49, #54). Headline results:

| Metric | Result |
| --- | --- |
| Ingest | 3,256 rows/s (batch p50 125 ms, p99 3.7 s) |
| Flush (explicit, after waiting for automatic flushes) | 6.1 s, leaving 2 segments of 10,000 rows and 67 MB on disk |
| Write-to-searchable p50 | 277 ms (always visible on the first search; the cost is one query) |
| Unfiltered search | 3.2 QPS, p50 312 ms, p99 497 ms, recall@10 0.055 |
| Filtered search | 3.4 to 5.3 QPS, recall@10 between 0.00 and 0.06 at every selectivity |
| 4-thread QPS | 3.7 to 13.2; only selective filtered cases gain from extra clients |
| Bytes read per query (`rchar`) | 31 to 49 MB, about 45 to 75 percent of the on-disk collection |
| Peak RSS | 260 MB, of which 15 MB was resident before the engine opened (dataset, oracle, harness) |

What the baseline shows:

- ANN recall collapses on clustered data. After the flush every query takes an HNSW plan (`vector_first_ann` or `cooperative_filtered_ann`) and returns almost none of the true neighbors. Selective filters (0.1 and 1 percent) also return fewer than k rows for every query. The exact path agrees with the oracle exactly; the harness tests check this.
- Every query reads most of the collection from disk, whatever the plan or filter.
- Extra client threads add little throughput.
- Range predicates (`--filter-style range`) always get a fixed 0.6 selectivity estimate from the planner, so selective range filters get `vector_first_ann` with post-filtering. That is why equality flags are the default style.

### Why ANN Recall Collapses

The harness is not the cause. The same collapse reproduces without it, both with `build_hnsw_index` and `search_hnsw` called directly and end to end through `query_exact`. The legacy HNSW graph in `crates/logpose-index/src/lib.rs` splits clustered data into one island per cluster, so a search can only reach the cluster that holds the segment's entry point. Three construction choices cause it:

- Levels come from counting trailing zero bits of a hash, capped at 4 (`deterministic_level`, `MAX_HNSW_LEVEL`). That gives each level half the nodes of the one below, instead of the usual 1/M, so the top layer holds 1/16 of the segment. For a 10,000-row segment that is about 20 nodes per cluster, far more than M, so even the top layer has no edges between clusters.
- Neighbor selection keeps the M closest candidates, with no diversity heuristic (`select_best_neighbors`, `trimmed_neighbors`). Once a cluster has more than M members, pruning drops every edge that leaves it.
- Layer 0 keeps M = 8 neighbors (`HnswBuildParams::default`) instead of the usual 2M.

Evidence at one bench-shaped segment (6,700 rows of 128 dimensions, 32 clusters, spread 1.0, 60 candidates):

| Graph construction | Recall@10 | Nodes reachable from the entry point on layer 0 |
| --- | --- | --- |
| Current | 0.07 | 152 of 6,700 (one cluster averages 209) |
| Levels at 1/M only | 0.43 | 168 |
| Layer 0 at 2M only | 0.48 | 6,574 |
| Selection heuristic only | 0.44 | 5,386 |
| All three, M and `ef_construction` unchanged | 0.88 | 6,576 |

At 10,000 rows per segment, which is what this baseline has, the current graph drops to 0.03. End to end at 3,000 rows in 3 segments, the three changes raise engine recall from 0.49 to 1.00 unfiltered and from 0.40 to 1.00 filtered. So the multi-segment merge, the re-resolution of candidates to their latest visible version, and the rerank all preserve whatever the graph returns. No single contained change reaches high recall. The fix is standard HNSW construction, which the HNSW v2 work in the engine v2 plan replaces wholesale, so the legacy index is left as is.

## Reading The Numbers

- `recall` compares each answer with a brute-force oracle that the harness computes itself, independent of the engine.
- `memory` is the harness process. For the in-process engine it includes the dataset and the oracle. Subtract `memory.before_target` to estimate what the engine added.
- `io_per_query.rchar` counts bytes returned by read syscalls, including page-cache hits. `read_bytes` counts only device reads, so it stays near zero when the data fits in the page cache.
- `plans` shows which physical plan the planner chose for each query.
