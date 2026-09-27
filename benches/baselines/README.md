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

The small preset uses 200 queries. This baseline uses 100 to keep the run near 15 minutes, because every query reloads collection state from disk.

The recorded run took 16 minutes on a 4-vCPU Xeon VM with 16 GB of RAM, while other build jobs were running. Absolute latencies are therefore pessimistic by up to about 2x: an uncontended probe on the same VM measured about 200 ms per query instead of about 480 ms. Headline results:

| Metric | Result |
| --- | --- |
| Ingest | 202 rows/s (each operation fsyncs and every write replays the WAL delta) |
| Flush (explicit, after two automatic flushes) | 4.8 s, leaving 3 segments and 67 MB on disk |
| Write-to-searchable p50 | 480 ms (always visible on the first search; the cost is one query) |
| Unfiltered search | 2.1 QPS, p50 485 ms, p99 674 ms, recall@10 0.08 |
| Filtered search | 1.8 to 4.1 QPS, recall@10 between 0.00 and 0.09 at every selectivity |
| 4-thread QPS | about the same as one client (1.8 to 6.6) |
| Bytes read per query (`rchar`) | 31 to 47 MB, about 70 percent of the on-disk collection |
| Peak RSS | 300 MB, of which 15 MB was resident before the engine opened (dataset, oracle, harness) |

What the baseline shows:

- ANN recall collapses on clustered data. After the flush every query takes an HNSW plan (`vector_first_ann` or `cooperative_filtered_ann`). The graph (M = 8, `ef_construction` = 32, no neighbor-selection heuristic) and the 60-candidate budget return almost none of the true neighbors. The exact path agrees with the oracle exactly; the harness tests check this.
- Every query reads roughly the whole collection from disk, whatever the plan or filter.
- Extra client threads do not add throughput.
- Range predicates (`--filter-style range`) always get a fixed 0.6 selectivity estimate from the planner, so selective range filters get `vector_first_ann` with post-filtering. That is why equality flags are the default style.

## Reading The Numbers

- `recall` compares each answer with a brute-force oracle that the harness computes itself, independent of the engine.
- `memory` is the harness process. For the in-process engine it includes the dataset and the oracle. Subtract `memory.before_target` to estimate what the engine added.
- `io_per_query.rchar` counts bytes returned by read syscalls, including page-cache hits. `read_bytes` counts only device reads, so it stays near zero when the data fits in the page cache.
- `plans` shows which physical plan the planner chose for each query.
