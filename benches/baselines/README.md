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

No single change reaches high recall. The fix applies all three: levels are drawn from the same deterministic hash but mapped to a geometric distribution with `mL = 1 / ln(M)`, layer 0 holds up to 2M neighbors, and both linking and pruning use the diversity heuristic with pruned connections kept (Malkov and Yashunin, Algorithm 4). The defaults also rose to M = 16, `ef_construction` = 128, and `ef_search` = 64. The heuristic also treats an exact copy of an already kept neighbor as redundant and breaks distance ties toward the newer node, so bursts of identical vectors stay reachable instead of forming islands. The sidecar version moved to 2. The reader rejects sidecars written by the old builder, and ANN queries score those segments exactly instead of failing, until a compaction that merges them writes current sidecars. The HNSW v2 work in the engine v2 plan still replaces this index wholesale.

## phase5-milvus-cohere-100k and phase5-milvus-openai-50k

LogPose against Milvus standalone on the same machine, VectorDBBench style (engine v2 plan, Phase 5 task 5). Each shape has a `.json` file (both systems' raw reports plus run resources) and a generated `.md` summary with one table per case. Reproduce both from the workspace root:

```bash
LOGPOSE_BENCH_DATA=$HOME/.cache/logpose-bench scripts/bench-milvus.sh cohere-100k openai-50k
```

The script needs Docker and Python 3. It builds `logpose-server` and `logpose-bench` in release mode, prepares each dataset with `logpose-bench vdb-prepare` (cached under `LOGPOSE_BENCH_DATA`), runs a fresh `logpose-server` and then a fresh `milvusdb/milvus:v2.6.24` standalone container (never both at once), and writes the two files per shape here. `SKIP_MILVUS=1` runs LogPose alone. `scripts/bench-milvus.sh tiny` is a two-minute smoke run.

What the run does, for both systems:

- Data: synthetic embedding-like vectors (a 256-cluster Gaussian mixture in 32 latent dimensions, randomly projected to 768 or 1,536 dimensions, plus noise), with the row count, dimensionality, 1,000 queries, and cosine metric of VectorDBBench Cohere 100K and OpenAI 50K. The public VectorDBBench files (`assets.zilliz.com`) and Hugging Face were blocked by the egress policy of the machine that ran it, so these are not the public datasets. Unlike an isotropic Gaussian, this data needs an `ef` of about 24 to 48 for recall@10 of 0.95 on an HNSW graph, which is where real embeddings land.
- Ground truth: exact top 10 computed by `vdb-prepare`, for unfiltered search and for `rank < t` filters matching 1 and 99 percent of rows, where `rank` is a seeded random permutation (uncorrelated with the vectors). Both drivers score against the same files.
- Index: HNSW with M = 16 and efConstruction = 200 on both (LogPose through the new `[index]` config table). LogPose traverses SQ8 codes and reranks with f32; Milvus searches f32. The `rank` field has LogPose's automatic inverted and sorted index and a Milvus `STL_SORT` index.
- Load: 1,000-row insert batches, then everything needed until the data is indexed and searchable (LogPose: flush, compact, wait for maintenance; Milvus: flush, compact, wait for the index, refresh the load).
- Search: per case, a serial pass over all 1,000 queries at each `ef` of the sweep until mean recall@10 reaches 0.95, then 20-second runs with 1, 4, and 8 concurrent clients at that `ef`, each client on its own connection. LogPose is driven by `logpose-bench vdb-run` over gRPC; Milvus by `scripts/bench/milvus_vdb.py` over `pymilvus`, one process per client.

Results of the committed run, at the `ef` where each system first reached recall@10 of 0.95 (QPS at 1 / 4 / 8 clients, p99 in ms at 1 client):

| Dataset | Case | LogPose recall | LogPose QPS | LogPose p99 | Milvus recall | Milvus QPS | Milvus p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cohere-100k | unfiltered | 0.976 | 150 / 165 / 187 | 15.9 | 0.971 | 78 / 164 / 170 | 54.2 |
| cohere-100k | filter 1% | 1.000 | 431 / 494 / 655 | 6.2 | 1.000 | 94 / 163 / 207 | 36.3 |
| cohere-100k | filter 99% | 0.977 | 158 / 285 / 515 | 15.6 | 0.971 | 74 / 83 / 101 | 65.0 |
| openai-50k | unfiltered | 0.988 | 283 / 456 / 474 | 8.5 | 0.960 | 100 / 91 / 113 | 28.7 |
| openai-50k | filter 1% | 1.000 | 390 / 786 / 824 | 6.2 | 1.000 | 52 / 96 / 127 | 130.7 |
| openai-50k | filter 99% | 0.988 | 244 / 454 / 483 | 9.7 | 0.959 | 96 / 149 / 155 | 38.6 |

| Dataset | LogPose load (insert + optimize) | Milvus load (insert + optimize) |
| --- | --- | --- |
| cohere-100k | 691 s (250 + 441), 7 inserts retried after write stalls | 220 s (22 + 198) |
| openai-50k | 334 s (10 + 324) | 191 s (30 + 161) |

Read these numbers with their caveats before quoting them:

- The machine was not quiet. It is a shared 4-vCPU Xeon VM (16 GB) where other jobs were compiling Rust throughout: the 1-minute load average was 10 to 27 when each system started (recorded per system in the `.md` files). Both systems ran under the same kind of contention, but not identical contention, and absolute numbers are far below what either reaches on idle hardware. Rerun on a dedicated machine before drawing conclusions from ratios.
- The Milvus client is Python. VectorDBBench drives Milvus the same way, but each search pays `pymilvus` serialization of a 768 or 1,536-float list, and on this loaded machine even a 2,000-row smoke collection took Milvus about 6 ms per search at the median. That fixed per-request cost, not the graph search, dominates Milvus latency at 50K and 100K rows; the LogPose driver is Rust.
- In these two files LogPose's `ef` column says 16, but LogPose always searches with at least `4 * k` = 40 candidates, so those rows ran at 40. Since this run, `vdb-run` reports the beam width that actually ran. Milvus ran at the `ef` shown (16 or 24).
- LogPose loads slowly. Each memtable flush builds its segment's HNSW graph inline with efConstruction 200, so on cohere-100k the flushes fell behind the inserts and the writer stalled writes seven times (the driver waits for the server's retry hint and retries). The explicit compaction then rebuilds the graph for the merged segment. Milvus acknowledges inserts into a growing segment and builds indexes in the background.
- LogPose's explicit compaction did not reach one segment: it stops where one job's maintenance-memory reservation ends. cohere-100k was left with an 80,000-row HNSW segment plus two 10,000-row segments that are below the 20,000-row graph threshold and are scanned exactly on every query; openai-50k with 20,000 and 30,000-row HNSW segments. Every query visits every segment and merges.
- LogPose unfiltered QPS barely scales from 1 to 8 clients on cohere-100k while the 99 percent filter case does; that difference was not investigated and may be contention noise.
- The 1 percent filter matches 1,000 or 500 rows. LogPose plans it as an exact scan of the matching rows (`PREDICATE_FIRST_EXACT`), which is why it is its fastest case; Milvus also searches small filtered sets by brute force.
- The planner used here is the pre-v2 planner (`VECTOR_FIRST_ANN` and `COOPERATIVE_FILTERED_ANN` plans). These files should be regenerated once planner v2 lands.
- Provenance: both files were produced by binaries built from commit `eb8dbbf` plus the insert-retry change later committed as `855bee0`; the `git_commit` fields in the reports name whatever `HEAD` was checked out when each report was written, and the working tree was dirty during the run.

## Reading The Numbers

- `recall` compares each answer with a brute-force oracle that the harness computes itself, independent of the engine.
- `memory` is the harness process. For the in-process engine it includes the dataset and the oracle. Subtract `memory.before_target` to estimate what the engine added.
- `io_per_query.rchar` counts bytes returned by read syscalls, including page-cache hits. `read_bytes` counts only device reads, so it stays near zero when the data fits in the page cache.
- `plans` shows which physical plan the planner chose for each query.
