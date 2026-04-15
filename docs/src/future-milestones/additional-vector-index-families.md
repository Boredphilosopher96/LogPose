# Additional Vector Index Families

## Goal

Grow LogPose from a planner with one ANN family into a planner that can choose among multiple vector index families based on data shape, top-k, filter selectivity, memory pressure, and recovery cost.

## Current State

LogPose currently has one real ANN path:

- immutable-unit HNSW sidecars
- flat sidecars for exact support and diagnostics
- planner-controlled exact, ANN, and hybrid merge execution

What is missing today:

- no IVF family in production code
- no scalar or product quantization
- no OPQ
- no disk-oriented graph family such as DiskANN or Vamana
- no planner capability model beyond effectively treating `hnsw` as the only ANN option

## Why This Matters

HNSW is a strong default, but it is not a universal winner.

Larger top-k, stricter memory budgets, colder datasets, and different filter selectivity patterns all push toward different operator families. If LogPose wants to stay planner-first, it needs more than one physical operator family to choose from.

## Research Anchors

- [Milvus index overview](https://milvus.io/docs/index-explained.md)
- [Milvus vector index families](https://milvus.io/docs/index-vector-fields.md)
- [Milvus IVF_FLAT](https://milvus.io/docs/ivf-flat.md)
- [Milvus IVF_PQ](https://milvus.io/docs/ivf-pq.md)
- [Milvus IVF_SQ8](https://milvus.io/docs/ivf-sq8.md)
- [Milvus DISKANN](https://milvus.io/docs/diskann.md)
- [Milvus filtered search](https://milvus.io/docs/filtered-search.md)
- [HNSW](https://arxiv.org/abs/1603.09320)
- [Product Quantization](https://doi.org/10.1109/TPAMI.2010.57)
- [Optimized Product Quantization](https://openaccess.thecvf.com/content_cvpr_2013/html/Ge_Optimized_Product_Quantization_2013_CVPR_paper.html)
- [DiskANN](https://www.microsoft.com/en-us/research/publication/diskann-fast-accurate-billion-point-nearest-neighbor-search-on-a-single-node/)
- [Survey of Filtered Approximate Nearest Neighbor Search over the Vector-Scalar Hybrid Data](https://arxiv.org/abs/2505.06501)
- [NaviX](https://arxiv.org/abs/2506.23397)

## Direction For LogPose

Add new families in stages.

### First: IVF_FLAT

This is the smallest step beyond HNSW and fits the current immutable-unit model well.

- train centroids per immutable unit
- assign records to lists
- search `nprobe` lists
- rerank with exact vectors using the same correctness contract LogPose already uses today

### Next: Compression-Aware IVF

Add compression only after the uncompressed IVF path is correct and observable.

- start with scalar quantization or a simple compressed path before full PQ if implementation cost matters most
- add IVF_PQ once codebook training, inspection, and rerank accounting are clear
- treat OPQ as a quality layer on top of PQ, not as a separate planner family

### Later: Disk-Oriented Graphs

DiskANN or Vamana-style layouts should come only after LogPose has explicit component residency and disk-aware profiling.

- use them only for immutable cold units at first
- keep raw-vector rerank and visibility semantics exact even when candidate generation is approximate

### Filtered ANN Must Stay First-Class

New index families only help if the planner still respects filter selectivity.

- tiny filtered populations should keep exact fallback
- IVF-style paths can help when prefiltering is affordable
- graph paths still need vector-first and cooperative filtered behavior
- explain output should surface why the planner chose a family and how many candidates it expanded or discarded

## Main Work Streams

### 1. Index Artifacts And Codecs

- add real sidecars and codecs for IVF-based and compressed families
- stop assuming `.hnsw.bin` is the only ANN artifact
- record family, build params, and artifact details in immutable-unit metadata

### 2. Planner Capability Model

- replace string-oriented ANN checks with family capabilities
- plan across `exact`, `graph`, `ivf`, and later `disk_graph` families
- fold top-k, selectivity, and unit stats into family choice

### 3. Inspect And Diagnostics

- surface family-specific params such as `nlist`, `nprobe`, code sizes, or disk fetch counts
- keep exact rerank counts separate from approximate candidate counts
- make operator-visible profiling rich enough to compare families under real workloads

### 4. Build And Maintenance Pipeline

- train centroids and codebooks during flush or compaction
- validate new artifacts during recovery and inspect
- keep corruption handling as disciplined as the current HNSW path

## Testing And Validation

- keep exact-oracle validation for every family
- add recall and latency sweeps across `top_k`, `nprobe`, and selectivity bands
- test correlated and anti-correlated filters, not just random predicates
- add codec corruption and recovery tests for every new sidecar type
- keep deterministic benchmarks with fixed corpora and explicit recall envelopes
- validate explain and profile output for every family so operator surfaces stay truthful

## Exit Criteria

- LogPose supports at least one non-HNSW ANN family in production code
- the planner can choose among exact, HNSW, and at least one additional family
- explain and profile output names the chosen family and its key parameters
- storage inspect and recovery understand the new artifacts
- exact-versus-ANN regressions and benchmarks exist for every new family
- filtered queries have documented and tested decision rules across families
