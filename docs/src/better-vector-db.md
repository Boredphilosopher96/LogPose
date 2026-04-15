# Better Vector DB Architecture

This memo captures a practical direction for building a vector database that is more database-like than Milvus without giving up the scale and retrieval performance that make systems like Milvus useful.

The goal is not to make a vector engine cosmetically resemble PostgreSQL. The goal is to close the most painful gaps:

- too much memory pressure
- explicit load and release semantics
- weak scalar and vector integration
- awkward updates and deletes
- limited visibility into query planning and system cost

As of April 2026, recent research suggests that the next generation of vector databases should look less like "a distributed ANN service with metadata support" and more like "a storage engine and query planner that happen to include ANN as a first-class operator."

## Why Milvus Feels Wrong

Milvus is good at what it was designed for: distributed approximate nearest neighbor search over large collections, with segment-based storage and background indexing.

It still feels unlike a normal database for a few structural reasons:

- query execution is segmented into growing and sealed data paths
- vector and scalar processing are only loosely coupled
- memory residency is user-visible through `load` and `release`
- indexing is largely offline and segment-oriented
- hybrid query behavior depends heavily on hand-tuned index choices and filter selectivity

None of those choices are irrational. They are a consequence of optimizing for scalable ANN search first. The downside is that database behavior becomes an afterthought instead of the primary abstraction.

## What Current Vector DBs Get Right

Current vector databases do several things well:

- They treat high-dimensional retrieval as a systems problem, not just a library call.
- They separate write ingestion from read-optimized search paths.
- They use immutable segment or slab files effectively for durability and distribution.
- They expose practical ANN options such as IVF, HNSW, PQ, and DiskANN-style layouts.
- They scale query throughput well for read-heavy workloads.

These are not small wins. A better design should preserve them.

## The Main Weaknesses

### 1. Memory still dominates the design

Graph indexes such as HNSW remain memory hungry. Even when disk-based variants are used, systems often keep enough compressed vectors, graph state, or routing metadata in memory that the "disk-native" label only partially solves the problem.

The deeper issue is not just index size. It is that vector systems often depend on memory residency for predictable performance, while exposing too little control over what should stay hot:

- graph topology
- centroids or routing summaries
- compressed codes
- raw vectors for reranking
- metadata bitmaps and predicate state

### 2. Scalar and vector access paths are still separate worlds

Many systems, including Milvus-style architectures, treat scalar filtering and vector retrieval as separate stages connected by a bitmap or candidate set.

That is workable, but it is not a unified query model. It creates predictable pain:

- prefiltering can destroy ANN recall unless the search expands aggressively
- postfiltering can waste a large amount of ANN work
- no single cost model reliably predicts the best plan across selectivity ranges
- complex predicates remain awkward

### 3. Updates are still second-class

ANN structures are excellent for reads and often awkward for writes. Inserts, updates, and deletes usually mean one of:

- a mutable in-memory side path
- tombstones plus background cleanup
- periodic rebuilds
- degraded graph or partition quality over time

This makes many vector systems feel fresh enough for append-heavy workloads, but not truly transactional or continuously updatable.

### 4. The system contract is fuzzy

Many vector databases do not state hybrid visibility rules clearly enough:

- when does a write become visible to vector search
- when does a delete stop appearing in results
- whether scalar filters and ANN results are snapshot-consistent
- whether mutable and sealed tiers are merged before or after reranking

Without a crisp contract, "database-like" behavior is mostly an expectation mismatch.

### 5. Operational ergonomics are still weak

Modern vector systems often require users to understand:

- index families and tuning knobs
- memory loading behavior
- compaction side effects
- segment states
- filter selectivity pathologies

That is too much machinery for most teams to reason about safely.

## What Recent Research Changes

Recent work strengthens a few design directions.

### Hybrid search should be planner-driven

Recent systems and papers increasingly argue that the main challenge is not inventing one magical index, but choosing and coordinating access paths well.

- [SingleStore-V](https://vldb.org/pvldb/vol17/p3772-chen.pdf) shows that vector search can be integrated into a relational engine with dedicated operators and per-segment vector indexes.
- [PostgreSQL-V](https://www.vldb.org/cidrdb/papers/2026/p2-liu.pdf) argues that a decoupled vector storage and execution path can make a general database more competitive.
- [Compass](https://arxiv.org/abs/2510.27141) pushes a similar idea from the hybrid-search angle: coordinate standard scalar and vector indexes instead of baking every predicate into a specialized structure.

This is important because it suggests that the next gain is not only better ANN, but better planning across heterogeneous operators.

### LSM-like vector storage is becoming credible

Several recent systems converge on the idea of immutable levels plus a mutable delta tier:

- [LSM-VEC](https://arxiv.org/abs/2505.17152)
- [MicroNN](https://machinelearning.apple.com/research/micronn-on-device)
- Pinecone's slab-based serverless architecture in its [ICML 2025 metadata filtering paper](https://www.pinecone.io/research/ICML_2025.pdf)

This direction is attractive because it makes freshness, durability, and background merge behavior more database-like than pure segment rebuild loops.

### Data layout matters more than many systems assume

Recent work shows that disk-resident and disaggregated vector search are not just "put the index on slower storage."

- [Gorgeous](https://arxiv.org/abs/2508.15290) emphasizes that graph adjacency can be more important to keep hot than raw vectors.
- [d-HNSW](https://arxiv.org/abs/2505.11783) shows that disaggregated memory can work if the graph layout and fetch path are redesigned for it.

The lesson is that memory management should focus on the highest-value structures, not just bulk caching of entire indexes.

### Filtered ANN is still an open systems problem

The field is still actively debating how to support filters robustly across selectivity ranges.

- [Survey of Filtered Approximate Nearest Neighbor Search over the Vector-Scalar Hybrid Data](https://arxiv.org/abs/2505.06501)
- [NaviX](https://arxiv.org/abs/2506.23397)
- [DIGRA](https://ira.lib.polyu.edu.hk/bitstream/10397/115423/1/3725399.pdf)
- [Efficient Dynamic Indexing for Range-Filtered ANNS](https://ira.lib.polyu.edu.hk/bitstream/10397/115424/1/3725401.pdf)

The strongest shared conclusion is that filtered vector search should not be treated as a corner case. It is central to real applications and should shape the storage and planning model.

## A Better Target Architecture

The right target is not "Milvus, but with nicer APIs." The right target is a database-shaped retrieval engine with explicit storage and planning rules.

### 1. Storage model

Use a two-tier write path:

- a mutable delta tier for fresh inserts, updates, and deletes
- immutable segment or slab files for compacted historical data

Each immutable unit should contain:

- vectors or compressed vector payloads
- a vector index suited to that unit's size and temperature
- a scalar metadata index
- tombstone and version metadata
- statistics for planning

This enables:

- fast fresh writes
- background merge and reindex
- durability without rebuilding everything
- per-level index heterogeneity

Small fresh units should use fast-to-build indexes. Larger compacted units can afford slower, higher-quality indexes.

### 2. Query execution model

The planner should treat vector search as a first-class physical operator, not a bolt-on function call.

For hybrid queries, it should choose among at least four strategies:

- prefilter then exact or approximate vector search
- vector-first candidate generation then postfilter
- cooperative filtered ANN traversal
- exact scan fallback for very small filtered populations

The choice should depend on:

- estimated filter selectivity
- top-k
- segment size
- mutable versus immutable tier
- index family
- memory temperature

This is not a full classical cost-based optimizer yet, but it is much better than static heuristics.

### 3. Memory model

Replace explicit `load` and `release` as the default user interface with adaptive residency management.

The cache manager should reason about different classes of objects:

- navigation structures such as graph adjacency
- routing summaries such as centroids
- compressed candidate payloads
- raw vector payloads for refinement
- scalar bitmap and posting structures

Users should still be able to pin hot namespaces, collections, or segments, but manual residency should be an override, not the default operating model.

### 4. Consistency contract

The engine should publish simple hybrid visibility rules:

- writes enter the mutable tier transactionally
- reads observe a consistent snapshot across scalar and vector state
- deletes are enforced before final ranking is returned
- compacted immutable tiers become visible atomically

Even if the ANN structures themselves are approximate, visibility rules should not be.

### 5. Observability model

Expose the system cost directly.

Operators should be able to answer:

- why this query was slow
- whether latency came from filter evaluation, ANN traversal, cold fetch, rerank, or merge
- how much memory is consumed by graphs, codes, raw vectors, and metadata
- whether the planner chose the wrong strategy

An `EXPLAIN HYBRID` or `EXPLAIN VECTOR` facility would be disproportionately valuable.

## What Should Not Be the Goal

A better vector database should not try to promise impossible things.

It should not promise:

- one universal index that wins across every workload
- complete elimination of memory sensitivity
- full relational optimization over approximate operators from day one
- graph-index performance with zero operational complexity
- PostgreSQL ergonomics with Milvus-scale ANN behavior and no tradeoffs

Those promises hide real cost.

## Where LogPose Stands Today

LogPose already implements the first architectural layer from this memo:

- local mutable plus immutable storage with WAL, manifests, immutable segments, and maintenance recovery
- planner-led exact execution plus HNSW-backed ANN and hybrid exact-plus-ANN merge
- explain and profile diagnostics, collection stats, and inspect surfaces that make planner behavior and storage layout operator-visible
- an explicit logical control-plane and data-plane split inside one runtime process

That means this memo is now mostly about what still needs to happen after the foundational phases, not about what the repository is still missing at the very beginning.

## Remaining Milestones For LogPose

The biggest remaining steps are now tracked in [Future Milestones](./future-milestones.md):

1. [Multi-Cluster Metadata and Consistency](./future-milestones/multicluster-metadata-and-consistency.md)
   - replace local placement files with an authoritative metadata plane, failover rules, and explicit consistency modes
2. [Additional Vector Index Families](./future-milestones/additional-vector-index-families.md)
   - add planner-selected IVF, compression-aware, and later disk-oriented families beyond today's HNSW path
3. [Full-System Simulation](./future-milestones/full-system-simulation.md)
   - extend the current deterministic harnesses into TigerBeetle-inspired seeded system simulation with replayable faults and liveness checks
4. [Web GUI](./future-milestones/web-gui.md)
   - turn the existing operator-facing contracts into a browser-based runtime, collection, query, and inspect console
5. [Blob Storage Integration](./future-milestones/blob-storage-integration.md)
   - move immutable artifacts toward real MinIO or S3-backed durability and recovery instead of local-only files plus metadata stubs

Those are product and systems milestones, not just more benchmark tuning.

## Recommendation for LogPose

If LogPose wants to be better than the current Milvus-style experience, it should focus first on being more principled, not more clever.

The best remaining bets are:

1. Treat hybrid planning as a core subsystem.
2. Design for mutable plus immutable storage from the start.
3. Add a real metadata authority before pretending distribution is finished.
4. Make durability and recovery semantics explicit across local and remote storage.
5. Build observability and deterministic system testing as first-class features.

That combination is realistic, differentiated, and aligned with where the research is moving.

## References

- [Milvus architecture overview](https://milvus.io/docs/architecture_overview.md)
- [Milvus load and release](https://milvus.io/docs/load-and-release.md)
- [SingleStore-V](https://vldb.org/pvldb/vol17/p3772-chen.pdf)
- [PostgreSQL-V](https://www.vldb.org/cidrdb/papers/2026/p2-liu.pdf)
- [LSM-VEC](https://arxiv.org/abs/2505.17152)
- [MicroNN](https://machinelearning.apple.com/research/micronn-on-device)
- [Compass](https://arxiv.org/abs/2510.27141)
- [NaviX](https://arxiv.org/abs/2506.23397)
- [Survey of Filtered Approximate Nearest Neighbor Search over the Vector-Scalar Hybrid Data](https://arxiv.org/abs/2505.06501)
- [DIGRA](https://ira.lib.polyu.edu.hk/bitstream/10397/115423/1/3725399.pdf)
- [Efficient Dynamic Indexing for Range-Filtered ANNS](https://ira.lib.polyu.edu.hk/bitstream/10397/115424/1/3725401.pdf)
- [Gorgeous](https://arxiv.org/abs/2508.15290)
- [d-HNSW](https://arxiv.org/abs/2505.11783)
- [Pinecone metadata filtering, ICML 2025](https://www.pinecone.io/research/ICML_2025.pdf)
