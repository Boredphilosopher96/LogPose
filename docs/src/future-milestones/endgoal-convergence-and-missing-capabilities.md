# Endgoal Convergence And Missing Capabilities

## Goal

Explain how the current milestone set converges on LogPose's target architecture and document the still-missing, high-impact work that is not yet cleanly owned by the existing milestone pages.

`better-vector-db.md` remains the philosophy and end-state memo. This chapter is the bridge between that memo and the concrete milestone set.

## Current Convergence Map

The current milestones already cover much of the path to the target architecture.

| Endgoal Theme | Already Covered By | Remaining Gap |
| --- | --- | --- |
| Distributed metadata, durability, and resilience | Multi-Cluster Metadata and Consistency, Blob Storage Integration, Full-System Simulation | cold or warm serving cost model, disk-native execution, disaggregated serving |
| Planner-led hybrid execution | Additional Vector Index Families | broader filtered-search strategy planning, memory-temperature-aware planning |
| Operator ergonomics and observability | Web GUI, existing explain/profile and inspect surfaces | component-level memory accounting and operator-visible memory decisions |
| Database-like write and visibility rules | already-implemented local storage and planner work, Blob Storage Integration | end-to-end visibility rules for cold, remote, and future disaggregated execution |
| Performance without Milvus-style `load` and `release` workflows | current planner and diagnostics foundations | adaptive residency management, hot-data overrides, and targeted SIMD kernel acceleration |

## What The Current Milestones Still Do Not Fully Cover

The current roadmap is strong, but finishing it would still leave a few endgoal gaps from `better-vector-db.md`.

The highest-impact uncovered areas are:

- adaptive memory and residency management
- full memory accounting and operator-visible memory arbitration
- planner awareness of hot, warm, and cold execution state
- targeted SIMD acceleration for hot vector math kernels
- disk-native and disaggregated serving follow-through
- broader filtered-search strategy planning beyond simply adding more index families

## Adaptive Memory And Residency

### Why Adaptive Memory And Residency Matters

`better-vector-db.md` explicitly rejects Milvus-style explicit `load` and `release` as the default user interface. That only works if LogPose grows a real residency subsystem instead of relying on ad hoc caching.

The central lesson from [Gorgeous](https://arxiv.org/abs/2508.15290) and [d-HNSW](https://arxiv.org/abs/2505.11783) is that not every vector artifact has equal value. Keeping graph adjacency or routing summaries hot can matter more than keeping full raw payloads resident.

### What Current Milestones Cover For Adaptive Memory And Residency

- `Additional Vector Index Families` acknowledges that DiskANN-style paths should wait for explicit component residency and disk-aware profiling.
- `Blob Storage Integration` covers remote durability of immutable artifacts.

That is necessary, but it does not yet define a serving-time residency manager.

### What LogPose Still Needs For Adaptive Memory And Residency

- residency classes for graph topology, centroids, compressed codes, raw rerank vectors, and scalar metadata structures
- value-aware admission and eviction instead of one undifferentiated cache
- pinning and reservation semantics for hot collections and background maintenance
- planner-visible cold-start and prefetch state

## Memory Accounting And Operator Visibility

### Why Memory Accounting And Operator Visibility Matter

The endgoal memo asks for clear memory accounting, not just better hit rates. Operators need to know whether memory pressure comes from graphs, codes, rerank payloads, scalar indexes, maintenance, or prefetch.

Vector databases are still weak here. Practical engine references such as [Velox memory management](https://facebookincubator.github.io/velox/develop/memory.html) and [DuckDB memory management](https://duckdb.org/2024/07/09/memory-management.html) show what enforceable accounting and arbitration look like in database systems.

### What Current Milestones Cover For Memory Accounting And Operator Visibility

- `Web GUI` and existing diagnostics improve visibility of runtime state and query behavior.
- current explain/profile surfaces already expose plan, candidate, rerank, merge, and fallback data.

What is still missing is memory accounting as a first-class observable system contract.

### What LogPose Still Needs For Memory Accounting And Operator Visibility

- bytes attributed by collection, immutable unit, and artifact class
- separation of resident, pinned, reclaimable, prefetched, and temporary build bytes
- budget arbitration between serving, compaction, blob sync, and index build work
- operator-visible memory breakdowns in stats, inspect, and future GUI surfaces

## Memory-Temperature-Aware Planning

### Why Memory-Temperature-Aware Planning Matters

`better-vector-db.md` says plan choice should depend on memory temperature. That means the planner must understand more than index family and selectivity. It must know whether the relevant graph, codes, or raw vectors are hot, warm, or cold.

This is the missing bridge between adaptive residency and planner-first execution.

### What Current Milestones Cover For Memory-Temperature-Aware Planning

- `Additional Vector Index Families` covers family choice across exact, graph, IVF, and later disk-oriented paths.
- `Blob Storage Integration` and `Multi-Cluster Metadata and Consistency` make cold or remote artifact placement more realistic.

The roadmap does not yet define how those storage states feed planner decisions.

### What LogPose Still Needs For Memory-Temperature-Aware Planning

- a hot, warm, cold, and later remote temperature model per unit and per artifact class
- planner cost inputs for fetch latency, rerank cost, and residency fraction
- explain/profile output that shows when cold state changed the chosen strategy
- feedback loops from observed execution back into planning heuristics

## SIMD Vector-Kernel Optimization

### Why SIMD Vector-Kernel Optimization Matters

SIMD should not be treated as a repo-wide coding style. It should be treated as a targeted optimization layer for hot vector math paths.

As LogPose grows IVF, PQ, OPQ, filtered exact fallback, and late rerank paths, CPU kernels will matter more. Systems such as [Faiss](https://faiss.ai/) and papers such as [Quicker ADC](https://arxiv.org/abs/1812.09162) and [Anisotropic Vector Quantization](https://arxiv.org/abs/1908.10396) show how much performance depends on specialized distance kernels, fused scoring, and efficient top-k loops.

### What Current Milestones Cover For SIMD Vector-Kernel Optimization

- `Additional Vector Index Families` grows the operator family catalog.
- current query diagnostics already measure candidate generation and rerank cost.

What is not covered yet is the kernel layer needed to keep those richer operators fast.

### What LogPose Still Needs For SIMD Vector-Kernel Optimization

- architecture-specific kernels for dot product, cosine, and L2 hot loops
- SIMD-aware exact rerank and candidate scan paths
- fused decode-and-score kernels for compressed families
- AVX2, AVX-512, and scalar fallback paths, with ARM-friendly equivalents where relevant
- benchmark discipline and operator diagnostics so optimized kernels stay observable and correct

## Disk-Native And Disaggregated Execution Follow-Through

### Why Disk-Native And Disaggregated Follow-Through Matter

Adding object storage is not the same as adding disk-native serving. Adding DiskANN later is also not enough by itself.

[DiskANN](https://www.microsoft.com/en-us/research/publication/diskann-fast-accurate-billion-point-nearest-neighbor-search-on-a-single-node/), [Gorgeous](https://arxiv.org/abs/2508.15290), and [d-HNSW](https://arxiv.org/abs/2505.11783) all point to the same systems lesson: cold serving requires co-design between layout, fetch policy, and execution. It is not just “put the index somewhere slower.”

### What Current Milestones Cover For Disk-Native And Disaggregated Follow-Through

- `Blob Storage Integration` covers remote immutable durability.
- `Additional Vector Index Families` acknowledges disk-oriented graph families.
- `Multi-Cluster Metadata and Consistency` makes remote ownership and cold artifact discovery realistic.

The roadmap still lacks a dedicated serving-time execution model for cold and remote data.

### What LogPose Still Needs For Disk-Native And Disaggregated Follow-Through

- async fetch and prefetch rules for cold traversal
- queue-depth and batching control for disk or remote candidate expansion
- explicit profile counters for bytes fetched, stall time, and fetch amplification
- a clear distinction between object-store durability and disaggregated serving-time execution

## Advanced Filtered-ANN Strategy Work

### Why Advanced Filtered-ANN Strategy Work Matters

The next gap after “more ANN families” is not another isolated index. It is a better filtered-search execution framework.

The [filtered-ANN survey](https://arxiv.org/abs/2505.06501), [Compass](https://arxiv.org/abs/2510.27141), [NaviX](https://arxiv.org/abs/2506.23397), [DIGRA](https://ira.lib.polyu.edu.hk/bitstream/10397/115423/1/3725399.pdf), and [Efficient Dynamic Indexing for Range-Filtered ANNS](https://ira.lib.polyu.edu.hk/bitstream/10397/115424/1/3725401.pdf) all reinforce that filtered search remains an open systems problem across selectivity bands, predicate shapes, and update behavior.

### What Current Milestones Cover For Advanced Filtered-ANN Strategy Work

- `Additional Vector Index Families` covers filtered-selectivity testing and family-aware explain surfaces.
- `better-vector-db.md` already points toward exact fallback, cooperative traversal, and planner-directed hybrid execution.

What is still missing is a milestone dedicated to general filtered-search strategy planning, not just family expansion.

### What LogPose Still Needs For Advanced Filtered-ANN Strategy Work

- scalar-first, vector-first, cooperative, and mixed candidate-injection plans under one executor model
- stronger predicate statistics and observed-versus-estimated selectivity feedback
- planner rules that adapt across tiny, medium, and broad filtered populations
- validation for correlated, anti-correlated, conjunctive, disjunctive, and range-heavy workloads

## What LogPose Must Still Build

The practical work streams that remain after the current milestone set are:

1. Adaptive residency manager
2. Memory accounting and arbitration subsystem
3. Memory-temperature-aware planner inputs and diagnostics
4. SIMD-specialized vector kernel layer with safe fallbacks
5. Disk-native serving path for cold immutable units
6. Disaggregated execution prototype, separate from blob durability
7. General filtered-search and strategy-planning milestone after index-family expansion

## Testing And Validation

These gaps should not be treated as benchmark-only work. They need the same disciplined verification style as the rest of LogPose.

- benchmark hot, warm, and cold execution separately
- compare SIMD and scalar kernels against exact-oracle correctness checks
- expose memory-sensitive planner choices through explain/profile output
- test filtered-search strategies across selectivity bands and correlation patterns
- extend simulation and fault-injection once cold or remote serving becomes real runtime behavior
- keep operator-visible accounting and diagnostics stable enough for CLI and future GUI regression coverage

## Exit Criteria

LogPose can claim convergence on the `better-vector-db.md` endgoal only when:

- operators no longer need explicit `load` and `release` style workflows for normal serving
- memory is accounted for by meaningful artifact classes and exposed through diagnostics
- the planner can choose differently for hot, warm, and cold data with truthful explain/profile output
- hot vector math paths have benchmarked SIMD acceleration with correct scalar fallbacks
- cold and remote immutable-serving behavior has an explicit execution contract, not just storage placement
- filtered-search strategy planning goes beyond family choice and stays robust across selectivity bands
- the milestone set and the endgoal memo no longer diverge on high-impact system work
