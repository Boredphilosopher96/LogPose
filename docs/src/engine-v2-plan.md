# Engine V2 Plan

This is the working plan for turning LogPose from a correct but naive prototype into a vector database that competes with Milvus on vector search and beats it on everyday database behavior.

It has three parts:

1. an honest audit of the code as of September 2026
2. the design decisions, already made, with the reasoning behind each
3. a phased implementation plan with concrete tasks, tests, and exit criteria

Read the whole document before starting a phase. Later phases depend on data-model choices made in earlier ones, and those choices are expensive to reverse.

## The Product Contract

LogPose should feel like a database that happens to have first-class vector search, not a vector index that happens to store metadata.

The contract, stated as things a user never has to do:

- never call `load` or `release`; a collection is queryable the moment it is created and after every restart
- never create an index before searching; indexes are built automatically with good defaults
- never call `flush` to make writes visible; an acknowledged write is visible to the next read
- never pick a consistency level to get read-your-writes; it is the default
- never size memory per collection by hand; the engine manages residency under one memory budget
- never run a separate message queue, object store, or metadata service for a single node; one binary with a data directory is a complete deployment

And the things a user expects from a normal database, which Milvus makes awkward:

- upsert by primary key, partial updates that do not resend the vector, and atomic multi-record batches
- get by primary key, scroll with a stable cursor (no offset ceiling), exact `count` with a filter, delete by filter
- order by a scalar field with a limit, with or without a vector in the query
- a typed schema that can gain and lose fields online, plus a dynamic JSON field for everything undeclared
- a rich filter language: equality, ranges, `IN`, null checks, array membership, prefix, nested JSON paths
- an `EXPLAIN` that tells the truth about what ran and why

Milvus comparison points this contract answers directly:

- Milvus requires `create_index` then `load` before search; LogPose requires neither.
- Milvus defaults to bounded-staleness reads; LogPose defaults to read-your-writes.
- Milvus caps `offset + limit`; LogPose uses keyset cursors with no ceiling.
- Milvus scalar indexes are opt-in per field; LogPose indexes declared filterable fields automatically.
- Milvus cluster mode needs etcd, a log broker, and object storage; LogPose single-node needs nothing, and cluster mode needs etcd only.

## Audit Of The Current Code

Numbers as of commit `7e8fc73`: about 47k lines of Rust, 378 tests (360 pass; the 18 failures are etcd tests run without etcd), clippy and fmt clean.

### What Is Worth Keeping

- CRC-framed WAL with prefix recovery and a checkpointed rotation protocol (`crates/logpose-wal`, `crates/logpose-storage`)
- snapshot semantics: `Snapshot { manifest_generation, visible_seq_no }` and read barriers (`crates/logpose-types/src/lib.rs:433`)
- the predicate AST and its validation (`crates/logpose-query/src/lib.rs:60-112`, `810-861`)
- the `EXPLAIN` and profile diagnostics shape (`QueryDiagnostics`)
- REST and gRPC parity through shared `AppState` methods
- seeded randomized model-checking harnesses for storage and service
- workspace lint discipline (no `unwrap`, no `unsafe`, no `todo!`)

### What Is Hollow

The engine is stateless per call. Every `StorageEngine` method starts with `load_collection_state` (`crates/logpose-storage/src/lib.rs:608`), which scans the collections directory for the descriptor, reads `CURRENT` and the manifest, and replays the entire unflushed WAL with CRC checks and JSON decoding. One ANN query makes five or six storage calls, and each repeats that work.

```text
one hybrid ANN query today

  call                      descriptor  manifest  WAL replay  all segments  all .hnsw.bin
  resolve descriptor            x
  current snapshot              x          x          x
  stats_snapshot (planner)      x          x          x          X (decode)
  ann_search_selected           x          x          x                        X (read)
  latest_visible_selected       x          x          x          ~ (partial)
  scan mutable delta            x          x          x

  X = O(collection size) work on every query
```

Consequences:

- the planner decodes every segment just to count live rows (`logpose-query/src/lib.rs:305` into `logpose-storage/src/lib.rs:853`)
- ANN reads and deserializes every segment's HNSW sidecar, which embeds a copy of every vector and every record's metadata JSON (`logpose-storage/src/lib.rs:1904`), so ANN reads at least as many bytes as an exact scan
- every write replays the whole WAL delta first, so ingest is quadratic across a flush interval, and every operation in a batch gets its own fsync
- all file I/O is blocking `std::fs` and `std::sync::Mutex` inside `async fn`, with no `spawn_blocking` anywhere

There are no scalar indexes. `ScalarFieldStats` (`logpose-types/src/lib.rs:504`) are planner statistics, including an unbounded `value_counts` map persisted in the manifest. Every predicate is evaluated by walking `serde_json::Value` per record.

Filtered ANN is post-filtering with restarts. `search_hnsw` (`logpose-index/src/lib.rs:499-562`) traverses without the filter, filters the result list, and if fewer than the budget survive, doubles `ef` and restarts from scratch, up to `ef = N`. Candidate queues are now binary heaps, but each restart repeats the whole traversal, so restrictive filters walk most of the segment. `CooperativeFilteredAnn` and `VectorFirstAnn` run identical code. `VectorFirstExact` is strictly dominated by `PredicateFirstExact`.

HNSW is minimal. It was built with `M = 8` on all layers, `ef_construction = 32`, levels from a hash capped at 4, and no neighbor-selection heuristic, which split clustered data into one island per cluster (recall@10 about 0.05). The legacy builder now uses standard construction (levels with `mL = 1 / ln(M)`, `2M` neighbors on layer 0, the diversity heuristic, `M = 16`, `ef_construction = 128`), but parameters are still not configurable (`HnswBuildParams` in `logpose-index/src/lib.rs`). Distance code is scalar, duplicated in storage and index, and cosine recomputes both norms per call.

Distribution fences metadata, not data. The etcd layer (`crates/logpose-storage-etcd`) has real leader election, membership leases, and CAS ownership promotion with epochs, but the epoch never reaches storage. Writes do "read owner from etcd, then write locally", which races with promotion. Nothing is replicated; failover in tests and the chaos harness copies directories by hand.

### Correctness Bugs

These can lose acknowledged data or wedge a node today. Each needs a failing regression test first.

1. Torn WAL tail is never truncated. `WalWriter::open` uses `append(true)` (`crates/logpose-wal/src/lib.rs:48`), so the next append lands after garbage and replay then fails on magic or CRC, making the collection unreadable.
2. Flush ignores failure to remove `PENDING_ROTATION` (`crates/logpose-storage/src/lib.rs:1273`). A later load then truncates `active.wal` and loses writes acknowledged after the flush.
3. `atomic_write` never fsyncs the parent directory (`crates/logpose-storage/src/lib.rs:2866`), and index sidecars are written with plain `fs::write` and no fsync (`crates/logpose-index/src/lib.rs:139`, `374`). After power loss the manifest can reference empty sidecars and there is no rebuild path.
4. A write batch is N separate WAL frames with no commit record (`crates/logpose-storage/src/lib.rs:1812-1822`). A crash mid-batch leaves a partial batch that replays as visible.
5. A node can stay leaderless forever. If the leadership lease dies while membership survives, `current_leader()` returns `None`, but the node still holds its dead lease and only campaigns when `leadership_lease.is_none()` (`crates/logpose-service/src/lib.rs:212`, `234`). Keep-alive responses with TTL 0 are ignored.
6. Two processes on one `storage_root` corrupt each other; locks are process-global statics keyed by path and there is no file lock (`crates/logpose-storage/src/lib.rs:2235`).
7. `cargo build` fails without a system `protoc` because `etcd-client` needs it; only `logpose-api-grpc` uses the vendored binary. The etcd tests hard-fail without etcd. `tests/smoke_workspace.rs` and `benches/bootstrap.rs` are never compiled.

### Docs Drift

`docs/src/future-milestones.md` says the original phase roadmap is complete. Every phase has a code path, but most paths are not engineered beyond test-fixture scale. That page should point here until Phase 5 lands.

## Design Decisions

These are decided. Revisit one only with benchmark evidence or a correctness argument, and update this section when you do.

### D1 Scale Target

Tier 1, the target for this plan: 10 million vectors of 768 dimensions per node, fully served with a 16 to 32 GB memory budget.

Tier 2, designed for but built later: 100 million vectors per node using 1-bit or PQ codes and a disk-resident graph.

Sizing arithmetic that drives the other decisions:

```text
10M x 768-dim
  raw f32 vectors              30.7 GB   -> must NOT be required in RAM
  SQ8 codes (1 byte/dim)        7.7 GB   -> resident
  HNSW layer 0, M=16 (32 x u32) 1.3 GB   -> resident
  primary-key index (~24 B/row) 0.24 GB  -> resident
  scalar indexes (roaring)      varies   -> resident when hot
  -----------------------------------------------------
  hot working set              ~9.5 GB + scalar indexes
  raw vectors read from SSD only for final rerank of a few hundred candidates
```

Implication: quantization is part of the core vector path in Phase 4, not a later optimization.

### D2 Single Binary, Shared-Nothing

- one `logpose-server` binary serves REST and gRPC over a local data directory
- single-node mode uses local metadata only; etcd is not required
- cluster mode (Phase 7) adds etcd for membership and ownership, hash sharding by primary key, and primary-backup replication by WAL shipping
- object storage is optional (backup and cold tiering), never required to serve

Rejected: a Milvus-style disaggregated design with a log broker and mandatory object storage. It scales well but makes "just works" impossible and multiplies operational surface.

### D3 Storage Engine Shape

An LSM of immutable segments over a mutable memtable, with resident versioned state.

```text
 write batch
     |
     v
 +--------+  1 frame, group-commit fsync   +-----------+  upsert pk   +-------------------+
 |  WAL   | -----------------------------> | memtable  | -----------> | primary-key index |
 +--------+                                +-----------+              | pk -> (seg, row)  |
                                                 | flush              +-------------------+
                                                 v                             | mark old row
 +------------------------------+          +---------------------------+      v
 | Version N  (ArcSwap)         |  refs    | segment v2 (immutable)    |   +------------------+
 |  memtable ref                | -------> |  ids / pk column          |   | deletion vectors |
 |  segment set                 |          |  vector columns (f32)     |   | roaring, COW,    |
 |  deletion-vector generation  | -------> |  quantized codes (SQ8)    |   | per generation   |
 +------------------------------+          |  scalar columns (typed)   |   +------------------+
        ^                                  |  scalar indexes (bitmaps) |
        | load() = Arc clone               |  vector index over row ids|
   query / scan                            |  stats, footer, CRCs      |
                                           +---------------------------+
```

- `Version` is the unit of consistency, as in RocksDB `Version` and `SuperVersion`. Readers `load()` an `Arc<Version>` and never block. Writers build a new `Version` and swap it in.
- Files are deleted when no live `Version` references them. That is the garbage collector.
- Each segment row has a dense `u32` row id. A global row address is `(segment_id, row_id)`.
- A primary-key index maps `pk -> (segment_id, row_id)` or a memtable slot. It is in memory and rebuilt from segment pk columns on open.
- An upsert of an existing pk marks the old row in that segment's deletion vector. Readers never "resolve the latest version" by scanning older tiers.
- Deletion vectors are roaring bitmaps, copy-on-write per `Version`. Between checkpoints, the WAL is their durability; at checkpoint they are written as `segments/<id>.dv.<generation>`.

Why this matters for scalar-plus-vector: every index (scalar or vector) speaks row ids, every filter becomes a bitmap, and deletes are one more bitmap `AND NOT`. That is the single data structure where the two worlds meet.

### D4 Schema And Data Model

A collection has a typed schema:

- exactly one primary key field, `string` or `int64`
- one or more named vector fields, each with dimension and metric; the segment format supports several from day one
- scalar fields: `bool`, `int64`, `float64`, `string`, `timestamp`, arrays of those, and `json`
- each scalar field has `index: auto | none | inverted | sorted`; `auto` picks inverted for `bool` and `string` and arrays, and sorted plus inverted for numbers and timestamps
- a dynamic field (`$extra`) stores any undeclared keys as JSON; it is filterable by scan and can be given path indexes later

Schema changes are online: adding a field is a metadata change (old segments return null), and dropping a field hides it and lets compaction reclaim space.

Example create request:

```json
{
  "name": "products",
  "primary_key": { "name": "sku", "type": "string" },
  "vectors": [
    { "name": "embedding", "dimensions": 768, "metric": "cosine" }
  ],
  "fields": [
    { "name": "tenant", "type": "string" },
    { "name": "price", "type": "float64" },
    { "name": "tags", "type": "array<string>" },
    { "name": "updated_at", "type": "timestamp" }
  ],
  "dynamic_fields": true
}
```

No index parameters appear in that request. Defaults are good enough; overrides exist for experts.

### D5 Vector Index Strategy

Heterogeneous by tier, chosen by the engine, not the user:

- memtable: exact SIMD scan (bounded by flush size)
- small segments (under about 20k rows, calibrate): no graph, exact SIMD scan over SQ8 codes plus f32 rerank
- large segments: HNSW over row ids, traversing SQ8 codes, reranking the top candidates with f32 vectors read from the segment
- Tier 2 later: 1-bit (RaBitQ-style) or PQ codes and a disk-resident graph for cold segments

Cosine vectors are normalized at ingest and searched as inner product. L2 ranks by squared distance.

### D6 Filtered Search

Decide per segment from the exact filter cardinality, which bitmaps make free:

```text
 predicate --compile--> per segment: B = bitmap(predicate) AND NOT deletion_vector
                                     n = |B|, N = live rows
      n <= exact_threshold             -> exact SIMD scan over B
      n / N small, above threshold     -> filter-aware graph walk (ACORN-1 style)
      n / N large                      -> normal HNSW walk, admit only rows in B
 then: per-segment top-k -> global k-way heap merge -> f32 rerank -> project fields
```

- search is resumable: when fewer than k survive, continue the same traversal with a larger beam instead of restarting
- thresholds are constants in one place, calibrated by the benchmark harness in Phase 0 and Phase 5
- the memtable keeps live in-memory scalar indexes, so it uses the same bitmap path

### D7 Consistency

- a write is acknowledged after the WAL group commit fsyncs and the memtable applies it, so the next read on the node sees it
- a batch is atomic: one WAL frame, all or nothing
- a read uses one `Version` for scalar and vector state, so it is snapshot-consistent
- clients can request a snapshot token for repeatable reads and cursors; tokens pin a `Version` and expire after a TTL (default 5 minutes)
- historical snapshots beyond pinned tokens are not kept; old files are garbage-collected

### D8 Memory And I/O

- one engine-wide memory budget (`storage.memory_limit`), split across artifact classes in priority order: graph adjacency and quantized codes, primary-key index, scalar indexes, scalar columns, raw f32 vectors, dynamic JSON
- segment files are read with positioned reads into an engine-managed buffer cache; no mmap
- on open, the engine warms graphs and codes in the background up to budget; queries against cold data still work, just slower, and `EXPLAIN` says so
- blocking I/O runs on a dedicated I/O thread pool; CPU search runs on a `rayon` pool; the tokio runtime only handles network and orchestration

### D9 Unsafe And SIMD

Keep `unsafe_code = "forbid"` in every LogPose crate. Get SIMD from `pulp` (runtime CPU dispatch across AVX2, AVX-512, and NEON) with a scalar reference kernel used as the test oracle. Revisit only if benchmarks show a gap of more than 20 percent against a hand-written intrinsic kernel, and even then isolate `unsafe` in one small crate.

### D10 Distribution Comes Last

Freeze new coordination features until Phase 5 is done. Bug fixes in the existing etcd code are fine.

When distribution resumes (Phase 7), it builds on engine primitives: an epoch in every WAL frame and manifest, WAL shipping keyed by the per-shard sequence number, and replica freshness as the applied sequence number.

### D11 API Shape

- REST and gRPC stay; the CLI and TUI are frozen apart from keeping them compiling
- gRPC stops carrying JSON strings (`metadata_json`); use a typed `Value` message
- one search request covers vector search, filters, `order_by`, `limit`, projection, and cursor
- data operations: `upsert`, `update` (partial, by pk or by filter), `delete` (by pk or by filter), `get`, `scroll`, `count`
- collection operations: create with schema, get, list, alter (add or drop field, rename), drop
- errors are typed from storage to the wire; not-owner and not-leader map to `UNAVAILABLE` or HTTP 503 with an owner hint

Example search request:

```json
{
  "vector": { "field": "embedding", "values": [0.12, -0.03] },
  "filter": {
    "and": [
      { "eq": { "tenant": "acme" } },
      { "range": { "price": { "gte": 10, "lt": 50 } } },
      { "contains": { "tags": "outdoor" } }
    ]
  },
  "limit": 10,
  "output_fields": ["sku", "price"],
  "explain": "profile"
}
```

The same request without `vector` and with `order_by: [{ "field": "updated_at", "direction": "desc" }]` is a plain filtered scan.

## Target Crate Layout

The large `lib.rs` files (storage 3.2k lines, index 1.6k, query 2.6k) should be split into modules as part of the rewrite. Suggested shape, keeping existing crate names:

```text
crates/logpose-wal        binary frames, group commit, tail repair, Vfs trait use
crates/logpose-storage    engine: Engine, CollectionHandle, Version, memtable,
                          segment v2 reader/writer, deletion vectors, pk index,
                          compaction, GC, buffer cache, Vfs
crates/logpose-index      vector/ (hnsw, flat, sq8, kernels)
                          scalar/ (inverted, sorted, stats)
crates/logpose-query      predicate compiler, planner, operators, explain
crates/logpose-types      schema, values, predicates, errors (typed)
```

Add a `Vfs` trait (filesystem abstraction) in `logpose-storage` with a real implementation and a fault-injecting one for tests.

## Phased Plan

```text
 critical path:  P0 --> P1 --> P2 --> P3 --> P4 --> P5
                         \       \       \
 parallel:                P6a     P6b     P6c        (API work tracks the engine)
                                  \
                                   P7 after P5        (distribution)
                                                P8    (later: sparse, BM25, tier 2)
```

Every PR, in every phase:

- runs `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace`, `cargo doc --workspace --no-deps`
- adds a failing test before a bug fix
- does not care about backwards compatibility of formats, APIs, or tests; delete what the new design replaces

### Phase 0 Stabilize And Measure

Goal: nothing loses data, the build and tests work on a clean machine, and there is a baseline to beat.

Tasks (all independent, run in parallel):

1. Fix bugs 1 through 7 from the audit, each with a regression test.
2. Make the build self-contained: point `PROTOC` at the vendored binary (for example through `.cargo/config.toml` `[env]` or a shared build helper), gate the etcd integration tests on `LOGPOSE_TEST_ETCD_ENDPOINTS`, and either wire `tests/smoke_workspace.rs` and `benches/bootstrap.rs` into a package or delete them and update `AGENTS.md`.
3. Add the `Vfs` trait under WAL, manifest, and segment I/O, plus a fault-injecting implementation that can drop unsynced writes, tear the last write, and fail fsync. Add a crash-and-reopen action to the randomized storage harness (`crates/logpose-storage/tests/support/randomized.rs`) that uses it.
4. Build `benches/` harness v1 (a new bench crate is fine):
   - datasets: synthetic generator with fixed seed; loaders for SIFT-128 and a 768-dim set (for example Cohere 1M from VectorDBBench)
   - filters at 0.1, 1, 10, 50, and 99 percent selectivity, both uncorrelated and anti-correlated with the query vector
   - metrics: ingest rows per second, time from write to searchable, QPS, p50 and p99 latency, recall@10 against exact, bytes read per query, resident memory
   - output: a JSON report committed under `benches/baselines/`
5. Replace `docs/src/future-milestones.md` "roadmap complete" language with a pointer to this plan.

Exit criteria: `cargo test --workspace` passes without etcd; crash tests run on the fault-injecting `Vfs`; a baseline report exists for the current engine.

### Phase 1 Resident Engine Core

Goal: no call does O(data) metadata work; writes are group-committed; I/O leaves the async runtime.

Tasks:

1. `Engine` owns `HashMap<CollectionId, Arc<CollectionHandle>>`, populated once at open. Descriptor lookup becomes a map lookup; delete `find_collection_descriptor`'s directory scan.
2. `CollectionHandle` holds `ArcSwap<Version>`, the memtable, and a single writer task fed by a channel. Maintenance (flush, compaction) runs on an engine-owned scheduler, replacing per-collection `thread::spawn`.
3. WAL v2: binary encoding (`postcard` or a hand-written codec), one frame per batch, group commit that coalesces concurrent batches into one fsync, tail repair on open. Frames carry `seq_no` ranges and a reserved `epoch` field (used in Phase 7).
4. Maintain live and deleted counts incrementally; `stats` becomes O(segments).
5. `Version` reference counting drives GC of manifests, rolled WAL files, and segments. Snapshot tokens pin a `Version` with TTL.
6. Exclusive file lock on `storage_root` at open.
7. Move file I/O to an I/O pool and search to a `rayon` pool; no blocking calls on tokio workers.
8. Split `crates/logpose-storage/src/lib.rs` into modules along the way.

Keep the current `StorageEngine` trait as the seam so the existing 360 tests keep passing, then change the trait freely once the new engine is behind it.

Exit criteria: a query on a hot collection performs zero metadata file reads; ingest throughput is flat as the memtable grows; group commit shows fewer fsyncs than batches under concurrency; existing tests pass.

### Phase 2 Data Model V2

Goal: typed schema, primary keys, row ids, deletion vectors, and a segment format built for columnar access.

Tasks:

1. Schema types in `logpose-types` (D4), validation on write, online add and drop field.
2. Segment v2 format:

   ```text
   [header: magic, version, schema hash, row count, section table]  CRC
   [pk column]                                                      CRC
   [vector column per vector field: 64-byte aligned f32]            CRC
   [scalar column per field: typed, strings dictionary-encoded]     CRC
   [dynamic JSON column: offsets + bytes, lazily decoded]           CRC
   [index sections: written in Phase 3 and Phase 4]                 CRC
   [footer: section offsets, lengths, CRCs]                         CRC
   ```

   Every byte is covered by a checksum, including the header and section table (today's entry table is not). Delete the JSON entry table and the flat JSON sidecar.
3. Primary-key index and deletion vectors (D3). Upsert, delete, and partial update mark old rows; readers apply deletion vectors; the "resolve latest version across tiers" path is deleted.
4. Size-tiered compaction: merge segments of similar size, drop deleted rows, never rewrite the whole collection at once.
5. Buffer cache v1 with a byte budget and artifact classes (D8).
6. With deletion vectors, any segment can be pruned by zone maps (min and max per column), not just a run of the oldest ones. Remove the conservative pruning rule and its shadowing tests; replace them with deletion-vector tests.

Exit criteria: an update costs one WAL frame plus O(1) index work; compaction write amplification is measured and bounded; crash tests cover flush, compaction, and deletion-vector checkpoints; the Phase 0 harness shows no regression.

### Phase 3 Scalar Indexes And Queries

Goal: the scalar half of "scalar plus vector". Filters cost index probes, not row scans.

Tasks:

1. Inverted index per segment: term to roaring bitmap, for `bool`, `string`, `int64`, and array elements.
2. Sorted index per segment: sorted `(value, row_id)` pairs with binary search producing a bitmap; also serves `order_by` scans.
3. Memtable indexes: `BTreeMap<Value, RoaringBitmap>` maintained on apply.
4. Predicate compiler in `logpose-query`: predicate to per-segment bitmap expression; `NOT` is `live AND NOT B`; unindexed and dynamic fields fall back to a column or JSON scan that also yields a bitmap.
5. Expression language additions: `in`, `not_in`, `contains`, `contains_any`, `prefix`, nested JSON paths on the dynamic field, `is_null`, `exists`.
6. Statistics: drop unbounded `value_counts`; keep HyperLogLog distinct counts, top-k frequent values, and equi-depth histograms for cross-segment estimates only.
7. Non-vector operations: `get` by pk, `count` with filter (popcount), `scroll` with keyset cursor and snapshot token, `delete` and `update` by filter, `order_by` plus `limit`.

Exit criteria: on 10M rows, a selective equality filter with `count` answers from bitmaps without touching columns; `scroll` over the whole collection returns every live row exactly once under concurrent writes; `EXPLAIN` shows estimated and actual row counts.

### Phase 4 Vector Core

Goal: Milvus-class vector search, with filtering built in.

Tasks:

1. HNSW v2 in `logpose-index`:
   - `BinaryHeap` candidate and result queues
   - visited set as a generation-stamped array, reused per thread
   - `M` default 16, `2M` links on layer 0, levels drawn with `mL = 1 / ln(M)`, no level cap
   - neighbor-selection heuristic (Malkov and Yashunin, algorithm 4)
   - nodes store row ids only; no copies of vectors or metadata
   - parallel build for large segments
2. SQ8 quantization per segment (per-dimension min and max), graph traversal on codes, f32 rerank of the top `k * rerank_factor` from the segment's vector column.
3. Distance kernels with `pulp`: dot, squared L2, SQ8 dot and L2, batched scoring for scans. Scalar kernels are the oracle in tests. Delete duplicated distance code in storage and index.
4. Filtered search per D6: exact scan over the bitmap, ACORN-1 style walk, and bitmap-admitting walk, chosen per segment. Resumable search.
5. Small-segment policy: no graph under the calibrated threshold.
6. Record `visited`, `filtered_out`, `expansions`, and `rerank_reads` per segment and surface them in `EXPLAIN` (today they are discarded at `crates/logpose-storage/src/lib.rs:1913`).
7. Per-collection overrides for `M` and `ef_construction`; per-query `ef` override.

Exit criteria, on the Phase 0 harness at 10M by 768 (or 1M if hardware is limited, stated in the report):

- recall@10 of at least 0.95 unfiltered
- recall@10 of at least 0.95 at every selectivity from 0.1 to 99 percent, including anti-correlated filters
- resident memory within the D1 sizing envelope
- QPS reported next to the Phase 0 baseline

### Phase 5 Planner V2 And The Milvus Benchmark

Goal: plans are real operator trees with costs, and LogPose is measured against Milvus on identical hardware.

Tasks:

1. Operator tree: `SegmentSource`, `BitmapProbe`, `ExactScan(allow)`, `GraphScan(allow)`, `MaskDeletes`, `TopK`, `Merge`, `Rerank`, `Project`, `OrderedScan`.
2. Cost model in distance computations, graph hops, and bytes touched (split by resident versus cold), calibrated from the harness. Delete the 0.45 and 0.6 magic constants.
3. Parallel per-segment execution with a global heap merge. Delete the full sort in `rank_matches_with`.
4. `EXPLAIN` renders the tree with estimated and actual values per operator and the reason for each strategy choice. Delete `VectorFirstExact` and the label-only plan kinds.
5. Add a VectorDBBench client for LogPose and run it against Milvus (HNSW index) on the same machine: Cohere 1M 768-dim and OpenAI 500K 1536-dim, unfiltered and filtered (1 and 99 percent), plus a 10M case. Commit the results under `benches/baselines/`.
   Status: the client (`logpose-bench vdb-prepare` and `vdb-run`), the Milvus driver, and the end-to-end script (`scripts/bench-milvus.sh`) are in. First results at 100K by 768 and 50K by 1,536, on synthetic embedding-like data because the public files were unreachable, are in `benches/baselines/phase5-milvus-*.md`. They were taken on a loaded machine with the pre-v2 planner and do not show the exit criterion. The 1M, 500K, and 10M runs remain, as does a rerun on a quiet machine after planner v2.

Exit criteria: at recall of at least 0.95, LogPose QPS is within 20 percent of Milvus HNSW or better on every case; filtered cases hold recall where Milvus drops; results are reproducible from a script.

Status (Phase 5a, tasks 1 to 4): landed. Plans are operator trees whose per-segment strategy is the cheapest under a cost model in distance computations, graph hops, and resident, random, and cold bytes, calibrated on the benchmark host (`calibrate_cost_model` in `crates/logpose-query/tests/recall.rs`); the exact-scan limit and the ACORN selectivity threshold are gone (the 0.45 and 0.6 constants went with the v1 planner). Units run in parallel with a global heap merge, large exact scans split into parallel morsels, and a walk that reaches the exact scan's price scans exactly instead. `EXPLAIN` returns the tree with estimated and actual work per operator and each strategy's reason; `vector_first_exact` and `tiny_population_exact_fallback` are gone. The engine design's [Phase 5 notes](engine-core-design.md#implementation-notes-phase-5-planner) have the details. Task 5 (VectorDBBench against Milvus) is separate and open.

Measured on the 4-core development host at 100,000 x 128 (one compacted segment, top 10, `ef = 64`, single client, warm cache), with the before and after builds run interleaved because the host was shared and loaded (load average 8 to 30); QPS is the median of three rounds (single rounds varied by up to 30 percent with the host's load), recall over 200 queries:

| Case (100,000 x 128, `ef = 64`) | QPS before | QPS after | Change | Recall@10 before | Recall@10 after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Unfiltered | 1,403 | 1,726 | +23 % | 0.985 | 0.985 |
| Uniform 10 % filter | 386 | 772 | 2.0 x | 0.990 | 1.000 |
| Uniform 1 % filter | 1,696 | 1,825 | +8 % | 1.000 | 1.000 |
| Anti-correlated 10 % filter | 108 | 767 | 7.1 x | 0.979 | 1.000 |

The unfiltered case is limited by the walk itself (about 250 µs, memory-latency bound on this host) and the one query-pool hop left per query (about 100 µs of wake-up latency on this host, which the old path paid for its first of three hops too). The uniform 1 percent case gains least: it was already an exact scan of 1,000 rows. Filtered cases gain most where the cost model scans exactly (in parallel morsels) instead of walking, and anti-correlated filters no longer escalate a walk past the exact scan's price, which is also why their recall is now exact.

### Phase 6 API V2

Runs alongside the engine phases. Each slice lands when its engine support exists.

- P6a (after Phase 1): typed errors end to end, replacing `LogPoseError::Message(String)` and the substring matching in `classify_message` (`crates/logpose-service/src/lib.rs:1294`); not-owner and not-leader become `UNAVAILABLE` or 503; configured request size limits; client-streaming bulk ingest in gRPC.
- P6b (after Phase 2): schema-based create, alter, list, and drop for collections; drop database; typed `Value` in proto instead of `metadata_json`; one database selector convention for all REST routes; `upsert`, `update`, `delete`, `get`. Landed as API v2 (`/v2/databases/{database}/collections/{collection}/...`, package `logpose.v2`), replacing v1.
- P6c (after Phase 3): search request per D11 with filters, `order_by`, cursor, projection; `count`, `scroll`, delete by filter. Landed with `top_k` in place of `limit`: the query takes a typed filter, a named vector field, one `order_by`, projection, `ef`, and a snapshot token or `pin`, and without a vector is a filtered scan; paging is `scroll`, whose opaque cursor carries a snapshot token; `count`; and delete and update by filter as one atomic batch, bounded by one WAL frame (64 MiB, `TOO_LARGE` beyond).
- Always: keep `proto/`, `openapi/`, and `docs/src/api-overview.md` in sync; fix the OpenAPI 3.1 `nullable` misuse; add a contract test that validates the OpenAPI document and checks REST and gRPC parity.

### Phase 7 Distribution

Starts after Phase 5.

1. Storage-level fencing: every WAL frame and manifest records the ownership epoch; the engine rejects writes from a stale epoch; an owner stops acknowledging writes before its ownership lease can expire; only the elected controller promotes, and only after the old owner's lease has expired.
2. Replication by WAL shipping from primary to replicas, keyed by sequence number; acknowledgment policy `primary` or `quorum`; replica freshness is the applied sequence number, which makes read barriers work after failover. A read barrier that a lagging replica has not reached yet then maps to `UNAVAILABLE` with a retry hint; until then it is `FAILED_PRECONDITION`, because waiting on a single node never satisfies it.
3. Hash sharding by primary key; scatter-gather search with global top-k merge; watch-driven routing caches instead of several etcd reads per request. Map etcd client errors per kind: today every one is `UNAVAILABLE` with a 1 s retry hint, including kinds that retrying cannot fix, such as permission denied or a request that exceeds etcd's size limit.
4. Fix the leader re-campaign bug if Phase 0 did not already, and extend the deterministic control-plane simulation to the etcd backend.
5. Optional: segments to object storage for backup and fast replica bootstrap (the blob storage milestone).

Exit criteria: `kill -9` of a primary under write load loses no quorum-acknowledged write; the chaos harness passes without copying directories by hand.

### Phase 8 Later

- sparse vectors, BM25 full-text, and reciprocal-rank fusion for hybrid dense plus sparse search
- Tier 2 scale: 1-bit or PQ codes, disk-resident graph for cold segments
- IVF family if benchmarks show a workload where it wins
- aggregations (`group_by` with `count`) from inverted indexes
- richer auth (OIDC, principal management), web GUI

## Stop, Freeze, Delete

Freeze until Phase 5:

- new etcd and coordination features
- CLI and TUI features
- any new index family

Delete as the replacement lands:

- `IndexKind::IvfPq`, `WalMode`, the flat JSON sidecar, `VectorFirstExact`
- JSON segment entry tables and JSON WAL payloads
- duplicated distance code and the three copies of the collection reference parser (`logpose-query`, `logpose-core`, `logpose-service`)
- string-typed `tier`, `index_kind`, and `route_kind`

## How To Run This Plan

- Work phase by phase on the critical path. Inside a phase, independent tasks run as parallel subagents, as `AGENTS.md` asks. Phase 0 is almost entirely parallel.
- Before writing Phase 1 and Phase 2 code, write a short design note for `Version`, the pk index, deletion vectors, and segment v2 (types, invariants, crash-recovery steps) and review it. Those structures are the foundation of everything after them.
- Keep the randomized harnesses as the safety net: extend the model with primary keys, deletion vectors, crash-and-reopen, and snapshot tokens as each lands.
- Measure every phase with the Phase 0 harness and commit the report. A phase that makes a benchmark worse without a stated reason is not done.
- Update this document when a decision changes or a phase completes.

## First Ten PRs

1. WAL tail repair on open, with a torn-tail-then-append regression test.
2. Durable publish: directory fsync in `atomic_write`, fsync for sidecars, propagate the `PENDING_ROTATION` removal error.
3. Atomic batches: one WAL frame and one fsync per batch.
4. Leader re-campaign when the leadership lease dies, with a test that revokes only that lease.
5. Self-contained build and etcd test gating; wire or delete the orphaned smoke test and bench.
6. Exclusive `storage_root` lock.
7. `Vfs` trait with a fault-injecting implementation and crash-and-reopen in the randomized storage harness.
8. Benchmark harness v1 and the committed baseline report.
9. Design note for the Phase 1 and Phase 2 engine core.
10. Phase 1 first slice: `Engine` and `CollectionHandle` with cached descriptors and manifests behind the existing trait.

PRs 1 through 8 are independent of each other.
