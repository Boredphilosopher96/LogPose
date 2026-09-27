# Engine Core Design

This is the low-level design for the resident engine core: Phase 1 (resident engine, group commit, I/O off the runtime) and Phase 2 (data model v2, primary keys, deletion vectors, segment v2, compaction, buffer cache) of the [Engine V2 Plan](./engine-v2-plan.md). Decisions D1 to D11 in that plan are the source of truth; this document turns them into types, byte layouts, protocols, and PRs.

It is written so that separate engineers can build separate parts without talking to each other. Every cross-part contract is stated as a Rust type sketch, a byte layout, or a numbered invariant. When a sentence says "must", a test should be able to check it.

Other work lands in parallel and is referenced, not redesigned, here:

| Parallel work | What this design consumes |
| --- | --- |
| HNSW over row ids (`logpose-index`) | a graph built over `u32` row ids, serialized into one contiguous CSR byte payload |
| SIMD kernels (`logpose-index`) | `dot`, `l2_sq`, SQ8 variants, and batched scoring over `&[f32]` and `&[u8]` |
| SQ8 (`logpose-index`) | per-segment quantizer params plus codes, serialized into one payload |
| Scalar inverted and sorted indexes (`logpose-index`) | builders over `(row_id, value)` and readers that return `RoaringBitmap` |
| Schema, `Value`, `Record` (`logpose-types`) | `Schema`, `FieldId`, `FieldType`, `Value`, `PrimaryKey`, `Record`, and the binary `Value` codec |
| WAL batch frames (Phase 0) | one frame per batch; WAL v2 below supersedes the format |
| Durability fixes (Phase 0) | directory fsync, tail repair, storage-root lock; this design absorbs them into the `Vfs` |

## Goals, Non-Goals, and Invariants

### Goals

- A hot query performs zero metadata file reads: the collection map, descriptor, schema, manifest, segment headers, and deletion vectors are resident.
- A write batch is one WAL frame; concurrent batches share one fsync (group commit); an acknowledged write is visible to the next read.
- Upsert, delete, and partial update of an existing primary key cost one WAL frame plus O(1) index work.
- Every index speaks `u32` row ids; every filter and every delete is a `RoaringBitmap`; a query is snapshot-consistent over one `Version`.
- All file I/O goes through one `Vfs` trait, so every durability claim is testable with injected crashes.
- No blocking I/O on tokio workers or on the query `rayon` pool.

### Non-Goals

- Replication, fencing, and sharding (Phase 7). The WAL frame and manifest reserve an `epoch` field and nothing else.
- Multi-collection transactions. A batch is atomic within one collection.
- Historical reads beyond pinned snapshot tokens (D7).
- Tier 2 (disk-resident graphs, PQ, 1-bit codes). The segment format leaves room for new section kinds.
- Backwards compatibility with v1 files. There are no users; v1 directories are rejected with a typed error.
- The query planner v2 cost model (Phase 5). This document fixes the interface the planner calls, not its cost model.

### Invariants

Tests assert these by number. "Logical state at `s`" means the map `pk -> row` obtained by applying all committed operations with sequence number at most `s`, in order, to an empty collection.

1. **I1 Visibility.** When `write` returns `Ok(ack)`, every `ReadView` opened afterwards (on any thread) has `visible_seq_no >= ack.last_seq_no` and reflects the whole batch.
2. **I2 Durability.** An acknowledged batch survives a crash at any point, including a crash during recovery.
3. **I3 Atomicity.** Every published `Version` has `visible_seq_no` equal to the last sequence number of some batch. After any crash, each batch is either entirely present or entirely absent.
4. **I4 Snapshot consistency.** Every read through one `ReadView` (get, scan, count, vector search, scroll page) observes exactly the logical state at its `visible_seq_no`.
5. **I5 Unique live key.** In every `Version`, each primary key has at most one live row across all units (memtables and segments).
6. **I6 Sequence monotonicity.** Sequence numbers are assigned gap-free starting at 1. Published `Version` ids strictly increase and `visible_seq_no` never decreases.
7. **I7 GC safety.** A segment file is deleted only when (a) the durable `CURRENT` manifest does not reference it and (b) no live `Version`, including a token-pinned one, references it. A WAL file, DV file, or manifest file is deleted only after a durable manifest supersedes it.
8. **I8 Crash-recovery equivalence.** After recovery, the logical state equals the logical state at `R`, where `R` is the last sequence number of the longest valid WAL frame prefix, and `R` is at least the last acknowledged sequence number.
9. **I9 Checkpoint coverage.** For the manifest `M` named by `CURRENT` with checkpoint `C`: every row of every segment in `M` that was deleted by an operation with sequence number at most `C` is set in the DV file that `M` names for that segment; and every operation with sequence number greater than `C` is in a WAL file that still exists.
10. **I10 Row address stability.** A `RowAddr { unit, row }` never changes meaning. Segments are immutable; memtable slots are append-only; deletion bits are only ever set, never cleared, for a given unit.
11. **I11 Recovery idempotence.** Recovery that crashes at any point and is rerun produces the same state as a single uninterrupted recovery.
12. **I12 Token repeatability.** Two reads with the same unexpired snapshot token return identical results.
13. **I13 Counter exactness.** `Version` live and deleted row counters equal the values computed from row counts and deletion-vector cardinalities.

## Module and Crate Layout

```text
 logpose-vfs              logpose-types
 (std only)               schema, Value, Record, PrimaryKey, FilterExpr,
   ^    ^                 typed errors, SeqNo, Epoch, UnitId, RowId, RowAddr
   |    |                    ^             ^             ^            ^
   |    +--- logpose-wal ----+             |             |            |
   |         frames, codec,                |             |            |
   |         reader, writer         logpose-index        |            |
   |              ^                 vector/, scalar/     |            |
   |              |                 (no I/O)             |            |
   |              |                      ^               |            |
   +------- logpose-storage -------------+---------------+            |
            engine, handle, version, writer, memtable, pk index,      |
            dv, segment v2, manifest, flush, compaction, gc,          |
            cache, recovery, read view                                |
                  ^                                                   |
                  |                                                   |
            logpose-query --------------------------------------------+
            predicate compiler, planner, operators, explain,
            RowSetResolver impl
                  ^
                  |
            logpose-service, logpose-core (wiring)
```

Arrows point from a crate to its dependencies. `logpose-vfs` depends only on `std` (plus `rand` for `FaultVfs`). `logpose-index` never does I/O: it builds structures from rows and (de)serializes them to and from byte buffers. `logpose-storage` owns every file. `logpose-query` depends on `logpose-storage` for the read interfaces and implements the `RowSetResolver` trait that storage defines, which `logpose-core` injects at engine construction. This keeps the dependency graph acyclic while letting the writer resolve delete-by-filter.

The `Vfs` lives in a new crate, `logpose-vfs`, because both `logpose-wal` and `logpose-storage` need it and `logpose-storage` depends on `logpose-wal`. See [Deviations From the Plan](#deviations-from-the-plan).

Module map for `crates/logpose-storage/src`:

| Module | Responsibility |
| --- | --- |
| `lib.rs` | re-exports, `EngineConfig` |
| `engine.rs` | `Engine`, collection map, storage-root lock, create and drop |
| `runtime.rs` | `Runtime`, `IoPool`, rayon pools, `run_cpu` bridging |
| `handle.rs` | `CollectionHandle`, publication, read barrier wait |
| `version.rs` | `Version`, `DeletionMap`, counters, invariant checker |
| `writer/mod.rs` | writer task loop, group commit pipeline |
| `writer/apply.rs` | the single `apply` function shared by live writes and WAL replay |
| `writer/pk_index.rs` | writer-private primary-key index, forwarding tables |
| `memtable/` | `MemtableData`, `VectorArena`, `MemColumn`, `MemScalarIndex` |
| `dv.rs` | `DeletionVector`, DV file codec |
| `segment/format.rs` | byte layout constants, header, section table, footer |
| `segment/writer.rs` | streaming segment writer |
| `segment/reader.rs` | `SegmentHandle`, lazy section access |
| `manifest.rs` | manifest v2 codec, `CURRENT` protocol |
| `flush.rs` | flush job |
| `compaction.rs` | policy and compaction job |
| `scheduler.rs` | engine-wide maintenance scheduler |
| `gc.rs` | obsolete-file tracking, deletion queue, orphan cleanup |
| `cache.rs` | buffer cache |
| `recovery.rs` | open and recover |
| `tokens.rs` | snapshot token registry |
| `read.rs` | `CollectionReader`, `ReadView`, `UnitView`, fetch plans |
| `catalog.rs` | database and principal descriptor files (moved from `lib.rs`) |
| `legacy.rs` | `StorageEngine` adapter during migration, deleted at the end |

`crates/logpose-wal/src`: `frame.rs` (header layout, CRC), `codec.rs` (payload types and postcard codec), `writer.rs` (append, rotate), `reader.rs` (scan, validate, tail repair). `crates/logpose-index/src`: `vector/{hnsw,flat,sq8,kernels}.rs`, `scalar/{inverted,sorted,stats}.rs`. `crates/logpose-query/src`: `compile.rs`, `planner.rs`, `ops/`, `explain.rs`, `resolver.rs`.

Shared identifiers in `logpose-types`:

```rust
/// Monotonic per-collection operation sequence number. 0 means "nothing".
pub type SeqNo = u64;
/// Ownership epoch. Always 0 until Phase 7.
pub type Epoch = u64;
/// Dense row id inside one unit.
pub type RowId = u32;

/// Identifier of a memtable or a segment. Allocated from one per-collection
/// counter (`next_unit_id`, persisted in the manifest); never reused.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct UnitId(pub u32);

/// Global row address. Stable for the life of the unit (I10).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RowAddr {
    pub unit: UnitId,
    pub row: RowId,
}
```

## Vfs Trait

All engine I/O (WAL, manifests, `CURRENT`, segments, DV files, descriptors, catalog files, the lock file) goes through `Arc<dyn Vfs>`. No `std::fs` call remains in `logpose-wal` or `logpose-storage` outside `StdVfs`.

```rust
/// Filesystem used by the engine. Implementations must be usable from many
/// threads. Every method is blocking and must only be called on the I/O pool.
pub trait Vfs: Send + Sync + 'static {
    /// Open or create a file.
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>>;
    /// Create a directory and its parents. Not durable until `sync_dir` of the parent.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// List entry names (not paths) in a directory, unsorted.
    fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>>;
    /// Atomically replace `to` with `from`. Not durable until `sync_dir(parent)`.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Remove a file. Not durable until `sync_dir(parent)`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    /// Remove a directory tree. Used only for dropped collections.
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;
    /// Make the directory's entry set (creates, renames, removes) durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// Take an exclusive advisory lock, failing fast if another holder exists.
    fn try_lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn VfsLock>>;
    /// Named crash point. `StdVfs` returns `Ok(())`; `FaultVfs` may return
    /// `Err(crashed)`. Engine code calls this at every step listed in the
    /// flush, compaction, and GC crash analyses.
    fn crash_point(&self, point: CrashPoint) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenMode {
    /// Read-only; the file must exist.
    Read,
    /// Create a new file; fail if it exists. Used for every immutable file.
    CreateNew,
    /// Open existing for append (WAL tail after repair).
    Append,
}

pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    pub len: u64,
}

pub trait VfsFile: Send + Sync {
    /// Positioned read (`pread`). Returns bytes read; short only at EOF.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    /// Read exactly `buf.len()` bytes or fail with `UnexpectedEof`.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    /// Append all slices at the current end of file; returns the new length.
    /// A single call is one "write" for the torn-write fault model.
    fn append(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<u64>;
    /// `fdatasync`.
    fn sync_data(&self) -> io::Result<()>;
    /// `fsync`.
    fn sync_all(&self) -> io::Result<()>;
    /// Current length, including unsynced appends.
    fn len(&self) -> io::Result<u64>;
    /// Truncate. Used only by WAL tail repair.
    fn set_len(&self, len: u64) -> io::Result<()>;
}

pub trait VfsLock: Send + Sync {}
```

The trait is deliberately narrow: no seek, no in-place overwrite, no mmap. Immutable files are written once with `CreateNew` plus `append`, and the only mutable file is the WAL tail.

### StdVfs

- `open` maps to `std::fs::OpenOptions`; `CreateNew` uses `create_new(true)`.
- `read_at` uses `std::os::unix::fs::FileExt::read_at` (safe `pread`). Windows is not a target.
- `append` uses `write_all_vectored` on a file opened with `append(true)`.
- `sync_dir` opens the directory read-only and calls `sync_all`.
- `try_lock_exclusive` uses `std::fs::File::try_lock` (stable since Rust 1.89) on `<root>/LOCK`. The lock is held by the `Engine` for its lifetime; a second process fails open with `LogPoseError::StorageRootLocked`.
- `crash_point` returns `Ok(())` and compiles to nothing.

### FaultVfs

An in-memory filesystem that models what a crash can do to a POSIX filesystem. It is deterministic given a seed.

```rust
pub struct FaultVfs {
    state: Mutex<FaultState>,
    rng: Mutex<StdRng>,
}

struct FaultState {
    /// Inode table. A name maps to an inode in the directory's namespace.
    inodes: HashMap<InodeId, Inode>,
    /// Current (volatile) directory namespaces.
    dirs: HashMap<PathBuf, BTreeMap<String, InodeId>>,
    /// Namespaces as of each directory's last `sync_dir`.
    durable_dirs: HashMap<PathBuf, BTreeMap<String, InodeId>>,
    plan: FaultPlan,
    mutating_ops: u64,
    crashed: bool,
    locks: HashSet<PathBuf>,
}

struct Inode {
    /// Content visible to readers now.
    current: Vec<u8>,
    /// Content as of the last successful `sync_data`/`sync_all`.
    durable: Vec<u8>,
    /// Byte length of `current` at the start of the most recent `append`.
    last_append_start: Option<u64>,
}

pub struct FaultPlan {
    /// Crash (fail this and every later operation) when the named point is hit.
    pub crash_at: Option<CrashPoint>,
    /// Crash before the N-th mutating operation (append, sync, rename, remove, create).
    pub crash_after_ops: Option<u64>,
    /// Fail the N-th `sync_data`/`sync_all` with EIO.
    pub fail_sync: Option<u64>,
    /// Fail appends once total appended bytes exceed this (ENOSPC).
    pub enospc_after_bytes: Option<u64>,
    /// How a crash treats unsynced appended bytes.
    pub tear: TearMode,
}

pub enum TearMode {
    /// Drop all unsynced bytes.
    DropUnsynced,
    /// Keep a random prefix of the unsynced suffix of each file.
    KeepRandomPrefix,
    /// Keep a random prefix, then overwrite the last kept sector with random bytes.
    TornGarbage,
    /// Keep or zero each unsynced 4 KiB page independently (out-of-order writeback).
    ReorderedPages,
}
```

Fault model, stated as the rules the implementation enforces:

- **Unsynced data is lost on crash.** On `crash()`, each inode's content becomes `durable` plus, per `TearMode`, some prefix of the unsynced suffix `current[durable.len()..]`.
- **Torn last write.** `KeepRandomPrefix` and `TornGarbage` make the last append partially present, possibly with garbage in its final 512-byte sector. `ReorderedPages` persists an arbitrary subset of the unsynced pages, as a kernel writing back dirty pages out of order can.
- **fsync can fail.** A failed sync returns EIO and, like Linux, leaves the unsynced suffix in an undefined state: it is randomly kept or dropped at crash, and a later successful sync does not make the lost part durable. The engine must therefore treat a WAL fsync failure as fatal for the collection.
- **Namespace changes are volatile until the directory is synced.** Create, rename, and remove edit `dirs`; only `sync_dir` copies the directory into `durable_dirs`. On crash, each directory reverts to its durable namespace. A renamed file may appear under its old name, a newly created file may vanish, and a removed file may come back.
- **Crash halts the process.** After a crash triggers, every call returns `io::ErrorKind::Other` with a `Crashed` payload. The test then drops the engine, calls `vfs.crash()` to compute the post-crash state, and reopens on the same `FaultVfs`.

Crash-point API for tests:

```rust
/// Named steps. Every variant appears exactly once in engine code.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum CrashPoint {
    WalAfterAppend,
    WalAfterSync,
    WalAfterRotateCreate,
    FlushAfterSegmentSync,
    FlushAfterDvSync,
    FlushAfterSegmentsDirSync,
    ManifestAfterFileSync,
    ManifestAfterDirSync,
    CurrentAfterTempSync,
    CurrentAfterRename,
    CurrentAfterDirSync,
    CompactionAfterOutputSync,
    CompactionAfterDvSync,
    GcAfterRemove,
    RecoveryAfterTailRepair,
    RecoveryAfterOrphanCleanup,
}

impl FaultVfs {
    pub fn new(seed: u64) -> Arc<Self>;
    pub fn set_plan(&self, plan: FaultPlan);
    /// Apply the crash model and clear `crashed`. Returns what was torn, for logs.
    pub fn crash(&self) -> CrashReport;
    /// Number of mutating ops so far; tests use it to enumerate `crash_after_ops`.
    pub fn mutating_ops(&self) -> u64;
    /// Byte-level fault injection for corruption tests.
    pub fn corrupt(&self, path: &Path, offset: u64, bytes: &[u8]);
}
```

Tests enumerate crashes two ways: by name (`crash_at`) for documented steps, and exhaustively by count (`crash_after_ops = k` for every `k` from 0 to the op count of a clean run), which catches steps nobody named.

## Engine, CollectionHandle and Version

### Engine Structs

```rust
/// One per process. Cheap to clone.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    config: EngineConfig,
    vfs: Arc<dyn Vfs>,
    root: PathBuf,
    /// Held for the engine's lifetime (Phase 0 bug 6).
    _root_lock: Box<dyn VfsLock>,
    /// Collections keyed by (database, name). Populated once at open;
    /// changed only by create, drop, and rename. Short critical sections only.
    collections: RwLock<HashMap<CollectionRef, Arc<CollectionHandle>>>,
    catalog: CatalogFiles,
    runtime: Runtime,
    cache: Arc<BufferCache>,
    scheduler: MaintenanceScheduler,
    gc: GcQueue,
    /// Injected by `logpose-core`; implemented in `logpose-query`.
    resolver: Arc<dyn RowSetResolver>,
    /// Engine-wide clock; a manual clock in tests (token TTL, age triggers).
    clock: Arc<dyn Clock>,
    shutdown: tokio_util::sync::CancellationToken,
}

pub struct EngineConfig {
    pub memory_limit: u64,
    pub io_threads: usize,          // default 8
    pub query_threads: usize,       // default available_parallelism
    pub maintenance_threads: usize, // default max(1, available_parallelism / 4)
    pub group: GroupCommitConfig,
    pub memtable: MemtableConfig,
    pub compaction: CompactionConfig,
    pub token_ttl: Duration,        // default 5 min
    pub max_tokens_per_collection: usize, // default 64
    pub wal_file_bytes: u64,        // default 64 MiB
}

pub struct Runtime {
    pub io: IoPool,
    pub query: rayon::ThreadPool,
    pub maintenance: rayon::ThreadPool,
}

/// Per collection. Shared by readers, the writer task, and jobs.
pub struct CollectionHandle {
    pub meta: Arc<CollectionMeta>,
    /// The published state. Readers `load_full()`; only the writer `store()`s.
    current: ArcSwap<Version>,
    /// Latest published visible_seq_no, for read-barrier waits.
    visible: tokio::sync::watch::Sender<SeqNo>,
    /// Client requests. Bounded: backpressure when full.
    requests: tokio::sync::mpsc::Sender<WriteRequest>,
    /// Job completions and permits. Unbounded, polled first (biased select).
    control: tokio::sync::mpsc::UnboundedSender<ControlMsg>,
    tokens: TokenRegistry,
    /// Open, ReadOnly (poisoned), Dropping, Closed.
    state: AtomicU8,
    /// First fatal error; set once when the writer poisons the collection.
    poison: OnceLock<Arc<LogPoseError>>,
}

/// Immutable identity and configuration. Replaced (new Arc) only on rename.
pub struct CollectionMeta {
    pub id: CollectionId,
    pub reference: CollectionRef,
    pub dir: PathBuf,
    pub config: CollectionConfig, // flush and compaction overrides
}
```

Descriptor lookup is a `HashMap` lookup under a read lock held for nanoseconds. `find_collection_descriptor`'s directory scan is deleted.

### Version

```rust
/// A complete, immutable, self-consistent view of one collection.
/// Everything reachable from a Version is immutable after publication,
/// except the buffer cache and FileHandle's obsolete flag, which have their
/// own synchronization.
pub struct Version {
    /// Strictly increasing per collection (I6).
    pub id: VersionId,
    pub meta: Arc<CollectionMeta>,
    pub schema: Arc<Schema>,
    /// Last sequence number of the last batch included (I3).
    pub visible_seq_no: SeqNo,
    /// Durable manifest at publish time (diagnostics only; not a read key).
    pub manifest_generation: u64,
    pub checkpoint_seq_no: SeqNo,
    /// Frozen memtables being flushed, oldest first. At most
    /// `memtable.max_frozen` (default 2).
    pub frozen: Arc<[Arc<MemtableData>]>,
    /// The active memtable as of `visible_seq_no`.
    pub active: Arc<MemtableData>,
    /// Segments, ascending by UnitId.
    pub segments: Arc<[Arc<SegmentHandle>]>,
    /// Deletion vectors for every unit that has any deleted row.
    pub deletes: DeletionMap,
    pub counters: VersionCounters,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VersionId(pub u64);

/// Persistent map; clone is O(1), update is O(log n) path copy.
#[derive(Clone, Default)]
pub struct DeletionMap(imbl::HashMap<UnitId, DeletionVector>);

#[derive(Clone, Copy, Debug, Default)]
pub struct VersionCounters {
    pub total_rows: u64,   // Σ row_count over units
    pub deleted_rows: u64, // Σ dv cardinality over units
    pub segment_count: u32,
    pub memtable_rows: u64,
    pub memtable_bytes: u64,
}
```

`stats` is O(units): counters live on the `Version`, and per-unit counts are `row_count - dv.len()`.

### Publication Protocol

The writer is the only code that calls `current.store`. Publication of a candidate `Version` `V` for group `N`:

1. The writer has applied group `N` to its private state and built `V` from it (all persistent structures, so building is O(fields) `Arc` clones).
2. The WAL append and fsync for group `N` completed successfully.
3. `current.store(Arc::new(V))`.
4. `visible.send_replace(V.visible_seq_no)`.
5. Complete every ack in group `N` with `CommitAck { last_seq_no, applied_ops, version: V.id }`.

Ack strictly after store gives I1: a client that has the ack and then calls `load_full` observes `V` or later, because `ArcSwap::store` is linearizable with respect to later loads.

Maintenance commits (flush, compaction) publish the same way, with no WAL frame.

### Single Writer Task

Each collection has one writer task on the tokio runtime. It owns `WriterState` exclusively; nothing else can touch it, so it has no locks.

```rust
pub enum WriteRequest {
    /// Upserts, deletes, and partial updates by pk. Atomic.
    Batch { ops: Vec<ClientOp>, ack: oneshot::Sender<Result<CommitAck>> },
    /// Resolved by the writer against its latest state (see WAL v2).
    DeleteByFilter { filter: FilterExpr, ack: oneshot::Sender<Result<CommitAck>> },
    UpdateByFilter { filter: FilterExpr, patch: Patch, ack: oneshot::Sender<Result<CommitAck>> },
    AlterSchema { change: SchemaChange, ack: oneshot::Sender<Result<CommitAck>> },
    /// Freeze and wait for the resulting flush to commit.
    Flush { ack: oneshot::Sender<Result<FlushAck>> },
    Compact { ack: oneshot::Sender<Result<CompactAck>> },
    Shutdown { ack: oneshot::Sender<()> },
}

pub enum ClientOp {
    Upsert(Record),
    Update { pk: PrimaryKey, patch: Patch },
    Delete(PrimaryKey),
}

pub enum ControlMsg {
    PermitGranted { kind: JobKind, permit: JobPermit },
    FlushDone(Result<FlushOutput>),
    CompactionDone(Result<CompactionOutput>),
    Tick, // every 100 ms: age triggers, pk rewrite slices
}

struct WriterState {
    schema: Arc<Schema>,
    next_seq_no: SeqNo,
    next_unit_id: u32,
    epoch: Epoch,
    wal: WalWriter,
    active: MemtableData,              // mutated in place (persistent structures)
    frozen: VecDeque<Arc<MemtableData>>,
    segments: Arc<[Arc<SegmentHandle>]>,
    deletes: DeletionMap,
    pk_index: PkIndex,
    durable: DurableState,             // manifest generation, checkpoint, DV gens, live files
    jobs: InFlightJobs,                // flush ticket, compaction tickets, reserved inputs
    counters: VersionCounters,
    last_version_id: VersionId,
}
```

The loop uses a biased `tokio::select!` that drains `control` before `requests`, so job commits are never stuck behind a write backlog.

### Group Commit

Group commit is a two-stage pipeline with at most one WAL I/O in flight:

```text
 requests ──► collect G(n+1) ──► prepare G(n+1) (writer) ────┐
                                                            │ wait io(n) done
                    io(n): append frames + fsync (I/O pool) ─┤
                                                            ▼
                                   publish V(n), ack G(n) ──► start io(n+1)
```

Algorithm, per iteration:

1. **Collect.** Take the first request (await). Then drain with `try_recv` until `max_group_requests` (256) or `max_group_bytes` (16 MiB) is reached or the channel is empty. If `commit_delay` (default 0) is non-zero and the group is smaller than `min_group_requests`, wait up to `commit_delay` for more. The default of 0 relies on the pipeline for batching: while `io(n)` runs, requests accumulate into `G(n+1)`.
2. **Prepare** (in the writer task, while `io(n)` is in flight). CPU work that scales with batch size (validation, cosine normalization, postcard encoding) runs on the query pool through `run_cpu` over owned request data; `apply` runs inline for groups under 64 rows and otherwise on the query pool with `WriterState` moved in and returned. For each request in arrival order:
   1. Validate against the schema (types, dimensions, required vectors, pk type, duplicate pk within the batch, frame size limit). On failure, fail that request's ack and skip it. No sequence number is consumed.
   2. For `Update` and filter requests, gather the old rows: memtable rows are read directly; segment rows go through a fetch stage on the I/O pool (the writer awaits it). On I/O error, fail that request only.
   3. Resolve filters through `RowSetResolver` against a `ReadView` over the writer's current private state (not the published one), so resolution sees every prior batch in order.
   4. Convert to `RowOp`s (full row images and pk deletes), assign `first_seq_no..=last_seq_no`, apply to the private state with `apply`, and encode one WAL frame.
   5. After the group, build the candidate `Version` and evaluate flush triggers. If one fires, set `freeze_pending`: the writer stops collecting, drains the pipeline (steps 3 and 4 for this group, then awaits its I/O and publishes it), and freezes before collecting the next group, so the frozen memtable's operations are exactly those in WAL files that end before the rotation.
3. **Await `io(n)`**, then publish `V(n)` and ack `G(n)` as in the publication protocol.
4. **Start `io(n+1)`**: one `append` call with all frames of `G(n+1)` as `IoSlice`s, then one `sync_data`. Frames are never split across `append` calls, so a torn write can only damage the last frames of the last group.

Maintenance messages drain the pipeline: when a `ControlMsg` is pending, the writer stops collecting, awaits the in-flight `io(n)`, publishes `V(n)`, and only then handles the message. Every maintenance step that runs on the writer (freeze, flush begin, compaction begin, reconcile, commit) therefore sees private state equal to the published state. The cost is one pipeline bubble per maintenance event.

Decision: one frame per client batch, many frames per fsync. The frame is the atomicity unit (I3) and the replication unit (Phase 7). A multi-batch frame would save 48 header bytes per batch and couple independent batches' fate for no benefit.

Decision: apply before fsync, publish after. Applying early lets `prepare(n+1)` see `G(n)`'s effects (a partial update of a pk inserted by the previous group) without waiting for the disk. The cost is that an fsync failure cannot be rolled back, so it poisons the collection:

- `io(n)` error: fail every ack in `G(n)` and `G(n+1)` with `LogPoseError::WalWriteFailed`, set `state = ReadOnly`, store the error in `poison`, stop accepting requests. The last published `Version` keeps serving reads. Reopening the engine recovers from what is on disk. This matches the fault model: after a failed fsync the page cache state is unknowable.

### Readers Pinning a Version

- A read calls `handle.current.load_full()` and holds the `Arc<Version>` for the whole request. It never blocks and never takes a lock.
- `arc_swap::Guard` (from `load()`) is allowed only for sub-microsecond peeks such as reading `visible_seq_no`, because guards occupy per-thread debt slots.
- A pinned `Version` keeps its memtables, segment handles, and deletion vectors alive; that is what makes I7 hold without any reader bookkeeping.
- A read barrier `min_seq_no` waits on `visible.subscribe()` until `visible_seq_no >= min_seq_no` or `barrier_timeout` (default 0, meaning fail immediately with `LogPoseError::ReadBarrierNotSatisfied`).

### Snapshot Tokens

```rust
/// Opaque to clients: base64url of the 36 bytes below.
pub struct SnapshotToken {
    pub collection_id: CollectionId, // 16 bytes
    pub version_id: VersionId,       // 8 bytes
    pub nonce: u64,                  // 8 bytes, random; prevents guessing
    pub crc: u32,                    // crc32c of the above
}

pub struct TokenRegistry {
    inner: Mutex<HashMap<(VersionId, u64), Pin>>,
}

struct Pin {
    version: Arc<Version>,
    expires_at: Instant,
}
```

- `ReadOptions { pin: true }` creates a token for the `Version` the read used, with `expires_at = now + token_ttl`.
- Every use of a token extends `expires_at` to `now + token_ttl` (sliding expiry), so a scroll that keeps paging never loses its snapshot.
- An unknown or expired token fails with `LogPoseError::SnapshotExpired`. Clients restart the scroll.
- A collection holds at most `max_tokens_per_collection` pins; creating one more fails with `LogPoseError::TooManySnapshots`. Pinned versions hold obsolete segment files on disk, so the cap bounds disk growth.
- One engine-wide reaper task runs every second and drops expired pins. Dropping a pin may drop the last reference to a `Version`, which may enqueue file deletions (see GC).

The registry mutex is held only for a hash map operation, never across I/O or `.await`.

### Memory Ordering and Locking Rules

1. The only cross-thread publication edge for engine state is `ArcSwap<Version>`: the writer's `store` happens-before any `load` that returns the new pointer. Everything reachable from a `Version` is either immutable or internally synchronized.
2. Internally synchronized objects: `BufferCache` (sharded mutexes plus atomics), `FileHandle::obsolete` (`AtomicBool`, `Release` store, `Acquire` load), metric counters (`Relaxed`).
3. No lock is held across `.await`, across I/O, or across a call into another component. Every critical section is a map or queue operation.
4. No nested locks. The engine has no lock order because no code path holds two locks.
5. Rayon tasks never block on a future and never do I/O. Tokio tasks never do blocking I/O or more than about 50 µs of CPU work; larger work goes to a pool.
6. `std::sync::Mutex` is used (not `tokio::sync::Mutex`), because no lock is held across `.await`.

### Async and Blocking Boundaries

| Work | Where it runs | Why |
| --- | --- | --- |
| Network, request routing, writer loop, scheduler, token reaper | tokio | orchestration only |
| WAL append and fsync, segment and DV writes, manifest publish, cache misses, directory ops | `IoPool` | blocking syscalls; bounded concurrency |
| Query CPU (bitmap compile, scoring, graph walks, merges) | `query` rayon pool | latency-sensitive CPU |
| Batch prepare and apply | `query` rayon pool | short, latency-sensitive |
| Flush and compaction CPU (column encode, index builds, HNSW) | `maintenance` rayon pool | long-running; must not starve queries |

```rust
/// Fixed-size pool of blocking threads with a bounded queue.
pub struct IoPool { /* crossbeam channel + N std threads */ }

impl IoPool {
    /// Run `f` on an I/O thread; the returned future resolves with its result.
    pub fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = Result<T>> + Send;
}

/// Run `f` on a rayon pool and await it from async code.
pub fn run_cpu<T: Send + 'static>(
    pool: &rayon::ThreadPool,
    f: impl FnOnce() -> T + Send + 'static,
) -> impl Future<Output = Result<T>> + Send;
```

A dedicated `IoPool` rather than `tokio::task::spawn_blocking`, because `spawn_blocking` has a 512-thread default shared with everything else and gives no queue-depth metric. Queries follow a staged pattern (fetch on `IoPool`, then compute on rayon), described in [Read Path](#read-path).

## WAL v2

### WAL Files

A collection's WAL is a directory of files named by the first sequence number they may contain:

```text
wal/00000000000000000001.wal
wal/00000000000000048213.wal   <- active (highest name)
```

- Only the highest-named file is ever appended to. Older files are never modified.
- Rotation happens when the active file exceeds `wal_file_bytes` (64 MiB) and at every memtable freeze, so that a checkpoint always falls on a file boundary.
- A file is deletable when the next file's first sequence number is at most `checkpoint_seq_no + 1` of a durable manifest (all its operations are checkpointed).
- `active.wal`, rolled files named by checkpoint, and `PENDING_ROTATION` are deleted from the design. Rotation never truncates or renames a file that holds uncheckpointed data, which removes Phase 0 bug 2 by construction.

### Frame Layout

All integers are little-endian. All CRCs in v2 formats are CRC-32C (Castagnoli, hardware-accelerated on x86 and ARM, the storage-industry default).

```text
offset size field
     0    4 magic          0x3257_504C ("LPW2")
     4    4 header_crc     crc32c(bytes 8..48)
     8    4 payload_len    bytes of payload, <= MAX_FRAME_PAYLOAD (64 MiB)
    12    1 frame_type     1 = WriteBatch, 2 = SchemaChange, 3 = Checkpoint
    13    1 flags          bit 0 reserved for compression (0); bit 1 GROUP_END
    14    2 format_version 1
    16    8 epoch          ownership epoch, 0 until Phase 7
    24    8 first_seq_no
    32    8 last_seq_no
    40    4 payload_crc    crc32c(payload)
    44    4 reserved       0
    48    n payload        postcard-encoded WalPayload
  48+n    p padding        zeros so the next frame starts 8-byte aligned (not CRC-covered)
```

The header has its own CRC so that a torn header is detected before `payload_len` is trusted. Replay rejects a frame whose `payload_len` exceeds `MAX_FRAME_PAYLOAD` or the remaining file length. A batch whose encoding exceeds `MAX_FRAME_PAYLOAD` is rejected at validation with `LogPoseError::BatchTooLarge`.

Sequence rules:

- `WriteBatch` covers `first_seq_no..=last_seq_no`, one sequence number per `RowOp`.
- `SchemaChange` consumes exactly one sequence number (`first == last`).
- `Checkpoint` consumes none. It carries `first == last == checkpoint_seq_no` and is excluded from the contiguity check.
- Across data frames, `first_seq_no` must equal the previous data frame's `last_seq_no + 1`. A gap in the uncheckpointed range is corruption.
- The last frame of every fsync group has `GROUP_END` set. Only the frames after the last durable `GROUP_END` can be unsynced, which is what lets tail repair tell a torn tail from corruption.

### Record Types

```rust
/// Postcard-encoded frame payload. The variant must match `frame_type`.
#[derive(Serialize, Deserialize)]
pub enum WalPayload {
    WriteBatch(WriteBatchPayload),
    SchemaChange(SchemaChangePayload),
    Checkpoint(CheckpointPayload),
}

#[derive(Serialize, Deserialize)]
pub struct WriteBatchPayload {
    /// Schema version the rows were validated against.
    pub schema_version: u32,
    /// One sequence number each, in order, starting at first_seq_no.
    pub ops: Vec<RowOp>,
}

#[derive(Serialize, Deserialize)]
pub enum RowOp {
    /// Blind write of a complete row image. Upserts, partial updates, and
    /// update-by-filter all become `Put` with the merged full row.
    Put(RowImage),
    /// Blind delete by key. Delete-by-filter becomes many `Delete`s.
    Delete(PrimaryKey),
}

/// A row normalized to the schema.
#[derive(Serialize, Deserialize)]
pub struct RowImage {
    pub pk: PrimaryKey,
    /// Sparse (FieldId, vector) pairs sorted by FieldId; absent = null.
    /// Cosine fields are already normalized. Each vector is encoded as
    /// length-prefixed little-endian f32 bytes (serde_bytes).
    pub vectors: Vec<(FieldId, F32Bytes)>,
    /// Sparse (FieldId, Value) pairs sorted by FieldId; absent = null.
    pub scalars: Vec<(FieldId, Value)>,
    /// Undeclared keys (`$extra`), as a Value::Object.
    pub dynamic: Option<Value>,
}

#[derive(Serialize, Deserialize)]
pub struct SchemaChangePayload {
    /// The complete new schema, including FieldId assignments and dropped-field tombstones.
    pub schema: Schema,
}

#[derive(Serialize, Deserialize)]
pub struct CheckpointPayload {
    pub manifest_generation: u64,
    pub checkpoint_seq_no: SeqNo,
}
```

Decision: operations are resolved into blind writes (full row images and deletes by key) at write time, never at apply time. Reasons:

- **Replay correctness over a fuzzy checkpoint.** Recovery starts from segments plus DV files that may already reflect some operations after the checkpoint (see [Deletion Vectors](#deletion-vectors)). Replaying a blind write over such a base is idempotent: the final state for a key depends only on the last operation on that key. A partial update or a filter delete replayed over a slightly different base could produce a different result.
- **Replay is independent of code.** Replay never evaluates predicates, reads old rows, or consults indexes, so a change in predicate semantics or an index bug cannot change what recovery produces.
- **Replication (Phase 7).** A replica applies frames without the primary's state.

Costs: a partial update logs the full row, including a 3 KB vector the client did not send; a large delete-by-filter logs every key (about 9 bytes per `int64` key). Both are acceptable next to the alternative's correctness risk.

Delete-by-filter and update-by-filter atomicity: the writer resolves the filter and writes one frame when the result fits `MAX_FRAME_PAYLOAD`. When it does not, the writer commits successive frames, each an atomic batch resolved against the state after the previous one, and the ack reports `chunks > 1`. This is a deliberate limit (see [Deviations From the Plan](#deviations-from-the-plan)).

Checkpoint frames are written as the first frame of every new WAL file and after every flush commit. Recovery uses them only as a cross-check (`manifest.checkpoint_seq_no >= marker`). They exist so a WAL tailer in Phase 7 learns what is safe to discard without reading manifests.

### Encoding Choice

Decision: postcard (via serde) for payloads, a hand-written layout for the frame header. The payload types (`Schema`, `Value`, `PrimaryKey`) are serde types owned by the schema PR, and a hand-written codec for them would duplicate that work and drift. Postcard is compact (varints) and deterministic. It is not self-describing, so `format_version` gates decoding and a golden-bytes test pins the encoding. Vectors use a `serde_bytes` wrapper (`F32Bytes`), so a 768-dim vector is one 3072-byte copy. The WAL is short-lived, so format evolution is handled by refusing to open a WAL with an unknown `format_version`.

### Tail Repair

On open, for the highest-named file only:

1. Scan frames from offset 0. Stop at the first frame that fails any check: short header, bad magic, bad `header_crc`, oversized or out-of-file `payload_len`, bad `payload_crc`, undecodable payload, or sequence discontinuity.
2. Let `valid_end` be the end offset (after padding) of the last good frame.
3. Distinguish a torn tail from corruption: search from the failed offset to EOF, at 8-byte steps, for frames with valid magic, `header_crc`, and `payload_crc`. Because a group is one append followed by one fsync, and the next group is appended only after that fsync returns, at most one group (the last) can be partially persisted, and a page cache may persist its pages in any order. So valid frames after the failure are expected, but only within one group. If some valid frame after the failure has `GROUP_END` set and is followed by any further valid frame, a later group was durably written after the damaged one: fail the collection open with `LogPoseError::WalCorrupt { file, offset }` and modify nothing.
4. Otherwise it is a torn tail: `set_len(valid_end)`, `sync_all`, `crash_point(RecoveryAfterTailRepair)`.
5. A bad frame in any file other than the highest-named one is corruption, unless the whole file is at or below the checkpoint (then it is skipped and deleted by orphan cleanup).

Appends only happen after tail repair (fixes Phase 0 bug 1). Media corruption inside the last group, with nothing written after it, is indistinguishable from a torn write and is truncated; the recovery report logs the truncated byte count and frame range either way. Any frame that passes all checks is replayed even if its batch was never acknowledged; the client of an unacknowledged batch must treat the outcome as unknown, which is standard.

### Rotation and Checkpoint Protocol

Rotation (writer, between groups, after the last `io(n)` completed):

1. `vfs.open(wal/<next_seq_no:020>.wal, CreateNew)`.
2. Append a `Checkpoint` frame (its own group, so `GROUP_END` is set) with the current durable manifest's generation and checkpoint, then `sync_data`.
3. `sync_dir(wal/)`, then `crash_point(WalAfterRotateCreate)`.
4. Switch `WalWriter` to the new file. The old file is never appended again.

A crash before step 3 may leave an empty file, a partial file, or no file; replay handles all three because the old file is complete and the new file holds at most a torn tail.

Checkpointing is the flush commit: publishing manifest `M` with `checkpoint_seq_no = C` makes every WAL file whose successor starts at or below `C + 1` obsolete. The writer enqueues those files for deletion after `M` is durable.

## Memtable

### Row Storage

A memtable is append-only in slots. All structures are persistent (`imbl`, the maintained fork of `im`), so the writer mutates its private copy in place and a published `Version` holds an O(1) clone that later writes never change.

```rust
/// Clone is O(number of fields): every field is an Arc or a persistent
/// structure. The writer owns one copy; each published Version holds a clone.
#[derive(Clone)]
pub struct MemtableData {
    pub unit: UnitId,
    pub schema: Arc<Schema>,
    /// Seq range of operations applied to this memtable.
    pub first_seq_no: SeqNo,
    pub last_seq_no: SeqNo,
    /// Engine-clock time of the first slot (monotonic), for the age trigger.
    pub created_at: Instant,
    /// Number of slots; slot ids are 0..slot_count.
    pub slot_count: u32,
    /// Per slot.
    pub pks: imbl::Vector<PrimaryKey>,
    pub seq_nos: imbl::Vector<SeqNo>,
    /// pk -> latest slot in this memtable (live or dead). Ordered, so it also
    /// serves keyset scroll by pk.
    pub pk_to_slot: imbl::OrdMap<PrimaryKey, u32>,
    /// One arena per live vector field, sorted by FieldId.
    pub vectors: Vec<(FieldId, VectorArena)>,
    /// One column per live scalar field, sorted by FieldId.
    pub columns: Vec<(FieldId, MemColumn)>,
    /// `$extra`, binary Value encoding.
    pub dynamic: imbl::Vector<Option<Arc<[u8]>>>,
    /// Live indexes over slots, per indexed field.
    pub indexes: Vec<(FieldId, MemScalarIndex)>,
    pub bytes: MemtableBytes,
}

/// Contiguous f32 storage in fixed blocks of BLOCK_ROWS rows.
pub struct VectorArena {
    pub dim: u32,
    /// Full blocks: BLOCK_ROWS * dim floats each. Never modified.
    pub blocks: imbl::Vector<Arc<[f32]>>,
    /// Partial last block, fewer than BLOCK_ROWS rows. Replaced (copy-on-write) when rows are added.
    pub tail: Arc<[f32]>,
    /// Slots whose vector is null (stored as zeros).
    pub nulls: Arc<RoaringBitmap>,
}
pub const BLOCK_ROWS: usize = 16;

pub enum MemColumn {
    Bool(imbl::Vector<Option<bool>>),
    Int64(imbl::Vector<Option<i64>>),
    Float64(imbl::Vector<Option<f64>>),
    Timestamp(imbl::Vector<Option<i64>>),
    String(imbl::Vector<Option<Arc<str>>>),
    Array(imbl::Vector<Option<Arc<[Value]>>>),
    Json(imbl::Vector<Option<Arc<[u8]>>>),
}
```

The vector arena is the only structure whose copy-on-write cost is not O(log n): appending to a shared tail copies at most `BLOCK_ROWS - 1` rows, once per group per vector field (the writer appends a whole group's rows in one step). At 768 dimensions that is at most 45 KB per group, and full 16-row blocks give the exact-scan kernels 48 KB contiguous runs.

Memtable slot deletions are not stored in `MemtableData`. They live in `Version::deletes` under the memtable's `UnitId`, exactly like a segment's deletion vector. That lets a frozen memtable stay immutable while the writer keeps deleting its rows.

### Mutable Scalar Indexes

```rust
/// Over slot ids. Persistent: updating one key path-copies O(log n) nodes and
/// copy-on-writes that key's bitmap.
pub enum MemScalarIndex {
    /// bool, string, int64, and array elements: equality and IN.
    Inverted {
        terms: imbl::OrdMap<IndexKey, Arc<RoaringBitmap>>,
        nulls: Arc<RoaringBitmap>,
    },
    /// int64, float64, timestamp: ranges and order_by. Keys are total-ordered
    /// (floats via a total-order wrapper; NaN rejected at validation).
    Sorted {
        values: imbl::OrdMap<OrderedKey, Arc<RoaringBitmap>>,
        nulls: Arc<RoaringBitmap>,
    },
}
```

`auto` indexing (D4) creates `Inverted` for `bool`, `string`, and arrays, and both `Sorted` and `Inverted` for numbers and timestamps. Indexes include dead slots; readers always `AND NOT` the unit's deletion vector, so slots never need to be removed from bitmaps. Copying a touched key's bitmap per group is bounded by the memtable size, at most a few KB per touched key.

### Upsert of an Existing Key

Decision: a new slot plus a deletion bit on the old slot, never in-place slot reuse. In-place reuse would mutate data that published `Version`s can see (breaking I4) unless every slot were copy-on-write, and it would need removal from every index bitmap. Append plus delete keeps every memtable structure append-only; the cost is dead slots, which count toward the flush trigger and are dropped at flush.

`apply` (one function, shared by the live writer and WAL replay), for each `RowOp` with sequence number `seq`:

1. `old = pk_index.resolve(pk)`, following forwarding tables (see [Primary-Key Index](#primary-key-index)).
2. If `old` is `Some(addr)`: `deletes.mark(addr)`.
3. For `Put(row)`: append slot `s` to the active memtable (pk, seq, vectors, columns, dynamic, index entries); `pk_to_slot.insert(pk, s)`; `pk_index.insert(pk, RowAddr { unit: active.unit, row: s })`.
4. For `Delete(pk)`: `pk_index.remove(pk)`. If `old` was `None`, the operation changes nothing but still consumes its sequence number.
5. Update counters.

### Size Accounting and Flush Triggers

```rust
pub struct MemtableBytes {
    /// pk + vector + scalar + dynamic bytes, per slot, including dead slots.
    pub payload: u64,
    /// 64 bytes per slot for persistent-structure overhead, plus 16 bytes per index entry.
    pub overhead: u64,
}

pub struct MemtableConfig {
    pub max_bytes: u64,        // default 64 MiB
    pub max_rows: u32,         // default 1_000_000
    pub max_age: Duration,     // default 10 min; bounds replay time
    pub max_wal_bytes: u64,    // default 256 MiB since checkpoint (delete-heavy workloads)
    pub max_frozen: usize,     // default 2
    pub global_fraction: f32,  // default 0.125 of memory_limit across all collections
    pub write_stall_timeout: Duration, // default 30 s
}
```

The writer freezes the active memtable after a group when any of these hold: `payload + overhead >= max_bytes`; `slot_count >= max_rows`; `created_at` older than `max_age`; WAL bytes since the durable checkpoint `>= max_wal_bytes`; or the engine asks because the global memtable budget is exceeded (largest memtable first). The WAL-bytes trigger exists because deletes of segment rows add nothing to the memtable but grow the WAL. A flush with an empty memtable is a pure checkpoint: DV files plus a manifest.

When `frozen.len() == max_frozen` and the active memtable hits a trigger, the writer stops draining `requests` until a flush commits, so the bounded channel pushes back on clients. A request that waits longer than `write_stall_timeout` fails with `LogPoseError::WriteStalled`.

### Frozen Memtable

Freeze (writer, between groups):

1. Move the active `MemtableData` into `frozen` as `Arc<MemtableData>` (no copy).
2. Create a new active memtable with a fresh `UnitId` and `first_seq_no = next_seq_no`.
3. Rotate the WAL so that the new file starts at `next_seq_no`.
4. Publish a `Version` whose `frozen` list includes the frozen memtable. Reads continue against it unchanged.
5. Request a flush permit from the scheduler.

A frozen memtable is never mutated. Deletions of its rows after the freeze accumulate in `deletes[frozen.unit]`.

## Primary-Key Index

Decision: the primary-key index is private to the writer task and is not published through `Version`. Readers never need a global `pk -> location` map:

- `get(pk)` probes each unit's own immutable key structure (a memtable's `pk_to_slot`; a segment's pk filter and sorted pk section) newest-first and returns the first live hit. By I5 at most one unit has a live row.
- Scroll by pk merges each unit's sorted pk order.

The writer needs the map only to find the old row that an upsert or delete must mark, and it is the only mutator. A published persistent map (HAMT) at 10M keys costs roughly two to three times the memory of a flat hash table and adds path copying to every write. Copy-on-write shards copy a whole shard whenever a reader still holds the old `Version`, which under concurrent reads means megabytes per group. A private flat table has neither cost.

```rust
pub struct PkIndex {
    map: hashbrown::HashMap<PkKey, RowAddr, foldhash::fast::RandomState>,
    /// Retired units whose rows moved (flush, compaction), not yet rewritten.
    forwards: HashMap<UnitId, Forwarding>,
    /// Incremental rewrite queue, processed in slices between groups.
    rewrites: VecDeque<RewriteTask>,
}

/// Compact key: int64 inline; strings inline up to 24 bytes (compact_str).
pub enum PkKey {
    Int(i64),
    Str(compact_str::CompactString),
}

pub struct Forwarding {
    pub target: UnitId,
    /// Old row -> new row, or u32::MAX when the row was not copied
    /// (it was already deleted in the job's snapshot).
    pub map: Arc<[u32]>,
}

struct RewriteTask {
    target: UnitId,
    /// New unit's pks in row order, and for each new row its old address.
    pks: Arc<[PrimaryKey]>,
    sources: Arc<[RowAddr]>,
    next_row: u32,
}
```

Lookup resolves forwarding: while the address's unit is in `forwards`, replace it with `RowAddr { unit: target, row: map[row] }`. Chains are at most as long as the number of unfinished rewrites, normally one.

Incremental rewrite: after every group and on every `Tick`, the writer processes up to `pk_rewrite_slice` (65,536) rows of the front task. For new row `o`: if the raw (unresolved) map entry for `pks[o]` equals `sources[o]`, set it to `RowAddr { unit: target, row: o }`; otherwise leave it, because the key moved or was deleted since. When a task finishes, the writer removes its `forwards` entries. This bounds the writer stall for a multi-million-row compaction to a few milliseconds per slice instead of one long pause.

Memory per row at load factors between 0.44 and 0.875:

| Key type | Entry | Per row with overhead | 10M rows |
| --- | --- | --- | --- |
| `int64` | 8 + 8 bytes + 1 control byte | about 24 bytes | about 240 MB |
| `string` up to 24 bytes | 24 + 8 bytes + 1 control byte | about 45 bytes | about 450 MB |
| `string` longer than 24 bytes | inline header + 8 + 1 + heap | about 60 bytes + length | about 1 GB at 40-byte keys |

The index is a fixed reservation against `memory_limit`, not buffer-cache memory. Long string keys are the expensive case; mapping a 64-bit hash to a location with a collision check against the unit's pk column is the documented fallback if a workload needs it, not built in Phase 2.

Rebuild on open (see [Recovery on Open](#recovery-on-open)):

1. For each segment in ascending `UnitId` order, on the maintenance pool: read the pk section and the row-meta section through the I/O pool into transient buffers (not cached), and emit `(pk, RowAddr)` for every row not set in the segment's deletion vector.
2. Insert into the map. On a duplicate live key (an I5 violation, which the engine prevents), keep the row with the higher sequence number, set the other's deletion bit in memory, increment `pk_duplicates_repaired`, and log an error. Tests run with `strict_invariants = true`, which turns this into a failed open.
3. WAL replay then updates the map through `apply`.

At 10M `int64` keys the rebuild reads about 120 MB of pk and seq columns and inserts 10M entries; with segments processed in parallel into per-segment vectors and a single-threaded insert, it takes about one to two seconds.

## Deletion Vectors

### DV Structure

```rust
/// Deleted rows of one unit. Clone is two Arc increments.
#[derive(Clone)]
pub struct DeletionVector {
    /// Large, rarely copied.
    pub base: Arc<RoaringBitmap>,
    /// Recent deletions. Copy-on-write per group; folded into `base` when
    /// recent.len() > max(4096, base.len() / 8).
    pub recent: Arc<RoaringBitmap>,
    /// Cached base.len() + recent.len(); the two are disjoint.
    pub len: u64,
}

impl DeletionVector {
    pub fn contains(&self, row: RowId) -> bool;
    /// bitmap := bitmap AND NOT self.
    pub fn subtract_from(&self, bitmap: &mut RoaringBitmap);
    /// Set the bit; return false if it was already set.
    pub fn mark(&mut self, row: RowId) -> bool;
    /// base OR recent, for serialization.
    pub fn to_bitmap(&self) -> RoaringBitmap;
}
```

Two tiers make copy-on-write per `Version` cheap: a published `Version` shares `base`, and the writer copies only the small `recent` bitmap on its first mark after a publish. The fold copies `base` at most once per one-eighth growth, so the amortized copy cost per deletion is O(1).

### Marking Rows

- **Delete of a key whose live row is in a segment:** `deletes[segment].mark(row)`.
- **Upsert or partial update of such a key:** build the full image first (for an update, from the old row), then mark the segment row and append the new row to the active memtable. Both happen in one `apply` call, inside one group, so no `Version` shows zero or two live rows for the key (I5).
- **Any operation on a key whose live row is in a memtable (active or frozen):** mark the slot in `deletes[memtable.unit]`.
- **Address in a retired unit:** resolve through forwarding first, so the bit lands on the unit that the current `Version` contains.

### DV Files

Layout of `segments/<unit:08x>.dv.<generation:08>`:

```text
offset size field
     0    8 magic            "LPDV" 0x00 0x00 0x02 0x00
     8    4 unit_id
    12    4 generation
    16    4 row_count        the segment's row count, for validation
    20    4 reserved
    24    8 covered_seq_no   every deletion with seq <= this is included
    32    8 bitmap_len
    40    n bitmap           RoaringBitmap portable serialization
  40+n    4 crc32c           over bytes 0..40+n
```

DV files are immutable: each checkpoint writes a new generation, and the manifest names the generation in force for each segment. DV files are written in exactly two places:

- **Flush job.** At flush start the writer snapshots every segment's `DeletionVector` (Arc clones). The job writes a new generation for each segment whose cardinality differs from its durable generation's. `covered_seq_no` is the writer's `visible_seq_no` at the snapshot, which is at least the checkpoint the flush publishes.
- **Compaction commit.** The writer writes generation 1 of the output segment when reconciliation set any bit (see [Compaction](#compaction)).

A DV file may contain deletions newer than the manifest's checkpoint. That is safe because every WAL operation is a blind write. The argument that I9 plus blind writes gives I8:

- The recovered base state (segments minus DV files) can differ from the logical state at checkpoint `C` only for keys that some operation after `C` touched, because a DV bit can be early but never missing (I9), and every segment row is a copy of a row written at or before `C` (flush copies only a memtable frozen at or before `C`; compaction copies existing segment rows).
- Replay applies every operation after `C` in order. For a key touched after `C`, the final state is the last operation's blind write, independent of the base. For every other key, the base already equals the state at `C`.

### DV Recovery From WAL Replay

On open, each segment's deletion vector is loaded from the DV file generation named by the manifest, or is empty when none is named. WAL replay from `checkpoint_seq_no + 1` then re-marks rows through `apply`. No other deletion state is persisted.

### Consumption by Compaction

Compaction copies only rows not set in its input snapshot's deletion vectors and records old-to-new row maps; deletions that arrive while it runs are transferred at commit. The protocol is in [Compaction](#compaction).

## Segment v2 File Format

A segment is one immutable file, `segments/<unit:08x>.seg`, written once with `CreateNew` and appends, then fsynced. It replaces the v1 `.lps` file, the JSON entry table, the flat JSON sidecar, and the HNSW sidecar (which embedded copies of every vector and every metadata document).

### File Structure

```text
offset 0
+-------------------------------------------------------------+
| FileHeader                                       128 bytes  |
+-------------------------------------------------------------+ 128
| section 0 payload                                           |
| zero padding to a 64-byte boundary                          |
+-------------------------------------------------------------+
| section 1 payload                                           |
| ...                                                         |
+-------------------------------------------------------------+ table_offset (64-aligned)
| SectionTable: section_count x SectionEntry (64 bytes each)  |
+-------------------------------------------------------------+
| Footer                                            64 bytes  |
+-------------------------------------------------------------+ EOF
```

The section table sits at the end because section lengths are unknown until they are written, and compaction outputs are streamed rather than built in memory. A reader reads the first 128 bytes and the last `64 + 64 * section_count` bytes (in practice one 4 KiB read at each end) and has the whole directory.

Alignment rules:

- Every section payload starts at a multiple of 64 bytes from the start of the file. Padding bytes are zero and not covered by any CRC.
- Inside a section, every array of `u16`, `u32`, `u64`, `i64`, `f32`, or `f64` starts at an offset that is a multiple of its element size relative to the section start (and so absolute, because sections are 64-aligned). This makes zero-copy typed views possible with `bytemuck::try_cast_slice` over cache buffers, which are 8-byte aligned.
- f32 vector rows are packed with stride `dim * 4` and no per-row padding. Rows are 64-byte aligned whenever `dim` is a multiple of 16 (128, 768, 1536); the SIMD kernels use unaligned loads, so other dimensions only cost a little bandwidth.

### Header

```text
offset size field
     0    8 magic           "LPSEG" 0x00 0x02 0x00
     8    4 format_version  2
    12    4 flags           reserved, 0
    16   16 collection_id   UUID bytes
    32    4 unit_id
    36    4 schema_version
    40    8 schema_hash     xxh3_64 of the SchemaSnapshot section payload
    48    4 row_count
    52    4 reserved
    56    8 min_seq_no
    64    8 max_seq_no
    72   52 reserved        zeros
   124    4 header_crc      crc32c(bytes 0..124)
```

### Section Table Entry

```text
offset size field
     0    2 kind            SectionKind code (table below)
     2    2 field_id        FieldId, or 0xFFFF when not per-field
     4    2 encoding        kind-specific encoding code
     6    2 flags           reserved, 0
     8    8 offset          absolute, 64-aligned
    16    8 length          payload bytes, excluding padding
    24    4 crc32c          of the whole payload
    28    4 aux32           kind-specific (dimension, page_rows, ...)
    32    8 aux64           kind-specific (element counts, ...)
    40   24 reserved        zeros
```

### Footer

```text
offset size field
     0    8 table_offset
     8    4 section_count
    12    4 table_crc       crc32c of the whole section table
    16    8 file_len        total file length, including this footer
    24    4 header_crc      copy of the header's CRC, cross-check
    28   24 reserved        zeros
    52    4 footer_crc      crc32c(bytes 0..52)
    56    8 magic           "LPSEGEND"
```

Every byte except padding is covered: the header by `header_crc`, the table by `table_crc`, each payload by its entry's `crc32c`, and the footer by `footer_crc`. That fixes the v1 gap where the entry table was unchecked.

### Section Kinds

| Code | Kind | Per field | Load unit | Cache class | Payload format owner |
| --- | --- | --- | --- | --- | --- |
| 1 | `SchemaSnapshot` | no | at open | none (parsed) | storage |
| 2 | `RowMeta` | no | whole | transient | storage |
| 3 | `PkColumn` | no | whole | `PkIndex` | storage |
| 4 | `PkSorted` | no | whole | `PkIndex` | storage |
| 5 | `PkFilter` | no | whole | `PkIndex` | storage |
| 6 | `Stats` | no | whole | `ScalarIndex` | storage |
| 10 | `VectorF32` | yes | 8 KiB page | `RawVectors` | storage |
| 11 | `VectorSq8` | yes | whole | `GraphAndCodes` | `logpose-index` SQ8 |
| 12 | `VectorGraph` | yes | whole | `GraphAndCodes` | `logpose-index` HNSW |
| 20 | `ScalarColumn` | yes | whole | `ScalarColumns` | storage |
| 21 | `ScalarInverted` | yes | whole | `ScalarIndex` | `logpose-index` scalar |
| 22 | `ScalarSorted` | yes | whole | `ScalarIndex` | `logpose-index` scalar |
| 30 | `DynamicJson` | no | 4096-row block | `DynamicJson` | storage |

Unknown section kinds are ignored by readers, which is how Tier 2 adds codes and disk graphs later.

### Row and Key Sections

- `SchemaSnapshot`: postcard `Schema` at write time. Makes a segment decodable on its own and pins `FieldId` meaning.
- `RowMeta`: per-row sequence numbers. Encoding 1 = `u64` plain; encoding 2 = `u32` offsets from `aux64 = base` when `max - min < 2^32`.
- `PkColumn`: keys in row order. `int64`: `i64[row_count]`. `string`: `u32 offsets[row_count + 1]` then UTF-8 bytes.
- `PkSorted`: row ids sorted by key. `int64`: `i64 keys[row_count]` then `u32 rows[row_count]` (binary search needs no other section). `string`: `u32 rows[row_count]` sorted by key bytes; binary search reads `PkColumn`.
- `PkFilter`: binary fuse filter (8-bit fingerprints, about 9 bits per key, 0.4 percent false positives) over `xxh3_64(canonical_pk_bytes, seed 0)`, where canonical bytes are `0x01 ++ i64 LE` or `0x02 ++ UTF-8`. The hash and seed are part of the format.

### Vector Sections

`VectorF32` (`aux32 = dim`, `aux64 = row_count`):

```text
offset size field
     0    4 dim
     4    4 row_count
     8    4 page_rows       max(1, 8192 / (dim * 4))
    12    4 page_count
    16    8 nulls_len       bytes of the null bitmap (0 when no nulls)
    24   40 reserved
    64    n nulls           RoaringBitmap portable serialization, padded to 8
     .  4*p page_crcs       crc32c of each page's bytes
     .    . padding to 64
     .    . data            row_count * dim f32 LE, rows contiguous; null rows are zeros
```

Pages are the load unit for rerank: a page is `page_rows` consecutive rows, verified against its own CRC when loaded. The section CRC covers the whole payload and is checked by compaction and by `inspect --verify`.

`VectorSq8` and `VectorGraph` payloads are produced and parsed by `logpose-index`. The contract storage relies on:

```rust
// logpose-index, owned by the SQ8 and HNSW PRs.
impl Sq8Codes {
    /// Serialize params (per-dimension min and scale) and codes (row_count * dim u8).
    pub fn write_to(&self, out: &mut Vec<u8>);
    /// Zero-copy view over an 8-byte-aligned buffer.
    pub fn view(bytes: &[u8]) -> Result<Sq8View<'_>>;
}
impl HnswGraph {
    /// CSR payload: header (metric, M, ef_construction, entry point, max level),
    /// then per level: u32 node ids (level >= 1 only), u32 offsets[n + 1], u32 neighbors[].
    /// Node ids are segment row ids. Rows with null vectors are not in the graph.
    pub fn write_to(&self, out: &mut Vec<u8>);
    pub fn view(bytes: &[u8]) -> Result<HnswView<'_>>;
}
```

Small-segment policy: `VectorGraph` is written only when the segment has at least `graph_min_rows` non-null vectors for that field (default 20,000, calibrated by the Phase 0 harness); below it, search is an exact SIMD scan. `VectorSq8` is written when there are at least `sq8_min_rows` (default 1,024); below it, exact scans use f32 directly. A flush of a default 64 MiB memtable at 768 dimensions produces about 21,000 rows, so flush outputs sit near the threshold and compaction outputs are always above it.

### Scalar Column Encodings

`ScalarColumn` payload:

```text
offset size field
     0    1 encoding        see table
     1    1 value_width     bytes per code or value where relevant
     2    2 reserved
     4    4 row_count
     8    8 nulls_len
    16    8 dict_len        0 when no dictionary
    24    8 data_len
    32   32 reserved
    64    . nulls           RoaringBitmap portable serialization, padded to 8
     .    . dict            present for dictionary encodings, padded to 8
     .    . data
```

| Encoding | Type | Data |
| --- | --- | --- |
| 1 `Int64Plain` | `int64`, `timestamp` (micros) | `i64[row_count]`, nulls stored as 0 |
| 2 `Float64Plain` | `float64` | `f64[row_count]`, nulls stored as 0.0 |
| 3 `BoolBitmap` | `bool` | RoaringBitmap of true rows |
| 4 `StringDict` | `string` | dict: `u32 offsets[d + 1]` then sorted unique bytes; data: codes of `value_width` 1, 2, or 4 bytes |
| 5 `StringPlain` | `string` | `u32 offsets[row_count + 1]` then bytes; used when distinct values exceed half the rows |
| 6 `Array` | `array<T>` | `u32 offsets[row_count + 1]` into a child block encoded as one of the above over all elements |
| 7 `JsonValue` | `json` | `u32 offsets[row_count + 1]` then binary `Value` bytes |

A dropped field has no section in segments written after the drop; readers return null for fields whose `FieldId` has no section, which is also how added fields read in old segments.

### Dynamic JSON Column

`DynamicJson` stores `$extra` as the binary `Value` codec from the schema PR, in blocks of 4096 rows:

```text
header (64 bytes): row_count u32, block_rows u32 (4096), block_count u32, reserved
block index: block_count x { offset u64 (relative to section), len u32, crc32c u32 }
blocks: each = u32 offsets[rows_in_block + 1] then value bytes; 8-byte aligned
```

A block is the load and verification unit, so a filter on `$extra.color` scans block by block and a projection of ten rows loads at most ten blocks.

### Index and Stats Sections

- `ScalarInverted` and `ScalarSorted`: produced and parsed by the scalar-index PR (`InvertedIndex::write_to`, `InvertedView::view`, and the same for sorted). Storage requires lookups that return `RoaringBitmap` over row ids, and, for sorted, an ordered iterator of `(value, row)` from a starting value in either direction (serves `order_by`).
- `Stats`: postcard `SegmentStats`: per field a zone map (`min`, `max`, `null_count`), a HyperLogLog sketch (precision 12), the top 16 values with counts, and a 32-bucket equi-depth histogram for numbers. There is no unbounded `value_counts`. Zone maps and distinct estimates are also copied into the manifest so pruning never opens the file.

### Lazy Section Loading

```rust
pub struct SegmentHandle {
    pub unit: UnitId,
    pub file: Arc<FileHandle>,
    pub header: SegmentHeader,
    pub row_count: u32,
    pub sections: Arc<[SectionEntry]>,
    /// Parsed at open: small and needed by every read.
    pub schema: Arc<Schema>,
    /// From the manifest: zone maps, counts, graph and SQ8 presence.
    pub summary: Arc<SegmentSummary>,
}

impl SegmentHandle {
    /// Read header, footer, section table, and SchemaSnapshot; verify CRCs,
    /// file_len against both the file and the manifest, and row_count.
    pub fn open(vfs: &dyn Vfs, path: &Path, expect: &ManifestSegment) -> Result<Self>;
    pub fn section(&self, kind: SectionKind, field: Option<FieldId>) -> Option<&SectionEntry>;
}
```

Section bytes are never read directly: every access goes through `BufferCache::get_or_load(CacheKey { file, section, page }, class, loader)`, where the loader runs on the `IoPool`, reads with `read_exact_at`, and verifies the whole-section CRC (whole units) or page CRC (paged units) before the bytes enter the cache. A CRC failure returns `LogPoseError::SegmentCorrupt { unit, section }`; the collection stays open and the query fails.

## Manifest v2 and CURRENT

### Manifest Contents

```rust
#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32, // 2
    pub collection_id: CollectionId,
    pub generation: u64,
    pub epoch: Epoch,
    /// Every operation with seq <= this is reflected in segments and DV files (I9).
    pub checkpoint_seq_no: SeqNo,
    /// Current schema, including FieldId assignments and dropped-field tombstones.
    pub schema: Schema,
    pub next_unit_id: u32,
    /// Ascending by unit.
    pub segments: Vec<ManifestSegment>,
    pub totals: ManifestTotals,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestSegment {
    pub unit: UnitId,
    pub file_len: u64,
    pub footer_crc: u32,
    pub row_count: u32,
    pub schema_version: u32,
    pub min_seq_no: SeqNo,
    pub max_seq_no: SeqNo,
    pub origin: SegmentOrigin, // Flush { memtable_seq_range } | Compaction { inputs: Vec<UnitId> }
    pub tier: u8,
    pub dv: Option<DvRef>,
    pub vectors: Vec<VectorSummary>, // per field: has_graph, has_sq8, non_null
    pub zones: Vec<FieldZone>,       // per field: min, max, null_count, distinct_estimate
}

#[derive(Serialize, Deserialize)]
pub struct DvRef {
    pub generation: u32,
    pub cardinality: u32,
    pub covered_seq_no: SeqNo,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestTotals {
    pub rows: u64,
    pub deleted_rows: u64,
    pub segment_bytes: u64,
}
```

File `manifests/<generation:020>.mf`:

```text
offset size field
     0    8 magic         "LPMANIF2"
     8    8 generation
    16    8 payload_len
    24    4 payload_crc   crc32c(payload)
    28    4 header_crc    crc32c(bytes 0..28)
    32    n payload       postcard Manifest
```

`CURRENT` is a 21-byte text file, `<generation:020>\n`, so operators can read it. The manifest named by `CURRENT` must decode, pass both CRCs, and carry the same generation, or the collection fails to open with `LogPoseError::ManifestCorrupt`.

`descriptor.json` keeps only identity and configuration (collection id, database, name, config overrides). Schema, dimensions, and metric move to the manifest and the WAL. `maintenance.json` is deleted: maintenance status is runtime state derived from the writer.

### Atomic Publish Protocol

Precondition: every new file the manifest references (segment files, DV files) has been fsynced and `segments/` has been fsynced after they were created.

1. Open `manifests/<g>.mf` with `CreateNew`, append, `sync_all`. `crash_point(ManifestAfterFileSync)`.
2. `sync_dir(manifests/)`. `crash_point(ManifestAfterDirSync)`.
3. Remove a stale `CURRENT.tmp` if present. Open `CURRENT.tmp` with `CreateNew`, append, `sync_all`. `crash_point(CurrentAfterTempSync)`.
4. `rename(CURRENT.tmp, CURRENT)`. `crash_point(CurrentAfterRename)`.
5. `sync_dir(<collection dir>)`. `crash_point(CurrentAfterDirSync)`.

The commit point is step 5 returning `Ok`. Before it, recovery may see either the old or the new `CURRENT`, and both name complete, consistent states. Only after step 5 does the writer publish the new `Version`, mark superseded files obsolete, and enqueue deletions.

Errors: a failure in steps 1 to 3 aborts the commit with no state change (the job is retried with backoff; the orphan files are cleaned later). A failure in step 4 or 5 leaves the durable `CURRENT` unknown, so the writer poisons the collection (read-only until reopen), exactly like a WAL fsync failure.

## Flush

### Flush Steps

The flush of frozen memtable `F` (unit `m`, frozen at last sequence number `L`), with durable manifest generation `g`:

1. **Freeze** (writer). As in [Frozen Memtable](#frozen-memtable): `F` joins `frozen`, the WAL rotates so the new file starts at `L + 1`, a `Version` is published, a permit is requested.
2. **Begin** (writer, when the permit arrives and `F` is the oldest frozen memtable; flushes are serialized per collection). First complete and publish any in-flight group. Then capture a `FlushInput`: `Arc<MemtableData>` for `F`, `D_F` (the snapshot of `deletes[m]`), `D_S` (snapshots of every segment's deletion vector), the segment list, the schema, a new unit id `s`, and DV generations for each segment whose cardinality differs from its durable generation. Let `J` be `visible_seq_no` now (`J >= L`).
3. **Build** (maintenance pool). Iterate `F`'s slots in order, skipping slots set in `D_F`. Assign row ids densely. Build pk, sorted pk, filter, row meta, columns, dynamic blocks, SQ8, graph (if at least `graph_min_rows`), scalar indexes, and stats. Record `slot_to_row: Arc<[u32]>` (`u32::MAX` for skipped slots). If no slot is live, steps 3 and 4 produce nothing and the flush is a pure checkpoint: no segment is added.
4. **Write segment** (I/O pool). Stream sections to `segments/<s:08x>.seg` (`CreateNew`), then `sync_all`. `crash_point(FlushAfterSegmentSync)`.
5. **Write DV files** (I/O pool). For each segment with a new generation: write `segments/<id>.dv.<gen>` from `D_S` with `covered_seq_no = J`, `sync_all`. `crash_point(FlushAfterDvSync)`.
6. **Sync directory.** `sync_dir(segments/)`. `crash_point(FlushAfterSegmentsDirSync)`.
7. **Commit manifest** (writer, on `FlushDone`). Complete any in-flight group first. Build manifest `g + 1`: the current `Version`'s segments plus `s`, `checkpoint_seq_no = L`. For each segment that is present both now and in `D_S` and got a new generation in step 5, name that generation; for every other segment (including segments that a compaction created after step 2), keep its current durable generation. Run the atomic publish protocol.
8. **Install** (writer). New `Version`: drop `F` from `frozen`; add `SegmentHandle` for `s`; set `deletes[s]` to the reconciliation of `F`'s late deletions, `{ slot_to_row[x] : x in deletes[m] now, x not in D_F }`; remove `deletes[m]`. Add forwarding `m -> (s, slot_to_row)` and a rewrite task. Publish. Enqueue for deletion the superseded DV generations, any DV file the job wrote for a segment that a compaction removed in the meantime, and the WAL files whose successor starts at or below `L + 1`. Append a `Checkpoint` frame with the next group.
9. **Rewrite** (writer, incremental). Rewrite pk-index entries from `m` to `s` in slices; drop the forwarding when done.

Why `checkpoint_seq_no = L` is correct (I9): the only operations at or below `L` not reflected in `F` are deletions of segment rows, and those are in `D_S`, which was captured at `J >= L`. Segments created by a compaction that committed between steps 2 and 7 carry their own DV generation written at that compaction's commit, which covers every deletion up to that commit (see [Compaction](#compaction)). The late deletions of `F`'s slots reconciled in step 8 all have sequence numbers above `J`, so the WAL still holds them.

### Flush Crash Analysis

| Crash after | Durable state | Recovery |
| --- | --- | --- |
| 1 freeze | new WAL file may exist (empty or with only a checkpoint frame) | manifest `g`; replay all WAL after `g`'s checkpoint, including `F`'s operations; `F` is rebuilt as part of the memtable |
| 2 begin | nothing new | same as above |
| 3 build | nothing new | same as above |
| 4 segment sync | `s.seg` exists, unreferenced | orphan cleanup deletes `s.seg`; replay as above |
| 5 DV sync | DV files exist, unreferenced | orphan cleanup deletes them; replay as above |
| 6 dir sync | same, now surely visible | same as above |
| 7 publish, before step 5 of the protocol | `g + 1` manifest may exist; `CURRENT` old or new | if `CURRENT` is old, as above plus deleting manifest `g + 1`; if new, load `g + 1`, replay from `L + 1` |
| 7 publish, after its step 5 | `CURRENT = g + 1` | load `g + 1`, replay from `L + 1`; late deletions of `F`'s rows are replayed from the WAL |
| 8 install | as above; some obsolete files may be deleted or not | as above; orphan cleanup removes leftovers |
| 9 rewrite | as above | as above (the pk index is rebuilt from scratch at open) |

Every row is either in `F` (reconstructed by replay) or in `s` (durable), never both as live rows, because `CURRENT` selects exactly one of the two worlds.

## Compaction

### Size-Tiered Policy

```rust
pub struct CompactionConfig {
    /// Tier t holds segments with live rows in [base_rows * ratio^(t-1), base_rows * ratio^t).
    pub base_rows: u32,            // default 32_768
    pub tier_ratio: u32,           // default 4
    pub min_merge: usize,          // default 4
    pub max_merge: usize,          // default 10
    pub max_output_rows: u32,      // default 2_000_000
    pub max_output_bytes: u64,     // default 8 GiB of f32 vectors
    /// Rewrite a segment whose deleted fraction reaches this.
    pub deleted_ratio: f32,        // default 0.2
    pub max_jobs_per_collection: usize, // default 2
    pub graph_min_rows: u32,       // default 20_000 (shared with flush)
    pub sq8_min_rows: u32,         // default 1_024
}
```

The writer runs the policy after every commit that changes the segment set, over unreserved segments, using live rows (`row_count - dv.len`):

1. **Deletion-driven.** If any segment has `dv.len / row_count >= deleted_ratio`, pick the one with the most deleted rows; add up to `min_merge - 1` of the smallest unreserved segments in the same or lower tier. Emit a job.
2. **Tiered.** For each tier from the lowest: if at least `min_merge` unreserved segments are in it, take them in ascending unit order until `max_merge`, `max_output_rows`, or `max_output_bytes` would be exceeded. Emit a job.
3. Never emit more than `max_jobs_per_collection` concurrent jobs, and never reserve a segment twice.

Write amplification: a row is rewritten once per tier it climbs, about `log_4(2,000,000 / 32,768)`, so roughly 3 compactions plus the flush. The engine counts bytes written by flush and by compaction and reports the ratio to bytes ingested, which the Phase 2 exit criterion measures.

### Compaction Steps

For inputs `I_1..I_n` (ascending unit ids) with durable manifest generation `g`:

1. **Begin** (writer, on permit). Complete and publish any in-flight group. Reserve the inputs. Capture `D0_i` (snapshot of `deletes[I_i]`) for each input, the current schema, and a new unit id `o`.
2. **Build** (maintenance pool, reads through the I/O pool with `CacheMode::Bypass`, so compaction does not evict hot data). For each input in order, for each row `r` not in `D0_i`: append the row to the output (dropping fields the schema dropped, filling null for fields it added) and set `map_i[r] = next output row`; set `map_i[r] = u32::MAX` for rows in `D0_i`. Retrain SQ8 and build the graph over the output. Record `pks` and `sources` in output row order.
3. **Write** (I/O pool). Stream `segments/<o:08x>.seg`, `sync_all`, `sync_dir(segments/)`. `crash_point(CompactionAfterOutputSync)`.
4. **Reconcile** (writer, on `CompactionDone`). Complete and publish any in-flight group. For each input, `delta_i = deletes[I_i] now AND NOT D0_i`. For each `r` in `delta_i`, set bit `map_i[r]` in `DV_o`. Every such `map_i[r]` is valid, because `r` was not in `D0_i`, so it was copied.
5. **Write output DV** (writer, I/O pool). If `DV_o` is not empty: write `segments/<o:08x>.dv.00000001` with `covered_seq_no = visible_seq_no`, `sync_all`, `sync_dir(segments/)`. `crash_point(CompactionAfterDvSync)`.
6. **Commit manifest.** Manifest `g + 1` = current segments minus inputs plus `o` (with `dv = Some(generation 1)` when step 5 wrote one), checkpoint unchanged. Atomic publish protocol.
7. **Install.** New `Version`: remove inputs and their `deletes` entries, add `o` with `DV_o`. Add forwarding `I_i -> (o, map_i)` for every input and one rewrite task. Release reservations. Publish. Remove the inputs from the writer's `live_files` (marking their `FileHandle`s obsolete) and enqueue the inputs' DV files for deletion.

### DV Reconciliation at Commit

This is the step that makes concurrent writes safe, so it is spelled out.

While the job runs (between steps 1 and 4), the writer keeps processing writes. A write that deletes or supersedes a row `r` of input `I_i` finds `RowAddr { I_i, r }` in the pk index (the inputs are still in every `Version`) and sets `r` in `deletes[I_i]`. The job never sees those bits, so the output contains a copy of `r` at `map_i[r]`.

At commit the writer transfers exactly those bits. Claim: after step 7, for every output row `x` copied from `(I_i, r)`, `x` is set in `DV_o` if and only if `r` is set in `deletes[I_i]` at commit time. Proof: `r` was copied, so `r` is not in `D0_i`. If `r` is set now, it is in `delta_i`, so `x` is set. Conversely, `DV_o` receives bits only from the `delta_i` sets, through the injective maps. Therefore the live row set, as a set of logical rows, is identical immediately before and after the swap, so I4 and I5 hold across the commit and no reader sees a resurrected or lost row. The writer does steps 4 to 7 without processing any write in between, so no deletion can fall between reconciliation and publication.

After the commit, a write that resolves a key to `(I_i, r)` (a pk-index entry not yet rewritten) follows the forwarding table to `(o, map_i[r])`, so its bit lands in `DV_o`. The rewrite task later points the entry directly at `(o, x)`, but only if the entry still equals the source address, so a key that was deleted or re-inserted during or after the compaction is never clobbered.

Durability of reconciled bits (I9): a reconciled deletion may have a sequence number at or below the current checkpoint `C`, because a flush can commit while the compaction runs and move the checkpoint past it; the WAL files holding it may already be deleted, and the input's DV file that recorded it is dropped with the input. That is why step 5 writes the output's DV file before the manifest is published, and why a DV file is required whenever `DV_o` is not empty.

### Compaction Crash Analysis

| Crash after | Durable state | Recovery |
| --- | --- | --- |
| 1 begin, 2 build | nothing new | manifest `g`; the job is forgotten and re-planned |
| 3 output sync | `o.seg` unreferenced | orphan cleanup deletes it |
| 4 reconcile | same | same |
| 5 output DV sync | `o.seg`, `o.dv.1` unreferenced | orphan cleanup deletes both |
| 6 publish, before its step 5 | `CURRENT` old or new | old: as above plus deleting manifest `g + 1`; new: load `g + 1` with `o` and `o.dv.1`, inputs are orphans and deleted |
| 7 install | `CURRENT = g + 1` | load `g + 1`; inputs and their DV files are orphans |

In every case the WAL after the durable checkpoint is untouched, so replay restores every later deletion, including those of `o`'s rows.

## Garbage Collection

### File Handles and Obsolescence

```rust
pub struct FileHandle {
    pub id: FileId,              // process-unique, used in cache keys
    pub path: PathBuf,
    pub file: Arc<dyn VfsFile>,
    obsolete: AtomicBool,
    gc: GcSender,
    cache: Weak<BufferCache>,
}

impl FileHandle {
    /// Called only by the writer, only after the manifest that drops this file is durable.
    pub fn mark_obsolete(&self) { self.obsolete.store(true, Ordering::Release) }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        // Cache entries hold only bytes; invalidate them either way.
        if let Some(cache) = self.cache.upgrade() { cache.invalidate_file(self.id) }
        if self.obsolete.load(Ordering::Acquire) {
            self.gc.send(GcTask::Remove(self.path.clone()));
        }
    }
}
```

Segment files are the only files readers touch lazily, so they are the only files refcounted through `Version`s:

- The writer holds `live_files: HashMap<UnitId, Arc<FileHandle>>` for every segment in the durable manifest.
- Each `SegmentHandle` holds an `Arc<FileHandle>`; every `Version` (including token-pinned ones) holds its `SegmentHandle`s.
- When a manifest that drops segment `X` becomes durable, the writer calls `mark_obsolete` and then drops its `live_files` entry. Because the writer's reference keeps the count above zero until after the `Release` store, whichever thread drops the last reference observes `obsolete = true` and enqueues the removal. That is I7 (a) and (b) together.
- A segment dropped from the manifest while an old `Version` still reads it stays on disk until that `Version` (or its token) is released.

WAL files, DV files, and manifests are never read after open, so no `Version` references them. The writer enqueues them for removal right after the superseding manifest is durable: WAL files whose successor starts at or below `checkpoint + 1`, DV generations no longer named, and manifests older than the previous generation (the previous one is kept for inspection).

The GC worker runs on the `IoPool`, calls `remove_file`, `crash_point(GcAfterRemove)`, and syncs each touched directory once per batch. The directory sync is only for prompt space reclamation; correctness does not depend on it.

### GC Crash Safety and Orphan Cleanup

A removal is not durable until its directory is synced, so after a crash a removed file may reappear. Neither outcome matters, because nothing references an obsolete file. Orphan cleanup runs during recovery, after `CURRENT` and its manifest `M` are loaded and before the writer starts (so before any unit id or DV generation can be reused):

1. `segments/`: remove every `.seg` whose unit is not in `M`, every `.dv.<gen>` whose `(unit, gen)` is not named by `M`, and every `.tmp`.
2. `manifests/`: remove every generation other than `M.generation` and `M.generation - 1`.
3. `wal/`: remove every file whose successor starts at or below `M.checkpoint_seq_no + 1`.
4. Remove `CURRENT.tmp`.
5. `sync_dir` every directory that changed. `crash_point(RecoveryAfterOrphanCleanup)`.

At engine level, a collection directory without `descriptor.json` (a create that crashed before its commit point) and any `*.dropped` directory are removed with `remove_dir_all`.

Collection create commits by writing `descriptor.json` last (temp file, fsync, rename, directory fsyncs of the collection directory and `collections/`), after the subdirectories, an empty generation-0 manifest, `CURRENT`, and the first WAL file exist. Drop renames the directory to `<uuid>.dropped`, syncs `collections/`, unregisters the handle, and removes the tree once the last `Version` is released.

## Buffer Cache

### Budget and Classes

One engine-wide cache holds segment section bytes. Its budget is derived from `storage.memory_limit`:

```text
cache_budget = memory_limit
             - pk_index_reservation      (sum of writer pk-index sizes, updated every second)
             - memtable_reservation      (global_fraction * memory_limit)
             - query_working_reserve     (10 percent of memory_limit, for bitmaps, heaps, visited sets)
```

```rust
/// Priority order from D8, highest first. Eviction starts from the bottom.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ArtifactClass {
    GraphAndCodes = 0,
    PkIndex = 1,      // PkColumn, PkSorted, PkFilter (read side)
    ScalarIndex = 2,  // inverted, sorted, stats
    ScalarColumns = 3,
    RawVectors = 4,   // VectorF32 pages
    DynamicJson = 5,  // DynamicJson blocks
}

pub struct ClassPolicy {
    /// Share of the budget this class keeps even under pressure from higher classes.
    pub floor: f32, // defaults: 0, 0, 0, 0.02, 0.05, 0.01
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CacheKey {
    pub file: FileId,
    pub section: u16, // index into the section table
    pub page: u32,    // 0 for whole-section units
}

pub struct BufferCache {
    budget: AtomicU64,
    used: [AtomicU64; 6],
    shards: Box<[Mutex<CacheShard>]>,     // 64 shards by key hash
    clocks: [Mutex<ClockRing>; 6],        // one ring per class
    policy: [ClassPolicy; 6],
}

struct CacheShard {
    entries: HashMap<CacheKey, CacheEntry>,
    /// Single-flight: concurrent misses on one key share one load.
    loading: HashMap<CacheKey, Shared<BoxFuture<'static, Result<Arc<AlignedBytes>>>>>,
}

struct CacheEntry {
    bytes: Arc<AlignedBytes>, // 8-byte-aligned buffer
    class: ArtifactClass,
    referenced: AtomicBool,
}

impl BufferCache {
    pub async fn get_or_load(
        &self,
        key: CacheKey,
        class: ArtifactClass,
        mode: CacheMode, // Normal | Bypass (compaction, rebuild)
        load: impl FnOnce() -> Result<AlignedBytes> + Send + 'static, // runs on IoPool
    ) -> Result<(Arc<AlignedBytes>, Hit)>;
    pub fn invalidate_file(&self, file: FileId);
    pub fn residency(&self, key: &CacheKey) -> bool;
}
```

### Eviction Policy

Decision: CLOCK (second chance), one ring per class, rather than LRU. A hit only sets `referenced` with a relaxed store, so concurrent hits never contend on a list lock; LRU must reorder a shared list on every hit. The loss in hit rate relative to LRU is small for this workload, whose hot set (graphs and codes) is scanned repeatedly.

Eviction runs inline on the inserting thread when `Σ used > budget`:

1. Choose the victim class: the lowest-priority class whose usage exceeds its floor; if none, the lowest-priority non-empty class.
2. Advance that class's hand. For each entry: if `referenced`, clear it and continue; if `Arc::strong_count(&bytes) > 1`, a reader has it pinned, so skip; otherwise remove it and subtract its size.
3. Repeat until under budget or a full sweep of every class found nothing evictable. In that case the insert still succeeds (correctness first) and `cache_overcommit_bytes` records the excess.

### Pinning

A reader's `fetch` returns a `PinSet` holding `Arc<AlignedBytes>` for every unit it loaded. Pinned entries are not evictable, and the CPU stage reads only through the `PinSet`, so it never waits on I/O and never finds its data gone. The pins drop with the `PinSet` at the end of the query stage.

### Warm-Up on Open

After recovery, a background task per engine loads sections in priority order: all `GraphAndCodes` sections (largest segment first), then `PkIndex`, then `ScalarIndex`, stopping each class when the cache reaches 90 percent of the budget. It uses the low-priority queue of the `IoPool` with at most two loads in flight, so foreground misses always go first. Queries do not wait for warm-up; a cold section is loaded on demand.

### Cold Reads in EXPLAIN

```rust
pub struct FetchReport {
    pub hits: u32,
    pub misses: u32,
    pub bytes_read: u64,
    pub io_micros: u64,
    pub by_class: [ClassFetch; 6], // misses and bytes per class
}
```

Every fetch stage returns a `FetchReport`, and the operator that asked for it records it. `EXPLAIN` prints, per operator, for example `GraphScan unit=0000002a cold: graph+codes 48.2 MB in 112 ms`. Before execution, the planner can ask `UnitView::residency(need)`, which returns `Resident` or `Cold { bytes }`, so the cost model can prefer resident paths.

## Recovery on Open

`Engine::open(config, vfs, resolver)`:

1. `try_lock_exclusive(<root>/LOCK)`; fail with `StorageRootLocked`.
2. Build the runtime (pools), the cache, the GC queue, and the scheduler.
3. Load catalog files (databases, principals, policies). Remove collection directories without `descriptor.json` and `*.dropped` directories.
4. For each collection, in parallel on the I/O pool with at most `io_threads` collections at once, run `open_collection`. A collection that fails is registered in the `Failed` state with its typed error; every call on it returns that error; the engine still opens.
5. Register the handles in `collections`, start the writers, start the token reaper, enqueue warm-up.

`open_collection(dir)`:

1. Read `descriptor.json` and `CURRENT`; load manifest `M` and verify it. A version-1 layout (`maintenance.json`, `active.wal`, or a JSON manifest) fails with `LogPoseError::UnsupportedFormat`.
2. Orphan cleanup, as in [GC Crash Safety and Orphan Cleanup](#gc-crash-safety-and-orphan-cleanup).
3. Open a `SegmentHandle` for every segment in `M` (header, footer, table, schema snapshot; about three small reads each).
4. Load each named DV file, verify CRC, `unit_id`, `row_count`, and generation; build `DeletionMap`.
5. Rebuild the pk index from segment pk and row-meta sections minus deletion vectors, as in [Primary-Key Index](#primary-key-index).
6. List `wal/`, sort by first sequence number, skip files entirely at or below `M.checkpoint_seq_no`, and run tail repair on the last file.
7. Replay frames in order. Skip frames with `last_seq_no <= checkpoint`. Check contiguity from `checkpoint + 1`. For each `WriteBatch`, call the same `apply` the live writer uses, into a fresh active memtable (unit id from `M.next_unit_id`); for `SchemaChange`, swap the schema; for `Checkpoint`, cross-check. Replay is CPU-bound and runs on the maintenance pool.
8. Set `next_seq_no = max(checkpoint, last replayed) + 1`, `next_unit_id` past every allocated id, `durable` from `M`, and `live_files` from the segment handles.
9. Open the WAL writer on the last file (after repair) in append mode, or create a new file if there is none.
10. Build and publish `Version` 1. If the replayed memtable already exceeds a flush trigger, freeze it right away.
11. Run `Version::check_invariants` when `strict_invariants` is on (tests): unique live keys, pk index agreement, DV bits below row counts, counter exactness.

Recovery is idempotent (I11): the only file changes before the writer starts are orphan removal (step 2), tail truncation (step 6), and creating an empty WAL file (step 9). Each removes or adds only bytes that no durable state references, so rerunning recovery after a crash at any point converges to the same state.

## Read Path

### Reader Interfaces

`logpose-storage` exposes data access; `logpose-query` owns predicate compilation, strategy choice, and execution. The query crate depends only on the types below.

```rust
/// Implemented by `Engine`.
pub trait CollectionReader: Send + Sync {
    fn read_view<'a>(
        &'a self,
        collection: &'a CollectionRef,
        options: ReadOptions,
    ) -> BoxFuture<'a, Result<ReadView>>;
}

#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    /// Read the pinned Version instead of the current one; extends the token's TTL.
    pub token: Option<SnapshotToken>,
    /// Read barrier: wait (up to the configured timeout) until visible_seq_no >= this.
    pub min_seq_no: Option<SeqNo>,
    /// Create a token for this view.
    pub pin: bool,
}

/// One Version plus the context to load its sections. Cheap to clone.
#[derive(Clone)]
pub struct ReadView {
    version: Arc<Version>,
    ctx: Arc<ReadContext>, // cache, IoPool, query pool
    token: Option<SnapshotToken>,
}

impl ReadView {
    pub fn schema(&self) -> &Arc<Schema>;
    pub fn visible_seq_no(&self) -> SeqNo;
    pub fn token(&self) -> Option<&SnapshotToken>;
    pub fn counters(&self) -> VersionCounters;
    /// Frozen memtables, the active memtable, then segments; oldest to newest.
    pub fn units(&self) -> Vec<UnitView<'_>>;
    pub fn query_pool(&self) -> &rayon::ThreadPool;
    /// The only async data access: load and pin sections, rows, or blocks.
    pub async fn fetch(&self, plan: FetchPlan) -> Result<(PinSet, FetchReport)>;
    /// Point lookups, newest unit first; does its own fetches.
    pub async fn get(&self, pks: &[PrimaryKey], projection: &Projection) -> Result<Vec<Option<RowData>>>;
}

pub struct FetchPlan {
    pub needs: Vec<(UnitId, SectionNeed)>,
}

pub enum SectionNeed {
    Pk,
    Stats,
    ScalarIndex(FieldId),
    Column(FieldId),
    DynamicBlocks(RoaringBitmap), // rows whose blocks are needed; full bitmap = whole column
    VectorIndex(FieldId),         // graph (if any) and SQ8 codes (if any)
    VectorRows(FieldId, RoaringBitmap),
}

pub enum Residency {
    Resident,
    Cold { bytes: u64 },
}

/// A memtable or a segment, with its deletion vector in this Version.
pub struct UnitView<'v> { /* &'v MemtableData or &'v SegmentHandle, Option<&'v DeletionVector> */ }

impl<'v> UnitView<'v> {
    pub fn id(&self) -> UnitId;
    pub fn is_memtable(&self) -> bool;
    pub fn row_count(&self) -> u32;
    pub fn live_count(&self) -> u32;
    /// Zone maps, null counts, distinct estimates. Always resident.
    pub fn summary(&self) -> &UnitSummary;
    pub fn deleted(&self) -> Option<&'v DeletionVector>;
    /// 0..row_count AND NOT deleted.
    pub fn live(&self) -> RoaringBitmap;
    pub fn residency(&self, need: &SectionNeed) -> Residency;

    // Synchronous accessors. Memtable units are always resident; segment units
    // require the section in `pins`, else Err(LogPoseError::NotFetched), which is
    // a bug in the caller, never a reason to do I/O.
    pub fn scalar_index<'p>(&self, field: FieldId, pins: &'p PinSet) -> Result<Option<ScalarIndexRef<'p>>>;
    pub fn column<'p>(&self, field: FieldId, pins: &'p PinSet) -> Result<ColumnRef<'p>>;
    pub fn dynamic<'p>(&self, pins: &'p PinSet) -> Result<DynamicRef<'p>>;
    pub fn vector_index<'p>(&self, field: FieldId, pins: &'p PinSet) -> Result<VectorIndexRef<'p>>;
    pub fn vector_rows<'p>(&self, field: FieldId, pins: &'p PinSet) -> Result<VectorRows<'p>>;
    pub fn pks<'p>(&self, pins: &'p PinSet) -> Result<PkRef<'p>>;
}

/// Uniform over memtable indexes and segment index sections.
pub trait ScalarIndexOps {
    fn eq(&self, value: &Value) -> RoaringBitmap;
    fn any_of(&self, values: &[Value]) -> RoaringBitmap;
    fn range(&self, low: Bound<&Value>, high: Bound<&Value>) -> Option<RoaringBitmap>; // None if not sorted
    fn nulls(&self) -> RoaringBitmap;
    /// Sorted indexes only: (value, row) pairs from `start` in `direction`.
    fn ordered(&self, start: Bound<&Value>, direction: Direction) -> Option<Box<dyn Iterator<Item = (Value, RowId)> + '_>>;
}

pub trait PkOps {
    fn pk_at(&self, row: RowId) -> PrimaryKey;
    /// Row of `pk` if present in this unit (live or not).
    fn find(&self, pk: &PrimaryKey) -> Option<RowId>;
    /// Rows in ascending pk order, strictly after `after`.
    fn ascending_after(&self, after: Option<&PrimaryKey>) -> Box<dyn Iterator<Item = (PrimaryKey, RowId)> + '_>;
}

/// Injected into the engine; implemented by logpose-query.
pub trait RowSetResolver: Send + Sync {
    /// Live rows of `view` matching `filter`, per unit.
    fn resolve<'a>(&'a self, view: &'a ReadView, filter: &'a FilterExpr) -> BoxFuture<'a, Result<Vec<(UnitId, RoaringBitmap)>>>;
}
```

The predicate AST moves from `logpose-query` to `logpose-types` as `FilterExpr`, so the writer can carry it without depending on the query crate.

### Staged Execution

A vector search runs as alternating fetch and compute stages, so that no rayon worker ever does I/O:

1. **View.** `read_view(collection, options)` loads one `Arc<Version>` (or the token's).
2. **Plan.** For each unit, use `summary()` zone maps to prune units the filter cannot match. For the rest, list the sections the filter needs (index if one exists for the field, otherwise column or dynamic blocks) and the vector index.
3. **Fetch 1** (I/O pool). `fetch(plan)`; memtable units need nothing.
4. **Compute 1** (query pool, one rayon task per unit):
   1. Compile the filter to a bitmap `B` using `ScalarIndexOps` or a column scan; `NOT p` becomes `live AND NOT B_p`. Then `B := B AND NOT deleted`. Without a filter, `B = live()`.
   2. `n = |B|`, `N = live_count`. Choose the strategy (thresholds are constants in `logpose-query`, calibrated by the harness): memtable, or no graph, or `n <= exact_threshold`, gives an exact scan over `B` (SQ8 codes when present, f32 otherwise); a graph with small `n / N` gives a filter-aware (ACORN-1 style) walk; otherwise a graph walk that admits only rows in `B`.
   3. Produce the unit's top `k * rerank_factor` candidates as `(RowAddr, approximate score)`.
5. **Merge.** Global k-way heap merge to the top `k * rerank_factor`.
6. **Fetch 2.** `VectorRows(field, rows)` for candidates from segments with SQ8 (memtable and f32-scanned candidates already have exact scores).
7. **Rerank** (query pool). Exact f32 scores; take the top `k`.
8. **Fetch 3 and project.** Fetch the projected columns and dynamic blocks for the final `k` rows and build output rows.

`EXPLAIN` records per unit the strategy, `n`, `N`, the reason, and each stage's `FetchReport`.

### Get, Count, Scroll, and Order By

These are query-crate functions over the same interfaces:

```rust
pub async fn get(reader: &dyn CollectionReader, c: &CollectionRef, pks: &[PrimaryKey], p: &Projection, o: ReadOptions) -> Result<Vec<Option<RowData>>>;
pub async fn count(reader: &dyn CollectionReader, c: &CollectionRef, filter: Option<&FilterExpr>, o: ReadOptions) -> Result<u64>;
pub async fn scroll(reader: &dyn CollectionReader, c: &CollectionRef, request: ScrollRequest) -> Result<ScrollPage>;

pub struct ScrollRequest {
    pub filter: Option<FilterExpr>,
    pub order: ScrollOrder,
    pub limit: u32,
    pub projection: Projection,
    /// None starts a scroll: the engine pins a Version and the cursor carries its token.
    pub cursor: Option<Cursor>,
}

pub enum ScrollOrder {
    Pk,
    Field { field: FieldId, direction: Direction }, // ties broken by pk ascending; nulls last
}

/// Opaque to clients (base64url of postcard).
pub struct Cursor {
    pub token: SnapshotToken,
    pub order: ScrollOrder,
    pub after: CursorKey,
}

pub enum CursorKey {
    Pk(PrimaryKey),
    Field(Option<Value>, PrimaryKey),
}

pub struct ScrollPage {
    pub rows: Vec<RowData>,
    pub next: Option<Cursor>,
}
```

- **Get.** For each key, walk units newest to oldest. Memtable: `pk_to_slot`. Segment: `PkFilter` probe, then `PkSorted` binary search. Return the first hit whose row is not deleted; if every hit is deleted (or there is none), the key is absent. By I5 at most one unit can have a live row, so the walk order affects only how soon the common case (a recently written key) exits.
- **Count.** Sum over units of `|B AND NOT deleted|`; no filter means `counters().total_rows - deleted_rows`. With indexes this touches only bitmaps.
- **Scroll by pk.** Each unit yields `ascending_after(cursor.after)` filtered by its `B`; a k-way merge by pk takes `limit` rows. Because the cursor's token pins one `Version`, every live row appears exactly once across pages, even under concurrent writes.
- **Order by a field** (with or without a scroll). Each unit yields rows in `(value, pk)` order: from the sorted index when there is one (`ordered`), otherwise by a heap over the column restricted to `B`. A k-way merge takes `limit`. With a vector in the query, `order_by` applies to the vector top-k result instead.

`get` resolves keys the same way readers always do; it never consults the writer's pk index.

## Migration Plan

The existing `StorageEngine` trait is the seam. The new engine lands behind it, the callers move to the new interfaces, and then the trait's read methods are deleted.

Phase 1, format-compatible in behavior, new internals:

1. **Split and Vfs.** Split `crates/logpose-storage/src/lib.rs` into the modules above without behavior change, and add `logpose-vfs`. All file I/O in `logpose-storage` and `logpose-wal` moves to `Vfs`. If the Phase 0 `Vfs` PR landed inside `logpose-storage`, this moves it to the new crate. Existing tests pass unchanged.
2. **Engine shell.** `Engine` owns the collection map and the storage-root lock; `LocalStorageEngine` becomes a thin wrapper that implements `StorageEngine` by delegating to `Engine`. Descriptor lookup becomes a map lookup; `find_collection_descriptor` and `list_collection_descriptors` directory scans are deleted. The process-global lock maps (`wal_rotation_locks`, `maintenance_operation_locks`, `maintenance_status_locks`, `maintenance_coordinator`) are deleted along with `thread::spawn` maintenance, replaced by the writer and the scheduler.
3. **Resident state.** `CollectionHandle` with `ArcSwap<Version>` and the writer task. At this step a `Version` holds the v1 manifest plus the replayed delta, and `load_collection_state` is deleted: every trait method reads the current `Version`.
4. **WAL v2 and group commit.** The writer writes WAL v2 frames with group commit; the v1 JSON WAL, `WalMode`, `rotate_active`, and `PENDING_ROTATION` handling are deleted. WAL v2 payloads are `RowOp`s, so the schema and row-op step (Phase 2 step 1 below) lands before this one; until segment v2 exists, the `Version` delta holds `(seq_no, RowOp)` pairs and flush converts them into v1 segment records.
5. **Pools, GC, tokens.** I/O moves to the `IoPool`, search CPU to rayon. Version-refcount GC and manifest v2 land. Snapshot tokens replace historical `Snapshot` reads.

Phase 2, data model:

1. **Schema and row ops.** `WriteOperation::{Put, Delete}` becomes `ClientOp`, the legacy adapter maps a v1-style `PutRecord { id, vector, metadata }` to a `Record` in a collection whose schema is "string pk `id`, one vector field `vector`, dynamic fields on", which is what the legacy `CreateCollectionRequest` creates. This keeps every existing test's data shape valid.
2. **Segment v2, memtable v2, pk index, DVs.** Flush writes segment v2; `resolve_latest_state_selected`, `resolve_latest_from_segments`, `resolve_latest_state_for_ids_selected`, `apply_resolved_record`, `read_segment_file`, the JSON entry table, `FlatIndexSidecar`, and `HnswIndexSidecar` are deleted.
3. **Compaction v2.** Size-tiered with reconciliation; `compact_state` is deleted.
4. **Read path.** `logpose-query` switches from `scan_exact_selected`, `ann_search_selected`, and `latest_visible_selected` to `CollectionReader`. Those trait methods, `VectorFirstExact`, `QueryUnitStats` string tiers, and the storage copy of distance code are deleted. `StorageEngine` shrinks to collection lifecycle and write, then `logpose-service` calls `Engine` directly and the trait and `legacy.rs` are deleted.

Test migration:

- Tests that assert v1 file layout (`checkpointed_rolled_wal_corruption_does_not_block_recovery`, the `PENDING_ROTATION` tests, `ann_queries_surface_corrupted_hnsw_sidecars`, `inspect_reports_manifest_wal_and_segment_targets`) are deleted in the PR that deletes the mechanism, and replaced by crash-point and corruption tests on the new format in that same PR.
- Tests that read old manifest generations (`old_snapshot_remains_readable_after_flush`, `rejects_snapshots_below_manifest_checkpoint`, `older_snapshots_*`) are rewritten to use snapshot tokens.
- Behavioral tests (create, write, delete, flush, reopen, compact, duplicate-id batches, dimension errors, namespaces) keep their assertions and switch to the new API when the trait method they use is deleted.
- The randomized harnesses (`crates/logpose-storage/tests/support/randomized.rs`, `crates/logpose-service/tests/randomized_service.rs`) stay green at every step; each PR extends their model rather than weakening it.

## Testing Strategy

### Randomized Model Checking

The storage harness gets a new model and action set. The model is a `BTreeMap<PrimaryKey, Row>` plus the list of acknowledged batches; it knows nothing about units, DVs, or files.

| Action | Model effect | Check |
| --- | --- | --- |
| `Upsert(batch)` | apply rows | ack seq range; I1 by an immediate `get` of every key |
| `Update(pk, patch)` | merge or `NotFound` | same |
| `Delete(pks)` | remove | same |
| `DeleteByFilter(f)`, `UpdateByFilter(f, p)` | evaluate `f` on the model | affected count |
| `Get(pks)`, `Count(f)`, `Search(v, f, k)` | none | exact equality; search compares against exact top-k over the model |
| `ScrollPage(cursor)` | none | the concatenation of pages equals the model at the token's `Version` |
| `Pin`, `Release`, `AdvanceClock(d)` | record model clone per token | token reads equal their clone (I12); expired tokens fail |
| `Flush`, `Compact` | none | full-state equality afterwards |
| `StepJob(job, phase)` | none | interleaves begin, build, and commit of a job with other actions |
| `Crash(k)` then `Reopen` | truncate to the durable prefix | I2, I3, I8 (below) |
| `FailSync(n)` | the in-flight batch becomes "unknown" | collection is read-only; reopen passes I8 |
| `AlterSchema(add or drop)` | update model schema | reads return null for new fields, hide dropped fields |

After every action the harness runs `Version::check_invariants` (I5, I10, I13). The seed and action trace are printed on failure, as today.

### Crash-Recovery Equivalence

Every test that crashes uses `FaultVfs`:

- **Exhaustive crash points.** Run a scenario once cleanly and count mutating ops `T`. For each `k` in `0..=T` and each `TearMode`, rerun with `crash_after_ops = k`, reopen, and check I8: the recovered state equals the model after some prefix of batches that includes every acknowledged batch and contains no partial batch. Scenarios: a single group commit, a flush, a compaction with concurrent deletes, a flush during a compaction, a checkpoint-only flush, and GC after a pinned token is released.
- **Named crash points.** One test per `CrashPoint` variant asserts the recovery outcome from the crash analysis tables in this document.
- **Recovery idempotence (I11).** Crash during recovery at every op count, reopen again, and compare to a clean recovery.
- **Corruption.** Flip bytes in each segment section, DV file, manifest, and mid-WAL frame; expect the typed error from the corresponding section (`SegmentCorrupt`, `ManifestCorrupt`, `WalCorrupt`), never a panic or silent data change.
- **Golden files.** Byte-exact WAL frames, manifest, DV file, and a small segment are committed under `crates/logpose-storage/tests/golden/`; format changes must update them deliberately.

### Concurrency Tests Without Loom

Loom does not fit async code with real I/O pools, so concurrency correctness comes from determinism plus stress:

- **Deterministic interleaving.** The scheduler has a manual mode in which jobs run only when the test calls `step(job, phase)`. The harness enumerates, for small scenarios, every position of each job phase relative to a short write sequence. This covers the DV reconciliation and the flush-during-compaction cases exhaustively.
- **Manual clock.** `Clock` is injected, so token TTL and age-based flushes are deterministic.
- **Stress with invariant readers.** N writer tasks issue random batches; M reader threads repeatedly take a `ReadView` and check internal consistency of that view: `count()` equals the number of rows a full scroll returns, every scrolled row's `get` returns the same row, no key appears twice, and `visible_seq_no` never decreases per reader. Background flush and compaction run with small thresholds. Runs for a bounded time in CI and longer in a nightly job.
- **Linearizability of acks.** A client that receives an ack immediately reads on another task and must see its write (I1), checked on every write in the stress test.

## PR Breakdown

Each PR keeps `cargo test --workspace` green, deletes what it replaces, and extends the harness for what it adds.

| PR | Title | Depends on | Parallel with |
| --- | --- | --- | --- |
| 1 | Split `logpose-storage` into modules (no behavior change) | none | 7 |
| 2 | `logpose-vfs` crate: `Vfs`, `StdVfs`, `FaultVfs`, crash points; route all storage and WAL I/O through it; crash-and-reopen in the harness | 1 | 7 |
| 3 | `Engine` shell: collection map, root lock, `IoPool` and rayon pools, `CollectionHandle` with `ArcSwap<Version>` over v1 state; delete directory scans and global lock maps | 2 | 4, 7, 8, 9 |
| 4 | WAL v2 in `logpose-wal`: frame codec, postcard payloads, reader with tail repair, writer with rotation; standalone tests on `FaultVfs` | 2, schema PR | 3, 7, 8, 9 |
| 5 | Writer task and group commit on WAL v2; `apply` shared with replay; poisoning on fsync failure; delete v1 WAL | 3, 4, 7 | 8, 9 |
| 6 | Manifest v2 and `CURRENT` protocol, version-refcount GC, orphan cleanup, snapshot tokens and reaper; replace historical snapshot reads | 5 | 8, 9 |
| 7 | Schema integration: `Schema`, `Record`, `FieldId` from the schema PR into descriptor, validation, `RowOp`, legacy mapping; move predicate AST to `FilterExpr` in `logpose-types` | schema PR | 2, 3, 4 |
| 8 | Segment v2 writer and reader as a standalone module: layout, all storage-owned encodings, opaque index sections, CRC and corruption tests, golden file | 2, 7 | 3 to 6, 9 |
| 9 | Buffer cache v1: classes, CLOCK, single-flight, pins, `FetchReport`, warm-up | 3 | 4 to 8 |
| 10 | Memtable v2, writer-private pk index, deletion vectors and DV files, flush to segment v2, recovery with pk rebuild; delete resolve-latest paths and v1 segments and sidecars | 5, 6, 8, 9 | none |
| 11 | Compaction v2: size-tiered policy, reconciliation, forwarding and incremental pk rewrite, scheduler priorities; delete `compact_state` | 10 | 12 |
| 12 | `CollectionReader`, `ReadView`, `UnitView`; query crate on the new interfaces for search, get, count, scroll, order by (using the HNSW, SQ8, kernel, and scalar-index PRs); `RowSetResolver`; delete old trait read methods | 10, index PRs | 11 |
| 13 | Harness v2 and crash-equivalence suite: full action table, exhaustive crash enumeration, deterministic job interleaving, stress tests | 10 (extends with 11, 12) | 11, 12 |
| 14 | Remove the `StorageEngine` trait and `legacy.rs`; service calls `Engine`; update `architecture.md`, `operations.md`, `configuration.md` | 11, 12, 13 | none |

```text
 engine lane:   1 -> 2 -> 3 -> 5 -> 6 -> 10 -> 11 -> 14
 WAL lane:           2 -> 4 -> 5
 schema lane:        7 -> 5;   2 + 7 -> 8 -> 10
 cache lane:              3 -> 9 -> 10
 read lane:                          10 -> 12 -> 14
 test lane:                          10 -> 13 -> 14
 external:      schema PR -> 7;  HNSW, SQ8, kernel, scalar-index PRs -> 12
```

Critical path: 1, 2, 3, 5, 6, 10, 11 or 12, 14. PRs 4, 7, 8, and 9 run beside it and should start as soon as their inputs land; 7 must land before 5 because WAL v2 payloads are `RowOp`s over the new schema types.

## Deviations From the Plan

1. **`Vfs` in its own crate.** The plan puts the `Vfs` trait in `logpose-storage`, but `logpose-wal` must use it and `logpose-storage` depends on `logpose-wal`, which would be a cycle. A small `logpose-vfs` crate below both fixes it without merging the WAL into storage.
2. **The primary-key index is not published through `Version`.** D3 describes an in-memory `pk -> (segment, row)` map rebuilt on open, which this design keeps, but as writer-private state. Readers resolve keys through each unit's immutable pk sections plus deletion vectors. Publishing the map would cost either a persistent map (two to three times the memory at 10M keys) or copy-on-write shards (megabytes copied per group under concurrent reads), and no reader needs it.
3. **Historical `Snapshot { manifest_generation, visible_seq_no }` reads are dropped.** The plan lists the snapshot type as worth keeping. With deletion vectors, a `Version` cannot be reconstructed for an arbitrary past sequence number, so repeatable reads use snapshot tokens (D7), and read barriers compare only `visible_seq_no` (the manifest generation is a physical detail that compaction changes without any logical change).
4. **Two rayon pools.** D8 names one rayon pool for CPU search. Index builds for multi-million-row compactions would starve queries on a shared pool, so maintenance CPU work gets its own smaller pool.
5. **Compaction writes the output's DV file at commit.** D3 says deletion vectors are written at checkpoints and the WAL covers them in between. That is not enough for compaction: a deletion reconciled onto the output may already be below the checkpoint (its WAL file deleted and its input's DV file dropped), so the output's DV file must be written before the compaction's manifest is published.
6. **Delete-by-filter and update-by-filter are atomic only up to one frame.** D7 makes every batch atomic. A filter matching more rows than fit in `MAX_FRAME_PAYLOAD` (about 7M `int64` keys) commits in several atomic chunks, reported in the ack, rather than requiring unbounded frames or an in-memory undo mechanism.
7. **The segment section table is at the end of the file.** Phase 2's layout sketch places the section table in the header. Compaction output is streamed, so section lengths are unknown when the header is written; the header keeps what is known up front (row count, schema hash, sequence range) and the footer points at the table.
