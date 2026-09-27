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
| Schema, `Value`, `Record` (`logpose-types`) | `CollectionSchema` (with `schema_version: u64`), `FieldId(u32)`, `FieldType`, `Value`, `PrimaryKey`, `Record`, `PartialUpdate`; the binary value codec is **not** in that PR and is specified here ([Binary Value Codec](#binary-value-codec)) |
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
9. **I9 Checkpoint coverage.** For the manifest `M` named by `CURRENT` with checkpoint `C`: every row of every segment in `M` that was deleted by an operation with sequence number at most `C` is set in the DV file that `M` names for that segment; and every operation with sequence number greater than `C` is in a WAL file that still exists. Conversely, every bit in a DV file, and every row a segment omits because it was deleted, comes from an operation whose WAL group was fsynced before the file was written (bits may be early, never speculative).
10. **I10 Row address stability.** A `RowAddr { unit, row }` never changes meaning. Segments are immutable; memtable slots are append-only; deletion bits are only ever set, never cleared, for a given unit.
11. **I11 Recovery idempotence.** Recovery that crashes at any point and is rerun produces the same state as a single uninterrupted recovery.
12. **I12 Token repeatability.** Two reads with the same unexpired snapshot token return identical results.
13. **I13 Counter exactness.** `Version` live and deleted row counters equal the values computed from row counts and deletion-vector cardinalities.
14. **I14 Durable visibility.** Every operation reflected in a published `Version` is durable: its WAL group's fsync returned `Ok` before the `Version` was stored. There is no visible-but-not-durable state, so a crash never takes back something a reader saw. Maintenance jobs snapshot only published (hence durable) state.

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

Arrows point from a crate to its dependencies. `logpose-vfs` depends only on `std`. `logpose-index` never does I/O: it builds structures from rows and (de)serializes them to and from byte buffers. `logpose-storage` owns every file. `logpose-query` depends on `logpose-storage` for the read interfaces and implements the `RowSetResolver` trait that storage defines, which `logpose-core` injects at engine construction. This keeps the dependency graph acyclic while letting the writer resolve delete-by-filter.

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
| `segment_v2/format.rs` | byte layout constants, header, section table, footer |
| `segment_v2/builder.rs` | `SegmentBuilder`, streaming segment writer |
| `segment_v2/reader.rs` | `SegmentReader` over a `SectionSource`, lazy section access (the engine's `SegmentHandle` wraps it) |
| `segment_v2/{pk,column,vector,dynamic,stats}.rs` | storage-owned section encodings |
| `manifest.rs` | manifest v2 codec, `CURRENT` protocol |
| `flush.rs` | flush job |
| `compaction.rs` | policy and compaction job |
| `scheduler.rs` | engine-wide maintenance scheduler |
| `gc.rs` | obsolete-file tracking, deletion queue, orphan cleanup |
| `cache/` | buffer cache: budget, CLOCK rings, single flight, pins, reports, warm-up |
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
    /// Truncate. Used only by WAL tail repair and failed-group rollback.
    fn set_len(&self, len: u64) -> io::Result<()>;
}

pub trait VfsLock: Send + Sync {}
```

The trait is deliberately narrow: no seek, no in-place overwrite, no mmap. Immutable files are written once with `CreateNew` plus `append`, and the only mutable file is the WAL tail. Because `CreateNew` fails on an existing name, no file name (segment unit id, DV generation, manifest generation) is ever reused within a process, including after a failed attempt; see [Id Allocation and Failed Commits](#id-allocation-and-failed-commits).

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
    WalAfterRollback,
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

As implemented in PR 2, the crate differs from the sketch above in small ways that tests rely on:

- **Process handles.** `FaultVfs::process()` returns an `Arc<dyn Vfs>` bound to the current boot. After `crash()`, that handle and every file opened through it fail forever, so a thread left over from the crashed engine cannot write into the rebooted state. Each engine open takes a new process handle. (PR 2 shared the root lock between engines on one handle through a registry keyed by `Vfs` identity; PR 3 deleted the registry, and a second engine on any handle now fails to open.)
- **Per-boot counters.** `crash()` resets the plan to `FaultPlan::default()` and the counters to zero, so `crash_after_ops`, `fail_sync` (file syncs) and the added `fail_sync_dir` (directory syncs) are zero-based indexes since the last reboot. `mutating_ops()`, `file_syncs()` and `crash_points_hit()` expose them.
- **Truncation is volatile.** An unsynced `set_len` may or may not survive a crash (never under `DropUnsynced`).
- **Unsynced directory changes persist as an ordered prefix.** A journaling filesystem commits metadata in the background, so a crash can keep some unsynced creates, renames and removes, not only none. On crash each directory keeps its synced entry set plus a random prefix of the changes made to it since (none under `DropUnsynced`). A rename within one directory is one atomic change; a rename across directories is two independent ones.
- **A failed sync may have written part of the data.** Writeback can reach the disk for some pages before EIO, so under every tear mode except `DropUnsynced` a failed sync persists a random prefix of the unsynced bytes and poisons the rest. A rollback after a failed WAL sync must therefore itself be synced, or the failed batch can come back.
- **Cut points favor the extremes.** Every "how much of the unsynced state survives" choice is none or all half the time, so the dangerous outcomes (a whole unacknowledged frame, every unsynced rename) are hit often rather than with probability 1/n.
- **No `rand`.** `FaultVfs` uses a local SplitMix64 generator, so `logpose-vfs` has no dependencies and a recorded seed replays identically across dependency upgrades.
- **Helpers.** `exists`, `read_file` and `parent_dir` are free functions over `&dyn Vfs`; `std_vfs()` is the shared `StdVfs` handle the convenience constructors use. `StdVfs::append` joins multiple slices into one `write_all`, because `write_all_vectored` is not stable.
- **Legacy crash points.** Until the engine rewrite lands, the v1 engine reports the points it has: the WAL points in `WalWriter` and `rotate_active`, `RecoveryAfterTailRepair` in `WalWriter::open`, the manifest and `CURRENT` points in the manifest publish, `FlushAfterSegmentSync` and `FlushAfterSegmentsDirSync` in a flush's segment publish, and `CompactionAfterOutputSync` in a compaction's. `FlushAfterDvSync`, `CompactionAfterDvSync`, `GcAfterRemove` and `RecoveryAfterOrphanCleanup` have no v1 step and are first reported by PRs 6, 10 and 11.

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
    pub schema: Arc<CollectionSchema>,
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

Position on visibility and durability: a group becomes visible only after its own fsync returned, and fsyncs of one collection's WAL are strictly sequential (at most one `io` in flight, and `io(n+1)` starts only after `V(n)` is published). So a reader can never observe `G(n+1)` before `G(n)` is durable, and never observe any group before it is durable (I14). The pipelining overlaps `prepare(n+1)` with `io(n)`; it never overlaps two fsyncs or publishes ahead of one. This is stricter than D7 requires, and it is what lets DV files and flush outputs, which are built from published state, contain only durable operations (the second half of I9).

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
    schema: Arc<CollectionSchema>,
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
    ids: IdCounters,                   // next_dv_gen, next_manifest_gen (never reused), group_no
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
   1. Validate against the writer's schema at this point in the request stream (an `AlterSchema` earlier in the same group has already been applied): types, dimensions, required vectors, pk type, non-finite vector components, zero-norm vectors in cosine fields (normalization would produce NaN), duplicate pk within the batch, frame size limit. Name-keyed `Record`s and `PartialUpdate`s become `FieldId`-keyed `RowImage`s here, and only here (see [Schema Changes](#schema-changes)). On failure, fail that request's ack and skip it. No sequence number is consumed.
   2. For `Update` and filter requests, gather the old rows: memtable rows are read directly; segment rows go through a fetch stage on the I/O pool (the writer awaits it). On I/O error, fail that request only.
   3. Resolve filters through `RowSetResolver` against a `ReadView` over the writer's current private state (not the published one), so resolution sees every prior batch in order. A filter request is resolved exactly once, to a fixed key set; see [Record Types](#record-types) for chunking.
   4. Convert to `RowOp`s (full row images and pk deletes), assign `first_seq_no..=last_seq_no`, apply to the private state with `apply`, and encode one WAL frame.
   5. After the group, build the candidate `Version` and evaluate flush triggers. If one fires, set `freeze_pending`: the writer stops collecting, drains the pipeline (steps 3 and 4 for this group, then awaits its I/O and publishes it), and freezes before collecting the next group, so the frozen memtable's operations are exactly those in WAL files that end before the rotation.
3. **Await `io(n)`**, then publish `V(n)` and ack `G(n)` as in the publication protocol.
4. **Start `io(n+1)`**: one `append` call with all frames of `G(n+1)` as `IoSlice`s, then one `sync_data`. Frames are never split across `append` calls, so a torn write can only damage the last frames of the last group.

Maintenance messages drain the pipeline: when a `ControlMsg` other than `Tick` is pending, the writer does not collect the next group, awaits the in-flight `io` (which covers the last prepared group), publishes its `Version`, and only then handles the message. After the drain no group is prepared but unpublished, so every maintenance step that runs on the writer (freeze, flush begin, compaction begin, reconcile, commit) sees private state equal to the published state, which is durable (I14). The cost is one pipeline bubble per maintenance event. `Tick` does not drain: age-trigger checks only read counters, and pk-index rewrite slices touch only writer-private state that no `Version` contains.

Copy-on-write rule: every shared structure the writer mutates (`Arc<RoaringBitmap>`, `Arc<[f32]>` tails, the `imbl` structures) is copied when it is *shared*, which is `Arc::make_mut` semantics (strong count above one), never when a flag says it was *published*. A candidate `V(n)` is built before `prepare(n+1)` mutates private state but published only after `io(n)`; a published-flag rule would let `prepare(n+1)` mutate structures that the not-yet-published `V(n)` already references.

Decision: one frame per client batch, many frames per fsync. The frame is the atomicity unit (I3) and the replication unit (Phase 7). A multi-batch frame would save 48 header bytes per batch and couple independent batches' fate for no benefit.

Decision: apply before fsync, publish after. Applying early lets `prepare(n+1)` see `G(n)`'s effects (a partial update of a pk inserted by the previous group) without waiting for the disk. The cost is that an fsync failure cannot be rolled back in memory, so it poisons the collection. The WAL writer tracks `synced_len`, the end offset of the last group whose `sync_data` returned `Ok`. On an `io(n)` error (append or sync):

1. Roll back the file: `set_len(synced_len)`, then `sync_all`, then `crash_point(WalAfterRollback)`. This is what the Phase 0 atomic-batch PR does, and it makes a failed batch definitely absent after recovery instead of possibly replayed.
2. Fail every ack in `G(n)` with `LogPoseError::WalWriteFailed { outcome }`, where `outcome` is `NotApplied` if the rollback's `sync_all` returned `Ok` and `Unknown` otherwise. Fail every ack in `G(n+1)` (prepared, never appended) with `WalWriteFailed { outcome: NotApplied }`. Clients must treat `Unknown` like a timeout.
3. Set `state = ReadOnly`, store the error in `poison`, stop accepting requests, abandon in-flight jobs (their `Done` messages are dropped; their files become orphans), and never publish again. Private state includes `G(n)` and `G(n+1)`, which are not durable, so no `Version`, manifest, or DV file may be built from it. The last published `Version` keeps serving reads.
4. The collection recovers only by reopening it, which runs [Recovery on Open](#recovery-on-open) including its durability barrier. If the rollback succeeded, reopening is safe in-process. If it failed, the page cache may hold `G(n)` frames that are not on disk, and Linux may report a later fsync as successful without writing them, so an in-process reopen could make non-durable frames visible. The handle therefore moves to `Failed { rollback_failed: true }`, in-process reopen is refused, and the operator must restart the process (ideally after checking the device). This residual case is the known fsync-failure hazard. It is handled by the boot-id fence in [Decisions on Review Questions](#decisions-on-review-questions).

This matches the fault model: after a failed fsync the page cache state is unknowable, so nothing after the failure is trusted until it has been re-synced.

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
- Expiry never pulls files out from under a running read. A request resolves the token to an `Arc<Version>` once, at `read_view`, and holds that `Arc` until it finishes; the pin only keeps the `Version` alive *between* requests. A scan that outlives the TTL completes normally (its files stay referenced, I7), and only the next page with that token fails with `SnapshotExpired`.
- Pins also keep retired memtables in memory: a token taken before a flush holds the flushed memtable (up to `memtable.max_bytes`) until it is released. The registry tracks `pinned_retired_bytes`, the bytes of memtables that only pinned `Version`s still reference, and charges them to the engine-wide memtable budget. When the engine-wide total exceeds `token_memory_limit` (default a quarter of the memtable budget), the reaper expires pins oldest-first until it is below the limit, and those tokens fail with `SnapshotExpired`. Without this, 64 tokens taken across 64 flushes could hold 4 GiB of memtables that no budget counts.

The registry mutex is held only for a hash map operation, never across I/O or `.await`.

### Memory Ordering and Locking Rules

1. The only cross-thread publication edge for engine state is `ArcSwap<Version>`: the writer's `store` happens-before any `load` that returns the new pointer. Everything reachable from a `Version` is either immutable or internally synchronized.
2. Internally synchronized objects: `BufferCache` (sharded mutexes plus atomics), `FileHandle::obsolete` (`AtomicBool`, `Release` store, `Acquire` load), metric counters (`Relaxed`).
3. No lock is held across `.await`, across I/O, or across a call into another component. Every critical section is a map or queue operation.
4. No nested locks. The engine has no lock order because no code path holds two locks.
5. Rayon tasks never block on a future and never do I/O. Tokio tasks never do blocking I/O or more than about 50 µs of CPU work; larger work goes to a pool. Freeing a flushed memtable (up to 1M slots of persistent-structure nodes) is such work: when the writer retires a memtable or a `Version` whose last holder may be the writer itself, it moves its `Arc` into a retire queue drained on the maintenance pool instead of dropping it inline.
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

### Implementation Notes (PR 3)

PR 3 builds the engine shell over the v1 state. Where it differs from the sketches above, this list is the current contract for later PRs.

- **Public surface.** `logpose_storage` exports `Engine`, `EngineConfig`, `CollectionHandle`, `CollectionMeta`, `Version`, `VersionId`, `VersionCounters`, `Runtime`, `RuntimeConfig`, `IoPool`, and `run_cpu`. `LocalStorageEngine` is a thin `StorageEngine` adapter over an `Engine` (`from_engine`, `engine()`). `EtcdBackedStorageEngine::with_local` and `AppState` share one engine between the data plane and the catalog.
- **One engine per root, in-process too.** The PR 2 in-process root-lock registry is deleted. `Engine::open` holds its `VfsLock` directly, and a second engine on the same root fails with the new typed `LogPoseError::StorageRootLocked`, even in the same process. Two engines would each keep resident state that the other never sees. Callers share an engine by cloning it. Tests that reopened a root while an older handle was alive now drop the old one first. Tests that changed files under a live engine now drop it, change the files, and reopen.
- **Shutdown.** Dropping the last `Engine` clone sets a shutdown flag. It then blocks until every engine task has finished, and only after that releases the root lock. An engine task is an I/O-pool closure or a maintenance job, and each holds a tracked `CoreRef`. So no task touches the root after the drop returns, and an immediate reopen always succeeds. Queued maintenance that has not started stays pending in `maintenance.json` and resumes on the next open. There is no `CancellationToken` and no `tokio-util` dependency.
- **Collection map.** The map is `RwLock<BTreeMap<CollectionRef, CollectionSlot>>`, because `CollectionRef` is `Ord` but not `Hash`. A slot is one of four states:
  - `Creating` reserves the name, so concurrent creates of one name have exactly one winner, and the files are written outside the lock.
  - `Dropping` holds the name while a drop is in progress.
  - `Open(Arc<CollectionHandle>)` is a served collection.
  - `Failed` holds a recovery error. Every call on the collection returns it, and the engine still opens.

  The `find_collection_descriptor` and `list_collection_descriptors` directory scans are deleted, so a lookup is a map lookup. A directory whose descriptor cannot be parsed cannot be registered by name, so `list_collections` fails, as it did before.
- **Recovery at open.** Collections are recovered at open, not on first use, in parallel on the I/O pool. Open first removes `*.dropped` directories and directories without `descriptor.json` (step 3 of [Recovery on Open](#recovery-on-open)). Then each collection runs these steps:
  1. Finish an interrupted v1 WAL rotation.
  2. Load the manifest and replay the WAL delta.
  3. Open the active WAL, repairing its tail.
  4. Build `Version` 1.

  The descriptor's `root_path` is replaced by the directory it was found in, so a moved root or a copied collection uses its own files. WAL tail repair (`RecoveryAfterTailRepair`) now happens at open, not on the first write.

  Persisted pending maintenance does not start at open. It resumes on the collection's first data-plane access through `StorageEngine` (a read, write, stats, inspect, flush, or compact) or on an explicit `recover_maintenance_descriptor`. Metadata and status reads never start it. This keeps the v1 behavior that a node which only reports status for a collection, such as a control-only node, never runs that collection's jobs. Placement-aware scheduling replaces this rule later.
- **Drop.** `Engine::drop_collection` runs these steps:
  1. Mark the handle dropped, so new calls fail with "does not exist".
  2. Wait for the collection's maintenance slot and writer slot.
  3. Rename the directory to `<dir>.dropped` and sync the parent directory. This is the commit point.
  4. Remove the directory.

  If the rename fails, nothing changed on disk and the handle serves again. If the rename happened but its directory sync failed, the outcome is unknown until a reopen: the handle keeps refusing calls. A pinned `Version` stays readable from memory, but a read that needs a segment file of the dropped collection fails. Drop is not yet exposed through `StorageEngine` or the APIs.
- **`Version` over v1 state.** A `Version` holds the v1 `Arc<Manifest>` plus a `DeltaLog`. The `DeltaLog` is a persistent append-only log of committed WAL batches: sealed chunks of 64 batches plus one open chunk. Publishing a batch copies at most 64 pointers plus one pointer per sealed chunk, and never clones a record. `VersionCounters` maintains `segment_count`, `memtable_rows`, and `memtable_bytes` incrementally, so the flush trigger is O(1). `total_rows` and `deleted_rows` need deletion vectors and come with PR 10. `Version::check_invariants` checks I3, delta contiguity, and I13 for these counters.
- **`imbl` is not allowed.** `imbl` is MPL-2.0, and `deny.toml` allows only MIT, Apache-2.0, BSD-3-Clause, Unicode-3.0, and Zlib. PR 10 (`DeletionMap`, memtable indexes) must choose a permissively licensed persistent map, write a crate-local one, or get an explicit license decision.
- **Writer stand-in.** PR 5 replaces two per-handle mutexes, which stand in for the writer task until then:
  - `writer: Mutex<WriterSlot>` owns the open active `WalWriter`. It serializes WAL appends, flush, reload-after-failure, and every `current.store`.
  - `maintenance: Mutex<()>` serializes flush and compaction, and is always taken before `writer`.

  Both are held across blocking I/O, but only on I/O-pool or job threads, never on a tokio worker and never across `.await`. This is a transitional exception to rules 3 and 4 of [Memory Ordering and Locking Rules](#memory-ordering-and-locking-rules). They replace the deleted process-global `wal_rotation_locks`, `maintenance_operation_locks`, `maintenance_status_locks`, and `maintenance_coordinator`. The maintenance queue and its status now live on the handle, and reads of the status need no file access.
- **Publication.** Only the holder of the writer slot publishes: `current.store`, then `visible.send_replace`, then the acknowledgement (I1). Compaction builds its output holding only the maintenance slot. It takes the writer slot just to publish the latest version with the new manifest, so writes continue while it runs.
- **Failures.**
  - A failed WAL append closes the cached `WalWriter`, and the next write reopens it with tail repair, as v1 did. That write first reloads the durable state: if the append's rollback also failed, the failed batch's whole frame may still be in the WAL (or only in the page cache), so the collection is poisoned rather than reuse its sequence numbers or make it visible. PR 5 replaces this with poisoning on every WAL failure.
  - A flush or compaction that fails after it may have changed durable state reloads the collection from disk under the writer slot and publishes the result.
  - If that reload also fails, the handle is poisoned ("read-only until it is reopened"). Writes and maintenance are refused, and reads keep serving the last `Version`. The only way to reopen a single collection is to reopen the engine.
- **Pools.** `Runtime { io, query, maintenance }` is as sketched.
  - `IoPool` is `std::sync::mpsc` feeding N threads, with a tokio `Semaphore` bounding queued plus running `run` jobs (default depth 1024). `in_flight()` is the queue-depth metric, and a panicking job is reported as an error.
  - Every `StorageEngine` method runs its blocking work (reads, writes, create) on the `IoPool`, so no blocking I/O runs on a tokio worker.
  - v1 flush and compaction interleave HNSW builds with blocking I/O. They would hog the I/O pool, and a blocked rayon worker can deadlock work stealing, so they run on neither. Instead they run on a separate `jobs` pool of `maintenance_threads` blocking threads, with at most one job loop per collection. A loop runs one job and then requeues itself behind the waiting jobs, so a collection whose writes keep refilling its queue cannot starve other collections or explicit flush and compaction requests.
  - The rayon `query` and `maintenance` pools exist, with `Engine::run_query` and `run_cpu`, but nothing uses them yet. Flush and compaction CPU moves to `maintenance` once flush is staged (PR 10), search moves to `query` (PR 12), and the `jobs` pool is deleted then.
- **Historical snapshots.** Reads of the current manifest generation use the resident `Version` and read no metadata files. A `Snapshot` naming an older generation still loads that manifest and replays the WAL from disk, until PR 6 replaces historical reads with tokens.
- **Read barrier.** `CollectionHandle::wait_visible(min_seq_no, timeout)` waits on the `watch` channel. The service still compares snapshots itself.
- **Deferred.**
  - PR 5: group commit, the writer task, and poisoning on fsync failure.
  - PR 6: manifest v2, id burning, GC, snapshot tokens, and the recovery durability barrier.
  - Later PRs: `Clock`, `BufferCache`, scheduler priorities, and `RowSetResolver` injection.
  - PR 10: `stats` still resolves every segment, which is O(data), until deletion-vector counters exist.

## Errors

`LogPoseError` (`crates/logpose-types/src/error.rs`) is typed from storage to the wire (P6a). Each variant has a canonical `ErrorCode`, a stable `reason`, and structured details; the REST and gRPC crates map them to wire statuses in one function each. The error names used in this document map onto it as follows:

- `WalCorrupt`, `SegmentCorrupt`, and `ManifestCorrupt` are `Corrupt { kind: CorruptionKind::{Wal, Segment, Manifest}, location, message }` (`DATA_LOSS`). `WalError` and `SegmentError` convert with `From`, using their `is_corruption`.
- A poisoned collection is `CollectionPoisoned { collection, reason }` (`UNAVAILABLE`).
- `ReadBarrierNotSatisfied` and `StorageRootLocked` are variants of the same name.
- `BatchTooLarge` is `TooLarge { what, size, limit }` (`RESOURCE_EXHAUSTED`).
- `WalWriteFailed { outcome }`, `WriteStalled`, `SnapshotExpired`, `TooManySnapshots`, `UnsupportedFormat`, and `NotFetched` do not exist yet. The PR that introduces one adds a variant with its code and reason, and adds it to `fixtures::one_of_each_variant`; the exhaustive match in the `error.rs` tests and the transport mapping tables fail until it does.

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
    44    4 group_no       low 32 bits of the writer's fsync-group counter
    48    n payload        postcard-encoded WalPayload
  48+n    p padding        zeros so the next frame starts 8-byte aligned (not CRC-covered)
```

The header has its own CRC so that a torn header is detected before `payload_len` is trusted. Replay rejects a frame whose `payload_len` exceeds `MAX_FRAME_PAYLOAD` or the remaining file length. A batch whose encoding exceeds `MAX_FRAME_PAYLOAD` is rejected at validation with `LogPoseError::BatchTooLarge`.

Sequence rules:

- `WriteBatch` covers `first_seq_no..=last_seq_no`, one sequence number per `RowOp`.
- `SchemaChange` consumes exactly one sequence number (`first == last`).
- `Checkpoint` consumes none. It carries `first == last == checkpoint_seq_no` and is excluded from the contiguity check.
- Across data frames, `first_seq_no` must equal the previous data frame's `last_seq_no + 1`. A gap in the uncheckpointed range is corruption.
- The last frame of every fsync group has `GROUP_END` set, and every frame of a group carries the same `group_no`; consecutive groups have consecutive numbers (mod 2^32). Only the frames after the last durable `GROUP_END` can be unsynced, and `group_no` is what lets tail repair tell which group a frame after a damaged one belongs to.
- The first data frame of a file has `first_seq_no` equal to the sequence number in the file name.
- On replay, a data frame with `first_seq_no <= checkpoint_seq_no < last_seq_no` is corruption: checkpoints fall on batch boundaries (a freeze happens between groups), so a straddling frame means a sequence number was written twice.

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
    /// `CollectionSchema::schema_version` the rows were validated against.
    pub schema_version: u64,
    /// One sequence number each, in order, starting at first_seq_no.
    pub ops: Vec<RowOp>,
}

#[derive(Serialize, Deserialize)]
pub enum RowOp {
    /// Blind write of a complete row image. Upserts, partial updates, and
    /// update-by-filter all become `Put` with the merged full row.
    Put(RowImage),
    /// Blind delete by key. Delete-by-filter becomes many `Delete`s.
    Delete(WirePk),
}

/// Externally tagged mirror of `logpose_types::PrimaryKey`, which is
/// `#[serde(untagged)]` for JSON and so cannot be decoded by postcard.
#[derive(Serialize, Deserialize)]
pub enum WirePk {
    Int64(i64),
    String(String),
}

/// A row normalized to the schema. Keyed by FieldId, never by name.
#[derive(Serialize, Deserialize)]
pub struct RowImage {
    pub pk: WirePk,
    /// Sparse (FieldId, vector) pairs sorted by FieldId; absent = null.
    /// Cosine fields are already normalized. Each vector is encoded as
    /// length-prefixed little-endian f32 bytes (serde_bytes).
    pub vectors: Vec<(FieldId, F32Bytes)>,
    /// Sparse (FieldId, value) pairs sorted by FieldId; absent = null.
    /// Each value is in the binary value codec (serde_bytes).
    pub scalars: Vec<(FieldId, ValueBytes)>,
    /// Undeclared keys (`$extra`): one JSON object node in the binary value
    /// codec, keys sorted, with no key that the schema the row was validated
    /// against declares or retires. None when there are no keys.
    pub dynamic: Option<ValueBytes>,
}

#[derive(Serialize, Deserialize)]
pub struct SchemaChangePayload {
    /// The complete new schema. `schema_version` is the previous one plus 1.
    /// Dropped fields are simply absent; `next_field_id` guarantees that
    /// their FieldIds are never reassigned.
    pub schema: CollectionSchema,
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

Delete-by-filter and update-by-filter atomicity: the writer resolves the filter exactly once, against its private state when the request reaches the front of the stream, to a fixed list of keys (and, for updates, their merged row images). It writes one frame when the result fits `MAX_FRAME_PAYLOAD`. When it does not, it splits the fixed key list into chunks, each one frame and one atomic batch, and commits them in consecutive groups without taking any other request from the channel in between; the ack reports `chunks > 1` and is sent after the last chunk is published. The filter is never re-evaluated between chunks: re-resolving an update-by-filter whose patch does not change the filter's truth value would match the already-updated rows again and never terminate. Readers can observe a prefix of the chunks, and a crash can leave a prefix durable (the ack then never arrives). This is a deliberate limit (see [Deviations From the Plan](#deviations-from-the-plan)).

Checkpoint frames are written as the first frame of every new WAL file and after every flush commit. Recovery uses them only as a cross-check (`manifest.checkpoint_seq_no >= marker`). They exist so a WAL tailer in Phase 7 learns what is safe to discard without reading manifests.

### Encoding Choice

Decision: postcard (via serde) for the payload envelope, a hand-written layout for the frame header, and the [Binary Value Codec](#binary-value-codec) for values. Postcard is compact (varints) and deterministic, but it is not self-describing, so it cannot decode anything that needs `deserialize_any`. Two schema-PR types do: `PrimaryKey` is `#[serde(untagged)]`, and `Value::Json` and `Record::extra` hold `serde_json::Value`. The WAL therefore never postcard-encodes `PrimaryKey`, `Value`, or `Record`; it uses `WirePk` and `ValueBytes`. `CollectionSchema` (plain structs and externally tagged enums) is postcard-safe, and PR 7 adds a round-trip test that keeps it so. `format_version` gates decoding and a golden-bytes test pins the encoding. Vectors use a `serde_bytes` wrapper (`F32Bytes`), so a 768-dim vector is one 3072-byte copy. The WAL is short-lived, so format evolution is handled by refusing to open a WAL with an unknown `format_version`.

### Binary Value Codec

One deterministic byte encoding for `Value` and JSON, owned by `logpose-types` (`value::codec`, PR 7) and used by WAL `ValueBytes`, memtable `Json` and `$extra` cells, the segment `JsonValue` scalar encoding, and `DynamicJson` blocks. The schema PR has no binary codec, so this document specifies it. Varints are LEB128; signed integers are zigzag varints.

```text
tag  Value            payload
0x00 Null             none
0x01 Bool false       none
0x02 Bool true        none
0x03 Int64            zigzag varint
0x04 Float64          8 bytes LE (finite; -0.0 already folded to 0.0)
0x05 String           varint byte length, UTF-8
0x06 Timestamp        zigzag varint microseconds
0x07 Array            varint count, then that many encoded Values
0x08 Json             one JSON node

tag  JSON node        payload
0x10 null             none
0x11 false            none
0x12 true             none
0x13 integer (i64)    zigzag varint
0x14 integer (u64)    varint, only for values above i64::MAX
0x15 float            8 bytes LE (finite; -0.0 folded to 0.0)
0x16 string           varint byte length, UTF-8
0x17 array            varint count, then nodes
0x18 object           varint count, then (varint key length, key UTF-8, node), keys in strictly increasing byte order
```

`decode(bytes, FieldType)` checks that the tag fits the field type, so a corrupt cell is a typed error, not a wrong value. Equal values always encode to equal bytes (object keys sorted, one integer form per value, one zero, and a `json` value of JSON `null` stored as `0x00` Null), which the segment dictionary encodings and golden files rely on. Decoding is strict and accepts only canonical bytes (minimal varints, no NaN, infinity or -0.0, `0x14` only above `i64::MAX`, valid UTF-8, timestamps within years 0000 to 9999, no null array elements, no trailing bytes), so `encode(decode(bytes)) == bytes` for every accepted input. Arrays and JSON containers nest at most 128 levels on both sides, and a declared length or count larger than the remaining input is rejected before anything is allocated.

### Tail Repair

On open, for the highest-named file only:

1. Scan frames from offset 0. Stop at the first frame that fails any check: short header, bad magic, bad `header_crc`, oversized or out-of-file `payload_len`, bad `payload_crc`, undecodable payload, or sequence discontinuity.
2. Let `valid_end` be the end offset (after padding) of the last good frame.
3. Distinguish a torn tail from corruption: search from the failed offset to EOF, at 8-byte steps, for frames with valid magic, `header_crc`, and `payload_crc`. Because a group is one append followed by one fsync, and the next group is appended only after that fsync returns, at most one group (the last) can be partially persisted, and a page cache may persist its pages in any order. So valid frames after the failure are expected, but only from the damaged frame's own group. That group is `g* = P.group_no` when the last good frame `P` lacks `GROUP_END`, `P.group_no + 1` when it has it, and unknown when the failure is at offset 0. The tail is torn only if every valid frame after the failure has `group_no == g*` (all one value when `g*` is unknown) and at most the last of them has `GROUP_END`. Otherwise a later group was durably written after the damaged one: fail the collection open with `LogPoseError::WalCorrupt { file, offset }` and modify nothing. `group_no` matters when the damaged frame is the `GROUP_END` frame of an acknowledged group followed by one complete later group: without it, the later group's single `GROUP_END` looks like the end of the torn group, and repair would truncate two acknowledged groups.
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

### Implementation Notes (PR 4)

PR 4 builds the frame layer as `logpose_wal::v2`, beside the version 1 WAL, which PR 5 deletes. Payloads are opaque bytes here; the frame type reuses `codec::PayloadKind`. The API:

- `WalFrame::{new, write_batch, schema_change, checkpoint}` validates the sequence rules and the size limit and computes the payload CRC, so the engine can build frames on the CPU pool. `WalWriter::append_group(&[WalFrame])` builds the headers (group number, `GROUP_END` on the last frame), issues one `append` and one `sync_data`, and returns a `GroupCommit` with the group number, sequence range and offsets.
- `WalRecovery::open(vfs, dir, WalConfig, checkpoint_seq_no)` runs the fence check, the durability barrier for `wal/` (`sync_all` on every WAL file, then `sync_dir`) and tail repair. `next_frame()` then streams the frames with `last_seq_no > checkpoint_seq_no`, one payload in memory at a time and checked strictly. `into_writer(&checkpoint_frame)` validates what is left, removes a marker from an earlier boot, and returns the writer, creating `wal/` and its first file if the directory has none. When the file it continues is empty (new, or truncated to nothing by tail repair), it first appends and syncs the checkpoint frame as that file's first group. `WalConfig` holds the rotation size, the epoch and the `BootId`.
- `WalWriter::rotate(&checkpoint_frame)`, `should_rotate()` and `remove_checkpointed(C)` cover rotation and checkpoint truncation. `clear_fence` is the operator acknowledgement. Errors are the typed `WalError` (`Corrupt`, `UnsupportedFormatVersion`, `UnexpectedFile`, `FsyncFailedSameBoot`, `FenceUnreadable`, `WriteFailed { outcome }`, `WriterFailed`, `InvalidFrame`, `FrameTooLarge`, `Io`), with `WriteOutcome::{NotApplied, Unknown { fenced }}`. PR 5 maps them onto `LogPoseError`.

Where the implementation is more specific than, or differs from, the text above:

1. **Incomplete groups are discarded as a unit.** Tail repair truncates to the end of the last frame with `GROUP_END`, not the end of the last good frame. A group that is only partly persisted was never acknowledged, so dropping all of it loses nothing. It also keeps every file ending on a group boundary, so the group-number rule holds for the frames appended after repair. I8's "longest valid WAL frame prefix" is therefore the longest valid prefix of whole groups.
2. **Every file starts with a checkpoint group.** The first frame of every WAL file is a checkpoint frame that is a group of its own, appended and synced before anything else lands in the file: rotation writes it, and so does `into_writer` for an empty file. Replay rejects a file whose first checksummed frame is anything else. This settles the case the tail repair rule above leaves unknown, a damaged frame at offset 0: that frame is the checkpoint group, durable before any later append, so any checksummed frame after it means a later group is durable and the open fails with `Corrupt`. Without it, a damaged first frame followed by exactly one acknowledged group looks like a torn first group, and repair would truncate the acknowledged group.
3. **Only checksum failures can be a torn tail.** A torn write cannot produce a frame whose checksums match, so a checksummed frame that breaks a rule is `WalError::Corrupt` (or `UnsupportedFormatVersion`) at once, anywhere in the log, and is never truncated. The rules are: an unknown type, reserved flag bits, `payload_len` above the limit, a bad sequence range for its type, a sequence gap, a first data frame that does not match the file name, or a group number that does not continue the previous frame. Also, when the damaged frame's header is checksummed (bad payload CRC, or the frame runs past the end of the file), the search for later frames starts after its claimed extent, so a payload that happens to contain frame bytes is not taken for a frame.
4. **Cross-file checks.** Replay also requires each file's name to equal the previous file's last data sequence number plus one, requires every file that another file follows to hold a data frame (rotation never leaves one without, so an older file with none has lost the operations before the next file's name), requires a file never to end inside a group, and rejects a checkpoint that lies beyond the end of the log or before the oldest file that is still needed. Checkpoint frames at or below the checkpoint are skipped like data frames, so replay returns exactly the frames with `last_seq_no > checkpoint_seq_no`.
5. **A clean rollback leaves the writer usable.** After `set_len(synced_len)` and `sync_all` succeed, `append_group` returns `WriteFailed { outcome: NotApplied }`, and the next group reuses the failed group's number, sequence numbers and offset. The engine still poisons the collection as [Group Commit](#group-commit) requires; that is PR 5's decision, not the frame layer's. If the rollback fails, the writer writes the marker (a temp file that is synced and then renamed over `FSYNC_FAILED`, followed by a directory sync) and fails for good. When the marker also cannot be written, the error carries `Unknown { fenced: false }` and the caller must stop the process. A marker ends with an end line, so a truncated marker never parses. An unparsable marker refuses the open (`FenceUnreadable`), as a marker from the current boot does.
6. **Rotation details.** `rotate` does nothing and returns `false` when the active file has no data frame, because the new file would have the same name. If the new file cannot be created, the writer stays on the old file. A failed append or sync of the checkpoint frame rolls the new file back to empty (`set_len(0)` and `sync_all`, or a fence if that fails). A failed append or sync, or a failed directory sync, fails the writer, because appending more to the old file while a newer file may exist would put sequence numbers into the old file that recovery attributes to the new one.
7. **Boot id.** `BootId::current()` reads `/proc/sys/kernel/random/boot_id` with `std::fs`, since it is host identity rather than engine state. Where that file is missing, it uses a value built from the process id and start time. Tests inject boot ids through `WalConfig`.
8. **Checksum.** CRC-32C comes from the `crc32c` crate, the same crate the segment v2 code uses. A test pins the check value `crc32c("123456789") = 0xE3069283`.

Tests: unit tests next to each module (header layout and golden bytes, every flipped header bit, decode bounds, sequence and group rules, marker format), end-to-end tests on `FaultVfs` for each repair and corruption rule and for rollback, fence and rotation failures, and `crates/logpose-wal/tests/wal_v2_crash.rs`. That file holds a model-checked random scenario (150 seeds times every `TearMode`, with crashes during writing and recovery, failed syncs, rotations and checkpoint truncation), exhaustive `crash_after_ops` enumeration for 4 seeds times every `TearMode` with a second crash after the repaired log is appended to, random byte flips across all frames of a multi-file log, and an in-process reopen after a rollback followed by a crash. `crates/logpose-wal/tests/wal_v2_fuzz.rs` damages random multi-file logs with bit flips, truncation and appended garbage, predicts the exact outcome from where the damage landed (every frame, the whole groups before a torn tail, or a typed corruption error), and checks it; `LOGPOSE_WAL_FUZZ_TRIALS` raises its trial count.

## Memtable

### Row Storage

A memtable is append-only in slots. All structures are persistent (`imbl`, the maintained fork of `im`), so the writer mutates its private copy in place and a published `Version` holds an O(1) clone that later writes never change.

```rust
/// Clone is O(number of fields): every field is an Arc or a persistent
/// structure. The writer owns one copy; each published Version holds a clone.
#[derive(Clone)]
pub struct MemtableData {
    pub unit: UnitId,
    /// Latest schema applied to this memtable. Schema changes apply in place
    /// (see Schema Changes); readers use `Version::schema`, never this, to map
    /// names to FieldIds.
    pub schema: Arc<CollectionSchema>,
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
    /// One column per scalar field declared at any time during this
    /// memtable's life, sorted by FieldId, with the first slot it covers.
    /// Slots below `first_slot` (rows written before the field was added)
    /// read null. Columns of dropped fields stay until flush and are never read.
    pub columns: Vec<(FieldId, u32 /* first_slot */, MemColumn)>,
    /// `$extra`: one JSON object node per slot in the binary value codec.
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
/// copy-on-writes the `recent` tier of that key's posting (see CowBitmap).
pub enum MemScalarIndex {
    /// bool, string, int64, and array elements: equality and IN.
    Inverted {
        terms: imbl::OrdMap<IndexKey, CowBitmap>,
        nulls: CowBitmap,
    },
    /// int64, float64, timestamp: ranges and order_by. Keys are total-ordered
    /// (floats via a total-order wrapper; NaN rejected at validation).
    Sorted {
        values: imbl::OrdMap<OrderedKey, CowBitmap>,
        nulls: CowBitmap,
    },
}
```

`auto` indexing (D4) creates `Inverted` for `bool`, `string`, and arrays, and both `Sorted` and `Inverted` for numbers and timestamps. Indexes include dead slots; readers always `AND NOT` the unit's deletion vector, so slots never need to be removed from bitmaps. Postings use the two-tier `CowBitmap` from [Deletion Vectors](#dv-structure), so a group copies at most about 8 KB per touched key. A plain `Arc<RoaringBitmap>` per key would copy the whole posting per group; for a low-cardinality field (a `bool`, or a `tenant` with a few values) in a 1M-slot memtable that is up to 128 KB per touched key per group, and several such fields per row.

### Upsert of an Existing Key

Decision: a new slot plus a deletion bit on the old slot, never in-place slot reuse. In-place reuse would mutate data that published `Version`s can see (breaking I4) unless every slot were copy-on-write, and it would need removal from every index bitmap. Append plus delete keeps every memtable structure append-only; the cost is dead slots, which count toward the flush trigger and are dropped at flush.

`apply` (one function, shared by the live writer and WAL replay), for each `RowOp` with sequence number `seq`:

1. `old = pk_index.resolve(pk)`, following forwarding tables (see [Primary-Key Index](#primary-key-index)).
2. If `old` is `Some(addr)`: `deletes.mark(addr)`.
3. For `Put(row)`: append slot `s` to the active memtable (pk, seq, vectors, columns, dynamic, index entries); `pk_to_slot.insert(pk, s)`; `pk_index.insert(pk, RowAddr { unit: active.unit, row: s })`. Values whose `FieldId` the memtable's current schema does not declare are discarded (this happens only in replay, when the manifest's schema is newer than the frame; see [Schema Changes](#schema-changes)).
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

## Schema Changes

`CollectionSchema` from the schema PR assigns every field a `FieldId(u32)` that is never reused (`next_field_id` only grows), bumps `schema_version: u64` on every change, and supports add scalar field (always nullable), drop field, and rename field. Dropped fields leave no tombstone; their ids are simply absent. `Record` and `PartialUpdate` are keyed by field *name*. Storage never stores or logs names: every WAL row, memtable column, and segment section is keyed by `FieldId`, and every WAL batch and segment is stamped with the `schema_version` it was written under.

Name to `FieldId` resolution happens in exactly two places, both against a known schema: the writer's validation step (against the writer's schema at that point in the request stream), and query compilation (against the `ReadView`'s `Version::schema`). A request that names a field dropped before it reached the writer is validated against the new schema, so with dynamic fields on its value lands in `$extra`, and with dynamic fields off it is rejected.

### Alter Protocol

`AlterSchema` is a request in the same stream as writes, so it is ordered with them:

1. In prepare, apply the change to the writer's schema, producing `S'` with `schema_version = v + 1`. Invalid changes fail the request and consume nothing.
2. Assign one sequence number and encode a `SchemaChange` frame carrying all of `S'`. It commits in its group like a batch (I3: it is a batch of one).
3. Apply in place, with no freeze: set `schema` on the writer, the active memtable, and the candidate `Version`. An added field gets a memtable column with `first_slot = slot_count` and, if indexed, empty indexes. A dropped field's memtable column and indexes stay until flush; no reader resolves its `FieldId`. A rename changes nothing in storage.
4. Later requests in the same group validate against `S'`.

The next manifest records the writer's schema at its commit, which may be newer than its `checkpoint_seq_no`. Segments record their own `SchemaSnapshot`: a flush or compaction writes sections for the fields declared in the schema it captured at Begin, and omits dropped ones. A segment whose snapshot predates an added field has no section for it, so the field reads null; a segment written before a drop still has the section, which nothing reads, and the next compaction omits it.

### Replay Across Schema Versions

Recovery starts from the manifest's schema `S_M`, which may already include changes whose `SchemaChange` frames lie after the checkpoint. Replay rules:

Let `cur` be the schema replay holds, initially `S_M`.

- A `SchemaChange` frame with `schema_version <= cur.schema_version` is already reflected and is skipped. One with `cur.schema_version + 1` becomes `cur` and is applied as in step 3. Any other version is `WalCorrupt`.
- A `WriteBatch` with `schema_version` greater than `cur.schema_version` is `WalCorrupt`. One with an older version is applied normally, and `apply` discards values for `FieldId`s that `cur` does not declare. That yields the same final state as live execution: the field was dropped at a later sequence number, which hides the values the batch wrote.

### Dynamic Field Shadowing

A key in `$extra` collides with a declared name when a field is added, or renamed, to a name that rows written earlier carry in `$extra`. The rule is a pure function of the stored bytes and the reading schema, so it cannot depend on whether a compaction has run:

- **Write.** Validation never stores a key in `$extra` that the writer's schema declares or retires (a retired name is rejected with `RecordError::RetiredKey`, since readers would hide it); a partial update that merges an old row removes such keys from the merged `$extra` (the old value is not promoted into the typed field).
- **Read.** A key in a row's `$extra` is visible (to projection, to `$extra` path filters, and to undeclared-name filters) only if the `ReadView`'s schema neither declares nor retires that name. Added fields therefore read null on old rows, as D4 requires, instead of exposing the old dynamic value under the new typed name.
- **Storage.** Flush and compaction copy `$extra` bytes unchanged. They never strip shadowed keys, because stripping would make a later drop or rename (which un-shadows the name) return different results depending on compaction timing.

Retired names stay shadowed: once a name has been declared, `$extra` values stored under it stay hidden after the field is dropped or renamed, until a field with that name is declared again. See [Decisions on Review Questions](#decisions-on-review-questions).

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

Lookup resolves forwarding: while the address's unit is in `forwards`, replace it with `RowAddr { unit: target, row: map[row] }`. Chains are at most as long as the number of unfinished rewrites, normally one. A chain arises when a compaction takes, as input, a unit whose own rewrite has not finished (flush output `s` compacted into `o`, or `o1` compacted again into `o2`); it resolves `m -> s -> o` or `I -> o1 -> o2`. Reaching `map[row] == u32::MAX` means an index entry pointed at a row that was already deleted when the job started, which I5 forbids; it is an invariant violation (`strict_invariants` fails the write; otherwise the entry is treated as absent and `pk_forwarding_violations` is incremented).

Incremental rewrite: after every group and on every `Tick`, the writer processes up to `pk_rewrite_slice` (65,536) rows of the front task. For new row `o`: if the raw (unresolved) map entry for `pks[o]` equals `sources[o]`, set it to `RowAddr { unit: target, row: o }`; otherwise leave it, because the key moved or was deleted since. When a task finishes, the writer removes its `forwards` entries. This bounds the writer stall for a multi-million-row compaction to a few milliseconds per slice instead of one long pause.

Tasks run strictly in FIFO order: a task starts only after every earlier task finished. Correctness depends on it. For a chain `I -> o1 -> o2`, task 2 compares raw entries against `(o1, x)`, but entries still hold `(I, r)` until task 1 rewrites them. If task 2 ran first, it would skip those keys, finish, and remove `forwards[o1]`, and task 1 would then write `(o1, x)` entries that resolve to nothing. With FIFO order, every entry that should point into `o1` does so before task 2 reads it, and when a task removes its `forwards` entry no raw entry points into that unit: the task rewrote every entry equal to a source, and any other entry for a source row would be a second live row for that key (I5).

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
/// Append-mostly bitmap with cheap copy-on-write, shared by deletion vectors
/// and memtable index postings. Clone is two Arc increments.
#[derive(Clone, Default)]
pub struct CowBitmap {
    /// Large, rarely copied.
    base: Arc<RoaringBitmap>,
    /// Recent insertions, disjoint from `base`. Copied (Arc::make_mut) on the
    /// first insert after a clone; folded into `base` when it would exceed
    /// RECENT_MAX entries.
    recent: Arc<RoaringBitmap>,
    /// Cached base.len() + recent.len().
    len: u64,
}
pub const RECENT_MAX: u64 = 4096;

impl CowBitmap {
    pub fn contains(&self, row: RowId) -> bool;
    /// Set the bit; return false if it was already set.
    pub fn insert(&mut self, row: RowId) -> bool;
    pub fn len(&self) -> u64;
    /// bitmap := bitmap AND NOT self.
    pub fn subtract_from(&self, bitmap: &mut RoaringBitmap);
    /// bitmap := bitmap OR self (index postings).
    pub fn union_into(&self, bitmap: &mut RoaringBitmap);
    /// base OR recent, for serialization.
    pub fn to_bitmap(&self) -> RoaringBitmap;
}

/// Deleted rows of one unit. `mark(row)` is `insert(row)` on the inner bitmap.
#[derive(Clone, Default)]
pub struct DeletionVector(pub CowBitmap);
```

Two tiers make copy-on-write per `Version` cheap: a published `Version` shares `base`, and the writer copies only `recent` on its first mark after a clone. Costs, with `RECENT_MAX = 4096`:

- Per group, per touched unit: one copy of `recent`, at most 4096 entries, about 8 KB in array containers.
- Per fold: one copy of `base`, at most `row_count / 8` bytes in bitmap containers (256 KB for a 2M-row segment), once per 4096 insertions, so at most about 64 bytes amortized per deletion.

A threshold proportional to `base` (for example `base.len() / 8`) would be wrong here. A 2M-row segment with 400k deleted rows would carry a `recent` of up to 50k entries (about 100 KB), copied once per group for every segment the group touches. A random-upsert stream over 10M rows touches most segments in every group, which would be megabytes of copying per group. The fixed bound keeps the per-group cost proportional to the number of touched units.

### Marking Rows

- **Delete of a key whose live row is in a segment:** `deletes[segment].mark(row)`.
- **Upsert or partial update of such a key:** build the full image first (for an update, from the old row), then mark the segment row and append the new row to the active memtable. Both happen in one `apply` call, inside one group, so no `Version` shows zero or two live rows for the key (I5).
- **Any operation on a key whose live row is in a memtable (active or frozen):** mark the slot in `deletes[memtable.unit]`.
- **Address in a retired unit:** resolve through forwarding first, so the bit lands on the unit that the current `Version` contains.

### DV Files

Layout of `segments/<unit:08x>.dv.<generation:016x>`:

```text
offset size field
     0    8 magic            "LPDV" 0x00 0x00 0x02 0x00
     8    4 unit_id
    12    4 row_count        the segment's row count, for validation
    16    8 generation       from the collection's next_dv_gen counter
    24    8 covered_seq_no   every deletion with seq <= this is included
    32    8 bitmap_len
    40    n bitmap           RoaringBitmap portable serialization
  40+n    4 crc32c           over bytes 0..40+n
```

DV files are immutable: each checkpoint writes a new generation, and the manifest names the generation in force for each segment. Generations come from one per-collection `u64` counter, `next_dv_gen`, persisted in the manifest like `next_unit_id` and never reused within a process (see [Id Allocation and Failed Commits](#id-allocation-and-failed-commits)); a per-segment counter would reuse a generation when a failed flush is retried. DV files are written in exactly two places:

- **Flush job.** At flush start the writer, having drained the pipeline, snapshots every segment's `DeletionVector` (Arc clones). The job writes a new generation for each segment whose cardinality differs from its durable generation's (bits are only ever added, so equal cardinality means equal sets). `covered_seq_no` is the writer's `visible_seq_no` at the snapshot, which is at least the checkpoint the flush publishes, and every bit in the snapshot is from a durable operation (I14).
- **Compaction commit.** The writer writes a DV file for the output segment when reconciliation set any bit (see [Compaction](#compaction)).

A DV file may contain deletions newer than the manifest's checkpoint. That is safe because every WAL operation is a blind write. The argument that I9 plus blind writes gives I8:

- The recovered base state (segments minus DV files) can differ from the logical state at checkpoint `C` only for keys that some operation after `C` touched, because a DV bit can be early but never missing (I9), and every segment row is a copy of a row written at or before `C` (flush copies only a memtable frozen at or before `C`; compaction copies existing segment rows). Rows a job omitted because they were already deleted (`D_F` in flush, `D0` in compaction) are early deletions of the same kind.
- Every early deletion comes from a durable operation (second half of I9), so `R` is at least its sequence number and replay reapplies it. If a DV file could hold a bit from a group whose fsync had not completed, a crash that lost that group would lose the row with nothing in the WAL to explain it; the drain before every job snapshot rules this out.
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
    36    4 row_count
    40    8 schema_version  CollectionSchema::schema_version (u64)
    48    8 schema_hash     xxh3_64 of the SchemaSnapshot section payload
    56    8 min_seq_no
    64    8 max_seq_no
    72   52 reserved        zeros
   124    4 header_crc      crc32c(bytes 0..124)
```

### Section Table Entry

```text
offset size field
     0    2 kind            SectionKind code (table below)
     2    2 encoding        kind-specific encoding code
     4    4 field_id        FieldId (u32), or 0xFFFF_FFFF when not per-field
     8    8 offset          absolute, 64-aligned
    16    8 length          payload bytes, excluding padding
    24    4 crc32c          of the whole payload
    28    4 aux32           kind-specific (dimension, page_rows, ...)
    32    8 aux64           kind-specific (element counts, ...)
    40    2 flags           reserved, 0
    42   22 reserved        zeros
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

- `SchemaSnapshot`: postcard `CollectionSchema` at write time. Makes a segment decodable on its own and pins `FieldId` meaning.
- `RowMeta`: per-row sequence numbers. Encoding 1 = `u64` plain; encoding 2 = `u32` offsets from `aux64 = base` when `max - min < 2^32`.
- `PkColumn`: keys in row order. `int64`: `i64[row_count]`. `string`: `u32 offsets[row_count + 1]` then UTF-8 bytes.
- `PkSorted`: row ids sorted by key. `int64`: `i64 keys[row_count]` then `u32 rows[row_count]` (binary search needs no other section). `string`: `u32 rows[row_count]` sorted by key bytes; binary search reads `PkColumn`.
- `PkFilter`: binary fuse filter (arity 3, 8-bit fingerprints, about 9 bits per key, 0.4 percent false positives) over `xxh3_64(canonical_pk_bytes, seed 0)`, where canonical bytes are `0x01 ++ i64 LE` or `0x02 ++ UTF-8`. The hash and seed are part of the format. Payload: `seed u64, segment_length u32, segment_count u32, array_length u32, key_count u32, reserved 8`, then `u8 fingerprints[array_length]`; a key's slots are `h0 = mulhi(h, segment_count * segment_length)`, `h1 = (h0 + segment_length) ^ ((h >> 18) & mask)`, `h2 = (h0 + 2 * segment_length) ^ (h & mask)` with `h = murmur3_fmix64(key + seed)`, and its fingerprint is `h ^ (h >> 32)` truncated to 8 bits.

### Vector Sections

`VectorF32` (`aux32 = dim`, `aux64 = row_count`):

```text
offset size field
     0    4 dim
     4    4 row_count
     8    4 page_rows       max(1, 8192 / (dim * 4))
    12    4 page_count
    16    8 nulls_len       bytes of the null bitmap (0 when no nulls)
    24    4 prefix_crc      crc32c of bytes 0..24 and 28..prefix_end (reserved, nulls, page_crcs)
    28   36 reserved
    64    n nulls           RoaringBitmap portable serialization, padded to 8
     .  4*p page_crcs       crc32c of each page's bytes
     .    . padding to 64
     .    . data            row_count * dim f32 LE, rows contiguous; null rows are zeros
```

Pages are the load unit for rerank: a page is `page_rows` consecutive rows, verified against its own CRC when loaded. The prefix (header, `nulls`, and `page_crcs`, 4 bytes per page, so about 4 MB for a 2M-row segment at 768 dimensions) is its own cache unit (`page = u32::MAX` in the cache key, class `RawVectors`), verified against `prefix_crc` and loaded on the first page access; page loads read their CRC from the pinned prefix. The section CRC covers the whole payload and is checked by compaction and by `inspect --verify`.

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
    32    4 dict_count      number of dictionary entries (0 without a dictionary)
    36   28 reserved
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
| 6 `Array` | `array<T>` | `u32 offsets[row_count + 1]` into a child block, zero padding to 8, then the child block: a complete column block (same 64-byte header) encoded as one of 1 to 5 over all elements, with no nulls |
| 7 `JsonValue` | `json` | `u32 offsets[row_count + 1]` then binary `Value` bytes |

A dropped field has no section in segments written after the drop; readers return null for fields whose `FieldId` has no section, which is also how added fields read in old segments.

### Dynamic JSON Column

`DynamicJson` stores `$extra` as the binary `Value` codec from the schema PR, in blocks of 4096 rows:

```text
header (64 bytes): row_count u32, block_rows u32 (4096), block_count u32,
                  index_crc u32 (crc32c of header bytes 0..12 and the block index), reserved
block index: block_count x { offset u64 (relative to section), len u32, crc32c u32 }
blocks: each = u32 offsets[rows_in_block + 1] then value bytes; 8-byte aligned
```

A block is the load and verification unit, so a filter on `$extra.color` scans block by block and a projection of ten rows loads at most ten blocks.

### Index and Stats Sections

- `ScalarInverted` and `ScalarSorted`: produced and parsed by the scalar-index PR (`InvertedIndex::write_to`, `InvertedView::view`, and the same for sorted). Storage requires lookups that return `RoaringBitmap` over row ids, and, for sorted, an ordered iterator of `(value, row)` from a starting value in either direction (serves `order_by`).
- `Stats`: postcard `SegmentStats`: per field a zone map (`min`, `max`, `null_count`), the exact distinct count, the top 16 values with counts, and a 32-bucket equi-depth histogram for numbers; array fields describe their elements. There is no unbounded `value_counts`. Zone maps and distinct counts are also copied into the manifest so pruning never opens the file.

### Lazy Section Loading

```rust
pub struct SegmentHandle {
    pub unit: UnitId,
    pub file: Arc<FileHandle>,
    pub header: SegmentHeader,
    pub row_count: u32,
    pub sections: Arc<[SectionEntry]>,
    /// Parsed at open: small and needed by every read.
    pub schema: Arc<CollectionSchema>,
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

The reader reads through a minimal positioned-read trait, `SectionSource { len, read_exact_at }`, which mirrors the read half of `VfsFile` so a Vfs file adapts with a forwarding impl; `MemorySource` and `FileSource` (std `pread`) implement it today.

Section bytes are never read directly: every access goes through `BufferCache::get_or_load(CacheKey { file, section, page }, class, loader)`, where the loader runs on the `IoPool`, reads with `read_exact_at`, and verifies the whole-section CRC (whole units) or page CRC (paged units) before the bytes enter the cache. A CRC failure returns `LogPoseError::SegmentCorrupt { unit, section }`; the collection stays open and the query fails.

### Implementation Notes (PR 8)

PR 8 implements the format as a standalone module, `logpose-storage::segment_v2`, not yet used by flush or compaction. Where this section left a detail open, the implementation fixed it as follows; the golden file `crates/logpose-storage/testdata/segment_v2/golden.seg` pins every byte, and any change to it requires a `format_version` bump.

- **Strict layout.** Open rejects a table whose sections are not in file order, back to back with only the padding that 64-byte alignment requires, starting at byte 128 and ending where the table starts. Padding therefore has a known length; `verify` (compaction and `inspect --verify`) also checks that every padding byte is zero, so every byte of the file is checked by something even though padding has no CRC.
- **Open reads** the header, the footer, the section table, and `SchemaSnapshot` (checked against `schema_hash`), and checks per-field sections against the snapshot (vector dimension in `aux32`, row count in `aux64`). Everything else is verified when loaded. Every read is bounds-checked against the validated file length before a buffer is allocated, and every count inside a section is checked against the section's length before anything is reserved. Materialized values are the exception by design: dictionary codes repeat their string and an `array<bool>` cell of any length fits in a few bytes of bitmap, so `ScalarColumn::value` and `read_rows` cost the logical size of the data. `verify` checks every stored value without materializing rows, so its work and memory stay bounded by the file size.
- **Section set.** `SchemaSnapshot`, `RowMeta`, `PkColumn`, `PkSorted`, `PkFilter`, and `Stats` are always present. Each declared vector and scalar field gets one section even if every row is null. `DynamicJson` is written only when at least one row has dynamic keys. Index sections come last, ordered by kind and field.
- **`RowMeta`** uses encoding 2 whenever every `seq - min` fits in `u32` (base `min` in `aux64`), else encoding 1. An empty segment has an empty encoding-1 payload and sequence range 0..0.
- **Offsets are `u32`**, as specified, so one string, JSON, array, or dynamic block column holds at most 4 GiB of values; the builder reports `TooLarge` past it. A segment holds at most `u32::MAX - 1` rows because `u32::MAX` is the forwarding sentinel.
- **Canonical stored values.** Null rows store 0, 0.0, code 0, or an empty range; floats are finite with no `-0.0`; dictionaries are strictly sorted. The decoder rejects anything else, so decoding and re-encoding reproduces the bytes.
- **Lazy units have their own CRCs.** The `DynamicJson` header and block index gained `index_crc`, because they are the unit loaded before any block and had no checksum of their own. The `VectorF32` `prefix_crc` covers the reserved bytes and the padding after the nulls too, so the whole prefix is checked as loaded.
- **Stats keep an exact distinct count** instead of a HyperLogLog sketch: every build (flush or compaction) sees every row it writes and nothing merges per-segment sketches, so a sketch would only add 4 KiB per field and estimation error.
- **CRC and hash crates.** CRC-32C comes from `crc32c` and `xxh3_64` from `twox-hash` (both MIT or Apache-2.0); `xxhash-rust` was not used because its BSL-1.0 license is outside the `deny.toml` allow list.
- **Builder memory.** `SegmentBuilder` holds every column until `finish`, which then streams the file section by section to any `Write`, so peak memory is about the size of the segment. That is what compaction's maintenance-memory reservation accounts for.

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
    pub schema: CollectionSchema,
    pub next_unit_id: u32,
    /// Next DV file generation; one counter for all segments.
    pub next_dv_gen: u64,
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
    pub generation: u64,
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

Errors: a failure in steps 1 to 3 aborts the commit with no state change to `CURRENT`; the job is abandoned and re-planned as in [Id Allocation and Failed Commits](#id-allocation-and-failed-commits). A failure in step 4 or 5 leaves the durable `CURRENT` unknown: the rename may be visible in the page cache but not on disk. The writer poisons the collection (read-only until reopen), exactly like a WAL fsync failure, and the reopen's [durability barrier](#recovery-on-open) syncs the collection directory before recovery trusts whichever `CURRENT` it reads.

### Id Allocation and Failed Commits

Every immutable file is created with `CreateNew`, so a name must never be issued twice within a process, including by a retry after a failure. The writer allocates, and never gives back:

- unit ids from `next_unit_id` (memtables, flush outputs, compaction outputs);
- DV generations from `next_dv_gen`;
- manifest generations from `next_manifest_gen`, which starts at the durable generation plus one and advances on every publish *attempt*.

A flush or compaction whose files or manifest publish (steps 1 to 3) fail is abandoned: its unit id, DV generations, and manifest generation are burned, and every file it created (segment, DV files, the partial `<g>.mf`) is enqueued for GC right away, because no durable manifest names them. The scheduler re-plans the work later with fresh ids, and the new attempt's Begin captures fresh inputs. Without this rule, a retry would hit `EEXIST` on its own leftovers and the job would fail forever until a restart ran orphan cleanup. A burned manifest generation leaves a gap in the numbering, which recovery tolerates: it only ever reads the generation `CURRENT` names. The manifest records the counters as of its commit; after a crash, orphan cleanup removes every file the counters could collide with before the writer starts.

## Flush

### Flush Steps

The flush of frozen memtable `F` (unit `m`, frozen at last sequence number `L`). Below, "manifest `g + 1`" means a manifest built on whatever manifest is durable at commit time (a compaction may have committed since Begin) with the next generation from `next_manifest_gen`; the same holds for compaction.

1. **Freeze** (writer). As in [Frozen Memtable](#frozen-memtable): `F` joins `frozen`, the WAL rotates so the new file starts at `L + 1`, a `Version` is published, a permit is requested.
2. **Begin** (writer, when the permit arrives and `F` is the oldest frozen memtable; flushes are serialized per collection). First complete and publish any in-flight group. Then capture a `FlushInput`: `Arc<MemtableData>` for `F`, `D_F` (the snapshot of `deletes[m]`), `D_S` (snapshots of every segment's deletion vector), the segment list, the schema, a new unit id `s`, and DV generations for each segment whose cardinality differs from its durable generation. Let `J` be `visible_seq_no` now (`J >= L`).
3. **Build** (maintenance pool). Iterate `F`'s slots in order, skipping slots set in `D_F`. Assign row ids densely. Build pk, sorted pk, filter, row meta, columns (for fields declared in the captured schema), dynamic blocks, SQ8, graph (if at least `graph_min_rows`), scalar indexes, and stats; the three index kinds from PR 12 on. Record `slot_to_row: Arc<[u32]>` (`u32::MAX` for skipped slots). If no slot is live, steps 3 and 4 produce nothing and the flush is a pure checkpoint: no segment is added.
4. **Write segment** (I/O pool). Stream sections to `segments/<s:08x>.seg` (`CreateNew`), then `sync_all`. `crash_point(FlushAfterSegmentSync)`.
5. **Write DV files** (I/O pool). For each segment with a new generation: write `segments/<id>.dv.<gen>` from `D_S` with `covered_seq_no = J`, `sync_all`. `crash_point(FlushAfterDvSync)`.
6. **Sync directory.** `sync_dir(segments/)`. `crash_point(FlushAfterSegmentsDirSync)`.
7. **Commit manifest** (writer, on `FlushDone`). Complete any in-flight group first. Build manifest `g + 1`: the current `Version`'s segments plus `s`, `checkpoint_seq_no = L`. For each segment that is present both now and in `D_S` and got a new generation in step 5, name that generation; for every other segment (including segments that a compaction created after step 2), keep its current durable generation. Run the atomic publish protocol.
8. **Install** (writer). New `Version`: drop `F` from `frozen`; add `SegmentHandle` for `s`; set `deletes[s]` to the reconciliation of `F`'s late deletions, `{ slot_to_row[x] : x in deletes[m] now, x not in D_F }`; remove `deletes[m]`. Add forwarding `m -> (s, slot_to_row)` and a rewrite task (for a pure checkpoint, no pk entry points into `m`, so there is neither). Publish. Enqueue for deletion the superseded DV generations, any DV file the job wrote for a segment that a compaction removed in the meantime, and the WAL files whose successor starts at or below `L + 1`. Append a `Checkpoint` frame with the next group.
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
    /// Tier 0 holds segments with fewer than base_rows live rows; tier t >= 1
    /// holds [base_rows * ratio^(t-1), base_rows * ratio^t).
    pub base_rows: u32,            // default 32_768
    pub tier_ratio: u32,           // default 4
    pub min_merge: usize,          // default 4
    pub max_merge: usize,          // default 10
    pub max_output_rows: u32,      // default 2_000_000, further capped by maintenance_memory
    pub max_output_bytes: u64,     // default 8 GiB of f32 vectors
    /// Engine-wide memory that running flush and compaction builds may hold.
    pub maintenance_memory: f32,   // default 0.2 of memory_limit
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
4. Size every job to fit its memory reservation. A graph build holds the output's f32 vectors and the graph under construction: `build_bytes = rows * (Σ dim * 4 + 32 * 4 * 1.1)` for the vector fields, plus the output's scalar columns. A 2M-row output at 768 dimensions needs about 6.4 GB, which does not fit beside a 9 GB hot set in a 16 GB budget. The scheduler grants a compaction permit only with a reservation of `build_bytes` from the engine-wide `maintenance_memory` pool, and the policy caps the output so that `build_bytes` is at most half the pool (two jobs can run). At `memory_limit = 16 GB` that caps 768-dimension outputs near 500k rows, so 10M rows settle into about 20 top-tier segments; at 32 GB, near 1M rows. `max_output_rows` is the upper bound when memory is plentiful.

Write amplification: a row is rewritten once per tier it climbs, about `log_4(max_output / 32,768)`, so roughly 2 to 3 compactions plus the flush. The engine counts bytes written by flush and by compaction and reports the ratio to bytes ingested, which the Phase 2 exit criterion measures.

### Compaction Steps

For inputs `I_1..I_n` (ascending unit ids):

1. **Begin** (writer, on permit, which carries the job's memory reservation). Complete and publish any in-flight group. Reserve the inputs. Capture `D0_i` (snapshot of `deletes[I_i]`) for each input, the current schema, and a new unit id `o`.
2. **Build** (maintenance pool, reads through the I/O pool with `CacheMode::Bypass`, so compaction does not evict hot data). For each input in order, for each row `r` not in `D0_i`: append the row to the output (dropping fields the schema dropped, filling null for fields it added) and set `map_i[r] = next output row`; set `map_i[r] = u32::MAX` for rows in `D0_i`. Retrain SQ8 and build the graph over the output. Record `pks` and `sources` in output row order.
3. **Write** (I/O pool). Stream `segments/<o:08x>.seg`, `sync_all`, `sync_dir(segments/)`. `crash_point(CompactionAfterOutputSync)`.
4. **Reconcile** (writer, on `CompactionDone`). Complete and publish any in-flight group. For each input, `delta_i = deletes[I_i] now AND NOT D0_i`. For each `r` in `delta_i`, set bit `map_i[r]` in `DV_o`. Every such `map_i[r]` is valid, because `r` was not in `D0_i`, so it was copied.
5. **Write output DV** (writer, I/O pool). If `DV_o` is not empty: allocate `d` from `next_dv_gen`, write `segments/<o:08x>.dv.<d:016x>` with `covered_seq_no = visible_seq_no`, `sync_all`, `sync_dir(segments/)`. `crash_point(CompactionAfterDvSync)`.
6. **Commit manifest.** Manifest `g + 1` = the currently durable manifest's segments minus inputs plus `o` (with `dv = Some(d)` when step 5 wrote one), its checkpoint unchanged. If a flush committed while the job ran, that durable manifest already names newer DV generations for the inputs and a newer checkpoint; both are carried forward (the inputs' generations are dropped with the inputs). Atomic publish protocol.
7. **Install.** New `Version`: remove inputs and their `deletes` entries, add `o` with `DV_o`. Add forwarding `I_i -> (o, map_i)` for every input and one rewrite task. Release reservations and the memory reservation. Publish. Remove the inputs from the writer's `live_files` (marking their `FileHandle`s obsolete) and enqueue for deletion the inputs' DV files named by the manifest this commit superseded.

The writer processes no write between steps 4 and 7, so a compaction commit stalls the write path for one DV-file write and one manifest publish, about five fsyncs. That is a deliberate trade: letting writes run during the publish would require a second reconciliation pass at install and serializing it against flush commits. A job that fails in steps 2, 3, 5, or 6 releases its reservations and burns its ids ([Id Allocation and Failed Commits](#id-allocation-and-failed-commits)).

### DV Reconciliation at Commit

This is the step that makes concurrent writes safe, so it is spelled out.

While the job runs (between steps 1 and 4), the writer keeps processing writes. A write that deletes or supersedes a row `r` of input `I_i` finds `RowAddr { I_i, r }` in the pk index (the inputs are still in every `Version`) and sets `r` in `deletes[I_i]`. The job never sees those bits, so the output contains a copy of `r` at `map_i[r]`.

At commit the writer transfers exactly those bits. Claim: after step 7, for every output row `x` copied from `(I_i, r)`, `x` is set in `DV_o` if and only if `r` is set in `deletes[I_i]` at commit time. Proof: `r` was copied, so `r` is not in `D0_i`. If `r` is set now, it is in `delta_i`, so `x` is set. Conversely, `DV_o` receives bits only from the `delta_i` sets, through the injective maps. Therefore the live row set, as a set of logical rows, is identical immediately before and after the swap, so I4 and I5 hold across the commit and no reader sees a resurrected or lost row. The writer does steps 4 to 7 without processing any write in between, so no deletion can fall between reconciliation and publication.

After the commit, a write that resolves a key to `(I_i, r)` (a pk-index entry not yet rewritten) follows the forwarding table to `(o, map_i[r])`, so its bit lands in `DV_o`. The rewrite task later points the entry directly at `(o, x)`, but only if the entry still equals the source address, so a key that was deleted or re-inserted during or after the compaction is never clobbered.

Worked cases, each a required deterministic-interleaving test:

- **Upsert moves a key during the job.** `k` lives at `(I_1, r)`. An upsert of `k` marks `r` in `deletes[I_1]` and appends a memtable slot; a flush may even move that slot into a new segment `s` before the compaction commits. At commit, `r` is in `delta_1`, so `map_1[r]` is set in `DV_o`: the stale copy is dead and `k`'s live row is the memtable or `s` row. The rewrite task finds `k`'s raw entry pointing at the memtable or `s`, not at `(I_1, r)`, and leaves it.
- **Upsert, then delete, during the job.** Both operations resolve through the pk index: the upsert marks `(I_1, r)`, the delete marks the memtable slot. `DV_o` gets `map_1[r]`; the key is absent everywhere, as in the logical state.
- **A row compacted twice.** Compaction 1 turns `(I, r)` into `(o1, x)`, and compaction 2 takes `o1` as input before rewrite task 1 finished (inputs are reserved by one job at a time, but a committed output is immediately eligible). A delete of `k` during compaction 2 resolves `(I, r) -> (o1, x)` through `forwards[I]`, marks `x` in `deletes[o1]`, and compaction 2's reconciliation transfers it to `(o2, map[x])`. After compaction 2 commits, the chain `I -> o1 -> o2` resolves any remaining stale entry, and FIFO rewrite order ([Primary-Key Index](#primary-key-index)) retires the chain safely.
- **A delete of a row already in `D0_i`.** It cannot reach the input: the pk index no longer points at a deleted row, so the delete resolves to the key's current row (or none).

Durability of reconciled bits (I9): a reconciled deletion may have a sequence number at or below the current checkpoint `C`, because a flush can commit while the compaction runs and move the checkpoint past it; the WAL files holding it may already be deleted, and the input's DV file that recorded it is dropped with the input. That is why step 5 writes the output's DV file before the manifest is published, and why a DV file is required whenever `DV_o` is not empty.

### Compaction Crash Analysis

| Crash after | Durable state | Recovery |
| --- | --- | --- |
| 1 begin, 2 build | nothing new | manifest `g`; the job is forgotten and re-planned |
| 3 output sync | `o.seg` unreferenced | orphan cleanup deletes it |
| 4 reconcile | same | same |
| 5 output DV sync | `o.seg`, `o.dv.<d>` unreferenced | orphan cleanup deletes both |
| 6 publish, before its step 5 | `CURRENT` old or new | old: as above plus deleting manifest `g + 1`; new: load `g + 1` with `o` and `o.dv.<d>`, inputs are orphans and deleted |
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

A removal is not durable until its directory is synced, so after a crash a removed file may reappear. Neither outcome matters, because nothing references an obsolete file. Orphan cleanup runs during recovery, after the durability barrier, after `CURRENT` and its manifest `M` are loaded, and before the writer starts (so before any unit id, DV generation, or manifest generation can be reused). It deletes relative to `M`, so it is only safe because the barrier has made `M`'s selection by `CURRENT` durable; otherwise a `CURRENT` rename seen only in the page cache could lead it to delete the segments of the manifest that is actually durable:

1. `segments/`: remove every `.seg` whose unit is not in `M`, every `.dv.<gen>` whose `(unit, gen)` is not named by `M`, and every `.tmp`.
2. `manifests/`: remove every generation other than `M.generation` and the newest generation below it (generations can have gaps after a failed publish), including every generation above `M.generation`.
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
             - maintenance_memory        (compaction.maintenance_memory * memory_limit, for job builds)
```

At `memory_limit = 16 GB` with 10M int64 keys this leaves about 9 GB, which is the D1 hot set (SQ8 codes plus layer-0 graph) with little room for scalar indexes; 24 GB or more leaves headroom. The warm-up stop at 90 percent and the per-class floors keep that case working, with cold scalar sections read on demand.

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

### Implementation Notes (PR 9)

PR 9 builds the cache as a standalone module, `logpose-storage::cache`, and routes every lazy segment v2 load through it. The cache itself does not depend on PR 3's types: the executor that runs misses is injected through the `LoadExecutor` trait. The engine owns one cache, sized by `EngineConfig::cache` and reachable as `Engine::cache()`, next to `runtime` on `EngineCore`, and `IoPool` implements `LoadExecutor`. A cache job goes through `IoPool::execute`, which does not wait for a queue slot, so submitting a miss never blocks, and a job dropped by a pool that has shut down fails its load with `LoadAborted`. Where this section left a detail open, or where the sketch did not survive contact with the code, the implementation does the following.

- **Layout.** `cache/` is a directory: `mod.rs` (`BufferCache`, eviction, budget), `bytes.rs` (`AlignedBytes`), `key.rs` (`ArtifactClass`, `FileId`, `CacheKey`), `flight.rs` (single flight, `LoadExecutor`, `Fetch`), `report.rs` (`Fetched`, `FetchReport`, `PinSet`, `CacheStats`), and `warm.rs`. The segment side is `segment_v2/unit.rs`.
- **Keys.** `CacheKey { file: FileId, section: u32, unit: CacheUnit }` with `CacheUnit::{Section, Index, Page(u32)}` replaces `page: u32` with the `0` and `u32::MAX` conventions. A whole-section read of a `VectorF32` section would otherwise share page 0's key, and the `DynamicJson` block index needs a unit of its own too. `section` is `u32` because the table's `section_count` is. `FileId` comes from a process-wide counter and is never reused.
- **Load units.** A `SegmentUnit` is a whole section, a `VectorF32` prefix or page, or a `DynamicJson` block index or block. Its loader reads the unit and checks its CRC (section CRC, `prefix_crc`, page CRC, `index_crc`, or block CRC) before the bytes can enter the cache, so a hit decodes without checking again. Pages are 8 KiB at 128 dimensions, 6 KiB at 768, and one row (over 8 KiB) above 2048. `SchemaSnapshot` (parsed at open), `RowMeta` (transient), unknown kinds, and whole `VectorF32` and `DynamicJson` sections (read only by scans, which use their pages and blocks elsewhere) are never inserted.
- **Two entry points, one flight.** `get_or_load_blocking(key, class, mode, load)` runs the loader on the calling thread, for code already on an I/O thread (the segment reader's accessors, warm-up loaders, jobs). `get_or_load(key, class, mode, executor, load) -> Fetch` hands the loader to a `LoadExecutor` and returns a future. Both share one `Flight` per key, which can be waited on with a condition variable or with wakers, so no `futures` dependency is needed. The lookup, the registration, and the hand-off all happen before `get_or_load` returns, so dropping the future cancels nothing another caller waits on. The loader is stored in the flight: a blocking caller that joins a load still sitting in the executor's queue takes it and runs it, so an I/O thread never waits for a job queued behind it on its own pool.
- **Failures.** A failed load (I/O, CRC, structure) is never inserted; every waiter gets the error and the next access loads again. `SegmentError` is now `Clone` (`Io` holds an `Arc<io::Error>`) so one failure reaches every waiter typed. A loader that panics, or a job its executor drops (a pool that has shut down), completes the flight with the new `SegmentError::LoadAborted`, so no waiter hangs. The typed CRC error is `SegmentError::Checksum { region }`, whose region names the section index and page or block; `LogPoseError::SegmentCorrupt { unit, section }` does not exist yet, and PR 10 maps to it when it adds the unit id.
- **Accounting.** An entry is charged its length rounded up to 8 bytes plus `ENTRY_OVERHEAD` (128 bytes for the map slot, entry, and ring slot). `CacheStats` reports usage per class and the hit, miss, wait, failure, eviction, invalidation, and overcommit counters.
- **CLOCK sweep.** A sweep visits each entry of the victim class at most twice. On the first lap a referenced entry gets its second chance; on the second lap the flag is ignored, so hits racing with the sweep cannot keep an unpinned entry resident over budget. Eviction runs only on insert, so `trim()` evicts after many pins are released and `set_budget` evicts at once. `overcommits` counts passes that ended over budget with only pinned entries left, and `overcommit_bytes` is the excess after the last pass. Floors hold against pins too: the fallback to the lowest-priority non-empty class applies only when no class is over its floor (floors that add up to more than the budget), so when every class over its floor is pinned the pass ends overcommitted instead of evicting a class below its floor.
- **Pins.** A pin is a held `Arc<AlignedBytes>`, as designed. A flight's result counts as a pin until its callers take it, so a unit cannot be evicted between its insert and its hand-off. `VectorPrefix` no longer copies the page CRCs into a `Vec<u32>`: it keeps the verified prefix bytes and reads each page's CRC from them, so "page loads read their CRC from the pinned prefix" holds literally and a `VectorHandle` pins its prefix while it lives.
- **Bypass and verify.** `CacheMode::Bypass` uses a resident entry or joins a load in flight, but never inserts and never marks an entry referenced. `SegmentReader::read_rows` (a full scan) uses it. `SegmentReader::verify` ignores the cache and reads the file, because it checks the file, not a cached copy.
- **Invalidation.** `FileHandle` does not exist yet, so a reader attached with `SegmentReader::with_cache` holds the registration (a `FileId` and the cache) and calls `invalidate_file` when dropped, which is what `FileHandle::drop` does in the sketch; PR 6 or 10 can move the registration into `FileHandle`. `invalidate_file` also detaches loads in flight for the file: their waiters still get the bytes, and nothing is inserted.
- **Warm-up.** `BufferCache::warm_up(items, executor)` runs items class by class in priority order, with at most two loads in flight. A class stops at the first item that would take the cache past 90 percent of the budget, counting loads still in flight; the next class gets what room is left. Resident items are skipped and failed items are counted. `SegmentReader::warm_up_items()` lists a segment's whole `GraphAndCodes`, `PkIndex`, and `ScalarIndex` sections; the engine concatenates segments largest first. PR 3's `IoPool` has no low-priority queue, so the two-in-flight bound is what keeps warm-up from crowding out foreground misses until one exists.
- **Reports.** `FetchReport` gains `waits` (units another caller was loading). Each access returns a `Fetched` (`Hit`, `Waited`, or `Loaded { bytes, micros }`), which `FetchReport::record` counts per class. The caller that started a load reports the miss even when a blocking caller ran it.
- **Budget.** `BudgetInputs::cache_budget()` implements the formula above (the query reserve is 10 percent of `memory_limit`). Recomputing it as the pk index grows is the engine's job and lands with PR 10.
- **Decoding.** Hits save the read and the CRC check, not the decode: `pk_column()`, `stats()`, and the other accessors still decode the cached bytes on every call. Zero-copy views over pinned bytes arrive with the read path in PR 12. `AlignedBytes` is 8-byte aligned, so `bytemuck::try_cast_slice` works on every array in a cached unit today.
- **Sources.** `VfsSource(Arc<dyn VfsFile>)` adapts an engine file to `SectionSource`. Async fetches need a `Clone` source, which `VfsSource`, `MemorySource`, and any `Arc<S>` are.

## Recovery on Open

`Engine::open(config, vfs, resolver)`:

1. `try_lock_exclusive(<root>/LOCK)`; fail with `StorageRootLocked`.
2. Build the runtime (pools), the cache, the GC queue, and the scheduler.
3. Load catalog files (databases, principals, policies). Remove collection directories without `descriptor.json` and `*.dropped` directories.
4. For each collection, in parallel on the I/O pool with at most `io_threads` collections at once, run `open_collection`. A collection that fails is registered in the `Failed` state with its typed error; every call on it returns that error; the engine still opens.
5. Register the handles in `collections`, start the writers, start the token reaper, enqueue warm-up.

`open_collection(dir)`:

1. **Durability barrier.** `sync_dir` the collection directory, `manifests/`, `segments/`, and `wal/`, and `sync_all` every file in `wal/`. Recovery then reasons only about state that is on disk. Without the barrier, an in-process reopen after a poisoned publish (a `CURRENT` rename that succeeded but whose directory sync failed) or after a WAL rollback failure could act on page-cache state: orphan cleanup would delete the segments of the manifest that is actually durable, or replay would publish frames that a power loss then takes back (I14). If any sync fails, the open fails.
2. Read `descriptor.json` and `CURRENT`; load manifest `M` and verify it. A version-1 layout (`maintenance.json`, `active.wal`, or a JSON manifest) fails with `LogPoseError::UnsupportedFormat`.
3. Orphan cleanup, as in [GC Crash Safety and Orphan Cleanup](#gc-crash-safety-and-orphan-cleanup).
4. Open a `SegmentHandle` for every segment in `M` (header, footer, table, schema snapshot; about three small reads each).
5. Load each named DV file, verify CRC, `unit_id`, `row_count`, and generation; build `DeletionMap`.
6. Rebuild the pk index from segment pk and row-meta sections minus deletion vectors, as in [Primary-Key Index](#primary-key-index).
7. List `wal/`, sort by first sequence number, skip files entirely at or below `M.checkpoint_seq_no`, and run tail repair on the last file.
8. Replay frames in order. Skip frames with `last_seq_no <= checkpoint`; reject a frame that straddles it. Check contiguity from `checkpoint + 1`. For each `WriteBatch`, call the same `apply` the live writer uses, into a fresh active memtable (unit id from `M.next_unit_id`); for `SchemaChange`, follow [Replay Across Schema Versions](#replay-across-schema-versions); for `Checkpoint`, cross-check. Replay is CPU-bound and runs on the maintenance pool.
9. Set `next_seq_no = max(checkpoint, last replayed) + 1`, `next_unit_id` past every allocated id, `next_dv_gen` from `M`, `next_manifest_gen = M.generation + 1`, the group counter to one past the last frame's `group_no`, `durable` from `M`, and `live_files` from the segment handles.
10. Open the WAL writer on the last file (after repair) in append mode with `synced_len` = its length, or create a new file named `next_seq_no` (then `sync_dir(wal/)`) if there is none.
11. Build and publish `Version` 1. If the replayed memtable already exceeds a flush trigger, freeze it right away.
12. Run `Version::check_invariants` when `strict_invariants` is on (tests): unique live keys, pk index agreement, DV bits below row counts, counter exactness.

Recovery is idempotent (I11): the only file changes before the writer starts are the barrier's syncs (step 1, no content change), orphan removal (step 3), tail truncation (step 7), and creating an empty WAL file (step 10). Each removes or adds only bytes that no durable state references, and each runs after the barrier, so rerunning recovery after a crash at any point converges to the same state.

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
    pub fn schema(&self) -> &Arc<CollectionSchema>;
    pub fn visible_seq_no(&self) -> SeqNo;
    pub fn token(&self) -> Option<&SnapshotToken>;
    pub fn counters(&self) -> VersionCounters;
    /// Segments ascending by UnitId, then frozen memtables oldest first, then
    /// the active memtable. Correctness never depends on the order (I5).
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
   1. Compile the filter to a bitmap `B` using `ScalarIndexOps` or a column scan; `NOT p` becomes `live AND NOT B_p`. Then `B := B AND NOT deleted`. Without a filter, `B = live()`. Field names resolve against `ReadView::schema()`: a declared name becomes its `FieldId` (a unit with no section or column for it yields nulls), and an undeclared name, when dynamic fields are on, becomes an `$extra` path subject to [shadowing](#dynamic-field-shadowing).
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
5. **Pools, GC, tokens.** I/O moves to the `IoPool`, search CPU to rayon. Version-refcount GC and manifest v2 land. Snapshot tokens replace historical `Snapshot` reads. Until segment v2 exists, `ManifestSegment` entries describe v1 segment files with the v2-only fields empty (`dv: None`, no `vectors` or `zones`, `footer_crc = 0` meaning unchecked); PR 10 removes that allowance.

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
| `FailSync(n)` | batches acked `NotApplied` are absent; `Unknown` ones may be either | collection is read-only; nothing new becomes visible; reopen passes I8 |
| `AlterSchema(add, drop, or rename)` | update model schema | reads return null for new fields, hide dropped fields, and apply `$extra` shadowing |

After every action the harness runs `Version::check_invariants` (I5, I10, I13). The seed and action trace are printed on failure, as today.

### Crash-Recovery Equivalence

Every test that crashes uses `FaultVfs`:

- **Exhaustive crash points.** Run a scenario once cleanly and count mutating ops `T`. For each `k` in `0..=T` and each `TearMode`, rerun with `crash_after_ops = k`, reopen, and check I8: the recovered state equals the model after some prefix of batches that includes every acknowledged batch and contains no partial batch. Scenarios: a single group commit, a flush, a compaction with concurrent deletes, a flush during a compaction, a checkpoint-only flush, a schema change followed by a flush, a failed manifest publish followed by a retry, and GC after a pinned token is released.
- **Visibility never runs ahead of durability (I14).** Readers record every state they observe; after each crash, every observed state must be a prefix of the recovered one.
- **Failure then in-process reopen.** Fail a WAL sync, a `CURRENT` rename, and a manifest directory sync; reopen on the same `FaultVfs` without a crash, then crash, then reopen again. The durability barrier must make both reopens agree.
- **WAL group classification.** Corrupt the `GROUP_END` frame of an acknowledged group that is followed by exactly one complete group; the open must fail with `WalCorrupt`, not truncate.
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
| 3 | `Engine` shell: collection map, root lock, `IoPool` and rayon pools, `CollectionHandle` with `ArcSwap<Version>` over v1 state; delete directory scans and global lock maps | 2 | 4, 7, 8 |
| 4 | WAL v2 frame layer in `logpose-wal`: header with `group_no`, reader with tail repair and group classification, writer with rotation and failed-group rollback; payloads are opaque bytes here; standalone tests on `FaultVfs` | 2 | 3, 7, 8, 9 |
| 5 | Writer task and group commit on WAL v2; `apply` shared with replay; `AlterSchema` in the stream and replay across schema versions; poisoning with rollback and outcome-typed errors; delete v1 WAL | 3, 4, 7 | 8, 9 |
| 6 | Manifest v2 (with the transitional v1 segment entries) and `CURRENT` protocol, id allocation and failed-commit handling, version-refcount GC, orphan cleanup, the recovery durability barrier, snapshot tokens and reaper with the pinned-memory limit; replace historical snapshot reads | 5 | 8, 9 |
| 7 | Schema integration: `CollectionSchema`, `Record`, `FieldId` from the schema PR into descriptor, validation, and `Record` to `RowImage` conversion; the binary value codec in `logpose-types`; WAL payload types (`WalPayload`, `RowOp`, `WirePk`, `ValueBytes`) and their postcard codec with golden bytes and a `CollectionSchema` round-trip test; legacy mapping; move predicate AST to `FilterExpr` in `logpose-types` | schema PR | 1, 2, 3, 4 |
| 8 | Segment v2 writer and reader as a standalone module: layout, all storage-owned encodings, a `SegmentBuilder` that accepts opaque index sections, CRC and corruption tests, golden file | 2, 7 | 3 to 6, 9 |
| 9 | Buffer cache v1: classes, CLOCK, single-flight, pins, `FetchReport`, warm-up | 3 | 4 to 8 |
| 10 | Memtable v2 with `CowBitmap` postings, writer-private pk index, deletion vectors and DV files, flush to segment v2 with storage-owned sections only (pk, row meta, f32 vectors, columns, dynamic, stats), recovery with pk rebuild; the legacy adapter serves ANN by exact scan; delete resolve-latest paths and v1 segments and sidecars | 5, 6, 8, 9 | none |
| 11 | Compaction v2: size-tiered policy with the maintenance-memory reservation, reconciliation, forwarding and FIFO incremental pk rewrite, scheduler priorities; delete `compact_state` | 10 | 12 |
| 12 | `CollectionReader`, `ReadView`, `UnitView`; index sections (SQ8, HNSW, scalar inverted and sorted) added to the shared `SegmentBuilder`, so flush and compaction write them; query crate on the new interfaces for search, get, count, scroll, order by; `RowSetResolver` and the filter write requests; delete old trait read methods | 10, index PRs | 11 |
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

Critical path: 1, 2, 3, 5, 6, 10, 11 or 12, 14. PRs 4, 7, 8, and 9 run beside it and should start as soon as their inputs land; 7 must land before 5 because WAL v2 payloads are `RowImage`s built from the new schema types and encoded with the codec that 7 adds. PR 4 takes opaque payloads so it does not wait for 7.

Why the index sections move to PR 12: flush (PR 10) and compaction (PR 11) would otherwise need the HNSW, SQ8, and scalar-index PRs, which do not exist yet as branches, and the engine critical path would wait on them. Until PR 12, segments carry no index sections, which every reader already handles (a segment below `graph_min_rows` has none either). PRs 11 and 12 both touch segment building, but 11 only calls `SegmentBuilder` and 12 only extends it, so they merge cleanly in either order.

## Deviations From the Plan

1. **`Vfs` in its own crate.** The plan puts the `Vfs` trait in `logpose-storage`, but `logpose-wal` must use it and `logpose-storage` depends on `logpose-wal`, which would be a cycle. A small `logpose-vfs` crate below both fixes it without merging the WAL into storage.
2. **The primary-key index is not published through `Version`.** D3 describes an in-memory `pk -> (segment, row)` map rebuilt on open, which this design keeps, but as writer-private state. Readers resolve keys through each unit's immutable pk sections plus deletion vectors. Publishing the map would cost either a persistent map (two to three times the memory at 10M keys) or copy-on-write shards (megabytes copied per group under concurrent reads), and no reader needs it.
3. **Historical `Snapshot { manifest_generation, visible_seq_no }` reads are dropped.** The plan lists the snapshot type as worth keeping. With deletion vectors, a `Version` cannot be reconstructed for an arbitrary past sequence number, so repeatable reads use snapshot tokens (D7), and read barriers compare only `visible_seq_no` (the manifest generation is a physical detail that compaction changes without any logical change).
4. **Two rayon pools.** D8 names one rayon pool for CPU search. Index builds for multi-million-row compactions would starve queries on a shared pool, so maintenance CPU work gets its own smaller pool.
5. **Compaction writes the output's DV file at commit.** D3 says deletion vectors are written at checkpoints and the WAL covers them in between. That is not enough for compaction: a deletion reconciled onto the output may already be below the checkpoint (its WAL file deleted and its input's DV file dropped), so the output's DV file must be written before the compaction's manifest is published.
6. **Delete-by-filter and update-by-filter are atomic only up to one frame.** D7 makes every batch atomic. A filter matching more rows than fit in `MAX_FRAME_PAYLOAD` (about 6M `int64` keys for a delete, about 21,000 rows for an update at 768 dimensions, since updates log full row images) commits in several atomic chunks over a key set resolved once, reported in the ack, rather than requiring unbounded frames or an in-memory undo mechanism. Readers may observe a prefix of the chunks. Justified: D7's atomicity is about client batches, which stay atomic, and a filter write of millions of rows is rare enough that an explicit chunk count in the ack is an acceptable contract.
7. **The segment section table is at the end of the file.** Phase 2's layout sketch places the section table in the header. Compaction output is streamed, so section lengths are unknown when the header is written; the header keeps what is known up front (row count, schema hash, sequence range) and the footer points at the table.
8. **Memtable indexes are persistent maps of two-tier bitmaps.** Phase 3 task 3 sketches `BTreeMap<Value, RoaringBitmap>`. A `Version` must hold an O(1) snapshot of the memtable while the writer keeps appending, so the map is `imbl::OrdMap` and each posting a `CowBitmap`; a `BTreeMap` would have to be cloned per published `Version`.
9. **Snapshot-token expiry is sliding and can come early.** D7 says tokens expire after a TTL. Here every use extends the TTL (so an active scroll never loses its snapshot), and the reaper may expire the oldest tokens early when pinned retired memtables exceed `token_memory_limit`. Both keep D7's intent (bounded retention) while making long scrolls usable and memory bounded.
10. **Compaction output size is bounded by memory, not only by `max_output_rows`.** The plan assumes large segments; at 16 GB and 768 dimensions the maintenance-memory reservation caps outputs near 500k rows, so 10M rows live in about 20 top-tier segments instead of 5. Search fans out to more graphs, which the Phase 4 benchmark must measure; the alternative, building graphs over rows that do not fit in memory, would violate D1's budget.

## Review Log

### Changes From Design Review

Principal storage-engine review of this design, before implementation. Each item names the defect and the fix now in the text above.

1. **WAL tail repair could truncate acknowledged groups** (high; [Tail Repair](#tail-repair)). The torn-versus-corrupt rule looked only at `GROUP_END` flags. If the damaged frame was the `GROUP_END` frame of an acknowledged group and exactly one complete group followed, the later group's single `GROUP_END` made the damage look like a torn tail, and repair truncated two acknowledged groups. Frames now carry `group_no`, and the tail is torn only if every valid frame after the damage belongs to the damaged frame's own group.
2. **WAL payloads could not be decoded, and the assumed codec did not exist** (high; [Record Types](#record-types), [Encoding Choice](#encoding-choice), [Binary Value Codec](#binary-value-codec)). The schema PR's `PrimaryKey` is `#[serde(untagged)]` and `Value::Json`/`Record::extra` hold `serde_json::Value`; postcard cannot deserialize either. The schema PR has no binary `Value` codec and no `Value::Object`. The WAL now uses `WirePk` and `ValueBytes`, and this document specifies the binary value codec, owned by PR 7.
3. **Schema changes had no protocol** (high; new [Schema Changes](#schema-changes)). `AlterSchema` and `SchemaChange` existed as types only. Added: in-stream alter with one sequence number and no freeze, `FieldId`-keyed memtable columns with `first_slot`, replay rules when the manifest's schema is newer than the frames, and a deterministic `$extra` shadowing rule that does not depend on compaction timing. Also aligned types with the schema PR: `CollectionSchema`, `schema_version: u64` in WAL, segment header, and manifest, and a `u32` `field_id` in the section table (`FieldId` is `u32` and never reused, so `u16` would overflow).
4. **Failed WAL fsync reported "failed" for batches that recovery could replay** (high; [Group Commit](#group-commit)). Acks said `WalWriteFailed` but the frames might survive. Now the writer rolls the file back to `synced_len` (as the Phase 0 atomic-batch PR does), errors carry `outcome: NotApplied | Unknown`, the poisoned writer publishes and commits nothing further, and a failed rollback refuses in-process reopen.
5. **Recovery acted on non-durable directory state** (high; [Recovery on Open](#recovery-on-open), [GC Crash Safety and Orphan Cleanup](#gc-crash-safety-and-orphan-cleanup)). After a poisoned publish (rename done, directory sync failed) an in-process reopen could read a `CURRENT` that exists only in the page cache, and orphan cleanup would then delete the segments of the manifest that is actually durable. Recovery now starts with a durability barrier that syncs every directory and WAL file it will reason about.
6. **Update-by-filter chunking could loop forever** (high; [Record Types](#record-types)). Each chunk re-resolved the filter against the state after the previous chunk, so a patch that keeps rows matching re-matched them indefinitely. The filter is now resolved once to a fixed key set, and chunks commit back to back with no interleaved requests.
7. **Retried jobs collided with their own files** (medium; new [Id Allocation and Failed Commits](#id-allocation-and-failed-commits)). A publish failure in steps 1 to 3 "retried with backoff" but reused the manifest generation, unit id, and per-segment DV generation, so `CreateNew` would fail until restart. Ids are now burned on failure, DV generations come from a per-collection `u64` counter in the manifest (file names use 16 hex digits), abandoned files are collected immediately, and a compaction commit builds on the latest durable manifest rather than the one at Begin.
8. **Visibility versus durability was implicit** (medium; [Invariants](#invariants), [Publication Protocol](#publication-protocol)). The design already published only after fsync, but nothing stated it, and the second half of I9 (DV bits must come only from durable operations) depended on it silently. Added I14, the explicit position (a reader never sees group `n+1` before group `n` is durable), the rule that copy-on-write keys on sharing (`Arc::make_mut`), not on a published flag, and clarified that `Tick` does not drain the pipeline.
9. **Deletion-vector copy-on-write cost grew with segment size** (medium; [DV Structure](#dv-structure)). Folding at `base.len() / 8` let `recent` reach about 100 KB on a large segment, copied once per group per touched segment; a random-upsert stream touches most segments per group. `recent` is now capped at 4096 entries, about 8 KB per touched unit per group.
10. **Memtable index postings copied whole bitmaps per group** (medium; [Mutable Scalar Indexes](#mutable-scalar-indexes)). The "few KB per touched key" claim fails for low-cardinality fields (up to 128 KB per key per group at 1M slots). Postings now use the same two-tier `CowBitmap`.
11. **Compaction build memory was unbudgeted** (medium; [Size-Tiered Policy](#size-tiered-policy), [Budget and Classes](#budget-and-classes)). A 2M-row, 768-dimension output needs about 6.4 GB of vectors and graph during the build, beside a 9 GB hot set in a 16 GB budget. Added a `maintenance_memory` pool that permits reserve from and that caps output size, and subtracted it from the cache budget. Listed as deviation 10.
12. **Pinned tokens held retired memtables outside every budget** (medium; [Snapshot Tokens](#snapshot-tokens)). Added `pinned_retired_bytes` accounting and early expiry past `token_memory_limit`, and stated that TTL expiry never affects a request already running (its `ReadView` holds the `Arc<Version>`).
13. **PR breakdown had a cycle and hidden dependencies** (medium; [PR Breakdown](#pr-breakdown)). PR 3 listed PR 9 as parallel although 9 depends on 3. PRs 10 and 11 built SQ8, graph, and scalar-index sections without depending on the index PRs, whose branches do not exist yet. PR 6 introduced manifest v2 before segment v2 existed. PR 4 needed PR 7's codec. Fixed: index sections move to PR 12, manifest v2 carries transitional v1 entries until PR 10, PR 4 takes opaque payloads, and PR 7 owns the codec and payload types.
14. **Smaller corrections.** FIFO rewrite order is now stated as a correctness requirement, with the chain cases spelled out, and a `u32::MAX` forward is an invariant violation (PK index). Replay rejects frames that straddle the checkpoint, and a file's first data frame must match its name (WAL). Tier 0 is defined for segments below `base_rows`. `units()` order is described correctly. Validation rejects non-finite vector components and zero-norm cosine vectors. The `VectorF32` page-CRC array gets its own CRC and cache unit. Orphan cleanup tolerates gaps in manifest generations. Large `Version` and memtable drops leave tokio workers. Worked DV-reconciliation cases for moved keys and twice-compacted rows are listed as required tests. Deviations 6 and 7 were sharpened and 8 to 10 added.

Verified and left unchanged: ack after publish gives I1; flush with `checkpoint_seq_no = L` and DV snapshots at `J >= L` gives I9, including compactions that commit between flush Begin and commit; the reconciliation proof (the maps are injective and the writer runs steps 4 to 7 with no write in between); I7 for token-pinned segments; blind-write replay over early DV bits.

### Decisions on Review Questions

These two questions from the design review are decided. The implementation PRs follow them.

- **fsync failure without successful rollback: fence by boot id, then require a reboot or an operator acknowledgement.** When the rollback after a failed WAL fsync also fails, the writer writes `wal/FSYNC_FAILED` (best effort, with a directory fsync) containing the current boot id from `/proc/sys/kernel/random/boot_id` (on other platforms, the process start time) and the affected sequence range. Then the handle moves to `Failed { rollback_failed: true }`. If the marker itself cannot be written, the engine stops the process, because it can no longer prove that a later open will notice the hazard. On open, recovery checks for the marker before the durability barrier:
  - marker boot id equals the current boot id: refuse to open the collection with `FsyncFailedSameBoot`. The page cache may still hold frames that never reached the device.
  - marker boot id differs: a reboot cleared the page cache, so the on-disk bytes are the truth. Recover normally, then delete the marker durably.
  - an operator can clear the marker with an explicit admin call after checking the device. The call is logged, and the next open recovers normally.

  `O_DIRECT` for the WAL tail was rejected. It needs aligned buffers and aligned offsets for every append, costs about one extra device write per group, and still does not protect the other files. The multi-node answer to a failing disk is Phase 7 replication, not heroics on a single node.
- **Shadowed `$extra` keys: shadow retired names permanently.** `CollectionSchema` gains `retired_names: BTreeSet<String>`. `drop_field` and `rename_field` add the old name to it. `add_field` with a retired name removes it from the set, because the name is declared again. The read rule in [Dynamic Field Shadowing](#dynamic-field-shadowing) becomes: a `$extra` key is visible only if the reading schema neither declares nor retires that name. Results are therefore stable across drops and renames and never depend on compaction timing. The cost is that a dynamic key that was once declared stays hidden in old rows. That is the least surprising behavior for a database: a dropped column does not come back. The addition to `logpose-types` lands with PR 7 (schema integration).

### Open Questions

- **Index PR interfaces.** `origin/claude/p4-hnsw-v2`, `origin/claude/p4-kernels-sq8`, and `origin/claude/p3-scalar-index` do not exist yet, so the `Sq8Codes`, `HnswGraph`, and scalar-index `write_to`/`view` contracts here are unverified. PR 12 is the only PR blocked on them.
- **Segment fan-out at 16 GB.** The memory cap yields about 20 top-tier segments at 10M by 768. Whether per-segment graph search at that fan-out meets the Phase 4 QPS target, or whether graph builds should instead stream vectors from disk to allow larger outputs, needs the benchmark.
- **Writer stall at commit.** Flush and compaction commits block writes for about five fsyncs. Measure p99 write latency under a steady flush rate before deciding whether to overlap the manifest publish with writes (which requires a second reconciliation pass).
