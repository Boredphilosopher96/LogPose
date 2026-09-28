//! Storage engine abstractions.
//!
//! [`Engine`] owns one storage root: the exclusive root lock, the resident collection map, the
//! thread pools, and each collection's [`CollectionHandle`], whose published [`Version`] readers
//! pin without locks. [`StorageEngine`] is the storage contract and [`LocalStorageEngine`]
//! implements it by delegating to an `Engine`. The implementation is split by concern:
//!
//! - `engine`: `Engine`, the collection map, the storage-root lock, and create and drop.
//! - `runtime`: the `IoPool`, the `query` and `maintenance` rayon pools, and `run_cpu`.
//! - `handle`, `version`: `CollectionHandle`, publication, and the immutable `Version`.
//! - `writer`: each collection's single writer task, group commit on the WAL, the `apply`
//!   function shared with replay, the writer-private primary-key index, and poisoning after a
//!   failed WAL write.
//! - `memtable`: the active and frozen memtables (vector arenas, typed columns, and scalar
//!   postings over append-only slots).
//! - `dv`: deletion vectors, their two-tier copy-on-write bitmaps, and DV files.
//! - `segment`: the engine's handle on one segment v2 file, and writing one.
//! - `legacy_view`: the v1 record view of v2 rows and v1 batches, for the `StorageEngine`
//!   adapter.
//! - `inspect`: collection statistics (O(units), from counters and zone maps) and `inspect`
//!   reports.
//! - `storage_engine`: the trait and its public request, inspection and blob-store types.
//! - `local_engine`: the trait implementation over `Engine`.
//! - `collections`, `catalog`: collection, database and principal descriptor files.
//! - `paths`: the on-disk layout.
//! - `recovery`: the durability barrier, loading the manifest `CURRENT` names, orphan cleanup,
//!   and replaying the WAL above its checkpoint.
//! - `state`: resolving a read to the current or a token-pinned `Version`.
//! - `read`: the read path's storage side: `CollectionReader`, `ReadView` (one pinned
//!   `Version`), `UnitView`, the staged fetch of segment sections, and `RowSetResolver`.
//! - `manifest`: manifest v2 and the `CURRENT` publish protocol.
//! - `gc`: version-refcounted segment files, the file-removal queue, and orphan cleanup.
//! - `tokens`, `clock`: snapshot tokens, their reaper, and the injectable clock.
//! - `segment_v2`: the segment file format, builder, and reader.
//! - `cache`: the buffer cache of segment section bytes that segment readers load through.
//! - `flush`, `compaction`, `maintenance`, `scheduler`: maintenance jobs, the size-tiered
//!   compaction policy, flush triggers, and the engine-wide scheduler of job permits.
//! - `durable_fs`, `fs_util`, `root_lock`, `error`: filesystem and error helpers.

#[cfg(test)]
use logpose_query as _;
#[cfg(test)]
use rand as _;

pub mod cache;
mod catalog;
mod clock;
mod collections;
mod compaction;
mod durable_fs;
mod dv;
mod engine;
mod error;
mod flush;
mod fs_util;
mod gc;
mod handle;
mod inspect;
mod legacy_view;
mod local_engine;
mod maintenance;
mod manifest;
mod memtable;
mod paths;
pub mod read;
mod recovery;
mod root_lock;
mod runtime;
mod scheduler;
mod segment;
pub mod segment_v2;
mod state;
mod storage_engine;
#[cfg(test)]
mod test_support;
mod tokens;
mod version;
mod writer;

pub use clock::{Clock, ManualClock, SystemClock};
pub use compaction::CompactionConfig;
pub use engine::{Engine, EngineConfig, FatalHandler};
pub use handle::{CollectionHandle, CollectionMeta, MaintenanceWritten};
pub use local_engine::LocalStorageEngine;
pub use memtable::MemtableConfig;
pub use read::{
    BoxFuture, CollectionReader, FetchPlan, Projection, ReadOptions, ReadView, Residency, RowData,
    RowSetResolver, SectionNeed, UnitView,
};
pub use runtime::{IoPool, Runtime, RuntimeConfig, run_cpu};
pub use scheduler::{MaintenanceScheduler, SchedulerStats};
pub use segment_v2::IndexPolicy;
pub use storage_engine::{
    BlobStore, CreateCollectionRequest, FetchedRecords, InspectReport, InspectTarget, StorageEngine,
};
pub use tokens::{InvalidSnapshotToken, SnapshotToken, TokenConfig};
pub use version::{Version, VersionCounters, VersionId};
pub use writer::{GroupCommitConfig, SchemaChange};
