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
//!   function shared with replay, and poisoning after a failed WAL write.
//! - `legacy_view`: the v1 record view of v2 rows, for the legacy read paths.
//! - `storage_engine`: the trait and its public request, inspection and blob-store types.
//! - `local_engine`: the trait implementation over `Engine`.
//! - `collections`, `catalog`: collection, database and principal descriptor files.
//! - `paths`: the on-disk layout.
//! - `recovery`: the durability barrier, loading the manifest `CURRENT` names, orphan cleanup,
//!   and replaying the WAL above its checkpoint.
//! - `state`: resolving a read to the current or a token-pinned `Version`.
//! - `manifest`: manifest v2 and the `CURRENT` publish protocol.
//! - `gc`: version-refcounted segment files, the file-removal queue, and orphan cleanup.
//! - `tokens`, `clock`: snapshot tokens, their reaper, and the injectable clock.
//! - `segment_v1`: the v1 segment file format.
//! - `segment_v2`: the v2 segment file format, builder, and reader (not yet wired in).
//! - `cache`: the buffer cache of segment section bytes that segment v2 readers load through.
//! - `flush`, `compaction`, `maintenance`: maintenance jobs and each collection's queue.
//! - `resolve`, `stats`, `metric`: latest-visible resolution, statistics and scoring.
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
mod engine;
mod error;
mod flush;
mod fs_util;
mod gc;
mod handle;
mod legacy_view;
mod local_engine;
mod maintenance;
mod manifest;
mod metric;
mod paths;
mod recovery;
mod resolve;
mod root_lock;
mod runtime;
mod segment_v1;
pub mod segment_v2;
mod state;
mod stats;
mod storage_engine;
#[cfg(test)]
mod test_support;
mod tokens;
mod version;
mod writer;

pub use clock::{Clock, ManualClock, SystemClock};
pub use engine::{Engine, EngineConfig, FatalHandler};
pub use handle::{CollectionHandle, CollectionMeta};
pub use local_engine::LocalStorageEngine;
pub use runtime::{IoPool, Runtime, RuntimeConfig, run_cpu};
pub use storage_engine::{
    BlobStore, CreateCollectionRequest, InspectReport, InspectTarget, StorageEngine,
};
pub use tokens::{InvalidSnapshotToken, SnapshotToken, TokenConfig};
pub use version::{Version, VersionCounters, VersionId};
pub use writer::{GroupCommitConfig, SchemaChange};
