//! Storage engine abstractions.
//!
//! [`StorageEngine`] is the storage contract and [`LocalStorageEngine`] implements it on the
//! local filesystem. The implementation is split by concern:
//!
//! - `storage_engine`: the trait and its public request, inspection and blob-store types.
//! - `local_engine`: the engine struct, its storage-root claim and the trait implementation.
//! - `collections`, `catalog`: collection, database and principal descriptor files.
//! - `paths`: the on-disk layout.
//! - `recovery`, `wal_rotation`, `state`: loading a collection's manifest and WAL delta.
//! - `manifest`, `segment_v1`: the v1 manifest and segment file formats.
//! - `flush`, `compaction`, `maintenance`: background maintenance jobs and their queue.
//! - `resolve`, `stats`, `metric`: latest-visible resolution, statistics and scoring.
//! - `durable_fs`, `fs_util`, `root_lock`, `error`: filesystem and error helpers.

#[cfg(test)]
use logpose_query as _;
#[cfg(test)]
use rand as _;

mod catalog;
mod collections;
mod compaction;
mod durable_fs;
mod error;
#[cfg(test)]
mod failpoints;
mod flush;
mod fs_util;
mod local_engine;
mod maintenance;
mod manifest;
mod metric;
mod paths;
mod recovery;
mod resolve;
mod root_lock;
mod segment_v1;
mod state;
mod stats;
mod storage_engine;
#[cfg(test)]
mod test_support;
mod wal_rotation;

pub use local_engine::LocalStorageEngine;
pub use storage_engine::{
    BlobStore, CreateCollectionRequest, InspectReport, InspectTarget, StorageEngine,
};
