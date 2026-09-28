//! Flush triggers, and the explicit flush and compaction entry points.
//!
//! Background maintenance has no queue of its own: each collection's writer freezes its active
//! memtable when a trigger below fires, plans compactions with the size-tiered policy, and asks
//! the engine-wide [`MaintenanceScheduler`](crate::MaintenanceScheduler) for a permit per job.
//! Maintenance status is runtime state the writer keeps on the collection handle; nothing about
//! it is persisted, because every job is re-planned from the durable state after a restart.

use crate::memtable::{MemtableConfig, MemtableData};
use logpose_catalog::CollectionDescriptor;
use std::time::Duration;

/// Whether `active` has reached a flush trigger at engine-clock time `now`: the collection's
/// operation count (every upsert, update, delete, and schema change counts, so a delete-heavy
/// workload that adds no slots still checkpoints), its byte threshold or the engine's
/// (whichever is lower), the engine's slot count, or its age.
pub(crate) fn should_flush(
    descriptor: &CollectionDescriptor,
    config: &MemtableConfig,
    active: &MemtableData,
    now: Duration,
) -> bool {
    if !active.has_ops() {
        return false;
    }
    let max_bytes = config
        .max_bytes
        .min(descriptor.flush_threshold_bytes as u64);
    active.op_count() >= descriptor.flush_threshold_ops as u64
        || active.bytes().total() >= max_bytes
        || active.slot_count() >= config.max_rows
        || now.saturating_sub(active.created_at) >= config.max_age
}

#[cfg(test)]
impl crate::engine::CoreRef {
    /// Flush every operation `handle` had when the call began. Blocking.
    pub(crate) fn flush_collection(
        &self,
        handle: &crate::handle::CollectionHandle,
    ) -> logpose_types::Result<logpose_types::Snapshot> {
        handle.flush_blocking()
    }

    /// Compact `handle`'s segments into one, as far as one job can hold. Blocking.
    pub(crate) fn compact_collection(
        &self,
        handle: &crate::handle::CollectionHandle,
    ) -> logpose_types::Result<logpose_types::Snapshot> {
        handle.compact_blocking()
    }
}
