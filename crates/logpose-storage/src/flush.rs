//! Flush: write the delta a writer-frozen `Version` holds as a v1 segment, then have the writer
//! publish the manifest that checkpoints it and a `Version` without it.
//!
//! The job runs on a job thread and talks to the collection's writer task:
//!
//! 1. **Begin.** The writer waits until no other maintenance job of the collection is active,
//!    drains its pipeline, rotates the WAL (so the frozen delta ends in an older file than every
//!    later write), and replies with the published, hence durable, `Version`. Its
//!    `visible_seq_no` `L` is the checkpoint. Writes continue meanwhile.
//! 2. **Build.** The job writes the delta's rows as a segment (`FlushAfterSegmentSync`,
//!    `FlushAfterSegmentsDirSync`). A delta of schema changes only needs no segment.
//! 3. **Commit.** The writer drains again, publishes manifest `g + 1` (the durable manifest
//!    plus the segment, checkpoint `L`, the writer's schema), drops the delta at or below `L`
//!    from its state, publishes the next `Version`, and queues a checkpoint frame for its next
//!    WAL group.
//!
//! A failure before the manifest's `CURRENT` rename abandons the flush with no state change; a
//! failure at or after it poisons the collection. Checkpointed WAL files are kept: historical
//! snapshots still replay them, until snapshot tokens and GC replace that (PR 6).

use crate::{
    engine::CoreRef, handle::CollectionHandle, legacy_view::legacy_record,
    segment_v1::SegmentPurpose, writer::JobCommit, writer::JobKind,
};
use logpose_types::{Result, Snapshot};
use std::sync::Arc;

impl CoreRef {
    /// Flush the collection's delta into a new segment. Blocking; runs on a job thread.
    pub(crate) fn flush_collection(&self, handle: &Arc<CollectionHandle>) -> Result<Snapshot> {
        let (ticket, frozen) = handle.begin_job(JobKind::Flush)?;
        if frozen.delta.is_empty() {
            return Ok(frozen.snapshot());
        }
        let checkpoint_seq_no = frozen.visible_seq_no;
        let mut records = Vec::with_capacity(frozen.delta.len());
        for record in frozen.delta.iter() {
            records.extend(legacy_record(&frozen.schema, record)?);
        }
        let segment = if records.is_empty() {
            None
        } else {
            Some(self.write_segment_file(handle.descriptor(), &records, SegmentPurpose::Flush)?)
        };
        ticket.commit(JobCommit::Flush {
            checkpoint_seq_no,
            segment,
        })
    }
}
