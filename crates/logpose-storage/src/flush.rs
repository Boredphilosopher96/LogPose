//! Flush: write the oldest frozen memtable as a segment v2 file and every grown deletion vector
//! as a new DV file, then have the writer publish the manifest that checkpoints them.
//!
//! The job's build runs on a job thread; the collection's writer task begins and commits it:
//!
//! 1. **Freeze** (writer). When the active memtable reaches a flush trigger (or an explicit
//!    flush needs it) and fewer than `max_frozen` memtables are frozen, the writer freezes it as
//!    `F` (rotating the WAL so every later write lands in a newer file), publishes, and asks the
//!    scheduler for a flush permit. Flushes of one collection run one at a time, oldest frozen
//!    memtable first.
//! 2. **Begin** (writer, on the permit). Capture `D_F` (`F`'s deleted slots), a new DV
//!    generation for every segment whose deletion vector grew since its durable generation with
//!    a snapshot of that vector (`D_S`), `J` (the published `visible_seq_no`), and the job's
//!    unit `s`.
//! 3. **Build** (job thread). `F`'s slots not in `D_F`, in slot order, become the rows of `s`;
//!    fields are written as the schema captured at the begin declares them.
//! 4. **Write** `segments/<s>.seg` and `sync_all` (`FlushAfterSegmentSync`), then each DV file
//!    with `covered_seq_no = J` (`FlushAfterDvSync` after each), then `sync_dir(segments/)`
//!    (`FlushAfterSegmentsDirSync`). A memtable with no live slot writes no segment: the flush
//!    is a pure checkpoint.
//! 5. **Commit** (writer). Manifest `g + 1` = the durable segments with their new DV
//!    generations, plus `s`, with checkpoint `L` (`F`'s last operation). Once it is durable
//!    the writer drops `F`, adds `s` with `F`'s late deletions (slots deleted after the begin,
//!    mapped to their rows), forwards the primary-key index from `F` to `s`, publishes, and
//!    removes the superseded DV files and the checkpointed WAL files.
//!
//! A failure before the manifest's `CURRENT` rename abandons the flush with no state change:
//! its unit, DV generations, and manifest generation are burned and its files removed, and `F`
//! stays frozen for the next flush. A failure at or after the rename poisons the collection.

use crate::{
    dv::{DvFile, dv_path, write_dv_file},
    engine::CoreRef,
    fs_util::crash_point,
    handle::{CollectionHandle, JobTicket},
    manifest::{DvRef, SegmentOrigin},
    paths::{SEGMENTS_DIR, segment_path},
    segment::{SegmentHandle, manifest_entry, write_segment},
    segment_v2::{SegmentBuilder, SegmentIdentity},
    version::Version,
    writer::{FlushStart, FlushedSegment, JobCommit, row_map},
};
use logpose_types::{LogPoseError, Result, UnitId};
use logpose_vfs::CrashPoint;
use std::sync::Arc;

impl CoreRef {
    /// Build and write what a flush begun at `version` commits: the segment of unit `unit`
    /// from the frozen memtable's live slots, and the DV files.
    pub(crate) fn build_flush(
        &self,
        handle: &Arc<CollectionHandle>,
        version: &Version,
        unit: UnitId,
        work: &FlushStart,
        ticket: &mut JobTicket,
    ) -> Result<JobCommit> {
        let vfs = self.vfs.as_ref();
        let dir = &handle.meta().dir;
        let memtable = &work.memtable;
        let schema = Arc::clone(&version.schema);

        let (slot_to_row, rows) =
            row_map(memtable.slot_count(), |slot| !work.deleted.contains(slot));
        let segment = if rows == 0 {
            None
        } else {
            let mut builder = SegmentBuilder::new(
                Arc::clone(&schema),
                SegmentIdentity {
                    collection_id: handle.meta().id.clone(),
                    unit_id: unit.0,
                },
            )
            .map_err(LogPoseError::from)?;
            let mut row_to_slot = Vec::with_capacity(rows as usize);
            for (slot, row) in (0..).zip(slot_to_row.iter()) {
                if *row == u32::MAX {
                    continue;
                }
                let image = memtable.row_image(slot).map_err(LogPoseError::internal)?;
                let seq_no = memtable
                    .seq_no(slot)
                    .ok_or_else(|| LogPoseError::internal(format!("slot {slot} has no seq")))?;
                builder
                    .push_row_image(seq_no, &image)
                    .map_err(LogPoseError::from)?;
                row_to_slot.push(slot);
            }
            let path = segment_path(dir, unit);
            ticket.writing_files();
            self.build_indexes(&mut builder)?;
            let (file, written) = write_segment(vfs, &path, builder)?;
            crash_point(vfs, Some(CrashPoint::FlushAfterSegmentSync))?;
            let entry = manifest_entry(
                unit,
                &schema,
                &written,
                SegmentOrigin::Flush {
                    first_seq_no: memtable.first_seq_no,
                    last_seq_no: memtable.last_seq_no,
                },
            );
            let segment = SegmentHandle::from_file(
                file,
                path,
                &handle.meta().id,
                entry,
                self.buffer_cache(),
                self.gc.clone(),
            )?;
            Some(FlushedSegment {
                handle: Arc::new(segment),
                slot_to_row: Arc::from(slot_to_row),
                row_to_slot: Arc::from(row_to_slot),
            })
        };

        let mut dvs = Vec::with_capacity(work.dvs.len());
        for write in &work.dvs {
            let segment_unit = write.segment.unit;
            let file = DvFile {
                unit: segment_unit,
                row_count: write.segment.row_count(),
                generation: write.generation,
                covered_seq_no: work.covered_seq_no,
                bitmap: write.deletes.to_bitmap(),
            };
            ticket.writing_files();
            write_dv_file(vfs, &dv_path(dir, segment_unit, write.generation), &file)?;
            crash_point(vfs, Some(CrashPoint::FlushAfterDvSync))?;
            dvs.push((
                segment_unit,
                DvRef {
                    generation: write.generation,
                    cardinality: u32::try_from(file.bitmap.len()).unwrap_or(u32::MAX),
                    covered_seq_no: work.covered_seq_no,
                },
            ));
        }
        if segment.is_some() || !dvs.is_empty() {
            let segments = dir.join(SEGMENTS_DIR);
            vfs.sync_dir(&segments).map_err(|error| {
                LogPoseError::io(format!("failed to sync '{}'", segments.display()), error)
            })?;
            crash_point(vfs, Some(CrashPoint::FlushAfterSegmentsDirSync))?;
        }
        Ok(JobCommit::Flush {
            memtable: memtable.unit,
            checkpoint_seq_no: memtable.last_seq_no,
            segment,
            dvs,
        })
    }
}
