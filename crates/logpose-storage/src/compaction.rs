//! Compaction: rewrite the collection's segments into one, dropping deleted rows, with the
//! deletions that arrive while it runs reconciled onto the output at commit.
//!
//! Until the size-tiered policy lands (PR 11), a compaction takes every segment as input once
//! the collection has `compaction_threshold_segments` of them. The protocol is the final one:
//!
//! 1. **Begin** (writer). Capture every segment with `D0_i`, its deletion vector now, and the
//!    job's unit `o`.
//! 2. **Build** (job thread, reads around the buffer cache). For each input in order, copy
//!    every row not in `D0_i` to the output (fields as the current schema declares them:
//!    dropped fields are gone, added fields read null) and record `map_i[r]`, the output row,
//!    or `u32::MAX` for a row in `D0_i`.
//! 3. **Write** `segments/<o>.seg`, `sync_all`, `sync_dir(segments/)`
//!    (`CompactionAfterOutputSync`).
//! 4. **Commit** (writer, with no write processed until the new version is published): for each
//!    input, every deletion set since the begin (`deletes[I_i] AND NOT D0_i`, the bits whose
//!    row was copied) lands on `map_i[r]` in `DV_o`; if `DV_o` is not empty its DV file is
//!    written and synced (`CompactionAfterDvSync`) before manifest `g + 1` (the durable
//!    segments minus the inputs plus `o`) is published. Then the inputs leave the version (their
//!    files once the last version holding them is released), the primary-key index is
//!    forwarded from each input to `o`, and the inputs' DV files are removed.
//!
//! Every live row is therefore live exactly once before and after the swap (I4, I5).

use crate::{
    engine::CoreRef,
    fs_util::crash_point,
    handle::{CollectionHandle, JobTicket},
    manifest::SegmentOrigin,
    paths::{SEGMENTS_DIR, segment_path},
    segment::{SegmentHandle, manifest_entry, write_segment},
    segment_v2::{SegmentBuilder, SegmentIdentity},
    version::Version,
    writer::{CompactStart, CompactedSegment, JobCommit, JobKind, JobWork},
};
use logpose_types::{LogPoseError, Result, RowAddr, Snapshot, UnitId, record::PrimaryKey};
use logpose_vfs::CrashPoint;
use std::sync::Arc;

impl CoreRef {
    /// Compact the collection's segments into one. Blocking; runs on a job thread.
    pub(crate) fn compact_collection(&self, handle: &Arc<CollectionHandle>) -> Result<Snapshot> {
        let (mut ticket, start) = handle.begin_job(JobKind::Compact)?;
        let JobWork::Compact(work) = start.work else {
            return Ok(start.version.snapshot());
        };
        let commit =
            self.build_compaction(handle, &start.version, start.unit, &work, &mut ticket)?;
        ticket.commit(commit)
    }

    /// Build and write what a compaction begun at `version` commits: the output segment of
    /// unit `unit` from the inputs' rows outside their captured deletion vectors, with the row
    /// maps the writer reconciles later deletions through.
    pub(crate) fn build_compaction(
        &self,
        handle: &Arc<CollectionHandle>,
        version: &Version,
        unit: UnitId,
        work: &CompactStart,
        ticket: &mut JobTicket<'_>,
    ) -> Result<JobCommit> {
        let vfs = self.vfs.as_ref();
        let dir = &handle.meta().dir;
        let schema = Arc::clone(&version.schema);
        let inputs = work
            .inputs
            .iter()
            .map(|(segment, _)| segment.unit)
            .collect::<Vec<_>>();
        let mut builder = SegmentBuilder::new(
            Arc::clone(&schema),
            SegmentIdentity {
                collection_id: handle.meta().id.clone(),
                unit_id: unit.0,
            },
        )
        .map_err(LogPoseError::from)?;
        let mut maps = Vec::with_capacity(work.inputs.len());
        let mut pks = Vec::new();
        let mut sources = Vec::new();
        for (segment, deleted) in &work.inputs {
            let rows = segment.read_rows()?;
            let mut map = Vec::with_capacity(rows.len());
            for (row, stored) in (0_u32..).zip(rows) {
                if deleted.contains(row) {
                    map.push(u32::MAX);
                    continue;
                }
                map.push(builder.row_count());
                builder
                    .push_row_image(stored.seq_no, &stored.image)
                    .map_err(LogPoseError::from)?;
                pks.push(PrimaryKey::from(stored.image.pk));
                sources.push(RowAddr {
                    unit: segment.unit,
                    row,
                });
            }
            maps.push(Arc::<[u32]>::from(map));
        }
        let output = if builder.row_count() == 0 {
            None
        } else {
            let path = segment_path(dir, unit);
            ticket.writing_files();
            self.build_indexes(&mut builder)?;
            let (file, written) = write_segment(vfs, &path, builder)?;
            let segments = dir.join(SEGMENTS_DIR);
            vfs.sync_dir(&segments).map_err(|error| {
                LogPoseError::io(format!("failed to sync '{}'", segments.display()), error)
            })?;
            crash_point(vfs, Some(CrashPoint::CompactionAfterOutputSync))?;
            let entry = manifest_entry(
                unit,
                &schema,
                &written,
                SegmentOrigin::Compaction {
                    inputs: inputs.clone(),
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
            Some(CompactedSegment {
                handle: Arc::new(segment),
                maps,
                pks: Arc::from(pks),
                sources: Arc::from(sources),
            })
        };
        Ok(JobCommit::Compact { inputs, output })
    }
}
