//! Compaction: rewrite every immutable segment into one replacement segment.
//!
//! The job begins through the collection's writer (which serializes it with flushes and
//! allocates its unit id), builds the replacement from the segments of the `Version` it was
//! given while writes continue, and has the writer publish the manifest that swaps it in. The
//! inputs' files stay on disk until the last `Version` that holds them, including a token-pinned
//! one, is released.

use crate::{
    engine::{CoreRef, EngineCore},
    handle::CollectionHandle,
    manifest::{Manifest, ManifestSegment, SegmentOrigin},
    resolve::{ResolvedState, resolve_latest_from_segments},
    segment_v1::{SegmentBuild, SegmentPurpose, SegmentRecord},
    writer::{JobCommit, JobKind},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{PutRecord, Result, Snapshot, WriteOperation};
use std::sync::Arc;

impl CoreRef {
    /// Compact the collection's segments into one. Blocking; runs on a job thread.
    pub(crate) fn compact_collection(&self, handle: &Arc<CollectionHandle>) -> Result<Snapshot> {
        let (mut ticket, start) = handle.begin_job(JobKind::Compact)?;
        let base = start.version;
        if base.manifest.segments.len() <= 1 {
            return Ok(base.snapshot());
        }
        let inputs = base.manifest.units().collect::<Vec<_>>();
        // Nothing durable changes until the writer publishes the manifest, so a failure here
        // leaves the published state valid; the writer removes the replacement's files.
        ticket.writing_files();
        let output = self.compact_segments(
            handle.descriptor(),
            &base.manifest,
            SegmentBuild {
                unit: start.unit,
                purpose: SegmentPurpose::Compaction,
                origin: SegmentOrigin::Compaction {
                    inputs: inputs.clone(),
                },
                schema_version: base.schema.schema_version(),
            },
        )?;
        ticket.commit(JobCommit::Compact { inputs, output })
    }
}

impl EngineCore {
    /// Write the replacement segment for every segment of `manifest`.
    fn compact_segments(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
        build: SegmentBuild,
    ) -> Result<ManifestSegment> {
        let resolved = resolve_latest_from_segments(self.vfs.as_ref(), descriptor, manifest)?;
        let mut compacted_records = resolved
            .into_values()
            .map(|state| match state {
                ResolvedState::Visible(record) => SegmentRecord {
                    seq_no: record.seq_no,
                    op: WriteOperation::Put(PutRecord {
                        id: record.id,
                        vector: record.vector,
                        metadata: record.metadata,
                    }),
                },
                ResolvedState::Deleted { id, seq_no } => SegmentRecord {
                    seq_no,
                    op: WriteOperation::Delete(logpose_types::DeleteRecord { id }),
                },
            })
            .collect::<Vec<_>>();
        compacted_records.sort_by_key(|record| record.seq_no);
        self.write_segment_file(descriptor, &compacted_records, build)
    }
}
