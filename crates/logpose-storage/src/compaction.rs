//! Compaction: rewrite every immutable segment into one replacement segment.
//!
//! The job begins through the collection's writer (which serializes it with flushes), builds
//! the replacement from the segments of the `Version` it was given while writes continue, and
//! has the writer publish the manifest that swaps it in.

use crate::{
    engine::{CoreRef, EngineCore},
    handle::CollectionHandle,
    manifest::{Manifest, SegmentMeta},
    resolve::{ResolvedState, resolve_latest_from_segments},
    segment_v1::{SegmentPurpose, SegmentRecord},
    writer::{JobCommit, JobKind},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{PutRecord, Result, Snapshot, WriteOperation};
use std::sync::Arc;

impl CoreRef {
    /// Compact the collection's segments into one. Blocking; runs on a job thread.
    pub(crate) fn compact_collection(&self, handle: &Arc<CollectionHandle>) -> Result<Snapshot> {
        let (ticket, base) = handle.begin_job(JobKind::Compact)?;
        if base.manifest.segments.len() <= 1 {
            return Ok(base.snapshot());
        }
        // Nothing durable changes until the writer publishes the manifest, so a failure here
        // leaves the published state valid; the replacement's files are orphans.
        let output = self.compact_segments(handle.descriptor(), &base.manifest)?;
        ticket.commit(JobCommit::Compact {
            inputs: base
                .manifest
                .segments
                .iter()
                .map(|segment| segment.segment_id.clone())
                .collect(),
            output,
        })
    }
}

impl EngineCore {
    /// Write the replacement segment for every segment of `manifest`.
    fn compact_segments(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
    ) -> Result<SegmentMeta> {
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
        self.write_segment_file(descriptor, &compacted_records, SegmentPurpose::Compaction)
    }
}
