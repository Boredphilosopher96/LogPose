//! Compaction: rewrite every immutable segment into one replacement segment.

use crate::{
    engine::{CoreRef, EngineCore},
    handle::CollectionHandle,
    manifest::Manifest,
    resolve::{ResolvedState, resolve_latest_from_segments},
    segment_v1::SegmentPurpose,
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{PutRecord, Result, Snapshot, WriteOperation};
use logpose_wal::WalRecord;
use std::sync::Arc;

impl CoreRef {
    /// Compact the collection's segments into one.
    ///
    /// Holds the maintenance slot for the whole job, so no flush publishes a manifest in
    /// between, but takes the writer slot only to publish: writes continue while the
    /// replacement segment is built, and the published `Version` keeps their delta.
    pub(crate) fn compact_collection(&self, handle: &Arc<CollectionHandle>) -> Result<Snapshot> {
        handle.ensure_writable()?;
        let _maintenance = handle.lock_maintenance()?;
        handle.ensure_writable()?;
        let base = handle.current();
        if base.manifest.segments.len() <= 1 {
            return Ok(base.snapshot());
        }

        let descriptor = handle.descriptor();
        // Nothing durable changes until the manifest is published, so a failure here leaves
        // the published state valid; the replacement's files are orphans.
        let next_manifest = self.compact_manifest(descriptor, &base.manifest)?;
        let published = self.publish_manifest(descriptor, &next_manifest);
        let mut writer = handle.lock_writer()?;
        match published {
            Ok(()) => {
                let latest = handle.current();
                let version = handle.publish(&writer, latest.with_manifest(next_manifest));
                Ok(version.snapshot())
            }
            Err(error) => Err(self.reload_after_failure(handle, &mut writer, error)),
        }
    }
}

impl EngineCore {
    /// Write the replacement segment for every segment of `manifest` and return the manifest
    /// that swaps it in.
    fn compact_manifest(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
    ) -> Result<Manifest> {
        let resolved = resolve_latest_from_segments(self.vfs.as_ref(), descriptor, manifest)?;
        let mut compacted_records = resolved
            .into_values()
            .map(|state| match state {
                ResolvedState::Visible(record) => WalRecord {
                    seq_no: record.seq_no,
                    op: WriteOperation::Put(PutRecord {
                        id: record.id,
                        vector: record.vector,
                        metadata: record.metadata,
                    }),
                },
                ResolvedState::Deleted { id, seq_no } => WalRecord {
                    seq_no,
                    op: WriteOperation::Delete(logpose_types::DeleteRecord { id }),
                },
            })
            .collect::<Vec<_>>();
        compacted_records.sort_by_key(|record| record.seq_no);

        let replacement =
            self.write_segment_file(descriptor, &compacted_records, SegmentPurpose::Compaction)?;
        Ok(Manifest {
            generation: manifest.generation + 1,
            checkpoint_seq_no: manifest.checkpoint_seq_no,
            segments: vec![replacement],
        })
    }
}
