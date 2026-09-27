//! Flush: write the mutable delta as a new segment, publish the manifest, rotate the WAL, and
//! publish a `Version` with an empty delta.

use crate::{
    engine::{CoreRef, EngineCore},
    fs_util::atomic_write,
    handle::CollectionHandle,
    manifest::Manifest,
    segment_v1::SegmentPurpose,
    version::{DeltaLog, Version},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{Result, Snapshot};
use logpose_wal::rotate_active;
use std::sync::Arc;

impl CoreRef {
    /// Flush the collection's delta into a new segment.
    ///
    /// Holds the maintenance slot and, because rotation replaces the active WAL, the writer slot
    /// for the whole flush, so writes wait for it. A failure after the durable state may have
    /// changed reloads the collection from disk (see `reload_after_failure`).
    pub(crate) fn flush_collection(&self, handle: &Arc<CollectionHandle>) -> Result<Snapshot> {
        handle.ensure_writable()?;
        let _maintenance = handle.lock_maintenance()?;
        let mut writer = handle.lock_writer()?;
        handle.ensure_writable()?;
        let current = handle.current();
        if current.delta.is_empty() {
            return Ok(current.snapshot());
        }

        // Rotation renames the active WAL, so the open writer must not outlive it.
        writer.wal = None;
        let descriptor = handle.descriptor();
        let flushed = self
            .flush_state(descriptor, &current)
            .and_then(|manifest| Ok((manifest, self.open_active_wal(descriptor)?)));
        match flushed {
            Ok((manifest, wal)) => {
                writer.wal = Some(wal);
                let version =
                    handle.publish(&writer, current.with_state(manifest, DeltaLog::default()));
                Ok(version.snapshot())
            }
            Err(error) => Err(self.reload_after_failure(handle, &mut writer, error)),
        }
    }
}

impl EngineCore {
    /// Write `version`'s delta as a segment, durably publish the next manifest, and rotate the
    /// WAL. Returns the published manifest.
    fn flush_state(
        &self,
        descriptor: &CollectionDescriptor,
        version: &Version,
    ) -> Result<Manifest> {
        let segment_records = version.delta.to_vec();
        let new_segment =
            self.write_segment_file(descriptor, &segment_records, SegmentPurpose::Flush)?;
        let checkpoint_seq_no = segment_records
            .last()
            .map(|record| record.seq_no)
            .unwrap_or(version.checkpoint_seq_no);

        let mut segments = version.manifest.segments.clone();
        segments.push(new_segment);

        let next_manifest = Manifest {
            generation: version.manifest_generation + 1,
            checkpoint_seq_no,
            segments,
        };
        atomic_write(
            self.vfs.as_ref(),
            &Self::pending_rotation_file_path(descriptor),
            checkpoint_seq_no.to_string().into_bytes(),
        )?;
        self.publish_manifest(descriptor, &next_manifest)?;

        rotate_active(
            &self.vfs,
            Self::active_wal_path(descriptor),
            Self::rolled_wal_path(descriptor, checkpoint_seq_no),
        )?;
        // A surviving marker makes the next recovery treat `active.wal` as checkpointed, so the
        // flush must not report success unless the marker is durably gone.
        self.clear_pending_rotation_marker(descriptor)?;
        Ok(next_manifest)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CreateCollectionRequest, LocalStorageEngine, StorageEngine,
        engine::EngineCore,
        failpoints,
        test_support::{put, unique_temp_dir, visible_ids},
    };
    use logpose_types::DistanceMetric;

    #[tokio::test]
    async fn flush_fails_when_pending_rotation_marker_cannot_be_removed() {
        let root = unique_temp_dir("storage-marker-removal-failure");
        let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
        let descriptor = engine
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        engine
            .write("documents", vec![put("alpha", vec![1.0, 0.0])])
            .await
            .expect("write should succeed");

        let marker_path = EngineCore::pending_rotation_file_path(&descriptor);
        failpoints::fail_marker_removal(&marker_path);
        let error = engine
            .flush("documents")
            .await
            .expect_err("flush must not report success while the rotation marker survives");
        assert!(
            error
                .to_string()
                .contains("failed to clear pending WAL rotation marker"),
            "unexpected error: {error}"
        );
        assert!(
            marker_path.exists(),
            "marker should survive the failed removal"
        );

        let refused = engine
            .write("documents", vec![put("beta", vec![0.0, 1.0])])
            .await
            .expect_err("writes must be refused while the stale marker cannot be cleared");
        assert!(
            refused
                .to_string()
                .contains("read-only until the engine is reopened"),
            "unexpected error: {refused}"
        );
        let visible = engine
            .scan_exact("documents", None)
            .await
            .expect("the last published version keeps serving reads");
        assert_eq!(visible_ids(&visible), vec!["alpha"]);

        // Reopening runs recovery, which clears the checkpointed marker.
        failpoints::clear_marker_removal_failure(&marker_path);
        drop(engine);
        let engine = LocalStorageEngine::new(&root).expect("storage engine should reopen");
        assert!(
            !marker_path.exists(),
            "recovery should clear the checkpointed marker"
        );
        engine
            .write("documents", vec![put("beta", vec![0.0, 1.0])])
            .await
            .expect("write should succeed after recovery");

        drop(engine);
        let reopened = LocalStorageEngine::new(&root).expect("storage engine should reopen");
        let visible = reopened
            .scan_exact("documents", None)
            .await
            .expect("scan should succeed after reopen");
        assert_eq!(visible_ids(&visible), vec!["alpha", "beta"]);
    }
}
