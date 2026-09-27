//! Flush: write the mutable delta as a new segment, publish the manifest, and rotate the WAL.

use crate::{
    LocalStorageEngine, fs_util::atomic_write, manifest::Manifest, segment_v1::SegmentPurpose,
    state::CollectionState,
};
use logpose_types::{Result, Snapshot};
use logpose_wal::rotate_active;

impl LocalStorageEngine {
    pub(crate) fn flush_state(&self, state: CollectionState) -> Result<Snapshot> {
        if state.delta.is_empty() {
            return Ok(Snapshot {
                manifest_generation: state.manifest.generation,
                visible_seq_no: state.visible_seq_no(),
            });
        }

        let segment_records = state.delta.clone();
        let new_segment =
            self.write_segment_file(&state.descriptor, &segment_records, SegmentPurpose::Flush)?;
        let checkpoint_seq_no = segment_records
            .last()
            .map(|record| record.seq_no)
            .unwrap_or(state.manifest.checkpoint_seq_no);

        let mut segments = state.manifest.segments.clone();
        segments.push(new_segment);

        let next_manifest = Manifest {
            generation: state.manifest.generation + 1,
            checkpoint_seq_no,
            segments,
        };
        atomic_write(
            self.vfs.as_ref(),
            &Self::pending_rotation_file_path(&state.descriptor),
            checkpoint_seq_no.to_string().into_bytes(),
        )?;
        self.publish_manifest(&state.descriptor, &next_manifest)?;

        let rolled_path = state
            .descriptor
            .root_path
            .join("wal")
            .join(format!("{checkpoint_seq_no:020}.wal"));
        rotate_active(
            &self.vfs,
            Self::active_wal_path(&state.descriptor),
            rolled_path,
        )?;
        // A surviving marker makes the next load treat `active.wal` as checkpointed, so the
        // flush must not report success unless the marker is durably gone.
        self.clear_pending_rotation_marker(&state.descriptor)?;

        Ok(Snapshot {
            manifest_generation: next_manifest.generation,
            visible_seq_no: checkpoint_seq_no,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CreateCollectionRequest, LocalStorageEngine, StorageEngine, failpoints,
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

        let marker_path = LocalStorageEngine::pending_rotation_file_path(&descriptor);
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

        engine
            .write("documents", vec![put("beta", vec![0.0, 1.0])])
            .await
            .expect_err("writes must be refused while the stale marker cannot be cleared");

        failpoints::clear_marker_removal_failure(&marker_path);
        engine
            .write("documents", vec![put("beta", vec![0.0, 1.0])])
            .await
            .expect("write should succeed once recovery clears the marker");
        assert!(
            !marker_path.exists(),
            "recovery should clear the checkpointed marker"
        );

        drop(engine);
        let reopened = LocalStorageEngine::new(&root).expect("storage engine should reopen");
        let visible = reopened
            .scan_exact("documents", None)
            .await
            .expect("scan should succeed after reopen");
        assert_eq!(visible_ids(&visible), vec!["alpha", "beta"]);
    }
}
