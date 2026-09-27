//! Open and recover: load a collection's manifest and WAL delta, completing an interrupted WAL rotation first.

use crate::{
    LocalStorageEngine,
    durable_fs::{read_file, sync_parent_dir},
    manifest::Manifest,
    state::CollectionState,
    wal_rotation::wal_rotation_lock,
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{LogPoseError, Result, SeqNo};
use logpose_vfs::Vfs;
use logpose_wal::{
    WalBatch, WalFileKind, WalWriter, replay_dir_after_checkpoint, replay_file, rotate_active,
};
use std::{path::Path, sync::Arc};

impl LocalStorageEngine {
    pub(crate) fn load_collection_state(
        &self,
        collection_name: &str,
        manifest_generation: Option<u64>,
    ) -> Result<CollectionState> {
        let descriptor = self.find_collection_descriptor(collection_name)?;
        self.load_collection_state_descriptor(descriptor, manifest_generation)
    }

    pub(crate) fn load_collection_state_descriptor(
        &self,
        descriptor: CollectionDescriptor,
        manifest_generation: Option<u64>,
    ) -> Result<CollectionState> {
        self.load_collection_state_descriptor_inner(descriptor, manifest_generation, false)
    }

    pub(crate) fn load_collection_state_descriptor_with_wal_lock(
        &self,
        descriptor: CollectionDescriptor,
        manifest_generation: Option<u64>,
    ) -> Result<CollectionState> {
        self.load_collection_state_descriptor_inner(descriptor, manifest_generation, true)
    }

    fn load_collection_state_descriptor_inner(
        &self,
        descriptor: CollectionDescriptor,
        manifest_generation: Option<u64>,
        wal_lock_already_held: bool,
    ) -> Result<CollectionState> {
        self.recover_persisted_maintenance(&descriptor)?;
        let current_generation = self.read_current_generation(&descriptor)?;
        let target_generation = manifest_generation.unwrap_or(current_generation);
        let has_pending_rotation = self.exists(&Self::pending_rotation_file_path(&descriptor))?;
        let wal_lock = if has_pending_rotation && !wal_lock_already_held {
            Some(wal_rotation_lock(&descriptor.root_path))
        } else {
            None
        };
        let _wal_guard = wal_lock.as_ref().map(|lock| {
            lock.lock()
                .expect("wal rotation lock should not be poisoned")
        });
        let current_manifest = if has_pending_rotation {
            let current_manifest = self.load_manifest(&descriptor, Some(current_generation))?;
            self.finish_pending_rotation(&descriptor, &current_manifest)?;
            Some(current_manifest)
        } else {
            None
        };
        let manifest = match current_manifest {
            Some(current_manifest) if target_generation == current_generation => current_manifest,
            _ => self.load_manifest(&descriptor, Some(target_generation))?,
        };
        let mut delta = replay_dir_after_checkpoint(
            self.vfs.as_ref(),
            descriptor.root_path.join("wal"),
            manifest.checkpoint_seq_no,
        )?;
        delta.sort_by_key(|record| record.seq_no);

        Ok(CollectionState {
            descriptor,
            manifest,
            delta,
        })
    }

    /// Finish a flush that published its manifest but crashed before its WAL rotation completed.
    ///
    /// The surviving `PENDING_ROTATION` marker names the checkpoint of the flush. If the durable
    /// manifest has that checkpoint, the flush was published, so the active WAL holds only
    /// checkpointed records: it is rolled to `<checkpoint>.wal` exactly as the flush would have
    /// done (older manifest generations, and so older snapshots, still replay those records from
    /// it), a fresh active WAL is created, and the marker is removed. If the manifest has another
    /// checkpoint, the flush was never published and the marker is stale; it is left for the
    /// next flush to overwrite.
    fn finish_pending_rotation(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
    ) -> Result<()> {
        let marker_path = Self::pending_rotation_file_path(descriptor);
        if !self.exists(&marker_path)? {
            return Ok(());
        }

        let marker = read_file(
            self.vfs.as_ref(),
            &marker_path,
            "failed to read pending WAL rotation marker",
        )?;
        let pending_checkpoint = String::from_utf8_lossy(&marker)
            .trim()
            .parse::<u64>()
            .map_err(|error| {
                LogPoseError::Message(format!(
                    "failed to parse pending WAL rotation marker: {error}"
                ))
            })?;

        if pending_checkpoint != manifest.checkpoint_seq_no {
            return Ok(());
        }

        let active_wal_path = Self::active_wal_path(descriptor);
        ensure_active_wal_is_checkpointed(
            self.vfs.as_ref(),
            &active_wal_path,
            &marker_path,
            pending_checkpoint,
        )?;
        let rolled_wal_path = Self::rolled_wal_path(descriptor, pending_checkpoint);
        if self.exists(&rolled_wal_path)? {
            // The rename already happened; `active.wal`, if present, is the new file. Nothing is
            // appended while the marker is live, so it can only hold checkpointed records.
            let mut wal_writer = WalWriter::open(Arc::clone(&self.vfs), &active_wal_path)?;
            wal_writer.truncate()?;
            sync_parent_dir(self.vfs.as_ref(), &active_wal_path)?;
        } else {
            rotate_active(&self.vfs, &active_wal_path, &rolled_wal_path)?;
        }
        self.clear_pending_rotation_marker(descriptor)
    }
}

/// Refuse to discard an active WAL that holds records above the checkpoint named by a surviving
/// `PENDING_ROTATION` marker.
///
/// The rotation protocol never appends to `active.wal` while the marker is live, so such records
/// can only be acknowledged writes that followed a flush whose marker removal was lost. They are
/// not in any segment, so truncating would silently drop them.
///
/// A torn tail is ignored by the active-WAL reader, so a crash mid-append never blocks
/// recovery. Any other defect is an error: truncating an undecodable WAL could discard exactly
/// the records this check exists to protect, and opening it for truncation fails the same way.
fn ensure_active_wal_is_checkpointed(
    vfs: &dyn Vfs,
    active_wal_path: &Path,
    marker_path: &Path,
    checkpoint_seq_no: SeqNo,
) -> Result<()> {
    let batches = replay_file(vfs, active_wal_path, WalFileKind::Active)?;
    let Some(max_seq_no) = batches.iter().map(WalBatch::last_seq_no).max() else {
        return Ok(());
    };
    if max_seq_no <= checkpoint_seq_no {
        return Ok(());
    }
    Err(LogPoseError::Message(format!(
        "refusing to truncate '{}': pending WAL rotation marker '{}' names checkpoint {checkpoint_seq_no}, \
         but the active WAL holds records up to seq {max_seq_no} that are not checkpointed; \
         these are acknowledged writes, so recovery stopped instead of discarding them. \
         If the manifest checkpoint is correct, remove the marker to replay them",
        active_wal_path.display(),
        marker_path.display(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CreateCollectionRequest, StorageEngine,
        test_support::{put, unique_temp_dir, visible_ids},
    };
    use logpose_types::DistanceMetric;
    use std::fs;

    #[tokio::test]
    async fn recovery_refuses_to_truncate_uncheckpointed_records_behind_stale_marker() {
        let root = unique_temp_dir("storage-stale-marker-recovery");
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
            .expect("first write should succeed");
        let flushed = engine
            .flush("documents")
            .await
            .expect("flush should succeed");
        let acked = engine
            .write("documents", vec![put("beta", vec![0.0, 1.0])])
            .await
            .expect("post-flush write should be acknowledged");
        assert!(acked.last_seq_no > flushed.visible_seq_no);

        // Simulate a flush whose marker removal was lost before the write above was acknowledged.
        let marker_path = LocalStorageEngine::pending_rotation_file_path(&descriptor);
        fs::write(&marker_path, flushed.visible_seq_no.to_string())
            .expect("stale marker should be written");
        drop(engine);

        let reopened = LocalStorageEngine::new(&root).expect("storage engine should reopen");
        let error = reopened
            .scan_exact("documents", None)
            .await
            .expect_err("recovery must not discard acknowledged writes");
        assert!(
            error.to_string().contains("refusing to truncate"),
            "unexpected error: {error}"
        );
        let active_wal = replay_file(
            &logpose_vfs::StdVfs,
            LocalStorageEngine::active_wal_path(&descriptor),
            WalFileKind::Active,
        )
        .expect("active wal should stay readable");
        assert_eq!(
            active_wal
                .iter()
                .flat_map(WalBatch::records)
                .map(|record| record.seq_no)
                .collect::<Vec<_>>(),
            vec![acked.last_seq_no],
            "the acknowledged write must remain in the active wal"
        );

        fs::remove_file(&marker_path).expect("operator removes the stale marker");
        let visible = reopened
            .scan_exact("documents", None)
            .await
            .expect("scan should succeed once the stale marker is gone");
        assert_eq!(visible_ids(&visible), vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn recovery_truncates_checkpointed_active_wal_behind_pending_marker() {
        let root = unique_temp_dir("storage-pending-marker-checkpointed");
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
        let active_path = LocalStorageEngine::active_wal_path(&descriptor);
        let unrotated = fs::read(&active_path).expect("active wal should be readable");
        let flushed = engine
            .flush("documents")
            .await
            .expect("flush should succeed");

        // Simulate a crash after the manifest was published but before the WAL was rotated.
        fs::write(&active_path, unrotated).expect("active wal should be restored");
        let marker_path = LocalStorageEngine::pending_rotation_file_path(&descriptor);
        fs::write(&marker_path, flushed.visible_seq_no.to_string())
            .expect("marker should be written");

        let stats = engine
            .stats("documents")
            .await
            .expect("checkpointed active wal should be discarded");
        assert_eq!(stats.live_record_count, 1);
        assert_eq!(stats.mutable_op_count, 0);
        assert!(!marker_path.exists(), "marker should be cleared");
        assert!(
            replay_file(&logpose_vfs::StdVfs, &active_path, WalFileKind::Active)
                .expect("active wal should be readable")
                .is_empty(),
            "checkpointed records should be truncated"
        );
    }

    /// Regression test for a bug the `FaultVfs` crash tests found: recovery used to truncate the
    /// checkpointed active WAL instead of rolling it, so the records between the previous and
    /// the new checkpoint vanished from every older manifest generation, and snapshots taken
    /// before the flush failed with "visible sequence exceeds maximum".
    #[tokio::test]
    async fn recovery_rolls_the_active_wal_so_older_snapshots_stay_readable() {
        let root = unique_temp_dir("storage-pending-marker-old-snapshot");
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
        let before_flush = engine
            .snapshot("documents")
            .await
            .expect("snapshot should succeed");
        let flushed = engine
            .flush("documents")
            .await
            .expect("flush should succeed");

        // Simulate a crash after the manifest was published but before the WAL was rotated.
        let active_path = LocalStorageEngine::active_wal_path(&descriptor);
        let rolled_path = LocalStorageEngine::rolled_wal_path(&descriptor, flushed.visible_seq_no);
        fs::rename(&rolled_path, &active_path).expect("rotation should be undone");
        let marker_path = LocalStorageEngine::pending_rotation_file_path(&descriptor);
        fs::write(&marker_path, flushed.visible_seq_no.to_string())
            .expect("marker should be written");

        let stats = engine
            .stats("documents")
            .await
            .expect("recovery should succeed");
        assert_eq!(stats.mutable_op_count, 0);
        assert!(!marker_path.exists(), "marker should be cleared");
        assert!(rolled_path.exists(), "recovery should finish the rotation");

        let old = engine
            .scan_exact("documents", Some(before_flush))
            .await
            .expect("a snapshot from before the flush should stay readable");
        assert_eq!(visible_ids(&old), vec!["alpha"]);
    }

    #[tokio::test]
    async fn recovery_truncates_checkpointed_active_wal_with_torn_tail_behind_pending_marker() {
        let root = unique_temp_dir("storage-pending-marker-torn-tail");
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
        let active_path = LocalStorageEngine::active_wal_path(&descriptor);
        let mut unrotated = fs::read(&active_path).expect("active wal should be readable");
        let flushed = engine
            .flush("documents")
            .await
            .expect("flush should succeed");

        // Crash after the manifest was published but before rotation, with a torn frame left
        // by an append that was never acknowledged.
        let torn_frame = unrotated[..unrotated.len() / 2].to_vec();
        unrotated.extend_from_slice(&torn_frame);
        fs::write(&active_path, unrotated).expect("active wal should be restored");
        let marker_path = LocalStorageEngine::pending_rotation_file_path(&descriptor);
        fs::write(&marker_path, flushed.visible_seq_no.to_string())
            .expect("marker should be written");

        let stats = engine
            .stats("documents")
            .await
            .expect("a torn tail must not block recovery of a checkpointed active wal");
        assert_eq!(stats.live_record_count, 1);
        assert_eq!(stats.mutable_op_count, 0);
        assert!(!marker_path.exists(), "marker should be cleared");
        assert_eq!(
            fs::metadata(&active_path)
                .expect("active wal should exist")
                .len(),
            0,
            "the checkpointed records and the torn tail should be truncated"
        );
    }
}
