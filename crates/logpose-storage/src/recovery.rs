//! Open and recover: build a collection's first `Version` from its manifest and WAL delta,
//! completing an interrupted WAL rotation first; reload after a failed job; and load historical
//! states for snapshots of older manifest generations.

use crate::{
    durable_fs::{read_file, sync_parent_dir},
    engine::EngineCore,
    fs_util::read_json,
    handle::{CollectionHandle, CollectionMeta, WriterSlot},
    maintenance::MaintenanceState,
    manifest::Manifest,
    state::{CollectionState, resolve_snapshot},
    version::{DeltaLog, Version},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{CollectionRef, CorruptionKind, LogPoseError, Result, SeqNo, Snapshot};
use logpose_vfs::Vfs;
use logpose_wal::{
    WalBatch, WalFileKind, WalWriter, replay_dir_after_checkpoint, replay_file, rotate_active,
};
use std::{path::Path, sync::Arc};

/// The outcome of recovering one collection directory at engine open.
pub(crate) enum RecoveredCollection {
    /// Recovered and ready to serve.
    Open(Arc<CollectionHandle>),
    /// The descriptor names the collection, but it cannot be served.
    Failed {
        reference: CollectionRef,
        /// The descriptor, when it is valid.
        descriptor: Option<Box<CollectionDescriptor>>,
        error: LogPoseError,
    },
    /// The descriptor cannot be parsed, so the collection is not even known by name.
    Unreadable { error: String },
}

impl EngineCore {
    /// Recover the collection in `dir`: read its descriptor, placement, and maintenance status,
    /// finish an interrupted WAL rotation, load the current manifest, replay the WAL above its
    /// checkpoint, repair the active WAL's tail, and build `Version` 1.
    pub(crate) fn recover_collection(&self, dir: &Path) -> RecoveredCollection {
        let descriptor_path = dir.join("descriptor.json");
        let mut descriptor =
            match read_json::<CollectionDescriptor>(self.vfs.as_ref(), &descriptor_path) {
                Ok(descriptor) => descriptor,
                Err(error) => {
                    return RecoveredCollection::Unreadable {
                        error: error.to_string(),
                    };
                }
            };
        // The collection lives where its descriptor was found. The stored path is the one it
        // was created at, which differs once a storage root is moved or a collection's files
        // are copied to another node.
        descriptor.root_path = dir.to_path_buf();
        let reference = descriptor.collection_ref();
        if let Err(error) = descriptor.validate() {
            return RecoveredCollection::Failed {
                reference,
                descriptor: None,
                error: LogPoseError::corrupt(
                    CorruptionKind::Descriptor,
                    format!(
                        "collection descriptor in '{}' is invalid: {error}",
                        dir.display()
                    ),
                ),
            };
        }
        match self.open_collection(descriptor.clone()) {
            Ok(handle) => RecoveredCollection::Open(handle),
            Err(error) => RecoveredCollection::Failed {
                reference,
                descriptor: Some(Box::new(descriptor)),
                error,
            },
        }
    }

    fn open_collection(&self, descriptor: CollectionDescriptor) -> Result<Arc<CollectionHandle>> {
        let assignment = self.load_collection_assignment(&descriptor)?;
        let persisted = self.load_maintenance_status(&descriptor)?;
        let (jobs, resume, changed) = MaintenanceState::recovered(persisted);
        if changed {
            self.persist_maintenance_status(&descriptor, jobs.status())?;
        }
        let (manifest, delta) = self.load_current_state(&descriptor)?;
        let wal = self.open_active_wal(&descriptor)?;
        let meta = Arc::new(CollectionMeta::new(descriptor, assignment));
        let version = Version::initial(meta, manifest, delta);
        let handle = CollectionHandle::new(version, Some(wal), jobs);
        // Persisted maintenance resumes on the first data-plane access, not here: a node that
        // only reports status for a collection it does not serve must never run its jobs.
        if !resume.is_empty() {
            handle.arm_maintenance_resume();
        }
        Ok(Arc::new(handle))
    }

    /// Open the active WAL for appends, truncating a torn tail.
    pub(crate) fn open_active_wal(&self, descriptor: &CollectionDescriptor) -> Result<WalWriter> {
        WalWriter::open(Arc::clone(&self.vfs), Self::active_wal_path(descriptor))
    }

    /// The durable current state: finish an interrupted WAL rotation, then load the manifest
    /// `CURRENT` names and replay the WAL above its checkpoint.
    pub(crate) fn load_current_state(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<(Manifest, DeltaLog)> {
        let current_generation = self.read_current_generation(descriptor)?;
        let manifest = self.load_manifest(descriptor, Some(current_generation))?;
        self.finish_pending_rotation(descriptor, &manifest)?;
        let delta = self.replay_delta(descriptor, manifest.checkpoint_seq_no)?;
        Ok((manifest, delta))
    }

    /// The state as of manifest `generation`, for snapshots of an older generation: that
    /// manifest plus every WAL record above its checkpoint that the WAL files still hold.
    pub(crate) fn load_historical_state(
        &self,
        descriptor: &CollectionDescriptor,
        generation: u64,
    ) -> Result<CollectionState> {
        let manifest = self.load_manifest(descriptor, Some(generation))?;
        let delta = self.replay_delta(descriptor, manifest.checkpoint_seq_no)?;
        Ok(CollectionState {
            manifest: Arc::new(manifest),
            delta,
        })
    }

    fn replay_delta(
        &self,
        descriptor: &CollectionDescriptor,
        checkpoint_seq_no: SeqNo,
    ) -> Result<DeltaLog> {
        let mut records = replay_dir_after_checkpoint(
            self.vfs.as_ref(),
            descriptor.root_path.join("wal"),
            checkpoint_seq_no,
        )?;
        records.sort_by_key(|record| record.seq_no);
        Ok(DeltaLog::from_records(records))
    }

    /// The state a read of `snapshot` runs against, and the resolved snapshot.
    ///
    /// The current manifest generation reads the published `Version` without any file access;
    /// an older generation is loaded from disk until snapshot tokens replace historical reads.
    pub(crate) fn read_state(
        &self,
        handle: &CollectionHandle,
        snapshot: Option<Snapshot>,
    ) -> Result<(CollectionState, Snapshot)> {
        handle.ensure_open()?;
        let version = handle.current();
        let state = match &snapshot {
            Some(snapshot) if snapshot.manifest_generation != version.manifest_generation => {
                self.load_historical_state(handle.descriptor(), snapshot.manifest_generation)?
            }
            _ => version.state(),
        };
        let snapshot = resolve_snapshot(&state, snapshot)?;
        Ok((state, snapshot))
    }

    /// After a job failed part-way (for example between publishing a manifest and rotating the
    /// WAL), the published `Version` may no longer match the files. Rebuild it from the durable
    /// state and publish that; if the reload fails too, poison the collection so it refuses
    /// writes until the engine is reopened. Returns `error` for the caller to report.
    pub(crate) fn reload_after_failure(
        &self,
        handle: &CollectionHandle,
        writer: &mut WriterSlot,
        error: LogPoseError,
    ) -> LogPoseError {
        writer.wal = None;
        let descriptor = handle.descriptor();
        let reloaded = self
            .load_current_state(descriptor)
            .and_then(|(manifest, delta)| Ok((manifest, delta, self.open_active_wal(descriptor)?)));
        match reloaded {
            Ok((manifest, delta, wal)) => {
                writer.wal = Some(wal);
                let next = handle.current().with_state(manifest, delta);
                handle.publish(writer, next);
            }
            Err(reload_error) => handle.poison(format!(
                "{error}; reloading the collection from disk also failed: {reload_error}"
            )),
        }
        error
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
                LogPoseError::corrupt(
                    CorruptionKind::Wal,
                    format!("failed to parse pending WAL rotation marker: {error}"),
                )
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
    Err(LogPoseError::corrupt(
        CorruptionKind::Wal,
        format!(
            "refusing to truncate '{}': pending WAL rotation marker '{}' names checkpoint {checkpoint_seq_no}, \
         but the active WAL holds records up to seq {max_seq_no} that are not checkpointed; \
         these are acknowledged writes, so recovery stopped instead of discarding them. \
         If the manifest checkpoint is correct, remove the marker to replay them",
            active_wal_path.display(),
            marker_path.display(),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CreateCollectionRequest, LocalStorageEngine, StorageEngine,
        test_support::{put, unique_temp_dir, visible_ids},
    };
    use logpose_types::DistanceMetric;
    use std::fs;

    async fn create_documents(engine: &LocalStorageEngine) -> CollectionDescriptor {
        engine
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created")
    }

    #[tokio::test]
    async fn recovery_refuses_to_truncate_uncheckpointed_records_behind_stale_marker() {
        let root = unique_temp_dir("storage-stale-marker-recovery");
        let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
        let descriptor = create_documents(&engine).await;
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
        drop(engine);

        // Simulate a flush whose marker removal was lost before the write above was acknowledged.
        let marker_path = EngineCore::pending_rotation_file_path(&descriptor);
        fs::write(&marker_path, flushed.visible_seq_no.to_string())
            .expect("stale marker should be written");

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
            EngineCore::active_wal_path(&descriptor),
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
        drop(reopened);

        fs::remove_file(&marker_path).expect("operator removes the stale marker");
        let reopened = LocalStorageEngine::new(&root).expect("storage engine should reopen");
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
        let descriptor = create_documents(&engine).await;
        engine
            .write("documents", vec![put("alpha", vec![1.0, 0.0])])
            .await
            .expect("write should succeed");
        let active_path = EngineCore::active_wal_path(&descriptor);
        let unrotated = fs::read(&active_path).expect("active wal should be readable");
        let flushed = engine
            .flush("documents")
            .await
            .expect("flush should succeed");
        drop(engine);

        // Simulate a crash after the manifest was published but before the WAL was rotated.
        fs::write(&active_path, unrotated).expect("active wal should be restored");
        let marker_path = EngineCore::pending_rotation_file_path(&descriptor);
        fs::write(&marker_path, flushed.visible_seq_no.to_string())
            .expect("marker should be written");

        let engine = LocalStorageEngine::new(&root).expect("storage engine should reopen");
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
        let descriptor = create_documents(&engine).await;
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
        drop(engine);

        // Simulate a crash after the manifest was published but before the WAL was rotated.
        let active_path = EngineCore::active_wal_path(&descriptor);
        let rolled_path = EngineCore::rolled_wal_path(&descriptor, flushed.visible_seq_no);
        fs::rename(&rolled_path, &active_path).expect("rotation should be undone");
        let marker_path = EngineCore::pending_rotation_file_path(&descriptor);
        fs::write(&marker_path, flushed.visible_seq_no.to_string())
            .expect("marker should be written");

        let engine = LocalStorageEngine::new(&root).expect("storage engine should reopen");
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
        let descriptor = create_documents(&engine).await;
        engine
            .write("documents", vec![put("alpha", vec![1.0, 0.0])])
            .await
            .expect("write should succeed");
        let active_path = EngineCore::active_wal_path(&descriptor);
        let mut unrotated = fs::read(&active_path).expect("active wal should be readable");
        let flushed = engine
            .flush("documents")
            .await
            .expect("flush should succeed");
        drop(engine);

        // Crash after the manifest was published but before rotation, with a torn frame left
        // by an append that was never acknowledged.
        let torn_frame = unrotated[..unrotated.len() / 2].to_vec();
        unrotated.extend_from_slice(&torn_frame);
        fs::write(&active_path, unrotated).expect("active wal should be restored");
        let marker_path = EngineCore::pending_rotation_file_path(&descriptor);
        fs::write(&marker_path, flushed.visible_seq_no.to_string())
            .expect("marker should be written");

        let engine = LocalStorageEngine::new(&root).expect("storage engine should reopen");
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
