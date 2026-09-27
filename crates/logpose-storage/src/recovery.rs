//! Open and recover: build a collection's first `Version` from its manifest and the WAL above
//! the manifest's checkpoint, start its writer task, and load historical states for snapshots
//! of older manifest generations.
//!
//! Recovery replays every WAL frame through the same `apply` the writer uses, starting from the
//! manifest's schema and following the `SchemaChange` frames in the log, so each batch is read
//! with the schema that was current at its sequence number.

use crate::{
    engine::{CoreRef, EngineCore},
    fs_util::read_json,
    handle::{CollectionHandle, CollectionMeta},
    maintenance::MaintenanceState,
    manifest::Manifest,
    state::{CollectionState, resolve_snapshot},
    version::{DeltaLog, Version, VersionId},
    writer::{self, LogicalState, WriterSeed, checkpoint_frame, replay_frame},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{CollectionRef, CorruptionKind, LogPoseError, Result, SeqNo, Snapshot};
use logpose_wal::{WalRecovery, WalWriter, read_committed};
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

impl CoreRef {
    /// Recover the collection in `dir`: read its descriptor, placement, and maintenance status,
    /// load the current manifest, recover the WAL above its checkpoint (fence check, durability
    /// barrier, tail repair, replay), build `Version` 1, and start the writer task.
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
        let manifest = Arc::new(self.load_manifest(&descriptor, None)?);
        let (state, wal) = self.recover_wal(&descriptor, &manifest)?;
        let meta = Arc::new(CollectionMeta::new(descriptor, assignment));
        let handle = self.start_collection(meta, manifest, state, wal, jobs)?;
        // Persisted maintenance resumes on the first data-plane access, not here: a node that
        // only reports status for a collection it does not serve must never run its jobs.
        if !resume.is_empty() {
            handle.arm_maintenance_resume();
        }
        Ok(handle)
    }

    /// Publish `Version` 1 over the recovered state and start the writer task.
    pub(crate) fn start_collection(
        &self,
        meta: Arc<CollectionMeta>,
        manifest: Arc<Manifest>,
        state: LogicalState,
        wal: WalWriter,
        jobs: MaintenanceState,
    ) -> Result<Arc<CollectionHandle>> {
        let version = Version::build(
            VersionId(1),
            Arc::clone(&meta),
            Arc::clone(&state.schema),
            Arc::clone(&manifest),
            state.delta.clone(),
        );
        let next_seq_no = wal.next_seq_no();
        if version.visible_seq_no.checked_add(1) != Some(next_seq_no) {
            return Err(LogPoseError::Corrupt {
                kind: CorruptionKind::Wal,
                location: Some(wal.active_path().display().to_string()),
                message: format!(
                    "the WAL continues at sequence number {next_seq_no}, but the recovered state \
                     ends at {}",
                    version.visible_seq_no
                ),
            });
        }
        let (channels, inbox) = writer::channels(&self.group_commit);
        let handle = Arc::new(CollectionHandle::new(version, channels, jobs));
        writer::spawn(
            self.clone(),
            Arc::clone(&handle),
            inbox,
            WriterSeed {
                wal,
                state,
                manifest,
                next_seq_no,
                version_id: VersionId(1),
            },
        );
        Ok(handle)
    }
}

impl EngineCore {
    /// Recover the WAL of a collection above `manifest`'s checkpoint and return the replayed
    /// state and the writer that continues the log.
    ///
    /// The WAL layer runs the fence check, the durability barrier for `wal/`, and tail repair,
    /// then streams the frames above the checkpoint; each one is applied with the writer's
    /// `apply`. Checkpoint frames are cross-checked against the manifest.
    pub(crate) fn recover_wal(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
    ) -> Result<(LogicalState, WalWriter)> {
        let checkpoint = manifest.checkpoint_seq_no;
        let mut recovery = WalRecovery::open(
            Arc::clone(&self.vfs),
            Self::wal_dir(descriptor),
            self.wal_config(),
            checkpoint,
        )?;
        let mut state = LogicalState {
            schema: Arc::new(manifest.schema.clone()),
            delta: DeltaLog::default(),
        };
        while let Some(frame) = recovery.next_frame()? {
            replay_frame(&mut state, checkpoint, frame)?;
        }
        if let Some(repair) = &recovery.report().tail_repair {
            tracing::warn!(
                collection = %descriptor.lookup_name(),
                file = %repair.file.display(),
                discarded_bytes = repair.original_len - repair.repaired_len,
                discarded_seq = ?repair.discarded_seq,
                "recovery truncated an unacknowledged WAL tail"
            );
        }
        let wal = recovery.into_writer(&checkpoint_frame(manifest)?)?;
        Ok((state, wal))
    }

    /// The state as of manifest `generation` and sequence number `through`, for snapshots of an
    /// older generation: that manifest plus the WAL frames in `checkpoint + 1..=through`, which
    /// must already be durable (published). Reads the live WAL without modifying it.
    pub(crate) fn load_historical_state(
        &self,
        descriptor: &CollectionDescriptor,
        generation: u64,
        through: SeqNo,
    ) -> Result<CollectionState> {
        let manifest = self.load_manifest(descriptor, Some(generation))?;
        let checkpoint = manifest.checkpoint_seq_no;
        let mut state = LogicalState {
            schema: Arc::new(manifest.schema.clone()),
            delta: DeltaLog::default(),
        };
        for frame in read_committed(
            self.vfs.as_ref(),
            &Self::wal_dir(descriptor),
            checkpoint,
            through,
        )? {
            replay_frame(&mut state, checkpoint, frame)?;
        }
        Ok(CollectionState {
            manifest: Arc::new(manifest),
            schema: state.schema,
            delta: state.delta,
        })
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
                if snapshot.visible_seq_no > version.visible_seq_no {
                    return Err(LogPoseError::invalid_field(
                        "snapshot",
                        format!(
                            "invalid snapshot: visible sequence {} exceeds maximum {}",
                            snapshot.visible_seq_no, version.visible_seq_no
                        ),
                    ));
                }
                self.load_historical_state(
                    handle.descriptor(),
                    snapshot.manifest_generation,
                    snapshot.visible_seq_no,
                )?
            }
            _ => version.state(),
        };
        let snapshot = resolve_snapshot(&state, snapshot)?;
        Ok((state, snapshot))
    }
}
