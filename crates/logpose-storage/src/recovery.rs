//! Open and recover: build a collection's first `Version` from its manifest and the WAL above
//! the manifest's checkpoint, and start its writer task.
//!
//! `open_collection` runs, in order:
//!
//! 1. The **durability barrier**: sync the collection directory and its data directories, so
//!    recovery reasons only about what is on disk (the WAL layer syncs `wal/` and its files
//!    inside `WalRecovery::open`, after its fence check).
//! 2. Read `CURRENT` and load the manifest it names; a version 1 layout fails here.
//! 3. **Orphan cleanup** relative to that manifest, before any id can be issued again.
//! 4. WAL recovery: fence check, WAL barrier, tail repair, and replay of every frame above the
//!    checkpoint through the same `apply` the writer uses, starting from the manifest's schema
//!    and following the `SchemaChange` frames in the log, so each batch is read with the schema
//!    that was current at its sequence number. WAL files at or below the checkpoint are then
//!    deleted.
//! 5. `Version` 1 and the writer task, seeded with the manifest's id counters.
//!
//! Every file change before the writer starts (orphan removal, tail truncation, WAL cleanup)
//! removes or adds only bytes no durable state references, and each runs after the barrier, so
//! recovery that crashes at any point and runs again converges to the same state (I11).

use crate::{
    engine::{CoreRef, EngineCore},
    fs_util::read_json,
    gc::{FileHandle, durability_barrier, remove_orphans},
    handle::{CollectionHandle, CollectionMeta},
    maintenance::MaintenanceState,
    manifest::{Manifest, load_manifest, manifest_corrupt, read_current},
    paths::UnitFiles,
    version::{DeltaLog, Version, VersionId},
    writer::{self, LogicalState, WriterSeed, checkpoint_frame, replay_frame},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{CollectionRef, CorruptionKind, LogPoseError, Result, UnitId};
use logpose_wal::{WalRecovery, WalWriter};
use std::{collections::BTreeMap, path::Path, sync::Arc};

/// The outcome of recovering one collection directory at engine open.
pub(crate) enum RecoveredCollection {
    /// Recovered and ready to serve.
    Open(Arc<CollectionHandle>),
    /// The descriptor names the collection, but it cannot be served.
    Failed {
        reference: CollectionRef,
        /// The descriptor, when it is valid.
        descriptor: Option<Box<CollectionDescriptor>>,
        error: String,
    },
    /// The descriptor cannot be parsed, so the collection is not even known by name.
    Unreadable { error: String },
}

/// What a collection's writer starts from besides its WAL and state.
pub(crate) struct DurableStart {
    pub(crate) manifest: Arc<Manifest>,
    /// The manifest generation kept beside the durable one, if any.
    pub(crate) previous_generation: Option<u64>,
    /// The first manifest generation the writer may issue.
    pub(crate) next_manifest_gen: u64,
    /// The first unit id the writer may issue.
    pub(crate) next_unit_id: u32,
}

impl CoreRef {
    /// Recover the collection in `dir`: read its descriptor, placement, and maintenance status,
    /// then recover its files (see the module docs), build `Version` 1, and start the writer.
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
                error: error.to_string(),
            };
        }
        match self.open_collection(descriptor.clone()) {
            Ok(handle) => RecoveredCollection::Open(handle),
            Err(error) => RecoveredCollection::Failed {
                reference,
                descriptor: Some(Box::new(descriptor)),
                error: error.to_string(),
            },
        }
    }

    fn open_collection(&self, descriptor: CollectionDescriptor) -> Result<Arc<CollectionHandle>> {
        let assignment = self.load_collection_assignment(&descriptor)?;
        let persisted = self.load_maintenance_status(&descriptor)?;
        let (jobs, resume, changed) = MaintenanceState::recovered(persisted);
        let durable = self.recover_manifest(&descriptor)?;
        if changed {
            self.persist_maintenance_status(&descriptor, jobs.status())?;
        }
        let (state, mut wal) = self.recover_wal(&descriptor, &durable.manifest)?;
        // Every operation in these files is in a segment of the durable manifest.
        wal.remove_checkpointed(durable.manifest.checkpoint_seq_no)?;
        let meta = Arc::new(CollectionMeta::new(descriptor, assignment));
        let handle = self.start_collection(meta, durable, state, wal, jobs)?;
        // Persisted maintenance resumes on the first data-plane access, not here: a node that
        // only reports status for a collection it does not serve must never run its jobs.
        if !resume.is_empty() {
            handle.arm_maintenance_resume();
        }
        Ok(handle)
    }

    /// The durability barrier, the manifest `CURRENT` names, and orphan cleanup relative to it.
    fn recover_manifest(&self, descriptor: &CollectionDescriptor) -> Result<DurableStart> {
        let dir = &descriptor.root_path;
        let vfs = self.vfs.as_ref();
        durability_barrier(vfs, dir)?;
        let generation = read_current(vfs, dir)?;
        let manifest = load_manifest(vfs, dir, generation)?;
        if manifest.collection_id != descriptor.collection_id {
            return Err(manifest_corrupt(
                &dir.join("CURRENT"),
                format!(
                    "manifest {generation} belongs to collection {}, not {}",
                    manifest.collection_id, descriptor.collection_id
                ),
            ));
        }
        let cleanup = remove_orphans(vfs, dir, &manifest)?;
        if !cleanup.removed.is_empty() {
            tracing::info!(
                collection = %descriptor.lookup_name(),
                removed = cleanup.removed.len(),
                "recovery removed files no durable manifest references"
            );
        }
        Ok(DurableStart {
            manifest: Arc::new(manifest),
            previous_generation: cleanup.previous_generation,
            next_manifest_gen: cleanup.next_manifest_gen,
            next_unit_id: cleanup.next_unit_id,
        })
    }

    /// Publish `Version` 1 over the recovered state and start the writer task.
    pub(crate) fn start_collection(
        &self,
        meta: Arc<CollectionMeta>,
        durable: DurableStart,
        state: LogicalState,
        wal: WalWriter,
        jobs: MaintenanceState,
    ) -> Result<Arc<CollectionHandle>> {
        let DurableStart {
            manifest,
            previous_generation,
            next_manifest_gen,
            next_unit_id,
        } = durable;
        let live_files = manifest
            .units()
            .map(|unit| {
                let files = UnitFiles::new(&meta.dir, unit).published();
                (
                    unit,
                    Arc::new(FileHandle::new(unit, files, self.gc.clone())),
                )
            })
            .collect::<BTreeMap<UnitId, _>>();
        let version = Version::build(
            VersionId(1),
            Arc::clone(&meta),
            Arc::clone(&state.schema),
            Arc::clone(&manifest),
            state.delta.clone(),
            live_files.values().cloned().collect(),
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
        let handle = Arc::new(CollectionHandle::new(
            version,
            channels,
            jobs,
            Arc::clone(&self.tokens),
        ));
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
                live_files,
                previous_generation,
                next_manifest_gen,
                next_unit_id,
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
}

#[cfg(test)]
mod tests;
