//! Storage engine abstractions.

#[cfg(test)]
use logpose_query as _;
#[cfg(test)]
use rand as _;

use async_trait::async_trait;
use logpose_catalog::CollectionDescriptor;
use logpose_index::{read_flat_index, read_hnsw_index};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, AnnCandidate, AnnSearchRequest, CollectionAssignment, CollectionRef,
    CollectionStats, CommitAck, LeadershipFence, LogPoseError, MaintenanceStatus, NodeRole,
    PutRecord, RecordId, Result, SeqNo, Snapshot, VisibleRecord, WriteOperation,
};
use logpose_wal::{
    WalBatch, WalFileKind, WalRecord, WalWriter, replay_dir_after_checkpoint, replay_file,
    rotate_active,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    thread,
};

mod catalog;
mod durable_fs;
mod error;
mod fs_util;
mod manifest;
mod metric;
mod paths;
mod resolve;
mod root_lock;
mod segment_v1;
mod state;
mod stats;
mod storage_engine;
#[cfg(test)]
mod test_support;

use durable_fs::{create_dir_all_synced, sync_dir, sync_parent_dir};
use error::{io_message, json_message};
use fs_util::{atomic_write, cleanup_dir, read_json, remove_file_if_exists};
use manifest::{Manifest, segment_artifact_file_name};
use metric::storage_metric_compare;
use resolve::{ResolvedState, resolve_latest_from_segments, resolve_latest_state_for_ids_selected};
use root_lock::StorageRootLock;
use segment_v1::read_segment_file;
use state::{CollectionState, resolve_snapshot};
use stats::approximate_record_bytes;

pub use storage_engine::{
    BlobStore, CreateCollectionRequest, InspectReport, InspectTarget, StorageEngine,
};

/// Local filesystem-backed storage engine.
///
/// Opening an engine claims exclusive ownership of its storage root for this process by locking
/// `<root>/LOCK`; the claim is held until the last clone of every engine on that root in this
/// process is dropped. Engines in the same process share the claim.
#[derive(Clone)]
pub struct LocalStorageEngine {
    root: PathBuf,
    blob_store: Option<Arc<dyn BlobStore>>,
    _root_lock: Arc<StorageRootLock>,
}

impl LocalStorageEngine {
    /// Open a local storage engine rooted at the provided path.
    ///
    /// Creates the root directory if needed and fails if another process holds the root.
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        Self::with_blob_store(root, None)
    }

    /// Open a local storage engine with an optional blob-store implementation.
    ///
    /// Creates the root directory if needed and fails if another process holds the root.
    pub fn with_blob_store(
        root: impl AsRef<Path>,
        blob_store: Option<Arc<dyn BlobStore>>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let root_lock = StorageRootLock::acquire(&root)?;
        Ok(Self {
            root,
            blob_store,
            _root_lock: Arc::new(root_lock),
        })
    }

    /// Build the descriptor that would be persisted for one collection request.
    pub fn plan_collection_descriptor(
        &self,
        request: &CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        let request = request.clone().with_defaults();
        let collection = CollectionRef::new(request.database_name.clone(), request.name.clone());
        if self.find_collection_descriptor_ref(&collection).is_ok() {
            return Err(LogPoseError::Message(format!(
                "collection '{}/{}' already exists",
                collection.database_name, collection.collection_name
            )));
        }

        let descriptor = CollectionDescriptor::new_in_database(
            request.database_name,
            request.name,
            request.dimensions,
            request.metric,
            self.collections_root(),
        );
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// Persist a collection using a previously planned descriptor.
    pub fn create_collection_from_descriptor(
        &self,
        descriptor: CollectionDescriptor,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<CollectionDescriptor> {
        create_dir_all_synced(&self.collections_root())?;
        if self
            .find_collection_descriptor_ref(&descriptor.collection_ref())
            .is_ok()
        {
            return Err(LogPoseError::Message(format!(
                "collection '{}/{}' already exists",
                descriptor.database_name, descriptor.name
            )));
        }

        descriptor.validate()?;
        let result = (|| -> Result<()> {
            self.ensure_database_descriptor(&descriptor.database_name)?;
            self.create_collection_directories(&descriptor)?;
            if let Some(assignment) = assignment {
                self.persist_collection_assignment(&descriptor, assignment)?;
            }
            self.publish_manifest(&descriptor, &Manifest::empty(0))?;
            self.persist_maintenance_status(&descriptor, &MaintenanceStatus::default())?;
            let mut wal_writer = WalWriter::open(Self::active_wal_path(&descriptor))?;
            wal_writer.truncate()?;
            sync_parent_dir(&Self::active_wal_path(&descriptor))?;
            atomic_write(
                &Self::descriptor_path(&descriptor),
                serde_json::to_vec_pretty(&descriptor).map_err(json_message)?,
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(descriptor),
            Err(error) => {
                cleanup_dir(&descriptor.root_path);
                Err(error)
            }
        }
    }

    fn load_collection_assignment(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        let path = Self::placement_file_path(descriptor);
        if !path.exists() {
            return Err(LogPoseError::Message(format!(
                "collection '{}' is missing placement metadata",
                descriptor.name
            )));
        }
        read_json(&path)
    }

    fn persist_collection_assignment(
        &self,
        descriptor: &CollectionDescriptor,
        assignment: &CollectionAssignment,
    ) -> Result<()> {
        atomic_write(
            &Self::placement_file_path(descriptor),
            serde_json::to_vec_pretty(assignment).map_err(json_message)?,
        )
    }

    fn create_collection_internal(
        &self,
        request: CreateCollectionRequest,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<CollectionDescriptor> {
        let descriptor = self.plan_collection_descriptor(&request)?;
        self.create_collection_from_descriptor(descriptor, assignment)
    }

    /// Open a collection descriptor using an explicit database namespace.
    pub async fn open_collection_in_database(
        &self,
        database_name: &str,
        name: &str,
    ) -> Result<CollectionDescriptor> {
        self.find_collection_descriptor_ref(&CollectionRef::new(database_name, name))
    }

    fn load_collection_state(
        &self,
        collection_name: &str,
        manifest_generation: Option<u64>,
    ) -> Result<CollectionState> {
        let descriptor = self.find_collection_descriptor(collection_name)?;
        self.load_collection_state_descriptor(descriptor, manifest_generation)
    }

    fn load_collection_state_descriptor(
        &self,
        descriptor: CollectionDescriptor,
        manifest_generation: Option<u64>,
    ) -> Result<CollectionState> {
        self.load_collection_state_descriptor_inner(descriptor, manifest_generation, false)
    }

    fn load_collection_state_descriptor_with_wal_lock(
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
        let has_pending_rotation = Self::pending_rotation_file_path(&descriptor).exists();
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
            Some(self.load_manifest(&descriptor, Some(current_generation))?)
        } else {
            None
        };
        let mut promoted_delta = Vec::new();
        let manifest = if let Some(current_manifest) = current_manifest.as_ref() {
            if self.clear_checkpointed_active_wal_if_pending(&descriptor, current_manifest)? {
                if target_generation == current_generation {
                    current_manifest.clone()
                } else {
                    let target_manifest =
                        self.load_manifest(&descriptor, Some(target_generation))?;
                    let previous_manifest = self
                        .load_manifest(&descriptor, Some(current_generation.saturating_sub(1)))?;
                    if !Self::rolled_wal_path(&descriptor, current_manifest.checkpoint_seq_no)
                        .exists()
                    {
                        promoted_delta = self.pending_rotation_promoted_delta(
                            &descriptor,
                            &previous_manifest,
                            current_manifest,
                        )?;
                    }
                    target_manifest
                }
            } else {
                self.load_manifest(&descriptor, Some(target_generation))?
            }
        } else {
            self.load_manifest(&descriptor, Some(target_generation))?
        };
        let delta = replay_dir_after_checkpoint(
            descriptor.root_path.join("wal"),
            manifest.checkpoint_seq_no,
        )?
        .into_iter()
        .chain(promoted_delta)
        .collect::<Vec<_>>();
        let mut delta = delta;
        delta.sort_by_key(|record| record.seq_no);

        Ok(CollectionState {
            descriptor,
            manifest,
            delta,
        })
    }

    fn pending_rotation_promoted_delta(
        &self,
        descriptor: &CollectionDescriptor,
        previous_manifest: &Manifest,
        current_manifest: &Manifest,
    ) -> Result<Vec<WalRecord>> {
        let known_segment_ids = previous_manifest
            .segments
            .iter()
            .map(|segment| segment.segment_id.as_str())
            .collect::<BTreeSet<_>>();
        let mut promoted = Vec::new();

        for segment in &current_manifest.segments {
            if known_segment_ids.contains(segment.segment_id.as_str()) {
                continue;
            }

            promoted.extend(read_segment_file(
                &descriptor
                    .root_path
                    .join("segments")
                    .join(&segment.file_name),
            )?);
        }

        Ok(promoted)
    }

    fn clear_checkpointed_active_wal_if_pending(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
    ) -> Result<bool> {
        let marker_path = Self::pending_rotation_file_path(descriptor);
        if !marker_path.exists() {
            return Ok(false);
        }

        let pending_checkpoint = fs::read_to_string(&marker_path)
            .map_err(|error| io_message("failed to read pending WAL rotation marker", error))?
            .trim()
            .parse::<u64>()
            .map_err(|error| {
                LogPoseError::Message(format!(
                    "failed to parse pending WAL rotation marker: {error}"
                ))
            })?;

        if pending_checkpoint == manifest.checkpoint_seq_no {
            let active_wal_path = Self::active_wal_path(descriptor);
            ensure_active_wal_is_checkpointed(&active_wal_path, &marker_path, pending_checkpoint)?;
            let mut wal_writer = WalWriter::open(&active_wal_path)?;
            wal_writer.truncate()?;
            Self::clear_pending_rotation_marker(descriptor)?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Remove the pending-rotation marker and make the removal durable.
    fn clear_pending_rotation_marker(descriptor: &CollectionDescriptor) -> Result<()> {
        let marker_path = Self::pending_rotation_file_path(descriptor);
        #[cfg(test)]
        failpoints::check_marker_removal(&marker_path)?;
        remove_file_if_exists(&marker_path, "failed to clear pending WAL rotation marker")?;
        sync_parent_dir(&marker_path)
    }

    fn create_collection_directories(&self, descriptor: &CollectionDescriptor) -> Result<()> {
        create_dir_all_synced(&descriptor.root_path)?;
        fs::create_dir_all(descriptor.root_path.join("manifests"))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("wal")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("segments")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("indexes")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("tmp")))
            .map_err(|error| io_message("failed to create collection directories", error))?;
        sync_dir(&descriptor.root_path)
    }

    fn find_collection_descriptor(&self, name: &str) -> Result<CollectionDescriptor> {
        self.find_collection_descriptor_ref(&Self::collection_ref_from_lookup(name))
    }

    fn find_collection_descriptor_ref(
        &self,
        collection: &CollectionRef,
    ) -> Result<CollectionDescriptor> {
        let collections_root = self.collections_root();
        if !collections_root.exists() {
            return Err(LogPoseError::Message(format!(
                "collection '{}/{}' does not exist",
                collection.database_name, collection.collection_name
            )));
        }

        for entry in fs::read_dir(&collections_root)
            .map_err(|error| io_message("failed to list collections root", error))?
        {
            let entry =
                entry.map_err(|error| io_message("failed to read collection entry", error))?;
            let path = entry.path().join("descriptor.json");
            if !path.exists() {
                continue;
            }

            let descriptor = read_json::<CollectionDescriptor>(&path)?;
            if descriptor.database_name == collection.database_name
                && descriptor.name == collection.collection_name
            {
                descriptor.validate()?;
                return Ok(descriptor);
            }
        }

        Err(LogPoseError::Message(format!(
            "collection '{}/{}' does not exist",
            collection.database_name, collection.collection_name
        )))
    }

    fn list_collection_descriptors(&self) -> Result<Vec<CollectionDescriptor>> {
        let collections_root = self.collections_root();
        if !collections_root.exists() {
            return Ok(Vec::new());
        }

        let mut descriptors = Vec::new();
        for entry in fs::read_dir(&collections_root)
            .map_err(|error| io_message("failed to list collections root", error))?
        {
            let entry =
                entry.map_err(|error| io_message("failed to read collection entry", error))?;
            let path = entry.path().join("descriptor.json");
            if !path.exists() {
                continue;
            }

            let descriptor = read_json::<CollectionDescriptor>(&path)?;
            descriptor.validate()?;
            descriptors.push(descriptor);
        }

        descriptors.sort_by(|left, right| {
            (&left.database_name, &left.name).cmp(&(&right.database_name, &right.name))
        });
        Ok(descriptors)
    }

    fn collection_ref_from_lookup(name: &str) -> CollectionRef {
        let parts = name.split('/').collect::<Vec<_>>();
        if parts.len() == 2 && parts.iter().all(|part| !part.trim().is_empty()) {
            CollectionRef::new(parts[0], parts[1])
        } else {
            CollectionRef::new_default(name)
        }
    }

    fn should_flush(&self, descriptor: &CollectionDescriptor, delta: &[WalRecord]) -> bool {
        if delta.len() >= descriptor.flush_threshold_ops {
            return true;
        }

        let approx_bytes = delta
            .iter()
            .map(|record| approximate_record_bytes(&record.op))
            .sum::<usize>();
        approx_bytes >= descriptor.flush_threshold_bytes
    }

    fn should_compact(&self, descriptor: &CollectionDescriptor, segment_count: usize) -> bool {
        segment_count >= descriptor.compaction_threshold_segments
    }

    fn load_maintenance_status(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        let path = Self::maintenance_file_path(descriptor);
        if !path.exists() {
            return Ok(MaintenanceStatus::default());
        }
        read_json(&path)
    }

    fn persist_maintenance_status(
        &self,
        descriptor: &CollectionDescriptor,
        status: &MaintenanceStatus,
    ) -> Result<()> {
        atomic_write(
            &Self::maintenance_file_path(descriptor),
            serde_json::to_vec_pretty(status).map_err(json_message)?,
        )
    }

    fn enqueue_maintenance(
        &self,
        descriptor: &CollectionDescriptor,
        operations: Vec<MaintenanceOperation>,
    ) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }

        let status_lock = maintenance_status_lock(&descriptor.root_path);
        {
            let _guard = status_lock
                .lock()
                .expect("maintenance status lock should not be poisoned");
            let mut persisted = self.load_maintenance_status(descriptor)?;
            for operation in &operations {
                let label = operation.as_str().to_owned();
                if persisted.in_progress.as_deref() == Some(label.as_str())
                    || persisted.pending.iter().any(|pending| pending == &label)
                {
                    continue;
                }
                persisted.pending.push(label);
            }
            self.persist_maintenance_status(descriptor, &persisted)?;
        }

        let key = descriptor.root_path.clone();
        let should_spawn = {
            let mut coordinator = maintenance_coordinator()
                .lock()
                .expect("maintenance coordinator lock should not be poisoned");
            let state = coordinator.entry(key.clone()).or_default();
            for operation in operations {
                if !state.queue.iter().any(|pending| pending == &operation) {
                    state.queue.push_back(operation);
                }
            }
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if should_spawn {
            let engine = self.clone();
            let collection_name = descriptor.lookup_name();
            thread::spawn(move || engine.run_maintenance_worker(collection_name, key));
        }
        Ok(())
    }

    fn recover_persisted_maintenance(&self, descriptor: &CollectionDescriptor) -> Result<()> {
        let key = descriptor.root_path.clone();
        {
            let coordinator = maintenance_coordinator()
                .lock()
                .expect("maintenance coordinator lock should not be poisoned");
            if coordinator.contains_key(&key) {
                return Ok(());
            }
        }

        let status_lock = maintenance_status_lock(&descriptor.root_path);
        let operations = {
            let _guard = status_lock
                .lock()
                .expect("maintenance status lock should not be poisoned");
            let mut status = self.load_maintenance_status(descriptor)?;
            let mut needs_persist = false;
            if let Some(in_progress) = status.in_progress.take() {
                if !status.pending.iter().any(|pending| pending == &in_progress) {
                    status.pending.insert(0, in_progress);
                }
                needs_persist = true;
            }
            let operations = status
                .pending
                .iter()
                .filter_map(|label| MaintenanceOperation::from_str(label))
                .collect::<Vec<_>>();
            if needs_persist {
                self.persist_maintenance_status(descriptor, &status)?;
            }
            operations
        };

        if operations.is_empty() {
            return Ok(());
        }

        self.enqueue_maintenance(descriptor, operations)
    }

    fn run_maintenance_worker(self, collection_name: String, coordinator_key: PathBuf) {
        loop {
            let operation = {
                let mut coordinator = maintenance_coordinator()
                    .lock()
                    .expect("maintenance coordinator lock should not be poisoned");
                let Some(state) = coordinator.get_mut(&coordinator_key) else {
                    return;
                };
                match state.queue.pop_front() {
                    Some(operation) => operation,
                    None => {
                        coordinator.remove(&coordinator_key);
                        return;
                    }
                }
            };

            let descriptor = match self.find_collection_descriptor(&collection_name) {
                Ok(descriptor) => descriptor,
                Err(_) => {
                    clear_maintenance_runtime_state(&coordinator_key);
                    return;
                }
            };

            let status_lock = maintenance_status_lock(&descriptor.root_path);
            if let Ok(_guard) = status_lock.lock()
                && let Ok(mut status) = self.load_maintenance_status(&descriptor)
            {
                let label = operation.as_str().to_owned();
                status.pending.retain(|pending| pending != &label);
                status.in_progress = Some(label);
                let _ = self.persist_maintenance_status(&descriptor, &status);
            }

            let result = self.perform_maintenance_operation(&collection_name, operation);

            let follow_up_operations = if result.is_ok() {
                self.load_collection_state(&collection_name, None)
                    .ok()
                    .map(|state| {
                        let mut operations = Vec::new();
                        if self.should_flush(&state.descriptor, &state.delta) {
                            operations.push(MaintenanceOperation::Flush);
                        }
                        if self.should_compact(&state.descriptor, state.manifest.segments.len()) {
                            operations.push(MaintenanceOperation::Compact);
                        }
                        operations
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            if let Ok(_guard) = status_lock.lock()
                && let Ok(mut status) = self.load_maintenance_status(&descriptor)
            {
                status.in_progress = None;
                match result {
                    Ok(_) => {
                        status.completed_runs += 1;
                        status.last_error = None;
                    }
                    Err(error) => {
                        status.last_error = Some(error.to_string());
                    }
                }
                let _ = self.persist_maintenance_status(&descriptor, &status);
            }

            if !follow_up_operations.is_empty() {
                let _ = self.enqueue_maintenance(&descriptor, follow_up_operations);
            }
        }
    }

    fn perform_maintenance_operation(
        &self,
        collection_name: &str,
        operation: MaintenanceOperation,
    ) -> Result<Snapshot> {
        let descriptor = self.find_collection_descriptor(collection_name)?;
        let manifest_lock = maintenance_operation_lock(&descriptor.root_path);
        match operation {
            MaintenanceOperation::Flush => {
                let _manifest_guard = manifest_lock
                    .lock()
                    .expect("maintenance operation lock should not be poisoned");
                let wal_lock = wal_rotation_lock(&descriptor.root_path);
                let _wal_guard = wal_lock
                    .lock()
                    .expect("wal rotation lock should not be poisoned");
                let state =
                    self.load_collection_state_descriptor_with_wal_lock(descriptor, None)?;
                self.flush_state(state)
            }
            MaintenanceOperation::Compact => {
                let _manifest_guard = manifest_lock
                    .lock()
                    .expect("maintenance operation lock should not be poisoned");
                let state = self.load_collection_state_descriptor(descriptor, None)?;
                self.compact_state(state)
            }
        }
    }

    fn flush_state(&self, state: CollectionState) -> Result<Snapshot> {
        if state.delta.is_empty() {
            return Ok(Snapshot {
                manifest_generation: state.manifest.generation,
                visible_seq_no: state.visible_seq_no(),
            });
        }

        let segment_records = state.delta.clone();
        let new_segment = self.write_segment_file(&state.descriptor, &segment_records)?;
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
            &Self::pending_rotation_file_path(&state.descriptor),
            checkpoint_seq_no.to_string().into_bytes(),
        )?;
        self.publish_manifest(&state.descriptor, &next_manifest)?;

        let rolled_path = state
            .descriptor
            .root_path
            .join("wal")
            .join(format!("{checkpoint_seq_no:020}.wal"));
        rotate_active(Self::active_wal_path(&state.descriptor), rolled_path)?;
        // A surviving marker makes the next load treat `active.wal` as checkpointed, so the
        // flush must not report success unless the marker is durably gone.
        Self::clear_pending_rotation_marker(&state.descriptor)?;

        Ok(Snapshot {
            manifest_generation: next_manifest.generation,
            visible_seq_no: checkpoint_seq_no,
        })
    }

    fn compact_state(&self, state: CollectionState) -> Result<Snapshot> {
        if state.manifest.segments.len() <= 1 {
            return Ok(Snapshot {
                manifest_generation: state.manifest.generation,
                visible_seq_no: state.visible_seq_no(),
            });
        }

        let resolved = resolve_latest_from_segments(&state.descriptor, &state.manifest)?;
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

        let replacement = self.write_segment_file(&state.descriptor, &compacted_records)?;
        let next_manifest = Manifest {
            generation: state.manifest.generation + 1,
            checkpoint_seq_no: state.manifest.checkpoint_seq_no,
            segments: vec![replacement],
        };
        self.publish_manifest(&state.descriptor, &next_manifest)?;

        Ok(Snapshot {
            manifest_generation: next_manifest.generation,
            visible_seq_no: state.visible_seq_no(),
        })
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
    active_wal_path: &Path,
    marker_path: &Path,
    checkpoint_seq_no: SeqNo,
) -> Result<()> {
    let batches = replay_file(active_wal_path, WalFileKind::Active)?;
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

#[async_trait]
impl StorageEngine for LocalStorageEngine {
    async fn engine_name(&self) -> &'static str {
        "local"
    }

    async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        self.create_collection_internal(
            request,
            Some(&CollectionAssignment {
                assigned_node: ANONYMOUS_LOCAL_NODE_NAME.to_owned(),
                assigned_role: NodeRole::Data,
            }),
        )
    }

    async fn create_collection_with_assignment(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
        _leader_fence: Option<LeadershipFence>,
    ) -> Result<CollectionDescriptor> {
        self.create_collection_internal(request, Some(&assignment))
    }

    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor> {
        self.find_collection_descriptor(name)
    }

    async fn has_local_collection(&self, name: &str) -> Result<bool> {
        Ok(self.find_collection_descriptor(name).is_ok())
    }

    async fn local_collection_matches_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<bool> {
        match self.find_collection_descriptor(&descriptor.lookup_name()) {
            Ok(local_descriptor) => Ok(local_descriptor.matches_serving_identity(descriptor)),
            Err(error) if error.to_string().contains("does not exist") => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn list_collections(&self) -> Result<Vec<CollectionDescriptor>> {
        self.list_collection_descriptors()
    }

    async fn collection_assignment_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        Ok(self.load_collection_assignment(descriptor)?)
    }

    async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        if operations.is_empty() {
            return Err(LogPoseError::Message(
                "write batch must include at least one operation".to_owned(),
            ));
        }

        let descriptor = self.find_collection_descriptor(collection_name)?;
        let wal_lock = wal_rotation_lock(&descriptor.root_path);
        let _guard = wal_lock
            .lock()
            .expect("wal rotation lock should not be poisoned");
        let state = self.load_collection_state_descriptor_with_wal_lock(descriptor, None)?;
        let existing_max = state.visible_seq_no();
        let mut seen_ids = BTreeMap::<RecordId, ()>::new();
        for operation in &operations {
            state.descriptor.validate_operation(operation)?;
            if seen_ids.insert(operation.id().clone(), ()).is_some() {
                return Err(LogPoseError::Message(format!(
                    "write batch includes duplicate record id '{}'",
                    operation.id()
                )));
            }
        }

        // The whole batch is one WAL frame with one fsync, so replay sees all of it or none.
        let applied_ops = operations.len();
        let batch = WalBatch::new(
            operations
                .into_iter()
                .zip(existing_max + 1..)
                .map(|(op, seq_no)| WalRecord { seq_no, op })
                .collect(),
        )?;
        let last_seq_no = batch.last_seq_no();
        let mut wal_writer = WalWriter::open(Self::active_wal_path(&state.descriptor))?;
        wal_writer.append_batch(&batch)?;
        let mut delta_after_write = state.delta.clone();
        delta_after_write.extend(batch.into_records());

        if self.should_flush(&state.descriptor, &delta_after_write) {
            self.enqueue_maintenance(&state.descriptor, vec![MaintenanceOperation::Flush])?;
        } else if self.should_compact(&state.descriptor, state.manifest.segments.len()) {
            self.enqueue_maintenance(&state.descriptor, vec![MaintenanceOperation::Compact])?;
        }

        Ok(CommitAck {
            last_seq_no,
            applied_ops,
            snapshot: Snapshot {
                manifest_generation: state.manifest.generation,
                visible_seq_no: last_seq_no,
            },
        })
    }

    async fn snapshot(&self, collection_name: &str) -> Result<Snapshot> {
        let state = self.load_collection_state(collection_name, None)?;
        Ok(Snapshot {
            manifest_generation: state.manifest.generation,
            visible_seq_no: state.visible_seq_no(),
        })
    }

    async fn scan_exact(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<VisibleRecord>> {
        self.scan_exact_internal(collection_name, snapshot, true, None)
    }

    async fn scan_exact_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        self.scan_exact_internal(
            collection_name,
            snapshot,
            include_mutable,
            Some(immutable_unit_ids.into_iter().collect()),
        )
    }

    async fn ann_search_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        immutable_unit_ids: Vec<String>,
        request: AnnSearchRequest,
        filter: Option<Arc<dyn for<'a> Fn(&'a Value) -> bool + Send + Sync>>,
    ) -> Result<Vec<AnnCandidate>> {
        let state = self.load_collection_state(
            collection_name,
            snapshot.as_ref().map(|value| value.manifest_generation),
        )?;
        let snapshot = resolve_snapshot(&state, snapshot)?;
        let metric = state.descriptor.metric;
        let selected = immutable_unit_ids.into_iter().collect::<BTreeSet<_>>();
        let mut candidates_by_record_id = BTreeMap::<RecordId, AnnCandidate>::new();
        let request_budget = request.candidate_budget.max(request.top_k);

        for segment in state
            .manifest
            .segments
            .iter()
            .rev()
            .filter(|segment| selected.contains(&segment.segment_id))
        {
            let hnsw_path = state.descriptor.root_path.join("indexes").join(
                segment_artifact_file_name(segment, "hnsw").ok_or_else(|| {
                    LogPoseError::Message(format!(
                        "segment '{}' is missing hnsw artifact metadata",
                        segment.segment_id
                    ))
                })?,
            );
            let hnsw = read_hnsw_index(&hnsw_path)
                .map_err(|error| io_message("failed to read hnsw sidecar", error))?;
            let search = logpose_index::search_hnsw(
                &hnsw,
                &request.vector,
                request_budget,
                filter.as_deref(),
            )
            .map_err(|error| io_message("failed to search hnsw sidecar", error))?;
            for candidate in search
                .candidates
                .into_iter()
                .filter(|candidate| candidate.seq_no <= snapshot.visible_seq_no)
                .map(|candidate| AnnCandidate {
                    unit_id: segment.segment_id.clone(),
                    record_id: candidate.record_id,
                    seq_no: candidate.seq_no,
                    value: candidate.value,
                })
            {
                match candidates_by_record_id.entry(candidate.record_id.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(candidate);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if candidate.seq_no > entry.get().seq_no {
                            entry.insert(candidate);
                        }
                    }
                }
            }
        }

        let mut candidates = candidates_by_record_id.into_values().collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            storage_metric_compare(metric, right.value, left.value)
                .then(right.seq_no.cmp(&left.seq_no))
                .then(left.record_id.cmp(&right.record_id))
                .then(left.unit_id.cmp(&right.unit_id))
        });
        candidates.truncate(request_budget);

        Ok(candidates)
    }

    async fn latest_visible_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        record_ids: Vec<RecordId>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        let state = self.load_collection_state(
            collection_name,
            snapshot.as_ref().map(|value| value.manifest_generation),
        )?;
        let snapshot = resolve_snapshot(&state, snapshot)?;
        let resolved = resolve_latest_state_for_ids_selected(
            &state,
            snapshot.visible_seq_no,
            &record_ids.into_iter().collect(),
            include_mutable,
            Some(immutable_unit_ids.into_iter().collect()),
        )?;
        let mut records = resolved
            .into_values()
            .filter_map(|state| match state {
                ResolvedState::Visible(record) => Some(record),
                ResolvedState::Deleted { .. } => None,
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(records)
    }

    async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        self.perform_maintenance_operation(collection_name, MaintenanceOperation::Flush)
    }

    async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        self.perform_maintenance_operation(collection_name, MaintenanceOperation::Compact)
    }

    async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        self.stats_snapshot(collection_name, None).await
    }

    async fn stats_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let state = self.load_collection_state_descriptor(
            descriptor.clone(),
            snapshot.as_ref().map(|value| value.manifest_generation),
        )?;
        self.collection_stats_from_state(state, snapshot)
    }

    async fn maintenance_status_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        self.load_maintenance_status(descriptor)
    }

    async fn recover_maintenance_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<()> {
        self.recover_persisted_maintenance(descriptor)
    }

    async fn stats_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let state = self.load_collection_state(
            collection_name,
            snapshot.as_ref().map(|value| value.manifest_generation),
        )?;
        self.collection_stats_from_state(state, snapshot)
    }

    async fn inspect(&self, collection_name: &str, target: InspectTarget) -> Result<InspectReport> {
        let state = self.load_collection_state(collection_name, None)?;
        match target {
            InspectTarget::Manifest => Ok(InspectReport {
                target: "manifest".to_owned(),
                payload: serde_json::to_value(&state.manifest).map_err(json_message)?,
            }),
            InspectTarget::Wal => Ok(InspectReport {
                target: "wal".to_owned(),
                payload: json!({
                    "checkpoint_seq_no": state.manifest.checkpoint_seq_no,
                    "records": state.delta,
                }),
            }),
            InspectTarget::Maintenance => Ok(InspectReport {
                target: "maintenance".to_owned(),
                payload: serde_json::to_value(self.load_maintenance_status(&state.descriptor)?)
                    .map_err(json_message)?,
            }),
            InspectTarget::Segment(segment_id) => {
                let segment = state
                    .manifest
                    .segments
                    .iter()
                    .find(|segment| segment.segment_id == segment_id)
                    .ok_or_else(|| {
                        LogPoseError::Message(format!("segment '{segment_id}' does not exist"))
                    })?;
                let records = read_segment_file(
                    &state
                        .descriptor
                        .root_path
                        .join("segments")
                        .join(&segment.file_name),
                )?;
                let index = read_flat_index(&state.descriptor.root_path.join("indexes").join(
                    segment_artifact_file_name(segment, "flat_exact").ok_or_else(|| {
                        LogPoseError::Message(format!(
                            "segment '{}' is missing flat artifact metadata",
                            segment.segment_id
                        ))
                    })?,
                ))
                .map_err(|error| io_message("failed to read flat index sidecar", error))?;
                let hnsw = read_hnsw_index(&state.descriptor.root_path.join("indexes").join(
                    segment_artifact_file_name(segment, "hnsw").ok_or_else(|| {
                        LogPoseError::Message(format!(
                            "segment '{}' is missing hnsw artifact metadata",
                            segment.segment_id
                        ))
                    })?,
                ))
                .map_err(|error| io_message("failed to read hnsw sidecar", error))?;
                Ok(InspectReport {
                    target: format!("segment:{segment_id}"),
                    payload: json!({
                        "segment": segment,
                        "artifacts": segment.artifacts,
                        "flat_index": index,
                        "hnsw_index": {
                            "index_kind": hnsw.index_kind.as_str(),
                            "dimensions": hnsw.dimensions,
                            "entry_point": hnsw.entry_point,
                            "max_level": hnsw.max_level,
                            "node_count": hnsw.nodes.len(),
                            "params": {
                                "max_neighbors": hnsw.params.max_neighbors,
                                "ef_construction": hnsw.params.ef_construction,
                                "ef_search": hnsw.params.ef_search,
                            },
                        },
                        "records": records,
                    }),
                })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceOperation {
    Flush,
    Compact,
}

impl MaintenanceOperation {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Flush => "flush",
            Self::Compact => "compact",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "flush" => Some(Self::Flush),
            "compact" => Some(Self::Compact),
            _ => None,
        }
    }
}

#[derive(Default)]
struct RuntimeMaintenanceState {
    running: bool,
    queue: VecDeque<MaintenanceOperation>,
}

fn maintenance_coordinator() -> &'static Mutex<BTreeMap<PathBuf, RuntimeMaintenanceState>> {
    static COORDINATOR: OnceLock<Mutex<BTreeMap<PathBuf, RuntimeMaintenanceState>>> =
        OnceLock::new();
    COORDINATOR.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn clear_maintenance_runtime_state(path: &Path) {
    let mut coordinator = maintenance_coordinator()
        .lock()
        .expect("maintenance coordinator lock should not be poisoned");
    coordinator.remove(path);
}

fn maintenance_operation_locks() -> &'static Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn maintenance_operation_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = maintenance_operation_locks()
        .lock()
        .expect("maintenance operation lock map should not be poisoned");
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn wal_rotation_locks() -> &'static Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn wal_rotation_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = wal_rotation_locks()
        .lock()
        .expect("wal rotation lock map should not be poisoned");
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn maintenance_status_locks() -> &'static Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn maintenance_status_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = maintenance_status_locks()
        .lock()
        .expect("maintenance status lock map should not be poisoned");
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Test-only fault injection for failures that cannot be provoked through the filesystem when
/// tests run with elevated privileges.
#[cfg(test)]
mod failpoints {
    use logpose_types::{LogPoseError, Result};
    use std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
        sync::{Mutex, OnceLock, PoisonError},
    };

    fn failing_marker_removals() -> &'static Mutex<BTreeSet<PathBuf>> {
        static PATHS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();
        PATHS.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    /// Make every removal of `marker_path` fail until [`clear_marker_removal_failure`] runs.
    pub(crate) fn fail_marker_removal(marker_path: &Path) {
        failing_marker_removals()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(marker_path.to_path_buf());
    }

    pub(crate) fn clear_marker_removal_failure(marker_path: &Path) {
        failing_marker_removals()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(marker_path);
    }

    pub(crate) fn check_marker_removal(marker_path: &Path) -> Result<()> {
        if failing_marker_removals()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(marker_path)
        {
            return Err(LogPoseError::Message(format!(
                "failed to clear pending WAL rotation marker '{}': injected failure",
                marker_path.display()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{put, unique_temp_dir, visible_ids};
    use logpose_types::DistanceMetric;
    use rand as _;
    use std::fs;

    #[test]
    fn maintenance_worker_clears_coordinator_on_descriptor_lookup_failure() {
        let root = unique_temp_dir("storage-maintenance-descriptor-failure");
        let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
        let coordinator_key = root.join("collections").join("missing-collection");

        {
            let mut coordinator = maintenance_coordinator()
                .lock()
                .expect("maintenance coordinator lock should not be poisoned");
            coordinator.insert(
                coordinator_key.clone(),
                RuntimeMaintenanceState {
                    running: true,
                    queue: VecDeque::from([MaintenanceOperation::Flush]),
                },
            );
        }

        engine.run_maintenance_worker("missing".to_owned(), coordinator_key.clone());

        let coordinator = maintenance_coordinator()
            .lock()
            .expect("maintenance coordinator lock should not be poisoned");
        assert!(
            !coordinator.contains_key(&coordinator_key),
            "descriptor lookup failure should clear runtime coordinator state"
        );
    }

    #[test]
    fn engine_open_fails_while_another_process_holds_the_storage_root() {
        let root = unique_temp_dir("storage-root-lock");
        let first = LocalStorageEngine::new(&root).expect("first engine should open");
        let second = LocalStorageEngine::new(&root).expect("in-process engines share the root");
        drop(first);
        drop(second);

        // An independent handle on LOCK is what another process looks like to the OS.
        let foreign = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join("LOCK"))
            .expect("engine should have created the lock file");
        foreign
            .try_lock()
            .expect("the root should be released once every engine is dropped");

        let error = LocalStorageEngine::new(&root)
            .err()
            .expect("engine must not open a root held by another process");
        assert!(
            error
                .to_string()
                .contains("is already in use by another process"),
            "unexpected error: {error}"
        );

        drop(foreign);
        LocalStorageEngine::new(&root).expect("engine should open after the holder exits");
    }

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
            replay_file(&active_path, WalFileKind::Active)
                .expect("active wal should be readable")
                .is_empty(),
            "checkpointed records should be truncated"
        );
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
