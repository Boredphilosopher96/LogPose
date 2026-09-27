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
    RecordId, Result, Snapshot, VisibleRecord, WriteOperation,
};
use logpose_wal::{WalBatch, WalRecord, WalWriter};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

mod catalog;
mod compaction;
mod durable_fs;
mod error;
#[cfg(test)]
mod failpoints;
mod flush;
mod fs_util;
mod maintenance;
mod manifest;
mod metric;
mod paths;
mod recovery;
mod resolve;
mod root_lock;
mod segment_v1;
mod state;
mod stats;
mod storage_engine;
#[cfg(test)]
mod test_support;
mod wal_rotation;

use durable_fs::{create_dir_all_synced, sync_dir, sync_parent_dir};
use error::{io_message, json_message};
use fs_util::{atomic_write, cleanup_dir, read_json};
use maintenance::MaintenanceOperation;
use manifest::{Manifest, segment_artifact_file_name};
use metric::storage_metric_compare;
use resolve::{ResolvedState, resolve_latest_state_for_ids_selected};
use root_lock::StorageRootLock;
use segment_v1::read_segment_file;
use state::resolve_snapshot;
use wal_rotation::wal_rotation_lock;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::unique_temp_dir;
    use rand as _;
    use std::fs;

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
}
