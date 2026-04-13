//! Storage engine abstractions.

#[cfg(test)]
use logpose_query as _;
#[cfg(test)]
use rand as _;

use async_trait::async_trait;
use crc32fast::hash;
use logpose_catalog::CollectionDescriptor;
use logpose_index::{
    FlatIndexEntrySource, FlatIndexSidecar, HnswBuildParams, HnswIndexEntrySource,
    HnswIndexSidecar, build_flat_index, build_hnsw_index, read_flat_index, read_hnsw_index,
    write_flat_index, write_hnsw_index,
};
use logpose_types::{
    AnnCandidate, AnnSearchRequest, CollectionStats, CommitAck, DistanceMetric, LogPoseError,
    MaintenanceStatus, PutRecord, QueryUnitArtifactStats, QueryUnitStats, RecordId, Result,
    ScalarFieldStats, SeqNo, Snapshot, VisibleRecord, WriteOperation,
};
use logpose_wal::{WalRecord, WalWriter, replay_dir, rotate_active};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File},
    io,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
};
use uuid::Uuid;

/// Durable storage surface for future engine implementations.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Return a short identifier for the engine implementation.
    async fn engine_name(&self) -> &'static str;

    /// Create a new collection rooted under the engine storage path.
    async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<CollectionDescriptor>;

    /// Open an existing collection by name.
    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor>;

    /// Persist one or more write operations durably.
    async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck>;

    /// Capture the current manifest generation and visible sequence boundary.
    async fn snapshot(&self, collection_name: &str) -> Result<Snapshot>;

    /// Resolve the currently visible records using exact scan semantics.
    async fn scan_exact(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<VisibleRecord>>;

    /// Resolve visible records for an explicit subset of mutable and immutable units.
    async fn scan_exact_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        let _ = include_mutable;
        let _ = immutable_unit_ids;
        self.scan_exact(collection_name, snapshot).await
    }

    /// Search immutable ANN-capable units for candidate ids before latest-visible resolution.
    async fn ann_search_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        immutable_unit_ids: Vec<String>,
        request: AnnSearchRequest,
        filter: Option<Arc<dyn for<'a> Fn(&'a Value) -> bool + Send + Sync>>,
    ) -> Result<Vec<AnnCandidate>> {
        let descriptor = self.open_collection(collection_name).await?;
        let records = self
            .scan_exact_selected(collection_name, snapshot, false, immutable_unit_ids)
            .await?;
        let filtered_records = if let Some(predicate) = filter.as_ref() {
            let mut filtered_records = Vec::new();
            for record in records {
                if predicate.as_ref()(&record.metadata) {
                    filtered_records.push(record);
                }
            }
            filtered_records
        } else {
            records
        };
        let mut scored = filtered_records
            .into_iter()
            .map(|record| {
                storage_metric_value(descriptor.metric, &request.vector, &record.vector)
                    .map(|value| (record, value))
            })
            .collect::<Result<Vec<_>>>()?;
        scored.sort_by(|(left_record, left_value), (right_record, right_value)| {
            storage_metric_compare(descriptor.metric, *right_value, *left_value)
                .then(left_record.id.cmp(&right_record.id))
        });
        scored.truncate(request.candidate_budget.max(request.top_k));
        Ok(scored
            .into_iter()
            .map(|(record, value)| AnnCandidate {
                unit_id: "exact-fallback".to_owned(),
                record_id: record.id,
                seq_no: record.seq_no,
                value,
            })
            .collect())
    }

    /// Resolve latest visible records for a focused set of ids across selected units.
    async fn latest_visible_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        record_ids: Vec<RecordId>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        let wanted = record_ids.into_iter().collect::<BTreeSet<_>>();
        let records = self
            .scan_exact_selected(
                collection_name,
                snapshot,
                include_mutable,
                immutable_unit_ids,
            )
            .await?;
        Ok(records
            .into_iter()
            .filter(|record| wanted.contains(&record.id))
            .collect())
    }

    /// Flush the mutable delta into a new immutable segment.
    async fn flush(&self, collection_name: &str) -> Result<Snapshot>;

    /// Compact immutable segments into a single replacement segment.
    async fn compact(&self, collection_name: &str) -> Result<Snapshot>;

    /// Return collection-level visibility and storage statistics.
    async fn stats(&self, collection_name: &str) -> Result<CollectionStats>;

    /// Return collection-level statistics for a specific read snapshot.
    async fn stats_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let _ = snapshot;
        self.stats(collection_name).await
    }

    /// Inspect on-disk storage state for operator debugging.
    async fn inspect(&self, collection_name: &str, target: InspectTarget) -> Result<InspectReport>;
}

/// Request payload for creating a collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateCollectionRequest {
    /// Human-readable collection name.
    pub name: String,
    /// Fixed embedding dimensionality.
    pub dimensions: usize,
    /// Distance metric reserved for future query layers.
    pub metric: DistanceMetric,
}

/// Target to inspect from the local storage layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InspectTarget {
    /// Inspect the active manifest.
    Manifest,
    /// Inspect WAL records that remain above the current checkpoint.
    Wal,
    /// Inspect persisted maintenance state.
    Maintenance,
    /// Inspect a specific immutable segment by segment id.
    Segment(String),
}

/// JSON-friendly inspection payload surfaced to the CLI.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InspectReport {
    /// Operator-selected inspection target.
    pub target: String,
    /// JSON payload describing the target.
    pub payload: Value,
}

/// Generic S3-compatible blob-store abstraction for future immutable uploads.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Upload an immutable object to a remote blob store.
    async fn put_object(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
}

/// Local filesystem-backed storage engine.
#[derive(Clone)]
pub struct LocalStorageEngine {
    root: PathBuf,
    blob_store: Option<Arc<dyn BlobStore>>,
}

impl LocalStorageEngine {
    /// Create a local storage engine rooted at the provided path.
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            blob_store: None,
        }
    }

    /// Create a local storage engine with an optional blob-store implementation.
    #[must_use]
    pub fn with_blob_store(root: impl AsRef<Path>, blob_store: Option<Arc<dyn BlobStore>>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            blob_store,
        }
    }

    fn collections_root(&self) -> PathBuf {
        self.root.join("collections")
    }

    fn active_wal_path(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("wal").join("active.wal")
    }

    fn flat_index_file_path(descriptor: &CollectionDescriptor, segment_id: &str) -> PathBuf {
        descriptor
            .root_path
            .join("indexes")
            .join(format!("{segment_id}.flat.json"))
    }

    fn hnsw_index_file_path(descriptor: &CollectionDescriptor, segment_id: &str) -> PathBuf {
        descriptor
            .root_path
            .join("indexes")
            .join(format!("{segment_id}.hnsw.bin"))
    }

    fn manifest_file_path(descriptor: &CollectionDescriptor, generation: u64) -> PathBuf {
        descriptor
            .root_path
            .join("manifests")
            .join(format!("{generation:020}.json"))
    }

    fn maintenance_file_path(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("maintenance.json")
    }

    fn current_manifest_pointer(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("CURRENT")
    }

    fn descriptor_path(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("descriptor.json")
    }

    fn load_collection_state(
        &self,
        collection_name: &str,
        manifest_generation: Option<u64>,
    ) -> Result<CollectionState> {
        let descriptor = self.find_collection_descriptor(collection_name)?;
        self.recover_persisted_maintenance(&descriptor)?;
        let manifest = self.load_manifest(&descriptor, manifest_generation)?;
        let delta = replay_dir(descriptor.root_path.join("wal"))?
            .into_iter()
            .filter(|record| record.seq_no > manifest.checkpoint_seq_no)
            .collect::<Vec<_>>();

        Ok(CollectionState {
            descriptor,
            manifest,
            delta,
        })
    }

    fn create_collection_directories(&self, descriptor: &CollectionDescriptor) -> Result<()> {
        fs::create_dir_all(descriptor.root_path.join("manifests"))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("wal")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("segments")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("indexes")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("tmp")))
            .map_err(|error| io_message("failed to create collection directories", error))
    }

    fn find_collection_descriptor(&self, name: &str) -> Result<CollectionDescriptor> {
        let collections_root = self.collections_root();
        if !collections_root.exists() {
            return Err(LogPoseError::Message(format!(
                "collection '{name}' does not exist"
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
            if descriptor.name == name {
                descriptor.validate()?;
                return Ok(descriptor);
            }
        }

        Err(LogPoseError::Message(format!(
            "collection '{name}' does not exist"
        )))
    }

    fn load_manifest(
        &self,
        descriptor: &CollectionDescriptor,
        generation_override: Option<u64>,
    ) -> Result<Manifest> {
        let generation = match generation_override {
            Some(generation) => generation,
            None => self.read_current_generation(descriptor)?,
        };

        let path = Self::manifest_file_path(descriptor, generation);
        if !path.exists() {
            if generation_override.is_some() && generation != 0 {
                return Err(LogPoseError::Message(format!(
                    "invalid snapshot: manifest generation {} does not exist",
                    generation
                )));
            }
            return Ok(Manifest::empty(generation));
        }
        read_json(&path)
    }

    fn read_current_generation(&self, descriptor: &CollectionDescriptor) -> Result<u64> {
        let path = Self::current_manifest_pointer(descriptor);
        if !path.exists() {
            return Ok(0);
        }
        let contents = fs::read_to_string(&path)
            .map_err(|error| io_message("failed to read CURRENT pointer", error))?;
        contents.trim().parse::<u64>().map_err(|error| {
            LogPoseError::Message(format!(
                "failed to parse CURRENT manifest generation: {error}"
            ))
        })
    }

    fn publish_manifest(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
    ) -> Result<()> {
        let manifest_path = Self::manifest_file_path(descriptor, manifest.generation);
        atomic_write(
            &manifest_path,
            serde_json::to_vec_pretty(manifest).map_err(json_message)?,
        )?;
        atomic_write(
            &Self::current_manifest_pointer(descriptor),
            manifest.generation.to_string().into_bytes(),
        )?;
        Ok(())
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
            let collection_name = descriptor.name.clone();
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
        let operation_lock = maintenance_operation_lock(&descriptor.root_path);
        let _guard = operation_lock
            .lock()
            .expect("maintenance operation lock should not be poisoned");
        let state = self.load_collection_state(collection_name, None)?;
        match operation {
            MaintenanceOperation::Flush => self.flush_state(state),
            MaintenanceOperation::Compact => self.compact_state(state),
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
        self.publish_manifest(&state.descriptor, &next_manifest)?;

        let rolled_path = state
            .descriptor
            .root_path
            .join("wal")
            .join(format!("{checkpoint_seq_no:020}.wal"));
        rotate_active(Self::active_wal_path(&state.descriptor), rolled_path)?;

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

    fn write_segment_file(
        &self,
        descriptor: &CollectionDescriptor,
        records: &[WalRecord],
    ) -> Result<SegmentMeta> {
        let segment_id = Uuid::new_v4().to_string();
        let temp_path = descriptor
            .root_path
            .join("tmp")
            .join(format!("{segment_id}.lps.tmp"));
        let final_path = descriptor
            .root_path
            .join("segments")
            .join(format!("{segment_id}.lps"));
        let sidecar_temp_path = descriptor
            .root_path
            .join("tmp")
            .join(format!("{segment_id}.flat.json.tmp"));
        let sidecar_path = Self::flat_index_file_path(descriptor, &segment_id);
        let hnsw_temp_path = descriptor
            .root_path
            .join("tmp")
            .join(format!("{segment_id}.hnsw.bin.tmp"));
        let hnsw_path = Self::hnsw_index_file_path(descriptor, &segment_id);

        let mut ids = Vec::new();
        let mut vectors = Vec::new();
        let mut metadata = Vec::new();
        let mut entries = Vec::new();
        let mut sidecar_entries = Vec::new();
        let mut hnsw_entry_sources = Vec::new();
        let mut put_count = 0usize;
        let mut delete_count = 0usize;
        let mut min_seq_no = u64::MAX;
        let mut max_seq_no = 0u64;

        for record in records {
            min_seq_no = min_seq_no.min(record.seq_no);
            max_seq_no = max_seq_no.max(record.seq_no);

            let id_offset = ids.len() as u64;
            let id_bytes = record.op.id().as_str().as_bytes();
            ids.extend_from_slice(id_bytes);

            match &record.op {
                WriteOperation::Put(put) => {
                    put_count += 1;
                    let vector_offset = vectors.len() as u64;
                    for value in &put.vector {
                        vectors.extend_from_slice(&value.to_le_bytes());
                    }
                    let metadata_offset = metadata.len() as u64;
                    let metadata_bytes = serde_json::to_vec(&put.metadata).map_err(json_message)?;
                    metadata.extend_from_slice(&metadata_bytes);

                    entries.push(SegmentEntry {
                        seq_no: record.seq_no,
                        record_id_offset: id_offset,
                        record_id_len: id_bytes.len() as u32,
                        kind: SegmentEntryKind::Put,
                        vector_offset,
                        vector_dimensions: put.vector.len() as u32,
                        metadata_offset,
                        metadata_len: metadata_bytes.len() as u32,
                    });
                    sidecar_entries.push(FlatIndexEntrySource {
                        is_put: true,
                        record_id_offset: id_offset,
                        vector_offset,
                        metadata_offset,
                        vector: Some(put.vector.clone()),
                        metadata: Some(put.metadata.clone()),
                    });
                    hnsw_entry_sources.push(Some(HnswIndexEntrySource {
                        entry_offset_index: entries.len() - 1,
                        record_id: put.id.clone(),
                        seq_no: record.seq_no,
                        vector: put.vector.clone(),
                        metadata: put.metadata.clone(),
                    }));
                }
                WriteOperation::Delete(_) => {
                    delete_count += 1;
                    entries.push(SegmentEntry {
                        seq_no: record.seq_no,
                        record_id_offset: id_offset,
                        record_id_len: id_bytes.len() as u32,
                        kind: SegmentEntryKind::Delete,
                        vector_offset: 0,
                        vector_dimensions: 0,
                        metadata_offset: 0,
                        metadata_len: 0,
                    });
                    sidecar_entries.push(FlatIndexEntrySource {
                        is_put: false,
                        record_id_offset: id_offset,
                        vector_offset: 0,
                        metadata_offset: 0,
                        vector: None,
                        metadata: None,
                    });
                    hnsw_entry_sources.push(None);
                }
            }
        }

        if records.is_empty() {
            min_seq_no = 0;
        }

        let header = SegmentHeader {
            version: 1,
            dimensions: descriptor.dimensions,
            entry_count: entries.len(),
        };
        let footer = SegmentFooter {
            payload_checksum: hash(
                &[ids.as_slice(), vectors.as_slice(), metadata.as_slice()].concat(),
            ),
        };

        let header_bytes = serde_json::to_vec(&header).map_err(json_message)?;
        let entry_bytes = serde_json::to_vec(&entries).map_err(json_message)?;
        let footer_bytes = serde_json::to_vec(&footer).map_err(json_message)?;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LPS1");
        for len in [
            header_bytes.len(),
            entry_bytes.len(),
            ids.len(),
            vectors.len(),
            metadata.len(),
            footer_bytes.len(),
        ] {
            bytes.extend_from_slice(&(len as u64).to_le_bytes());
        }
        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(&entry_bytes);
        bytes.extend_from_slice(&ids);
        bytes.extend_from_slice(&vectors);
        bytes.extend_from_slice(&metadata);
        bytes.extend_from_slice(&footer_bytes);

        let flat_index = build_flat_index(segment_id.clone(), &sidecar_entries);
        let visible_hnsw_entries =
            visible_hnsw_entries(records, &hnsw_entry_sources, descriptor.dimensions)
                .map_err(|error| io_message("failed to build hnsw sidecar", error))?;
        let hnsw_index = build_hnsw_index(
            segment_id.clone(),
            descriptor.metric,
            HnswBuildParams::default(),
            &visible_hnsw_entries,
        )
        .map_err(|error| io_message("failed to build hnsw sidecar", error))?;
        publish_segment_artifacts(
            SegmentArtifactPaths {
                segment_temp_path: &temp_path,
                segment_path: &final_path,
                flat_temp_path: &sidecar_temp_path,
                flat_path: &sidecar_path,
                hnsw_temp_path: &hnsw_temp_path,
                hnsw_path: &hnsw_path,
            },
            bytes,
            &flat_index,
            &hnsw_index,
        )?;

        let segment_bytes = final_path
            .metadata()
            .map(|metadata| metadata.len() as usize)
            .unwrap_or_default();
        let flat_bytes = sidecar_path
            .metadata()
            .map(|metadata| metadata.len() as usize)
            .unwrap_or_default();
        let hnsw_bytes = hnsw_path
            .metadata()
            .map(|metadata| metadata.len() as usize)
            .unwrap_or_default();
        let artifacts = vec![
            QueryUnitArtifactStats {
                kind: "flat_exact".to_owned(),
                file_name: sidecar_path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_else(|| format!("{segment_id}.flat.json")),
                approx_bytes: flat_bytes,
            },
            QueryUnitArtifactStats {
                kind: "hnsw".to_owned(),
                file_name: hnsw_path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_else(|| format!("{segment_id}.hnsw.bin")),
                approx_bytes: hnsw_bytes,
            },
        ];
        let component_bytes = segment_component_bytes(&hnsw_index, segment_bytes, flat_bytes);

        let remote = descriptor
            .remote_blob
            .as_ref()
            .map(|config| RemoteArtifact {
                key: format!(
                    "{}/collections/{}/segments/{}.lps",
                    config.prefix, descriptor.collection_id, segment_id
                ),
                status: if self.blob_store.is_some() {
                    RemoteSyncState::PendingUpload
                } else {
                    RemoteSyncState::UploadSkipped
                },
            });

        Ok(SegmentMeta {
            segment_id: segment_id.clone(),
            file_name: final_path
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| "segment.lps".to_owned()),
            min_seq_no,
            max_seq_no,
            put_count,
            delete_count,
            dimensions: descriptor.dimensions,
            checksum: footer.payload_checksum,
            approx_bytes: segment_bytes + flat_bytes + hnsw_bytes,
            index_kind: hnsw_index.index_kind.as_str().to_owned(),
            scalar_fields: flat_index.scalar_fields,
            artifacts,
            component_bytes,
            remote,
        })
    }
}

struct SegmentArtifactPaths<'a> {
    segment_temp_path: &'a Path,
    segment_path: &'a Path,
    flat_temp_path: &'a Path,
    flat_path: &'a Path,
    hnsw_temp_path: &'a Path,
    hnsw_path: &'a Path,
}

fn publish_segment_artifacts(
    paths: SegmentArtifactPaths<'_>,
    segment_bytes: Vec<u8>,
    flat_index: &FlatIndexSidecar,
    hnsw_index: &HnswIndexSidecar,
) -> Result<()> {
    atomic_write(paths.segment_temp_path, segment_bytes)?;
    if let Err(error) = write_flat_index(paths.flat_temp_path, flat_index) {
        cleanup_file(paths.segment_temp_path);
        cleanup_file(paths.flat_temp_path);
        cleanup_file(paths.hnsw_temp_path);
        return Err(io_message("failed to publish flat index sidecar", error));
    }
    if let Err(error) = write_hnsw_index(paths.hnsw_temp_path, hnsw_index) {
        cleanup_file(paths.segment_temp_path);
        cleanup_file(paths.flat_temp_path);
        cleanup_file(paths.hnsw_temp_path);
        return Err(io_message("failed to publish hnsw sidecar", error));
    }
    if let Err(error) = fs::rename(paths.segment_temp_path, paths.segment_path) {
        cleanup_file(paths.segment_temp_path);
        cleanup_file(paths.flat_temp_path);
        cleanup_file(paths.hnsw_temp_path);
        return Err(io_message("failed to publish segment file", error));
    }
    if let Err(error) = fs::rename(paths.flat_temp_path, paths.flat_path) {
        cleanup_file(paths.segment_path);
        cleanup_file(paths.flat_temp_path);
        cleanup_file(paths.hnsw_temp_path);
        return Err(io_message("failed to publish flat index sidecar", error));
    }
    if let Err(error) = fs::rename(paths.hnsw_temp_path, paths.hnsw_path) {
        cleanup_file(paths.segment_path);
        cleanup_file(paths.flat_path);
        cleanup_file(paths.flat_temp_path);
        cleanup_file(paths.hnsw_temp_path);
        return Err(io_message("failed to publish hnsw sidecar", error));
    }
    Ok(())
}

fn cleanup_file(path: &Path) {
    let _ = fs::remove_file(path);
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
        fs::create_dir_all(self.collections_root())
            .map_err(|error| io_message("failed to create collections root", error))?;
        if self.find_collection_descriptor(&request.name).is_ok() {
            return Err(LogPoseError::Message(format!(
                "collection '{}' already exists",
                request.name
            )));
        }

        let descriptor = CollectionDescriptor::new(
            request.name,
            request.dimensions,
            request.metric,
            self.collections_root(),
        );
        self.create_collection_directories(&descriptor)?;
        atomic_write(
            &Self::descriptor_path(&descriptor),
            serde_json::to_vec_pretty(&descriptor).map_err(json_message)?,
        )?;
        self.publish_manifest(&descriptor, &Manifest::empty(0))?;
        self.persist_maintenance_status(&descriptor, &MaintenanceStatus::default())?;
        let mut wal_writer = WalWriter::open(Self::active_wal_path(&descriptor))?;
        wal_writer.truncate()?;
        Ok(descriptor)
    }

    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor> {
        let descriptor = self.find_collection_descriptor(name)?;
        self.recover_persisted_maintenance(&descriptor)?;
        Ok(descriptor)
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

        let state = self.load_collection_state(collection_name, None)?;
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

        let mut wal_writer = WalWriter::open(Self::active_wal_path(&state.descriptor))?;
        let mut last_seq_no = existing_max;
        let mut delta_after_write = state.delta.clone();
        for operation in &operations {
            last_seq_no += 1;
            wal_writer.append(last_seq_no, operation)?;
            delta_after_write.push(WalRecord {
                seq_no: last_seq_no,
                op: operation.clone(),
            });
        }

        if self.should_flush(&state.descriptor, &delta_after_write) {
            self.enqueue_maintenance(&state.descriptor, vec![MaintenanceOperation::Flush])?;
        } else if self.should_compact(&state.descriptor, state.manifest.segments.len()) {
            self.enqueue_maintenance(&state.descriptor, vec![MaintenanceOperation::Compact])?;
        }

        Ok(CommitAck {
            last_seq_no,
            applied_ops: operations.len(),
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

    async fn stats_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let state = self.load_collection_state(
            collection_name,
            snapshot.as_ref().map(|value| value.manifest_generation),
        )?;
        let effective_snapshot = resolve_snapshot(&state, snapshot)?;
        let resolved =
            resolve_latest_state_selected(&state, effective_snapshot.visible_seq_no, true, None)?;
        let mut live_record_count = 0usize;
        let mut deleted_record_count = 0usize;
        for value in resolved.values() {
            match value {
                ResolvedState::Visible(_) => live_record_count += 1,
                ResolvedState::Deleted { .. } => deleted_record_count += 1,
            }
        }
        let maintenance = self.load_maintenance_status(&state.descriptor)?;
        let delta_records = state
            .delta
            .iter()
            .filter(|record| record.seq_no <= effective_snapshot.visible_seq_no)
            .cloned()
            .collect::<Vec<_>>();
        let mut query_units = vec![mutable_query_unit(&delta_records)];
        query_units.extend(state.manifest.segments.iter().map(QueryUnitStats::from));

        Ok(CollectionStats {
            collection_id: state.descriptor.collection_id.clone(),
            collection_name: state.descriptor.name.clone(),
            manifest_generation: effective_snapshot.manifest_generation,
            visible_seq_no: effective_snapshot.visible_seq_no,
            mutable_op_count: delta_records.len(),
            segment_count: state.manifest.segments.len(),
            live_record_count,
            deleted_record_count,
            maintenance,
            query_units,
        })
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

impl LocalStorageEngine {
    fn scan_exact_internal(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Option<std::collections::BTreeSet<String>>,
    ) -> Result<Vec<VisibleRecord>> {
        let state = self.load_collection_state(
            collection_name,
            snapshot.as_ref().map(|value| value.manifest_generation),
        )?;
        let snapshot = resolve_snapshot(&state, snapshot)?;

        let resolved = resolve_latest_state_selected(
            &state,
            snapshot.visible_seq_no,
            include_mutable,
            immutable_unit_ids,
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
}

#[derive(Clone, Debug)]
struct CollectionState {
    descriptor: CollectionDescriptor,
    manifest: Manifest,
    delta: Vec<WalRecord>,
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

fn resolve_snapshot(state: &CollectionState, snapshot: Option<Snapshot>) -> Result<Snapshot> {
    let snapshot = snapshot.unwrap_or(Snapshot {
        manifest_generation: state.manifest.generation,
        visible_seq_no: state.visible_seq_no(),
    });

    if snapshot.manifest_generation != state.manifest.generation {
        return Err(LogPoseError::Message(format!(
            "invalid snapshot: manifest generation {} is unavailable",
            snapshot.manifest_generation
        )));
    }

    let max_visible = state.visible_seq_no();
    if snapshot.visible_seq_no > max_visible {
        return Err(LogPoseError::Message(format!(
            "invalid snapshot: visible sequence {} exceeds maximum {} for manifest generation {}",
            snapshot.visible_seq_no, max_visible, snapshot.manifest_generation
        )));
    }
    if snapshot.visible_seq_no < state.manifest.checkpoint_seq_no {
        return Err(LogPoseError::Message(format!(
            "invalid snapshot: visible sequence {} is below checkpoint {} for manifest generation {}",
            snapshot.visible_seq_no, state.manifest.checkpoint_seq_no, snapshot.manifest_generation
        )));
    }

    Ok(snapshot)
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

impl CollectionState {
    fn visible_seq_no(&self) -> SeqNo {
        self.delta.last().map(|record| record.seq_no).unwrap_or(
            self.manifest
                .checkpoint_seq_no
                .max(self.manifest.max_segment_seq_no()),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Manifest {
    generation: u64,
    checkpoint_seq_no: SeqNo,
    segments: Vec<SegmentMeta>,
}

impl Manifest {
    fn empty(generation: u64) -> Self {
        Self {
            generation,
            checkpoint_seq_no: 0,
            segments: Vec::new(),
        }
    }

    fn max_segment_seq_no(&self) -> SeqNo {
        self.segments
            .iter()
            .map(|segment| segment.max_seq_no)
            .max()
            .unwrap_or(0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SegmentMeta {
    segment_id: String,
    file_name: String,
    min_seq_no: SeqNo,
    max_seq_no: SeqNo,
    put_count: usize,
    delete_count: usize,
    dimensions: usize,
    checksum: u32,
    #[serde(default)]
    approx_bytes: usize,
    #[serde(default = "default_index_kind")]
    index_kind: String,
    #[serde(default)]
    scalar_fields: BTreeMap<String, ScalarFieldStats>,
    #[serde(default)]
    artifacts: Vec<QueryUnitArtifactStats>,
    #[serde(default)]
    component_bytes: BTreeMap<String, usize>,
    remote: Option<RemoteArtifact>,
}

fn default_index_kind() -> String {
    "hnsw".to_owned()
}

impl From<&SegmentMeta> for QueryUnitStats {
    fn from(segment: &SegmentMeta) -> Self {
        Self {
            unit_id: segment.segment_id.clone(),
            tier: "immutable".to_owned(),
            index_kind: segment.index_kind.clone(),
            min_seq_no: segment.min_seq_no,
            max_seq_no: segment.max_seq_no,
            put_count: segment.put_count,
            delete_count: segment.delete_count,
            approx_bytes: segment.approx_bytes,
            scalar_fields: segment.scalar_fields.clone(),
            artifact_stats: segment.artifacts.clone(),
            component_bytes: segment.component_bytes.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RemoteArtifact {
    key: String,
    status: RemoteSyncState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RemoteSyncState {
    PendingUpload,
    UploadSkipped,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SegmentHeader {
    version: u16,
    dimensions: usize,
    entry_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SegmentFooter {
    payload_checksum: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SegmentEntry {
    seq_no: SeqNo,
    record_id_offset: u64,
    record_id_len: u32,
    kind: SegmentEntryKind,
    vector_offset: u64,
    vector_dimensions: u32,
    metadata_offset: u64,
    metadata_len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SegmentEntryKind {
    Put,
    Delete,
}

#[derive(Clone, Debug)]
enum ResolvedState {
    Visible(VisibleRecord),
    Deleted { id: RecordId, seq_no: SeqNo },
}

fn resolve_latest_state_selected(
    state: &CollectionState,
    visible_seq_no: SeqNo,
    include_mutable: bool,
    immutable_unit_ids: Option<std::collections::BTreeSet<String>>,
) -> Result<BTreeMap<RecordId, ResolvedState>> {
    let mut resolved = BTreeMap::new();

    if include_mutable {
        for record in state
            .delta
            .iter()
            .rev()
            .filter(|record| record.seq_no <= visible_seq_no)
        {
            apply_resolved_record(&mut resolved, record.clone());
        }
    }

    for segment in state.manifest.segments.iter().rev().filter(|segment| {
        immutable_unit_ids
            .as_ref()
            .is_none_or(|selected| selected.contains(&segment.segment_id))
    }) {
        let records = read_segment_file(
            &state
                .descriptor
                .root_path
                .join("segments")
                .join(&segment.file_name),
        )?;
        for record in records
            .into_iter()
            .rev()
            .filter(|record| record.seq_no <= visible_seq_no)
        {
            apply_resolved_record(&mut resolved, record);
        }
    }

    Ok(resolved)
}

fn resolve_latest_from_segments(
    descriptor: &CollectionDescriptor,
    manifest: &Manifest,
) -> Result<BTreeMap<RecordId, ResolvedState>> {
    let mut resolved = BTreeMap::new();
    for segment in manifest.segments.iter().rev() {
        let records = read_segment_file(
            &descriptor
                .root_path
                .join("segments")
                .join(&segment.file_name),
        )?;
        for record in records.into_iter().rev() {
            apply_resolved_record(&mut resolved, record);
        }
    }
    Ok(resolved)
}

fn resolve_latest_state_for_ids_selected(
    state: &CollectionState,
    visible_seq_no: SeqNo,
    wanted_ids: &BTreeSet<RecordId>,
    include_mutable: bool,
    immutable_unit_ids: Option<BTreeSet<String>>,
) -> Result<BTreeMap<RecordId, ResolvedState>> {
    let mut resolved = BTreeMap::new();

    if include_mutable {
        for record in state
            .delta
            .iter()
            .rev()
            .filter(|record| record.seq_no <= visible_seq_no)
        {
            if wanted_ids.contains(record.op.id()) {
                apply_resolved_record(&mut resolved, record.clone());
            }
            if resolved.len() == wanted_ids.len() {
                return Ok(resolved);
            }
        }
    }

    for segment in state.manifest.segments.iter().rev().filter(|segment| {
        immutable_unit_ids
            .as_ref()
            .is_none_or(|selected| selected.contains(&segment.segment_id))
    }) {
        let records = read_segment_file(
            &state
                .descriptor
                .root_path
                .join("segments")
                .join(&segment.file_name),
        )?;
        for record in records
            .into_iter()
            .rev()
            .filter(|record| record.seq_no <= visible_seq_no && wanted_ids.contains(record.op.id()))
        {
            apply_resolved_record(&mut resolved, record);
        }
        if resolved.len() == wanted_ids.len() {
            return Ok(resolved);
        }
    }

    Ok(resolved)
}

fn mutable_query_unit(delta: &[WalRecord]) -> QueryUnitStats {
    let sidecar = build_flat_index(
        "mutable-delta",
        &delta
            .iter()
            .map(|record| match &record.op {
                WriteOperation::Put(put) => FlatIndexEntrySource {
                    is_put: true,
                    record_id_offset: 0,
                    vector_offset: 0,
                    metadata_offset: 0,
                    vector: Some(put.vector.clone()),
                    metadata: Some(put.metadata.clone()),
                },
                WriteOperation::Delete(_) => FlatIndexEntrySource {
                    is_put: false,
                    record_id_offset: 0,
                    vector_offset: 0,
                    metadata_offset: 0,
                    vector: None,
                    metadata: None,
                },
            })
            .collect::<Vec<_>>(),
    );

    QueryUnitStats {
        unit_id: "mutable-delta".to_owned(),
        tier: "mutable".to_owned(),
        index_kind: "raw".to_owned(),
        min_seq_no: delta.first().map(|record| record.seq_no).unwrap_or(0),
        max_seq_no: delta.last().map(|record| record.seq_no).unwrap_or(0),
        put_count: sidecar.put_count,
        delete_count: sidecar.delete_count,
        approx_bytes: delta
            .iter()
            .map(|record| approximate_record_bytes(&record.op))
            .sum(),
        scalar_fields: sidecar.scalar_fields,
        artifact_stats: vec![QueryUnitArtifactStats {
            kind: "mutable_delta".to_owned(),
            file_name: String::new(),
            approx_bytes: delta
                .iter()
                .map(|record| approximate_record_bytes(&record.op))
                .sum(),
        }],
        component_bytes: BTreeMap::from([(
            "mutable_delta".to_owned(),
            delta
                .iter()
                .map(|record| approximate_record_bytes(&record.op))
                .sum(),
        )]),
    }
}

fn visible_hnsw_entries(
    records: &[WalRecord],
    entry_sources: &[Option<HnswIndexEntrySource>],
    dimensions: usize,
) -> io::Result<Vec<HnswIndexEntrySource>> {
    let mut seen = BTreeSet::new();
    let mut visible = Vec::new();
    for (index, record) in records.iter().enumerate().rev() {
        let record_id = record.op.id().clone();
        if !seen.insert(record_id) {
            continue;
        }
        let Some(entry) = entry_sources.get(index).and_then(|entry| entry.clone()) else {
            continue;
        };
        if entry.vector.len() != dimensions {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "stored vector '{}' expected {} dimensions but found {}",
                    entry.record_id,
                    dimensions,
                    entry.vector.len()
                ),
            ));
        }
        visible.push(entry);
    }
    visible.reverse();
    Ok(visible)
}

fn segment_component_bytes(
    hnsw_index: &HnswIndexSidecar,
    raw_segment_bytes: usize,
    flat_bytes: usize,
) -> BTreeMap<String, usize> {
    let ann_vectors = hnsw_index
        .nodes
        .iter()
        .map(|node| node.record.vector.len() * std::mem::size_of::<f32>())
        .sum::<usize>();
    let ann_metadata = hnsw_index
        .nodes
        .iter()
        .map(|node| node.record.metadata.to_string().len())
        .sum::<usize>();
    let ann_graph = hnsw_index
        .nodes
        .iter()
        .map(|node| {
            std::mem::size_of::<u32>()
                + std::mem::size_of::<u8>()
                + node.neighbors_by_level.len() * std::mem::size_of::<u32>()
                + node
                    .neighbors_by_level
                    .iter()
                    .map(|neighbors| neighbors.len() * std::mem::size_of::<u32>())
                    .sum::<usize>()
        })
        .sum::<usize>();

    BTreeMap::from([
        ("raw_segment".to_owned(), raw_segment_bytes),
        ("exact_flat".to_owned(), flat_bytes),
        ("ann_graph".to_owned(), ann_graph),
        ("ann_vectors".to_owned(), ann_vectors),
        ("ann_metadata".to_owned(), ann_metadata),
    ])
}

fn segment_artifact_file_name<'a>(segment: &'a SegmentMeta, kind: &str) -> Option<&'a str> {
    segment
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == kind)
        .map(|artifact| artifact.file_name.as_str())
}

fn storage_metric_value(metric: DistanceMetric, query: &[f32], candidate: &[f32]) -> Result<f32> {
    if query.len() != candidate.len() {
        return Err(LogPoseError::Message(format!(
            "vector expected {} dimensions but found {}",
            query.len(),
            candidate.len()
        )));
    }

    Ok(match metric {
        DistanceMetric::Dot => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| lhs * rhs)
            .sum(),
        DistanceMetric::Cosine => {
            let dot: f32 = query
                .iter()
                .zip(candidate)
                .map(|(lhs, rhs)| lhs * rhs)
                .sum();
            let query_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
            let candidate_norm = candidate
                .iter()
                .map(|value| value * value)
                .sum::<f32>()
                .sqrt();
            if query_norm == 0.0 || candidate_norm == 0.0 {
                0.0
            } else {
                dot / (query_norm * candidate_norm)
            }
        }
        DistanceMetric::L2 => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| {
                let delta = lhs - rhs;
                delta * delta
            })
            .sum::<f32>()
            .sqrt(),
    })
}

fn storage_metric_compare(metric: DistanceMetric, left: f32, right: f32) -> Ordering {
    match metric {
        DistanceMetric::Dot | DistanceMetric::Cosine => {
            left.partial_cmp(&right).unwrap_or(Ordering::Equal)
        }
        DistanceMetric::L2 => right.partial_cmp(&left).unwrap_or(Ordering::Equal),
    }
}

fn apply_resolved_record(resolved: &mut BTreeMap<RecordId, ResolvedState>, record: WalRecord) {
    let id = record.op.id().clone();
    if resolved.contains_key(&id) {
        return;
    }

    match record.op {
        WriteOperation::Put(put) => {
            resolved.insert(
                id,
                ResolvedState::Visible(VisibleRecord {
                    id: put.id,
                    vector: put.vector,
                    metadata: put.metadata,
                    seq_no: record.seq_no,
                }),
            );
        }
        WriteOperation::Delete(delete) => {
            resolved.insert(
                id,
                ResolvedState::Deleted {
                    id: delete.id,
                    seq_no: record.seq_no,
                },
            );
        }
    }
}

fn read_segment_file(path: &Path) -> Result<Vec<WalRecord>> {
    let bytes = fs::read(path).map_err(|error| io_message("failed to read segment file", error))?;
    if bytes.len() < 4 || &bytes[..4] != b"LPS1" {
        return Err(LogPoseError::Message(format!(
            "invalid segment magic in '{}'",
            path.display()
        )));
    }

    let mut offset = 4usize;
    let read_len = |bytes: &[u8], offset: &mut usize| -> Result<usize> {
        let slice = checked_slice(bytes, *offset, 8, "segment length header")?;
        let value = u64::from_le_bytes(
            slice
                .try_into()
                .expect("segment length slice should fit after bounds check"),
        ) as usize;
        *offset += 8;
        Ok(value)
    };
    let header_len = read_len(&bytes, &mut offset)?;
    let entry_len = read_len(&bytes, &mut offset)?;
    let ids_len = read_len(&bytes, &mut offset)?;
    let vectors_len = read_len(&bytes, &mut offset)?;
    let metadata_len = read_len(&bytes, &mut offset)?;
    let footer_len = read_len(&bytes, &mut offset)?;

    let header: SegmentHeader =
        serde_json::from_slice(checked_slice(&bytes, offset, header_len, "segment header")?)
            .map_err(json_message)?;
    offset += header_len;
    let entries: Vec<SegmentEntry> = serde_json::from_slice(checked_slice(
        &bytes,
        offset,
        entry_len,
        "segment entry table",
    )?)
    .map_err(json_message)?;
    offset += entry_len;

    let ids = checked_slice(&bytes, offset, ids_len, "segment id section")?;
    offset += ids_len;
    let vectors = checked_slice(&bytes, offset, vectors_len, "segment vector section")?;
    offset += vectors_len;
    let metadata = checked_slice(&bytes, offset, metadata_len, "segment metadata section")?;
    offset += metadata_len;
    let footer: SegmentFooter =
        serde_json::from_slice(checked_slice(&bytes, offset, footer_len, "segment footer")?)
            .map_err(json_message)?;

    let actual_checksum = hash(&[ids, vectors, metadata].concat());
    if actual_checksum != footer.payload_checksum {
        return Err(LogPoseError::Message(format!(
            "checksum mismatch while reading segment '{}': expected {}, got {}",
            path.display(),
            footer.payload_checksum,
            actual_checksum
        )));
    }

    let mut records = Vec::with_capacity(header.entry_count);
    for entry in entries {
        let id_slice = checked_slice(
            ids,
            entry.record_id_offset as usize,
            entry.record_id_len as usize,
            "segment record id",
        )?;
        let id = RecordId::new(std::str::from_utf8(id_slice).map_err(|error| {
            LogPoseError::Message(format!("failed to decode record id from segment: {error}"))
        })?);

        let op = match entry.kind {
            SegmentEntryKind::Put => {
                let mut vector = Vec::with_capacity(entry.vector_dimensions as usize);
                let vector_start = entry.vector_offset as usize;
                let vector_end = vector_start + entry.vector_dimensions as usize * 4;
                for chunk in checked_slice(
                    vectors,
                    vector_start,
                    vector_end.saturating_sub(vector_start),
                    "segment vector payload",
                )?
                .chunks_exact(4)
                {
                    vector.push(f32::from_le_bytes(
                        chunk.try_into().expect("vector chunk should be four bytes"),
                    ));
                }
                let metadata_start = entry.metadata_offset as usize;
                let metadata_end = metadata_start + entry.metadata_len as usize;
                let metadata_value = serde_json::from_slice(checked_slice(
                    metadata,
                    metadata_start,
                    metadata_end.saturating_sub(metadata_start),
                    "segment metadata payload",
                )?)
                .map_err(json_message)?;
                WriteOperation::Put(PutRecord {
                    id,
                    vector,
                    metadata: metadata_value,
                })
            }
            SegmentEntryKind::Delete => WriteOperation::Delete(logpose_types::DeleteRecord { id }),
        };

        records.push(WalRecord {
            seq_no: entry.seq_no,
            op,
        });
    }
    Ok(records)
}

fn approximate_record_bytes(operation: &WriteOperation) -> usize {
    match operation {
        WriteOperation::Put(put) => {
            put.id.as_str().len()
                + put.vector.len() * std::mem::size_of::<f32>()
                + serde_json::to_vec(&put.metadata)
                    .map(|value| value.len())
                    .unwrap_or(0)
                + 32
        }
        WriteOperation::Delete(delete) => delete.id.as_str().len() + 16,
    }
}

fn read_json<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = fs::read(path).map_err(|error| io_message("failed to read JSON file", error))?;
    serde_json::from_slice(&bytes).map_err(json_message)
}

fn atomic_write(path: &Path, bytes: Vec<u8>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| io_message("failed to create parent directory", error))?;
    }
    static ATOMIC_WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);
    let temp_path = path.with_file_name(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".to_owned()),
        std::process::id(),
        ATOMIC_WRITE_COUNTER.fetch_add(1, AtomicOrdering::Relaxed),
    ));
    let mut file = File::create(&temp_path)
        .map_err(|error| io_message("failed to create temp file", error))?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| io_message("failed to write temp file", error))?;
    fs::rename(&temp_path, path)
        .map_err(|error| io_message("failed to atomically rename file", error))
}

fn checked_slice<'a>(bytes: &'a [u8], start: usize, len: usize, label: &str) -> Result<&'a [u8]> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| LogPoseError::Message(format!("overflow while reading {label}")))?;
    if end > bytes.len() {
        return Err(LogPoseError::Message(format!(
            "truncated segment while reading {label}: need {end} bytes but file has {}",
            bytes.len()
        )));
    }
    Ok(&bytes[start..end])
}

fn io_message(context: &str, error: std::io::Error) -> LogPoseError {
    LogPoseError::Message(format!("{context}: {error}"))
}

fn json_message(error: serde_json::Error) -> LogPoseError {
    LogPoseError::Message(format!("failed to serialize or deserialize JSON: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_index::FlatIndexEntrySource;
    use logpose_types::{DistanceMetric, PutRecord, RecordId, WriteOperation};
    use rand as _;
    use serde_json::json;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn truncated_segment_returns_error_instead_of_panicking() {
        let root = unique_temp_dir("storage-truncated-segment");
        let runtime = tokio::runtime::Runtime::new().expect("runtime should build");

        let segment_path = runtime.block_on(async {
            let engine = LocalStorageEngine::new(&root);
            let descriptor = engine
                .create_collection(CreateCollectionRequest {
                    name: "broken".to_owned(),
                    dimensions: 2,
                    metric: DistanceMetric::Cosine,
                })
                .await
                .expect("collection should be created");

            engine
                .write(
                    "broken",
                    vec![WriteOperation::Put(PutRecord {
                        id: RecordId::new("id-1"),
                        vector: vec![1.0, 1.0],
                        metadata: json!({"status":"ok"}),
                    })],
                )
                .await
                .expect("write should succeed");
            engine.flush("broken").await.expect("flush should succeed");

            let manifest = engine
                .inspect("broken", InspectTarget::Manifest)
                .await
                .expect("inspect should succeed");
            let segment_file = manifest.payload["segments"][0]["file_name"]
                .as_str()
                .expect("segment file should exist");
            descriptor.root_path.join("segments").join(segment_file)
        });

        let bytes = fs::read(&segment_path).expect("segment file should exist");
        fs::write(&segment_path, &bytes[..10]).expect("truncate should succeed");

        let result = std::panic::catch_unwind(|| read_segment_file(&segment_path));
        assert!(result.is_ok(), "truncated segment should not panic");
        assert!(result.expect("result should exist").is_err());
    }

    #[test]
    fn visible_hnsw_entries_ignore_shadowed_dimension_mismatches() {
        let records = vec![
            WalRecord {
                seq_no: 1,
                op: WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![9.0],
                    metadata: json!({"version":1}),
                }),
            },
            WalRecord {
                seq_no: 2,
                op: WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"version":2}),
                }),
            },
        ];
        let entries = vec![
            Some(HnswIndexEntrySource {
                entry_offset_index: 0,
                record_id: RecordId::new("alpha"),
                seq_no: 1,
                vector: vec![9.0],
                metadata: json!({"version":1}),
            }),
            Some(HnswIndexEntrySource {
                entry_offset_index: 1,
                record_id: RecordId::new("alpha"),
                seq_no: 2,
                vector: vec![1.0, 0.0],
                metadata: json!({"version":2}),
            }),
        ];

        let visible =
            visible_hnsw_entries(&records, &entries, 2).expect("latest visible record is valid");

        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].seq_no, 2);
        assert_eq!(visible[0].vector, vec![1.0, 0.0]);
    }

    #[test]
    fn write_segment_file_rejects_visible_dimension_mismatches() {
        let root = unique_temp_dir("storage-visible-dimension-mismatch");
        let runtime = tokio::runtime::Runtime::new().expect("runtime should build");

        let result = runtime.block_on(async {
            let engine = LocalStorageEngine::new(&root);
            let descriptor = engine
                .create_collection(CreateCollectionRequest {
                    name: "broken".to_owned(),
                    dimensions: 2,
                    metric: DistanceMetric::Dot,
                })
                .await
                .expect("collection should be created");

            engine.write_segment_file(
                &descriptor,
                &[WalRecord {
                    seq_no: 1,
                    op: WriteOperation::Put(PutRecord {
                        id: RecordId::new("alpha"),
                        vector: vec![1.0],
                        metadata: json!({"kind":"broken"}),
                    }),
                }],
            )
        });

        let error = result.expect_err("visible dimension mismatch should fail segment build");
        assert!(
            error
                .to_string()
                .contains("failed to build hnsw sidecar: stored vector 'alpha' expected 2 dimensions but found 1"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn sidecar_publish_failure_cleans_up_published_segment_file() {
        let root = unique_temp_dir("storage-sidecar-cleanup");
        let temp_path = root.join("tmp").join("segment.lps.tmp");
        let final_path = root.join("segments").join("segment.lps");
        let sidecar_temp_path = root.join("tmp").join("segment.flat.json.tmp");
        let sidecar_path = root.join("indexes").join("segment.flat.json");
        let hnsw_temp_path = root.join("tmp").join("segment.hnsw.bin.tmp");
        let hnsw_path = root.join("indexes").join("segment.hnsw.bin");
        fs::create_dir_all(final_path.parent().expect("segment parent should exist"))
            .expect("segment parent should be created");
        fs::create_dir_all(sidecar_path.parent().expect("index parent should exist"))
            .expect("index parent should be created");
        fs::create_dir_all(&sidecar_path).expect("directory should force sidecar publish failure");

        let flat_index = build_flat_index(
            "segment",
            &[FlatIndexEntrySource {
                is_put: true,
                record_id_offset: 0,
                vector_offset: 0,
                metadata_offset: 0,
                vector: Some(vec![1.0, 0.0]),
                metadata: Some(json!({"kind":"keep"})),
            }],
        );
        let hnsw_index = build_hnsw_index(
            "segment",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[HnswIndexEntrySource {
                entry_offset_index: 0,
                record_id: RecordId::new("alpha"),
                seq_no: 1,
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            }],
        )
        .expect("hnsw index should build");

        let result = publish_segment_artifacts(
            SegmentArtifactPaths {
                segment_temp_path: &temp_path,
                segment_path: &final_path,
                flat_temp_path: &sidecar_temp_path,
                flat_path: &sidecar_path,
                hnsw_temp_path: &hnsw_temp_path,
                hnsw_path: &hnsw_path,
            },
            b"segment-bytes".to_vec(),
            &flat_index,
            &hnsw_index,
        );

        assert!(result.is_err(), "sidecar publish should fail");
        assert!(
            !final_path.exists(),
            "segment file should be removed after sidecar publish failure"
        );
        assert!(
            !temp_path.exists(),
            "temporary segment file should be cleaned up after failure"
        );
        assert!(
            !sidecar_temp_path.exists(),
            "temporary sidecar file should be cleaned up after failure"
        );
        assert!(
            !hnsw_temp_path.exists(),
            "temporary hnsw file should be cleaned up after failure"
        );
    }

    #[test]
    fn maintenance_worker_clears_coordinator_on_descriptor_lookup_failure() {
        let root = unique_temp_dir("storage-maintenance-descriptor-failure");
        let engine = LocalStorageEngine::new(&root);
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

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
        fs::create_dir_all(&dir).expect("temp dir should be created");
        dir
    }
}
