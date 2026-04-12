//! Storage engine abstractions.

use async_trait::async_trait;
use crc32fast::hash;
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    CollectionStats, CommitAck, DistanceMetric, LogPoseError, PutRecord, RecordId, Result, SeqNo,
    Snapshot, VisibleRecord, WriteOperation,
};
use logpose_wal::{WalRecord, WalWriter, replay_dir, rotate_active};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
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

    /// Flush the mutable delta into a new immutable segment.
    async fn flush(&self, collection_name: &str) -> Result<Snapshot>;

    /// Compact immutable segments into a single replacement segment.
    async fn compact(&self, collection_name: &str) -> Result<Snapshot>;

    /// Return collection-level visibility and storage statistics.
    async fn stats(&self, collection_name: &str) -> Result<CollectionStats>;

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

    fn manifest_file_path(descriptor: &CollectionDescriptor, generation: u64) -> PathBuf {
        descriptor
            .root_path
            .join("manifests")
            .join(format!("{generation:020}.json"))
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

        let mut ids = Vec::new();
        let mut vectors = Vec::new();
        let mut metadata = Vec::new();
        let mut entries = Vec::new();
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

        atomic_write(&temp_path, bytes)?;
        fs::rename(&temp_path, &final_path)
            .map_err(|error| io_message("failed to publish segment file", error))?;

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
            segment_id,
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
            remote,
        })
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
        let mut wal_writer = WalWriter::open(Self::active_wal_path(&descriptor))?;
        wal_writer.truncate()?;
        Ok(descriptor)
    }

    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor> {
        self.find_collection_descriptor(name)
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
        let mut wal_writer = WalWriter::open(Self::active_wal_path(&state.descriptor))?;
        let mut last_seq_no = existing_max;
        let mut delta_after_write = state.delta.clone();

        let mut seen_ids = BTreeMap::<RecordId, ()>::new();
        for operation in &operations {
            state.descriptor.validate_operation(operation)?;
            if seen_ids.insert(operation.id().clone(), ()).is_some() {
                return Err(LogPoseError::Message(format!(
                    "write batch includes duplicate record id '{}'",
                    operation.id()
                )));
            }
            last_seq_no += 1;
            wal_writer.append(last_seq_no, operation)?;
            delta_after_write.push(WalRecord {
                seq_no: last_seq_no,
                op: operation.clone(),
            });
        }

        if self.should_flush(&state.descriptor, &delta_after_write) {
            self.flush_state(CollectionState {
                descriptor: state.descriptor.clone(),
                manifest: state.manifest.clone(),
                delta: delta_after_write,
            })?;
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
        let state = self.load_collection_state(
            collection_name,
            snapshot.as_ref().map(|value| value.manifest_generation),
        )?;
        let snapshot = snapshot.unwrap_or(Snapshot {
            manifest_generation: state.manifest.generation,
            visible_seq_no: state.visible_seq_no(),
        });

        let resolved = resolve_latest_state(&state, snapshot.visible_seq_no)?;
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
        let state = self.load_collection_state(collection_name, None)?;
        self.flush_state(state)
    }

    async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        let state = self.load_collection_state(collection_name, None)?;
        self.compact_state(state)
    }

    async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        let state = self.load_collection_state(collection_name, None)?;
        let resolved = resolve_latest_state(&state, state.visible_seq_no())?;
        let mut live_record_count = 0usize;
        let mut deleted_record_count = 0usize;
        for value in resolved.values() {
            match value {
                ResolvedState::Visible(_) => live_record_count += 1,
                ResolvedState::Deleted { .. } => deleted_record_count += 1,
            }
        }
        let visible_seq_no = state.visible_seq_no();

        Ok(CollectionStats {
            collection_id: state.descriptor.collection_id.clone(),
            collection_name: state.descriptor.name.clone(),
            manifest_generation: state.manifest.generation,
            visible_seq_no,
            mutable_op_count: state.delta.len(),
            segment_count: state.manifest.segments.len(),
            live_record_count,
            deleted_record_count,
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
                Ok(InspectReport {
                    target: format!("segment:{segment_id}"),
                    payload: json!({
                        "segment": segment,
                        "records": records,
                    }),
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
struct CollectionState {
    descriptor: CollectionDescriptor,
    manifest: Manifest,
    delta: Vec<WalRecord>,
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
    remote: Option<RemoteArtifact>,
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

fn resolve_latest_state(
    state: &CollectionState,
    visible_seq_no: SeqNo,
) -> Result<BTreeMap<RecordId, ResolvedState>> {
    let mut resolved = BTreeMap::new();

    for record in state
        .delta
        .iter()
        .rev()
        .filter(|record| record.seq_no <= visible_seq_no)
    {
        apply_resolved_record(&mut resolved, record.clone());
    }

    for segment in state.manifest.segments.iter().rev() {
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
    let temp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "tmp".to_owned())
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
    use logpose_types::{DeleteRecord, DistanceMetric, PutRecord, RecordId, WriteOperation};
    use serde_json::json;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[tokio::test]
    async fn create_write_scan_and_delete_records() {
        let root = unique_temp_dir("storage-write-scan");
        let engine = LocalStorageEngine::new(&root);

        engine
            .create_collection(CreateCollectionRequest {
                name: "colors".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Cosine,
            })
            .await
            .expect("collection should be created");

        engine
            .write(
                "colors",
                vec![
                    WriteOperation::Put(PutRecord {
                        id: RecordId::new("alpha"),
                        vector: vec![1.0, 0.0],
                        metadata: json!({"color":"red"}),
                    }),
                    WriteOperation::Put(PutRecord {
                        id: RecordId::new("beta"),
                        vector: vec![0.0, 1.0],
                        metadata: json!({"color":"green"}),
                    }),
                ],
            )
            .await
            .expect("writes should succeed");

        let before_delete = engine
            .scan_exact("colors", None)
            .await
            .expect("scan should succeed");
        assert_eq!(before_delete.len(), 2);

        engine
            .write(
                "colors",
                vec![WriteOperation::Delete(DeleteRecord {
                    id: RecordId::new("alpha"),
                })],
            )
            .await
            .expect("delete should succeed");

        let after_delete = engine
            .scan_exact("colors", None)
            .await
            .expect("scan should succeed");
        assert_eq!(after_delete.len(), 1);
        assert_eq!(after_delete[0].id.as_str(), "beta");
    }

    #[tokio::test]
    async fn flush_persists_visible_records_for_reopen() {
        let root = unique_temp_dir("storage-flush");
        let engine = LocalStorageEngine::new(&root);

        engine
            .create_collection(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 3,
                metric: DistanceMetric::Dot,
            })
            .await
            .expect("collection should be created");

        engine
            .write(
                "documents",
                vec![WriteOperation::Put(PutRecord {
                    id: RecordId::new("doc-1"),
                    vector: vec![0.1, 0.2, 0.3],
                    metadata: json!({"topic":"intro"}),
                })],
            )
            .await
            .expect("write should succeed");

        engine
            .flush("documents")
            .await
            .expect("flush should succeed");

        let reopened = LocalStorageEngine::new(&root);
        let visible = reopened
            .scan_exact("documents", None)
            .await
            .expect("scan should succeed");
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].id.as_str(), "doc-1");

        let stats = reopened
            .stats("documents")
            .await
            .expect("stats should succeed");
        assert_eq!(stats.segment_count, 1);
        assert_eq!(stats.mutable_op_count, 0);
    }

    #[tokio::test]
    async fn compact_merges_segments_and_preserves_latest_versions() {
        let root = unique_temp_dir("storage-compact");
        let engine = LocalStorageEngine::new(&root);

        engine
            .create_collection(CreateCollectionRequest {
                name: "profiles".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::L2,
            })
            .await
            .expect("collection should be created");

        engine
            .write(
                "profiles",
                vec![WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 1.0],
                    metadata: json!({"version":1}),
                })],
            )
            .await
            .expect("write should succeed");
        engine
            .flush("profiles")
            .await
            .expect("flush should succeed");

        engine
            .write(
                "profiles",
                vec![
                    WriteOperation::Put(PutRecord {
                        id: RecordId::new("alpha"),
                        vector: vec![2.0, 2.0],
                        metadata: json!({"version":2}),
                    }),
                    WriteOperation::Put(PutRecord {
                        id: RecordId::new("beta"),
                        vector: vec![3.0, 3.0],
                        metadata: json!({"version":1}),
                    }),
                ],
            )
            .await
            .expect("write should succeed");
        engine
            .flush("profiles")
            .await
            .expect("flush should succeed");

        let before = engine
            .stats("profiles")
            .await
            .expect("stats should succeed");
        assert_eq!(before.segment_count, 2);

        engine
            .compact("profiles")
            .await
            .expect("compaction should succeed");

        let after = engine
            .stats("profiles")
            .await
            .expect("stats should succeed");
        assert_eq!(after.segment_count, 1);

        let visible = engine
            .scan_exact("profiles", None)
            .await
            .expect("scan should succeed");
        assert_eq!(visible.len(), 2);
        let alpha = visible
            .iter()
            .find(|record| record.id.as_str() == "alpha")
            .expect("alpha should be present");
        assert_eq!(alpha.vector, vec![2.0, 2.0]);
    }

    #[tokio::test]
    async fn old_snapshot_remains_readable_after_flush() {
        let root = unique_temp_dir("storage-snapshot-flush");
        let engine = LocalStorageEngine::new(&root);

        let descriptor = engine
            .create_collection(CreateCollectionRequest {
                name: "events".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Cosine,
            })
            .await
            .expect("collection should be created");

        engine
            .write(
                "events",
                vec![WriteOperation::Put(PutRecord {
                    id: RecordId::new("evt-1"),
                    vector: vec![1.0, 2.0],
                    metadata: json!({"kind":"login"}),
                })],
            )
            .await
            .expect("write should succeed");

        let snapshot = engine
            .snapshot("events")
            .await
            .expect("snapshot should succeed");
        engine.flush("events").await.expect("flush should succeed");

        let visible = engine
            .scan_exact("events", Some(snapshot))
            .await
            .expect("old snapshot should still scan");
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].id.as_str(), "evt-1");

        let wal_dir = descriptor.root_path.join("wal");
        let rolled = fs::read_dir(wal_dir)
            .expect("wal dir should exist")
            .filter_map(|entry| entry.ok().map(|value| value.path()))
            .filter(|path| {
                path.file_name()
                    .map(|name| name != "active.wal")
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(rolled, 1);
    }

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
