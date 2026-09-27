//! `LocalStorageEngine`: the filesystem-backed engine, its storage-root claim, and its `StorageEngine` implementation.

use crate::{
    BlobStore, CreateCollectionRequest, InspectReport, InspectTarget, StorageEngine,
    error::{io_message, json_message},
    maintenance::MaintenanceOperation,
    manifest::{SegmentMeta, segment_artifact_file_name},
    metric::{storage_metric_compare, storage_metric_value},
    resolve::{ResolvedState, resolve_latest_state_for_ids_selected},
    root_lock::StorageRootLock,
    segment_v1::read_segment_file,
    state::resolve_snapshot,
    wal_rotation::wal_rotation_lock,
};
use async_trait::async_trait;
use logpose_catalog::CollectionDescriptor;
use logpose_index::{is_unsupported_hnsw_version, read_flat_index, read_hnsw_index};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, AnnCandidate, AnnSearchRequest, CollectionAssignment,
    CollectionStats, CommitAck, DistanceMetric, LeadershipFence, LogPoseError, MaintenanceStatus,
    NodeRole, RecordId, Result, SeqNo, Snapshot, VisibleRecord, WriteOperation,
};
use logpose_wal::{WalBatch, WalRecord, WalWriter};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

/// Score a segment's records exactly, standing in for its HNSW sidecar when that cannot be read.
///
/// The candidates match what the sidecar would hold: the latest record per id in the segment,
/// when it is a put visible at `visible_seq_no` and admitted by `filter`, best `budget` first.
fn exact_segment_candidates(
    collection_root: &Path,
    segment: &SegmentMeta,
    metric: DistanceMetric,
    query: &[f32],
    visible_seq_no: SeqNo,
    budget: usize,
    filter: Option<&(dyn for<'a> Fn(&'a Value) -> bool + Send + Sync)>,
) -> Result<Vec<AnnCandidate>> {
    let records = read_segment_file(&collection_root.join("segments").join(&segment.file_name))?;
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::new();
    for record in records.iter().rev() {
        if !seen.insert(record.op.id()) {
            continue;
        }
        let WriteOperation::Put(put) = &record.op else {
            continue;
        };
        if record.seq_no > visible_seq_no || filter.is_some_and(|filter| !filter(&put.metadata)) {
            continue;
        }
        candidates.push(AnnCandidate {
            unit_id: segment.segment_id.clone(),
            record_id: put.id.clone(),
            seq_no: record.seq_no,
            value: storage_metric_value(metric, query, &put.vector)?,
        });
    }
    candidates.sort_by(|left, right| {
        storage_metric_compare(metric, right.value, left.value)
            .then(left.record_id.cmp(&right.record_id))
    });
    candidates.truncate(budget);
    Ok(candidates)
}

/// Local filesystem-backed storage engine.
///
/// Opening an engine claims exclusive ownership of its storage root for this process by locking
/// `<root>/LOCK`; the claim is held until the last clone of every engine on that root in this
/// process is dropped. Engines in the same process share the claim.
#[derive(Clone)]
pub struct LocalStorageEngine {
    pub(crate) root: PathBuf,
    pub(crate) blob_store: Option<Arc<dyn BlobStore>>,
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
            let segment_candidates = match read_hnsw_index(&hnsw_path) {
                Ok(hnsw) => logpose_index::search_hnsw(
                    &hnsw,
                    &request.vector,
                    request_budget,
                    filter.as_deref(),
                )
                .map_err(|error| io_message("failed to search hnsw sidecar", error))?
                .candidates
                .into_iter()
                .filter(|candidate| candidate.seq_no <= snapshot.visible_seq_no)
                .map(|candidate| AnnCandidate {
                    unit_id: segment.segment_id.clone(),
                    record_id: candidate.record_id,
                    seq_no: candidate.seq_no,
                    value: candidate.value,
                })
                .collect::<Vec<_>>(),
                // A sidecar from an older graph layout cannot be traversed, but the segment's
                // records are still readable: score them exactly rather than fail the query.
                // A compaction that merges the segment writes a current sidecar.
                Err(error) if is_unsupported_hnsw_version(&error) => exact_segment_candidates(
                    &state.descriptor.root_path,
                    segment,
                    metric,
                    &request.vector,
                    snapshot.visible_seq_no,
                    request_budget,
                    filter.as_deref(),
                )?,
                Err(error) => return Err(io_message("failed to read hnsw sidecar", error)),
            };
            for candidate in segment_candidates {
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
                                "max_neighbors_layer0": hnsw.params.max_neighbors_for_layer(0),
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
