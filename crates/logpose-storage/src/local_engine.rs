//! `LocalStorageEngine`: the `StorageEngine` implementation over an [`Engine`].
//!
//! Every trait method resolves its collection with a map lookup and runs its blocking work on
//! the engine's I/O pool (reads) or maintenance job threads (flush and compaction), never on a
//! tokio worker. Writes go to the collection's writer task, which does its own I/O on the I/O
//! pool. Reads of the current state use the published `Version` and read no metadata files.

use crate::{
    BlobStore, CreateCollectionRequest, InspectReport, InspectTarget, StorageEngine,
    collections::collection_ref_from_lookup,
    durable_fs::{path_exists, read_file},
    engine::{CoreRef, Engine, EngineConfig, EngineCore, not_found},
    error::{io_message, json_message},
    handle::CollectionHandle,
    legacy_view::{legacy_ops, legacy_record},
    maintenance::MaintenanceOperation,
    manifest::{SegmentMeta, segment_artifact_file_name},
    metric::{storage_metric_compare, storage_metric_value},
    resolve::{ResolvedState, resolve_latest_state_for_ids_selected},
    segment_v1::read_segment_file,
    tokens::SnapshotToken,
};
use async_trait::async_trait;
use logpose_catalog::CollectionDescriptor;
use logpose_index::{
    FlatIndexSidecar, HnswIndexSidecar, decode_flat_index, decode_hnsw_index,
    is_unsupported_hnsw_version,
};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, AnnCandidate, AnnSearchRequest, CollectionAssignment, CollectionRef,
    CollectionStats, CommitAck, CorruptionKind, DistanceMetric, LeadershipFence, LogPoseError,
    MaintenanceStatus, NodeRole, RecordId, ResourceKind, Result, SeqNo, Snapshot, VisibleRecord,
    WriteOperation,
};
use logpose_vfs::{Vfs, std_vfs};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

/// A metadata filter over record metadata, as the query layer passes it down.
type MetadataFilter = Arc<dyn for<'a> Fn(&'a Value) -> bool + Send + Sync>;

/// Local filesystem-backed storage engine: the [`StorageEngine`] trait over an [`Engine`].
///
/// Every file access goes through the engine's [`Vfs`]: [`StdVfs`](logpose_vfs::StdVfs) for
/// the convenience constructors, or any `Vfs` passed to [`LocalStorageEngine::with_vfs`] (tests
/// use [`FaultVfs`](logpose_vfs::FaultVfs) to inject crashes).
///
/// Opening claims the storage root exclusively: a second engine on the same root fails with
/// [`LogPoseError::StorageRootLocked`], in this process or another. Share one engine by cloning
/// it; the root is released when the last clone is dropped.
#[derive(Clone, Debug)]
pub struct LocalStorageEngine {
    engine: Engine,
}

impl LocalStorageEngine {
    /// Open a local storage engine rooted at the provided path on the real filesystem.
    ///
    /// Creates the root directory if needed and fails if another engine holds the root.
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        Self::with_blob_store(root, None)
    }

    /// Open a local storage engine on the real filesystem with an optional blob-store
    /// implementation.
    ///
    /// Creates the root directory if needed and fails if another engine holds the root.
    pub fn with_blob_store(
        root: impl AsRef<Path>,
        blob_store: Option<Arc<dyn BlobStore>>,
    ) -> Result<Self> {
        Self::with_vfs(std_vfs(), root, blob_store)
    }

    /// Open a local storage engine that performs all file I/O through `vfs`.
    ///
    /// Creates the root directory if needed and fails if another engine holds the root.
    pub fn with_vfs(
        vfs: Arc<dyn Vfs>,
        root: impl AsRef<Path>,
        blob_store: Option<Arc<dyn BlobStore>>,
    ) -> Result<Self> {
        let config = EngineConfig {
            blob_store,
            ..EngineConfig::default()
        };
        Ok(Self::from_engine(Engine::open(vfs, root, config)?))
    }

    /// Serve the `StorageEngine` trait over an open engine.
    #[must_use]
    pub fn from_engine(engine: Engine) -> Self {
        Self { engine }
    }

    /// The engine behind this adapter.
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The filesystem this engine performs all I/O through.
    #[must_use]
    pub fn vfs(&self) -> &Arc<dyn Vfs> {
        self.engine.vfs()
    }

    /// Build the descriptor that would be persisted for one collection request.
    pub fn plan_collection_descriptor(
        &self,
        request: &CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        self.engine.core().plan_collection_descriptor(request)
    }

    /// Persist a collection using a previously planned descriptor. Blocking.
    pub fn create_collection_from_descriptor(
        &self,
        descriptor: CollectionDescriptor,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<CollectionDescriptor> {
        self.engine
            .create_collection(descriptor, assignment)
            .map(|handle| handle.descriptor().clone())
    }

    /// [`LocalStorageEngine::create_collection_from_descriptor`] on the I/O pool, for async
    /// callers.
    pub async fn create_collection_from_descriptor_async(
        &self,
        descriptor: CollectionDescriptor,
        assignment: Option<CollectionAssignment>,
    ) -> Result<CollectionDescriptor> {
        self.engine
            .io(move |core| {
                core.create_collection(descriptor, assignment.as_ref())
                    .map(|handle| handle.descriptor().clone())
            })
            .await
    }

    /// Open a collection descriptor using an explicit database namespace.
    pub async fn open_collection_in_database(
        &self,
        database_name: &str,
        name: &str,
    ) -> Result<CollectionDescriptor> {
        self.engine
            .collection(&CollectionRef::new(database_name, name))
            .map(|handle| handle.descriptor().clone())
    }

    /// Pin the current state of `collection_name` for repeatable reads, returning the token and
    /// the snapshot it names. While the token lives, reads through it (and exact reads of that
    /// snapshot) see exactly this state, across flushes and compactions (I12).
    ///
    /// Fails with [`LogPoseError::TooManySnapshots`] when the collection already holds the most
    /// pins it allows or pinned snapshots exceed the engine's pinned-memory limit.
    pub fn pin_snapshot(&self, collection_name: &str) -> Result<(SnapshotToken, Snapshot)> {
        let handle = self.handle(collection_name)?;
        handle.ensure_open()?;
        let version = handle.current();
        let token = handle.pin_version(Arc::clone(&version))?;
        Ok((token, version.snapshot()))
    }

    /// Unpin `token`. Returns whether it was pinned.
    pub fn release_snapshot(&self, collection_name: &str, token: &SnapshotToken) -> Result<bool> {
        Ok(self.handle(collection_name)?.release_snapshot(token))
    }

    /// Every visible record of the state `token` pins, extending the token's expiry. Fails with
    /// [`LogPoseError::SnapshotExpired`] once the token expired or was released.
    pub async fn scan_exact_at_token(
        &self,
        collection_name: &str,
        token: SnapshotToken,
    ) -> Result<Vec<VisibleRecord>> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| {
            core.scan_exact_internal(handle, token, true, None)
        })
        .await
    }

    /// Statistics of the state `token` pins, extending the token's expiry.
    pub async fn stats_at_token(
        &self,
        collection_name: &str,
        token: SnapshotToken,
    ) -> Result<CollectionStats> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| {
            core.collection_stats(handle, token)
        })
        .await
    }

    fn handle(&self, name: &str) -> Result<Arc<CollectionHandle>> {
        self.engine.collection(&collection_ref_from_lookup(name))
    }

    /// Run data-plane work for `handle` on the I/O pool. The first data-plane access of a
    /// recovered collection resumes its persisted maintenance, as a v1 state load did.
    async fn data_io<T: Send + 'static>(
        &self,
        handle: Arc<CollectionHandle>,
        f: impl FnOnce(&CoreRef, &Arc<CollectionHandle>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.engine
            .io(move |core| {
                core.resume_armed_maintenance(&handle);
                f(core, &handle)
            })
            .await
    }

    /// The handle serving `descriptor`, which must name the same collection (same id).
    fn handle_for(&self, descriptor: &CollectionDescriptor) -> Result<Arc<CollectionHandle>> {
        let reference = descriptor.collection_ref();
        let handle = self.engine.collection(&reference)?;
        if handle.meta().id != descriptor.collection_id {
            return Err(not_found(&reference));
        }
        Ok(handle)
    }

    async fn create(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
    ) -> Result<CollectionDescriptor> {
        self.engine
            .io(move |core| {
                let descriptor = core.plan_collection_descriptor(&request)?;
                core.create_collection(descriptor, Some(&assignment))
                    .map(|handle| handle.descriptor().clone())
            })
            .await
    }

    async fn maintain(
        &self,
        collection_name: &str,
        operation: MaintenanceOperation,
    ) -> Result<Snapshot> {
        let handle = self.handle(collection_name)?;
        self.engine
            .job(move |core| {
                core.resume_armed_maintenance(&handle);
                core.perform_maintenance(&handle, operation)
            })
            .await
    }

    async fn stats_of(
        &self,
        handle: Arc<CollectionHandle>,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        self.data_io(handle, move |core, handle| {
            core.collection_stats(handle, snapshot)
        })
        .await
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
        self.create(
            request,
            CollectionAssignment {
                assigned_node: ANONYMOUS_LOCAL_NODE_NAME.to_owned(),
                assigned_role: NodeRole::Data,
            },
        )
        .await
    }

    async fn create_collection_with_assignment(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
        _leader_fence: Option<LeadershipFence>,
    ) -> Result<CollectionDescriptor> {
        self.create(request, assignment).await
    }

    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor> {
        self.handle(name).map(|handle| handle.descriptor().clone())
    }

    async fn has_local_collection(&self, name: &str) -> Result<bool> {
        Ok(self.handle(name).is_ok())
    }

    async fn local_collection_matches_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<bool> {
        match self.handle(&descriptor.lookup_name()) {
            Ok(handle) => Ok(handle.descriptor().matches_serving_identity(descriptor)),
            Err(error) if error.to_string().contains("does not exist") => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn list_collections(&self) -> Result<Vec<CollectionDescriptor>> {
        self.engine.core().list_descriptors()
    }

    async fn collection_assignment_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        self.handle_for(descriptor)?
            .meta()
            .assignment
            .clone()
            .ok_or_else(|| {
                LogPoseError::internal(format!(
                    "collection '{}' is missing placement metadata",
                    descriptor.name
                ))
            })
    }

    async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        let handle = self.handle(collection_name)?;
        if handle.take_maintenance_resume() {
            let core = self.engine.core();
            let resumed = Arc::clone(&handle);
            // Resuming persists the maintenance status: blocking I/O, so not on this worker.
            let _ = self
                .engine
                .runtime()
                .io
                .execute(move || core.resume_maintenance(&resumed));
        }
        let ops = legacy_ops(handle.descriptor(), operations)?;
        handle.write(ops).await
    }

    async fn snapshot(&self, collection_name: &str) -> Result<Snapshot> {
        let handle = self.handle(collection_name)?;
        handle.ensure_open()?;
        Ok(handle.current().snapshot())
    }

    async fn scan_exact(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<VisibleRecord>> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| {
            core.scan_exact_internal(handle, snapshot, true, None)
        })
        .await
    }

    async fn scan_exact_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| {
            core.scan_exact_internal(
                handle,
                snapshot,
                include_mutable,
                Some(immutable_unit_ids.into_iter().collect()),
            )
        })
        .await
    }

    async fn ann_search_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        immutable_unit_ids: Vec<String>,
        request: AnnSearchRequest,
        filter: Option<MetadataFilter>,
    ) -> Result<Vec<AnnCandidate>> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| {
            core.ann_search_selected(handle, snapshot, immutable_unit_ids, &request, filter)
        })
        .await
    }

    async fn latest_visible_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        record_ids: Vec<RecordId>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| {
            core.latest_visible_selected(
                handle,
                snapshot,
                record_ids,
                include_mutable,
                immutable_unit_ids,
            )
        })
        .await
    }

    async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        self.maintain(collection_name, MaintenanceOperation::Flush)
            .await
    }

    async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        self.maintain(collection_name, MaintenanceOperation::Compact)
            .await
    }

    async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        self.stats_snapshot(collection_name, None).await
    }

    async fn stats_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let handle = self.handle_for(descriptor)?;
        self.stats_of(handle, snapshot).await
    }

    async fn maintenance_status_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        let handle = self.handle_for(descriptor)?;
        Ok(self.engine.core().maintenance_status(&handle))
    }

    async fn recover_maintenance_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<()> {
        let handle = self.handle_for(descriptor)?;
        self.engine
            .io(move |core| {
                handle.take_maintenance_resume();
                core.resume_maintenance(&handle);
                Ok(())
            })
            .await
    }

    async fn stats_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let handle = self.handle(collection_name)?;
        self.stats_of(handle, snapshot).await
    }

    async fn inspect(&self, collection_name: &str, target: InspectTarget) -> Result<InspectReport> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| core.inspect(handle, target))
            .await
    }
}

impl CoreRef {
    /// Commit v1 `operations` as one batch through the collection's writer. Blocking; for
    /// threads outside any async runtime (tests, job threads).
    #[cfg(test)]
    pub(crate) fn write(
        &self,
        handle: &Arc<CollectionHandle>,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        let ops = legacy_ops(handle.descriptor(), operations)?;
        handle.write_blocking(ops)
    }
}

impl EngineCore {
    pub(crate) fn exists(&self, path: &Path) -> Result<bool> {
        path_exists(self.vfs.as_ref(), path)
    }

    fn read_hnsw_sidecar(&self, path: &Path) -> Result<HnswIndexSidecar> {
        let bytes = read_file(self.vfs.as_ref(), path, "failed to read hnsw sidecar")?;
        decode_hnsw_index(bytes, path)
            .map_err(|error| io_message("failed to read hnsw sidecar", error))
    }

    /// Read an HNSW sidecar the ANN path can traverse. `Ok(None)` means the sidecar has a graph
    /// layout this build does not read; the segment's records are still readable, so the caller
    /// scores them exactly instead of failing the query.
    fn read_current_hnsw_sidecar(&self, path: &Path) -> Result<Option<HnswIndexSidecar>> {
        let bytes = read_file(self.vfs.as_ref(), path, "failed to read hnsw sidecar")?;
        match decode_hnsw_index(bytes, path) {
            Ok(hnsw) => Ok(Some(hnsw)),
            Err(error) if is_unsupported_hnsw_version(&error) => Ok(None),
            Err(error) => Err(io_message("failed to read hnsw sidecar", error)),
        }
    }

    fn read_flat_sidecar(&self, path: &Path) -> Result<FlatIndexSidecar> {
        let bytes = read_file(self.vfs.as_ref(), path, "failed to read flat index sidecar")?;
        decode_flat_index(&bytes)
            .map_err(|error| io_message("failed to read flat index sidecar", error))
    }

    fn ann_search_selected(
        &self,
        handle: &CollectionHandle,
        at: impl Into<crate::state::ReadAt>,
        immutable_unit_ids: Vec<String>,
        request: &AnnSearchRequest,
        filter: Option<MetadataFilter>,
    ) -> Result<Vec<AnnCandidate>> {
        let (state, snapshot) = self.read_state(handle, at)?;
        let descriptor = handle.descriptor();
        let metric = descriptor.metric;
        let selected = immutable_unit_ids.into_iter().collect::<BTreeSet<_>>();
        let mut candidates_by_record_id = BTreeMap::<RecordId, AnnCandidate>::new();
        let request_budget = request.candidate_budget.max(request.top_k);

        for segment in state
            .manifest
            .legacy_segments()
            .rev()
            .filter(|segment| selected.contains(&segment.segment_id))
        {
            let hnsw_path = descriptor.root_path.join("indexes").join(
                segment_artifact_file_name(segment, "hnsw").ok_or_else(|| {
                    LogPoseError::corrupt(
                        CorruptionKind::Manifest,
                        format!(
                            "segment '{}' is missing hnsw artifact metadata",
                            segment.segment_id
                        ),
                    )
                })?,
            );
            let segment_candidates = match self.read_current_hnsw_sidecar(&hnsw_path)? {
                Some(hnsw) => logpose_index::search_hnsw(
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
                None => exact_segment_candidates(
                    self.vfs.as_ref(),
                    &descriptor.root_path,
                    segment,
                    metric,
                    &request.vector,
                    snapshot.visible_seq_no,
                    request_budget,
                    filter.as_deref(),
                )?,
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

    fn latest_visible_selected(
        &self,
        handle: &CollectionHandle,
        at: impl Into<crate::state::ReadAt>,
        record_ids: Vec<RecordId>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        let (state, snapshot) = self.read_state(handle, at)?;
        let resolved = resolve_latest_state_for_ids_selected(
            self.vfs.as_ref(),
            handle.descriptor(),
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

    fn inspect(&self, handle: &CollectionHandle, target: InspectTarget) -> Result<InspectReport> {
        handle.ensure_open()?;
        let version = handle.current();
        let descriptor = handle.descriptor();
        match target {
            InspectTarget::Manifest => Ok(InspectReport {
                target: "manifest".to_owned(),
                payload: version.manifest.inspect_json(),
            }),
            InspectTarget::Wal => {
                let mut records = Vec::with_capacity(version.delta.len());
                for record in version.delta.iter() {
                    records.extend(legacy_record(&version.schema, record)?);
                }
                Ok(InspectReport {
                    target: "wal".to_owned(),
                    payload: json!({
                        "checkpoint_seq_no": version.checkpoint_seq_no,
                        "schema_version": version.schema.schema_version(),
                        "records": records,
                    }),
                })
            }
            InspectTarget::Maintenance => Ok(InspectReport {
                target: "maintenance".to_owned(),
                payload: serde_json::to_value(self.maintenance_status(handle))
                    .map_err(json_message)?,
            }),
            InspectTarget::Segment(segment_id) => {
                let segment = version
                    .manifest
                    .legacy_segments()
                    .find(|segment| segment.segment_id == segment_id)
                    .ok_or_else(|| {
                        LogPoseError::not_found(ResourceKind::Segment, segment_id.clone())
                    })?;
                let records = read_segment_file(
                    self.vfs.as_ref(),
                    &descriptor
                        .root_path
                        .join("segments")
                        .join(&segment.file_name),
                )?;
                let index = self.read_flat_sidecar(&descriptor.root_path.join("indexes").join(
                    segment_artifact_file_name(segment, "flat_exact").ok_or_else(|| {
                        LogPoseError::corrupt(
                            CorruptionKind::Manifest,
                            format!(
                                "segment '{}' is missing flat artifact metadata",
                                segment.segment_id
                            ),
                        )
                    })?,
                ))?;
                let hnsw = self.read_hnsw_sidecar(&descriptor.root_path.join("indexes").join(
                    segment_artifact_file_name(segment, "hnsw").ok_or_else(|| {
                        LogPoseError::corrupt(
                            CorruptionKind::Manifest,
                            format!(
                                "segment '{}' is missing hnsw artifact metadata",
                                segment.segment_id
                            ),
                        )
                    })?,
                ))?;
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

/// Score a segment's records exactly, standing in for its HNSW sidecar when that cannot be read.
///
/// The candidates match what the sidecar would hold: the latest record per id in the segment,
/// when it is a put visible at `visible_seq_no` and admitted by `filter`, best `budget` first.
#[allow(clippy::too_many_arguments)]
fn exact_segment_candidates(
    vfs: &dyn Vfs,
    collection_root: &Path,
    segment: &SegmentMeta,
    metric: DistanceMetric,
    query: &[f32],
    visible_seq_no: SeqNo,
    budget: usize,
    filter: Option<&(dyn for<'a> Fn(&'a Value) -> bool + Send + Sync)>,
) -> Result<Vec<AnnCandidate>> {
    let records = read_segment_file(
        vfs,
        &collection_root.join("segments").join(&segment.file_name),
    )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::unique_temp_dir;
    use std::fs;

    #[test]
    fn engine_open_fails_while_another_engine_holds_the_storage_root() {
        let root = unique_temp_dir("storage-root-lock");
        let first = LocalStorageEngine::new(&root).expect("first engine should open");
        let error = LocalStorageEngine::new(&root)
            .expect_err("a second engine on the same root must not open");
        assert!(
            matches!(error, LogPoseError::StorageRootLocked { .. }),
            "unexpected error: {error}"
        );
        let shared = first.clone();
        drop(first);
        assert!(
            LocalStorageEngine::new(&root).is_err(),
            "a clone keeps the root locked"
        );
        drop(shared);

        // An independent handle on LOCK is what another process looks like to the OS.
        let foreign = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join("LOCK"))
            .expect("engine should have created the lock file");
        foreign
            .try_lock()
            .expect("the root should be released once every clone is dropped");

        let error = LocalStorageEngine::new(&root)
            .expect_err("engine must not open a root held by another process");
        assert!(
            error
                .to_string()
                .contains("is already in use by another engine"),
            "unexpected error: {error}"
        );

        drop(foreign);
        LocalStorageEngine::new(&root).expect("engine should open after the holder exits");
    }
}
