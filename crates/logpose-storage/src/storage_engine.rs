//! The `StorageEngine` trait and the public request, inspection and blob-store types around it.

use crate::metric::{storage_metric_compare, storage_metric_value};
use async_trait::async_trait;
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    AnnCandidate, AnnSearchRequest, CollectionAssignment, CollectionRef, CollectionStats,
    CommitAck, DEFAULT_DATABASE_NAME, DistanceMetric, LeadershipFence, LogPoseError,
    MaintenanceStatus, RecordId, Result, Snapshot, VisibleRecord, WriteOperation,
    legacy::{LEGACY_PRIMARY_KEY_FIELD, LEGACY_VECTOR_FIELD},
    record::{ClientOp, PrimaryKey, Record},
    schema::{
        CollectionSchema, CreateCollectionSpec, PrimaryKeySpec, PrimaryKeyType, SchemaChange,
        VectorFieldSpec,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};

/// Durable storage surface for future engine implementations.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Return a short identifier for the engine implementation.
    async fn engine_name(&self) -> &'static str;

    /// Verify that the engine's metadata authority is currently reachable.
    async fn metadata_status(&self) -> Result<()> {
        Ok(())
    }

    /// Create a new collection rooted under the engine storage path.
    async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<CollectionDescriptor>;

    /// Create a collection and persist its initial placement assignment.
    async fn create_collection_with_assignment(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
        leader_fence: Option<LeadershipFence>,
    ) -> Result<CollectionDescriptor> {
        let _ = request;
        let _ = assignment;
        let _ = leader_fence;
        Err(LogPoseError::internal(
            "persisted collection assignments are not supported by this storage engine".to_owned(),
        ))
    }

    /// Open an existing collection by name.
    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor>;

    /// Return whether the collection's local on-disk state exists on this node.
    async fn has_local_collection(&self, name: &str) -> Result<bool> {
        match self.open_collection(name).await {
            Ok(_) => Ok(true),
            Err(error) if error.to_string().contains("does not exist") => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Return whether the local on-disk descriptor matches the authoritative descriptor.
    async fn local_collection_matches_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<bool> {
        if !self.has_local_collection(&descriptor.lookup_name()).await? {
            return Ok(false);
        }
        let local_descriptor = self.open_collection(&descriptor.lookup_name()).await?;
        Ok(local_descriptor.matches_serving_identity(descriptor))
    }

    /// List every known collection descriptor.
    async fn list_collections(&self) -> Result<Vec<CollectionDescriptor>> {
        Err(LogPoseError::internal(
            "listing collections is not supported by this storage engine".to_owned(),
        ))
    }

    /// Drop a collection: refuse new calls on it, wait for its in-flight write and
    /// maintenance job, and durably remove its files. A metadata-store-backed engine also
    /// removes the collection's metadata, fenced by `leader_fence`.
    async fn drop_collection(
        &self,
        collection_name: &str,
        leader_fence: Option<LeadershipFence>,
    ) -> Result<()> {
        let _ = collection_name;
        let _ = leader_fence;
        Err(unsupported("dropping collections"))
    }

    /// The collection's live schema: the schema of its current published state.
    async fn schema(&self, collection_name: &str) -> Result<Arc<CollectionSchema>> {
        let _ = collection_name;
        Err(unsupported("reading schemas"))
    }

    /// Change the collection's schema online, ordered with the writes around it. Durable and
    /// visible once this returns.
    async fn alter_schema(&self, collection_name: &str, change: SchemaChange) -> Result<CommitAck> {
        let _ = collection_name;
        let _ = change;
        Err(unsupported("schema changes"))
    }

    /// Durably commit `ops` (upserts, partial updates, deletes by key) as one atomic batch,
    /// validated against the collection's schema at its point in the write stream.
    ///
    /// Validation errors name the operation by its position, relative to the batch: a field
    /// path such as `[2].price`, which callers prefix with the request field that holds the
    /// batch (see [`LogPoseError::with_field_prefix`]).
    async fn write_batch(&self, collection_name: &str, ops: Vec<ClientOp>) -> Result<CommitAck> {
        let _ = collection_name;
        let _ = ops;
        Err(unsupported("typed writes"))
    }

    /// Point lookups of `keys` in the current state, each projected to `output_fields` (see
    /// [`Projection::resolve`](logpose_types::record::Projection::resolve)).
    async fn get_records(
        &self,
        collection_name: &str,
        keys: Vec<PrimaryKey>,
        output_fields: Vec<String>,
    ) -> Result<FetchedRecords> {
        let _ = collection_name;
        let _ = keys;
        let _ = output_fields;
        Err(unsupported("point lookups"))
    }

    /// Load the persisted placement assignment for a collection descriptor.
    async fn collection_assignment_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        let _ = descriptor;
        Err(LogPoseError::internal(
            "persisted collection assignments are not supported by this storage engine".to_owned(),
        ))
    }

    /// Persist v1-shaped write operations durably. Only for collections of the single-vector
    /// shape [`CreateCollectionRequest::new`] creates; the APIs use
    /// [`write_batch`](Self::write_batch). Deleted with the legacy read paths.
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
        // The legacy read paths search the first vector field.
        let metric = descriptor
            .schema
            .vectors()
            .first()
            .map(|field| field.metric)
            .ok_or_else(|| LogPoseError::internal("a collection schema has no vector field"))?;
        let mut scored = filtered_records
            .into_iter()
            .map(|record| {
                storage_metric_value(metric, &request.vector, &record.vector)
                    .map(|value| (record, value))
            })
            .collect::<Result<Vec<_>>>()?;
        scored.sort_by(|(left_record, left_value), (right_record, right_value)| {
            storage_metric_compare(metric, *right_value, *left_value)
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

    /// Return collection-level statistics using a previously loaded descriptor.
    async fn stats_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let _ = snapshot;
        self.stats(&descriptor.lookup_name()).await
    }

    /// Return persisted maintenance state without reconstructing full collection stats.
    async fn maintenance_status_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        Ok(self.stats_descriptor(descriptor, None).await?.maintenance)
    }

    /// Resume persisted maintenance state for a descriptor when the runtime can serve it locally.
    async fn recover_maintenance_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<()> {
        let _ = descriptor;
        Ok(())
    }

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
    /// Database containing the collection. Blank values default to `default`.
    pub database_name: String,
    /// The collection's name and schema, as the create request carries them (engine plan
    /// decision D4). Validated when the collection is planned, so errors name spec fields
    /// such as `vectors[0].dimensions`.
    pub spec: CreateCollectionSpec,
}

impl CreateCollectionRequest {
    /// Create a collection from a schema-based spec in an explicit database namespace.
    #[must_use]
    pub fn from_spec(database_name: impl Into<String>, spec: CreateCollectionSpec) -> Self {
        Self {
            database_name: database_name.into(),
            spec,
        }
    }

    /// A request in the default database for the single-vector shape: string primary key
    /// `id`, one vector field `vector` with `dimensions` and `metric`, and dynamic fields on.
    #[must_use]
    pub fn new(name: impl Into<String>, dimensions: usize, metric: DistanceMetric) -> Self {
        Self::in_database(DEFAULT_DATABASE_NAME, name, dimensions, metric)
    }

    /// [`CreateCollectionRequest::new`] in an explicit database namespace.
    #[must_use]
    pub fn in_database(
        database_name: impl Into<String>,
        name: impl Into<String>,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> Self {
        Self::from_spec(
            database_name,
            CreateCollectionSpec {
                name: name.into(),
                primary_key: PrimaryKeySpec {
                    name: LEGACY_PRIMARY_KEY_FIELD.to_owned(),
                    key_type: PrimaryKeyType::String,
                },
                vectors: vec![VectorFieldSpec {
                    name: LEGACY_VECTOR_FIELD.to_owned(),
                    // Out-of-range dimensions fail validation when the collection is planned.
                    dimensions: u32::try_from(dimensions).unwrap_or(u32::MAX),
                    metric,
                }],
                fields: Vec::new(),
                dynamic_fields: true,
            },
        )
    }

    /// The collection name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.spec.name
    }

    /// Return the canonical database/collection reference for this request.
    #[must_use]
    pub fn collection_ref(&self) -> CollectionRef {
        let request = self.clone().with_defaults();
        CollectionRef::new(request.database_name, request.spec.name)
    }

    /// Return the canonical database/collection lookup key for this request.
    #[must_use]
    pub fn lookup_name(&self) -> String {
        self.collection_ref().lookup_name()
    }

    pub(crate) fn with_defaults(self) -> Self {
        let database_name = if self.database_name.trim().is_empty() {
            DEFAULT_DATABASE_NAME.to_owned()
        } else {
            self.database_name
        };
        Self {
            database_name,
            spec: self.spec,
        }
    }
}

/// The live records a point lookup found, read from one published state.
#[derive(Clone, Debug, PartialEq)]
pub struct FetchedRecords {
    /// The schema of that state, which the records follow.
    pub schema: Arc<CollectionSchema>,
    /// The state the lookup read.
    pub snapshot: Snapshot,
    /// One entry per requested key, in request order: the projected record, or `None` when
    /// the key has no live record.
    pub records: Vec<Option<Record>>,
}

fn unsupported(what: &str) -> LogPoseError {
    LogPoseError::internal(format!("{what} is not supported by this storage engine"))
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
