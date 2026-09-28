//! The `StorageEngine` trait and the public request, inspection and blob-store types around it.

use crate::read::CollectionReader;
use async_trait::async_trait;
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    CollectionAssignment, CollectionRef, CollectionStats, CommitAck, DEFAULT_DATABASE_NAME,
    DistanceMetric, LeadershipFence, LogPoseError, MaintenanceStatus, Result, Snapshot,
    WriteOperation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Collection lifecycle, writes, maintenance, and statistics over one storage root.
///
/// Data reads go through the [`CollectionReader`] supertrait: a read resolves one
/// [`ReadView`](crate::ReadView) and `logpose-query` executes over it.
#[async_trait]
pub trait StorageEngine: CollectionReader + Send + Sync {
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

    /// Persist one or more write operations durably.
    async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck>;

    /// Capture the current manifest generation and visible sequence boundary.
    async fn snapshot(&self, collection_name: &str) -> Result<Snapshot>;

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
    /// Human-readable collection name.
    pub name: String,
    /// Fixed embedding dimensionality.
    pub dimensions: usize,
    /// Distance metric reserved for future query layers.
    pub metric: DistanceMetric,
}

impl CreateCollectionRequest {
    /// Create a collection request in the default database namespace.
    #[must_use]
    pub fn new(name: impl Into<String>, dimensions: usize, metric: DistanceMetric) -> Self {
        Self::in_database(DEFAULT_DATABASE_NAME, name, dimensions, metric)
    }

    /// Create a collection request in an explicit database namespace.
    #[must_use]
    pub fn in_database(
        database_name: impl Into<String>,
        name: impl Into<String>,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> Self {
        Self {
            database_name: database_name.into(),
            name: name.into(),
            dimensions,
            metric,
        }
    }

    /// Return the canonical database/collection reference for this request.
    #[must_use]
    pub fn collection_ref(&self) -> CollectionRef {
        let request = self.clone().with_defaults();
        CollectionRef::new(request.database_name, request.name)
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
            name: self.name,
            dimensions: self.dimensions,
            metric: self.metric,
        }
    }
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
