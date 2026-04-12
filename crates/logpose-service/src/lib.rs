//! Shared application service orchestration for LogPose data APIs.

#[cfg(test)]
use axum as _;
#[cfg(test)]
use http_body_util as _;
#[cfg(test)]
use logpose_api_grpc as _;
#[cfg(test)]
use logpose_api_rest as _;
#[cfg(test)]
use logpose_config as _;
#[cfg(test)]
use logpose_core as _;
#[cfg(test)]
use rand as _;
#[cfg(test)]
use serde_json as _;
#[cfg(test)]
use tokio as _;
#[cfg(test)]
use tonic as _;
#[cfg(test)]
use tower as _;

use logpose_query::{QueryError, QueryRequest, QueryResponse, query_exact};
use logpose_storage::{
    CreateCollectionRequest, InspectReport, InspectTarget, LocalStorageEngine, StorageEngine,
};
use logpose_types::{CollectionStats, CommitAck, LogPoseError, Snapshot, WriteOperation};
use std::{fmt, path::Path, sync::Arc};
use thiserror::Error;

/// Service-local result type.
pub type Result<T> = std::result::Result<T, ServiceError>;

/// Shared service errors mapped from storage and query layers.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ServiceError {
    /// The requested resource already exists.
    #[error("{0}")]
    AlreadyExists(String),
    /// The requested resource does not exist.
    #[error("{0}")]
    NotFound(String),
    /// The caller supplied an invalid request.
    #[error("{0}")]
    InvalidArgument(String),
    /// The system failed while processing the request.
    #[error("{0}")]
    Internal(String),
}

/// Shared application orchestration over the current storage and query layers.
#[derive(Clone)]
pub struct LogPoseDataService {
    storage: Arc<dyn StorageEngine>,
}

impl fmt::Debug for LogPoseDataService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogPoseDataService")
            .field("storage_engine", &"<dyn StorageEngine>")
            .finish()
    }
}

impl LogPoseDataService {
    /// Build a service over an arbitrary storage engine implementation.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self { storage }
    }

    /// Build a service over the local filesystem-backed engine.
    #[must_use]
    pub fn local(root: impl AsRef<Path>) -> Self {
        Self::new(Arc::new(LocalStorageEngine::new(root)))
    }

    /// Create a collection.
    pub async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        self.storage
            .create_collection(request)
            .await
            .map_err(Into::into)
    }

    /// Fetch collection metadata by name.
    pub async fn get_collection(
        &self,
        collection_name: &str,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        self.storage
            .open_collection(collection_name)
            .await
            .map_err(Into::into)
    }

    /// Persist a write batch.
    pub async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        self.storage
            .write(collection_name, operations)
            .await
            .map_err(Into::into)
    }

    /// Execute a filtered exact query.
    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse> {
        query_exact(self.storage.as_ref(), request)
            .await
            .map_err(Into::into)
    }

    /// Capture the current read snapshot.
    pub async fn snapshot(&self, collection_name: &str) -> Result<Snapshot> {
        self.storage
            .snapshot(collection_name)
            .await
            .map_err(Into::into)
    }

    /// Return collection-level stats.
    pub async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        self.storage
            .stats(collection_name)
            .await
            .map_err(Into::into)
    }

    /// Flush the mutable delta to a new segment.
    pub async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        self.storage
            .flush(collection_name)
            .await
            .map_err(Into::into)
    }

    /// Compact immutable segments.
    pub async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        self.storage
            .compact(collection_name)
            .await
            .map_err(Into::into)
    }

    /// Inspect arbitrary operator-visible storage state.
    pub async fn inspect(
        &self,
        collection_name: &str,
        target: InspectTarget,
    ) -> Result<InspectReport> {
        self.storage
            .inspect(collection_name, target)
            .await
            .map_err(Into::into)
    }

    /// Inspect the current manifest.
    pub async fn inspect_manifest(&self, collection_name: &str) -> Result<InspectReport> {
        self.inspect(collection_name, InspectTarget::Manifest).await
    }

    /// Inspect the unresolved WAL delta.
    pub async fn inspect_wal(&self, collection_name: &str) -> Result<InspectReport> {
        self.inspect(collection_name, InspectTarget::Wal).await
    }

    /// Inspect a specific segment.
    pub async fn inspect_segment(
        &self,
        collection_name: &str,
        segment_id: String,
    ) -> Result<InspectReport> {
        self.inspect(collection_name, InspectTarget::Segment(segment_id))
            .await
    }
}

impl From<LogPoseError> for ServiceError {
    fn from(error: LogPoseError) -> Self {
        match error {
            LogPoseError::Message(message) => classify_message(message),
        }
    }
}

impl From<QueryError> for ServiceError {
    fn from(error: QueryError) -> Self {
        match error {
            QueryError::RequestVectorDimensionMismatch { .. }
            | QueryError::VectorDimensionMismatch { .. } => {
                Self::InvalidArgument(error.to_string())
            }
            QueryError::StoredVectorDimensionMismatch { .. } => Self::Internal(error.to_string()),
            QueryError::Storage(error) => error.into(),
        }
    }
}

fn classify_message(message: String) -> ServiceError {
    if message.contains("already exists") {
        ServiceError::AlreadyExists(message)
    } else if message.contains("does not exist") {
        ServiceError::NotFound(message)
    } else if message.contains("expected")
        || message.contains("unsupported")
        || message.contains("duplicate record id")
        || message.contains("must include at least one operation")
        || message.contains("must not be empty")
    {
        ServiceError::InvalidArgument(message)
    } else {
        ServiceError::Internal(message)
    }
}
