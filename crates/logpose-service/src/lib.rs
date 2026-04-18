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

use logpose_config::LogPoseConfig;
use logpose_query::{QueryError, QueryRequest, QueryResponse, query_exact};
use logpose_storage::{
    CreateCollectionRequest, InspectReport, InspectTarget, LocalStorageEngine, StorageEngine,
};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, BuildInfo, CollectionAssignment, CollectionPlacement, CollectionRef,
    CollectionStats, CommitAck, LogPoseError, MaintenanceBacklog, MaintenanceStatus, NodeRole,
    NodeRuntimeStatus, Snapshot, WriteOperation,
};
use std::{fmt, net::IpAddr, path::Path, sync::Arc};
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

    /// Create a collection with an explicit persisted placement assignment.
    pub async fn create_collection_with_assignment(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        self.storage
            .create_collection_with_assignment(request, assignment)
            .await
            .map_err(Into::into)
    }

    /// Fetch collection metadata by name.
    pub async fn get_collection(
        &self,
        collection_name: &str,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        self.resolved_collection_descriptor(collection_name).await
    }

    /// List all known collections.
    pub async fn list_collections(&self) -> Result<Vec<logpose_catalog::CollectionDescriptor>> {
        self.storage.list_collections().await.map_err(Into::into)
    }

    /// Load the persisted placement assignment for a descriptor.
    pub async fn collection_assignment_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        self.storage
            .collection_assignment_descriptor(descriptor)
            .await
            .map_err(Into::into)
    }

    /// Return the underlying engine identifier.
    pub async fn engine_name(&self) -> &'static str {
        self.storage.engine_name().await
    }

    /// Verify whether the backing metadata authority is currently reachable.
    pub async fn metadata_status(&self) -> Result<()> {
        self.storage.metadata_status().await.map_err(Into::into)
    }

    /// Return whether the collection's local on-disk state exists on this node.
    pub async fn has_local_collection(&self, collection_name: &str) -> Result<bool> {
        self.storage
            .has_local_collection(collection_name)
            .await
            .map_err(Into::into)
    }

    /// Return whether the local on-disk descriptor matches the authoritative descriptor.
    pub async fn local_collection_matches_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<bool> {
        self.storage
            .local_collection_matches_descriptor(descriptor)
            .await
            .map_err(Into::into)
    }

    /// Persist a write batch.
    pub async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage
            .write(&descriptor.lookup_name(), operations)
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
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage
            .snapshot(&descriptor.lookup_name())
            .await
            .map_err(Into::into)
    }

    /// Return collection-level stats.
    pub async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.stats_descriptor(&descriptor, None).await
    }

    /// Return collection-level stats using a previously loaded descriptor.
    pub async fn stats_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let mut stats = self
            .storage
            .stats_descriptor(descriptor, snapshot)
            .await
            .map_err(ServiceError::from)?;
        stats.tenant_name = descriptor.tenant_name.clone();
        stats.database_name = descriptor.database_name.clone();
        Ok(stats)
    }

    /// Load persisted maintenance state without reconstructing full stats.
    pub async fn maintenance_status_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        self.storage
            .maintenance_status_descriptor(descriptor)
            .await
            .map_err(Into::into)
    }

    /// Resume persisted maintenance for a descriptor when the current runtime can serve it.
    pub async fn recover_maintenance_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<()> {
        self.storage
            .recover_maintenance_descriptor(descriptor)
            .await
            .map_err(Into::into)
    }

    /// Flush the mutable delta to a new segment.
    pub async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage
            .flush(&descriptor.lookup_name())
            .await
            .map_err(Into::into)
    }

    /// Compact immutable segments.
    pub async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage
            .compact(&descriptor.lookup_name())
            .await
            .map_err(Into::into)
    }

    /// Inspect arbitrary operator-visible storage state.
    pub async fn inspect(
        &self,
        collection_name: &str,
        target: InspectTarget,
    ) -> Result<InspectReport> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage
            .inspect(&descriptor.lookup_name(), target)
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

    async fn resolved_collection_descriptor(
        &self,
        collection_name: &str,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        let reference = parse_collection_reference(collection_name).map_err(ServiceError::from)?;
        let descriptor = self
            .storage
            .open_collection(collection_name)
            .await
            .map_err(|error| qualify_collection_error(error, collection_name))
            .map_err(ServiceError::from)?;
        ensure_collection_reference_matches_descriptor(&reference, &descriptor, collection_name)
            .map_err(ServiceError::from)?;
        Ok(descriptor)
    }
}

/// Shared control-plane orchestration over local data-plane services.
#[derive(Clone, Debug)]
pub struct LogPoseControlService {
    data: Arc<LogPoseDataService>,
    config: LogPoseConfig,
    build: BuildInfo,
}

impl LogPoseControlService {
    /// Build a control-plane service over a shared data service and runtime config.
    #[must_use]
    pub fn new(data: Arc<LogPoseDataService>, config: LogPoseConfig, build: BuildInfo) -> Self {
        Self {
            data,
            config,
            build,
        }
    }

    /// Create a collection through the control-plane surface.
    pub async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        match self.config.node_role {
            NodeRole::Data => {
                return Err(ServiceError::InvalidArgument(
                    "data-only nodes cannot accept control-plane collection lifecycle mutations"
                        .to_owned(),
                ));
            }
            NodeRole::Control => {
                return Err(ServiceError::InvalidArgument(
                    "control-only nodes cannot accept control-plane collection lifecycle mutations without a local data plane"
                        .to_owned(),
                ));
            }
            NodeRole::Combined => {}
        }
        self.data
            .create_collection_with_assignment(request, self.initial_assignment())
            .await
    }

    /// Return the placement summary for one collection.
    pub async fn collection_placement(&self, collection_name: &str) -> Result<CollectionPlacement> {
        let descriptor = self.data.get_collection(collection_name).await?;
        let assignment = self.assignment_for_descriptor(&descriptor).await?;
        let local_collection_available = self
            .data
            .local_collection_matches_descriptor(&descriptor)
            .await?;
        Ok(self.local_placement(&descriptor, &assignment, local_collection_available))
    }

    /// Return aggregated runtime and maintenance status for the local node.
    pub async fn runtime_status(&self) -> Result<NodeRuntimeStatus> {
        let metadata_ready = self.data.metadata_status().await.is_ok();
        let descriptors = if metadata_ready {
            self.data.list_collections().await?
        } else {
            Vec::new()
        };
        let mut placements = Vec::with_capacity(descriptors.len());
        let mut local_descriptors = Vec::new();
        for descriptor in &descriptors {
            let assignment = self.assignment_for_descriptor(descriptor).await?;
            let local_collection_available = self
                .data
                .local_collection_matches_descriptor(descriptor)
                .await?;
            let placement =
                self.local_placement(descriptor, &assignment, local_collection_available);
            if placement.route_kind == "local" {
                local_descriptors.push(descriptor);
            }
            placements.push(placement);
        }
        placements.sort_by(|left, right| {
            (
                &left.tenant_name,
                &left.database_name,
                &left.collection_name,
            )
                .cmp(&(
                    &right.tenant_name,
                    &right.database_name,
                    &right.collection_name,
                ))
        });

        let mut maintenance = MaintenanceBacklog::default();
        for descriptor in local_descriptors {
            let status = self.data.maintenance_status_descriptor(descriptor).await?;
            if !status.pending.is_empty() {
                maintenance.collections_with_pending += 1;
                maintenance.pending_operations += status.pending.len();
            }
            if status.in_progress.is_some() {
                maintenance.collections_in_progress += 1;
            }
            if status.last_error.is_some() {
                maintenance.collections_with_errors += 1;
            }
        }

        Ok(NodeRuntimeStatus {
            metadata: logpose_types::NodeMetadata::new(self.config.node_name.clone(), &self.build),
            role: self.config.node_role,
            rest_endpoint: http_endpoint(&self.config.rest_host, self.config.rest_port),
            grpc_endpoint: http_endpoint(&self.config.grpc_host, self.config.grpc_port),
            storage_engine: self.data.engine_name().await.to_owned(),
            control_plane_ready: metadata_ready
                && matches!(
                    self.config.node_role,
                    NodeRole::Combined | NodeRole::Control
                ),
            data_plane_ready: metadata_ready
                && matches!(self.config.node_role, NodeRole::Combined | NodeRole::Data),
            collection_count: placements
                .iter()
                .filter(|placement| placement.route_kind == "local")
                .count(),
            collections: placements,
            maintenance,
        })
    }

    fn local_placement(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
        assignment: &CollectionAssignment,
        local_collection_available: bool,
    ) -> CollectionPlacement {
        let assignment_targets_this_runtime = assignment.assigned_node == self.config.node_name
            || assignment.assigned_node == ANONYMOUS_LOCAL_NODE_NAME;
        let serves_local_assignment = assignment_targets_this_runtime
            && local_collection_available
            && match assignment.assigned_role {
                NodeRole::Combined => self.config.node_role == NodeRole::Combined,
                NodeRole::Control => {
                    matches!(
                        self.config.node_role,
                        NodeRole::Combined | NodeRole::Control
                    )
                }
                NodeRole::Data => {
                    matches!(self.config.node_role, NodeRole::Combined | NodeRole::Data)
                }
            };
        let route_kind = if serves_local_assignment {
            "local"
        } else {
            "recorded"
        };
        CollectionPlacement {
            collection_id: descriptor.collection_id.clone(),
            tenant_name: descriptor.tenant_name.clone(),
            database_name: descriptor.database_name.clone(),
            collection_name: descriptor.name.clone(),
            assigned_node: assignment.assigned_node.clone(),
            assigned_role: assignment.assigned_role,
            route_kind: route_kind.to_owned(),
            route_reason: match (
                serves_local_assignment,
                assignment_targets_this_runtime,
                local_collection_available,
                assignment.assigned_node.as_str(),
                assignment.assigned_role,
                self.config.node_role,
            ) {
                (
                    true,
                    _,
                    true,
                    ANONYMOUS_LOCAL_NODE_NAME,
                    NodeRole::Combined,
                    NodeRole::Combined,
                ) => "anonymous local combined assignment".to_owned(),
                (true, _, true, ANONYMOUS_LOCAL_NODE_NAME, NodeRole::Data, NodeRole::Combined) => {
                    "anonymous local data-plane assignment".to_owned()
                }
                (true, _, true, ANONYMOUS_LOCAL_NODE_NAME, NodeRole::Data, NodeRole::Data) => {
                    "anonymous local data-plane assignment".to_owned()
                }
                (false, true, false, ANONYMOUS_LOCAL_NODE_NAME, assigned_role, _) => format!(
                    "anonymous local {assigned_role} assignment targets this runtime but local collection state is absent"
                ),
                (
                    false,
                    true,
                    true,
                    ANONYMOUS_LOCAL_NODE_NAME,
                    assigned_role,
                    NodeRole::Control,
                ) => {
                    format!(
                        "anonymous local {assigned_role} assignment is recorded while this process runs as control-only"
                    )
                }
                (false, true, true, ANONYMOUS_LOCAL_NODE_NAME, assigned_role, current_role) => {
                    format!(
                        "anonymous local {assigned_role} assignment is recorded while this process runs as {current_role}"
                    )
                }
                (true, _, true, _, NodeRole::Combined, NodeRole::Combined) => {
                    "single-node combined runtime keeps control-plane and data-plane colocated"
                        .to_owned()
                }
                (true, _, true, _, NodeRole::Data, NodeRole::Combined) => {
                    "single-node combined runtime exposes a local data-plane assignment".to_owned()
                }
                (true, _, true, _, NodeRole::Data, NodeRole::Data) => {
                    "local data-plane assignment".to_owned()
                }
                (false, true, false, _, assigned_role, _) => format!(
                    "persisted local {assigned_role} assignment targets this runtime but local collection state is absent"
                ),
                (false, true, true, _, assigned_role, NodeRole::Control) => format!(
                    "persisted local {assigned_role} assignment is recorded while this process runs as control-only"
                ),
                (false, true, true, _, assigned_role, current_role) => format!(
                    "persisted local {assigned_role} assignment is recorded while this process runs as {current_role}"
                ),
                (true, _, true, _, assigned_role, current_role)
                    if assigned_role != current_role =>
                {
                    format!(
                        "persisted local {assigned_role} assignment is being inspected from a {current_role} runtime"
                    )
                }
                (true, _, true, _, assigned_role, _) => {
                    format!("persisted local {assigned_role} assignment")
                }
                (true, _, false, _, assigned_role, _) => format!(
                    "persisted local {assigned_role} assignment cannot be served because local collection state is absent"
                ),
                (false, false, _, _, assigned_role, _) => format!(
                    "persisted placement targets node '{}' with role '{}'",
                    assignment.assigned_node, assigned_role
                ),
            },
        }
    }

    fn initial_assignment(&self) -> CollectionAssignment {
        CollectionAssignment {
            assigned_node: self.config.node_name.clone(),
            assigned_role: NodeRole::Data,
        }
    }

    async fn assignment_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        self.data.collection_assignment_descriptor(descriptor).await
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
            | QueryError::VectorDimensionMismatch { .. }
            | QueryError::InvalidPredicate(_) => Self::InvalidArgument(error.to_string()),
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
    } else if message.contains("unsupported")
        || message.contains("duplicate record id")
        || message.contains("must include at least one operation")
        || message.contains("must not be empty")
        || message.contains("must not contain '/'")
        || message.contains("must be greater than 0")
        || message.contains("invalid snapshot")
        || message.contains("manual reconciliation is required")
        || message.contains("reconciliation is required")
        || is_dimension_validation_error(&message)
    {
        ServiceError::InvalidArgument(message)
    } else {
        ServiceError::Internal(message)
    }
}

fn is_dimension_validation_error(message: &str) -> bool {
    message.contains("record '")
        && message.contains(" dimensions")
        && message.contains(" expected ")
        && message.contains(" found ")
}

fn http_endpoint(host: &str, port: u16) -> String {
    let authority = match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]"),
        _ => host.to_owned(),
    };
    format!("http://{authority}:{port}")
}

fn parse_collection_reference(collection_name: &str) -> logpose_types::Result<CollectionRef> {
    let reference = match collection_name
        .trim()
        .split('/')
        .collect::<Vec<_>>()
        .as_slice()
    {
        [collection_name] => CollectionRef::new_default(*collection_name),
        [tenant_name, database_name, collection_name] => {
            CollectionRef::new(*tenant_name, *database_name, *collection_name)
        }
        _ => {
            return Err(LogPoseError::Message(format!(
                "unsupported collection reference '{collection_name}': expected 'collection' or 'tenant/database/collection'"
            )));
        }
    };
    reference.validate()?;
    Ok(reference)
}

fn ensure_collection_reference_matches_descriptor(
    reference: &CollectionRef,
    descriptor: &logpose_catalog::CollectionDescriptor,
    original_name: &str,
) -> logpose_types::Result<()> {
    if reference.tenant_name != descriptor.tenant_name
        || reference.database_name != descriptor.database_name
        || reference.collection_name != descriptor.name
    {
        return Err(LogPoseError::Message(format!(
            "collection '{original_name}' does not exist"
        )));
    }
    Ok(())
}

fn qualify_collection_error(error: LogPoseError, collection_name: &str) -> LogPoseError {
    match error {
        LogPoseError::Message(message) if message.contains("does not exist") => {
            LogPoseError::Message(format!("collection '{collection_name}' does not exist"))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use logpose_storage::{CreateCollectionRequest, InspectReport, InspectTarget};
    use logpose_types::{
        AnnSearchRequest, CollectionStats, CommitAck, DistanceMetric, Snapshot, VisibleRecord,
    };
    use serde_json::json;
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    #[test]
    fn preserves_checksum_style_expected_messages_as_internal_errors() {
        let error = ServiceError::from(LogPoseError::Message(
            "checksum mismatch while reading segment 'abc': expected 10, got 11".to_owned(),
        ));

        assert!(
            matches!(error, ServiceError::Internal(message) if message.contains("checksum mismatch"))
        );
    }

    #[test]
    fn classifies_reconciliation_failures_as_invalid_argument() {
        let error = ServiceError::from(LogPoseError::Message(
            "collection 'default/default/documents' has authoritative metadata in etcd but local state finalization is still pending; manual reconciliation is required before serving it".to_owned(),
        ));

        assert!(matches!(error, ServiceError::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn create_collection_uses_plain_storage_create_when_assignments_are_unsupported() {
        #[derive(Debug)]
        struct CreateOnlyStorageEngine {
            root: PathBuf,
            next_id: AtomicU64,
        }

        #[async_trait]
        impl StorageEngine for CreateOnlyStorageEngine {
            async fn engine_name(&self) -> &'static str {
                "create-only"
            }

            async fn create_collection(
                &self,
                request: CreateCollectionRequest,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                let suffix = self.next_id.fetch_add(1, Ordering::Relaxed);
                Ok(logpose_catalog::CollectionDescriptor::new(
                    request.name,
                    request.dimensions,
                    request.metric,
                    self.root.join(format!("collection-{suffix}")),
                ))
            }

            async fn open_collection(
                &self,
                name: &str,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                Err(LogPoseError::Message(format!(
                    "collection '{name}' does not exist"
                )))
            }

            async fn write(
                &self,
                collection_name: &str,
                _operations: Vec<WriteOperation>,
            ) -> logpose_types::Result<CommitAck> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn snapshot(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn scan_exact(
                &self,
                collection_name: &str,
                _snapshot: Option<Snapshot>,
            ) -> logpose_types::Result<Vec<VisibleRecord>> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn ann_search_selected(
                &self,
                collection_name: &str,
                _snapshot: Option<Snapshot>,
                _immutable_unit_ids: Vec<String>,
                _request: AnnSearchRequest,
                _filter: Option<Arc<dyn for<'a> Fn(&'a serde_json::Value) -> bool + Send + Sync>>,
            ) -> logpose_types::Result<Vec<logpose_types::AnnCandidate>> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn flush(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn compact(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn stats(&self, collection_name: &str) -> logpose_types::Result<CollectionStats> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn inspect(
                &self,
                collection_name: &str,
                target: InspectTarget,
            ) -> logpose_types::Result<InspectReport> {
                let _ = collection_name;
                Ok(InspectReport {
                    target: match target {
                        InspectTarget::Manifest => "manifest".to_owned(),
                        InspectTarget::Wal => "wal".to_owned(),
                        InspectTarget::Maintenance => "maintenance".to_owned(),
                        InspectTarget::Segment(segment_id) => {
                            format!("segment:{segment_id}")
                        }
                    },
                    payload: json!({}),
                })
            }
        }

        let service = LogPoseDataService::new(Arc::new(CreateOnlyStorageEngine {
            root: std::env::temp_dir().join("logpose-create-only-engine"),
            next_id: AtomicU64::new(0),
        }));

        let descriptor = service
            .create_collection(CreateCollectionRequest {
                tenant_name: "default".to_owned(),
                database_name: "default".to_owned(),
                name: "documents".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Dot,
            })
            .await
            .expect("plain storage create should still succeed");

        assert_eq!(descriptor.name, "documents");
        assert_eq!(descriptor.dimensions, 2);
        assert_eq!(descriptor.metric, DistanceMetric::Dot);
    }

    #[tokio::test]
    async fn runtime_status_surfaces_metadata_unready_without_failing() {
        #[derive(Debug)]
        struct MetadataUnavailableStorageEngine;

        #[async_trait]
        impl StorageEngine for MetadataUnavailableStorageEngine {
            async fn engine_name(&self) -> &'static str {
                "metadata-unavailable"
            }

            async fn metadata_status(&self) -> logpose_types::Result<()> {
                Err(LogPoseError::Message(
                    "etcd metadata operation failed: connection refused".to_owned(),
                ))
            }

            async fn create_collection(
                &self,
                _request: CreateCollectionRequest,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                Err(LogPoseError::Message("unsupported".to_owned()))
            }

            async fn open_collection(
                &self,
                name: &str,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                Err(LogPoseError::Message(format!(
                    "collection '{name}' does not exist"
                )))
            }

            async fn list_collections(
                &self,
            ) -> logpose_types::Result<Vec<logpose_catalog::CollectionDescriptor>> {
                Err(LogPoseError::Message(
                    "list_collections should not run when metadata is unavailable".to_owned(),
                ))
            }

            async fn write(
                &self,
                collection_name: &str,
                _operations: Vec<WriteOperation>,
            ) -> logpose_types::Result<CommitAck> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn snapshot(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn scan_exact(
                &self,
                collection_name: &str,
                _snapshot: Option<Snapshot>,
            ) -> logpose_types::Result<Vec<VisibleRecord>> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn ann_search_selected(
                &self,
                collection_name: &str,
                _snapshot: Option<Snapshot>,
                _immutable_unit_ids: Vec<String>,
                _request: AnnSearchRequest,
                _filter: Option<Arc<dyn for<'a> Fn(&'a serde_json::Value) -> bool + Send + Sync>>,
            ) -> logpose_types::Result<Vec<logpose_types::AnnCandidate>> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn flush(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn compact(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn stats(&self, collection_name: &str) -> logpose_types::Result<CollectionStats> {
                Err(LogPoseError::Message(format!(
                    "collection '{collection_name}' does not exist"
                )))
            }

            async fn inspect(
                &self,
                collection_name: &str,
                target: InspectTarget,
            ) -> logpose_types::Result<InspectReport> {
                let _ = collection_name;
                Ok(InspectReport {
                    target: match target {
                        InspectTarget::Manifest => "manifest".to_owned(),
                        InspectTarget::Wal => "wal".to_owned(),
                        InspectTarget::Maintenance => "maintenance".to_owned(),
                        InspectTarget::Segment(segment_id) => format!("segment:{segment_id}"),
                    },
                    payload: json!({}),
                })
            }
        }

        let data = Arc::new(LogPoseDataService::new(Arc::new(
            MetadataUnavailableStorageEngine,
        )));
        let control =
            LogPoseControlService::new(data, LogPoseConfig::default(), BuildInfo::current());

        let status = control
            .runtime_status()
            .await
            .expect("runtime status should still surface readiness state");

        assert!(!status.control_plane_ready);
        assert!(!status.data_plane_ready);
        assert_eq!(status.collection_count, 0);
        assert!(status.collections.is_empty());
    }
}
