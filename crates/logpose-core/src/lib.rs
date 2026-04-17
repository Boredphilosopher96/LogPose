//! Shared service lifecycle types.

use logpose_config::LogPoseConfig;
use logpose_query::{QueryRequest, QueryResponse};
use logpose_service::{
    LogPoseControlService, LogPoseDataService, Result as ServiceResult, ServiceError,
};
use logpose_storage::{InspectReport, InspectTarget, LocalStorageEngine};
use logpose_storage_etcd::EtcdBackedStorageEngine;
use logpose_types::{
    BuildInfo, CollectionStats, CommitAck, MetadataBackend, NodeMetadata, NodeRole, Snapshot,
    WriteOperation,
};
use serde::Serialize;
use std::sync::Arc;

/// Top-level state shared by transport layers and tools.
#[derive(Clone, Debug, Serialize)]
pub struct AppState {
    /// Effective runtime configuration.
    pub config: LogPoseConfig,
    /// Build metadata exposed through APIs and diagnostics.
    pub build: BuildInfo,
    /// Shared application control-plane service used by admin and diagnostics workflows.
    #[serde(skip_serializing)]
    pub control: Arc<LogPoseControlService>,
    /// Shared application data-plane service used internally by app-state helpers.
    #[serde(skip_serializing)]
    data: Arc<LogPoseDataService>,
}

impl AppState {
    /// Construct shared state from configuration.
    #[must_use]
    pub fn new(config: LogPoseConfig) -> Self {
        config
            .validate()
            .expect("invalid runtime configuration for AppState");
        let build = BuildInfo::current();
        let storage: Arc<dyn logpose_storage::StorageEngine> = match config.metadata.backend {
            MetadataBackend::Local => Arc::new(LocalStorageEngine::new(&config.storage_root)),
            MetadataBackend::Etcd => Arc::new(
                EtcdBackedStorageEngine::new(&config.storage_root, config.metadata.etcd.clone())
                    .expect("invalid etcd metadata configuration"),
            ),
        };
        let data = Arc::new(LogPoseDataService::new(storage));
        let control = Arc::new(LogPoseControlService::new(
            Arc::clone(&data),
            config.clone(),
            build.clone(),
        ));
        Self {
            control,
            data,
            config,
            build,
        }
    }

    /// Canonical node metadata exposed through operator-visible surfaces.
    #[must_use]
    pub fn metadata(&self) -> NodeMetadata {
        NodeMetadata::new(self.config.node_name.clone(), &self.build)
    }

    /// Fetch collection metadata by name.
    pub async fn get_collection(
        &self,
        collection_name: &str,
    ) -> ServiceResult<logpose_catalog::CollectionDescriptor> {
        self.data.get_collection(collection_name).await
    }

    /// Persist one write batch through the data-plane surface.
    pub async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> ServiceResult<CommitAck> {
        self.require_local_data_plane_collection(collection_name)
            .await?;
        self.data.write(collection_name, operations).await
    }

    /// Execute a query through the data-plane surface.
    pub async fn query(&self, request: QueryRequest) -> ServiceResult<QueryResponse> {
        self.require_local_data_plane_collection(&request.collection_name)
            .await?;
        self.data.query(request).await
    }

    /// Capture a read snapshot through the data-plane surface.
    pub async fn snapshot(&self, collection_name: &str) -> ServiceResult<Snapshot> {
        self.require_local_data_plane_collection(collection_name)
            .await?;
        self.data.snapshot(collection_name).await
    }

    /// Return collection stats through the data-plane surface.
    pub async fn stats(&self, collection_name: &str) -> ServiceResult<CollectionStats> {
        self.require_local_data_plane_collection(collection_name)
            .await?;
        self.data.stats(collection_name).await
    }

    /// Flush one collection through the data-plane surface.
    pub async fn flush(&self, collection_name: &str) -> ServiceResult<Snapshot> {
        self.require_local_data_plane_collection(collection_name)
            .await?;
        self.data.flush(collection_name).await
    }

    /// Compact one collection through the data-plane surface.
    pub async fn compact(&self, collection_name: &str) -> ServiceResult<Snapshot> {
        self.require_local_data_plane_collection(collection_name)
            .await?;
        self.data.compact(collection_name).await
    }

    /// Inspect one collection through the data-plane surface.
    pub async fn inspect(
        &self,
        collection_name: &str,
        target: InspectTarget,
    ) -> ServiceResult<InspectReport> {
        self.require_local_data_plane_collection(collection_name)
            .await?;
        self.data.inspect(collection_name, target).await
    }

    fn require_data_plane(&self) -> ServiceResult<()> {
        if matches!(self.config.node_role, NodeRole::Combined | NodeRole::Data) {
            Ok(())
        } else {
            Err(ServiceError::InvalidArgument(format!(
                "node '{}' is running as '{}' and cannot accept data-plane operations",
                self.config.node_name, self.config.node_role
            )))
        }
    }

    async fn require_local_data_plane_collection(
        &self,
        collection_name: &str,
    ) -> ServiceResult<()> {
        self.require_data_plane()?;
        let placement = self.control.collection_placement(collection_name).await?;
        if placement.route_kind == "local"
            && matches!(placement.assigned_role, NodeRole::Combined | NodeRole::Data)
        {
            return Ok(());
        }

        Err(ServiceError::InvalidArgument(format!(
            "collection '{collection_name}' is assigned to node '{}' with role '{}' and is not locally served by node '{}'",
            placement.assigned_node, placement.assigned_role, self.config.node_name
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_reserved_anonymous_local_node_name_at_runtime_bootstrap() {
        let result = std::panic::catch_unwind(|| {
            AppState::new(LogPoseConfig {
                node_name: "local".to_owned(),
                ..LogPoseConfig::default()
            })
        });

        assert!(
            result.is_err(),
            "reserved anonymous local node name should panic"
        );
    }
}
