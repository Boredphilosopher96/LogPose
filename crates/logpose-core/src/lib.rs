//! Shared service lifecycle types.

#[cfg(test)]
use etcd_client as _;
use logpose_auth::{AccessTier, AuthenticationMode, DatabaseRole, Principal};
use logpose_catalog::DatabaseDescriptor;
use logpose_config::LogPoseConfig;
use logpose_query::{QueryRequest, QueryResponse};
use logpose_service::{
    LogPoseControlService, LogPoseDataService, Result as ServiceResult, ServiceError,
};
use logpose_storage::{CreateCollectionRequest, InspectReport, InspectTarget, LocalStorageEngine};
use logpose_storage_etcd::{EtcdBackedStorageEngine, EtcdCatalogStore};
use logpose_types::{
    BuildInfo, CollectionRef, CollectionStats, CommitAck, DEFAULT_DATABASE_NAME, LeadershipFence,
    MetadataBackend, NodeMembershipStatus, NodeMetadata, NodeRole, Snapshot, WriteOperation,
};
use serde::Serialize;
#[cfg(test)]
use serde_json as _;
use std::{path::PathBuf, sync::Arc};
#[cfg(test)]
use tokio as _;

/// Transport-neutral request authentication context.
#[derive(Clone, Debug, Default)]
pub struct RequestAuth {
    bearer_token: Option<String>,
}

impl RequestAuth {
    /// Build a request auth context with one bearer token.
    #[must_use]
    pub fn bearer_token(token: impl Into<String>) -> Self {
        Self {
            bearer_token: Some(token.into()),
        }
    }

    fn bearer_token_str(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DatabasePermission {
    ReadOnly,
    ReadWrite,
    Owner,
}

/// Top-level state shared by transport layers and tools.
#[derive(Clone, Serialize)]
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
    /// Shared database and policy metadata surface used by authenticated APIs.
    #[serde(skip_serializing)]
    shared_catalog: SharedCatalog,
}

#[derive(Clone)]
enum SharedCatalog {
    Local,
    Etcd(EtcdCatalogStore),
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
        let catalog = logpose_service::local_catalog_store(&config.storage_root);
        let shared_catalog = match config.metadata.backend {
            MetadataBackend::Local => SharedCatalog::Local,
            MetadataBackend::Etcd => SharedCatalog::Etcd(
                EtcdCatalogStore::new(config.metadata.etcd.clone())
                    .expect("invalid etcd metadata configuration for shared catalog"),
            ),
        };
        let control = Arc::new(LogPoseControlService::new(
            Arc::clone(&data),
            catalog,
            config.clone(),
            build.clone(),
        ));
        control.register_local_replica_archive_exporter();
        control
            .sync_bootstrap_principals()
            .expect("failed to persist bootstrap principals");
        Self {
            control,
            data,
            config,
            build,
            shared_catalog,
        }
    }

    /// Canonical node metadata exposed through operator-visible surfaces.
    #[must_use]
    pub fn metadata(&self) -> NodeMetadata {
        NodeMetadata::new(self.config.node_name.clone(), &self.build)
    }

    /// Create or replace one database descriptor after enforcing operator access.
    pub async fn put_database_with_auth(
        &self,
        auth: &RequestAuth,
        descriptor: DatabaseDescriptor,
    ) -> ServiceResult<DatabaseDescriptor> {
        self.require_operator(auth).await?;
        self.put_database_shared(descriptor).await
    }

    /// Read one database descriptor after enforcing operator access.
    pub async fn database_with_auth(
        &self,
        auth: &RequestAuth,
        database_name: &str,
    ) -> ServiceResult<DatabaseDescriptor> {
        self.require_operator(auth).await?;
        self.database_shared(database_name).await
    }

    /// List database descriptors after enforcing operator access.
    pub async fn databases_with_auth(
        &self,
        auth: &RequestAuth,
    ) -> ServiceResult<Vec<DatabaseDescriptor>> {
        self.require_operator(auth).await?;
        self.databases_shared().await
    }

    /// Return runtime status after enforcing operator access when auth is enabled.
    pub async fn runtime_status_with_auth(
        &self,
        auth: &RequestAuth,
    ) -> ServiceResult<logpose_types::NodeRuntimeStatus> {
        self.require_operator(auth).await?;
        self.control.runtime_status().await
    }

    /// Read one node membership status after enforcing operator access when auth is enabled.
    pub async fn node_membership_status_with_auth(
        &self,
        auth: &RequestAuth,
        node_id: &str,
    ) -> ServiceResult<NodeMembershipStatus> {
        self.require_operator(auth).await?;
        self.control.node_membership_status(node_id).await
    }

    /// Export one locally owned collection as an internal replica-repair archive.
    pub async fn export_local_replica_archive(
        &self,
        collection_name: &str,
        expected_snapshot: Option<&Snapshot>,
    ) -> ServiceResult<PathBuf> {
        self.control
            .export_local_replica_archive(collection_name, expected_snapshot)
            .await
    }

    /// Mark one node as draining after enforcing operator access when auth is enabled.
    pub async fn drain_node_with_auth(
        &self,
        auth: &RequestAuth,
        node_id: &str,
    ) -> ServiceResult<NodeMembershipStatus> {
        self.require_operator(auth).await?;
        self.control.drain_node(node_id).await
    }

    /// Restore one node to ready serving state after enforcing operator access when auth is enabled.
    pub async fn undrain_node_with_auth(
        &self,
        auth: &RequestAuth,
        node_id: &str,
    ) -> ServiceResult<NodeMembershipStatus> {
        self.require_operator(auth).await?;
        self.control.undrain_node(node_id).await
    }

    /// Create one collection after enforcing database write access when auth is enabled.
    pub async fn create_collection_with_auth(
        &self,
        auth: &RequestAuth,
        request: CreateCollectionRequest,
    ) -> ServiceResult<logpose_catalog::CollectionDescriptor> {
        self.require_database_permission(
            auth,
            &request.database_name,
            DatabasePermission::ReadWrite,
        )
        .await?;
        self.require_control_plane_collection_mutation()?;
        self.ensure_shared_database_descriptor(&request.database_name)
            .await?;
        let descriptor = self.control.create_collection(request).await?;
        self.control
            .acknowledge_local_replica_update(&descriptor.lookup_name(), None, true)
            .await?;
        Ok(descriptor)
    }

    /// Create or replace one database-scoped access policy after enforcing owner access.
    pub async fn set_database_access_policy_with_auth(
        &self,
        auth: &RequestAuth,
        policy: logpose_auth::DatabaseAccessPolicy,
    ) -> ServiceResult<logpose_auth::DatabaseAccessPolicy> {
        self.require_database_owner_permission(auth, &policy.database_name)
            .await?;
        self.put_database_access_policy_shared(policy).await
    }

    /// Read one database-scoped access policy after enforcing owner access.
    pub async fn database_access_policy_with_auth(
        &self,
        auth: &RequestAuth,
        database_name: &str,
    ) -> ServiceResult<logpose_auth::DatabaseAccessPolicy> {
        self.require_database_owner_permission(auth, database_name)
            .await?;
        self.database_access_policy_shared(database_name).await
    }

    /// Return collection placement after enforcing operator access when auth is enabled.
    pub async fn collection_placement_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
    ) -> ServiceResult<logpose_types::CollectionPlacement> {
        self.require_operator(auth).await?;
        self.control.collection_placement(collection_name).await
    }

    /// Promote one collection owner after enforcing operator access when auth is enabled.
    pub async fn promote_collection_owner_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
        node_id: &str,
    ) -> ServiceResult<logpose_types::CollectionPlacement> {
        self.require_operator(auth).await?;
        self.control
            .promote_collection_owner(collection_name, node_id)
            .await
    }

    /// Rebalance one collection after enforcing operator access when auth is enabled.
    pub async fn rebalance_collection_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
        node_id: Option<&str>,
    ) -> ServiceResult<logpose_types::CollectionPlacement> {
        self.require_operator(auth).await?;
        self.control
            .rebalance_collection(collection_name, node_id)
            .await
    }

    /// Fetch collection metadata by name after enforcing database read access when auth is enabled.
    pub async fn get_collection_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
    ) -> ServiceResult<logpose_catalog::CollectionDescriptor> {
        let collection = parse_collection_reference(collection_name)?;
        self.require_database_permission(
            auth,
            &collection.database_name,
            DatabasePermission::ReadOnly,
        )
        .await?;
        self.get_collection(collection_name).await
    }

    /// Fetch collection metadata by name.
    pub async fn get_collection(
        &self,
        collection_name: &str,
    ) -> ServiceResult<logpose_catalog::CollectionDescriptor> {
        self.data.get_collection(collection_name).await
    }

    /// Persist one write batch after enforcing database write access when auth is enabled.
    pub async fn write_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> ServiceResult<CommitAck> {
        let collection = parse_collection_reference(collection_name)?;
        self.require_database_permission(
            auth,
            &collection.database_name,
            DatabasePermission::ReadWrite,
        )
        .await?;
        self.write(collection_name, operations).await
    }

    /// Persist one write batch through the data-plane surface.
    pub async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> ServiceResult<CommitAck> {
        self.control
            .require_local_write_ownership(collection_name)
            .await?;
        let stale_report_already_cleared = self
            .control
            .prepare_local_replica_update(collection_name)
            .await?;
        let ack = match self.data.write(collection_name, operations).await {
            Ok(ack) => ack,
            Err(error) => {
                return match self
                    .control
                    .restore_local_replica_report_after_failed_update(collection_name)
                    .await
                {
                    Ok(()) => Err(error),
                    Err(restore_error) => Err(ServiceError::Internal(format!(
                        "write for collection '{collection_name}' failed with '{error}', and restoring authoritative replica metadata also failed with '{restore_error}'"
                    ))),
                };
            }
        };
        self.control
            .acknowledge_local_replica_update(
                collection_name,
                Some(ack.snapshot.clone()),
                stale_report_already_cleared,
            )
            .await?;
        Ok(ack)
    }

    /// Execute a query after enforcing database read access when auth is enabled.
    pub async fn query_with_auth(
        &self,
        auth: &RequestAuth,
        request: QueryRequest,
    ) -> ServiceResult<QueryResponse> {
        let collection = parse_collection_reference(&request.collection_name)?;
        self.require_database_permission(
            auth,
            &collection.database_name,
            DatabasePermission::ReadOnly,
        )
        .await?;
        self.query(request).await
    }

    /// Execute a query through the data-plane surface.
    pub async fn query(&self, request: QueryRequest) -> ServiceResult<QueryResponse> {
        let placement = self
            .require_local_data_plane_collection(&request.collection_name)
            .await?;
        reject_promoted_read_barriers(&placement, request.read_barrier.as_ref())?;
        self.data.query(request).await
    }

    /// Capture a read snapshot through the data-plane surface.
    pub async fn snapshot(&self, collection_name: &str) -> ServiceResult<Snapshot> {
        self.require_local_data_plane_collection(collection_name)
            .await?;
        self.data.snapshot(collection_name).await
    }

    /// Return collection stats after enforcing database read access when auth is enabled.
    pub async fn stats_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
    ) -> ServiceResult<CollectionStats> {
        self.stats_for_read_with_auth(auth, collection_name, None, None)
            .await
    }

    /// Return collection stats for one explicit snapshot after enforcing database read access.
    pub async fn stats_at_snapshot_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> ServiceResult<CollectionStats> {
        self.stats_for_read_with_auth(auth, collection_name, snapshot, None)
            .await
    }

    /// Return collection stats for one exact snapshot or lower-bound read barrier.
    pub async fn stats_for_read_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        read_barrier: Option<Snapshot>,
    ) -> ServiceResult<CollectionStats> {
        let collection = parse_collection_reference(collection_name)?;
        self.require_database_permission(
            auth,
            &collection.database_name,
            DatabasePermission::ReadOnly,
        )
        .await?;
        self.stats_for_read(collection_name, snapshot, read_barrier)
            .await
    }

    /// Return collection stats through the data-plane surface.
    pub async fn stats(&self, collection_name: &str) -> ServiceResult<CollectionStats> {
        self.stats_for_read(collection_name, None, None).await
    }

    /// Return collection stats through the data-plane surface for one explicit snapshot.
    pub async fn stats_at_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> ServiceResult<CollectionStats> {
        self.stats_for_read(collection_name, snapshot, None).await
    }

    /// Return collection stats through the data-plane surface for one exact snapshot or barrier.
    pub async fn stats_for_read(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        read_barrier: Option<Snapshot>,
    ) -> ServiceResult<CollectionStats> {
        let placement = self
            .require_local_data_plane_collection(collection_name)
            .await?;
        reject_promoted_read_barriers(&placement, read_barrier.as_ref())?;
        self.data
            .stats_for_read(collection_name, snapshot, read_barrier)
            .await
    }

    /// Flush one collection after enforcing database write access when auth is enabled.
    pub async fn flush_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
    ) -> ServiceResult<Snapshot> {
        let collection = parse_collection_reference(collection_name)?;
        self.require_database_permission(
            auth,
            &collection.database_name,
            DatabasePermission::ReadWrite,
        )
        .await?;
        self.flush(collection_name).await
    }

    /// Flush one collection through the data-plane surface.
    pub async fn flush(&self, collection_name: &str) -> ServiceResult<Snapshot> {
        self.control
            .require_local_write_ownership(collection_name)
            .await?;
        let stale_report_already_cleared = self
            .control
            .prepare_local_replica_update(collection_name)
            .await?;
        let snapshot = match self.data.flush(collection_name).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return match self
                    .control
                    .restore_local_replica_report_after_failed_update(collection_name)
                    .await
                {
                    Ok(()) => Err(error),
                    Err(restore_error) => Err(ServiceError::Internal(format!(
                        "flush for collection '{collection_name}' failed with '{error}', and restoring authoritative replica metadata also failed with '{restore_error}'"
                    ))),
                };
            }
        };
        self.control
            .acknowledge_local_replica_update(
                collection_name,
                Some(snapshot.clone()),
                stale_report_already_cleared,
            )
            .await?;
        Ok(snapshot)
    }

    /// Compact one collection after enforcing database write access when auth is enabled.
    pub async fn compact_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
    ) -> ServiceResult<Snapshot> {
        let collection = parse_collection_reference(collection_name)?;
        self.require_database_permission(
            auth,
            &collection.database_name,
            DatabasePermission::ReadWrite,
        )
        .await?;
        self.compact(collection_name).await
    }

    /// Compact one collection through the data-plane surface.
    pub async fn compact(&self, collection_name: &str) -> ServiceResult<Snapshot> {
        self.control
            .require_local_write_ownership(collection_name)
            .await?;
        let stale_report_already_cleared = self
            .control
            .prepare_local_replica_update(collection_name)
            .await?;
        let snapshot = match self.data.compact(collection_name).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return match self
                    .control
                    .restore_local_replica_report_after_failed_update(collection_name)
                    .await
                {
                    Ok(()) => Err(error),
                    Err(restore_error) => Err(ServiceError::Internal(format!(
                        "compact for collection '{collection_name}' failed with '{error}', and restoring authoritative replica metadata also failed with '{restore_error}'"
                    ))),
                };
            }
        };
        self.control
            .acknowledge_local_replica_update(
                collection_name,
                Some(snapshot.clone()),
                stale_report_already_cleared,
            )
            .await?;
        Ok(snapshot)
    }

    /// Inspect one collection after enforcing database read access when auth is enabled.
    pub async fn inspect_with_auth(
        &self,
        auth: &RequestAuth,
        collection_name: &str,
        target: InspectTarget,
    ) -> ServiceResult<InspectReport> {
        let collection = parse_collection_reference(collection_name)?;
        self.require_database_permission(
            auth,
            &collection.database_name,
            DatabasePermission::ReadOnly,
        )
        .await?;
        self.inspect(collection_name, target).await
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

    fn auth_enabled(&self) -> bool {
        !self.config.auth.bootstrap_tokens.is_empty()
    }

    async fn require_operator(&self, auth: &RequestAuth) -> ServiceResult<()> {
        let principal = match self.authenticate(auth).await? {
            Some(principal) => principal,
            None => return Ok(()),
        };

        if matches!(principal.access_tier, AccessTier::Operator) {
            Ok(())
        } else {
            Err(ServiceError::PermissionDenied(format!(
                "principal '{}' is not allowed to perform operator actions",
                principal.name
            )))
        }
    }

    async fn require_database_permission(
        &self,
        auth: &RequestAuth,
        database_name: &str,
        permission: DatabasePermission,
    ) -> ServiceResult<()> {
        self.require_database_permission_inner(auth, database_name, permission, true)
            .await
    }

    async fn require_database_owner_permission(
        &self,
        auth: &RequestAuth,
        database_name: &str,
    ) -> ServiceResult<()> {
        self.require_database_permission_inner(
            auth,
            database_name,
            DatabasePermission::Owner,
            false,
        )
        .await
    }

    async fn require_database_permission_inner(
        &self,
        auth: &RequestAuth,
        database_name: &str,
        permission: DatabasePermission,
        allow_unauthenticated_if_disabled: bool,
    ) -> ServiceResult<()> {
        let policy = match self.database_access_policy_shared(database_name).await {
            Ok(policy) => Some(policy),
            Err(ServiceError::NotFound(_)) => None,
            Err(error) => return Err(error),
        };

        if allow_unauthenticated_if_disabled
            && matches!(
                policy.as_ref().map(|policy| &policy.authentication_mode),
                Some(AuthenticationMode::Disabled)
            )
        {
            return Ok(());
        }

        let principal = match self.authenticate(auth).await? {
            Some(principal) => principal,
            None => return Ok(()),
        };

        if matches!(principal.access_tier, AccessTier::Operator) {
            return Ok(());
        }

        let policy = match policy {
            Some(policy) => policy,
            None => {
                return Err(ServiceError::PermissionDenied(format!(
                    "principal '{}' is not allowed to access database '{database_name}'",
                    principal.name
                )));
            }
        };

        let allowed = policy.role_bindings.iter().any(|binding| {
            binding.principal_name == principal.name
                && database_role_satisfies(&binding.role, permission)
        });

        if allowed {
            Ok(())
        } else {
            Err(ServiceError::PermissionDenied(format!(
                "principal '{}' is not allowed to access database '{database_name}'",
                principal.name
            )))
        }
    }

    async fn authenticate(&self, auth: &RequestAuth) -> ServiceResult<Option<Principal>> {
        if !self.auth_enabled() {
            return Ok(None);
        }

        let token = auth
            .bearer_token_str()
            .ok_or_else(|| ServiceError::Unauthenticated("missing bearer token".to_owned()))?;
        let bootstrap_principal = self
            .config
            .auth
            .bootstrap_tokens
            .iter()
            .find(|entry| constant_time_eq(&entry.token, token))
            .map(|entry| entry.principal.clone())
            .ok_or_else(|| ServiceError::Unauthenticated("invalid bearer token".to_owned()))?;

        match self.principal_shared(&bootstrap_principal.name).await {
            Ok(principal) => Ok(Some(principal)),
            Err(ServiceError::NotFound(_)) => Ok(Some(bootstrap_principal)),
            Err(error) => Err(error),
        }
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

    fn require_control_plane_database_mutation(&self) -> ServiceResult<()> {
        match self.config.node_role {
            NodeRole::Data => Err(ServiceError::InvalidArgument(
                "data-only nodes cannot accept control-plane database mutations".to_owned(),
            )),
            NodeRole::Control | NodeRole::Combined => Ok(()),
        }
    }

    fn require_control_plane_collection_mutation(&self) -> ServiceResult<()> {
        match self.config.node_role {
            NodeRole::Data => Err(ServiceError::InvalidArgument(
                "data-only nodes cannot accept control-plane collection lifecycle mutations"
                    .to_owned(),
            )),
            NodeRole::Control => Err(ServiceError::InvalidArgument(
                "control-only nodes cannot accept control-plane collection lifecycle mutations without a local data plane"
                    .to_owned(),
            )),
            NodeRole::Combined => Ok(()),
        }
    }

    async fn require_local_data_plane_collection(
        &self,
        collection_name: &str,
    ) -> ServiceResult<logpose_types::CollectionPlacement> {
        self.require_data_plane()?;
        let placement = self.control.collection_placement(collection_name).await?;
        if placement.route_kind == "local"
            && matches!(placement.assigned_role, NodeRole::Combined | NodeRole::Data)
        {
            return Ok(placement);
        }

        let routed_node = placement
            .owner_node
            .clone()
            .unwrap_or_else(|| placement.assigned_node.clone());

        Err(ServiceError::InvalidArgument(format!(
            "collection '{}' is assigned to node '{}' with role '{}' and is not locally served by node '{}'",
            placement_identity(&placement),
            routed_node,
            placement.assigned_role,
            self.config.node_name
        )))
    }

    async fn put_database_shared(
        &self,
        descriptor: DatabaseDescriptor,
    ) -> ServiceResult<DatabaseDescriptor> {
        self.require_control_plane_database_mutation()?;
        let leader_fence = self.control.require_local_control_plane_leader().await?;
        match &self.shared_catalog {
            SharedCatalog::Local => self.control.put_database(descriptor).await,
            SharedCatalog::Etcd(catalog) => {
                let leader_fence = required_leadership_fence(leader_fence)?;
                catalog
                    .put_database(descriptor, &leader_fence.node_id, leader_fence.lease_id)
                    .await
                    .map_err(Into::into)
            }
        }
    }

    async fn database_shared(&self, database_name: &str) -> ServiceResult<DatabaseDescriptor> {
        match &self.shared_catalog {
            SharedCatalog::Local => self.control.database(database_name).await,
            SharedCatalog::Etcd(catalog) => catalog
                .get_database(database_name)
                .await
                .map_err(Into::into),
        }
    }

    async fn databases_shared(&self) -> ServiceResult<Vec<DatabaseDescriptor>> {
        match &self.shared_catalog {
            SharedCatalog::Local => self.control.databases().await,
            SharedCatalog::Etcd(catalog) => catalog.list_databases().await.map_err(Into::into),
        }
    }

    async fn put_database_access_policy_shared(
        &self,
        policy: logpose_auth::DatabaseAccessPolicy,
    ) -> ServiceResult<logpose_auth::DatabaseAccessPolicy> {
        self.require_control_plane_database_mutation()?;
        let leader_fence = self.control.require_local_control_plane_leader().await?;
        match &self.shared_catalog {
            SharedCatalog::Local => self.control.set_database_access_policy(policy).await,
            SharedCatalog::Etcd(catalog) => {
                let leader_fence = required_leadership_fence(leader_fence)?;
                catalog
                    .put_database_access_policy(
                        policy,
                        &leader_fence.node_id,
                        leader_fence.lease_id,
                    )
                    .await
                    .map_err(Into::into)
            }
        }
    }

    async fn database_access_policy_shared(
        &self,
        database_name: &str,
    ) -> ServiceResult<logpose_auth::DatabaseAccessPolicy> {
        match &self.shared_catalog {
            SharedCatalog::Local => self.control.database_access_policy(database_name).await,
            SharedCatalog::Etcd(catalog) => catalog
                .get_database_access_policy(database_name)
                .await
                .map_err(Into::into),
        }
    }

    async fn principal_shared(&self, principal_name: &str) -> ServiceResult<Principal> {
        match &self.shared_catalog {
            SharedCatalog::Local => self.control.principal(principal_name).await,
            SharedCatalog::Etcd(catalog) => catalog
                .get_principal(principal_name)
                .await
                .map_err(Into::into),
        }
    }

    async fn ensure_shared_database_descriptor(&self, database_name: &str) -> ServiceResult<()> {
        let database_name = if database_name.trim().is_empty() {
            DEFAULT_DATABASE_NAME
        } else {
            database_name
        };
        if matches!(&self.shared_catalog, SharedCatalog::Local) {
            return Ok(());
        }
        match self.database_shared(database_name).await {
            Ok(_) => Ok(()),
            Err(ServiceError::NotFound(_)) => self
                .put_database_shared(DatabaseDescriptor::new(database_name))
                .await
                .map(|_| ()),
            Err(error) => Err(error),
        }
    }
}

fn placement_identity(placement: &logpose_types::CollectionPlacement) -> String {
    format!("{}/{}", placement.database_name, placement.collection_name)
}

fn required_leadership_fence(fence: Option<LeadershipFence>) -> ServiceResult<LeadershipFence> {
    fence.ok_or_else(|| {
        ServiceError::Internal(
            "etcd-backed control-plane mutations require a local leadership fence".to_owned(),
        )
    })
}

fn reject_promoted_read_barriers(
    placement: &logpose_types::CollectionPlacement,
    read_barrier: Option<&Snapshot>,
) -> ServiceResult<()> {
    if read_barrier.is_some() && placement.ownership_epoch.is_some_and(|epoch| epoch > 1) {
        return Err(ServiceError::FailedPrecondition(format!(
            "collection '{}' is serving at ownership epoch {} and cannot safely satisfy read barriers after promotion until replica freshness metadata is implemented",
            placement_identity(placement),
            placement.ownership_epoch.unwrap_or_default(),
        )));
    }
    Ok(())
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
}

fn parse_collection_reference(collection_name: &str) -> ServiceResult<CollectionRef> {
    let reference = match collection_name
        .trim()
        .split('/')
        .collect::<Vec<_>>()
        .as_slice()
    {
        [collection_name] => CollectionRef::new_default(*collection_name),
        [database_name, collection_name] => CollectionRef::new(*database_name, *collection_name),
        _ => {
            return Err(ServiceError::InvalidArgument(format!(
                "unsupported collection reference '{collection_name}': expected 'collection' or 'database/collection'"
            )));
        }
    };
    reference
        .validate()
        .map_err(|error| ServiceError::InvalidArgument(error.to_string()))?;
    Ok(reference)
}

fn database_role_satisfies(role: &DatabaseRole, permission: DatabasePermission) -> bool {
    match permission {
        DatabasePermission::ReadOnly => matches!(
            role,
            DatabaseRole::ReadOnly | DatabaseRole::ReadWrite | DatabaseRole::Owner
        ),
        DatabasePermission::ReadWrite => {
            matches!(role, DatabaseRole::ReadWrite | DatabaseRole::Owner)
        }
        DatabasePermission::Owner => matches!(role, DatabaseRole::Owner),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_auth::{
        AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
        Principal, PrincipalKind,
    };
    use logpose_config::BootstrapTokenConfig;
    use logpose_service::local_catalog_store;
    use logpose_storage::CreateCollectionRequest;
    use logpose_types::DistanceMetric;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

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

    #[test]
    fn parse_collection_reference_accepts_database_collection() {
        let reference = parse_collection_reference("analytics/documents")
            .expect("database-qualified collection name should parse");

        assert_eq!(reference.database_name, "analytics");
        assert_eq!(reference.collection_name, "documents");
    }

    #[test]
    fn constant_time_eq_matches_expected_string_equality() {
        assert!(constant_time_eq("reader-token", "reader-token"));
        assert!(!constant_time_eq("reader-token", "reader-token-x"));
        assert!(!constant_time_eq("reader-token", "writer-token"));
    }

    #[tokio::test]
    async fn create_collection_with_auth_binds_policy_checks_to_database_name() {
        let state = AppState::new(auth_test_config(
            "core-auth-database-scope",
            vec![
                BootstrapTokenConfig {
                    token: "operator-token".to_owned(),
                    principal: Principal::new_with_access_tier(
                        "ops-admin",
                        PrincipalKind::User,
                        AccessTier::Operator,
                    ),
                },
                BootstrapTokenConfig {
                    token: "writer-token".to_owned(),
                    principal: Principal::new_with_access_tier(
                        "writer-service",
                        PrincipalKind::Service,
                        AccessTier::Service,
                    ),
                },
            ],
        ));
        state
            .control
            .set_database_access_policy(DatabaseAccessPolicy {
                database_name: "analytics".to_owned(),
                authentication_mode: AuthenticationMode::ExternalToken,
                role_bindings: vec![DatabaseRoleBinding {
                    database_name: "analytics".to_owned(),
                    principal_name: "writer-service".to_owned(),
                    role: DatabaseRole::ReadWrite,
                }],
            })
            .await
            .expect("database policy should persist");

        let descriptor = state
            .create_collection_with_auth(
                &RequestAuth::bearer_token("writer-token"),
                CreateCollectionRequest {
                    database_name: "analytics".to_owned(),
                    name: "documents".to_owned(),
                    dimensions: 2,
                    metric: DistanceMetric::Dot,
                    replication_factor: 1,
                },
            )
            .await
            .expect("database-scoped policy should authorize the request");

        assert_eq!(descriptor.database_name, "analytics");
        assert_eq!(descriptor.name, "documents");
    }

    #[tokio::test]
    async fn disabled_database_authentication_mode_allows_unauthenticated_database_access() {
        let state = AppState::new(auth_test_config(
            "core-auth-disabled-mode",
            vec![BootstrapTokenConfig {
                token: "operator-token".to_owned(),
                principal: Principal::new_with_access_tier(
                    "ops-admin",
                    PrincipalKind::User,
                    AccessTier::Operator,
                ),
            }],
        ));
        state
            .control
            .set_database_access_policy(DatabaseAccessPolicy {
                database_name: "analytics".to_owned(),
                authentication_mode: AuthenticationMode::Disabled,
                role_bindings: Vec::new(),
            })
            .await
            .expect("database policy should persist");

        let descriptor = state
            .create_collection_with_auth(
                &RequestAuth::default(),
                CreateCollectionRequest::in_database(
                    "analytics",
                    "documents",
                    2,
                    DistanceMetric::Dot,
                ),
            )
            .await
            .expect("disabled database auth should allow unauthenticated creation");

        assert_eq!(descriptor.database_name, "analytics");
        assert_eq!(descriptor.name, "documents");
    }

    #[tokio::test]
    async fn persisted_principals_override_bootstrap_access_tier_during_authentication() {
        let state = AppState::new(auth_test_config(
            "core-auth-persisted-principal",
            vec![BootstrapTokenConfig {
                token: "operator-token".to_owned(),
                principal: Principal::new_with_access_tier(
                    "ops-admin",
                    PrincipalKind::User,
                    AccessTier::Operator,
                ),
            }],
        ));
        local_catalog_store(&state.config.storage_root)
            .put_principal(Principal::new_with_access_tier(
                "ops-admin",
                PrincipalKind::User,
                AccessTier::Observer,
            ))
            .expect("persisted principal should override bootstrap tier");

        let error = state
            .put_database_with_auth(
                &RequestAuth::bearer_token("operator-token"),
                DatabaseDescriptor::new("analytics"),
            )
            .await
            .expect_err("observer-tier persisted principal should not retain operator access");

        assert!(
            error
                .to_string()
                .contains("not allowed to perform operator actions"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn persisted_principal_overrides_survive_local_restart() {
        let config = auth_test_config(
            "core-auth-persisted-principal-restart",
            vec![BootstrapTokenConfig {
                token: "operator-token".to_owned(),
                principal: Principal::new_with_access_tier(
                    "ops-admin",
                    PrincipalKind::User,
                    AccessTier::Operator,
                ),
            }],
        );
        let state = AppState::new(config.clone());
        local_catalog_store(&state.config.storage_root)
            .put_principal(Principal::new_with_access_tier(
                "ops-admin",
                PrincipalKind::User,
                AccessTier::Observer,
            ))
            .expect("persisted principal should be updated before restart");
        drop(state);

        let restarted = AppState::new(config);
        let error = restarted
            .put_database_with_auth(
                &RequestAuth::bearer_token("operator-token"),
                DatabaseDescriptor::new("analytics"),
            )
            .await
            .expect_err("restarted runtime should preserve the persisted observer tier");

        assert!(
            error
                .to_string()
                .contains("not allowed to perform operator actions"),
            "unexpected error: {error}"
        );
    }

    fn auth_test_config(label: &str, bootstrap_tokens: Vec<BootstrapTokenConfig>) -> LogPoseConfig {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let storage_root = std::env::temp_dir().join(format!("logpose-core-{label}-{suffix}"));
        fs::create_dir_all(&storage_root).expect("auth test storage root should be created");

        let mut config = LogPoseConfig {
            node_name: label.to_owned(),
            storage_root,
            ..LogPoseConfig::default()
        };
        config.auth.bootstrap_tokens = bootstrap_tokens;
        config
    }
}
