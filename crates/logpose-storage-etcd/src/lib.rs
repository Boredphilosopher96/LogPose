//! Etcd-backed metadata overlay for collection placement assignments.

#[cfg(test)]
use anyhow as _;
use async_trait::async_trait;
#[cfg(test)]
use clap as _;
use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, LeaseKeepAliveStream, LeaseKeeper,
    PutOptions, ResponseHeader, Txn, TxnOp, WatchOptions,
};
use logpose_auth::{DatabaseAccessPolicy, Principal};
use logpose_catalog::{CollectionDescriptor, DatabaseDescriptor};
use logpose_storage::{
    CreateCollectionRequest, InspectReport, InspectTarget, LocalStorageEngine, StorageEngine,
};
use logpose_types::{
    AnnCandidate, AnnSearchRequest, CollectionAssignment, CollectionRef, CollectionStats,
    CommitAck, DEFAULT_DATABASE_NAME, EtcdMetadataConfig, LeadershipFence, LogPoseError,
    MaintenanceStatus, NodeRole, RecordId, Result, Snapshot, VisibleRecord, WriteOperation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

// The metadata configuration types (`MetadataBackend`, `EtcdMetadataConfig`,
// and `MetadataConfig`) live in `logpose-types` so that crates like
// `logpose-config` can depend only on the foundational types crate without
// pulling in the etcd client implementation.

/// Storage engine wrapper that uses etcd for assignment metadata.
#[derive(Clone)]
pub struct EtcdBackedStorageEngine {
    local: Arc<LocalStorageEngine>,
    etcd: EtcdPlacementStore,
}

/// Shared etcd-backed catalog metadata for database descriptors and policies.
#[derive(Clone)]
pub struct EtcdCatalogStore {
    etcd: EtcdPlacementStore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CollectionMetadataRevision {
    assignment_mod_revision: i64,
    descriptor_mod_revision: i64,
    owner_mod_revision: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredCollectionDescriptor {
    descriptor: CollectionDescriptor,
    ready: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct ShardReplicaTargetSet {
    replicas: Vec<ShardReplicaTarget>,
}

/// Leader-selected desired replica target for one shard.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardReplicaTarget {
    /// Node identifier selected as a replica target.
    pub node_id: String,
    /// Runtime role recorded for the replica target node.
    pub node_role: NodeRole,
}

/// One node's published local materialization report for a shard.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardReplicaReport {
    /// Reporting node identifier.
    pub node_id: String,
    /// Runtime role recorded for the reporting node.
    pub node_role: NodeRole,
    /// Whether the node currently has matching local collection state.
    pub materialized: bool,
    /// Local snapshot when the collection is materialized.
    pub snapshot: Option<Snapshot>,
    /// Ownership epoch observed when the report was published.
    pub ownership_epoch: Option<u64>,
    /// Membership mod revision observed when the report was published.
    #[serde(default)]
    pub membership_mod_revision: Option<i64>,
    /// Etcd mod revision observed when the report was read.
    #[serde(skip_serializing, default)]
    pub mod_revision: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ShardReplicaReportWithRevision {
    report: ShardReplicaReport,
    mod_revision: i64,
}

impl StoredCollectionDescriptor {
    fn pending(descriptor: &CollectionDescriptor) -> Self {
        Self {
            descriptor: descriptor.without_root_path(),
            ready: false,
        }
    }

    fn ready(descriptor: &CollectionDescriptor) -> Self {
        Self {
            descriptor: descriptor.without_root_path(),
            ready: true,
        }
    }
}

impl EtcdBackedStorageEngine {
    /// Construct the wrapper over a local storage root.
    pub fn new(root: impl AsRef<Path>, config: EtcdMetadataConfig) -> Result<Self> {
        let etcd = EtcdPlacementStore::new(config)?;
        Ok(Self {
            local: Arc::new(LocalStorageEngine::new(root)),
            etcd,
        })
    }

    async fn materialize_runtime_descriptor(
        &self,
        descriptor: CollectionDescriptor,
    ) -> Result<CollectionDescriptor> {
        match self.local.open_collection(&descriptor.lookup_name()).await {
            Ok(local_descriptor) if local_descriptor.matches_serving_identity(&descriptor) => {
                Ok(local_descriptor)
            }
            Ok(_) | Err(_) => Ok(descriptor),
        }
    }
}

impl EtcdCatalogStore {
    /// Construct one shared catalog metadata store over the configured etcd cluster.
    pub fn new(config: EtcdMetadataConfig) -> Result<Self> {
        Ok(Self {
            etcd: EtcdPlacementStore::new(config)?,
        })
    }

    /// Create or replace one database descriptor in shared metadata.
    pub async fn put_database(
        &self,
        descriptor: DatabaseDescriptor,
        leader_node_id: &str,
        leader_lease_id: i64,
    ) -> Result<DatabaseDescriptor> {
        let mut descriptor = descriptor;
        descriptor.is_default = descriptor.name == DEFAULT_DATABASE_NAME;
        match self.get_database(&descriptor.name).await {
            Ok(existing) => {
                descriptor.database_id = existing.database_id;
            }
            Err(error) if error.to_string().contains("does not exist") => {}
            Err(error) => return Err(error),
        }
        descriptor.validate()?;
        let key = self.etcd.database_descriptor_key(&descriptor.name);
        let value = serde_json::to_string(&descriptor).map_err(json_encode_message)?;
        let leadership_key = self.etcd.leadership_key();
        let leadership_value = self
            .etcd
            .leadership_value(leader_node_id, leader_lease_id)?;
        let txn = Txn::new()
            .when([Compare::value(
                leadership_key,
                CompareOp::Equal,
                leadership_value,
            )])
            .and_then([TxnOp::put(key, value, Some(PutOptions::new()))]);
        let mut client = self.etcd.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "node '{leader_node_id}' is not the active control-plane leader"
            )));
        }
        Ok(descriptor)
    }

    /// Read one shared database descriptor.
    pub async fn get_database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        validate_database_name(database_name)?;
        let key = self.etcd.database_descriptor_key(database_name);
        let mut client = self.etcd.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            if database_name == DEFAULT_DATABASE_NAME {
                return Ok(DatabaseDescriptor::new(DEFAULT_DATABASE_NAME));
            }
            return Err(LogPoseError::Message(format!(
                "database '{database_name}' does not exist"
            )));
        };
        let descriptor: DatabaseDescriptor =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// List all shared database descriptors.
    pub async fn list_databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        let mut client = self.etcd.client().await?;
        let response = client
            .get(
                self.etcd.databases_prefix(),
                Some(
                    GetOptions::new()
                        .with_prefix()
                        .with_sort(etcd_client::SortTarget::Key, etcd_client::SortOrder::Ascend),
                ),
            )
            .await
            .map_err(etcd_message)?;
        let mut descriptors = Vec::new();
        for kv in response.kvs() {
            let key = std::str::from_utf8(kv.key()).map_err(|error| {
                LogPoseError::Message(format!("failed to decode metadata key as utf-8: {error}"))
            })?;
            if !key.ends_with("/descriptor") {
                continue;
            }
            let descriptor: DatabaseDescriptor =
                serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
            descriptor.validate()?;
            descriptors.push(descriptor);
        }
        if !descriptors
            .iter()
            .any(|descriptor| descriptor.name == DEFAULT_DATABASE_NAME)
        {
            descriptors.push(DatabaseDescriptor::new(DEFAULT_DATABASE_NAME));
        }
        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(descriptors)
    }

    /// Create or replace one shared principal descriptor.
    pub async fn put_principal(&self, principal: Principal) -> Result<Principal> {
        principal.validate().map_err(string_message)?;
        let key = self.etcd.principal_descriptor_key(&principal.name);
        let value = serde_json::to_string(&principal).map_err(json_encode_message)?;
        let mut client = self.etcd.client().await?;
        client
            .put(key, value, Some(PutOptions::new()))
            .await
            .map_err(etcd_message)?;
        Ok(principal)
    }

    /// Read one shared principal descriptor.
    pub async fn get_principal(&self, principal_name: &str) -> Result<Principal> {
        validate_principal_name(principal_name)?;
        let key = self.etcd.principal_descriptor_key(principal_name);
        let mut client = self.etcd.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Err(LogPoseError::Message(format!(
                "principal '{principal_name}' does not exist"
            )));
        };
        let principal: Principal =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        principal.validate().map_err(string_message)?;
        Ok(principal)
    }

    /// List all shared principal descriptors.
    pub async fn list_principals(&self) -> Result<Vec<Principal>> {
        let mut client = self.etcd.client().await?;
        let response = client
            .get(
                self.etcd.principals_prefix(),
                Some(
                    GetOptions::new()
                        .with_prefix()
                        .with_sort(etcd_client::SortTarget::Key, etcd_client::SortOrder::Ascend),
                ),
            )
            .await
            .map_err(etcd_message)?;
        let mut principals = Vec::new();
        for kv in response.kvs() {
            let key = std::str::from_utf8(kv.key()).map_err(|error| {
                LogPoseError::Message(format!("failed to decode metadata key as utf-8: {error}"))
            })?;
            if !key.ends_with("/descriptor") {
                continue;
            }
            let principal: Principal =
                serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
            principal.validate().map_err(string_message)?;
            principals.push(principal);
        }
        principals.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(principals)
    }

    /// Create or replace one shared database access policy.
    pub async fn put_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
        leader_node_id: &str,
        leader_lease_id: i64,
    ) -> Result<DatabaseAccessPolicy> {
        policy.validate().map_err(string_message)?;
        self.get_database(&policy.database_name).await?;
        let key = self.etcd.database_policy_key(&policy.database_name);
        let value = serde_json::to_string(&policy).map_err(json_encode_message)?;
        let leadership_key = self.etcd.leadership_key();
        let leadership_value = self
            .etcd
            .leadership_value(leader_node_id, leader_lease_id)?;
        let txn = Txn::new()
            .when([Compare::value(
                leadership_key,
                CompareOp::Equal,
                leadership_value,
            )])
            .and_then([TxnOp::put(key, value, Some(PutOptions::new()))]);
        let mut client = self.etcd.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "node '{leader_node_id}' is not the active control-plane leader"
            )));
        }
        Ok(policy)
    }

    /// Read one shared database access policy.
    pub async fn get_database_access_policy(
        &self,
        database_name: &str,
    ) -> Result<DatabaseAccessPolicy> {
        validate_database_name(database_name)?;
        let key = self.etcd.database_policy_key(database_name);
        let mut client = self.etcd.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Err(LogPoseError::Message(format!(
                "database access policy '{database_name}' does not exist"
            )));
        };
        let policy: DatabaseAccessPolicy =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        policy.validate().map_err(string_message)?;
        Ok(policy)
    }
}

#[async_trait]
impl StorageEngine for EtcdBackedStorageEngine {
    async fn engine_name(&self) -> &'static str {
        "local+etcd-metadata"
    }

    async fn metadata_status(&self) -> Result<()> {
        self.etcd.metadata_status().await
    }

    async fn create_collection(
        &self,
        _request: CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        Err(LogPoseError::Message(
            "etcd-backed storage requires create_collection_with_assignment so authoritative metadata is written before local state"
                .to_owned(),
        ))
    }

    async fn create_collection_with_assignment(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
        leader_fence: Option<LeadershipFence>,
    ) -> Result<CollectionDescriptor> {
        let leader_fence = leader_fence.ok_or_else(|| {
            LogPoseError::Message(
                "etcd-backed collection creation requires a control-plane leadership fence"
                    .to_owned(),
            )
        })?;
        let collection_name = request.lookup_name();
        if self.local.open_collection(&collection_name).await.is_ok() {
            return Err(LogPoseError::Message(format!(
                "collection '{}' already exists",
                collection_name
            )));
        }
        let descriptor = self.local.plan_collection_descriptor(&request)?;
        let metadata_revision = match self
            .etcd
            .put_collection_metadata_if_absent(
                &collection_name,
                &descriptor,
                &assignment,
                &leader_fence,
            )
            .await
        {
            Ok(revision) => revision,
            Err(error) => {
                if assignment_conflict(&error) {
                    let local_collection_exists =
                        self.local.open_collection(&collection_name).await.is_ok();
                    let existing_assignment = if local_collection_exists {
                        None
                    } else {
                        self.etcd.get_assignment(&collection_name).await?
                    };
                    if matching_assignment_without_local_state(
                        &error,
                        local_collection_exists,
                        existing_assignment.as_ref(),
                        &assignment,
                    ) {
                        return Err(stale_assignment_requires_manual_reconciliation_error(
                            &collection_name,
                        ));
                    }
                }
                return Err(error);
            }
        };
        match self
            .local
            .create_collection_from_descriptor(descriptor.clone(), Some(&assignment))
        {
            Ok(local_descriptor) => match self
                .etcd
                .mark_collection_ready_if_revision_matches(
                    &collection_name,
                    &descriptor,
                    metadata_revision,
                    &leader_fence,
                )
                .await
            {
                Ok(()) => Ok(local_descriptor),
                Err(error) => {
                    let local_cleanup = match std::fs::remove_dir_all(&local_descriptor.root_path) {
                        Ok(()) => Ok(()),
                        Err(cleanup_error)
                            if cleanup_error.kind() == std::io::ErrorKind::NotFound =>
                        {
                            Ok(())
                        }
                        Err(cleanup_error) => Err(LogPoseError::Message(format!(
                            "failed to remove partially finalized local collection state for '{}': {cleanup_error}",
                            collection_name
                        ))),
                    };
                    let metadata_cleanup = self
                        .etcd
                        .delete_collection_metadata_if_revision_matches(
                            &collection_name,
                            metadata_revision,
                        )
                        .await;
                    match (local_cleanup, metadata_cleanup) {
                        (Ok(()), Ok(())) => Err(error),
                        (Err(cleanup_error), Ok(())) => Err(rollback_failure_error(
                            &collection_name,
                            &error.to_string(),
                            cleanup_error,
                        )),
                        (Ok(()), Err(rollback_error)) | (Err(_), Err(rollback_error)) => {
                            Err(rollback_failure_error(
                                &collection_name,
                                &error.to_string(),
                                rollback_error,
                            ))
                        }
                    }
                }
            },
            Err(error) => match self
                .etcd
                .delete_collection_metadata_if_revision_matches(&collection_name, metadata_revision)
                .await
            {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(rollback_failure_error(
                    &collection_name,
                    &error.to_string(),
                    rollback_error,
                )),
            },
        }
    }

    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor> {
        match self.etcd.get_descriptor(name).await? {
            Some(stored_descriptor) if stored_descriptor.ready => {
                self.materialize_runtime_descriptor(stored_descriptor.descriptor)
                    .await
            }
            Some(_) => Err(pending_descriptor_requires_manual_reconciliation_error(
                &canonical_collection_lookup_name(name),
            )),
            None => Err(LogPoseError::Message(format!(
                "collection '{}' has no authoritative descriptor metadata in etcd; reconciliation is required before serving it",
                canonical_collection_lookup_name(name)
            ))),
        }
    }

    async fn has_local_collection(&self, name: &str) -> Result<bool> {
        self.local.has_local_collection(name).await
    }

    async fn local_collection_matches_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<bool> {
        match self.local.open_collection(&descriptor.lookup_name()).await {
            Ok(local_descriptor) => Ok(local_descriptor.matches_serving_identity(descriptor)),
            Err(error) if error.to_string().contains("does not exist") => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn list_collections(&self) -> Result<Vec<CollectionDescriptor>> {
        let mut descriptors = Vec::new();
        for stored_descriptor in self.etcd.list_descriptors().await? {
            if !stored_descriptor.ready {
                continue;
            }
            descriptors.push(
                self.materialize_runtime_descriptor(stored_descriptor.descriptor)
                    .await?,
            );
        }
        Ok(descriptors)
    }

    async fn list_local_collections(&self) -> Result<Vec<CollectionDescriptor>> {
        self.local.list_local_collections().await
    }

    async fn export_local_collection_archive(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<Vec<u8>> {
        self.local.export_local_collection_archive(descriptor).await
    }

    async fn export_local_collection_archive_to_path(
        &self,
        descriptor: &CollectionDescriptor,
        archive_path: &Path,
    ) -> Result<()> {
        self.local
            .export_local_collection_archive_to_path(descriptor, archive_path)
            .await
    }

    async fn collection_assignment_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        match self.etcd.get_assignment(&descriptor.lookup_name()).await {
            Ok(Some(assignment)) => Ok(assignment),
            Ok(None) => Err(LogPoseError::Message(format!(
                "collection '{}' has no authoritative assignment metadata in etcd; reconciliation is required before serving it",
                descriptor.lookup_name()
            ))),
            Err(error) => Err(error),
        }
    }

    async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        self.local.write(collection_name, operations).await
    }

    async fn snapshot(&self, collection_name: &str) -> Result<Snapshot> {
        self.local.snapshot(collection_name).await
    }

    async fn scan_exact(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<VisibleRecord>> {
        self.local.scan_exact(collection_name, snapshot).await
    }

    async fn scan_exact_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        self.local
            .scan_exact_selected(
                collection_name,
                snapshot,
                include_mutable,
                immutable_unit_ids,
            )
            .await
    }

    async fn ann_search_selected(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        immutable_unit_ids: Vec<String>,
        request: AnnSearchRequest,
        filter: Option<Arc<dyn for<'a> Fn(&'a Value) -> bool + Send + Sync>>,
    ) -> Result<Vec<AnnCandidate>> {
        self.local
            .ann_search_selected(
                collection_name,
                snapshot,
                immutable_unit_ids,
                request,
                filter,
            )
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
        self.local
            .latest_visible_selected(
                collection_name,
                snapshot,
                record_ids,
                include_mutable,
                immutable_unit_ids,
            )
            .await
    }

    async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        self.local.flush(collection_name).await
    }

    async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        self.local.compact(collection_name).await
    }

    async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        self.local.stats(collection_name).await
    }

    async fn stats_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        self.local.stats_descriptor(descriptor, snapshot).await
    }

    async fn maintenance_status_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        self.local.maintenance_status_descriptor(descriptor).await
    }

    async fn recover_maintenance_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<()> {
        self.local.recover_maintenance_descriptor(descriptor).await
    }

    async fn stats_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        self.local.stats_snapshot(collection_name, snapshot).await
    }

    async fn inspect(&self, collection_name: &str, target: InspectTarget) -> Result<InspectReport> {
        self.local.inspect(collection_name, target).await
    }
}

#[derive(Clone)]
struct EtcdPlacementStore {
    client: Arc<tokio::sync::Mutex<Option<Client>>>,
    endpoints: Vec<String>,
    timeout_ms: u64,
    key_prefix: String,
    cluster_name: String,
}

impl EtcdPlacementStore {
    fn new(config: EtcdMetadataConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(None)),
            endpoints: config.endpoints,
            timeout_ms: config.timeout_ms,
            key_prefix: config.key_prefix.trim_end_matches('/').to_owned(),
            cluster_name: config.cluster_name,
        })
    }

    async fn client(&self) -> Result<Client> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            let options = etcd_client::ConnectOptions::default()
                .with_keep_alive(Duration::from_secs(5), Duration::from_secs(2))
                .with_timeout(Duration::from_millis(self.timeout_ms));
            let client = Client::connect(self.endpoints.clone(), Some(options))
                .await
                .map_err(etcd_message)?;
            *guard = Some(client);
        }
        guard
            .as_ref()
            .cloned()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))
    }

    fn collections_prefix(&self) -> String {
        format!(
            "{}/clusters/{}/collections/",
            self.key_prefix, self.cluster_name
        )
    }

    fn cluster_prefix(&self) -> String {
        format!("{}/clusters/{}/", self.key_prefix, self.cluster_name)
    }

    fn databases_prefix(&self) -> String {
        format!(
            "{}/clusters/{}/databases/",
            self.key_prefix, self.cluster_name
        )
    }

    fn principals_prefix(&self) -> String {
        format!(
            "{}/clusters/{}/principals/",
            self.key_prefix, self.cluster_name
        )
    }

    fn assignment_key(&self, collection_name: &str) -> String {
        let collection_name = canonical_collection_lookup_name(collection_name);
        format!(
            "{}/clusters/{}/collections/{collection_name}/assignment",
            self.key_prefix, self.cluster_name
        )
    }

    fn descriptor_key(&self, collection_name: &str) -> String {
        let collection_name = canonical_collection_lookup_name(collection_name);
        format!(
            "{}/clusters/{}/collections/{collection_name}/descriptor",
            self.key_prefix, self.cluster_name
        )
    }

    fn shard_owner_key(&self, collection: &CollectionRef, shard_id: &str) -> String {
        format!(
            "{}/clusters/{}/collections/{}/shards/{shard_id}/owner",
            self.key_prefix,
            self.cluster_name,
            collection.lookup_name()
        )
    }

    fn shard_failover_key(&self, collection: &CollectionRef, shard_id: &str) -> String {
        format!(
            "{}/clusters/{}/collections/{}/shards/{shard_id}/failover",
            self.key_prefix,
            self.cluster_name,
            collection.lookup_name()
        )
    }

    fn shard_replica_targets_key(&self, collection: &CollectionRef, shard_id: &str) -> String {
        format!(
            "{}/clusters/{}/collections/{}/shards/{shard_id}/replica_targets",
            self.key_prefix,
            self.cluster_name,
            collection.lookup_name()
        )
    }

    fn shard_replica_report_prefix(&self, collection: &CollectionRef, shard_id: &str) -> String {
        format!(
            "{}/clusters/{}/collections/{}/shards/{shard_id}/replicas/",
            self.key_prefix,
            self.cluster_name,
            collection.lookup_name()
        )
    }

    fn shard_replica_report_key(
        &self,
        collection: &CollectionRef,
        shard_id: &str,
        node_id: &str,
    ) -> String {
        format!(
            "{}{node_id}",
            self.shard_replica_report_prefix(collection, shard_id)
        )
    }

    fn leadership_key(&self) -> String {
        format!(
            "{}/clusters/{}/controllers/leader",
            self.key_prefix, self.cluster_name
        )
    }

    fn membership_key(&self, node_id: &str) -> String {
        format!(
            "{}/clusters/{}/members/{node_id}",
            self.key_prefix, self.cluster_name
        )
    }

    fn leadership_value(&self, node_id: &str, lease_id: i64) -> Result<String> {
        serde_json::to_string(&LeadershipRecord {
            node_id: node_id.to_owned(),
            lease_id,
        })
        .map_err(json_encode_message)
    }

    fn database_descriptor_key(&self, database_name: &str) -> String {
        format!(
            "{}/clusters/{}/databases/{database_name}/descriptor",
            self.key_prefix, self.cluster_name
        )
    }

    fn database_policy_key(&self, database_name: &str) -> String {
        format!(
            "{}/clusters/{}/databases/{database_name}/policy",
            self.key_prefix, self.cluster_name
        )
    }

    fn principal_descriptor_key(&self, principal_name: &str) -> String {
        format!(
            "{}/clusters/{}/principals/{principal_name}/descriptor",
            self.key_prefix, self.cluster_name
        )
    }

    async fn metadata_status(&self) -> Result<()> {
        let mut client = self.client().await?;
        client
            .get(
                self.collections_prefix(),
                Some(GetOptions::new().with_prefix().with_limit(1)),
            )
            .await
            .map_err(etcd_message)?;
        Ok(())
    }

    async fn load_cluster_metadata(&self) -> Result<ClusterMetadataSnapshot> {
        #[derive(Default)]
        struct CollectionAccumulator {
            assignment: Option<CollectionAssignment>,
            descriptor: Option<CollectionDescriptor>,
            descriptor_ready: bool,
            owner: Option<ShardOwnership>,
            replica_targets: Vec<ShardReplicaTarget>,
            replica_reports: Vec<ShardReplicaReport>,
            failover_reason: Option<String>,
        }

        let collections_prefix = self.collections_prefix();
        let members_prefix = format!(
            "{}/clusters/{}/members/",
            self.key_prefix, self.cluster_name
        );
        let leadership_key = self.leadership_key();
        let mut client = self.client().await?;
        let response = client
            .get(
                self.cluster_prefix(),
                Some(
                    GetOptions::new()
                        .with_prefix()
                        .with_sort(etcd_client::SortTarget::Key, etcd_client::SortOrder::Ascend),
                ),
            )
            .await
            .map_err(etcd_message)?;
        let revision = response
            .header()
            .map(ResponseHeader::revision)
            .unwrap_or_default();
        let mut members = Vec::<MembershipRecord>::new();
        let mut leader = None;
        let mut collections = BTreeMap::<String, CollectionAccumulator>::new();
        for kv in response.kvs() {
            let key = std::str::from_utf8(kv.key()).map_err(|error| {
                LogPoseError::Message(format!("failed to decode metadata key as utf-8: {error}"))
            })?;
            if key.starts_with(&members_prefix) {
                let mut record: MembershipRecord =
                    serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
                record.lease_id = kv.lease();
                record.mod_revision = kv.mod_revision();
                members.push(record);
                continue;
            }
            if key == leadership_key {
                leader = Some(serde_json::from_slice(kv.value()).map_err(json_decode_message)?);
                continue;
            }
            let Some(collection_suffix) = key.strip_prefix(&collections_prefix) else {
                continue;
            };
            if let Some(lookup_name) = collection_suffix.strip_suffix("/assignment") {
                collections
                    .entry(canonical_collection_lookup_name(lookup_name))
                    .or_default()
                    .assignment =
                    Some(serde_json::from_slice(kv.value()).map_err(json_decode_message)?);
                continue;
            }
            if let Some(lookup_name) = collection_suffix.strip_suffix("/descriptor") {
                let stored: StoredCollectionDescriptor =
                    serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
                let entry = collections
                    .entry(canonical_collection_lookup_name(lookup_name))
                    .or_default();
                entry.descriptor = Some(stored.descriptor);
                entry.descriptor_ready = stored.ready;
                continue;
            }
            if collection_suffix.ends_with("/owner") && collection_suffix.contains("/shards/") {
                let mut ownership: ShardOwnership =
                    serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
                ownership.mod_revision = kv.mod_revision();
                let lookup_name = ownership.collection.lookup_name();
                collections.entry(lookup_name).or_default().owner = Some(ownership);
                continue;
            }
            if let Some(lookup_name) = collection_suffix
                .strip_suffix("/replica_targets")
                .filter(|suffix| suffix.contains("/shards/"))
            {
                let lookup_name = collection_ref_from_lookup_name(
                    lookup_name.split("/shards/").next().unwrap_or_default(),
                )
                .lookup_name();
                let target_set: ShardReplicaTargetSet =
                    serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
                collections.entry(lookup_name).or_default().replica_targets = target_set.replicas;
                continue;
            }
            if collection_suffix.contains("/replicas/") && collection_suffix.contains("/shards/") {
                let lookup_name = collection_ref_from_lookup_name(
                    collection_suffix
                        .split("/shards/")
                        .next()
                        .unwrap_or_default(),
                )
                .lookup_name();
                let mut report: ShardReplicaReport =
                    serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
                report.mod_revision = kv.mod_revision();
                collections
                    .entry(lookup_name)
                    .or_default()
                    .replica_reports
                    .push(report);
                continue;
            }
            if collection_suffix.ends_with("/failover") && collection_suffix.contains("/shards/") {
                let reason: ShardFailoverReason =
                    serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
                let lookup_name = collection_ref_from_lookup_name(
                    collection_suffix
                        .split("/shards/")
                        .next()
                        .unwrap_or_default(),
                )
                .lookup_name();
                collections.entry(lookup_name).or_default().failover_reason = Some(reason.reason);
            }
        }
        members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(ClusterMetadataSnapshot {
            revision,
            members,
            leader,
            collections: collections
                .into_iter()
                .map(|(lookup_name, mut accumulator)| {
                    accumulator
                        .replica_targets
                        .sort_by(|left, right| left.node_id.cmp(&right.node_id));
                    accumulator
                        .replica_reports
                        .sort_by(|left, right| left.node_id.cmp(&right.node_id));
                    ClusterCollectionMetadata {
                        collection: accumulator
                            .descriptor
                            .as_ref()
                            .map(CollectionDescriptor::collection_ref)
                            .or_else(|| {
                                accumulator
                                    .owner
                                    .as_ref()
                                    .map(|owner| owner.collection.clone())
                            })
                            .unwrap_or_else(|| collection_ref_from_lookup_name(&lookup_name)),
                        assignment: accumulator.assignment,
                        descriptor: accumulator.descriptor,
                        descriptor_ready: accumulator.descriptor_ready,
                        owner: accumulator.owner,
                        replica_targets: accumulator.replica_targets,
                        replica_reports: accumulator.replica_reports,
                        failover_reason: accumulator.failover_reason,
                    }
                })
                .collect(),
        })
    }

    async fn put_collection_metadata_if_absent(
        &self,
        collection_name: &str,
        descriptor: &CollectionDescriptor,
        assignment: &CollectionAssignment,
        leader_fence: &LeadershipFence,
    ) -> Result<CollectionMetadataRevision> {
        let collection = collection_ref_from_lookup_name(collection_name);
        let assignment_key = self.assignment_key(collection_name);
        let descriptor_key = self.descriptor_key(collection_name);
        let owner_key = self.shard_owner_key(&collection, "0");
        let assignment_value = serde_json::to_string(assignment).map_err(json_encode_message)?;
        let descriptor_value =
            serde_json::to_string(&StoredCollectionDescriptor::pending(descriptor))
                .map_err(json_encode_message)?;
        let owner_value = serde_json::to_string(&ShardOwnership {
            collection,
            shard_id: "0".to_owned(),
            owner_node_id: assignment.assigned_node.clone(),
            epoch: 1,
            mod_revision: 0,
        })
        .map_err(json_encode_message)?;
        let mut compares = self.leader_fence_compares(leader_fence).await?;
        compares.extend([
            Compare::version(assignment_key.clone(), CompareOp::Equal, 0),
            Compare::version(descriptor_key.clone(), CompareOp::Equal, 0),
            Compare::version(owner_key.clone(), CompareOp::Equal, 0),
        ]);
        let txn = Txn::new().when(compares).and_then([
            TxnOp::put(
                assignment_key.clone(),
                assignment_value,
                Some(PutOptions::new()),
            ),
            TxnOp::put(
                descriptor_key.clone(),
                descriptor_value,
                Some(PutOptions::new()),
            ),
            TxnOp::put(owner_key, owner_value, Some(PutOptions::new())),
        ]);
        let mut client = self.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if response.succeeded() {
            let revision = response
                .header()
                .map(ResponseHeader::revision)
                .ok_or_else(|| {
                    LogPoseError::Message(
                        "etcd txn response missing header revision after collection metadata write"
                            .to_owned(),
                    )
                })?;
            Ok(CollectionMetadataRevision {
                assignment_mod_revision: revision,
                descriptor_mod_revision: revision,
                owner_mod_revision: revision,
            })
        } else {
            Err(LogPoseError::Message(format!(
                "collection '{collection_name}' already has metadata assignment in etcd"
            )))
        }
    }

    async fn mark_collection_ready_if_revision_matches(
        &self,
        collection_name: &str,
        descriptor: &CollectionDescriptor,
        revision: CollectionMetadataRevision,
        leader_fence: &LeadershipFence,
    ) -> Result<()> {
        let assignment_key = self.assignment_key(collection_name);
        let descriptor_key = self.descriptor_key(collection_name);
        let owner_key =
            self.shard_owner_key(&collection_ref_from_lookup_name(collection_name), "0");
        let descriptor_value =
            serde_json::to_string(&StoredCollectionDescriptor::ready(descriptor))
                .map_err(json_encode_message)?;
        let mut compares = self.leader_fence_compares(leader_fence).await?;
        compares.extend([
            Compare::mod_revision(
                assignment_key.clone(),
                CompareOp::Equal,
                revision.assignment_mod_revision,
            ),
            Compare::mod_revision(
                descriptor_key.clone(),
                CompareOp::Equal,
                revision.descriptor_mod_revision,
            ),
            Compare::mod_revision(owner_key, CompareOp::Equal, revision.owner_mod_revision),
        ]);
        let txn = Txn::new().when(compares).and_then([TxnOp::put(
            descriptor_key,
            descriptor_value,
            Some(PutOptions::new()),
        )]);
        let mut client = self.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(LogPoseError::Message(format!(
                "authoritative etcd metadata for collection '{collection_name}' changed before local state could be finalized; manual reconciliation is required"
            )))
        }
    }

    async fn get_assignment(&self, collection_name: &str) -> Result<Option<CollectionAssignment>> {
        let key = self.assignment_key(collection_name);
        let mut client = self.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let assignment = serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        Ok(Some(assignment))
    }

    async fn get_descriptor(
        &self,
        collection_name: &str,
    ) -> Result<Option<StoredCollectionDescriptor>> {
        let key = self.descriptor_key(collection_name);
        let mut client = self.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let descriptor = serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        Ok(Some(descriptor))
    }

    async fn get_descriptor_with_revision(
        &self,
        collection_name: &str,
    ) -> Result<Option<(StoredCollectionDescriptor, i64)>> {
        let key = self.descriptor_key(collection_name);
        let mut client = self.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let descriptor = serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        Ok(Some((descriptor, kv.mod_revision())))
    }

    async fn get_replica_report_with_revision(
        &self,
        collection: &CollectionRef,
        shard_id: &str,
        node_id: &str,
    ) -> Result<Option<ShardReplicaReportWithRevision>> {
        let key = self.shard_replica_report_key(collection, shard_id, node_id);
        let mut client = self.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let mut report: ShardReplicaReport =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        report.mod_revision = kv.mod_revision();
        Ok(Some(ShardReplicaReportWithRevision {
            report,
            mod_revision: kv.mod_revision(),
        }))
    }

    async fn ready_member_with_revision(
        &self,
        node_id: &str,
    ) -> Result<Option<(MembershipRecord, i64)>> {
        let key = self.membership_key(node_id);
        let mut client = self.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let mut record: MembershipRecord =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        record.lease_id = kv.lease();
        record.mod_revision = kv.mod_revision();
        Ok(Some((record, kv.mod_revision())))
    }

    async fn member_with_expected_lease_and_revision(
        &self,
        node_id: &str,
        expected_lease_id: i64,
    ) -> Result<Option<(MembershipRecord, i64)>> {
        let Some((record, mod_revision)) = self.ready_member_with_revision(node_id).await? else {
            return Ok(None);
        };
        if record.lease_id != expected_lease_id {
            return Ok(None);
        }
        Ok(Some((record, mod_revision)))
    }

    async fn ready_member_with_expected_lease_and_revision(
        &self,
        node_id: &str,
        expected_lease_id: i64,
    ) -> Result<Option<(MembershipRecord, i64)>> {
        let Some((record, mod_revision)) = self
            .member_with_expected_lease_and_revision(node_id, expected_lease_id)
            .await?
        else {
            return Ok(None);
        };
        if record.state != "ready" {
            return Ok(None);
        }
        Ok(Some((record, mod_revision)))
    }

    async fn leader_fence_compares(&self, leader_fence: &LeadershipFence) -> Result<Vec<Compare>> {
        let Some((member, membership_mod_revision)) = self
            .ready_member_with_expected_lease_and_revision(
                &leader_fence.node_id,
                leader_fence.membership_lease_id,
            )
            .await?
        else {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not the active control-plane leader",
                leader_fence.node_id
            )));
        };
        if !matches!(member.node_role, NodeRole::Combined | NodeRole::Control) {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not a registered control-plane member",
                leader_fence.node_id
            )));
        }
        let leadership_key = self.leadership_key();
        let leadership_value =
            self.leadership_value(&leader_fence.node_id, leader_fence.lease_id)?;
        let membership_key = self.membership_key(&leader_fence.node_id);
        Ok(vec![
            Compare::value(leadership_key, CompareOp::Equal, leadership_value),
            Compare::mod_revision(
                membership_key.clone(),
                CompareOp::Equal,
                membership_mod_revision,
            ),
            Compare::lease(
                membership_key,
                CompareOp::Equal,
                leader_fence.membership_lease_id,
            ),
        ])
    }

    async fn list_descriptors(&self) -> Result<Vec<StoredCollectionDescriptor>> {
        let mut client = self.client().await?;
        let response = client
            .get(
                self.collections_prefix(),
                Some(
                    GetOptions::new()
                        .with_prefix()
                        .with_sort(etcd_client::SortTarget::Key, etcd_client::SortOrder::Ascend),
                ),
            )
            .await
            .map_err(etcd_message)?;
        let mut descriptors = Vec::new();
        for kv in response.kvs() {
            let key = std::str::from_utf8(kv.key()).map_err(|error| {
                LogPoseError::Message(format!("failed to decode metadata key as utf-8: {error}"))
            })?;
            if !key.ends_with("/descriptor") {
                continue;
            }
            descriptors.push(serde_json::from_slice(kv.value()).map_err(json_decode_message)?);
        }
        Ok(descriptors)
    }

    async fn delete_collection_metadata_if_revision_matches(
        &self,
        collection_name: &str,
        revision: CollectionMetadataRevision,
    ) -> Result<()> {
        let assignment_key = self.assignment_key(collection_name);
        let descriptor_key = self.descriptor_key(collection_name);
        let collection = collection_ref_from_lookup_name(collection_name);
        let owner_key = self.shard_owner_key(&collection, "0");
        let shard_prefix = format!(
            "{}/clusters/{}/collections/{}/shards/0/",
            self.key_prefix,
            self.cluster_name,
            collection.lookup_name()
        );
        let txn = Txn::new()
            .when([
                Compare::mod_revision(
                    assignment_key.clone(),
                    CompareOp::Equal,
                    revision.assignment_mod_revision,
                ),
                Compare::mod_revision(
                    descriptor_key.clone(),
                    CompareOp::Equal,
                    revision.descriptor_mod_revision,
                ),
                Compare::mod_revision(
                    owner_key.clone(),
                    CompareOp::Equal,
                    revision.owner_mod_revision,
                ),
            ])
            .and_then([
                TxnOp::delete(assignment_key, Some(DeleteOptions::new())),
                TxnOp::delete(descriptor_key, Some(DeleteOptions::new())),
                TxnOp::delete(owner_key, Some(DeleteOptions::new())),
                TxnOp::delete(shard_prefix, Some(DeleteOptions::new().with_prefix())),
            ]);
        let mut client = self.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(LogPoseError::Message(format!(
                "authoritative etcd metadata for collection '{collection_name}' changed before rollback could remove it; manual reconciliation is required"
            )))
        }
    }
}

fn json_encode_message(error: serde_json::Error) -> LogPoseError {
    LogPoseError::Message(format!("failed to encode metadata payload: {error}"))
}

fn json_decode_message(error: serde_json::Error) -> LogPoseError {
    LogPoseError::Message(format!("failed to decode metadata payload: {error}"))
}

fn etcd_message(error: etcd_client::Error) -> LogPoseError {
    LogPoseError::Message(format!("etcd metadata operation failed: {error}"))
}

fn string_message(error: String) -> LogPoseError {
    LogPoseError::Message(error)
}

fn validate_database_name(value: &str) -> Result<()> {
    DatabaseDescriptor::new(value).validate()
}

fn validate_principal_name(value: &str) -> Result<()> {
    Principal::new(value, logpose_auth::PrincipalKind::Service)
        .validate()
        .map_err(string_message)
}

fn assignment_conflict(error: &LogPoseError) -> bool {
    error
        .to_string()
        .contains("already has metadata assignment in etcd")
}

fn canonical_collection_lookup_name(collection_name: &str) -> String {
    let parts = collection_name.split('/').collect::<Vec<_>>();
    if parts.len() == 2 && parts.iter().all(|part| !part.trim().is_empty()) {
        format!("{}/{}", parts[0], parts[1])
    } else {
        CollectionRef::new_default(collection_name).lookup_name()
    }
}

fn collection_ref_from_lookup_name(collection_name: &str) -> CollectionRef {
    let canonical = canonical_collection_lookup_name(collection_name);
    match canonical.split('/').collect::<Vec<_>>().as_slice() {
        [database_name, collection_name] => CollectionRef::new(*database_name, *collection_name),
        _ => CollectionRef::new_default(canonical),
    }
}

fn matching_assignment_without_local_state(
    error: &LogPoseError,
    local_collection_exists: bool,
    existing_assignment: Option<&CollectionAssignment>,
    requested_assignment: &CollectionAssignment,
) -> bool {
    assignment_conflict(error)
        && !local_collection_exists
        && existing_assignment == Some(requested_assignment)
}

fn stale_assignment_requires_manual_reconciliation_error(collection_name: &str) -> LogPoseError {
    LogPoseError::Message(format!(
        "collection '{collection_name}' has matching assignment metadata in etcd but no local collection state; manual reconciliation is required before recreating it"
    ))
}

fn pending_descriptor_requires_manual_reconciliation_error(collection_name: &str) -> LogPoseError {
    LogPoseError::Message(format!(
        "collection '{collection_name}' has authoritative metadata in etcd but local state finalization is still pending; manual reconciliation is required before serving it"
    ))
}

fn rollback_failure_error(
    collection_name: &str,
    create_error: &str,
    rollback_error: LogPoseError,
) -> LogPoseError {
    LogPoseError::Message(format!(
        "{create_error}; rollback of authoritative etcd metadata for collection '{collection_name}' also failed: {rollback_error}"
    ))
}

/// Lease-backed membership record registered in etcd.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipLease {
    /// Node identifier registered in metadata.
    pub node_id: String,
    /// Etcd lease identifier backing this membership.
    pub lease_id: i64,
    /// Etcd key containing the membership payload.
    pub key: String,
}

/// Controller leadership claim record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeadershipLease {
    /// Current leader node identifier.
    pub node_id: String,
    /// Etcd lease identifier backing the leadership claim.
    pub lease_id: i64,
    /// Etcd key containing the leadership payload.
    pub key: String,
}

/// Membership payload persisted in etcd.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipRecord {
    /// Node identifier registered in metadata.
    pub node_id: String,
    /// Runtime role advertised by the member.
    pub node_role: logpose_types::NodeRole,
    /// Node state marker used by control loops.
    pub state: String,
    /// Active etcd lease currently backing this membership row.
    #[serde(default)]
    pub lease_id: i64,
    /// Etcd mod revision observed when the membership row was read.
    #[serde(skip_serializing, default)]
    pub mod_revision: i64,
    /// Advertised REST endpoint for peer-to-peer replica repair.
    #[serde(default)]
    pub rest_endpoint: Option<String>,
}

/// Leadership payload persisted in etcd.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeadershipRecord {
    /// Current leader node identifier.
    pub node_id: String,
    /// Active etcd lease backing the current leadership claim.
    pub lease_id: i64,
}

/// Shard ownership record used for epoch-based write fencing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardOwnership {
    /// Collection owning the shard.
    #[serde(flatten)]
    pub collection: CollectionRef,
    /// Shard identifier string.
    pub shard_id: String,
    /// Current owner node identifier.
    pub owner_node_id: String,
    /// Monotonic ownership epoch.
    pub epoch: u64,
    /// Etcd mod revision observed when the record was read.
    #[serde(skip_serializing, default)]
    pub mod_revision: i64,
}

/// Operator-visible owner-transition reason persisted in etcd.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardFailoverReason {
    /// Human-readable reason for the most recent owner change.
    pub reason: String,
}

/// Result of a promotion or ownership move transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PromotionResult {
    /// Ownership update transaction committed.
    Applied(ShardOwnership),
    /// Ownership update conflicted with a newer revision.
    Conflict,
}

/// Watch-friendly cluster metadata snapshot loaded from authoritative etcd keys.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterMetadataSnapshot {
    /// Cluster-wide etcd revision observed for the snapshot.
    pub revision: i64,
    /// All currently visible membership records.
    pub members: Vec<MembershipRecord>,
    /// Current controller leader when one exists.
    pub leader: Option<LeadershipRecord>,
    /// Collection-level metadata bundles keyed under the collections prefix.
    pub collections: Vec<ClusterCollectionMetadata>,
}

/// Authoritative metadata bundle for one collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterCollectionMetadata {
    /// Canonical database/collection identity.
    pub collection: CollectionRef,
    /// Current placement assignment when one exists.
    pub assignment: Option<CollectionAssignment>,
    /// Current descriptor payload when one exists.
    pub descriptor: Option<CollectionDescriptor>,
    /// Whether the authoritative descriptor has been marked ready for serving.
    pub descriptor_ready: bool,
    /// Current shard owner for shard `0` when one exists.
    pub owner: Option<ShardOwnership>,
    /// Leader-selected desired replica targets for shard `0`.
    pub replica_targets: Vec<ShardReplicaTarget>,
    /// Per-node replica materialization reports for shard `0`.
    pub replica_reports: Vec<ShardReplicaReport>,
    /// Last recorded reason for an owner transition when one exists.
    pub failover_reason: Option<String>,
}

/// Distributed coordination helper over etcd leases and CAS primitives.
#[derive(Clone)]
pub struct EtcdCoordinationClient {
    store: EtcdPlacementStore,
    config: EtcdMetadataConfig,
    lease_sessions: Arc<tokio::sync::Mutex<BTreeMap<i64, LeaseSession>>>,
}

#[derive(Debug)]
struct LeaseSession {
    keeper: LeaseKeeper,
}

impl EtcdCoordinationClient {
    /// Build a coordination helper from etcd metadata config.
    pub fn new(config: EtcdMetadataConfig) -> Result<Self> {
        let store = EtcdPlacementStore::new(config.clone())?;
        Ok(Self {
            store,
            config,
            lease_sessions: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        })
    }

    /// Load a point-in-time cluster metadata snapshot from authoritative etcd keys.
    pub async fn load_cluster_metadata(&self) -> Result<ClusterMetadataSnapshot> {
        self.store.load_cluster_metadata().await
    }

    /// Block until any authoritative metadata under the cluster prefix changes.
    pub async fn wait_for_cluster_metadata_change(&self, revision: i64) -> Result<i64> {
        let start_revision = revision.saturating_add(1);
        let mut client = self.store.client().await?;
        let (_, mut stream) = client
            .watch(
                self.store.cluster_prefix(),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(start_revision)
                        .with_progress_notify(),
                ),
            )
            .await
            .map_err(etcd_message)?;
        loop {
            let Some(response) = stream.message().await.map_err(etcd_message)? else {
                return Err(LogPoseError::Message(
                    "etcd metadata watch stream ended unexpectedly".to_owned(),
                ));
            };
            if response.canceled() {
                let compact_revision = response.compact_revision();
                let cancel_reason = response.cancel_reason();
                let detail = if compact_revision > 0 {
                    format!("watch was compacted at revision {compact_revision}")
                } else if cancel_reason.is_empty() {
                    "watch was canceled".to_owned()
                } else {
                    cancel_reason.to_owned()
                };
                return Err(LogPoseError::Message(format!(
                    "etcd metadata watch failed: {detail}"
                )));
            }
            if response.created() || response.events().is_empty() {
                continue;
            }
            return Ok(response
                .header()
                .map(ResponseHeader::revision)
                .unwrap_or(start_revision));
        }
    }

    /// Register node membership with an etcd lease.
    pub async fn register_membership(
        &self,
        node_id: &str,
        node_role: logpose_types::NodeRole,
    ) -> Result<MembershipLease> {
        self.register_membership_with_endpoint(node_id, node_role, None)
            .await
    }

    /// Register node membership with an advertised REST endpoint.
    pub async fn register_membership_with_endpoint(
        &self,
        node_id: &str,
        node_role: logpose_types::NodeRole,
        rest_endpoint: Option<&str>,
    ) -> Result<MembershipLease> {
        let membership_key = self.store.membership_key(node_id);
        let mut client = self.store.client().await?;
        let lease = client
            .lease_grant(self.config.membership_ttl_secs, None)
            .await
            .map_err(etcd_message)?;
        let lease_id = lease.id();
        let existing_state = client
            .get(membership_key.clone(), None)
            .await
            .map_err(etcd_message)?
            .kvs()
            .first()
            .map(|kv| {
                let mut record: MembershipRecord =
                    serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
                record.lease_id = kv.lease();
                record.mod_revision = kv.mod_revision();
                Ok(record.state)
            })
            .transpose()?
            .unwrap_or_else(|| "ready".to_owned());
        let encoded = serde_json::to_string(&MembershipRecord {
            node_id: node_id.to_owned(),
            node_role,
            state: existing_state,
            lease_id,
            mod_revision: 0,
            rest_endpoint: rest_endpoint.map(ToOwned::to_owned),
        })
        .map_err(json_encode_message)?;
        match client
            .put(
                membership_key.clone(),
                encoded,
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await
        {
            Ok(_) => {
                if let Err(error) = self.attach_keep_alive_session(&mut client, lease_id).await {
                    let _ = client.lease_revoke(lease_id).await;
                    return Err(error);
                }
                Ok(MembershipLease {
                    node_id: node_id.to_owned(),
                    lease_id,
                    key: membership_key,
                })
            }
            Err(error) => {
                let _ = client.lease_revoke(lease_id).await;
                Err(etcd_message(error))
            }
        }
    }

    /// Keep one lease alive by issuing a keep-alive signal.
    pub async fn keep_alive(&self, lease_id: i64) -> Result<()> {
        let mut sessions = self.lease_sessions.lock().await;
        let session = sessions.get_mut(&lease_id).ok_or_else(|| {
            LogPoseError::Message(format!(
                "no keep-alive session is registered for lease '{lease_id}'"
            ))
        })?;
        let keeper = &mut session.keeper;
        keeper.keep_alive().await.map_err(etcd_message)?;
        Ok(())
    }

    /// Revoke one lease and remove any local keep-alive session.
    pub async fn revoke_lease(&self, lease_id: i64) -> Result<()> {
        self.lease_sessions.lock().await.remove(&lease_id);
        let mut client = self.store.client().await?;
        client.lease_revoke(lease_id).await.map_err(etcd_message)?;
        Ok(())
    }

    /// Try to acquire controller leadership using lease-backed CAS.
    pub async fn try_acquire_leadership(
        &self,
        node_id: &str,
        expected_membership_lease_id: i64,
    ) -> Result<Option<LeadershipLease>> {
        let leadership_key = format!(
            "{}/clusters/{}/controllers/leader",
            self.store.key_prefix, self.config.cluster_name
        );
        let Some((member, membership_mod_revision)) = self
            .store
            .ready_member_with_expected_lease_and_revision(node_id, expected_membership_lease_id)
            .await?
        else {
            return Ok(None);
        };
        if !matches!(member.node_role, NodeRole::Combined | NodeRole::Control) {
            return Ok(None);
        }
        let membership_key = self.store.membership_key(node_id);
        let mut client = self.store.client().await?;
        let lease = client
            .lease_grant(self.config.leadership_ttl_secs, None)
            .await
            .map_err(etcd_message)?;
        let lease_id = lease.id();
        let encoded = serde_json::to_string(&LeadershipRecord {
            node_id: node_id.to_owned(),
            lease_id,
        })
        .map_err(json_encode_message)?;
        let txn = Txn::new()
            .when([
                Compare::version(leadership_key.clone(), CompareOp::Equal, 0),
                Compare::mod_revision(
                    membership_key.clone(),
                    CompareOp::Equal,
                    membership_mod_revision,
                ),
                Compare::lease(
                    membership_key,
                    CompareOp::Equal,
                    expected_membership_lease_id,
                ),
            ])
            .and_then([TxnOp::put(
                leadership_key.clone(),
                encoded,
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        let response = match client.txn(txn).await {
            Ok(response) => response,
            Err(error) => {
                let _ = client.lease_revoke(lease_id).await;
                return Err(etcd_message(error));
            }
        };
        if !response.succeeded() {
            let _ = client.lease_revoke(lease_id).await;
            return Ok(None);
        }
        if let Err(error) = self.attach_keep_alive_session(&mut client, lease_id).await {
            let _ = client.lease_revoke(lease_id).await;
            return Err(error);
        }
        Ok(Some(LeadershipLease {
            node_id: node_id.to_owned(),
            lease_id,
            key: leadership_key,
        }))
    }

    /// Validate that one runtime still holds both the local membership lease and controller leadership.
    pub async fn validate_local_leadership(
        &self,
        node_id: &str,
        expected_membership_lease_id: i64,
        expected_leadership_lease_id: i64,
    ) -> Result<bool> {
        let Some((member, _)) = self
            .store
            .ready_member_with_expected_lease_and_revision(node_id, expected_membership_lease_id)
            .await?
        else {
            return Ok(false);
        };
        if !matches!(member.node_role, NodeRole::Combined | NodeRole::Control) {
            return Ok(false);
        }
        Ok(self.current_leader().await?.is_some_and(|leader| {
            leader.node_id == node_id && leader.lease_id == expected_leadership_lease_id
        }))
    }

    /// Return all currently visible membership records under the configured cluster.
    pub async fn list_membership(&self) -> Result<Vec<MembershipRecord>> {
        let membership_prefix = format!(
            "{}/clusters/{}/members/",
            self.store.key_prefix, self.config.cluster_name
        );
        let mut client = self.store.client().await?;
        let response = client
            .get(
                membership_prefix,
                Some(
                    GetOptions::new()
                        .with_prefix()
                        .with_sort(etcd_client::SortTarget::Key, etcd_client::SortOrder::Ascend),
                ),
            )
            .await
            .map_err(etcd_message)?;
        response
            .kvs()
            .iter()
            .map(|kv| {
                let mut record: MembershipRecord =
                    serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
                record.lease_id = kv.lease();
                record.mod_revision = kv.mod_revision();
                Ok(record)
            })
            .collect()
    }

    /// Read one membership record by node identifier.
    pub async fn membership(&self, node_id: &str) -> Result<Option<MembershipRecord>> {
        let key = self.store.membership_key(node_id);
        let mut client = self.store.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let mut record: MembershipRecord =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        record.lease_id = kv.lease();
        record.mod_revision = kv.mod_revision();
        Ok(Some(record))
    }

    /// Read the authoritative placement assignment for one collection.
    pub async fn collection_assignment(
        &self,
        collection: &CollectionRef,
    ) -> Result<Option<CollectionAssignment>> {
        self.store.get_assignment(&collection.lookup_name()).await
    }

    /// Update one node membership state while preserving the active lease.
    pub async fn set_membership_state(
        &self,
        node_id: &str,
        state: &str,
        leader_fence: &LeadershipFence,
    ) -> Result<MembershipRecord> {
        let state = state.trim();
        if state.is_empty() {
            return Err(LogPoseError::Message(
                "membership state must not be empty".to_owned(),
            ));
        }
        let key = self.store.membership_key(node_id);
        let mut client = self.store.client().await?;
        let response = client.get(key.clone(), None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Err(LogPoseError::Message(format!(
                "membership record for node '{node_id}' does not exist"
            )));
        };
        let mut record: MembershipRecord =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        record.state = state.to_owned();
        let lease_id = kv.lease();
        record.lease_id = lease_id;
        record.mod_revision = kv.mod_revision();
        let encoded = serde_json::to_string(&record).map_err(json_encode_message)?;
        let mut compares = self.store.leader_fence_compares(leader_fence).await?;
        compares.push(Compare::mod_revision(
            key.clone(),
            CompareOp::Equal,
            kv.mod_revision(),
        ));
        let txn = Txn::new().when(compares).and_then([TxnOp::put(
            key,
            encoded,
            Some(PutOptions::new().with_lease(lease_id)),
        )]);
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not the active control-plane leader, or membership state for node '{node_id}' changed concurrently; retry the operation",
                leader_fence.node_id,
            )));
        }
        Ok(record)
    }

    /// Restore one local membership record from an expected prior state without
    /// mutating any other node's membership.
    pub async fn restore_local_membership_state(
        &self,
        node_id: &str,
        expected_state: &str,
        state: &str,
    ) -> Result<MembershipRecord> {
        let expected_state = expected_state.trim();
        let state = state.trim();
        if expected_state.is_empty() || state.is_empty() {
            return Err(LogPoseError::Message(
                "membership state must not be empty".to_owned(),
            ));
        }
        let key = self.store.membership_key(node_id);
        let mut client = self.store.client().await?;
        let response = client.get(key.clone(), None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Err(LogPoseError::Message(format!(
                "membership record for node '{node_id}' does not exist"
            )));
        };
        let mut record: MembershipRecord =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        if record.state != expected_state {
            return Err(LogPoseError::Message(format!(
                "membership record for node '{node_id}' is '{}' and cannot be restored to '{state}' from '{expected_state}'",
                record.state
            )));
        }
        record.state = state.to_owned();
        let lease_id = kv.lease();
        record.lease_id = lease_id;
        let encoded = serde_json::to_string(&record).map_err(json_encode_message)?;
        let txn = Txn::new()
            .when([Compare::mod_revision(
                key.clone(),
                CompareOp::Equal,
                kv.mod_revision(),
            )])
            .and_then([TxnOp::put(
                key,
                encoded,
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "membership state for node '{node_id}' changed concurrently; retry the operation"
            )));
        }
        Ok(record)
    }

    /// Return the current controller leader when one exists.
    pub async fn current_leader(&self) -> Result<Option<LeadershipRecord>> {
        let leadership_key = format!(
            "{}/clusters/{}/controllers/leader",
            self.store.key_prefix, self.config.cluster_name
        );
        let mut client = self.store.client().await?;
        let response = client
            .get(leadership_key, None)
            .await
            .map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let record = serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        Ok(Some(record))
    }

    /// Read the current owner for one shard with mod revision for CAS updates.
    pub async fn shard_owner(
        &self,
        collection: &CollectionRef,
        shard_id: &str,
    ) -> Result<Option<ShardOwnership>> {
        let key = self.shard_owner_key(collection, shard_id);
        let mut client = self.store.client().await?;
        let response = client.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let mut ownership: ShardOwnership =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        ownership.mod_revision = kv.mod_revision();
        Ok(Some(ownership))
    }

    /// Persist the leader-selected desired replica targets for one shard.
    pub async fn set_shard_replica_targets(
        &self,
        collection: &CollectionRef,
        shard_id: &str,
        replica_targets: Vec<ShardReplicaTarget>,
        leader_fence: &LeadershipFence,
    ) -> Result<Vec<ShardReplicaTarget>> {
        let key = self.store.shard_replica_targets_key(collection, shard_id);
        let encoded = serde_json::to_string(&ShardReplicaTargetSet {
            replicas: replica_targets.clone(),
        })
        .map_err(json_encode_message)?;
        let txn = Txn::new()
            .when(self.store.leader_fence_compares(leader_fence).await?)
            .and_then([TxnOp::put(key, encoded, Some(PutOptions::new()))]);
        let mut client = self.store.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not the active control-plane leader",
                leader_fence.node_id
            )));
        }
        Ok(replica_targets)
    }

    /// Publish one node's current local materialization report for a shard.
    pub async fn publish_shard_replica_report(
        &self,
        collection: &CollectionRef,
        shard_id: &str,
        report: &ShardReplicaReport,
        expected_membership_lease_id: i64,
        expected_report_mod_revision: Option<i64>,
    ) -> Result<bool> {
        let Some((member, membership_mod_revision)) = self
            .store
            .member_with_expected_lease_and_revision(&report.node_id, expected_membership_lease_id)
            .await?
        else {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not currently registered in cluster membership",
                report.node_id
            )));
        };
        if !matches!(member.node_role, NodeRole::Combined | NodeRole::Data)
            || member.node_role != report.node_role
        {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not a registered data-serving member",
                report.node_id
            )));
        }
        if report.materialized && member.state != "ready" {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not a ready data-serving member",
                report.node_id
            )));
        }
        let membership_key = self.store.membership_key(&report.node_id);
        let key = self
            .store
            .shard_replica_report_key(collection, shard_id, &report.node_id);
        let mut persisted = report.clone();
        persisted.membership_mod_revision = Some(membership_mod_revision);
        let existing_report = self
            .store
            .get_replica_report_with_revision(collection, shard_id, &report.node_id)
            .await?;
        if let Some(existing) = existing_report.as_ref() {
            persisted.mod_revision = existing.mod_revision;
        }
        if existing_report
            .as_ref()
            .is_some_and(|existing| existing.report == persisted)
        {
            return Ok(false);
        }
        let encoded = serde_json::to_string(&persisted).map_err(json_encode_message)?;
        let report_compare = match expected_report_mod_revision {
            Some(expected_mod_revision) => {
                Compare::mod_revision(key.clone(), CompareOp::Equal, expected_mod_revision)
            }
            None => existing_report.map_or_else(
                || Compare::create_revision(key.clone(), CompareOp::Equal, 0),
                |existing| {
                    Compare::mod_revision(key.clone(), CompareOp::Equal, existing.mod_revision)
                },
            ),
        };
        let txn = Txn::new()
            .when([
                Compare::mod_revision(
                    membership_key.clone(),
                    CompareOp::Equal,
                    membership_mod_revision,
                ),
                Compare::lease(
                    membership_key,
                    CompareOp::Equal,
                    expected_membership_lease_id,
                ),
                report_compare,
            ])
            .and_then([TxnOp::put(key, encoded, Some(PutOptions::new()))]);
        let mut client = self.store.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "membership for node '{}' changed while publishing replica state; retry the update",
                report.node_id
            )));
        }
        Ok(true)
    }

    /// Read one node's authoritative shard replica report when it exists.
    pub async fn shard_replica_report(
        &self,
        collection: &CollectionRef,
        shard_id: &str,
        node_id: &str,
    ) -> Result<Option<ShardReplicaReport>> {
        Ok(self
            .store
            .get_replica_report_with_revision(collection, shard_id, node_id)
            .await?
            .map(|report| report.report))
    }

    /// Remove one node's authoritative shard replica report.
    pub async fn delete_shard_replica_report(
        &self,
        collection: &CollectionRef,
        shard_id: &str,
        node_id: &str,
        expected_membership_lease_id: i64,
    ) -> Result<()> {
        let Some((member, membership_mod_revision)) = self
            .store
            .ready_member_with_expected_lease_and_revision(node_id, expected_membership_lease_id)
            .await?
        else {
            return Err(LogPoseError::Message(format!(
                "node '{node_id}' is not currently registered in cluster membership"
            )));
        };
        if !matches!(member.node_role, NodeRole::Combined | NodeRole::Data) {
            return Err(LogPoseError::Message(format!(
                "node '{node_id}' is not a registered data-serving member"
            )));
        }
        let membership_key = self.store.membership_key(node_id);
        let key = self
            .store
            .shard_replica_report_key(collection, shard_id, node_id);
        let mut client = self.store.client().await?;
        let response = client
            .txn(
                Txn::new()
                    .when([
                        Compare::mod_revision(
                            membership_key.clone(),
                            CompareOp::Equal,
                            membership_mod_revision,
                        ),
                        Compare::lease(
                            membership_key,
                            CompareOp::Equal,
                            expected_membership_lease_id,
                        ),
                    ])
                    .and_then([TxnOp::delete(key, None)]),
            )
            .await
            .map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "membership for node '{node_id}' changed while clearing replica state; retry the update"
            )));
        }
        Ok(())
    }

    /// Promote or move shard ownership using revision-based CAS fencing.
    ///
    /// On a successful CAS, the returned [`ShardOwnership`] observes *our own
    /// write* strictly: the payload is the exact candidate we issued and
    /// `mod_revision` is taken from the committing transaction's header
    /// revision, which is the cluster revision at which the put applied.
    /// This avoids the race window a follow-up `get` would expose, where a
    /// later writer could advance the key's `mod_revision` between the CAS
    /// and the read-back.
    pub async fn promote_shard_owner(
        &self,
        current: &ShardOwnership,
        new_owner_node_id: &str,
        leader_fence: &LeadershipFence,
    ) -> Result<PromotionResult> {
        if current.owner_node_id == new_owner_node_id {
            return Ok(PromotionResult::Conflict);
        }
        let Some((member, membership_mod_revision)) = self
            .store
            .ready_member_with_revision(new_owner_node_id)
            .await?
        else {
            return Ok(PromotionResult::Conflict);
        };
        if member.state != "ready"
            || !matches!(member.node_role, NodeRole::Combined | NodeRole::Data)
        {
            return Ok(PromotionResult::Conflict);
        }
        let current_owner_membership = self
            .store
            .ready_member_with_revision(&current.owner_node_id)
            .await?;
        if current_owner_membership
            .as_ref()
            .is_some_and(|(member, _)| member.state == "ready")
        {
            return Ok(PromotionResult::Conflict);
        }
        let key = self.shard_owner_key(&current.collection, &current.shard_id);
        let descriptor_lookup_name = current.collection.lookup_name();
        let Some((stored_descriptor, descriptor_mod_revision)) = self
            .store
            .get_descriptor_with_revision(&descriptor_lookup_name)
            .await?
        else {
            return Ok(PromotionResult::Conflict);
        };
        if !stored_descriptor.ready {
            return Ok(PromotionResult::Conflict);
        }
        let Some(current_owner_report) = self
            .store
            .get_replica_report_with_revision(
                &current.collection,
                &current.shard_id,
                &current.owner_node_id,
            )
            .await?
        else {
            return Ok(PromotionResult::Conflict);
        };
        let Some(candidate_report) = self
            .store
            .get_replica_report_with_revision(
                &current.collection,
                &current.shard_id,
                new_owner_node_id,
            )
            .await?
        else {
            return Ok(PromotionResult::Conflict);
        };
        if !candidate_report.report.materialized
            || current_owner_report.report.ownership_epoch != Some(current.epoch)
            || candidate_report.report.ownership_epoch != Some(current.epoch)
            || candidate_report.report.membership_mod_revision != Some(membership_mod_revision)
            || current_owner_report.report.snapshot.is_none()
            || candidate_report.report.snapshot != current_owner_report.report.snapshot
        {
            return Ok(PromotionResult::Conflict);
        }
        let descriptor_key = self.store.descriptor_key(&descriptor_lookup_name);
        let membership_key = self.store.membership_key(new_owner_node_id);
        let current_owner_membership_key = self.store.membership_key(&current.owner_node_id);
        let current_owner_report_key = self.store.shard_replica_report_key(
            &current.collection,
            &current.shard_id,
            &current.owner_node_id,
        );
        let candidate_report_key = self.store.shard_replica_report_key(
            &current.collection,
            &current.shard_id,
            new_owner_node_id,
        );
        let mut candidate = ShardOwnership {
            collection: current.collection.clone(),
            shard_id: current.shard_id.clone(),
            owner_node_id: new_owner_node_id.to_owned(),
            epoch: current.epoch.saturating_add(1),
            mod_revision: 0,
        };
        let encoded = serde_json::to_string(&candidate).map_err(json_encode_message)?;
        let mut compares = self.store.leader_fence_compares(leader_fence).await?;
        compares.extend([
            Compare::mod_revision(key.clone(), CompareOp::Equal, current.mod_revision),
            Compare::mod_revision(descriptor_key, CompareOp::Equal, descriptor_mod_revision),
            Compare::mod_revision(membership_key, CompareOp::Equal, membership_mod_revision),
            Compare::mod_revision(
                current_owner_report_key,
                CompareOp::Equal,
                current_owner_report.mod_revision,
            ),
            Compare::mod_revision(
                candidate_report_key,
                CompareOp::Equal,
                candidate_report.mod_revision,
            ),
        ]);
        if let Some((_, current_owner_membership_mod_revision)) = current_owner_membership {
            compares.push(Compare::mod_revision(
                current_owner_membership_key,
                CompareOp::Equal,
                current_owner_membership_mod_revision,
            ));
        } else {
            compares.push(Compare::version(
                current_owner_membership_key,
                CompareOp::Equal,
                0,
            ));
        }
        let txn = Txn::new()
            .when(compares)
            .and_then([TxnOp::put(key.clone(), encoded, None)]);
        let mut client = self.store.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Ok(PromotionResult::Conflict);
        }
        let revision = response
            .header()
            .map(ResponseHeader::revision)
            .ok_or_else(|| {
                LogPoseError::Message(
                    "etcd txn response missing header revision after successful put".to_owned(),
                )
            })?;
        candidate.mod_revision = revision;
        Ok(PromotionResult::Applied(candidate))
    }

    /// Persist a new authoritative placement assignment under the active control-plane leader fence.
    pub async fn set_collection_assignment(
        &self,
        collection: &CollectionRef,
        assignment: CollectionAssignment,
        leader_fence: &LeadershipFence,
    ) -> Result<CollectionAssignment> {
        let assignment_key = self.store.assignment_key(&collection.lookup_name());
        let assignment_value = serde_json::to_string(&assignment).map_err(json_encode_message)?;
        let txn = Txn::new()
            .when(self.store.leader_fence_compares(leader_fence).await?)
            .and_then([TxnOp::put(
                assignment_key,
                assignment_value,
                Some(PutOptions::new()),
            )]);
        let mut client = self.store.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not the active control-plane leader",
                leader_fence.node_id
            )));
        }
        Ok(assignment)
    }

    /// Persist the last owner-transition reason under the active control-plane leader fence.
    pub async fn set_shard_failover_reason(
        &self,
        collection: &CollectionRef,
        shard_id: &str,
        reason: &str,
        leader_fence: &LeadershipFence,
    ) -> Result<()> {
        let failover_key = self.store.shard_failover_key(collection, shard_id);
        let failover_value = serde_json::to_string(&ShardFailoverReason {
            reason: reason.to_owned(),
        })
        .map_err(json_encode_message)?;
        let txn = Txn::new()
            .when(self.store.leader_fence_compares(leader_fence).await?)
            .and_then([TxnOp::put(
                failover_key,
                failover_value,
                Some(PutOptions::new()),
            )]);
        let mut client = self.store.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if !response.succeeded() {
            return Err(LogPoseError::Message(format!(
                "node '{}' is not the active control-plane leader",
                leader_fence.node_id
            )));
        }
        Ok(())
    }

    fn shard_owner_key(&self, collection: &CollectionRef, shard_id: &str) -> String {
        format!(
            "{}/clusters/{}/collections/{}/shards/{shard_id}/owner",
            self.store.key_prefix,
            self.config.cluster_name,
            collection.lookup_name()
        )
    }

    async fn attach_keep_alive_session(&self, client: &mut Client, lease_id: i64) -> Result<()> {
        let (keeper, stream) = client
            .lease_keep_alive(lease_id)
            .await
            .map_err(etcd_message)?;
        self.spawn_keep_alive_stream_drain(lease_id, stream);
        self.lease_sessions
            .lock()
            .await
            .insert(lease_id, LeaseSession { keeper });
        Ok(())
    }

    fn spawn_keep_alive_stream_drain(&self, lease_id: i64, mut stream: LeaseKeepAliveStream) {
        let sessions = Arc::clone(&self.lease_sessions);
        tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => {
                        let _ = sessions.lock().await.remove(&lease_id);
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_storage::CreateCollectionRequest;
    use logpose_types::DistanceMetric;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn assignment(node_id: &str) -> CollectionAssignment {
        CollectionAssignment {
            assigned_node: node_id.to_owned(),
            assigned_role: logpose_types::NodeRole::Data,
        }
    }

    fn etcd_config(endpoint: &str) -> EtcdMetadataConfig {
        EtcdMetadataConfig {
            endpoints: vec![endpoint.to_owned()],
            key_prefix: "/logpose/metadata".to_owned(),
            timeout_ms: 250,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
            cluster_name: "test-cluster".to_owned(),
        }
    }

    fn test_etcd_endpoint() -> String {
        std::env::var("LOGPOSE_TEST_ETCD_ENDPOINTS")
            .ok()
            .and_then(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .find(|endpoint| !endpoint.is_empty())
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| "http://127.0.0.1:2379".to_owned())
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("logpose-storage-etcd-{label}-{suffix}"));
        fs::create_dir_all(&path).expect("temp dir should be created");
        path
    }

    #[test]
    fn assignment_keys_include_cluster_and_namespace() {
        let store = EtcdPlacementStore::new(EtcdMetadataConfig {
            cluster_name: "prod-cluster".to_owned(),
            ..etcd_config("http://127.0.0.1:2379")
        })
        .expect("store should build");

        assert_eq!(
            store.assignment_key("analytics/documents"),
            "/logpose/metadata/clusters/prod-cluster/collections/analytics/documents/assignment"
        );
    }

    #[test]
    fn assignment_keys_do_not_collide_across_clusters() {
        let prod = EtcdPlacementStore::new(EtcdMetadataConfig {
            endpoints: vec!["http://127.0.0.1:2379".to_owned()],
            key_prefix: "/logpose/metadata".to_owned(),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
            cluster_name: "prod-cluster".to_owned(),
        })
        .expect("prod store should build");
        let staging = EtcdPlacementStore::new(EtcdMetadataConfig {
            endpoints: vec!["http://127.0.0.1:2379".to_owned()],
            key_prefix: "/logpose/metadata".to_owned(),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
            cluster_name: "staging-cluster".to_owned(),
        })
        .expect("staging store should build");

        assert_ne!(
            prod.assignment_key("analytics/documents"),
            staging.assignment_key("analytics/documents")
        );
    }

    #[test]
    fn descriptor_keys_include_cluster_and_namespace() {
        let store = EtcdPlacementStore::new(EtcdMetadataConfig {
            cluster_name: "prod-cluster".to_owned(),
            ..etcd_config("http://127.0.0.1:2379")
        })
        .expect("store should build");

        assert_eq!(
            store.descriptor_key("analytics/documents"),
            "/logpose/metadata/clusters/prod-cluster/collections/analytics/documents/descriptor"
        );
    }

    #[test]
    fn shard_owner_keys_include_cluster_and_namespace() {
        let client = EtcdCoordinationClient::new(EtcdMetadataConfig {
            cluster_name: "prod-cluster".to_owned(),
            ..etcd_config("http://127.0.0.1:2379")
        })
        .expect("coordination client should build");

        assert_eq!(
            client.shard_owner_key(&CollectionRef::new("analytics", "documents"), "0"),
            "/logpose/metadata/clusters/prod-cluster/collections/analytics/documents/shards/0/owner"
        );
    }

    #[test]
    fn json_decode_errors_are_labeled_as_decode_failures() {
        let error = serde_json::from_slice::<MembershipRecord>(b"{not-json")
            .map_err(json_decode_message)
            .expect_err("invalid payload should fail to decode");

        assert!(
            error
                .to_string()
                .contains("failed to decode metadata payload"),
            "error should mention decode, got: {error}"
        );
    }

    #[test]
    fn membership_record_round_trip_json() {
        let payload = MembershipRecord {
            node_id: "node-a".to_owned(),
            node_role: logpose_types::NodeRole::Combined,
            state: "ready".to_owned(),
            lease_id: 11,
            mod_revision: 0,
            rest_endpoint: Some("http://127.0.0.1:8080".to_owned()),
        };
        let encoded = serde_json::to_vec(&payload);
        assert!(encoded.is_ok(), "payload should encode");
        let decoded: serde_json::Result<MembershipRecord> =
            serde_json::from_slice(encoded.as_deref().unwrap_or_default());
        assert!(decoded.is_ok(), "payload should decode");
        if let Ok(decoded) = decoded {
            assert_eq!(decoded, payload);
        }
    }

    #[test]
    fn leadership_record_round_trip_json() {
        let payload = LeadershipRecord {
            node_id: "leader-a".to_owned(),
            lease_id: 42,
        };
        let encoded = serde_json::to_vec(&payload);
        assert!(encoded.is_ok(), "payload should encode");
        let decoded: serde_json::Result<LeadershipRecord> =
            serde_json::from_slice(encoded.as_deref().unwrap_or_default());
        assert!(decoded.is_ok(), "payload should decode");
        if let Ok(decoded) = decoded {
            assert_eq!(decoded, payload);
        }
    }

    #[test]
    fn shard_ownership_round_trip_json() {
        let payload = ShardOwnership {
            collection: CollectionRef::new("analytics", "documents"),
            shard_id: "shard-0".to_owned(),
            owner_node_id: "node-a".to_owned(),
            epoch: 4,
            mod_revision: 11,
        };
        let encoded = serde_json::to_vec(&payload);
        assert!(encoded.is_ok(), "payload should encode");
        let decoded: serde_json::Result<ShardOwnership> =
            serde_json::from_slice(encoded.as_deref().unwrap_or_default());
        assert!(decoded.is_ok(), "payload should decode");
        if let Ok(decoded) = decoded {
            assert_eq!(
                decoded,
                ShardOwnership {
                    mod_revision: 0,
                    ..payload
                }
            );
        }
    }

    #[test]
    fn shard_ownership_does_not_persist_mod_revision() {
        let payload = ShardOwnership {
            collection: CollectionRef::new("analytics", "documents"),
            shard_id: "shard-0".to_owned(),
            owner_node_id: "node-a".to_owned(),
            epoch: 4,
            mod_revision: 11,
        };

        let encoded = serde_json::to_value(&payload).expect("payload should encode");

        assert!(
            encoded.get("mod_revision").is_none(),
            "mod_revision should not be persisted in etcd payloads"
        );
        assert_eq!(
            encoded.get("database_name"),
            Some(&serde_json::json!("analytics"))
        );
        assert_eq!(
            encoded.get("collection_name"),
            Some(&serde_json::json!("documents"))
        );
    }

    #[test]
    fn stored_collection_descriptors_strip_runtime_paths_and_track_readiness() {
        let descriptor = CollectionDescriptor::new_in_database(
            "analytics",
            "documents",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/storage-etcd-tests"),
        );

        let pending = StoredCollectionDescriptor::pending(&descriptor);
        let ready = StoredCollectionDescriptor::ready(&descriptor);

        assert_eq!(pending.descriptor.root_path, PathBuf::new());
        assert_eq!(ready.descriptor.root_path, PathBuf::new());
        assert!(!pending.ready);
        assert!(ready.ready);
        assert!(pending.descriptor.matches_serving_identity(&descriptor));
        assert!(ready.descriptor.matches_serving_identity(&descriptor));
    }

    #[test]
    fn matching_assignment_without_local_state_requires_manual_reconciliation() {
        let requested = assignment("node-a");
        let error = LogPoseError::Message(
            "collection 'documents' already has metadata assignment in etcd".to_owned(),
        );

        assert!(matching_assignment_without_local_state(
            &error,
            false,
            Some(&requested),
            &requested,
        ));
        assert!(!matching_assignment_without_local_state(
            &error,
            true,
            Some(&requested),
            &requested,
        ));
    }

    #[test]
    fn matching_assignment_without_local_state_requires_matching_assignment_payload() {
        let requested = assignment("node-a");
        let different = assignment("node-b");
        let error = LogPoseError::Message(
            "collection 'documents' already has metadata assignment in etcd".to_owned(),
        );

        assert!(!matching_assignment_without_local_state(
            &error,
            false,
            Some(&different),
            &requested,
        ));
        assert!(!matching_assignment_without_local_state(
            &error, false, None, &requested,
        ));
    }

    #[test]
    fn matching_assignment_without_local_state_does_not_trigger_for_other_errors() {
        let requested = assignment("node-a");
        let error =
            LogPoseError::Message("etcd metadata operation failed: permission denied".to_owned());

        assert!(!matching_assignment_without_local_state(
            &error,
            false,
            Some(&requested),
            &requested,
        ));
    }

    #[test]
    fn stale_assignment_requires_manual_reconciliation_error_is_explicit() {
        let error = stale_assignment_requires_manual_reconciliation_error("analytics/documents");

        assert!(error.to_string().contains("manual reconciliation"));
        assert!(error.to_string().contains("analytics/documents"));
    }

    #[test]
    fn rollback_failure_error_includes_assignment_key_and_manual_reconciliation_hint() {
        let _store = EtcdPlacementStore::new(EtcdMetadataConfig {
            endpoints: vec!["http://127.0.0.1:2379".to_owned()],
            key_prefix: "/logpose/metadata".to_owned(),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
            cluster_name: "prod-cluster".to_owned(),
        })
        .expect("store should build");
        let error = rollback_failure_error(
            "analytics/documents",
            "local collection bootstrap failed",
            LogPoseError::Message("etcd metadata operation failed: permission denied".to_owned()),
        );

        assert!(
            error
                .to_string()
                .contains("local collection bootstrap failed"),
            "create failure should remain visible: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("etcd metadata operation failed: permission denied"),
            "rollback failure should remain visible: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("rollback of authoritative etcd metadata"),
            "rollback failure should mention authoritative metadata rollback: {error}"
        );
        assert!(
            error.to_string().contains("analytics/documents"),
            "rollback failure should preserve the collection identity: {error}"
        );
    }

    #[tokio::test]
    async fn etcd_backed_storage_rejects_plain_create_collection() {
        let root = unique_temp_dir("reject-plain-create");
        let engine = EtcdBackedStorageEngine::new(&root, etcd_config("http://127.0.0.1:2379"))
            .expect("engine should build");

        let error = engine
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect_err("plain create should be rejected");

        assert!(
            error
                .to_string()
                .contains("create_collection_with_assignment")
        );
    }

    #[tokio::test]
    async fn authoritative_assignment_reads_fail_closed_when_etcd_is_unreachable() {
        let root = unique_temp_dir("fail-closed-assignment");
        let local = LocalStorageEngine::new(&root);
        let descriptor = local
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("local collection should be created");
        let engine = EtcdBackedStorageEngine::new(&root, etcd_config("http://127.0.0.1:1"))
            .expect("engine should build");

        let error = engine
            .collection_assignment_descriptor(&descriptor)
            .await
            .expect_err("authoritative metadata lookup should fail closed");

        assert!(error.to_string().contains("etcd metadata operation failed"));
    }

    #[tokio::test]
    async fn collection_metadata_rollback_deletes_shard_replica_state() {
        let endpoint = test_etcd_endpoint();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let config = EtcdMetadataConfig {
            endpoints: vec![endpoint],
            key_prefix: format!("/logpose/metadata/rollback-cleanup-{suffix}"),
            cluster_name: format!("rollback-cleanup-{suffix}"),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
        };
        let store = EtcdPlacementStore::new(config.clone()).expect("store should build");
        let collection = CollectionRef::new_default("documents");
        let assignment_key = store.assignment_key(&collection.lookup_name());
        let descriptor_key = store.descriptor_key(&collection.lookup_name());
        let owner_key = store.shard_owner_key(&collection, "0");
        let replica_targets_key = store.shard_replica_targets_key(&collection, "0");
        let replica_report_key = store.shard_replica_report_key(&collection, "0", "node-b");
        let failover_key = store.shard_failover_key(&collection, "0");
        let shard_prefix = format!(
            "{}/clusters/{}/collections/{}/shards/0/",
            config.key_prefix,
            config.cluster_name,
            collection.lookup_name()
        );

        let mut client = Client::connect(config.endpoints.clone(), None)
            .await
            .expect("etcd client should connect");
        client
            .put(
                assignment_key.clone(),
                serde_json::to_string(&assignment("node-a")).expect("assignment should serialize"),
                None,
            )
            .await
            .expect("assignment metadata should be seeded");
        client
            .put(
                descriptor_key.clone(),
                serde_json::to_string(&StoredCollectionDescriptor::ready(
                    &CollectionDescriptor::new(
                        "documents",
                        2,
                        DistanceMetric::Dot,
                        unique_temp_dir("rollback-cleanup-root"),
                    ),
                ))
                .expect("descriptor should serialize"),
                None,
            )
            .await
            .expect("descriptor metadata should be seeded");
        client
            .put(
                owner_key.clone(),
                serde_json::to_string(&ShardOwnership {
                    collection: collection.clone(),
                    shard_id: "0".to_owned(),
                    owner_node_id: "node-a".to_owned(),
                    epoch: 1,
                    mod_revision: 0,
                })
                .expect("owner should serialize"),
                None,
            )
            .await
            .expect("owner metadata should be seeded");
        client
            .put(
                replica_targets_key,
                serde_json::to_string(&ShardReplicaTargetSet {
                    replicas: vec![ShardReplicaTarget {
                        node_id: "node-b".to_owned(),
                        node_role: NodeRole::Data,
                    }],
                })
                .expect("replica targets should serialize"),
                None,
            )
            .await
            .expect("replica targets should be seeded");
        client
            .put(
                replica_report_key,
                serde_json::to_string(&ShardReplicaReport {
                    node_id: "node-b".to_owned(),
                    node_role: NodeRole::Data,
                    materialized: true,
                    snapshot: Some(Snapshot {
                        manifest_generation: 3,
                        visible_seq_no: 9,
                    }),
                    ownership_epoch: Some(1),
                    membership_mod_revision: Some(7),
                    mod_revision: 0,
                })
                .expect("replica report should serialize"),
                None,
            )
            .await
            .expect("replica report should be seeded");
        client
            .put(
                failover_key,
                serde_json::to_string(&ShardFailoverReason {
                    reason: "seeded failover".to_owned(),
                })
                .expect("failover reason should serialize"),
                None,
            )
            .await
            .expect("failover reason should be seeded");

        let assignment_mod_revision = client
            .get(assignment_key.clone(), None)
            .await
            .expect("assignment metadata should be readable")
            .kvs()[0]
            .mod_revision();
        let descriptor_mod_revision = client
            .get(descriptor_key.clone(), None)
            .await
            .expect("descriptor metadata should be readable")
            .kvs()[0]
            .mod_revision();
        let owner_mod_revision = client
            .get(owner_key.clone(), None)
            .await
            .expect("owner metadata should be readable")
            .kvs()[0]
            .mod_revision();

        store
            .delete_collection_metadata_if_revision_matches(
                &collection.lookup_name(),
                CollectionMetadataRevision {
                    assignment_mod_revision,
                    descriptor_mod_revision,
                    owner_mod_revision,
                },
            )
            .await
            .expect("rollback cleanup should succeed");

        let shard_entries = client
            .get(shard_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .expect("shard metadata should be readable after cleanup");
        assert!(
            shard_entries.kvs().is_empty(),
            "rollback should remove shard replica targets, reports, and failover metadata too"
        );
        let snapshot = store
            .load_cluster_metadata()
            .await
            .expect("cluster metadata snapshot should load after cleanup");
        assert!(
            snapshot.collections.is_empty(),
            "rollback cleanup should leave no collection metadata behind"
        );
    }

    #[tokio::test]
    async fn publish_shard_replica_report_is_idempotent() {
        let endpoint = test_etcd_endpoint();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let config = EtcdMetadataConfig {
            endpoints: vec![endpoint],
            key_prefix: format!("/logpose/metadata/report-idempotency-{suffix}"),
            cluster_name: format!("report-idempotency-{suffix}"),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
        };
        let coordination =
            EtcdCoordinationClient::new(config).expect("coordination client should build");
        let membership = coordination
            .register_membership("node-a", NodeRole::Data)
            .await
            .expect("membership should register");
        let collection = CollectionRef::new_default("documents");
        let report = ShardReplicaReport {
            node_id: "node-a".to_owned(),
            node_role: NodeRole::Data,
            materialized: true,
            snapshot: Some(Snapshot {
                manifest_generation: 4,
                visible_seq_no: 12,
            }),
            ownership_epoch: Some(2),
            membership_mod_revision: None,
            mod_revision: 0,
        };

        let first = coordination
            .publish_shard_replica_report(&collection, "0", &report, membership.lease_id, None)
            .await
            .expect("initial report publish should succeed");
        let second = coordination
            .publish_shard_replica_report(&collection, "0", &report, membership.lease_id, None)
            .await
            .expect("replaying the same report should stay idempotent");

        assert!(first);
        assert!(!second);

        coordination
            .revoke_lease(membership.lease_id)
            .await
            .expect("membership lease should revoke during cleanup");
    }

    #[tokio::test]
    async fn stale_membership_lease_cannot_publish_replica_report() {
        let endpoint = test_etcd_endpoint();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let config = EtcdMetadataConfig {
            endpoints: vec![endpoint],
            key_prefix: format!("/logpose/metadata/report-fence-{suffix}"),
            cluster_name: format!("report-fence-{suffix}"),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
        };
        let stale = EtcdCoordinationClient::new(config.clone())
            .expect("stale coordination client should build");
        let fresh =
            EtcdCoordinationClient::new(config).expect("fresh coordination client should build");
        let stale_membership = stale
            .register_membership("node-a", NodeRole::Data)
            .await
            .expect("stale membership should register");
        let fresh_membership = fresh
            .register_membership("node-a", NodeRole::Data)
            .await
            .expect("fresh membership should replace the stale one");
        let collection = CollectionRef::new_default("documents");
        let report = ShardReplicaReport {
            node_id: "node-a".to_owned(),
            node_role: NodeRole::Data,
            materialized: true,
            snapshot: Some(Snapshot {
                manifest_generation: 4,
                visible_seq_no: 12,
            }),
            ownership_epoch: Some(2),
            membership_mod_revision: None,
            mod_revision: 0,
        };

        let stale_error = stale
            .publish_shard_replica_report(
                &collection,
                "0",
                &report,
                stale_membership.lease_id,
                None,
            )
            .await
            .expect_err("stale membership should not publish replica state");
        assert!(
            stale_error
                .to_string()
                .contains("not currently registered in cluster membership"),
            "unexpected error: {stale_error}"
        );

        let published = fresh
            .publish_shard_replica_report(
                &collection,
                "0",
                &report,
                fresh_membership.lease_id,
                None,
            )
            .await
            .expect("fresh membership should publish replica state");
        assert!(published);

        fresh
            .revoke_lease(fresh_membership.lease_id)
            .await
            .expect("fresh membership lease should revoke during cleanup");
    }

    #[tokio::test]
    async fn stale_membership_lease_cannot_acquire_leadership() {
        let endpoint = test_etcd_endpoint();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let config = EtcdMetadataConfig {
            endpoints: vec![endpoint],
            key_prefix: format!("/logpose/metadata/leadership-fence-{suffix}"),
            cluster_name: format!("leadership-fence-{suffix}"),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
        };
        let stale = EtcdCoordinationClient::new(config.clone())
            .expect("stale coordination client should build");
        let fresh =
            EtcdCoordinationClient::new(config).expect("fresh coordination client should build");
        let stale_membership = stale
            .register_membership("node-a", NodeRole::Combined)
            .await
            .expect("stale membership should register");
        let fresh_membership = fresh
            .register_membership("node-a", NodeRole::Combined)
            .await
            .expect("fresh membership should replace the stale one");

        let stale_leadership = stale
            .try_acquire_leadership("node-a", stale_membership.lease_id)
            .await
            .expect("stale leadership attempt should not error");
        assert!(
            stale_leadership.is_none(),
            "stale membership should not acquire leadership"
        );

        let fresh_leadership = fresh
            .try_acquire_leadership("node-a", fresh_membership.lease_id)
            .await
            .expect("fresh leadership attempt should succeed");
        assert!(
            fresh_leadership.is_some(),
            "fresh membership should acquire leadership"
        );

        if let Some(leadership) = fresh_leadership {
            fresh
                .revoke_lease(leadership.lease_id)
                .await
                .expect("leadership lease should revoke during cleanup");
        }
        fresh
            .revoke_lease(fresh_membership.lease_id)
            .await
            .expect("fresh membership lease should revoke during cleanup");
    }

    #[tokio::test]
    async fn stale_membership_lease_cannot_delete_replica_report() {
        let endpoint = test_etcd_endpoint();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let config = EtcdMetadataConfig {
            endpoints: vec![endpoint],
            key_prefix: format!("/logpose/metadata/report-delete-fence-{suffix}"),
            cluster_name: format!("report-delete-fence-{suffix}"),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 10,
        };
        let stale = EtcdCoordinationClient::new(config.clone())
            .expect("stale coordination client should build");
        let fresh =
            EtcdCoordinationClient::new(config).expect("fresh coordination client should build");
        let stale_membership = stale
            .register_membership("node-a", NodeRole::Data)
            .await
            .expect("stale membership should register");
        let collection = CollectionRef::new_default("documents");
        let report = ShardReplicaReport {
            node_id: "node-a".to_owned(),
            node_role: NodeRole::Data,
            materialized: true,
            snapshot: Some(Snapshot {
                manifest_generation: 4,
                visible_seq_no: 12,
            }),
            ownership_epoch: Some(2),
            membership_mod_revision: None,
            mod_revision: 0,
        };
        stale
            .publish_shard_replica_report(
                &collection,
                "0",
                &report,
                stale_membership.lease_id,
                None,
            )
            .await
            .expect("initial report publish should succeed");
        let fresh_membership = fresh
            .register_membership("node-a", NodeRole::Data)
            .await
            .expect("fresh membership should replace the stale one");

        let stale_error = stale
            .delete_shard_replica_report(&collection, "0", "node-a", stale_membership.lease_id)
            .await
            .expect_err("stale membership should not clear replica state");
        assert!(
            stale_error
                .to_string()
                .contains("node 'node-a' is not currently registered in cluster membership"),
            "unexpected error: {stale_error}"
        );

        let persisted = fresh
            .shard_replica_report(&collection, "0", "node-a")
            .await
            .expect("replica report lookup should succeed");
        assert!(
            persisted.is_some(),
            "stale delete must not remove the report"
        );

        fresh
            .delete_shard_replica_report(&collection, "0", "node-a", fresh_membership.lease_id)
            .await
            .expect("fresh membership should clear replica state");
        let cleared = fresh
            .shard_replica_report(&collection, "0", "node-a")
            .await
            .expect("replica report lookup should succeed after delete");
        assert!(cleared.is_none(), "fresh delete should remove the report");

        fresh
            .revoke_lease(fresh_membership.lease_id)
            .await
            .expect("fresh membership lease should revoke during cleanup");
    }
}
