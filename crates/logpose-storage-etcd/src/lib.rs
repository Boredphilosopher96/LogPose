//! Etcd-backed metadata overlay for collection placement assignments.

#[cfg(test)]
use anyhow as _;
use async_trait::async_trait;
#[cfg(test)]
use clap as _;
use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, LeaseKeepAliveStream, LeaseKeeper,
    PutOptions, ResponseHeader, Txn, TxnOp,
};
use logpose_auth::{DatabaseAccessPolicy, Principal};
use logpose_catalog::{CollectionDescriptor, DatabaseDescriptor};
use logpose_storage::{
    BoxFuture, CollectionReader, CreateCollectionRequest, InspectReport, InspectTarget,
    LocalStorageEngine, ReadOptions, ReadView, StorageEngine,
};
use logpose_types::{
    CollectionAssignment, CollectionRef, CollectionStats, CommitAck, CorruptionKind,
    DEFAULT_DATABASE_NAME, EtcdMetadataConfig, LeadershipFence, LogPoseError, MaintenanceStatus,
    ResourceKind, Result, Snapshot, WriteOperation, error::ROUTING_RETRY_AFTER,
};
// Only a dependency so Cargo downloads the vendored protoc; see Cargo.toml.
use protoc_bin_vendored as _;
use serde::{Deserialize, Serialize};

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
    /// Construct the wrapper over a local storage root, opening its engine.
    ///
    /// Fails if another engine holds the root; to share an open engine, use
    /// [`EtcdBackedStorageEngine::with_local`].
    pub fn new(root: impl AsRef<Path>, config: EtcdMetadataConfig) -> Result<Self> {
        Self::with_local(LocalStorageEngine::new(root)?, config)
    }

    /// Construct the wrapper over an already open local engine.
    pub fn with_local(local: LocalStorageEngine, config: EtcdMetadataConfig) -> Result<Self> {
        Ok(Self {
            local: Arc::new(local),
            etcd: EtcdPlacementStore::new(config)?,
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
            Err(LogPoseError::NotFound { .. }) => {}
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
            return Err(LogPoseError::NotLeader {
                node: leader_node_id.to_owned(),
                leader_node: None,
            });
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
            return Err(LogPoseError::not_found(
                ResourceKind::Database,
                database_name,
            ));
        };
        let descriptor: DatabaseDescriptor =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        descriptor
            .validate()
            .map_err(|error| stored_invalid(kv.key(), error))?;
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
                LogPoseError::corrupt(
                    CorruptionKind::Metadata,
                    format!("failed to decode metadata key as utf-8: {error}"),
                )
            })?;
            if !key.ends_with("/descriptor") {
                continue;
            }
            let descriptor: DatabaseDescriptor =
                serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
            descriptor
                .validate()
                .map_err(|error| stored_invalid(kv.key(), error))?;
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
            return Err(LogPoseError::not_found(
                ResourceKind::Principal,
                principal_name,
            ));
        };
        let principal: Principal =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        principal
            .validate()
            .map_err(|error| stored_invalid(kv.key(), error))?;
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
                LogPoseError::corrupt(
                    CorruptionKind::Metadata,
                    format!("failed to decode metadata key as utf-8: {error}"),
                )
            })?;
            if !key.ends_with("/descriptor") {
                continue;
            }
            let principal: Principal =
                serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
            principal
                .validate()
                .map_err(|error| stored_invalid(kv.key(), error))?;
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
            return Err(LogPoseError::NotLeader {
                node: leader_node_id.to_owned(),
                leader_node: None,
            });
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
            return Err(LogPoseError::not_found(
                ResourceKind::DatabasePolicy,
                database_name,
            ));
        };
        let policy: DatabaseAccessPolicy =
            serde_json::from_slice(kv.value()).map_err(json_decode_message)?;
        policy
            .validate()
            .map_err(|error| stored_invalid(kv.key(), error))?;
        Ok(policy)
    }
}

impl CollectionReader for EtcdBackedStorageEngine {
    fn read_view<'a>(
        &'a self,
        collection: &'a CollectionRef,
        options: ReadOptions,
    ) -> BoxFuture<'a, Result<ReadView>> {
        self.local.read_view(collection, options)
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
        Err(LogPoseError::internal(
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
            LogPoseError::internal(
                "etcd-backed collection creation requires a control-plane leadership fence",
            )
        })?;
        let collection_name = request.lookup_name();
        if self.local.open_collection(&collection_name).await.is_ok() {
            return Err(LogPoseError::already_exists(
                ResourceKind::Collection,
                collection_name,
            ));
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
            .create_collection_from_descriptor_async(descriptor.clone(), Some(assignment.clone()))
            .await
        {
            Ok(local_descriptor) => {
                self.etcd
                    .mark_collection_ready_if_revision_matches(
                        &collection_name,
                        &descriptor,
                        metadata_revision,
                        &leader_fence,
                    )
                    .await?;
                Ok(local_descriptor)
            }
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
            None => Err(LogPoseError::not_found(
                ResourceKind::Collection,
                canonical_collection_lookup_name(name),
            )),
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
            Err(LogPoseError::NotFound { .. }) => Ok(false),
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

    async fn collection_assignment_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        match self.etcd.get_assignment(&descriptor.lookup_name()).await {
            Ok(Some(assignment)) => Ok(assignment),
            Ok(None) => Err(LogPoseError::ReconciliationRequired {
                collection: descriptor.lookup_name(),
                message: format!(
                    "collection '{}' has no authoritative assignment metadata in etcd; reconciliation is required before serving it",
                    descriptor.lookup_name()
                ),
            }),
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
            .ok_or_else(|| LogPoseError::internal("etcd client initialization failed"))
    }

    fn collections_prefix(&self) -> String {
        format!(
            "{}/clusters/{}/collections/",
            self.key_prefix, self.cluster_name
        )
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

    fn leadership_key(&self) -> String {
        format!(
            "{}/clusters/{}/controllers/leader",
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
        let leadership_key = self.leadership_key();
        let leadership_value =
            self.leadership_value(&leader_fence.node_id, leader_fence.lease_id)?;
        let owner_value = serde_json::to_string(&ShardOwnership {
            collection,
            shard_id: "0".to_owned(),
            owner_node_id: assignment.assigned_node.clone(),
            epoch: 1,
            mod_revision: 0,
        })
        .map_err(json_encode_message)?;
        let txn = Txn::new()
            .when([
                Compare::value(leadership_key, CompareOp::Equal, leadership_value),
                Compare::version(assignment_key.clone(), CompareOp::Equal, 0),
                Compare::version(descriptor_key.clone(), CompareOp::Equal, 0),
                Compare::version(owner_key.clone(), CompareOp::Equal, 0),
            ])
            .and_then([
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
                    LogPoseError::internal(
                        "etcd txn response missing header revision after collection metadata write",
                    )
                })?;
            Ok(CollectionMetadataRevision {
                assignment_mod_revision: revision,
                descriptor_mod_revision: revision,
                owner_mod_revision: revision,
            })
        } else {
            Err(LogPoseError::already_exists(
                ResourceKind::CollectionAssignment,
                collection_name,
            ))
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
        let leadership_key = self.leadership_key();
        let leadership_value =
            self.leadership_value(&leader_fence.node_id, leader_fence.lease_id)?;
        let txn = Txn::new()
            .when([
                Compare::value(leadership_key, CompareOp::Equal, leadership_value),
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
            ])
            .and_then([TxnOp::put(
                descriptor_key,
                descriptor_value,
                Some(PutOptions::new()),
            )]);
        let mut client = self.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(LogPoseError::ReconciliationRequired {
                collection: collection_name.to_owned(),
                message: format!(
                    "authoritative etcd metadata for collection '{collection_name}' changed before local state could be finalized; manual reconciliation is required"
                ),
            })
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
                LogPoseError::corrupt(
                    CorruptionKind::Metadata,
                    format!("failed to decode metadata key as utf-8: {error}"),
                )
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
        let owner_key =
            self.shard_owner_key(&collection_ref_from_lookup_name(collection_name), "0");
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
            ]);
        let mut client = self.client().await?;
        let response = client.txn(txn).await.map_err(etcd_message)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(LogPoseError::ReconciliationRequired {
                collection: collection_name.to_owned(),
                message: format!(
                    "authoritative etcd metadata for collection '{collection_name}' changed before rollback could remove it; manual reconciliation is required"
                ),
            })
        }
    }
}

fn json_encode_message(error: serde_json::Error) -> LogPoseError {
    LogPoseError::internal(format!("failed to encode metadata payload: {error}"))
}

fn json_decode_message(error: serde_json::Error) -> LogPoseError {
    LogPoseError::corrupt(
        CorruptionKind::Metadata,
        format!("failed to decode metadata payload: {error}"),
    )
}

/// A record read back from `key` fails validation. Records are validated before they are
/// written, so this is damaged metadata, not a bad request.
fn stored_invalid(key: &[u8], error: impl std::fmt::Display) -> LogPoseError {
    let key = String::from_utf8_lossy(key).into_owned();
    LogPoseError::Corrupt {
        kind: CorruptionKind::Metadata,
        message: format!("stored metadata record '{key}' fails validation: {error}"),
        location: Some(key),
    }
}

/// The metadata store could not be reached or refused the operation; retrying may help.
fn etcd_message(error: etcd_client::Error) -> LogPoseError {
    LogPoseError::Unavailable {
        message: format!("etcd metadata operation failed: {error}"),
        retry_after: Some(ROUTING_RETRY_AFTER),
    }
}

fn string_message(error: String) -> LogPoseError {
    LogPoseError::invalid_argument(error)
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
    matches!(
        error,
        LogPoseError::AlreadyExists {
            resource: ResourceKind::CollectionAssignment,
            ..
        }
    )
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
    LogPoseError::ReconciliationRequired {
        collection: collection_name.to_owned(),
        message: format!(
            "collection '{collection_name}' has matching assignment metadata in etcd but no local collection state; manual reconciliation is required before recreating it"
        ),
    }
}

fn pending_descriptor_requires_manual_reconciliation_error(collection_name: &str) -> LogPoseError {
    LogPoseError::ReconciliationRequired {
        collection: collection_name.to_owned(),
        message: format!(
            "collection '{collection_name}' has authoritative metadata in etcd but local state finalization is still pending; manual reconciliation is required before serving it"
        ),
    }
}

fn rollback_failure_error(
    collection_name: &str,
    create_error: &str,
    rollback_error: LogPoseError,
) -> LogPoseError {
    LogPoseError::ReconciliationRequired {
        collection: collection_name.to_owned(),
        message: format!(
            "{create_error}; rollback of authoritative etcd metadata for collection '{collection_name}' also failed: {rollback_error}; manual reconciliation is required"
        ),
    }
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

/// Result of a promotion or ownership move transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PromotionResult {
    /// Ownership update transaction committed.
    Applied(ShardOwnership),
    /// Ownership update conflicted with a newer revision.
    Conflict,
}

/// Distributed coordination helper over etcd leases and CAS primitives.
#[derive(Clone)]
pub struct EtcdCoordinationClient {
    store: EtcdPlacementStore,
    config: EtcdMetadataConfig,
    lease_sessions: Arc<tokio::sync::Mutex<BTreeMap<i64, SharedLeaseSession>>>,
}

/// Outcome of one lease keep-alive round trip.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseKeepAlive {
    /// Etcd refreshed the lease.
    Alive {
        /// Remaining time-to-live etcd reported after the refresh.
        ttl_secs: i64,
    },
    /// Etcd no longer knows the lease: it expired or was revoked, and every
    /// key attached to it is gone. The caller must acquire a new lease.
    Expired,
}

type SharedLeaseSession = Arc<tokio::sync::Mutex<LeaseSession>>;

#[derive(Debug)]
struct LeaseSession {
    keeper: LeaseKeeper,
    stream: LeaseKeepAliveStream,
}

impl LeaseSession {
    /// Send one keep-alive request and wait for its response on the stream.
    async fn round_trip(&mut self) -> Result<LeaseKeepAlive> {
        self.keeper.keep_alive().await.map_err(etcd_message)?;
        match self.stream.message().await {
            Ok(Some(response)) if response.ttl() > 0 => Ok(LeaseKeepAlive::Alive {
                ttl_secs: response.ttl(),
            }),
            Ok(Some(_)) => Ok(LeaseKeepAlive::Expired),
            Ok(None) => Err(LogPoseError::unavailable(format!(
                "etcd keep-alive stream for lease '{}' closed",
                self.keeper.id()
            ))),
            Err(error) if is_lease_not_found(&error) => Ok(LeaseKeepAlive::Expired),
            Err(error) => Err(etcd_message(error)),
        }
    }
}

/// Whether etcd rejected a lease operation because the lease no longer exists.
fn is_lease_not_found(error: &etcd_client::Error) -> bool {
    match error {
        // Raised by the client when opening a keep-alive stream returns TTL 0.
        etcd_client::Error::LeaseKeepAliveError(message) => message.contains("lease not found"),
        // `rpctypes.ErrGRPCLeaseNotFound` on the server side.
        etcd_client::Error::GRpcStatus(status) => {
            status.message().contains("requested lease not found")
        }
        _ => false,
    }
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

    /// Register node membership with an etcd lease.
    pub async fn register_membership(
        &self,
        node_id: &str,
        node_role: logpose_types::NodeRole,
    ) -> Result<MembershipLease> {
        let membership_key = format!(
            "{}/clusters/{}/members/{node_id}",
            self.store.key_prefix, self.config.cluster_name
        );
        let payload = serde_json::json!({
            "node_id": node_id,
            "node_role": node_role,
            "state": "ready",
        });
        let encoded = serde_json::to_string(&payload).map_err(json_encode_message)?;
        let mut client = self.store.client().await?;
        let lease = client
            .lease_grant(self.config.membership_ttl_secs, None)
            .await
            .map_err(etcd_message)?;
        let lease_id = lease.id();
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
                    return Err(error.into());
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

    /// Refresh one lease and report whether etcd still holds it.
    ///
    /// Waits for etcd's keep-alive response, so a lease that expired or was
    /// revoked is reported as [`LeaseKeepAlive::Expired`] instead of being
    /// silently kept. A transport failure is an error and leaves the lease
    /// state unknown; the next call reopens the keep-alive stream.
    pub async fn keep_alive(&self, lease_id: i64) -> Result<LeaseKeepAlive> {
        let existing = self.lease_sessions.lock().await.get(&lease_id).cloned();
        let session = match existing {
            Some(session) => session,
            None => {
                let mut client = self.store.client().await?;
                match self.attach_keep_alive_session(&mut client, lease_id).await {
                    Ok(session) => session,
                    Err(KeepAliveAttachError::Expired) => return Ok(LeaseKeepAlive::Expired),
                    Err(KeepAliveAttachError::Failed(error)) => return Err(error),
                }
            }
        };
        let round_trip = {
            let mut guard = session.lock().await;
            tokio::time::timeout(
                Duration::from_millis(self.store.timeout_ms.max(1)),
                guard.round_trip(),
            )
            .await
        };
        let outcome = round_trip.unwrap_or_else(|_| {
            Err(LogPoseError::unavailable(format!(
                "etcd keep-alive for lease '{lease_id}' timed out after {}ms",
                self.store.timeout_ms
            )))
        });
        if !matches!(outcome, Ok(LeaseKeepAlive::Alive { .. })) {
            // A dead lease never recovers, and a failed or timed-out stream may
            // still deliver a late response, so drop the session either way.
            self.forget_session(lease_id, &session).await;
        }
        outcome
    }

    /// Revoke one lease and remove any local keep-alive session.
    pub async fn revoke_lease(&self, lease_id: i64) -> Result<()> {
        self.lease_sessions.lock().await.remove(&lease_id);
        let mut client = self.store.client().await?;
        client.lease_revoke(lease_id).await.map_err(etcd_message)?;
        Ok(())
    }

    /// Try to acquire controller leadership using lease-backed CAS.
    pub async fn try_acquire_leadership(&self, node_id: &str) -> Result<Option<LeadershipLease>> {
        let leadership_key = format!(
            "{}/clusters/{}/controllers/leader",
            self.store.key_prefix, self.config.cluster_name
        );
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
            .when([Compare::version(
                leadership_key.clone(),
                CompareOp::Equal,
                0,
            )])
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
            return Err(error.into());
        }
        Ok(Some(LeadershipLease {
            node_id: node_id.to_owned(),
            lease_id,
            key: leadership_key,
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
            .map(|kv| serde_json::from_slice(kv.value()).map_err(json_decode_message))
            .collect()
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
    ) -> Result<PromotionResult> {
        let members = self.list_membership().await?;
        if !members.iter().any(|member| {
            member.node_id == new_owner_node_id
                && member.state == "ready"
                && matches!(
                    member.node_role,
                    logpose_types::NodeRole::Combined | logpose_types::NodeRole::Data
                )
        }) {
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
        let descriptor_key = self.store.descriptor_key(&descriptor_lookup_name);
        let mut candidate = ShardOwnership {
            collection: current.collection.clone(),
            shard_id: current.shard_id.clone(),
            owner_node_id: new_owner_node_id.to_owned(),
            epoch: current.epoch.saturating_add(1),
            mod_revision: 0,
        };
        let encoded = serde_json::to_string(&candidate).map_err(json_encode_message)?;
        let txn = Txn::new()
            .when([
                Compare::mod_revision(key.clone(), CompareOp::Equal, current.mod_revision),
                Compare::mod_revision(descriptor_key, CompareOp::Equal, descriptor_mod_revision),
            ])
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
                LogPoseError::internal(
                    "etcd txn response missing header revision after successful put",
                )
            })?;
        candidate.mod_revision = revision;
        Ok(PromotionResult::Applied(candidate))
    }

    fn shard_owner_key(&self, collection: &CollectionRef, shard_id: &str) -> String {
        format!(
            "{}/clusters/{}/collections/{}/shards/{shard_id}/owner",
            self.store.key_prefix,
            self.config.cluster_name,
            collection.lookup_name()
        )
    }

    async fn attach_keep_alive_session(
        &self,
        client: &mut Client,
        lease_id: i64,
    ) -> std::result::Result<SharedLeaseSession, KeepAliveAttachError> {
        // Opening the stream waits for etcd's first keep-alive response, which
        // the per-request gRPC timeout does not cover once headers arrive.
        let opened = tokio::time::timeout(
            Duration::from_millis(self.store.timeout_ms.max(1)),
            client.lease_keep_alive(lease_id),
        )
        .await
        .map_err(|_| {
            KeepAliveAttachError::Failed(LogPoseError::unavailable(format!(
                "etcd keep-alive stream for lease '{lease_id}' did not open within {}ms",
                self.store.timeout_ms
            )))
        })?;
        let (keeper, stream) = opened.map_err(|error| {
            if is_lease_not_found(&error) {
                KeepAliveAttachError::Expired
            } else {
                KeepAliveAttachError::Failed(etcd_message(error))
            }
        })?;
        let session = Arc::new(tokio::sync::Mutex::new(LeaseSession { keeper, stream }));
        self.lease_sessions
            .lock()
            .await
            .insert(lease_id, Arc::clone(&session));
        Ok(session)
    }

    async fn forget_session(&self, lease_id: i64, session: &SharedLeaseSession) {
        let mut sessions = self.lease_sessions.lock().await;
        if sessions
            .get(&lease_id)
            .is_some_and(|current| Arc::ptr_eq(current, session))
        {
            sessions.remove(&lease_id);
        }
    }
}

enum KeepAliveAttachError {
    Expired,
    Failed(LogPoseError),
}

impl From<KeepAliveAttachError> for LogPoseError {
    fn from(error: KeepAliveAttachError) -> Self {
        match error {
            KeepAliveAttachError::Expired => {
                LogPoseError::unavailable("etcd lease expired before keep-alive started")
            }
            KeepAliveAttachError::Failed(error) => error,
        }
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
        let error = LogPoseError::already_exists(ResourceKind::CollectionAssignment, "documents");

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
        let error = LogPoseError::already_exists(ResourceKind::CollectionAssignment, "documents");

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
        let error = LogPoseError::unavailable("etcd metadata operation failed: permission denied");

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
            LogPoseError::unavailable("etcd metadata operation failed: permission denied"),
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
        let local = LocalStorageEngine::new(&root).expect("storage engine should open");
        let descriptor = local
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("local collection should be created");
        let engine =
            EtcdBackedStorageEngine::with_local(local.clone(), etcd_config("http://127.0.0.1:1"))
                .expect("engine should build");

        let error = engine
            .collection_assignment_descriptor(&descriptor)
            .await
            .expect_err("authoritative metadata lookup should fail closed");

        assert!(error.to_string().contains("etcd metadata operation failed"));
    }
}
