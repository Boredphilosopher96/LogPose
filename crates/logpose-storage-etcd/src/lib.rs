//! Etcd-backed metadata overlay for collection placement assignments.

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, ResponseHeader, Txn, TxnOp,
};
use logpose_catalog::CollectionDescriptor;
use logpose_storage::{
    CreateCollectionRequest, InspectReport, InspectTarget, LocalStorageEngine, StorageEngine,
};
use logpose_types::{
    AnnCandidate, AnnSearchRequest, CollectionAssignment, CollectionStats, CommitAck,
    EtcdMetadataConfig, LogPoseError, MaintenanceStatus, RecordId, Result, Snapshot, VisibleRecord,
    WriteOperation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::Path, sync::Arc, time::Duration};

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

impl EtcdBackedStorageEngine {
    /// Construct the wrapper over a local storage root.
    pub fn new(root: impl AsRef<Path>, config: EtcdMetadataConfig) -> Result<Self> {
        let etcd = EtcdPlacementStore::new(config)?;
        Ok(Self {
            local: Arc::new(LocalStorageEngine::new(root)),
            etcd,
        })
    }
}

#[async_trait]
impl StorageEngine for EtcdBackedStorageEngine {
    async fn engine_name(&self) -> &'static str {
        "local+etcd-metadata"
    }

    async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        self.local.create_collection(request).await
    }

    async fn create_collection_with_assignment(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
    ) -> Result<CollectionDescriptor> {
        self.etcd
            .put_assignment_if_absent(&request.name, &assignment)
            .await?;
        match self
            .local
            .create_collection_with_assignment(request.clone(), assignment)
            .await
        {
            Ok(descriptor) => Ok(descriptor),
            Err(error) => {
                let _ = self.etcd.delete_assignment(&request.name).await;
                Err(error)
            }
        }
    }

    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor> {
        self.local.open_collection(name).await
    }

    async fn list_collections(&self) -> Result<Vec<CollectionDescriptor>> {
        self.local.list_collections().await
    }

    async fn collection_assignment_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        if let Some(assignment) = self.etcd.get_assignment(&descriptor.name).await? {
            return Ok(assignment);
        }
        self.local
            .collection_assignment_descriptor(descriptor)
            .await
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
}

impl EtcdPlacementStore {
    fn new(config: EtcdMetadataConfig) -> Result<Self> {
        if config.endpoints.is_empty() {
            return Err(LogPoseError::Message(
                "metadata.etcd.endpoints must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(None)),
            endpoints: config.endpoints,
            timeout_ms: config.timeout_ms,
            key_prefix: config.key_prefix.trim_end_matches('/').to_owned(),
        })
    }

    async fn client(&self) -> Result<tokio::sync::MutexGuard<'_, Option<Client>>> {
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
        Ok(guard)
    }

    fn assignment_key(&self, collection_name: &str) -> String {
        format!(
            "{}/collections/{collection_name}/assignment",
            self.key_prefix
        )
    }

    async fn put_assignment_if_absent(
        &self,
        collection_name: &str,
        assignment: &CollectionAssignment,
    ) -> Result<()> {
        let key = self.assignment_key(collection_name);
        let value = serde_json::to_string(assignment).map_err(json_message)?;
        let txn = Txn::new()
            .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key.clone(), value, Some(PutOptions::new()))]);
        let mut client = self.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let response = client_ref.txn(txn).await.map_err(etcd_message)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(LogPoseError::Message(format!(
                "collection '{collection_name}' already has metadata assignment in etcd"
            )))
        }
    }

    async fn get_assignment(&self, collection_name: &str) -> Result<Option<CollectionAssignment>> {
        let key = self.assignment_key(collection_name);
        let mut client = self.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let response = client_ref.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let assignment = serde_json::from_slice(kv.value()).map_err(json_message)?;
        Ok(Some(assignment))
    }

    async fn delete_assignment(&self, collection_name: &str) -> Result<()> {
        let mut client = self.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        client_ref
            .delete(
                self.assignment_key(collection_name),
                Some(DeleteOptions::new()),
            )
            .await
            .map_err(etcd_message)?;
        Ok(())
    }
}

fn json_message(error: serde_json::Error) -> LogPoseError {
    LogPoseError::Message(format!("failed to encode metadata payload: {error}"))
}

fn etcd_message(error: etcd_client::Error) -> LogPoseError {
    LogPoseError::Message(format!("etcd metadata operation failed: {error}"))
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
    /// Node state marker used by control loops.
    pub state: String,
}

/// Leadership payload persisted in etcd.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeadershipRecord {
    /// Current leader node identifier.
    pub node_id: String,
}

/// Shard ownership record used for epoch-based write fencing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardOwnership {
    /// Collection name owning the shard.
    pub collection_name: String,
    /// Shard identifier string.
    pub shard_id: String,
    /// Current owner node identifier.
    pub owner_node_id: String,
    /// Monotonic ownership epoch.
    pub epoch: u64,
    /// Etcd mod revision observed when the record was read.
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
}

impl EtcdCoordinationClient {
    /// Build a coordination helper from etcd metadata config.
    pub fn new(config: EtcdMetadataConfig) -> Result<Self> {
        let store = EtcdPlacementStore::new(config.clone())?;
        Ok(Self { store, config })
    }

    /// Register node membership with an etcd lease.
    pub async fn register_membership(&self, node_id: &str) -> Result<MembershipLease> {
        let membership_key = format!(
            "{}/clusters/{}/members/{node_id}",
            self.store.key_prefix, self.config.cluster_name
        );
        let payload = serde_json::json!({
            "node_id": node_id,
            "state": "ready",
        });
        let encoded = serde_json::to_string(&payload).map_err(json_message)?;
        let mut client = self.store.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let lease = client_ref
            .lease_grant(self.config.membership_ttl_secs, None)
            .await
            .map_err(etcd_message)?;
        let lease_id = lease.id();
        match client_ref
            .put(
                membership_key.clone(),
                encoded,
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await
        {
            Ok(_) => Ok(MembershipLease {
                node_id: node_id.to_owned(),
                lease_id,
                key: membership_key,
            }),
            Err(error) => {
                let _ = client_ref.lease_revoke(lease_id).await;
                Err(etcd_message(error))
            }
        }
    }

    /// Keep one lease alive by issuing a keep-alive signal.
    pub async fn keep_alive(&self, lease_id: i64) -> Result<()> {
        let mut client = self.store.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let (mut keeper, _stream) = client_ref
            .lease_keep_alive(lease_id)
            .await
            .map_err(etcd_message)?;
        keeper.keep_alive().await.map_err(etcd_message)?;
        Ok(())
    }

    /// Try to acquire controller leadership using lease-backed CAS.
    pub async fn try_acquire_leadership(&self, node_id: &str) -> Result<Option<LeadershipLease>> {
        let leadership_key = format!(
            "{}/clusters/{}/controllers/leader",
            self.store.key_prefix, self.config.cluster_name
        );
        let payload = serde_json::json!({
            "node_id": node_id,
        });
        let encoded = serde_json::to_string(&payload).map_err(json_message)?;
        let mut client = self.store.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let lease = client_ref
            .lease_grant(self.config.leadership_ttl_secs, None)
            .await
            .map_err(etcd_message)?;
        let lease_id = lease.id();
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
        let response = match client_ref.txn(txn).await {
            Ok(response) => response,
            Err(error) => {
                let _ = client_ref.lease_revoke(lease_id).await;
                return Err(etcd_message(error));
            }
        };
        if !response.succeeded() {
            let _ = client_ref.lease_revoke(lease_id).await;
            return Ok(None);
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
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let response = client_ref
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
            .map(|kv| serde_json::from_slice(kv.value()).map_err(json_message))
            .collect()
    }

    /// Return the current controller leader when one exists.
    pub async fn current_leader(&self) -> Result<Option<LeadershipRecord>> {
        let leadership_key = format!(
            "{}/clusters/{}/controllers/leader",
            self.store.key_prefix, self.config.cluster_name
        );
        let mut client = self.store.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let response = client_ref
            .get(leadership_key, None)
            .await
            .map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let record = serde_json::from_slice(kv.value()).map_err(json_message)?;
        Ok(Some(record))
    }

    /// Read the current owner for one shard with mod revision for CAS updates.
    pub async fn shard_owner(
        &self,
        collection_name: &str,
        shard_id: &str,
    ) -> Result<Option<ShardOwnership>> {
        let key = self.shard_owner_key(collection_name, shard_id);
        let mut client = self.store.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let response = client_ref.get(key, None).await.map_err(etcd_message)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let mut ownership: ShardOwnership =
            serde_json::from_slice(kv.value()).map_err(json_message)?;
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
        let key = self.shard_owner_key(&current.collection_name, &current.shard_id);
        let mut candidate = ShardOwnership {
            collection_name: current.collection_name.clone(),
            shard_id: current.shard_id.clone(),
            owner_node_id: new_owner_node_id.to_owned(),
            epoch: current.epoch.saturating_add(1),
            mod_revision: 0,
        };
        let encoded = serde_json::to_string(&candidate).map_err(json_message)?;
        let txn = Txn::new()
            .when([Compare::mod_revision(
                key.clone(),
                CompareOp::Equal,
                current.mod_revision,
            )])
            .and_then([TxnOp::put(key.clone(), encoded, None)]);
        let mut client = self.store.client().await?;
        let client_ref = client
            .as_mut()
            .ok_or_else(|| LogPoseError::Message("etcd client initialization failed".to_owned()))?;
        let response = client_ref.txn(txn).await.map_err(etcd_message)?;
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

    fn shard_owner_key(&self, collection_name: &str, shard_id: &str) -> String {
        format!(
            "{}/clusters/{}/collections/{collection_name}/shards/{shard_id}/owner",
            self.store.key_prefix, self.config.cluster_name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_record_round_trip_json() {
        let payload = MembershipRecord {
            node_id: "node-a".to_owned(),
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
            collection_name: "documents".to_owned(),
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
            assert_eq!(decoded, payload);
        }
    }
}
