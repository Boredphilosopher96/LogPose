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
use logpose_auth as _;
#[cfg(test)]
use logpose_config as _;
#[cfg(test)]
use logpose_core as _;
#[cfg(test)]
use rand as _;
#[cfg(test)]
use serde as _;
#[cfg(test)]
use serde_json as _;
#[cfg(test)]
use tokio as _;
#[cfg(test)]
use tonic as _;
#[cfg(test)]
use tower as _;

use logpose_auth::{DatabaseAccessPolicy, Principal};
use logpose_catalog::{CatalogStore, DatabaseDescriptor};
use logpose_config::LogPoseConfig;
use logpose_query::{QueryError, QueryRequest, QueryResponse, query_exact};
use logpose_storage::{
    CreateCollectionRequest, InspectReport, InspectTarget, LocalStorageEngine, StorageEngine,
};
use logpose_storage_etcd::{
    ClusterCollectionMetadata, ClusterMetadataSnapshot, EtcdCoordinationClient, LeadershipLease,
    LeadershipRecord, MembershipRecord, PromotionResult, ShardOwnership,
};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, BuildInfo, CollectionAssignment, CollectionPlacement, CollectionRef,
    CollectionStats, CommitAck, CoordinationStatus, LeadershipFence, LogPoseError,
    MaintenanceBacklog, MaintenanceStatus, MetadataBackend, NodeMembershipStatus, NodeRole,
    NodeRuntimeStatus, ReplicaPlacement, Snapshot, WriteOperation,
};
use std::{
    collections::BTreeMap,
    fmt,
    net::IpAddr,
    path::Path,
    sync::{
        Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicBool, Ordering},
    },
};
use thiserror::Error;
use tokio::{
    runtime::Handle,
    time::{Duration, Instant, interval, sleep},
};

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
    /// The request is well-formed but cannot be satisfied by the current node state yet.
    #[error("{0}")]
    FailedPrecondition(String),
    /// The caller failed authentication.
    #[error("{0}")]
    Unauthenticated(String),
    /// The caller lacks permission for this operation.
    #[error("{0}")]
    PermissionDenied(String),
    /// The system failed while processing the request.
    #[error("{0}")]
    Internal(String),
}

#[derive(Clone)]
enum CoordinationRuntime {
    Local,
    Etcd(Arc<EtcdRuntime>),
}

#[derive(Debug)]
struct EtcdRuntime {
    snapshot: Arc<RwLock<CoordinationStatus>>,
    metadata: Arc<RwLock<MetadataRuntimeState>>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Default)]
struct MetadataRuntimeState {
    snapshot: ClusterMetadataSnapshot,
    last_error: Option<String>,
}

impl Drop for EtcdRuntime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl CoordinationRuntime {
    fn new(config: &LogPoseConfig, data: Arc<LogPoseDataService>) -> Self {
        if config.metadata.backend != MetadataBackend::Etcd {
            return Self::Local;
        }

        let snapshot = Arc::new(RwLock::new(CoordinationStatus {
            cluster_name: config.metadata.etcd.cluster_name.clone(),
            membership_registered: false,
            membership_state: None,
            membership_lease_id: None,
            registered_members: Vec::new(),
            leader_node: None,
            is_local_leader: false,
            leadership_lease_id: None,
            metadata_revision: None,
            watch_lag: None,
            last_error: None,
        }));
        let metadata = Arc::new(RwLock::new(MetadataRuntimeState::default()));
        let runtime = Arc::new(EtcdRuntime {
            snapshot: Arc::clone(&snapshot),
            metadata: Arc::clone(&metadata),
            shutdown: Arc::new(AtomicBool::new(false)),
        });
        let client = EtcdCoordinationClient::new(config.metadata.etcd.clone())
            .expect("invalid etcd coordination configuration");
        let node_name = config.node_name.clone();
        let node_role = config.node_role;
        let tick = coordination_tick(&config.metadata.etcd);
        let shutdown = Arc::clone(&runtime.shutdown);
        match Handle::try_current() {
            Ok(handle) => {
                let watch_client = client.clone();
                let watch_snapshot = Arc::clone(&snapshot);
                let watch_metadata = Arc::clone(&metadata);
                let watch_shutdown = Arc::clone(&shutdown);
                handle.spawn(async move {
                    run_metadata_watch_loop(
                        watch_client,
                        watch_snapshot,
                        watch_metadata,
                        watch_shutdown,
                    )
                    .await;
                });
                handle.spawn(async move {
                    run_coordination_loop(
                        client, data, snapshot, metadata, shutdown, node_name, node_role, tick,
                    )
                    .await;
                });
            }
            Err(error) => {
                coordination_write(&snapshot).last_error = Some(format!(
                    "etcd coordination loop did not start because no tokio runtime was available: {error}"
                ));
            }
        }
        Self::Etcd(runtime)
    }

    async fn snapshot(&self) -> Option<CoordinationStatus> {
        match self {
            Self::Local => None,
            Self::Etcd(runtime) => Some(coordination_read(&runtime.snapshot).clone()),
        }
    }

    async fn metadata_snapshot(&self) -> Option<ClusterMetadataSnapshot> {
        match self {
            Self::Local => None,
            Self::Etcd(runtime) => Some(metadata_read(&runtime.metadata).snapshot.clone()),
        }
    }
}

fn coordination_tick(config: &logpose_types::EtcdMetadataConfig) -> Duration {
    let ttl_secs = config
        .membership_ttl_secs
        .min(config.leadership_ttl_secs)
        .max(1) as u64;
    Duration::from_secs((ttl_secs / 3).max(1))
}

async fn run_coordination_loop(
    client: EtcdCoordinationClient,
    data: Arc<LogPoseDataService>,
    snapshot: Arc<RwLock<CoordinationStatus>>,
    metadata: Arc<RwLock<MetadataRuntimeState>>,
    shutdown: Arc<AtomicBool>,
    node_name: String,
    node_role: NodeRole,
    tick: Duration,
) {
    let mut membership_lease_id = None;
    let mut leadership_lease: Option<LeadershipLease> = None;
    let mut ticker = interval(tick);
    let mut first_iteration = true;
    loop {
        let mut pending_error = None;
        let mut force_refresh = false;
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        if first_iteration {
            first_iteration = false;
        } else {
            ticker.tick().await;
        }
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        if let Some(lease_id) = membership_lease_id
            && let Err(error) = client.keep_alive(lease_id).await
        {
            let error = error.to_string();
            if recover_tracked_membership_after_keep_alive_error(
                &error,
                &mut membership_lease_id,
                &mut leadership_lease,
            ) {
                force_refresh = true;
            }
            note_coordination_error(&mut pending_error, error);
        }

        if membership_lease_id.is_none() {
            match client.register_membership(&node_name, node_role).await {
                Ok(lease) => {
                    membership_lease_id = Some(lease.lease_id);
                    force_refresh = true;
                    clear_coordination_error(&snapshot).await;
                }
                Err(error) => {
                    record_coordination_error(&snapshot, error.to_string()).await;
                    continue;
                }
            }
        }

        if let Some(lease) = &leadership_lease
            && let Err(error) = client.keep_alive(lease.lease_id).await
        {
            let error = error.to_string();
            if recover_tracked_leadership_after_keep_alive_error(&error, &mut leadership_lease) {
                force_refresh = true;
            }
            note_coordination_error(&mut pending_error, error);
        }

        let cached_membership_ready = metadata_read(&metadata)
            .snapshot
            .members
            .iter()
            .find(|member| member.node_id == node_name)
            .is_none_or(|member| member.state == "ready");
        if leadership_lease.is_none()
            && cached_membership_ready
            && matches!(node_role, NodeRole::Combined | NodeRole::Control)
        {
            match client.try_acquire_leadership(&node_name).await {
                Ok(lease) => {
                    leadership_lease = lease;
                    force_refresh = true;
                    clear_coordination_error(&snapshot).await;
                }
                Err(error) => note_coordination_error(&mut pending_error, error.to_string()),
            }
        }

        if force_refresh {
            match client.load_cluster_metadata().await {
                Ok(cluster_snapshot) => {
                    let mut current = metadata_write(&metadata);
                    current.snapshot = cluster_snapshot;
                    current.last_error = None;
                }
                Err(error) => note_coordination_error(&mut pending_error, error.to_string()),
            }
        }

        let metadata_state = metadata_read(&metadata).clone();
        if let Some(error) = metadata_state.last_error {
            note_coordination_error(&mut pending_error, error);
        }
        if metadata_state.snapshot.revision == 0 {
            if let Some(error) = pending_error {
                record_coordination_error(&snapshot, error).await;
            }
            continue;
        }
        if let Some(leadership_lease) = leadership_lease.as_ref()
            && matches!(node_role, NodeRole::Combined | NodeRole::Data)
        {
            match reconcile_local_leader_failover(
                &client,
                &data,
                &node_name,
                leadership_lease,
                &metadata_state.snapshot,
            )
            .await
            {
                Ok(applied) => {
                    if applied {
                        match client.load_cluster_metadata().await {
                            Ok(cluster_snapshot) => {
                                let mut current = metadata_write(&metadata);
                                current.snapshot = cluster_snapshot;
                                current.last_error = None;
                            }
                            Err(error) => {
                                note_coordination_error(&mut pending_error, error.to_string())
                            }
                        }
                    }
                }
                Err(error) => note_coordination_error(&mut pending_error, error.to_string()),
            }
        }
        let members = Ok(metadata_state.snapshot.members);
        let leader = Ok(metadata_state.snapshot.leader);
        if let Ok(member_records) = &members
            && membership_lease_id.is_some()
            && !member_records
                .iter()
                .any(|member| member.node_id == node_name)
        {
            revoke_tracked_leadership_lease(&client, &mut leadership_lease).await;
            revoke_tracked_membership_lease(&client, &mut membership_lease_id).await;
        }
        if let Ok(member_records) = &members
            && leadership_lease.is_some()
            && member_records
                .iter()
                .find(|member| member.node_id == node_name)
                .is_some_and(|member| member.state != "ready")
        {
            revoke_tracked_leadership_lease(&client, &mut leadership_lease).await;
        }
        if let Ok(Some(leader_record)) = &leader
            && leadership_lease.is_some()
            && (leader_record.node_id != node_name
                || Some(leader_record.lease_id)
                    != leadership_lease.as_ref().map(|lease| lease.lease_id))
        {
            revoke_tracked_leadership_lease(&client, &mut leadership_lease).await;
        }
        reconcile_coordination_snapshot(
            &snapshot,
            &node_name,
            membership_lease_id,
            leadership_lease.as_ref(),
            &members,
            &leader,
            CoordinationSnapshotUpdate {
                metadata_revision: Some(metadata_state.snapshot.revision),
                watch_lag: Some(0),
                pending_error,
            },
        );
    }

    if let Some(lease) = leadership_lease.take() {
        let _ = client.revoke_lease(lease.lease_id).await;
    }
    if let Some(lease_id) = membership_lease_id.take() {
        let _ = client.revoke_lease(lease_id).await;
    }
}

async fn revoke_tracked_leadership_lease(
    client: &EtcdCoordinationClient,
    leadership_lease: &mut Option<LeadershipLease>,
) {
    if let Some(lease) = leadership_lease.take() {
        let _ = client.revoke_lease(lease.lease_id).await;
    }
}

async fn revoke_tracked_membership_lease(
    client: &EtcdCoordinationClient,
    membership_lease_id: &mut Option<i64>,
) {
    if let Some(lease_id) = membership_lease_id.take() {
        let _ = client.revoke_lease(lease_id).await;
    }
}

async fn reconcile_local_leader_failover(
    client: &EtcdCoordinationClient,
    data: &LogPoseDataService,
    node_name: &str,
    leadership_lease: &LeadershipLease,
    snapshot: &ClusterMetadataSnapshot,
) -> Result<bool> {
    for collection in &snapshot.collections {
        let Some(descriptor) = collection.descriptor.as_ref() else {
            continue;
        };
        if !collection.descriptor_ready || descriptor.replication_factor <= 1 {
            continue;
        }
        let Some(ownership) = collection.owner.as_ref() else {
            continue;
        };
        if ownership.owner_node_id == node_name {
            continue;
        }
        let owner_ready = snapshot
            .members
            .iter()
            .find(|member| member.node_id == ownership.owner_node_id)
            .is_some_and(|member| member.state == "ready");
        if owner_ready {
            continue;
        }
        let mut replica_candidates = snapshot
            .members
            .iter()
            .filter(|member| {
                member.node_id != ownership.owner_node_id
                    && matches!(member.node_role, NodeRole::Combined | NodeRole::Data)
                    && member.state == "ready"
            })
            .collect::<Vec<_>>();
        replica_candidates.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let local_is_desired_replica = replica_candidates
            .into_iter()
            .take(descriptor.replication_factor.saturating_sub(1))
            .any(|member| member.node_id == node_name);
        if !local_is_desired_replica {
            continue;
        }
        if !data.local_collection_matches_descriptor(descriptor).await? {
            continue;
        }

        let promotion = client
            .promote_shard_owner(ownership, node_name)
            .await
            .map_err(ServiceError::from)?;
        let PromotionResult::Applied(_) = promotion else {
            continue;
        };

        client
            .set_collection_assignment(
                &collection.collection,
                CollectionAssignment {
                    assigned_node: node_name.to_owned(),
                    assigned_role: NodeRole::Data,
                },
                &LeadershipFence {
                    node_id: node_name.to_owned(),
                    lease_id: leadership_lease.lease_id,
                },
            )
            .await
            .map_err(ServiceError::from)?;
        client
            .set_shard_failover_reason(
                &collection.collection,
                &ownership.shard_id,
                &format!(
                    "automatic self-promotion on node '{node_name}' after owner '{}' lost readiness",
                    ownership.owner_node_id
                ),
                &LeadershipFence {
                    node_id: node_name.to_owned(),
                    lease_id: leadership_lease.lease_id,
                },
            )
            .await
            .map_err(ServiceError::from)?;
        return Ok(true);
    }
    Ok(false)
}

fn recover_tracked_membership_after_keep_alive_error(
    error: &str,
    membership_lease_id: &mut Option<i64>,
    leadership_lease: &mut Option<LeadershipLease>,
) -> bool {
    if !keep_alive_session_missing(error) {
        return false;
    }
    *membership_lease_id = None;
    *leadership_lease = None;
    true
}

fn recover_tracked_leadership_after_keep_alive_error(
    error: &str,
    leadership_lease: &mut Option<LeadershipLease>,
) -> bool {
    if !keep_alive_session_missing(error) {
        return false;
    }
    *leadership_lease = None;
    true
}

fn keep_alive_session_missing(error: &str) -> bool {
    error.contains("no keep-alive session is registered for lease '")
}

fn note_coordination_error(pending_error: &mut Option<String>, error: String) {
    if pending_error.is_none() {
        *pending_error = Some(error);
    }
}

struct CoordinationSnapshotUpdate {
    metadata_revision: Option<i64>,
    watch_lag: Option<u64>,
    pending_error: Option<String>,
}

fn reconcile_coordination_snapshot(
    snapshot: &RwLock<CoordinationStatus>,
    node_name: &str,
    membership_lease_id: Option<i64>,
    leadership_lease: Option<&LeadershipLease>,
    members: &logpose_types::Result<Vec<MembershipRecord>>,
    leader: &logpose_types::Result<Option<LeadershipRecord>>,
    update: CoordinationSnapshotUpdate,
) {
    let local_membership_state = members.as_ref().ok().and_then(|member_records| {
        member_records
            .iter()
            .find(|member| member.node_id == node_name)
            .map(|member| member.state.clone())
    });
    let membership_confirmed = members.as_ref().is_ok_and(|member_records| {
        membership_lease_id.is_some()
            && member_records
                .iter()
                .any(|member| member.node_id == node_name)
    });
    let visible_leader = leader
        .as_ref()
        .ok()
        .and_then(|leader_record| leader_record.as_ref().cloned());
    let mut current = coordination_write(snapshot);
    current.membership_registered = membership_confirmed;
    current.membership_state = local_membership_state;
    current.membership_lease_id = membership_lease_id.filter(|_| membership_confirmed);
    if let Ok(member_records) = members {
        current.registered_members = member_records
            .iter()
            .map(|member| member.node_id.clone())
            .collect();
        current.registered_members.sort();
    } else {
        current.registered_members.clear();
    }
    current.leader_node = visible_leader.as_ref().map(|record| record.node_id.clone());
    current.is_local_leader = membership_confirmed
        && leadership_lease.is_some_and(|lease| {
            visible_leader.as_ref().is_some_and(|record| {
                record.node_id == node_name && record.lease_id == lease.lease_id
            })
        });
    current.leadership_lease_id = leadership_lease
        .map(|lease| lease.lease_id)
        .filter(|_| current.is_local_leader);
    current.metadata_revision = update.metadata_revision;
    current.watch_lag = update.watch_lag;
    current.last_error = reconcile_coordination_last_error(update.pending_error, members, leader);
}

fn reconcile_coordination_last_error(
    pending_error: Option<String>,
    members: &logpose_types::Result<Vec<MembershipRecord>>,
    leader: &logpose_types::Result<Option<LeadershipRecord>>,
) -> Option<String> {
    match (pending_error, members, leader) {
        (Some(pending_error), Ok(_), Ok(_)) => Some(pending_error),
        (Some(pending_error), Err(members_error), Err(leader_error)) => {
            Some(format!("{pending_error}; {members_error}; {leader_error}"))
        }
        (Some(pending_error), Err(error), Ok(_)) | (Some(pending_error), Ok(_), Err(error)) => {
            Some(format!("{pending_error}; {error}"))
        }
        (None, Err(members_error), Err(leader_error)) => {
            Some(format!("{members_error}; {leader_error}"))
        }
        (None, Err(error), Ok(_)) | (None, Ok(_), Err(error)) => Some(error.to_string()),
        (None, Ok(_), Ok(_)) => None,
    }
}

async fn record_coordination_error(snapshot: &RwLock<CoordinationStatus>, error: String) {
    let mut current = coordination_write(snapshot);
    current.last_error = Some(error);
    current.membership_registered = false;
    current.membership_state = None;
    current.membership_lease_id = None;
    current.registered_members.clear();
    current.leader_node = None;
    current.is_local_leader = false;
    current.leadership_lease_id = None;
    current.metadata_revision = None;
    current.watch_lag = None;
}

async fn clear_coordination_error(snapshot: &RwLock<CoordinationStatus>) {
    coordination_write(snapshot).last_error = None;
}

fn coordination_read(
    snapshot: &RwLock<CoordinationStatus>,
) -> RwLockReadGuard<'_, CoordinationStatus> {
    match snapshot.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn coordination_write(
    snapshot: &RwLock<CoordinationStatus>,
) -> RwLockWriteGuard<'_, CoordinationStatus> {
    match snapshot.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn metadata_read(
    snapshot: &RwLock<MetadataRuntimeState>,
) -> RwLockReadGuard<'_, MetadataRuntimeState> {
    match snapshot.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn metadata_write(
    snapshot: &RwLock<MetadataRuntimeState>,
) -> RwLockWriteGuard<'_, MetadataRuntimeState> {
    match snapshot.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn record_metadata_watch_error(snapshot: &RwLock<MetadataRuntimeState>, error: String) {
    let mut current = metadata_write(snapshot);
    current.snapshot = ClusterMetadataSnapshot::default();
    current.last_error = Some(error);
}

async fn run_metadata_watch_loop(
    client: EtcdCoordinationClient,
    coordination: Arc<RwLock<CoordinationStatus>>,
    metadata: Arc<RwLock<MetadataRuntimeState>>,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::SeqCst) {
        match client.load_cluster_metadata().await {
            Ok(snapshot) => {
                let mut current = metadata_write(&metadata);
                current.snapshot = snapshot;
                current.last_error = None;
            }
            Err(error) => {
                let error = error.to_string();
                record_metadata_watch_error(&metadata, error.clone());
                record_coordination_error(&coordination, error).await;
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        }

        let current_revision = metadata_read(&metadata).snapshot.revision;
        match client
            .wait_for_cluster_metadata_change(current_revision)
            .await
        {
            Ok(_) => {}
            Err(error) => {
                let error = error.to_string();
                record_metadata_watch_error(&metadata, error.clone());
                record_coordination_error(&coordination, error).await;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
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
        leader_fence: Option<LeadershipFence>,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        self.storage
            .create_collection_with_assignment(request, assignment, leader_fence)
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

    /// Return collection-level stats for an explicit read snapshot.
    pub async fn stats_at_snapshot(
        &self,
        collection_name: &str,
        snapshot: Snapshot,
    ) -> Result<CollectionStats> {
        self.stats_for_read(collection_name, Some(snapshot), None)
            .await
    }

    /// Return collection-level stats for one exact snapshot or lower-bound read barrier.
    pub async fn stats_for_read(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        read_barrier: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        let snapshot = self
            .resolve_read_snapshot(&descriptor.lookup_name(), snapshot, read_barrier)
            .await?;
        self.stats_descriptor(&descriptor, snapshot).await
    }

    /// Return collection-level stats using a previously loaded descriptor.
    pub async fn stats_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        self.storage
            .stats_descriptor(descriptor, snapshot)
            .await
            .map_err(ServiceError::from)
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

    async fn resolve_read_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        read_barrier: Option<Snapshot>,
    ) -> Result<Option<Snapshot>> {
        match (snapshot, read_barrier) {
            (Some(_), Some(_)) => Err(ServiceError::InvalidArgument(
                "snapshot and read_barrier cannot be provided together".to_owned(),
            )),
            (Some(snapshot), None) => Ok(Some(snapshot)),
            (None, None) => Ok(None),
            (None, Some(read_barrier)) => {
                let current = self.snapshot(collection_name).await?;
                if current.satisfies_read_barrier(&read_barrier) {
                    Ok(Some(current))
                } else {
                    Err(ServiceError::FailedPrecondition(format!(
                        "read barrier generation {}, seq {} is not yet visible; current snapshot is generation {}, seq {}",
                        read_barrier.manifest_generation,
                        read_barrier.visible_seq_no,
                        current.manifest_generation,
                        current.visible_seq_no
                    )))
                }
            }
        }
    }
}

/// Build a filesystem-backed catalog store rooted under the runtime storage directory.
#[must_use]
pub fn local_catalog_store(root: impl AsRef<Path>) -> Arc<dyn CatalogStore> {
    Arc::new(LocalStorageEngine::new(root))
}

/// Shared control-plane orchestration over local data-plane services.
#[derive(Clone)]
pub struct LogPoseControlService {
    data: Arc<LogPoseDataService>,
    catalog: Arc<dyn CatalogStore>,
    config: LogPoseConfig,
    build: BuildInfo,
    coordination: CoordinationRuntime,
    coordination_client: Option<EtcdCoordinationClient>,
}

impl fmt::Debug for LogPoseControlService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogPoseControlService")
            .field("data_service", &"<LogPoseDataService>")
            .field("catalog_store", &"<dyn CatalogStore>")
            .field("node_name", &self.config.node_name)
            .field("node_role", &self.config.node_role)
            .field(
                "coordination_backend",
                &match &self.coordination {
                    CoordinationRuntime::Local => "local",
                    CoordinationRuntime::Etcd(_) => "etcd",
                },
            )
            .finish()
    }
}

impl LogPoseControlService {
    /// Build a control-plane service over a shared data service and runtime config.
    #[must_use]
    pub fn new(
        data: Arc<LogPoseDataService>,
        catalog: Arc<dyn CatalogStore>,
        config: LogPoseConfig,
        build: BuildInfo,
    ) -> Self {
        let coordination = CoordinationRuntime::new(&config, Arc::clone(&data));
        let coordination_client = if config.metadata.backend == MetadataBackend::Etcd {
            Some(
                EtcdCoordinationClient::new(config.metadata.etcd.clone())
                    .expect("invalid etcd coordination configuration"),
            )
        } else {
            None
        };
        Self {
            data,
            catalog,
            config,
            build,
            coordination,
            coordination_client,
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
        let leader_fence = self.require_local_control_plane_leader().await?;
        let assignment = self.initial_assignment();
        if self.config.metadata.backend == MetadataBackend::Etcd
            && let Some(leader_fence) = leader_fence.as_ref()
            && assignment.assigned_node != leader_fence.node_id
        {
            return Err(ServiceError::InvalidArgument(format!(
                "etcd-backed collection creation requires the active control-plane leader '{}' to own the initial assignment; got '{}'",
                leader_fence.node_id, assignment.assigned_node
            )));
        }
        self.data
            .create_collection_with_assignment(request, assignment, leader_fence)
            .await
    }

    /// Create or replace one database-scoped access policy.
    pub async fn set_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        match self.config.node_role {
            NodeRole::Data => {
                return Err(ServiceError::InvalidArgument(
                    "data-only nodes cannot accept control-plane database policy mutations"
                        .to_owned(),
                ));
            }
            NodeRole::Control | NodeRole::Combined => {}
        }
        self.require_local_control_plane_leader().await?;
        self.catalog
            .put_database_access_policy(policy)
            .map_err(Into::into)
    }

    /// Create or replace one database descriptor.
    pub async fn put_database(&self, descriptor: DatabaseDescriptor) -> Result<DatabaseDescriptor> {
        match self.config.node_role {
            NodeRole::Data => {
                return Err(ServiceError::InvalidArgument(
                    "data-only nodes cannot accept control-plane database mutations".to_owned(),
                ));
            }
            NodeRole::Control | NodeRole::Combined => {}
        }
        self.require_local_control_plane_leader().await?;
        self.catalog.put_database(descriptor).map_err(Into::into)
    }

    /// Read one database descriptor.
    pub async fn database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        self.catalog.get_database(database_name).map_err(Into::into)
    }

    /// List every database descriptor.
    pub async fn databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        self.catalog.list_databases().map_err(Into::into)
    }

    /// Read one database-scoped access policy.
    pub async fn database_access_policy(
        &self,
        database_name: &str,
    ) -> Result<DatabaseAccessPolicy> {
        self.catalog
            .get_database_access_policy(database_name)
            .map_err(Into::into)
    }

    /// Read one persisted principal descriptor.
    pub async fn principal(&self, principal_name: &str) -> Result<Principal> {
        self.catalog
            .get_principal(principal_name)
            .map_err(Into::into)
    }

    /// Return the placement summary for one collection.
    pub async fn collection_placement(&self, collection_name: &str) -> Result<CollectionPlacement> {
        let descriptor = self.descriptor_for_collection(collection_name).await?;
        let assignment = self
            .authoritative_assignment_for_descriptor(&descriptor)
            .await?;
        let ownership = self
            .authoritative_ownership_for_descriptor(&descriptor)
            .await?;
        let local_collection_available = self
            .data
            .local_collection_matches_descriptor(&descriptor)
            .await?;
        let coordination = self.coordination.snapshot().await;
        let replicas = self
            .replica_targets_for_descriptor(
                &descriptor,
                &assignment,
                ownership.as_ref(),
                local_collection_available,
            )
            .await;
        let failover_reason = self.failover_reason_for_descriptor(&descriptor).await;
        Ok(self.local_placement(
            &descriptor,
            &assignment,
            ownership.as_ref(),
            replicas,
            failover_reason,
            local_collection_available,
            coordination.as_ref(),
        ))
    }

    /// Return aggregated runtime and maintenance status for the local node.
    pub async fn runtime_status(&self) -> Result<NodeRuntimeStatus> {
        let metadata_ready = self.data.metadata_status().await.is_ok();
        let coordination = self.coordination.snapshot().await;
        let descriptors = if metadata_ready {
            let local_descriptors = self.data.list_collections().await?;
            if let Some(cached_descriptors) = self.cached_ready_descriptors().await {
                let mut descriptors_by_lookup = BTreeMap::new();
                for descriptor in local_descriptors {
                    descriptors_by_lookup.insert(descriptor.lookup_name(), descriptor);
                }
                for descriptor in cached_descriptors {
                    descriptors_by_lookup.insert(descriptor.lookup_name(), descriptor);
                }
                descriptors_by_lookup.into_values().collect()
            } else {
                local_descriptors
            }
        } else {
            Vec::new()
        };
        let mut placements = Vec::with_capacity(descriptors.len());
        let mut local_descriptors = Vec::new();
        for descriptor in &descriptors {
            let assignment = self.status_assignment_for_descriptor(descriptor).await?;
            let ownership = self.status_ownership_for_descriptor(descriptor).await?;
            let local_collection_available = self
                .data
                .local_collection_matches_descriptor(descriptor)
                .await?;
            let replicas = self
                .replica_targets_for_descriptor(
                    descriptor,
                    &assignment,
                    ownership.as_ref(),
                    local_collection_available,
                )
                .await;
            let failover_reason = self.failover_reason_for_descriptor(descriptor).await;
            let placement = self.local_placement(
                descriptor,
                &assignment,
                ownership.as_ref(),
                replicas,
                failover_reason,
                local_collection_available,
                coordination.as_ref(),
            );
            if placement.route_kind == "local" {
                if descriptor.root_path.as_os_str().is_empty() {
                    local_descriptors.push(
                        self.data
                            .get_collection(&descriptor.lookup_name())
                            .await
                            .unwrap_or_else(|_| descriptor.clone()),
                    );
                } else {
                    local_descriptors.push(descriptor.clone());
                }
            }
            placements.push(placement);
        }
        placements.sort_by(|left, right| {
            (&left.database_name, &left.collection_name)
                .cmp(&(&right.database_name, &right.collection_name))
        });

        let mut maintenance = MaintenanceBacklog::default();
        for descriptor in local_descriptors {
            let status = self.data.maintenance_status_descriptor(&descriptor).await?;
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

        let control_coordination_ready = coordination.as_ref().is_none_or(|status| {
            coordination_membership_ready(status)
                && status.last_error.is_none()
                && (!matches!(
                    self.config.node_role,
                    NodeRole::Combined | NodeRole::Control
                ) || status.is_local_leader)
        });
        let data_coordination_ready = coordination.as_ref().is_none_or(|status| {
            coordination_membership_ready(status) && status.last_error.is_none()
        });

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
                )
                && control_coordination_ready,
            data_plane_ready: metadata_ready
                && matches!(self.config.node_role, NodeRole::Combined | NodeRole::Data)
                && data_coordination_ready,
            collection_count: placements
                .iter()
                .filter(|placement| placement.route_kind == "local")
                .count(),
            collections: placements,
            coordination,
            maintenance,
        })
    }

    /// Persist configured bootstrap principals into the catalog store.
    pub fn sync_bootstrap_principals(&self) -> Result<()> {
        for token in &self.config.auth.bootstrap_tokens {
            match self.catalog.get_principal(&token.principal.name) {
                Ok(_) => {}
                Err(error) if error.to_string().contains("does not exist") => {
                    self.catalog
                        .put_principal(token.principal.clone())
                        .map_err(ServiceError::from)?;
                }
                Err(error) => return Err(ServiceError::from(error)),
            }
        }
        Ok(())
    }

    /// Return the current distributed coordination status when one exists.
    pub async fn coordination_status(&self) -> Option<CoordinationStatus> {
        self.coordination.snapshot().await
    }

    /// Read one node membership status from authoritative metadata.
    pub async fn node_membership_status(&self, node_id: &str) -> Result<NodeMembershipStatus> {
        let Some(client) = &self.coordination_client else {
            return Err(ServiceError::InvalidArgument(
                "node membership operations require the etcd metadata backend".to_owned(),
            ));
        };
        let record = client.membership(node_id).await?.ok_or_else(|| {
            ServiceError::NotFound(format!(
                "membership record for node '{node_id}' does not exist"
            ))
        })?;
        Ok(NodeMembershipStatus {
            node_id: record.node_id,
            node_role: record.node_role,
            state: record.state,
        })
    }

    /// Mark one node as draining in authoritative metadata.
    pub async fn drain_node(&self, node_id: &str) -> Result<NodeMembershipStatus> {
        self.set_node_membership_state(node_id, "draining").await
    }

    /// Restore one node to the ready serving state in authoritative metadata.
    pub async fn undrain_node(&self, node_id: &str) -> Result<NodeMembershipStatus> {
        self.set_node_membership_state(node_id, "ready").await
    }

    async fn set_node_membership_state(
        &self,
        node_id: &str,
        state: &str,
    ) -> Result<NodeMembershipStatus> {
        let Some(client) = &self.coordination_client else {
            return Err(ServiceError::InvalidArgument(
                "node membership operations require the etcd metadata backend".to_owned(),
            ));
        };
        let local_leader = match self.require_local_control_plane_leader().await {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => client
                .current_leader()
                .await?
                .as_ref()
                .is_some_and(|leader| leader.node_id == self.config.node_name),
        };
        if !(local_leader || state == "ready" && node_id == self.config.node_name) {
            return Err(ServiceError::InvalidArgument(format!(
                "node '{}' is not the active control-plane leader",
                self.config.node_name
            )));
        }
        let record = client.set_membership_state(node_id, state).await?;
        Ok(NodeMembershipStatus {
            node_id: record.node_id,
            node_role: record.node_role,
            state: record.state,
        })
    }

    /// Promote one collection shard to a specific owner and update the recorded assignment.
    pub async fn promote_collection_owner(
        &self,
        collection_name: &str,
        new_owner_node_id: &str,
    ) -> Result<CollectionPlacement> {
        let Some(client) = &self.coordination_client else {
            return Err(ServiceError::InvalidArgument(
                "manual promotion requires the etcd metadata backend".to_owned(),
            ));
        };
        let descriptor = self.descriptor_for_collection(collection_name).await?;
        let Some(current) = self
            .authoritative_ownership_for_descriptor(&descriptor)
            .await?
        else {
            return Err(ServiceError::InvalidArgument(format!(
                "collection '{}/{}' has no authoritative shard ownership metadata",
                descriptor.database_name, descriptor.name
            )));
        };
        let leader_fence = self
            .require_local_control_plane_leader()
            .await?
            .ok_or_else(|| {
                ServiceError::Internal(
                    "etcd-backed promotion requires a local leadership fence".to_owned(),
                )
            })?;
        match client
            .promote_shard_owner(&current, new_owner_node_id)
            .await?
        {
            PromotionResult::Applied(promoted) => {
                client
                    .set_collection_assignment(
                        &promoted.collection,
                        CollectionAssignment {
                            assigned_node: promoted.owner_node_id.clone(),
                            assigned_role: NodeRole::Data,
                        },
                        &leader_fence,
                    )
                    .await?;
                client
                    .set_shard_failover_reason(
                        &promoted.collection,
                        &promoted.shard_id,
                        &format!(
                            "manual promotion to node '{new_owner_node_id}' from node '{}'",
                            current.owner_node_id
                        ),
                        &leader_fence,
                    )
                    .await?;
                self.collection_placement(&descriptor.lookup_name()).await
            }
            PromotionResult::Conflict => Err(ServiceError::FailedPrecondition(format!(
                "failed to promote collection '{}/{}' to node '{new_owner_node_id}' because the current owner or candidate readiness changed",
                descriptor.database_name, descriptor.name
            ))),
        }
    }

    /// Rebalance one collection by promoting it to a ready peer.
    pub async fn rebalance_collection(
        &self,
        collection_name: &str,
        target_node_id: Option<&str>,
    ) -> Result<CollectionPlacement> {
        let Some(client) = &self.coordination_client else {
            return Err(ServiceError::InvalidArgument(
                "rebalance requires the etcd metadata backend".to_owned(),
            ));
        };
        let descriptor = self.descriptor_for_collection(collection_name).await?;
        let Some(current) = self
            .authoritative_ownership_for_descriptor(&descriptor)
            .await?
        else {
            return Err(ServiceError::InvalidArgument(format!(
                "collection '{}/{}' has no authoritative shard ownership metadata",
                descriptor.database_name, descriptor.name
            )));
        };
        let target_node_id = match target_node_id {
            Some(target_node_id) => target_node_id.to_owned(),
            None => client
                .list_membership()
                .await?
                .into_iter()
                .find(|member| {
                    member.node_id != current.owner_node_id
                        && member.state == "ready"
                        && matches!(member.node_role, NodeRole::Combined | NodeRole::Data)
                })
                .map(|member| member.node_id)
                .ok_or_else(|| {
                    ServiceError::FailedPrecondition(format!(
                        "no ready rebalance target is available for collection '{}/{}'",
                        descriptor.database_name, descriptor.name
                    ))
                })?,
        };
        let placement = self
            .promote_collection_owner(&descriptor.lookup_name(), &target_node_id)
            .await?;
        if let (Some(client), Some(leader_fence)) = (
            self.coordination_client.as_ref(),
            self.require_local_control_plane_leader().await?,
        ) {
            let reference = CollectionRef::new(&descriptor.database_name, &descriptor.name);
            client
                .set_shard_failover_reason(
                    &reference,
                    "0",
                    &format!("manual rebalance to node '{target_node_id}'"),
                    &leader_fence,
                )
                .await?;
        }
        Ok(placement)
    }

    fn local_placement(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
        assignment: &CollectionAssignment,
        ownership: Option<&ShardOwnership>,
        replicas: Vec<ReplicaPlacement>,
        failover_reason: Option<String>,
        local_collection_available: bool,
        coordination: Option<&CoordinationStatus>,
    ) -> CollectionPlacement {
        let owner_node = ownership.map(|ownership| ownership.owner_node_id.clone());
        let ownership_epoch = ownership.map(|ownership| ownership.epoch);
        let metadata_revision =
            coordination.and_then(|coordination| coordination.metadata_revision);
        if let Some(ownership) = ownership {
            let owner_targets_this_runtime = ownership.owner_node_id == self.config.node_name;
            let local_membership_ready = coordination
                .map(coordination_membership_ready)
                .unwrap_or(true);
            let serves_local_assignment = owner_targets_this_runtime
                && local_collection_available
                && local_membership_ready
                && self.role_can_serve_assignment(assignment.assigned_role);
            let route_kind = if serves_local_assignment {
                "local"
            } else {
                "recorded"
            };
            return CollectionPlacement {
                collection_id: descriptor.collection_id.clone(),
                database_name: descriptor.database_name.clone(),
                collection_name: descriptor.name.clone(),
                assigned_node: assignment.assigned_node.clone(),
                assigned_role: assignment.assigned_role,
                owner_node,
                ownership_epoch,
                replicas,
                failover_reason,
                metadata_revision,
                route_kind: route_kind.to_owned(),
                route_reason: if serves_local_assignment {
                    format!(
                        "ownership epoch {} is active on this runtime",
                        ownership.epoch
                    )
                } else if owner_targets_this_runtime && !local_collection_available {
                    format!(
                        "ownership epoch {} targets this runtime but local collection state is absent",
                        ownership.epoch
                    )
                } else if owner_targets_this_runtime && !local_membership_ready {
                    format!(
                        "ownership epoch {} targets this runtime but the local membership lease is not active",
                        ownership.epoch
                    )
                } else if owner_targets_this_runtime {
                    format!(
                        "ownership epoch {} targets this runtime but role '{}' cannot serve it from '{}'",
                        ownership.epoch, assignment.assigned_role, self.config.node_role
                    )
                } else {
                    format!(
                        "ownership epoch {} is assigned to node '{}'",
                        ownership.epoch, ownership.owner_node_id
                    )
                },
            };
        }
        if self.coordination_client.is_some() {
            return CollectionPlacement {
                collection_id: descriptor.collection_id.clone(),
                database_name: descriptor.database_name.clone(),
                collection_name: descriptor.name.clone(),
                assigned_node: assignment.assigned_node.clone(),
                assigned_role: assignment.assigned_role,
                owner_node,
                ownership_epoch,
                replicas,
                failover_reason,
                metadata_revision,
                route_kind: "recorded".to_owned(),
                route_reason: format!(
                    "authoritative shard ownership metadata is missing for collection '{}/{}'; reconciliation is required before this runtime can serve it",
                    descriptor.database_name, descriptor.name
                ),
            };
        }
        let assignment_targets_this_runtime = assignment.assigned_node == self.config.node_name
            || assignment.assigned_node == ANONYMOUS_LOCAL_NODE_NAME;
        let serves_local_assignment = assignment_targets_this_runtime
            && local_collection_available
            && self.role_can_serve_assignment(assignment.assigned_role);
        let route_kind = if serves_local_assignment {
            "local"
        } else {
            "recorded"
        };
        CollectionPlacement {
            collection_id: descriptor.collection_id.clone(),
            database_name: descriptor.database_name.clone(),
            collection_name: descriptor.name.clone(),
            assigned_node: assignment.assigned_node.clone(),
            assigned_role: assignment.assigned_role,
            owner_node,
            ownership_epoch,
            replicas,
            failover_reason,
            metadata_revision,
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

    async fn replica_targets_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
        assignment: &CollectionAssignment,
        ownership: Option<&ShardOwnership>,
        local_collection_available: bool,
    ) -> Vec<ReplicaPlacement> {
        let desired_replica_count = descriptor.replication_factor.saturating_sub(1);
        if desired_replica_count == 0 {
            return Vec::new();
        }
        let Some(snapshot) = self.coordination.metadata_snapshot().await else {
            return Vec::new();
        };
        if snapshot.revision == 0 {
            return Vec::new();
        }
        let owner_node = ownership
            .map(|ownership| ownership.owner_node_id.as_str())
            .unwrap_or(assignment.assigned_node.as_str());
        let mut candidates = snapshot
            .members
            .into_iter()
            .filter(|member| {
                member.node_id != owner_node
                    && matches!(member.node_role, NodeRole::Combined | NodeRole::Data)
                    && member.state == "ready"
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        candidates
            .into_iter()
            .take(desired_replica_count)
            .map(|member| ReplicaPlacement {
                node_id: member.node_id.clone(),
                node_role: member.node_role,
                state: if member.node_id == self.config.node_name {
                    if local_collection_available {
                        "ready".to_owned()
                    } else {
                        "absent".to_owned()
                    }
                } else {
                    "unknown".to_owned()
                },
            })
            .collect()
    }

    async fn failover_reason_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Option<String> {
        self.cached_collection_metadata(&descriptor.lookup_name())
            .await
            .and_then(|collection| collection.failover_reason)
    }

    async fn cached_ready_descriptors(&self) -> Option<Vec<logpose_catalog::CollectionDescriptor>> {
        let snapshot = self.coordination.metadata_snapshot().await?;
        if snapshot.revision == 0 {
            return None;
        }
        Some(
            snapshot
                .collections
                .into_iter()
                .filter_map(|collection| {
                    if collection.descriptor_ready {
                        collection.descriptor
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }

    async fn cached_collection_metadata(
        &self,
        collection_name: &str,
    ) -> Option<ClusterCollectionMetadata> {
        let reference = parse_collection_reference(collection_name).ok()?;
        let lookup_name = reference.lookup_name();
        let snapshot = self.coordination.metadata_snapshot().await?;
        if snapshot.revision == 0 {
            return None;
        }
        snapshot
            .collections
            .into_iter()
            .find(|collection| collection.collection.lookup_name() == lookup_name)
    }

    async fn descriptor_for_collection(
        &self,
        collection_name: &str,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        if let Some(descriptor) = self
            .cached_collection_metadata(collection_name)
            .await
            .and_then(|collection| {
                if collection.descriptor_ready {
                    collection.descriptor
                } else {
                    None
                }
            })
        {
            return Ok(descriptor);
        }
        self.data.get_collection(collection_name).await
    }

    async fn assignment_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        if let Some(assignment) = self
            .cached_collection_metadata(&descriptor.lookup_name())
            .await
            .and_then(|collection| collection.assignment)
        {
            return Ok(assignment);
        }
        self.data.collection_assignment_descriptor(descriptor).await
    }

    async fn authoritative_assignment_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        let Some(client) = &self.coordination_client else {
            return self.assignment_for_descriptor(descriptor).await;
        };
        if let Some(assignment) = client
            .collection_assignment(&CollectionRef::new(
                descriptor.database_name.clone(),
                descriptor.name.clone(),
            ))
            .await?
        {
            Ok(assignment)
        } else {
            self.assignment_for_descriptor(descriptor).await
        }
    }

    async fn status_assignment_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        match self
            .authoritative_assignment_for_descriptor(descriptor)
            .await
        {
            Ok(assignment) => Ok(assignment),
            Err(_) => self.assignment_for_descriptor(descriptor).await,
        }
    }

    async fn ownership_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<Option<ShardOwnership>> {
        if let Some(owner) = self
            .cached_collection_metadata(&descriptor.lookup_name())
            .await
            .and_then(|collection| collection.owner)
        {
            return Ok(Some(owner));
        }
        let Some(client) = &self.coordination_client else {
            return Ok(None);
        };
        client
            .shard_owner(
                &CollectionRef::new(descriptor.database_name.clone(), descriptor.name.clone()),
                "0",
            )
            .await
            .map_err(Into::into)
    }

    async fn authoritative_ownership_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<Option<ShardOwnership>> {
        let Some(client) = &self.coordination_client else {
            return self.ownership_for_descriptor(descriptor).await;
        };
        client
            .shard_owner(
                &CollectionRef::new(descriptor.database_name.clone(), descriptor.name.clone()),
                "0",
            )
            .await
            .map_err(Into::into)
    }

    async fn status_ownership_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<Option<ShardOwnership>> {
        match self
            .authoritative_ownership_for_descriptor(descriptor)
            .await
        {
            Ok(ownership) => Ok(ownership),
            Err(_) => self.ownership_for_descriptor(descriptor).await,
        }
    }

    /// Require this runtime to own the active write path for one collection.
    pub async fn require_local_write_ownership(&self, collection_name: &str) -> Result<()> {
        if !matches!(self.config.node_role, NodeRole::Combined | NodeRole::Data) {
            return Err(ServiceError::InvalidArgument(format!(
                "node '{}' is running as '{}' and cannot accept data-plane operations",
                self.config.node_name, self.config.node_role
            )));
        }
        let descriptor = self.descriptor_for_collection(collection_name).await?;
        let assignment = self.assignment_for_descriptor(&descriptor).await?;
        let ownership = self
            .authoritative_ownership_for_descriptor(&descriptor)
            .await?;
        let local_collection_available = self
            .data
            .local_collection_matches_descriptor(&descriptor)
            .await?;
        let coordination = self.coordination.snapshot().await;
        if self.coordination_client.is_some() && ownership.is_none() {
            return Err(ServiceError::InvalidArgument(format!(
                "collection '{}/{}' has no authoritative shard ownership metadata and cannot accept writes until reconciliation completes",
                descriptor.database_name, descriptor.name
            )));
        }
        let replicas = self
            .replica_targets_for_descriptor(
                &descriptor,
                &assignment,
                ownership.as_ref(),
                local_collection_available,
            )
            .await;
        let failover_reason = self.failover_reason_for_descriptor(&descriptor).await;
        let placement = self.local_placement(
            &descriptor,
            &assignment,
            ownership.as_ref(),
            replicas,
            failover_reason,
            local_collection_available,
            coordination.as_ref(),
        );
        if placement.route_kind == "local"
            && matches!(placement.assigned_role, NodeRole::Combined | NodeRole::Data)
        {
            return Ok(());
        }
        let routed_node = placement
            .owner_node
            .clone()
            .unwrap_or_else(|| placement.assigned_node.clone());
        Err(ServiceError::InvalidArgument(format!(
            "collection '{}/{}' is assigned to node '{}' with role '{}' and is not locally served by node '{}'",
            descriptor.database_name,
            descriptor.name,
            routed_node,
            placement.assigned_role,
            self.config.node_name
        )))
    }

    fn role_can_serve_assignment(&self, assigned_role: NodeRole) -> bool {
        match assigned_role {
            NodeRole::Combined => self.config.node_role == NodeRole::Combined,
            NodeRole::Control => {
                matches!(
                    self.config.node_role,
                    NodeRole::Combined | NodeRole::Control
                )
            }
            NodeRole::Data => matches!(self.config.node_role, NodeRole::Combined | NodeRole::Data),
        }
    }

    /// Require this runtime to hold the active etcd-backed control-plane leadership.
    pub async fn require_local_control_plane_leader(&self) -> Result<Option<LeadershipFence>> {
        if !matches!(
            self.config.node_role,
            NodeRole::Combined | NodeRole::Control
        ) {
            return Ok(None);
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let Some(coordination) = self.coordination.snapshot().await else {
                return Ok(None);
            };
            if coordination.is_local_leader && coordination_membership_ready(&coordination) {
                let lease_id = coordination.leadership_lease_id.ok_or_else(|| {
                    ServiceError::Internal(format!(
                        "node '{}' reported local leadership without a lease id",
                        self.config.node_name
                    ))
                })?;
                return Ok(Some(LeadershipFence {
                    node_id: self.config.node_name.clone(),
                    lease_id,
                }));
            }
            if coordination.leader_node.is_none()
                && let Some(client) = &self.coordination_client
                && let Ok(Some(leader_record)) = client.current_leader().await
            {
                if leader_record.node_id == self.config.node_name {
                    return Ok(Some(LeadershipFence {
                        node_id: self.config.node_name.clone(),
                        lease_id: leader_record.lease_id,
                    }));
                }
                return Err(ServiceError::InvalidArgument(format!(
                    "node '{}' is not the active control-plane leader; current leader is '{}'",
                    self.config.node_name, leader_record.node_id
                )));
            }
            if coordination.leader_node.is_some() || Instant::now() >= deadline {
                let leader = coordination
                    .leader_node
                    .unwrap_or_else(|| "none".to_owned());
                return Err(ServiceError::InvalidArgument(format!(
                    "node '{}' is not the active control-plane leader; current leader is '{}'",
                    self.config.node_name, leader
                )));
            }
            sleep(Duration::from_millis(25)).await;
        }
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
        || message.contains("must not be a relative path component")
        || message.contains("must be greater than 0")
        || message.contains("role binding database_name must match policy database_name")
        || message.contains("authentication_mode")
        || message.contains("is_default")
        || message.contains("invalid snapshot")
        || message.contains("not the active control-plane leader")
        || message.contains("already has metadata assignment in etcd")
        || message.contains("snapshot and read_barrier cannot be provided together")
        || message.contains("manual reconciliation is required")
        || message.contains("reconciliation is required")
        || is_dimension_validation_error(&message)
    {
        ServiceError::InvalidArgument(message)
    } else if message.contains("read barrier") {
        ServiceError::FailedPrecondition(message)
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
        [database_name, collection_name] => CollectionRef::new(*database_name, *collection_name),
        _ => {
            return Err(LogPoseError::Message(format!(
                "unsupported collection reference '{collection_name}': expected 'collection' or 'database/collection'"
            )));
        }
    };
    reference.validate()?;
    Ok(reference)
}

fn coordination_membership_ready(status: &CoordinationStatus) -> bool {
    status.membership_registered
        && status
            .membership_state
            .as_deref()
            .unwrap_or("ready")
            .eq("ready")
}

fn ensure_collection_reference_matches_descriptor(
    reference: &CollectionRef,
    descriptor: &logpose_catalog::CollectionDescriptor,
    original_name: &str,
) -> logpose_types::Result<()> {
    if reference.database_name != descriptor.database_name
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
        time::{SystemTime, UNIX_EPOCH},
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
            "collection 'default/documents' has authoritative metadata in etcd but local state finalization is still pending; manual reconciliation is required before serving it".to_owned(),
        ));

        assert!(matches!(error, ServiceError::InvalidArgument(_)));
    }

    #[test]
    fn parse_collection_reference_accepts_database_collection() {
        let reference = parse_collection_reference("analytics/documents")
            .expect("database-qualified collection name should parse");

        assert_eq!(reference.database_name, "analytics");
        assert_eq!(reference.collection_name, "documents");
    }

    #[test]
    fn reconcile_coordination_snapshot_preserves_healthy_fields_with_pending_error() {
        let snapshot = RwLock::new(CoordinationStatus {
            cluster_name: "default".to_owned(),
            membership_registered: false,
            membership_state: None,
            membership_lease_id: None,
            registered_members: Vec::new(),
            leader_node: None,
            is_local_leader: false,
            leadership_lease_id: None,
            metadata_revision: None,
            watch_lag: None,
            last_error: None,
        });

        reconcile_coordination_snapshot(
            &snapshot,
            "node-a",
            Some(11),
            Some(&LeadershipLease {
                node_id: "node-a".to_owned(),
                lease_id: 22,
                key: "/leaders/node-a".to_owned(),
            }),
            &Ok(vec![MembershipRecord {
                node_id: "node-a".to_owned(),
                node_role: NodeRole::Combined,
                state: "ready".to_owned(),
            }]),
            &Ok(Some(LeadershipRecord {
                node_id: "node-a".to_owned(),
                lease_id: 22,
            })),
            CoordinationSnapshotUpdate {
                metadata_revision: Some(42),
                watch_lag: Some(0),
                pending_error: Some("membership keep-alive failed".to_owned()),
            },
        );

        let current = coordination_read(&snapshot).clone();
        assert!(current.membership_registered);
        assert_eq!(current.membership_lease_id, Some(11));
        assert_eq!(current.registered_members, vec!["node-a".to_owned()]);
        assert_eq!(current.leader_node.as_deref(), Some("node-a"));
        assert!(current.is_local_leader);
        assert_eq!(current.leadership_lease_id, Some(22));
        assert_eq!(current.metadata_revision, Some(42));
        assert_eq!(current.watch_lag, Some(0));
        assert_eq!(
            current.last_error.as_deref(),
            Some("membership keep-alive failed")
        );
    }

    #[test]
    fn missing_membership_keep_alive_session_drops_tracked_membership_and_leadership() {
        let mut membership_lease_id = Some(11);
        let mut leadership_lease = Some(LeadershipLease {
            node_id: "node-a".to_owned(),
            lease_id: 22,
            key: "/leaders/node-a".to_owned(),
        });

        let reset = recover_tracked_membership_after_keep_alive_error(
            "no keep-alive session is registered for lease '11'",
            &mut membership_lease_id,
            &mut leadership_lease,
        );

        assert!(reset);
        assert_eq!(membership_lease_id, None);
        assert_eq!(leadership_lease, None);
    }

    #[test]
    fn missing_leadership_keep_alive_session_drops_only_tracked_leadership() {
        let mut leadership_lease = Some(LeadershipLease {
            node_id: "node-a".to_owned(),
            lease_id: 22,
            key: "/leaders/node-a".to_owned(),
        });

        let reset = recover_tracked_leadership_after_keep_alive_error(
            "no keep-alive session is registered for lease '22'",
            &mut leadership_lease,
        );

        assert!(reset);
        assert_eq!(leadership_lease, None);
    }

    #[test]
    fn transport_keep_alive_errors_do_not_drop_tracked_leases() {
        let mut membership_lease_id = Some(11);
        let mut leadership_lease = Some(LeadershipLease {
            node_id: "node-a".to_owned(),
            lease_id: 22,
            key: "/leaders/node-a".to_owned(),
        });

        let membership_reset = recover_tracked_membership_after_keep_alive_error(
            "transport error: connection refused",
            &mut membership_lease_id,
            &mut leadership_lease,
        );

        assert!(!membership_reset);
        assert_eq!(membership_lease_id, Some(11));
        assert_eq!(
            leadership_lease.as_ref().map(|lease| lease.lease_id),
            Some(22)
        );

        let leadership_reset = recover_tracked_leadership_after_keep_alive_error(
            "transport error: connection refused",
            &mut leadership_lease,
        );

        assert!(!leadership_reset);
        assert_eq!(
            leadership_lease.as_ref().map(|lease| lease.lease_id),
            Some(22)
        );
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
                database_name: "default".to_owned(),
                name: "documents".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Dot,
                replication_factor: 1,
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
        let catalog_root = std::env::temp_dir().join(format!(
            "logpose-service-metadata-unavailable-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        let control = LogPoseControlService::new(
            data,
            local_catalog_store(&catalog_root),
            LogPoseConfig::default(),
            BuildInfo::current(),
        );

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
