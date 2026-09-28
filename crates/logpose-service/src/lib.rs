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
use thiserror as _;
#[cfg(test)]
use tokio as _;
#[cfg(test)]
use tonic as _;
#[cfg(test)]
use tower as _;

use logpose_auth::{DatabaseAccessPolicy, Principal};
use logpose_catalog::{CatalogStore, DatabaseDescriptor};
use logpose_config::LogPoseConfig;
use logpose_query::{
    CountRecordsRequest, CountRecordsResponse, QueryRequest, QueryResponse, ScrollRecordsRequest,
    ScrollRecordsResponse, WithSchema,
};
use logpose_storage::{
    CreateCollectionRequest, InspectReport, InspectTarget, LocalStorageEngine, ReadOptions,
    StorageEngine,
};
use logpose_storage_etcd::{
    EtcdCoordinationClient, LeadershipLease, LeadershipRecord, LeaseKeepAlive, MembershipRecord,
    ShardOwnership,
};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, BuildInfo, CollectionAssignment, CollectionPlacement, CollectionRef,
    CollectionStats, CommitAck, CoordinationStatus, LeadershipFence, LogPoseError,
    MaintenanceBacklog, MaintenanceStatus, MetadataBackend, NodeRole, NodeRuntimeStatus,
    ResourceKind, Snapshot,
    filter::FilterExpr,
    record::{ClientOp, PartialUpdate, PrimaryKey, Projection, Record, RecordPatch},
    schema::{CollectionSchema, SchemaChange},
};
use std::{
    fmt,
    net::IpAddr,
    path::Path,
    sync::{
        Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    runtime::Handle,
    time::{Duration, Instant, interval, sleep},
};

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

/// Service-local result type. Every layer reports the one typed [`LogPoseError`].
pub type Result<T> = std::result::Result<T, LogPoseError>;

#[derive(Clone)]
enum CoordinationRuntime {
    Local,
    Etcd(Arc<EtcdRuntime>),
}

#[derive(Debug)]
struct EtcdRuntime {
    snapshot: Arc<RwLock<CoordinationStatus>>,
    shutdown: Arc<AtomicBool>,
}

impl Drop for EtcdRuntime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl CoordinationRuntime {
    fn new(config: &LogPoseConfig) -> Self {
        if config.metadata.backend != MetadataBackend::Etcd {
            return Self::Local;
        }

        let snapshot = Arc::new(RwLock::new(CoordinationStatus {
            cluster_name: config.metadata.etcd.cluster_name.clone(),
            membership_registered: false,
            membership_lease_id: None,
            registered_members: Vec::new(),
            leader_node: None,
            is_local_leader: false,
            leadership_lease_id: None,
            last_error: None,
        }));
        let runtime = Arc::new(EtcdRuntime {
            snapshot: Arc::clone(&snapshot),
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
                handle.spawn(async move {
                    run_coordination_loop(client, snapshot, shutdown, node_name, node_role, tick)
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
}

fn coordination_tick(config: &logpose_types::EtcdMetadataConfig) -> Duration {
    let ttl_secs = config
        .membership_ttl_secs
        .min(config.leadership_ttl_secs)
        .max(1) as u64;
    Duration::from_secs((ttl_secs / 3).max(1))
}

/// Drive etcd membership and controller leadership for this node.
///
/// Each tick refreshes the leases the node holds, drops any claim etcd no
/// longer backs (a dead lease, or a membership or leader key that is missing
/// or owned by someone else), and re-acquires what is missing in the same
/// tick. Losing membership also gives up leadership, because a node that is
/// not a registered member must not lead.
async fn run_coordination_loop(
    client: EtcdCoordinationClient,
    snapshot: Arc<RwLock<CoordinationStatus>>,
    shutdown: Arc<AtomicBool>,
    node_name: String,
    node_role: NodeRole,
    tick: Duration,
) {
    let campaigns = matches!(node_role, NodeRole::Combined | NodeRole::Control);
    let mut membership_lease_id: Option<i64> = None;
    let mut leadership_lease: Option<LeadershipLease> = None;
    let mut ticker = interval(tick);
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        ticker.tick().await;
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let mut pending_error = None;

        if let Some(lease_id) = membership_lease_id {
            match client.keep_alive(lease_id).await {
                Ok(LeaseKeepAlive::Alive { .. }) => {}
                Ok(LeaseKeepAlive::Expired) => membership_lease_id = None,
                Err(error) => note_coordination_error(&mut pending_error, error.to_string()),
            }
        }
        if let Some(lease) = &leadership_lease {
            match client.keep_alive(lease.lease_id).await {
                Ok(LeaseKeepAlive::Alive { .. }) => {}
                Ok(LeaseKeepAlive::Expired) => leadership_lease = None,
                Err(error) => note_coordination_error(&mut pending_error, error.to_string()),
            }
        }

        let mut members = client.list_membership().await;
        let mut leader = client.current_leader().await;
        let mut dropped_leases = Vec::new();
        if membership_lease_id.is_some()
            && members
                .as_ref()
                .is_ok_and(|records| !lists_member(records, &node_name))
        {
            dropped_leases.extend(membership_lease_id.take());
        }
        if membership_lease_id.is_none() {
            dropped_leases.extend(leadership_lease.take().map(|lease| lease.lease_id));
        }
        if let Some(lease) = &leadership_lease
            && leader
                .as_ref()
                .is_ok_and(|record| !holds_leadership(record.as_ref(), lease))
        {
            dropped_leases.extend(leadership_lease.take().map(|lease| lease.lease_id));
        }
        // Stop advertising lost claims before the revoke round trips, so
        // request gates never see a claim this tick already knows is gone.
        demote_lost_claims(
            &snapshot,
            &node_name,
            membership_lease_id,
            leadership_lease.as_ref(),
        );
        for lease_id in &dropped_leases {
            let _ = client.revoke_lease(*lease_id).await;
        }
        // Revoking a lease deletes the keys attached to it, so a leader key
        // read before the revoke that named one of those leases is now vacant.
        if leader.as_ref().is_ok_and(|record| {
            record
                .as_ref()
                .is_some_and(|record| dropped_leases.contains(&record.lease_id))
        }) {
            leader = Ok(None);
        }

        let mut acquired = false;
        if membership_lease_id.is_none() {
            match client.register_membership(&node_name, node_role).await {
                Ok(lease) => {
                    membership_lease_id = Some(lease.lease_id);
                    acquired = true;
                }
                Err(error) => {
                    record_coordination_error(&snapshot, error.to_string());
                    continue;
                }
            }
        }
        // Any live leader key makes the campaign transaction fail, including
        // one left by this node's previous process, so only campaign when the
        // key is vacant or unreadable instead of granting a lease every tick.
        if campaigns && leadership_lease.is_none() && !matches!(leader, Ok(Some(_))) {
            match client.try_acquire_leadership(&node_name).await {
                Ok(Some(lease)) => {
                    leadership_lease = Some(lease);
                    acquired = true;
                }
                Ok(None) => {}
                Err(error) => note_coordination_error(&mut pending_error, error.to_string()),
            }
        }
        if acquired {
            members = client.list_membership().await;
            leader = client.current_leader().await;
        }

        reconcile_coordination_snapshot(
            &snapshot,
            &node_name,
            membership_lease_id,
            leadership_lease.as_ref(),
            &members,
            &leader,
            pending_error,
        );
    }

    if let Some(lease) = leadership_lease.take() {
        let _ = client.revoke_lease(lease.lease_id).await;
    }
    if let Some(lease_id) = membership_lease_id.take() {
        let _ = client.revoke_lease(lease_id).await;
    }
}

fn lists_member(members: &[MembershipRecord], node_name: &str) -> bool {
    members.iter().any(|member| member.node_id == node_name)
}

/// Whether the visible leader key is backed by the lease this node holds.
fn holds_leadership(leader: Option<&LeadershipRecord>, lease: &LeadershipLease) -> bool {
    leader
        .is_some_and(|record| record.node_id == lease.node_id && record.lease_id == lease.lease_id)
}

/// Clear snapshot claims this tick found lost, before revoking them or
/// re-acquiring replacements, so request gates stop trusting them immediately.
fn demote_lost_claims(
    snapshot: &RwLock<CoordinationStatus>,
    node_name: &str,
    membership_lease_id: Option<i64>,
    leadership_lease: Option<&LeadershipLease>,
) {
    let mut current = coordination_write(snapshot);
    if membership_lease_id.is_none() {
        current.membership_registered = false;
        current.membership_lease_id = None;
        current
            .registered_members
            .retain(|member| member != node_name);
    }
    if membership_lease_id.is_none() || leadership_lease.is_none() {
        current.is_local_leader = false;
        current.leadership_lease_id = None;
        if current.leader_node.as_deref() == Some(node_name) {
            current.leader_node = None;
        }
    }
}

fn note_coordination_error(pending_error: &mut Option<String>, error: String) {
    if pending_error.is_none() {
        *pending_error = Some(error);
    }
}

fn reconcile_coordination_snapshot(
    snapshot: &RwLock<CoordinationStatus>,
    node_name: &str,
    membership_lease_id: Option<i64>,
    leadership_lease: Option<&LeadershipLease>,
    members: &logpose_types::Result<Vec<MembershipRecord>>,
    leader: &logpose_types::Result<Option<LeadershipRecord>>,
    pending_error: Option<String>,
) {
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
    current.last_error = reconcile_coordination_last_error(pending_error, members, leader);
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

fn record_coordination_error(snapshot: &RwLock<CoordinationStatus>, error: String) {
    let mut current = coordination_write(snapshot);
    current.last_error = Some(error);
    current.membership_registered = false;
    current.membership_lease_id = None;
    current.registered_members.clear();
    current.leader_node = None;
    current.is_local_leader = false;
    current.leadership_lease_id = None;
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
    ///
    /// Fails if another process holds the storage root.
    pub fn local(root: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(Arc::new(LocalStorageEngine::with_resolver(
            root,
            logpose_query::resolver(),
        )?)))
    }

    /// Create a collection.
    pub async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        self.storage.create_collection(request).await
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
        self.storage.list_collections().await
    }

    /// Load the persisted placement assignment for a descriptor.
    pub async fn collection_assignment_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        self.storage
            .collection_assignment_descriptor(descriptor)
            .await
    }

    /// Return the underlying engine identifier.
    pub async fn engine_name(&self) -> &'static str {
        self.storage.engine_name().await
    }

    /// Verify whether the backing metadata authority is currently reachable.
    pub async fn metadata_status(&self) -> Result<()> {
        self.storage.metadata_status().await
    }

    /// Return whether the collection's local on-disk state exists on this node.
    pub async fn has_local_collection(&self, collection_name: &str) -> Result<bool> {
        self.storage.has_local_collection(collection_name).await
    }

    /// Return whether the local on-disk descriptor matches the authoritative descriptor.
    pub async fn local_collection_matches_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<bool> {
        self.storage
            .local_collection_matches_descriptor(descriptor)
            .await
    }

    /// List the collections of one database, each with its live schema.
    pub async fn list_collections_in_database(
        &self,
        database_name: &str,
    ) -> Result<Vec<logpose_catalog::CollectionDescriptor>> {
        let mut descriptors = self.storage.list_collections().await?;
        descriptors.retain(|descriptor| descriptor.database_name == database_name);
        Ok(descriptors)
    }

    /// Drop a collection, fenced by `leader_fence` when metadata lives in a shared store.
    pub async fn drop_collection(
        &self,
        collection_name: &str,
        leader_fence: Option<LeadershipFence>,
    ) -> Result<()> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage
            .drop_collection(&descriptor.lookup_name(), leader_fence)
            .await
    }

    /// Change a collection's schema online and return the collection with its new schema.
    pub async fn alter_collection(
        &self,
        collection_name: &str,
        change: SchemaChange,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        let lookup_name = descriptor.lookup_name();
        self.storage.alter_schema(&lookup_name, change).await?;
        self.storage.open_collection(&lookup_name).await
    }

    /// The collection's live schema.
    pub async fn schema(&self, collection_name: &str) -> Result<Arc<CollectionSchema>> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage.schema(&descriptor.lookup_name()).await
    }

    /// Commit upserts, partial updates, and deletes by key as one atomic batch. Validation
    /// errors name the operation as `operations[i]`.
    pub async fn write(&self, collection_name: &str, ops: Vec<ClientOp>) -> Result<CommitAck> {
        self.write_batch(collection_name, ops, "operations").await
    }

    /// Insert or replace whole records as one atomic batch. Validation errors name the
    /// offending field as `records[i].<field>`.
    pub async fn upsert(&self, collection_name: &str, records: Vec<Record>) -> Result<CommitAck> {
        self.write_batch(
            collection_name,
            records.into_iter().map(ClientOp::Upsert).collect(),
            "records",
        )
        .await
    }

    /// Change some fields of existing records as one atomic batch. A key without a live record
    /// fails the batch with `NOT_FOUND`. Validation errors name `records[i].<field>`.
    pub async fn update(
        &self,
        collection_name: &str,
        updates: Vec<PartialUpdate>,
    ) -> Result<CommitAck> {
        self.write_batch(
            collection_name,
            updates.into_iter().map(ClientOp::Update).collect(),
            "records",
        )
        .await
    }

    /// Delete records by primary key as one atomic batch. A key without a live record is a
    /// no-op. Validation errors name `keys[i]`.
    pub async fn delete(&self, collection_name: &str, keys: Vec<PrimaryKey>) -> Result<CommitAck> {
        self.write_batch(
            collection_name,
            keys.into_iter().map(ClientOp::Delete).collect(),
            "keys",
        )
        .await
    }

    /// Point lookups by primary key, projected to `output_fields` (every field when empty).
    pub async fn get_records(
        &self,
        collection_name: &str,
        keys: Vec<PrimaryKey>,
        output_fields: Vec<String>,
    ) -> Result<FetchedRecords> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        let view = self
            .storage
            .read_view(&descriptor.collection_ref(), ReadOptions::default())
            .await?;
        let schema = Arc::clone(view.schema());
        let projection = Projection::resolve(&schema, &output_fields)?;
        for (index, key) in keys.iter().enumerate() {
            schema.validate_primary_key(key).map_err(|error| {
                LogPoseError::invalid_field(format!("keys[{index}]"), error.to_string())
            })?;
        }
        let rows = view
            .get(
                &keys,
                logpose_storage::Projection {
                    vectors: projection.selects_vectors(&schema),
                    seq_no: false,
                },
            )
            .await?;
        Ok(FetchedRecords {
            snapshot: view.snapshot(),
            records: rows
                .into_iter()
                .map(|row| row.map(|row| projection.apply(row.record)))
                .collect(),
            schema,
        })
    }

    async fn write_batch(
        &self,
        collection_name: &str,
        ops: Vec<ClientOp>,
        field: &str,
    ) -> Result<CommitAck> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage
            .write_batch(&descriptor.lookup_name(), ops)
            .await
            .map_err(|error| error.with_field_prefix(field))
    }

    /// Delete every live record matching `filter`, resolved against the writer's latest state
    /// and committed as one atomic batch; `applied_ops` is the number deleted. Filter errors name
    /// the node below `filter`. A match whose keys do not fit one WAL frame fails with
    /// `TooLarge` and deletes nothing.
    pub async fn delete_by_filter(
        &self,
        collection_name: &str,
        filter: FilterExpr,
    ) -> Result<CommitAck> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage
            .delete_by_filter(&descriptor.lookup_name(), filter)
            .await
    }

    /// Apply `patch` to every live record matching `filter`, as [`delete_by_filter`] resolves
    /// and commits them; `applied_ops` is the number updated. Patch errors name
    /// `patch.<field>`.
    ///
    /// [`delete_by_filter`]: Self::delete_by_filter
    pub async fn update_by_filter(
        &self,
        collection_name: &str,
        filter: FilterExpr,
        patch: RecordPatch,
    ) -> Result<CommitAck> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        let lookup_name = descriptor.lookup_name();
        let schema = self.storage.schema(&lookup_name).await?;
        let patch = patch
            .validate(&schema)
            .map_err(|error| error.to_error("patch", None))?;
        let placeholder = match schema.primary_key_type() {
            logpose_types::schema::PrimaryKeyType::Int64 => PrimaryKey::Int64(0),
            logpose_types::schema::PrimaryKeyType::String => PrimaryKey::String("_".to_owned()),
        };
        self.storage
            .update_by_filter(&lookup_name, filter, patch.into_update(placeholder))
            .await
            .map_err(|error| error.with_field_prefix("patch"))
    }

    /// Search a collection (or scan it in order): see [`logpose_query::query`].
    pub async fn query_collection(
        &self,
        collection_name: &str,
        request: QueryRequest,
    ) -> Result<WithSchema<QueryResponse>> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        logpose_query::query(
            self.storage.as_ref(),
            &descriptor.collection_ref(),
            request,
        )
        .await
        .map_err(Into::into)
    }

    /// Count the live records matching a filter: see [`logpose_query::count_records`].
    pub async fn count_records(
        &self,
        collection_name: &str,
        request: CountRecordsRequest,
    ) -> Result<CountRecordsResponse> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        logpose_query::count_records(
            self.storage.as_ref(),
            &descriptor.collection_ref(),
            request,
        )
        .await
        .map_err(Into::into)
    }

    /// One page of a scroll: see [`logpose_query::scroll_records`].
    pub async fn scroll_records(
        &self,
        collection_name: &str,
        request: ScrollRecordsRequest,
    ) -> Result<WithSchema<ScrollRecordsResponse>> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        logpose_query::scroll_records(
            self.storage.as_ref(),
            &descriptor.collection_ref(),
            request,
        )
        .await
        .map_err(Into::into)
    }

    /// Capture the current read snapshot.
    pub async fn snapshot(&self, collection_name: &str) -> Result<Snapshot> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage.snapshot(&descriptor.lookup_name()).await
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
        self.storage.stats_descriptor(descriptor, snapshot).await
    }

    /// The collection's maintenance status (runtime state) without reconstructing full stats.
    pub async fn maintenance_status_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        self.storage.maintenance_status_descriptor(descriptor).await
    }

    /// Flush the mutable delta to a new segment.
    pub async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage.flush(&descriptor.lookup_name()).await
    }

    /// Compact immutable segments.
    pub async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.storage.compact(&descriptor.lookup_name()).await
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
        let reference = CollectionRef::parse(collection_name)?;
        let descriptor = self
            .storage
            .open_collection(collection_name)
            .await
            .map_err(|error| qualify_collection_error(error, collection_name))?;
        ensure_collection_reference_matches_descriptor(&reference, &descriptor, collection_name)?;
        Ok(descriptor)
    }

    async fn resolve_read_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        read_barrier: Option<Snapshot>,
    ) -> Result<Option<Snapshot>> {
        match (snapshot, read_barrier) {
            (Some(_), Some(_)) => Err(LogPoseError::invalid_field(
                "read_barrier",
                "snapshot and read_barrier cannot be provided together",
            )),
            (Some(snapshot), None) => Ok(Some(snapshot)),
            (None, None) => Ok(None),
            (None, Some(read_barrier)) => {
                let current = self.snapshot(collection_name).await?;
                if current.satisfies_read_barrier(&read_barrier) {
                    // Read the current state, not `current`: a state published since is newer,
                    // so it satisfies the barrier too, while `current` stops being readable
                    // once a flush or compaction supersedes its generation.
                    Ok(None)
                } else {
                    Err(LogPoseError::ReadBarrierNotSatisfied {
                        collection: collection_name.to_owned(),
                        required_manifest_generation: read_barrier.manifest_generation,
                        required_seq_no: read_barrier.visible_seq_no,
                        visible_manifest_generation: current.manifest_generation,
                        visible_seq_no: current.visible_seq_no,
                    })
                }
            }
        }
    }
}

/// Build a filesystem-backed catalog store rooted under the runtime storage directory.
///
/// Opens its own engine, so it fails if another engine holds the storage root; a process that
/// already serves the root shares that engine's catalog instead.
pub fn local_catalog_store(root: impl AsRef<Path>) -> Result<Arc<dyn CatalogStore>> {
    Ok(Arc::new(LocalStorageEngine::new(root)?))
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
        let coordination = CoordinationRuntime::new(&config);
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

    /// The catalog store behind this control plane.
    #[must_use]
    pub fn catalog_store(&self) -> &Arc<dyn CatalogStore> {
        &self.catalog
    }

    /// Create a collection through the control-plane surface.
    pub async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        self.require_collection_lifecycle_role()?;
        let leader_fence = self.require_local_control_plane_leader().await?;
        let assignment = self.initial_assignment();
        if self.config.metadata.backend == MetadataBackend::Etcd
            && let Some(leader_fence) = leader_fence.as_ref()
            && assignment.assigned_node != leader_fence.node_id
        {
            return Err(LogPoseError::internal(format!(
                "etcd-backed collection creation requires the active control-plane leader '{}' to own the initial assignment; got '{}'",
                leader_fence.node_id, assignment.assigned_node
            )));
        }
        self.data
            .create_collection_with_assignment(request, assignment, leader_fence)
            .await
    }

    /// Drop a collection through the control-plane surface.
    pub async fn drop_collection(&self, collection_name: &str) -> Result<()> {
        self.require_collection_lifecycle_role()?;
        let leader_fence = self.require_local_control_plane_leader().await?;
        self.data
            .drop_collection(collection_name, leader_fence)
            .await
    }

    /// Delete a database and its access policy from the local catalog. Refuses the default
    /// database and a database that still holds a collection.
    pub async fn drop_database(&self, database_name: &str) -> Result<()> {
        match self.config.node_role {
            NodeRole::Data => {
                return Err(LogPoseError::WrongNodeRole {
                    node: self.config.node_name.clone(),
                    role: self.config.node_role,
                    operation: "control-plane database mutations".to_owned(),
                });
            }
            NodeRole::Control | NodeRole::Combined => {}
        }
        self.require_local_control_plane_leader().await?;
        self.catalog.delete_database(database_name)
    }

    fn require_collection_lifecycle_role(&self) -> Result<()> {
        match self.config.node_role {
            NodeRole::Data | NodeRole::Control => Err(LogPoseError::WrongNodeRole {
                node: self.config.node_name.clone(),
                role: self.config.node_role,
                operation: "control-plane collection lifecycle mutations".to_owned(),
            }),
            NodeRole::Combined => Ok(()),
        }
    }

    /// Create or replace one database-scoped access policy.
    pub async fn set_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        match self.config.node_role {
            NodeRole::Data => {
                return Err(LogPoseError::WrongNodeRole {
                    node: self.config.node_name.clone(),
                    role: self.config.node_role,
                    operation: "control-plane database policy mutations".to_owned(),
                });
            }
            NodeRole::Control | NodeRole::Combined => {}
        }
        self.require_local_control_plane_leader().await?;
        self.catalog.put_database_access_policy(policy)
    }

    /// Create or replace one database descriptor.
    pub async fn put_database(&self, descriptor: DatabaseDescriptor) -> Result<DatabaseDescriptor> {
        match self.config.node_role {
            NodeRole::Data => {
                return Err(LogPoseError::WrongNodeRole {
                    node: self.config.node_name.clone(),
                    role: self.config.node_role,
                    operation: "control-plane database mutations".to_owned(),
                });
            }
            NodeRole::Control | NodeRole::Combined => {}
        }
        self.require_local_control_plane_leader().await?;
        self.catalog.put_database(descriptor)
    }

    /// Read one database descriptor.
    pub async fn database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        self.catalog.get_database(database_name)
    }

    /// List every database descriptor.
    pub async fn databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        self.catalog.list_databases()
    }

    /// Read one database-scoped access policy.
    pub async fn database_access_policy(
        &self,
        database_name: &str,
    ) -> Result<DatabaseAccessPolicy> {
        self.catalog.get_database_access_policy(database_name)
    }

    /// Read one persisted principal descriptor.
    pub async fn principal(&self, principal_name: &str) -> Result<Principal> {
        self.catalog.get_principal(principal_name)
    }

    /// Return the placement summary for one collection.
    pub async fn collection_placement(&self, collection_name: &str) -> Result<CollectionPlacement> {
        let descriptor = self.data.get_collection(collection_name).await?;
        let assignment = self.assignment_for_descriptor(&descriptor).await?;
        let ownership = self.ownership_for_descriptor(&descriptor).await?;
        let local_collection_available = self
            .data
            .local_collection_matches_descriptor(&descriptor)
            .await?;
        let coordination = self.coordination.snapshot().await;
        Ok(self.local_placement(
            &descriptor,
            &assignment,
            ownership.as_ref(),
            local_collection_available,
            coordination.as_ref(),
        ))
    }

    /// Return aggregated runtime and maintenance status for the local node.
    pub async fn runtime_status(&self) -> Result<NodeRuntimeStatus> {
        let metadata_ready = self.data.metadata_status().await.is_ok();
        let coordination = self.coordination.snapshot().await;
        let descriptors = if metadata_ready {
            self.data.list_collections().await?
        } else {
            Vec::new()
        };
        let mut placements = Vec::with_capacity(descriptors.len());
        let mut local_descriptors = Vec::new();
        for descriptor in &descriptors {
            let assignment = self.assignment_for_descriptor(descriptor).await?;
            let ownership = self.ownership_for_descriptor(descriptor).await?;
            let local_collection_available = self
                .data
                .local_collection_matches_descriptor(descriptor)
                .await?;
            let placement = self.local_placement(
                descriptor,
                &assignment,
                ownership.as_ref(),
                local_collection_available,
                coordination.as_ref(),
            );
            if placement.route_kind == "local" {
                local_descriptors.push(descriptor);
            }
            placements.push(placement);
        }
        placements.sort_by(|left, right| {
            (&left.database_name, &left.collection_name)
                .cmp(&(&right.database_name, &right.collection_name))
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

        let control_coordination_ready = coordination.as_ref().is_none_or(|status| {
            status.membership_registered
                && status.last_error.is_none()
                && (!matches!(
                    self.config.node_role,
                    NodeRole::Combined | NodeRole::Control
                ) || status.is_local_leader)
        });
        let data_coordination_ready = coordination
            .as_ref()
            .is_none_or(|status| status.membership_registered && status.last_error.is_none());

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
                Err(LogPoseError::NotFound { .. }) => {
                    self.catalog.put_principal(token.principal.clone())?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Return the current distributed coordination status when one exists.
    pub async fn coordination_status(&self) -> Option<CoordinationStatus> {
        self.coordination.snapshot().await
    }

    fn local_placement(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
        assignment: &CollectionAssignment,
        ownership: Option<&ShardOwnership>,
        local_collection_available: bool,
        coordination: Option<&CoordinationStatus>,
    ) -> CollectionPlacement {
        let owner_node = ownership.map(|ownership| ownership.owner_node_id.clone());
        let ownership_epoch = ownership.map(|ownership| ownership.epoch);
        if let Some(ownership) = ownership {
            let owner_targets_this_runtime = ownership.owner_node_id == self.config.node_name;
            let local_membership_ready = coordination
                .map(|coordination| coordination.membership_registered)
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

    async fn ownership_for_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<Option<ShardOwnership>> {
        let Some(client) = &self.coordination_client else {
            return Ok(None);
        };
        client
            .shard_owner(
                &CollectionRef::new(descriptor.database_name.clone(), descriptor.name.clone()),
                "0",
            )
            .await
    }

    /// Require this runtime to own the active write path for one collection.
    pub async fn require_local_write_ownership(&self, collection_name: &str) -> Result<()> {
        if !matches!(self.config.node_role, NodeRole::Combined | NodeRole::Data) {
            return Err(LogPoseError::WrongNodeRole {
                node: self.config.node_name.clone(),
                role: self.config.node_role,
                operation: "data-plane operations".to_owned(),
            });
        }
        let descriptor = self.data.get_collection(collection_name).await?;
        let assignment = self.assignment_for_descriptor(&descriptor).await?;
        let ownership = self.ownership_for_descriptor(&descriptor).await?;
        let local_collection_available = self
            .data
            .local_collection_matches_descriptor(&descriptor)
            .await?;
        let coordination = self.coordination.snapshot().await;
        if self.coordination_client.is_some() && ownership.is_none() {
            return Err(LogPoseError::Unavailable {
                message: format!(
                    "collection '{}/{}' has no authoritative shard ownership metadata and cannot accept writes until reconciliation completes",
                    descriptor.database_name, descriptor.name
                ),
                retry_after: Some(logpose_types::error::ROUTING_RETRY_AFTER),
            });
        }
        let placement = self.local_placement(
            &descriptor,
            &assignment,
            ownership.as_ref(),
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
        Err(LogPoseError::NotOwner {
            collection: format!("{}/{}", descriptor.database_name, descriptor.name),
            node: self.config.node_name.clone(),
            owner_node: Some(routed_node),
        })
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
            if coordination.is_local_leader {
                let lease_id = coordination.leadership_lease_id.ok_or_else(|| {
                    LogPoseError::internal(format!(
                        "node '{}' reported local leadership without a lease id",
                        self.config.node_name
                    ))
                })?;
                return Ok(Some(LeadershipFence {
                    node_id: self.config.node_name.clone(),
                    lease_id,
                }));
            }
            if coordination.leader_node.is_some() || Instant::now() >= deadline {
                return Err(LogPoseError::NotLeader {
                    node: self.config.node_name.clone(),
                    leader_node: coordination.leader_node,
                });
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
}

fn http_endpoint(host: &str, port: u16) -> String {
    let authority = match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]"),
        _ => host.to_owned(),
    };
    format!("http://{authority}:{port}")
}

fn ensure_collection_reference_matches_descriptor(
    reference: &CollectionRef,
    descriptor: &logpose_catalog::CollectionDescriptor,
    original_name: &str,
) -> logpose_types::Result<()> {
    if reference.database_name != descriptor.database_name
        || reference.collection_name != descriptor.name
    {
        return Err(LogPoseError::not_found(
            ResourceKind::Collection,
            original_name,
        ));
    }
    Ok(())
}

/// Report any missing resource on the way to a collection (its database, say) as the
/// collection the caller named.
fn qualify_collection_error(error: LogPoseError, collection_name: &str) -> LogPoseError {
    match error {
        LogPoseError::NotFound { .. } => {
            LogPoseError::not_found(ResourceKind::Collection, collection_name)
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
        CollectionStats, CommitAck, DistanceMetric, Snapshot, WriteOperation,
        legacy::record_from_put,
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
    fn parse_collection_reference_accepts_database_collection() {
        let reference = CollectionRef::parse("analytics/documents")
            .expect("database-qualified collection name should parse");

        assert_eq!(reference.database_name, "analytics");
        assert_eq!(reference.collection_name, "documents");
    }

    #[test]
    fn reconcile_coordination_snapshot_preserves_healthy_fields_with_pending_error() {
        let snapshot = RwLock::new(CoordinationStatus {
            cluster_name: "default".to_owned(),
            membership_registered: false,
            membership_lease_id: None,
            registered_members: Vec::new(),
            leader_node: None,
            is_local_leader: false,
            leadership_lease_id: None,
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
            Some("membership keep-alive failed".to_owned()),
        );

        let current = coordination_read(&snapshot).clone();
        assert!(current.membership_registered);
        assert_eq!(current.membership_lease_id, Some(11));
        assert_eq!(current.registered_members, vec!["node-a".to_owned()]);
        assert_eq!(current.leader_node.as_deref(), Some("node-a"));
        assert!(current.is_local_leader);
        assert_eq!(current.leadership_lease_id, Some(22));
        assert_eq!(
            current.last_error.as_deref(),
            Some("membership keep-alive failed")
        );
    }

    #[test]
    fn demote_lost_claims_clears_local_leadership_before_recampaigning() {
        let leading = CoordinationStatus {
            cluster_name: "default".to_owned(),
            membership_registered: true,
            membership_lease_id: Some(11),
            registered_members: vec!["node-a".to_owned(), "node-b".to_owned()],
            leader_node: Some("node-a".to_owned()),
            is_local_leader: true,
            leadership_lease_id: Some(22),
            last_error: None,
        };
        let lease = LeadershipLease {
            node_id: "node-a".to_owned(),
            lease_id: 22,
            key: "/leaders/node-a".to_owned(),
        };

        let snapshot = RwLock::new(leading.clone());
        demote_lost_claims(&snapshot, "node-a", Some(11), Some(&lease));
        assert_eq!(*coordination_read(&snapshot), leading);

        let snapshot = RwLock::new(leading.clone());
        demote_lost_claims(&snapshot, "node-a", Some(11), None);
        let current = coordination_read(&snapshot).clone();
        assert!(current.membership_registered);
        assert_eq!(current.membership_lease_id, Some(11));
        assert!(!current.is_local_leader);
        assert_eq!(current.leadership_lease_id, None);
        assert_eq!(current.leader_node, None);

        let snapshot = RwLock::new(leading);
        demote_lost_claims(&snapshot, "node-a", None, Some(&lease));
        let current = coordination_read(&snapshot).clone();
        assert!(!current.membership_registered);
        assert_eq!(current.membership_lease_id, None);
        assert_eq!(current.registered_members, vec!["node-b".to_owned()]);
        assert!(!current.is_local_leader);
        assert_eq!(current.leadership_lease_id, None);
    }

    #[tokio::test]
    async fn create_collection_uses_plain_storage_create_when_assignments_are_unsupported() {
        #[derive(Debug)]
        struct CreateOnlyStorageEngine {
            root: PathBuf,
            next_id: AtomicU64,
        }

        impl logpose_storage::CollectionReader for CreateOnlyStorageEngine {
            fn read_view<'a>(
                &'a self,
                collection: &'a CollectionRef,
                _options: logpose_storage::ReadOptions,
            ) -> logpose_storage::BoxFuture<'a, logpose_types::Result<logpose_storage::ReadView>>
            {
                Box::pin(async move {
                    Err(LogPoseError::not_found(
                        ResourceKind::Collection,
                        collection.lookup_name(),
                    ))
                })
            }
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
                    request.name().to_owned(),
                    request.spec.build_schema()?,
                    self.root.join(format!("collection-{suffix}")),
                ))
            }

            async fn open_collection(
                &self,
                name: &str,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                Err(LogPoseError::not_found(ResourceKind::Collection, name))
            }

            async fn write(
                &self,
                collection_name: &str,
                _operations: Vec<WriteOperation>,
            ) -> logpose_types::Result<CommitAck> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
            }

            async fn snapshot(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
            }

            async fn flush(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
            }

            async fn compact(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
            }

            async fn stats(&self, collection_name: &str) -> logpose_types::Result<CollectionStats> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
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
            .create_collection(CreateCollectionRequest::in_database(
                "default".to_owned(),
                "documents".to_owned(),
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("plain storage create should still succeed");

        assert_eq!(descriptor.name, "documents");
        assert_eq!(descriptor.schema.vectors()[0].dimensions, 2);
        assert_eq!(descriptor.schema.vectors()[0].metric, DistanceMetric::Dot);
    }

    #[tokio::test]
    async fn runtime_status_surfaces_metadata_unready_without_failing() {
        #[derive(Debug)]
        struct MetadataUnavailableStorageEngine;

        impl logpose_storage::CollectionReader for MetadataUnavailableStorageEngine {
            fn read_view<'a>(
                &'a self,
                collection: &'a CollectionRef,
                _options: logpose_storage::ReadOptions,
            ) -> logpose_storage::BoxFuture<'a, logpose_types::Result<logpose_storage::ReadView>>
            {
                Box::pin(async move {
                    Err(LogPoseError::unavailable(format!(
                        "metadata for '{}' is unavailable",
                        collection.lookup_name()
                    )))
                })
            }
        }

        #[async_trait]
        impl StorageEngine for MetadataUnavailableStorageEngine {
            async fn engine_name(&self) -> &'static str {
                "metadata-unavailable"
            }

            async fn metadata_status(&self) -> logpose_types::Result<()> {
                Err(LogPoseError::unavailable(
                    "etcd metadata operation failed: connection refused",
                ))
            }

            async fn create_collection(
                &self,
                _request: CreateCollectionRequest,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                Err(LogPoseError::internal("unsupported".to_owned()))
            }

            async fn open_collection(
                &self,
                name: &str,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                Err(LogPoseError::not_found(ResourceKind::Collection, name))
            }

            async fn list_collections(
                &self,
            ) -> logpose_types::Result<Vec<logpose_catalog::CollectionDescriptor>> {
                Err(LogPoseError::internal(
                    "list_collections should not run when metadata is unavailable".to_owned(),
                ))
            }

            async fn write(
                &self,
                collection_name: &str,
                _operations: Vec<WriteOperation>,
            ) -> logpose_types::Result<CommitAck> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
            }

            async fn snapshot(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
            }

            async fn flush(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
            }

            async fn compact(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
            }

            async fn stats(&self, collection_name: &str) -> logpose_types::Result<CollectionStats> {
                Err(LogPoseError::not_found(
                    ResourceKind::Collection,
                    collection_name,
                ))
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
            local_catalog_store(&catalog_root).expect("catalog store should open"),
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

    /// Stats behind a read barrier check the barrier against the current snapshot and then read
    /// the current state. A flush that lands between the two is not an error: the state it
    /// publishes is newer, so it satisfies the barrier too, while the checked snapshot's
    /// generation is no longer retained.
    #[tokio::test]
    async fn stats_behind_a_read_barrier_survive_a_flush_between_check_and_read() {
        /// A local engine that, once armed, writes and flushes right after it hands out a
        /// snapshot.
        struct FlushAfterSnapshot {
            inner: logpose_storage::LocalStorageEngine,
            armed: std::sync::atomic::AtomicBool,
        }

        impl logpose_storage::CollectionReader for FlushAfterSnapshot {
            fn read_view<'a>(
                &'a self,
                collection: &'a CollectionRef,
                options: logpose_storage::ReadOptions,
            ) -> logpose_storage::BoxFuture<'a, logpose_types::Result<logpose_storage::ReadView>>
            {
                self.inner.read_view(collection, options)
            }
        }

        #[async_trait]
        impl StorageEngine for FlushAfterSnapshot {
            async fn engine_name(&self) -> &'static str {
                "flush-after-snapshot"
            }

            async fn create_collection(
                &self,
                request: CreateCollectionRequest,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                self.inner.create_collection(request).await
            }

            async fn open_collection(
                &self,
                name: &str,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                self.inner.open_collection(name).await
            }

            async fn write(
                &self,
                collection_name: &str,
                operations: Vec<WriteOperation>,
            ) -> logpose_types::Result<CommitAck> {
                self.inner.write(collection_name, operations).await
            }

            async fn write_batch(
                &self,
                collection_name: &str,
                operations: Vec<ClientOp>,
            ) -> logpose_types::Result<CommitAck> {
                self.inner.write_batch(collection_name, operations).await
            }

            async fn snapshot(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                let snapshot = self.inner.snapshot(collection_name).await?;
                if self.armed.swap(false, Ordering::SeqCst) {
                    let put = WriteOperation::Put(logpose_types::PutRecord {
                        id: logpose_types::RecordId::new("late"),
                        vector: vec![0.0, 1.0],
                        metadata: json!({}),
                    });
                    self.inner.write(collection_name, vec![put]).await?;
                    self.inner.flush(collection_name).await?;
                }
                Ok(snapshot)
            }

            async fn flush(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                self.inner.flush(collection_name).await
            }

            async fn compact(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                self.inner.compact(collection_name).await
            }

            async fn stats(&self, collection_name: &str) -> logpose_types::Result<CollectionStats> {
                self.inner.stats(collection_name).await
            }

            async fn stats_descriptor(
                &self,
                descriptor: &logpose_catalog::CollectionDescriptor,
                snapshot: Option<Snapshot>,
            ) -> logpose_types::Result<CollectionStats> {
                self.inner.stats_descriptor(descriptor, snapshot).await
            }

            async fn inspect(
                &self,
                collection_name: &str,
                target: InspectTarget,
            ) -> logpose_types::Result<InspectReport> {
                self.inner.inspect(collection_name, target).await
            }
        }

        let root = std::env::temp_dir().join(format!(
            "logpose-service-barrier-flush-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        let storage = Arc::new(FlushAfterSnapshot {
            inner: logpose_storage::LocalStorageEngine::new(&root).expect("engine should open"),
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let service = LogPoseDataService::new(Arc::clone(&storage) as Arc<dyn StorageEngine>);
        service
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        let put = record_from_put(logpose_types::PutRecord {
            id: logpose_types::RecordId::new("first"),
            vector: vec![1.0, 0.0],
            metadata: json!({}),
        })
        .expect("record");
        let ack = service
            .upsert("documents", vec![put])
            .await
            .expect("write should succeed");

        storage.armed.store(true, Ordering::SeqCst);
        let stats = service
            .stats_for_read("documents", None, Some(ack.snapshot.clone()))
            .await
            .expect("a flush after the barrier check does not expire the read");
        assert!(stats.manifest_generation > ack.snapshot.manifest_generation);
        assert!(stats.visible_seq_no > ack.snapshot.visible_seq_no);
        assert_eq!(stats.live_record_count, 2);

        drop(service);
        drop(storage);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The runtime status sums each local collection's maintenance status (runtime state the
    /// engine keeps): collections with jobs waiting and how many, collections with a job
    /// running, and collections whose last job failed, telling apart collections of the same
    /// name in different databases.
    #[tokio::test]
    async fn runtime_status_aggregates_every_local_collections_maintenance_backlog() {
        /// A local engine that reports a chosen maintenance status per collection.
        struct FixedMaintenance {
            inner: LocalStorageEngine,
            statuses: std::collections::BTreeMap<String, MaintenanceStatus>,
        }

        impl logpose_storage::CollectionReader for FixedMaintenance {
            fn read_view<'a>(
                &'a self,
                collection: &'a CollectionRef,
                options: logpose_storage::ReadOptions,
            ) -> logpose_storage::BoxFuture<'a, logpose_types::Result<logpose_storage::ReadView>>
            {
                self.inner.read_view(collection, options)
            }
        }

        #[async_trait]
        impl StorageEngine for FixedMaintenance {
            async fn engine_name(&self) -> &'static str {
                "fixed-maintenance"
            }

            async fn create_collection(
                &self,
                request: CreateCollectionRequest,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                self.inner.create_collection(request).await
            }

            async fn create_collection_with_assignment(
                &self,
                request: CreateCollectionRequest,
                assignment: CollectionAssignment,
                leader_fence: Option<LeadershipFence>,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                self.inner
                    .create_collection_with_assignment(request, assignment, leader_fence)
                    .await
            }

            async fn open_collection(
                &self,
                name: &str,
            ) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
                self.inner.open_collection(name).await
            }

            async fn has_local_collection(&self, name: &str) -> logpose_types::Result<bool> {
                self.inner.has_local_collection(name).await
            }

            async fn local_collection_matches_descriptor(
                &self,
                descriptor: &logpose_catalog::CollectionDescriptor,
            ) -> logpose_types::Result<bool> {
                self.inner
                    .local_collection_matches_descriptor(descriptor)
                    .await
            }

            async fn list_collections(
                &self,
            ) -> logpose_types::Result<Vec<logpose_catalog::CollectionDescriptor>> {
                self.inner.list_collections().await
            }

            async fn collection_assignment_descriptor(
                &self,
                descriptor: &logpose_catalog::CollectionDescriptor,
            ) -> logpose_types::Result<CollectionAssignment> {
                self.inner
                    .collection_assignment_descriptor(descriptor)
                    .await
            }

            async fn write(
                &self,
                collection_name: &str,
                operations: Vec<WriteOperation>,
            ) -> logpose_types::Result<CommitAck> {
                self.inner.write(collection_name, operations).await
            }

            async fn snapshot(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                self.inner.snapshot(collection_name).await
            }

            async fn flush(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                self.inner.flush(collection_name).await
            }

            async fn compact(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
                self.inner.compact(collection_name).await
            }

            async fn stats(&self, collection_name: &str) -> logpose_types::Result<CollectionStats> {
                self.inner.stats(collection_name).await
            }

            async fn maintenance_status_descriptor(
                &self,
                descriptor: &logpose_catalog::CollectionDescriptor,
            ) -> logpose_types::Result<MaintenanceStatus> {
                Ok(self
                    .statuses
                    .get(&descriptor.lookup_name())
                    .cloned()
                    .unwrap_or_default())
            }

            async fn inspect(
                &self,
                collection_name: &str,
                target: InspectTarget,
            ) -> logpose_types::Result<InspectReport> {
                self.inner.inspect(collection_name, target).await
            }
        }

        let root = std::env::temp_dir().join(format!(
            "logpose-service-maintenance-backlog-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        let status =
            |pending: &[&str], in_progress: Option<&str>, error: Option<&str>| MaintenanceStatus {
                pending: pending.iter().map(|label| (*label).to_owned()).collect(),
                in_progress: in_progress.map(str::to_owned),
                last_error: error.map(|message| logpose_types::MaintenanceError {
                    job: "flush".to_owned(),
                    message: message.to_owned(),
                    failed_at_unix_ms: 0,
                    consecutive_failures: 1,
                }),
                completed_runs: 0,
            };
        let storage = Arc::new(FixedMaintenance {
            inner: LocalStorageEngine::new(root.join("data")).expect("engine should open"),
            statuses: [
                (
                    "default/documents".to_owned(),
                    status(&["flush", "compact"], None, Some("disk full")),
                ),
                (
                    "analytics/documents".to_owned(),
                    status(&["compact"], Some("flush"), None),
                ),
            ]
            .into_iter()
            .collect(),
        });
        let data = Arc::new(LogPoseDataService::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>
        ));
        let control = LogPoseControlService::new(
            data,
            local_catalog_store(root.join("catalog")).expect("catalog store should open"),
            LogPoseConfig::default(),
            BuildInfo::current(),
        );
        for database in ["default", "analytics"] {
            control
                .create_collection(CreateCollectionRequest::in_database(
                    database,
                    "documents",
                    2,
                    DistanceMetric::Dot,
                ))
                .await
                .expect("collection should be created");
        }

        let runtime = control
            .runtime_status()
            .await
            .expect("runtime status should load");
        assert_eq!(runtime.collection_count, 2);
        assert_eq!(runtime.maintenance.collections_with_pending, 2);
        assert_eq!(runtime.maintenance.pending_operations, 3);
        assert_eq!(runtime.maintenance.collections_in_progress, 1);
        assert_eq!(runtime.maintenance.collections_with_errors, 1);

        drop(control);
        drop(storage);
        let _ = std::fs::remove_dir_all(&root);
    }
}
