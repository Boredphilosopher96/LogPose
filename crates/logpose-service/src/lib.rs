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
    CollectionHandle, CollectionReader, CreateCollectionRequest, Engine, EngineConfig,
    InspectReport, InspectTarget, ReadOptions,
};
use logpose_storage_etcd::{
    EtcdCollectionCatalog, EtcdCoordinationClient, LeadershipLease, LeadershipRecord,
    LeaseKeepAlive, MembershipRecord, ShardOwnership,
};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, BuildInfo, CollectionAssignment, CollectionPlacement, CollectionRef,
    CollectionStats, CommitAck, CoordinationStatus, EtcdMetadataConfig, LeadershipFence,
    LogPoseError, MaintenanceBacklog, MaintenanceStatus, MetadataBackend, NodeRole,
    NodeRuntimeStatus, ResourceKind, Snapshot,
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

/// Where collection metadata lives.
#[derive(Clone)]
enum CollectionCatalog {
    /// The engine's own descriptors are authoritative (a single node).
    Local,
    /// Etcd is authoritative; the engine serves the collections this node owns.
    Etcd(EtcdCollectionCatalog),
}

/// The data plane: collection lifecycle, writes, reads, and maintenance over the storage
/// [`Engine`], with collection metadata from the engine itself or from etcd.
#[derive(Clone)]
pub struct LogPoseDataService {
    engine: Engine,
    catalog: CollectionCatalog,
}

impl fmt::Debug for LogPoseDataService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogPoseDataService")
            .field("engine", &self.engine)
            .field("catalog", &self.engine_name())
            .finish()
    }
}

impl LogPoseDataService {
    /// Serve `engine` with its own descriptors as the collection metadata (a single node).
    #[must_use]
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            catalog: CollectionCatalog::Local,
        }
    }

    /// Serve `engine` with collection metadata in etcd.
    ///
    /// # Errors
    ///
    /// An invalid etcd configuration.
    pub fn with_etcd(engine: Engine, config: EtcdMetadataConfig) -> Result<Self> {
        Ok(Self {
            catalog: CollectionCatalog::Etcd(EtcdCollectionCatalog::new(engine.clone(), config)?),
            engine,
        })
    }

    /// Open an engine on the local filesystem at `root`, with `logpose-query`'s resolver for
    /// filter writes, and serve it with local collection metadata.
    ///
    /// # Errors
    ///
    /// Another engine holds the storage root, or it cannot be opened.
    pub fn local(root: impl AsRef<Path>) -> Result<Self> {
        let engine = Engine::open_local(
            root,
            EngineConfig {
                resolver: Some(logpose_query::resolver()),
                ..EngineConfig::default()
            },
        )?;
        Ok(Self::new(engine))
    }

    /// The storage engine this service serves.
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Create a collection placed on this node as a data node.
    pub async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        self.create_collection_with_assignment(
            request,
            CollectionAssignment {
                assigned_node: ANONYMOUS_LOCAL_NODE_NAME.to_owned(),
                assigned_role: NodeRole::Data,
            },
            None,
        )
        .await
    }

    /// Create a collection with an explicit persisted placement assignment. With etcd
    /// metadata the create is fenced by `leader_fence`, which it requires.
    pub async fn create_collection_with_assignment(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
        leader_fence: Option<LeadershipFence>,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        match &self.catalog {
            CollectionCatalog::Local => {
                let descriptor = self.engine.plan_collection_descriptor(&request)?;
                self.engine
                    .create_collection(descriptor, Some(assignment))
                    .await
                    .map(|handle| handle.describe())
            }
            CollectionCatalog::Etcd(catalog) => {
                let leader_fence = leader_fence.ok_or_else(|| {
                    LogPoseError::internal(
                        "etcd-backed collection creation requires a control-plane leadership fence",
                    )
                })?;
                catalog
                    .create_collection(request, assignment, leader_fence)
                    .await
            }
        }
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
        match &self.catalog {
            CollectionCatalog::Local => self.engine.list_collections(),
            CollectionCatalog::Etcd(catalog) => catalog.list_collections().await,
        }
    }

    /// Load the persisted placement assignment for a descriptor.
    pub async fn collection_assignment_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        match &self.catalog {
            CollectionCatalog::Local => self
                .handle_for(descriptor)?
                .meta()
                .assignment
                .clone()
                .ok_or_else(|| {
                    LogPoseError::internal(format!(
                        "collection '{}' is missing placement metadata",
                        descriptor.name
                    ))
                }),
            CollectionCatalog::Etcd(catalog) => catalog.assignment(descriptor).await,
        }
    }

    /// Where collection metadata lives: `local`, or `local+etcd-metadata`.
    #[must_use]
    pub fn engine_name(&self) -> &'static str {
        match &self.catalog {
            CollectionCatalog::Local => "local",
            CollectionCatalog::Etcd(_) => "local+etcd-metadata",
        }
    }

    /// Verify whether the backing metadata authority is currently reachable.
    pub async fn metadata_status(&self) -> Result<()> {
        match &self.catalog {
            CollectionCatalog::Local => Ok(()),
            CollectionCatalog::Etcd(catalog) => catalog.metadata_status().await,
        }
    }

    /// Return whether this node's engine serves exactly the collection `descriptor` names (the
    /// same collection id).
    pub fn local_collection_matches_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<bool> {
        match self.engine.collection(&descriptor.collection_ref()) {
            Ok(handle) => Ok(handle.descriptor().matches_serving_identity(descriptor)),
            Err(LogPoseError::NotFound { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// List the collections of one database, each with its live schema.
    pub async fn list_collections_in_database(
        &self,
        database_name: &str,
    ) -> Result<Vec<logpose_catalog::CollectionDescriptor>> {
        let mut descriptors = self.list_collections().await?;
        descriptors.retain(|descriptor| descriptor.database_name == database_name);
        Ok(descriptors)
    }

    /// Drop a collection. With etcd metadata the drop is fenced by `leader_fence`, which it
    /// requires.
    pub async fn drop_collection(
        &self,
        collection_name: &str,
        leader_fence: Option<LeadershipFence>,
    ) -> Result<()> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        match &self.catalog {
            CollectionCatalog::Local => {
                self.engine
                    .drop_collection(&descriptor.collection_ref())
                    .await
            }
            CollectionCatalog::Etcd(catalog) => {
                let leader_fence = leader_fence.ok_or_else(|| {
                    LogPoseError::internal(
                        "etcd-backed collection drops require a control-plane leadership fence",
                    )
                })?;
                catalog
                    .drop_collection(&descriptor.lookup_name(), leader_fence)
                    .await
            }
        }
    }

    /// Change a collection's schema online and return the collection with its new schema.
    /// With etcd metadata the new schema is then published to the catalog, best effort: the
    /// engine's schema is authoritative, and describing the collection heals a stale catalog.
    pub async fn alter_collection(
        &self,
        collection_name: &str,
        change: SchemaChange,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        let handle = self.handle_for(&descriptor)?;
        handle.alter_schema(change).await?;
        if let CollectionCatalog::Etcd(catalog) = &self.catalog {
            let _ = catalog
                .publish_schema(&descriptor.lookup_name(), &*handle.schema()?)
                .await;
        }
        self.resolved_collection_descriptor(collection_name).await
    }

    /// The collection's live schema.
    pub async fn schema(&self, collection_name: &str) -> Result<Arc<CollectionSchema>> {
        self.handle(collection_name).await?.schema()
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

    /// Point lookups by primary key, projected to `output_fields` (every scalar field and
    /// `$extra` key, but no vector, when empty).
    pub async fn get_records(
        &self,
        collection_name: &str,
        keys: Vec<PrimaryKey>,
        output_fields: Vec<String>,
    ) -> Result<FetchedRecords> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        let view = self
            .engine
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
        self.handle(collection_name)
            .await?
            .write(ops)
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
        self.handle(collection_name)
            .await?
            .delete_by_filter(filter)
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
        let handle = self.handle(collection_name).await?;
        let schema = handle.schema()?;
        let patch = patch
            .validate(&schema)
            .map_err(|error| error.to_error("patch", None))?;
        let placeholder = match schema.primary_key_type() {
            logpose_types::schema::PrimaryKeyType::Int64 => PrimaryKey::Int64(0),
            logpose_types::schema::PrimaryKeyType::String => PrimaryKey::String("_".to_owned()),
        };
        handle
            .update_by_filter(filter, patch.into_update(placeholder))
            .await
            .map_err(|error| match &error {
                // Filter errors already name their node below `filter`.
                LogPoseError::InvalidArgument {
                    field: Some(field), ..
                } if field == "filter" || field.starts_with("filter.") => error,
                _ => error.with_field_prefix("patch"),
            })
    }

    /// Search a collection (or scan it in order): see [`logpose_query::query`].
    pub async fn query_collection(
        &self,
        collection_name: &str,
        request: QueryRequest,
    ) -> Result<WithSchema<QueryResponse>> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        logpose_query::query(&self.engine, &descriptor.collection_ref(), request)
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
        logpose_query::count_records(&self.engine, &descriptor.collection_ref(), request)
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
        logpose_query::scroll_records(&self.engine, &descriptor.collection_ref(), request)
            .await
            .map_err(Into::into)
    }

    /// Capture the current read snapshot.
    pub async fn snapshot(&self, collection_name: &str) -> Result<Snapshot> {
        self.handle(collection_name).await?.snapshot()
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

    /// Return collection-level stats for one exact snapshot or lower-bound read barrier. With
    /// a barrier the stats describe the current state, which must satisfy it: they are taken
    /// from one published state, so a flush in between cannot move them past the check.
    pub async fn stats_for_read(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        read_barrier: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        match (snapshot, read_barrier) {
            (Some(_), Some(_)) => Err(LogPoseError::invalid_field(
                "read_barrier",
                "snapshot and read_barrier cannot be provided together",
            )),
            (snapshot, None) => self.stats_descriptor(&descriptor, snapshot).await,
            (None, Some(read_barrier)) => {
                let stats = self.stats_descriptor(&descriptor, None).await?;
                let read = Snapshot {
                    manifest_generation: stats.manifest_generation,
                    visible_seq_no: stats.visible_seq_no,
                };
                if read.satisfies_read_barrier(&read_barrier) {
                    Ok(stats)
                } else {
                    Err(LogPoseError::ReadBarrierNotSatisfied {
                        collection: descriptor.lookup_name(),
                        required_manifest_generation: read_barrier.manifest_generation,
                        required_seq_no: read_barrier.visible_seq_no,
                        visible_manifest_generation: read.manifest_generation,
                        visible_seq_no: read.visible_seq_no,
                    })
                }
            }
        }
    }

    /// Return collection-level stats using a previously loaded descriptor.
    pub async fn stats_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        self.handle_for(descriptor)?.stats(snapshot)
    }

    /// The collection's maintenance status (runtime state) without reconstructing full stats.
    pub fn maintenance_status_descriptor(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        Ok(self.handle_for(descriptor)?.maintenance_status())
    }

    /// Flush the mutable delta to a new segment.
    pub async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        self.handle(collection_name).await?.flush().await
    }

    /// Compact immutable segments.
    pub async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        self.handle(collection_name).await?.compact().await
    }

    /// Inspect arbitrary operator-visible storage state.
    pub async fn inspect(
        &self,
        collection_name: &str,
        target: InspectTarget,
    ) -> Result<InspectReport> {
        self.handle(collection_name).await?.inspect(target).await
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

    /// The local handle serving the collection `collection_name` names.
    async fn handle(&self, collection_name: &str) -> Result<Arc<CollectionHandle>> {
        let descriptor = self.resolved_collection_descriptor(collection_name).await?;
        self.handle_for(&descriptor)
    }

    /// The local handle serving `descriptor`'s collection: the same collection id, not only
    /// the same name.
    fn handle_for(
        &self,
        descriptor: &logpose_catalog::CollectionDescriptor,
    ) -> Result<Arc<CollectionHandle>> {
        let reference = descriptor.collection_ref();
        let handle = self.engine.collection(&reference)?;
        if handle.meta().id != descriptor.collection_id {
            return Err(LogPoseError::not_found(
                ResourceKind::Collection,
                format!("{}/{}", reference.database_name, reference.collection_name),
            ));
        }
        Ok(handle)
    }

    async fn resolved_collection_descriptor(
        &self,
        collection_name: &str,
    ) -> Result<logpose_catalog::CollectionDescriptor> {
        let reference = CollectionRef::parse(collection_name)?;
        let descriptor = match &self.catalog {
            CollectionCatalog::Local => self
                .engine
                .collection(&reference)
                .map(|handle| handle.describe()),
            CollectionCatalog::Etcd(catalog) => catalog.describe(collection_name).await,
        }
        .map_err(|error| qualify_collection_error(error, collection_name))?;
        ensure_collection_reference_matches_descriptor(&reference, &descriptor, collection_name)?;
        Ok(descriptor)
    }
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
        let local_collection_available =
            self.data.local_collection_matches_descriptor(&descriptor)?;
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
            let local_collection_available =
                self.data.local_collection_matches_descriptor(descriptor)?;
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

        let maintenance = maintenance_backlog(
            &local_descriptors
                .into_iter()
                .map(|descriptor| self.data.maintenance_status_descriptor(descriptor))
                .collect::<Result<Vec<_>>>()?,
        );

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
            storage_engine: self.data.engine_name().to_owned(),
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
        let local_collection_available =
            self.data.local_collection_matches_descriptor(&descriptor)?;
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

/// The maintenance backlog of a node's local collections, from each one's maintenance status:
/// collections with jobs waiting and how many, collections with a job running, and collections
/// whose last job failed.
fn maintenance_backlog(statuses: &[MaintenanceStatus]) -> MaintenanceBacklog {
    let mut backlog = MaintenanceBacklog::default();
    for status in statuses {
        if !status.pending.is_empty() {
            backlog.collections_with_pending += 1;
            backlog.pending_operations += status.pending.len();
        }
        if status.in_progress.is_some() {
            backlog.collections_in_progress += 1;
        }
        if status.last_error.is_some() {
            backlog.collections_with_errors += 1;
        }
    }
    backlog
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
    use logpose_types::DistanceMetric;

    /// A fresh temp directory named `logpose-service-{label}-…`, removed when the returned
    /// guard drops, also when the test panics.
    fn temp_root(label: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("logpose-service-{label}-"))
            .tempdir()
            .expect("temp dir should be created")
    }

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

    /// With etcd metadata unreachable, the runtime status reports the node unready instead of
    /// failing, and lists no collections.
    #[tokio::test]
    async fn runtime_status_surfaces_metadata_unready_without_failing() {
        let root_dir = temp_root("metadata-unavailable");
        let root = root_dir.path().to_path_buf();
        let engine =
            Engine::open_local(&root, EngineConfig::default()).expect("engine should open");
        let data = Arc::new(
            LogPoseDataService::with_etcd(
                engine.clone(),
                EtcdMetadataConfig {
                    endpoints: vec!["http://127.0.0.1:1".to_owned()],
                    timeout_ms: 250,
                    ..EtcdMetadataConfig::default()
                },
            )
            .expect("the etcd catalog should build"),
        );
        let control = LogPoseControlService::new(
            data,
            Arc::new(engine),
            LogPoseConfig::default(),
            BuildInfo::current(),
        );

        let status = control
            .runtime_status()
            .await
            .expect("runtime status should still surface readiness state");

        assert!(!status.control_plane_ready);
        assert!(!status.data_plane_ready);
        assert_eq!(status.storage_engine, "local+etcd-metadata");
        assert_eq!(status.collection_count, 0);
        assert!(status.collections.is_empty());
        drop(control);
    }

    /// With etcd metadata, a collection create is fenced by the control-plane leader's lease:
    /// without a fence it is refused before it touches etcd or the engine.
    #[tokio::test]
    async fn etcd_metadata_refuses_an_unfenced_create() {
        let root_dir = temp_root("etcd-unfenced");
        let root = root_dir.path().to_path_buf();
        let engine =
            Engine::open_local(&root, EngineConfig::default()).expect("engine should open");
        let data = LogPoseDataService::with_etcd(
            engine.clone(),
            EtcdMetadataConfig {
                endpoints: vec!["http://127.0.0.1:1".to_owned()],
                timeout_ms: 250,
                ..EtcdMetadataConfig::default()
            },
        )
        .expect("the etcd catalog should build");

        let error = data
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect_err("an unfenced create is refused");
        assert!(error.to_string().contains("leadership fence"), "{error}");
        assert!(
            engine.list_collections().expect("list").is_empty(),
            "nothing was created locally"
        );
        drop((data, engine));
    }

    /// Stats behind a read barrier come from one published state that satisfies it: a newer
    /// state (here after a flush) passes, a barrier ahead of the collection fails, and a
    /// barrier together with an exact snapshot is refused.
    #[tokio::test]
    async fn stats_behind_a_read_barrier_describe_one_state_that_satisfies_it() {
        let root_dir = temp_root("barrier-stats");
        let root = root_dir.path().to_path_buf();
        let service = LogPoseDataService::local(&root).expect("service should open");
        service
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        let ack = service
            .upsert(
                "documents",
                vec![Record::new("first").with_vector("vector", vec![1.0, 0.0])],
            )
            .await
            .expect("write should succeed");
        service.flush("documents").await.expect("flush");
        service
            .upsert(
                "documents",
                vec![Record::new("second").with_vector("vector", vec![0.0, 1.0])],
            )
            .await
            .expect("write should succeed");

        let stats = service
            .stats_for_read("documents", None, Some(ack.snapshot.clone()))
            .await
            .expect("a newer state satisfies the barrier");
        assert!(stats.manifest_generation > ack.snapshot.manifest_generation);
        assert_eq!(stats.visible_seq_no, ack.snapshot.visible_seq_no + 1);
        assert_eq!(stats.live_record_count, 2);

        let ahead = Snapshot {
            manifest_generation: stats.manifest_generation,
            visible_seq_no: stats.visible_seq_no + 1,
        };
        let error = service
            .stats_for_read("documents", None, Some(ahead))
            .await
            .expect_err("a barrier ahead of the collection is not satisfied");
        assert!(
            matches!(error, LogPoseError::ReadBarrierNotSatisfied { .. }),
            "{error}"
        );
        let error = service
            .stats_for_read(
                "documents",
                Some(ack.snapshot.clone()),
                Some(ack.snapshot.clone()),
            )
            .await
            .expect_err("a snapshot and a barrier together are refused");
        assert!(error.to_string().contains("read_barrier"), "{error}");

        drop(service);
    }

    /// The runtime status sums each local collection's maintenance status: collections with
    /// jobs waiting and how many, collections with a job running, and collections whose last
    /// job failed.
    #[test]
    fn the_maintenance_backlog_sums_every_local_collections_status() {
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
        let backlog = maintenance_backlog(&[
            status(&["flush", "compact"], None, Some("disk full")),
            status(&["compact"], Some("flush"), None),
            status(&[], None, None),
        ]);
        assert_eq!(backlog.collections_with_pending, 2);
        assert_eq!(backlog.pending_operations, 3);
        assert_eq!(backlog.collections_in_progress, 1);
        assert_eq!(backlog.collections_with_errors, 1);
    }

    /// Poll `done` until it holds, failing after ten seconds.
    async fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            sleep(Duration::from_millis(5)).await;
        }
    }

    /// The runtime status covers every local collection, telling apart collections of the same
    /// name in different databases, and reads each one's maintenance status from its own
    /// handle: a flush waiting on one of them shows up once, and only for that collection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_status_reports_every_local_collection() {
        let root_dir = temp_root("runtime-status");
        let root = root_dir.path().to_path_buf();
        let data = Arc::new(LogPoseDataService::local(&root).expect("service should open"));
        let engine = data.engine().clone();
        let control = LogPoseControlService::new(
            Arc::clone(&data),
            Arc::new(engine.clone()),
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
        assert_eq!(runtime.storage_engine, "local");
        assert_eq!(runtime.collection_count, 2);
        assert_eq!(
            runtime
                .collections
                .iter()
                .map(|placement| placement.database_name.as_str())
                .collect::<Vec<_>>(),
            ["analytics", "default"]
        );
        assert_eq!(runtime.maintenance, MaintenanceBacklog::default());

        // Hold every maintenance permit, so an explicit flush of one collection waits.
        engine.scheduler().pause();
        data.upsert(
            "analytics/documents",
            vec![Record::new("first").with_vector("vector", vec![1.0, 0.0])],
        )
        .await
        .expect("write should succeed");
        let flush = tokio::spawn({
            let data = Arc::clone(&data);
            async move { data.flush("analytics/documents").await }
        });
        let analytics = engine
            .collection(&CollectionRef::new("analytics", "documents"))
            .expect("the collection is open");
        wait_until("the flush to wait for its permit", || {
            analytics.maintenance_status().pending == ["flush"]
        })
        .await;
        let waiting = analytics.maintenance_status();
        let runtime = control
            .runtime_status()
            .await
            .expect("runtime status should load");
        assert_eq!(runtime.maintenance.collections_with_pending, 1);
        assert_eq!(runtime.maintenance.pending_operations, 1);
        assert_eq!(
            runtime.maintenance.collections_in_progress,
            usize::from(waiting.in_progress.is_some())
        );
        assert_eq!(runtime.maintenance.collections_with_errors, 0);

        engine.scheduler().resume();
        flush
            .await
            .expect("the flush task should finish")
            .expect("the flush should succeed");
        let runtime = control
            .runtime_status()
            .await
            .expect("runtime status should load");
        assert_eq!(runtime.maintenance, MaintenanceBacklog::default());

        drop((control, analytics, data, engine));
    }

    /// A dropped collection is gone for every call, including calls holding its old
    /// descriptor, and its name can be reused: the new collection is a different one (another
    /// id) and starts empty.
    #[tokio::test]
    async fn a_dropped_collection_can_be_recreated_empty_under_the_same_name() {
        let root_dir = temp_root("drop-recreate");
        let root = root_dir.path().to_path_buf();
        let service = LogPoseDataService::local(&root).expect("service should open");
        let request = || CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot);
        let first = service
            .create_collection(request())
            .await
            .expect("collection should be created");
        service
            .upsert(
                "documents",
                vec![Record::new("old").with_vector("vector", vec![1.0, 0.0])],
            )
            .await
            .expect("write should succeed");
        service.flush("documents").await.expect("flush");

        service
            .drop_collection("documents", None)
            .await
            .expect("drop should succeed");
        for error in [
            service.get_collection("documents").await.map(|_| ()),
            service.stats("documents").await.map(|_| ()),
            service.stats_descriptor(&first, None).await.map(|_| ()),
            service.drop_collection("documents", None).await,
        ] {
            let error = error.expect_err("a dropped collection is gone");
            assert!(matches!(error, LogPoseError::NotFound { .. }), "{error}");
        }

        let second = service
            .create_collection(request())
            .await
            .expect("the name can be reused");
        assert_ne!(second.collection_id, first.collection_id);
        let error = service
            .stats_descriptor(&first, None)
            .await
            .expect_err("the old descriptor names the dropped collection, not the new one");
        assert!(matches!(error, LogPoseError::NotFound { .. }), "{error}");
        let stats = service.stats("documents").await.expect("stats");
        assert_eq!(stats.live_record_count, 0);
        assert_eq!(stats.segment_count, 0);
        let fetched = service
            .get_records("documents", vec![PrimaryKey::from("old")], Vec::new())
            .await
            .expect("get should succeed");
        assert_eq!(fetched.records, vec![None]);
        service
            .upsert(
                "documents",
                vec![Record::new("new").with_vector("vector", vec![0.0, 1.0])],
            )
            .await
            .expect("the new collection takes writes");

        drop(service);
    }

    /// A drop while writes are in flight waits for them: each write either commits before the
    /// drop or fails with `NotFound`, never with an unknown outcome, and a collection created
    /// again under the name holds none of them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_drop_during_writes_fails_them_cleanly() {
        let root_dir = temp_root("drop-during-writes");
        let root = root_dir.path().to_path_buf();
        let service = Arc::new(LogPoseDataService::local(&root).expect("service should open"));
        let request = || CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot);
        service
            .create_collection(request())
            .await
            .expect("collection should be created");
        let writers = (0..4)
            .map(|writer| {
                let service = Arc::clone(&service);
                tokio::spawn(async move {
                    let mut committed = 0_usize;
                    loop {
                        let record = Record::new(format!("w{writer}-{committed}"))
                            .with_vector("vector", vec![1.0, 0.0]);
                        match service.upsert("documents", vec![record]).await {
                            Ok(_) => committed += 1,
                            Err(error) => return (committed, error),
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        wait_until("the writers to commit", || {
            service
                .engine()
                .collection(&CollectionRef::new_default("documents"))
                .is_ok_and(|handle| handle.visible_seq_no() >= 20)
        })
        .await;

        service
            .drop_collection("documents", None)
            .await
            .expect("drop should succeed");
        let mut committed = 0;
        for writer in writers {
            let (count, error) = writer.await.expect("writer task");
            committed += count;
            assert!(matches!(error, LogPoseError::NotFound { .. }), "{error}");
        }
        assert!(committed >= 20, "the writers committed before the drop");

        service
            .create_collection(request())
            .await
            .expect("the name can be reused");
        let stats = service.stats("documents").await.expect("stats");
        assert_eq!(stats.live_record_count, 0);

        drop(service);
    }
}
