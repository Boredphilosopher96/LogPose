//! gRPC-backed client helpers for LogPose operator workflows.
//!
//! Server errors arrive as [`ClientError::Server`] with a decoded [`ServerError`]: the
//! [`ErrorReason`], code, metadata, field violations, and retry hint the server sent. Requests
//! are sent once unless the client is given a [`RetryPolicy`] or a [`RedirectPolicy`]; see
//! [`retry`] for what each retries and when.
//!
//! Every collection-scoped call names its collection with a [`CollectionRef`]: the database
//! and collection names the server requires on every such request.

mod error;
pub mod retry;
#[cfg(test)]
mod test_support;

pub use error::{ClientError, Result, ServerError, ServerErrorKind, grpc_code_name};
pub use logpose_types::{ErrorCode, ErrorReason, FieldViolation};
pub use retry::{NodeResolver, RedirectPolicy, RetryPolicy};

use logpose_api_grpc::convert::{
    collection_from_proto, create_request_to_proto, database_policy_to_proto,
    json_object_from_proto, metric_from_proto, primary_key_from_proto, primary_key_to_proto,
    record_from_proto, record_to_proto, schema_change_to_proto, snapshot_from_proto,
    snapshot_to_proto, update_to_proto,
};
use logpose_api_grpc::proto::{
    self, AlterCollectionRequest, CollectionPlacementReply, CommitAckReply,
    CompactCollectionRequest, CoordinationStatusReply, DeleteRecordsRequest, DropCollectionRequest,
    DropDatabaseRequest, FlushCollectionRequest, GetCollectionPlacementRequest,
    GetCollectionRequest, GetCollectionStatsRequest, GetDatabasePolicyRequest, GetDatabaseRequest,
    GetMetadataRequest, GetRecordsRequest, GetRuntimeStatusRequest, InspectCollectionRequest,
    ListCollectionsRequest, ListDatabasesRequest, MaintenanceBacklogReply,
    PutDatabasePolicyRequest, PutDatabaseRequest, QueryCollectionRequest, ScalarValue,
    SnapshotReply, UpdateRecordsRequest, UpsertRecordsRequest,
    log_pose_service_client::LogPoseServiceClient,
};
use logpose_auth::{AuthenticationMode, DatabaseRole};
pub use logpose_auth::{DatabaseAccessPolicy, DatabaseRoleBinding};
pub use logpose_catalog::{CollectionDescriptor, DatabaseDescriptor};
#[cfg(test)]
use logpose_config as _;
#[cfg(test)]
use logpose_core as _;
use logpose_query::{
    ExplainMode, FilterComparison, FilterExpr, FilterOperator, MetadataFilter, QueryDiagnostics,
    QueryMatch, QueryPlanKind, QueryRequest, QueryResponse, QueryStageTimings, ScalarMetadataValue,
};
pub use logpose_storage::{CreateCollectionRequest, InspectReport, InspectTarget};
use logpose_types::{
    CollectionId, CollectionPlacement, CollectionStats, CommitAck, CoordinationStatus,
    MaintenanceBacklog, MaintenanceStatus, NodeMetadata, NodeRole, NodeRuntimeStatus,
    QueryUnitStats, RecordId, ScalarFieldStats, Snapshot,
};
pub use logpose_types::{
    CollectionRef,
    record::{PartialUpdate, PrimaryKey, Record},
    schema::SchemaChange,
};
use retry::Operation;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::BTreeMap,
    future::Future,
    ops::Deref,
    sync::{Arc, Mutex, PoisonError},
};
use tonic::{
    Request, Response, Status,
    codegen::InterceptedService,
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
    transport::{Channel, Endpoint},
};

/// Client connection settings shared across tools and SDKs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    /// gRPC endpoint URL.
    pub grpc_endpoint: String,
    /// Optional bearer token attached to every gRPC request.
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            grpc_endpoint: "http://127.0.0.1:50051".to_owned(),
            auth_token: None,
        }
    }
}

/// Namespace-aware response wrapper for operations whose payload omits collection identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScopedCollectionResponse<T> {
    /// Database containing the collection.
    pub database_name: String,
    /// Collection name inside the database.
    pub collection_name: String,
    /// Operation payload.
    #[serde(flatten)]
    pub response: T,
}

impl<T> ScopedCollectionResponse<T> {
    /// Recover the collection reference attached to this response.
    #[must_use]
    pub fn collection(&self) -> CollectionRef {
        CollectionRef::new(self.database_name.clone(), self.collection_name.clone())
    }
}

impl<T> Deref for ScopedCollectionResponse<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

/// The result of a point lookup by primary key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordsResponse {
    /// The live records found, in request order, projected to the requested fields.
    pub records: Vec<Record>,
    /// The requested keys without a live record, in request order.
    pub missing_keys: Vec<PrimaryKey>,
    /// The state the lookup read.
    pub snapshot: Snapshot,
}

#[derive(Clone, Debug, Default)]
struct AuthInterceptor {
    authorization: Option<MetadataValue<Ascii>>,
}

impl AuthInterceptor {
    fn new(auth_token: Option<&str>) -> Result<Self> {
        let authorization = auth_token.map(bearer_metadata_value).transpose()?;
        Ok(Self { authorization })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: Request<()>,
    ) -> std::result::Result<Request<()>, tonic::Status> {
        if let Some(value) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        Ok(request)
    }
}

type ServiceClient = LogPoseServiceClient<InterceptedService<Channel, AuthInterceptor>>;

/// Thin gRPC client over the shared LogPose server contract.
///
/// Cloning is cheap: clones share the connection and the redirect connections.
#[derive(Clone)]
pub struct LogPoseClient {
    inner: ServiceClient,
    interceptor: AuthInterceptor,
    retry: RetryPolicy,
    redirects: Option<RedirectPolicy>,
    /// Lazily connected clients for redirect endpoints, by endpoint URL.
    peers: Arc<Mutex<BTreeMap<String, ServiceClient>>>,
}

impl LogPoseClient {
    /// Connect to a LogPose gRPC endpoint.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        Self::connect_with_auth(endpoint, None).await
    }

    /// Connect to a LogPose gRPC endpoint with an optional bearer token.
    pub async fn connect_with_auth(
        endpoint: impl Into<String>,
        auth_token: Option<&str>,
    ) -> Result<Self> {
        let channel = Endpoint::new(endpoint.into())?.connect().await?;
        let interceptor = AuthInterceptor::new(auth_token)?;
        Ok(Self {
            inner: LogPoseServiceClient::with_interceptor(channel, interceptor.clone()),
            interceptor,
            retry: RetryPolicy::disabled(),
            redirects: None,
            peers: Arc::default(),
        })
    }

    /// Connect using a shared client configuration.
    pub async fn from_config(config: &ClientConfig) -> Result<Self> {
        Self::connect_with_auth(config.grpc_endpoint.clone(), config.auth_token.as_deref()).await
    }

    /// Retry retryable server errors with `policy`. Without one, every request is sent once.
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// Follow `NOT_OWNER` and `NOT_LEADER` errors to the node they name with `policy`.
    /// Without one, those errors are returned.
    #[must_use]
    pub fn with_redirects(mut self, policy: RedirectPolicy) -> Self {
        self.redirects = Some(policy);
        self
    }

    /// The retry policy in effect.
    #[must_use]
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry
    }

    /// Send one unary request, following redirects and retrying as the policies allow.
    async fn call<Req, Reply, Fut>(
        &self,
        operation: Operation,
        request: Req,
        rpc: impl Fn(ServiceClient, Request<Req>) -> Fut,
    ) -> Result<Reply>
    where
        Req: Clone,
        Fut: Future<Output = std::result::Result<Response<Reply>, Status>>,
    {
        if !self.retry.applies_to(operation) && self.redirects.is_none() {
            return Ok(rpc(self.inner.clone(), Request::new(request))
                .await?
                .into_inner());
        }
        let mut target = self.inner.clone();
        let mut attempt = 1;
        let mut redirects = 0;
        loop {
            let status = match rpc(target.clone(), Request::new(request.clone())).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) => status,
            };
            let error = ServerError::from_status(status);
            if let Some(endpoint) = self
                .redirects
                .as_ref()
                .and_then(|policy| policy.target(redirects, &error))
            {
                target = self.peer(&endpoint)?;
                redirects += 1;
                continue;
            }
            let Some(delay) = self.retry.delay_before_retry(operation, attempt, &error) else {
                return Err(error.into());
            };
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    /// The client for a redirect endpoint, connected lazily on first use.
    fn peer(&self, endpoint: &str) -> Result<ServiceClient> {
        let mut peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(client) = peers.get(endpoint) {
            return Ok(client.clone());
        }
        let channel = Endpoint::new(endpoint.to_owned())?.connect_lazy();
        let client = LogPoseServiceClient::with_interceptor(channel, self.interceptor.clone());
        peers.insert(endpoint.to_owned(), client.clone());
        Ok(client)
    }

    /// Fetch canonical node metadata from the server.
    pub async fn metadata(&self) -> Result<NodeMetadata> {
        let response = self
            .call(
                Operation::Read,
                GetMetadataRequest {},
                |mut client, request| async move { client.get_metadata(request).await },
            )
            .await?;
        Ok(NodeMetadata {
            product: response.product,
            node_name: response.node_name,
            version: response.version,
            git_sha: response.git_sha,
            profile: response.profile,
        })
    }

    /// Fetch runtime and maintenance status from the control plane.
    pub async fn runtime_status(&self) -> Result<NodeRuntimeStatus> {
        let response = self
            .call(
                Operation::Read,
                GetRuntimeStatusRequest {},
                |mut client, request| async move { client.get_runtime_status(request).await },
            )
            .await?;
        runtime_status_from_proto(response)
    }

    /// Create a database if it does not exist, and return its descriptor.
    pub async fn put_database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        let response = self
            .call(
                Operation::Write,
                PutDatabaseRequest {
                    database_name: database_name.to_owned(),
                },
                |mut client, request| async move { client.put_database(request).await },
            )
            .await?;
        database_descriptor_from_proto(response)
    }

    /// Read one database descriptor.
    pub async fn database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        let response = self
            .call(
                Operation::Read,
                GetDatabaseRequest {
                    database_name: database_name.to_owned(),
                },
                |mut client, request| async move { client.get_database(request).await },
            )
            .await?;
        database_descriptor_from_proto(response)
    }

    /// List every database descriptor.
    pub async fn databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        let response = self
            .call(
                Operation::Read,
                ListDatabasesRequest {},
                |mut client, request| async move { client.list_databases(request).await },
            )
            .await?;
        response
            .databases
            .into_iter()
            .map(database_descriptor_from_proto)
            .collect()
    }

    /// Drop an empty database and its access policy.
    pub async fn drop_database(&self, database_name: &str) -> Result<()> {
        self.call(
            Operation::Write,
            DropDatabaseRequest {
                database_name: database_name.to_owned(),
            },
            |mut client, request| async move { client.drop_database(request).await },
        )
        .await?;
        Ok(())
    }

    /// Create or replace one database access policy.
    pub async fn set_database_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        let reply = database_policy_to_proto(policy);
        let response = self
            .call(
                Operation::Write,
                PutDatabasePolicyRequest {
                    database_name: reply.database_name,
                    authentication_mode: reply.authentication_mode,
                    role_bindings: reply.role_bindings,
                },
                |mut client, request| async move { client.put_database_policy(request).await },
            )
            .await?;
        database_policy_from_proto(response)
    }

    /// Read one database access policy.
    pub async fn database_policy(&self, database_name: &str) -> Result<DatabaseAccessPolicy> {
        let response = self
            .call(
                Operation::Read,
                GetDatabasePolicyRequest {
                    database_name: database_name.to_owned(),
                },
                |mut client, request| async move { client.get_database_policy(request).await },
            )
            .await?;
        database_policy_from_proto(response)
    }

    /// Create a collection with a typed schema.
    pub async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        let response = self
            .call(
                Operation::Write,
                create_request_to_proto(request.database_name, request.spec),
                |mut client, request| async move { client.create_collection(request).await },
            )
            .await?;
        Ok(collection_from_proto(response)?)
    }

    /// Fetch a collection with its live schema.
    pub async fn collection(&self, collection: &CollectionRef) -> Result<CollectionDescriptor> {
        let response = self
            .call(
                Operation::Read,
                GetCollectionRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                },
                |mut client, request| async move { client.get_collection(request).await },
            )
            .await?;
        Ok(collection_from_proto(response)?)
    }

    /// List the collections of one database, each with its live schema.
    pub async fn collections(&self, database_name: &str) -> Result<Vec<CollectionDescriptor>> {
        let response = self
            .call(
                Operation::Read,
                ListCollectionsRequest {
                    database_name: database_name.to_owned(),
                },
                |mut client, request| async move { client.list_collections(request).await },
            )
            .await?;
        response
            .collections
            .into_iter()
            .map(|collection| collection_from_proto(collection).map_err(Into::into))
            .collect()
    }

    /// Change a collection's schema online and return the collection with its new schema.
    pub async fn alter_collection(
        &self,
        collection: &CollectionRef,
        change: SchemaChange,
    ) -> Result<CollectionDescriptor> {
        let response = self
            .call(
                Operation::Write,
                AlterCollectionRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                    change: Some(schema_change_to_proto(change)),
                },
                |mut client, request| async move { client.alter_collection(request).await },
            )
            .await?;
        Ok(collection_from_proto(response)?)
    }

    /// Drop a collection and its data.
    pub async fn drop_collection(&self, collection: &CollectionRef) -> Result<()> {
        self.call(
            Operation::Write,
            DropCollectionRequest {
                database_name: collection.database_name.clone(),
                collection_name: collection.collection_name.clone(),
            },
            |mut client, request| async move { client.drop_collection(request).await },
        )
        .await?;
        Ok(())
    }

    /// Fetch placement metadata for one collection.
    pub async fn collection_placement(
        &self,
        collection: &CollectionRef,
    ) -> Result<CollectionPlacement> {
        let response = self
            .call(
                Operation::Read,
                GetCollectionPlacementRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                },
                |mut client, request| async move { client.get_collection_placement(request).await },
            )
            .await?;
        collection_placement_from_proto(response)
    }

    /// Insert or replace whole records as one atomic batch.
    pub async fn upsert(
        &self,
        collection: &CollectionRef,
        records: Vec<Record>,
    ) -> Result<ScopedCollectionResponse<CommitAck>> {
        let response = self
            .call(
                Operation::Write,
                UpsertRecordsRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                    records: records.into_iter().map(record_to_proto).collect(),
                },
                |mut client, request| async move { client.upsert_records(request).await },
            )
            .await?;
        commit_ack_from_proto(response)
    }

    /// Change some fields of existing records as one atomic batch. A key without a live
    /// record fails the batch with `NOT_FOUND`.
    pub async fn update(
        &self,
        collection: &CollectionRef,
        updates: Vec<PartialUpdate>,
    ) -> Result<ScopedCollectionResponse<CommitAck>> {
        let response = self
            .call(
                Operation::Write,
                UpdateRecordsRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                    records: updates.into_iter().map(update_to_proto).collect(),
                },
                |mut client, request| async move { client.update_records(request).await },
            )
            .await?;
        commit_ack_from_proto(response)
    }

    /// Delete records by primary key as one atomic batch. Missing keys are no-ops.
    pub async fn delete(
        &self,
        collection: &CollectionRef,
        keys: Vec<PrimaryKey>,
    ) -> Result<ScopedCollectionResponse<CommitAck>> {
        let response = self
            .call(
                Operation::Write,
                DeleteRecordsRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                    keys: keys.into_iter().map(primary_key_to_proto).collect(),
                },
                |mut client, request| async move { client.delete_records(request).await },
            )
            .await?;
        commit_ack_from_proto(response)
    }

    /// Read records by primary key, projected to `output_fields` (every field when empty).
    pub async fn get(
        &self,
        collection: &CollectionRef,
        keys: Vec<PrimaryKey>,
        output_fields: Vec<String>,
    ) -> Result<ScopedCollectionResponse<RecordsResponse>> {
        let response = self
            .call(
                Operation::Read,
                GetRecordsRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                    keys: keys.into_iter().map(primary_key_to_proto).collect(),
                    output_fields,
                },
                |mut client, request| async move { client.get_records(request).await },
            )
            .await?;
        Ok(ScopedCollectionResponse {
            database_name: response.database_name,
            collection_name: response.collection_name,
            response: RecordsResponse {
                records: response
                    .records
                    .into_iter()
                    .enumerate()
                    .map(|(index, record)| record_from_proto(record, &format!("records[{index}]")))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
                missing_keys: response
                    .missing_keys
                    .into_iter()
                    .enumerate()
                    .map(|(index, key)| {
                        primary_key_from_proto(Some(key), &format!("missing_keys[{index}]"))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?,
                snapshot: response.snapshot.map(snapshot_from_proto).ok_or_else(|| {
                    ClientError::InvalidResponse("get response missing snapshot".to_owned())
                })?,
            },
        })
    }

    /// Search a collection's first vector field. `request.collection_name` names the
    /// collection as `database/collection`.
    pub async fn query(
        &self,
        request: QueryRequest,
    ) -> Result<ScopedCollectionResponse<QueryResponse>> {
        validate_read_constraints(request.snapshot.as_ref(), request.read_barrier.as_ref())?;
        let collection = CollectionRef::parse(&request.collection_name)
            .map_err(|error| ClientError::InvalidRequest(error.to_string()))?;
        let response = self
            .call(
                Operation::Read,
                QueryCollectionRequest {
                    database_name: collection.database_name,
                    collection_name: collection.collection_name,
                    vector: request.vector,
                    top_k: request.top_k as u64,
                    snapshot: request.snapshot.map(snapshot_to_proto),
                    read_barrier: request.read_barrier.map(snapshot_to_proto),
                    filters: request
                        .filters
                        .into_iter()
                        .map(metadata_filter_to_proto)
                        .collect::<Result<Vec<_>>>()?,
                    predicate: request.predicate.map(predicate_to_proto).transpose()?,
                    explain: explain_mode_to_proto(request.explain) as i32,
                    snapshot_token: request.snapshot_token.unwrap_or_default(),
                    pin: request.pin,
                },
                |mut client, request| async move { client.query_collection(request).await },
            )
            .await?;

        Ok(ScopedCollectionResponse {
            database_name: response.database_name,
            collection_name: response.collection_name,
            response: QueryResponse {
                metric: metric_from_proto(response.metric, "metric")?,
                top_k: response.top_k as usize,
                returned: response.returned as usize,
                snapshot: response.snapshot.map(snapshot_from_proto).ok_or_else(|| {
                    ClientError::InvalidResponse("query response missing snapshot".to_owned())
                })?,
                matches: response
                    .matches
                    .into_iter()
                    .map(query_match_from_proto)
                    .collect::<Result<Vec<_>>>()?,
                diagnostics: response
                    .diagnostics
                    .map(query_diagnostics_from_proto)
                    .transpose()?,
                snapshot_token: (!response.snapshot_token.is_empty())
                    .then_some(response.snapshot_token),
            },
        })
    }

    /// Fetch collection-level statistics, at an exact snapshot or behind a read barrier when
    /// one is given.
    pub async fn stats(
        &self,
        collection: &CollectionRef,
        snapshot: Option<Snapshot>,
        read_barrier: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        validate_read_constraints(snapshot.as_ref(), read_barrier.as_ref())?;
        let response = self
            .call(
                Operation::Read,
                GetCollectionStatsRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                    snapshot: snapshot.map(snapshot_to_proto),
                    read_barrier: read_barrier.map(snapshot_to_proto),
                },
                |mut client, request| async move { client.get_collection_stats(request).await },
            )
            .await?;
        Ok(CollectionStats {
            collection_id: parse_collection_id(&response.collection_id)?,
            database_name: response.database_name,
            collection_name: response.collection_name,
            manifest_generation: response.manifest_generation,
            visible_seq_no: response.visible_seq_no,
            mutable_op_count: response.mutable_op_count as usize,
            segment_count: response.segment_count as usize,
            live_record_count: response.live_record_count as usize,
            deleted_record_count: response.deleted_record_count as usize,
            maintenance: response
                .maintenance
                .map(maintenance_status_from_proto)
                .transpose()?
                .unwrap_or_default(),
            query_units: response
                .query_units
                .into_iter()
                .map(query_unit_stats_from_proto)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    /// Flush the mutable delta into a new segment.
    pub async fn flush(
        &self,
        collection: &CollectionRef,
    ) -> Result<ScopedCollectionResponse<Snapshot>> {
        let response = self
            .call(
                Operation::Write,
                FlushCollectionRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                },
                |mut client, request| async move { client.flush_collection(request).await },
            )
            .await?;
        Ok(snapshot_reply_from_proto(response))
    }

    /// Compact immutable segments.
    pub async fn compact(
        &self,
        collection: &CollectionRef,
    ) -> Result<ScopedCollectionResponse<Snapshot>> {
        let response = self
            .call(
                Operation::Write,
                CompactCollectionRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                },
                |mut client, request| async move { client.compact_collection(request).await },
            )
            .await?;
        Ok(snapshot_reply_from_proto(response))
    }

    /// Inspect operator-visible storage state.
    pub async fn inspect(
        &self,
        collection: &CollectionRef,
        target: InspectTarget,
    ) -> Result<ScopedCollectionResponse<InspectReport>> {
        let response = self
            .call(
                Operation::Read,
                InspectCollectionRequest {
                    database_name: collection.database_name.clone(),
                    collection_name: collection.collection_name.clone(),
                    target: inspect_target_to_proto(&target) as i32,
                    segment_id: inspect_segment_id(&target),
                },
                |mut client, request| async move { client.inspect_collection(request).await },
            )
            .await?;
        Ok(ScopedCollectionResponse {
            database_name: response.database_name,
            collection_name: response.collection_name,
            response: InspectReport {
                target: response.target,
                payload: serde_json::from_str(&response.payload_json)?,
            },
        })
    }
}

fn validate_read_constraints(
    snapshot: Option<&Snapshot>,
    read_barrier: Option<&Snapshot>,
) -> Result<()> {
    if snapshot.is_some() && read_barrier.is_some() {
        return Err(ClientError::InvalidRequest(
            "snapshot and read_barrier cannot be provided together".to_owned(),
        ));
    }
    Ok(())
}

fn bearer_metadata_value(token: &str) -> Result<MetadataValue<Ascii>> {
    let token = token.trim();
    if token.is_empty() {
        return Err(ClientError::InvalidAuthToken(
            "client auth token must not be empty".to_owned(),
        ));
    }
    MetadataValue::try_from(format!("Bearer {token}")).map_err(|error| {
        ClientError::InvalidAuthToken(format!(
            "client auth token could not be encoded as authorization metadata: {error}"
        ))
    })
}

fn database_descriptor_from_proto(
    reply: proto::DatabaseDescriptorReply,
) -> Result<DatabaseDescriptor> {
    Ok(DatabaseDescriptor {
        database_id: reply.database_id.parse()?,
        name: reply.name,
        is_default: reply.is_default,
    })
}

fn database_policy_from_proto(
    reply: proto::DatabaseAccessPolicyReply,
) -> Result<DatabaseAccessPolicy> {
    let database_name = reply.database_name;
    Ok(DatabaseAccessPolicy {
        authentication_mode: authentication_mode_from_proto(reply.authentication_mode)?,
        role_bindings: reply
            .role_bindings
            .into_iter()
            .map(|binding| {
                Ok(DatabaseRoleBinding {
                    database_name: database_name.clone(),
                    principal_name: binding.principal_name,
                    role: database_role_from_proto(binding.role)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        database_name,
    })
}

fn commit_ack_from_proto(reply: CommitAckReply) -> Result<ScopedCollectionResponse<CommitAck>> {
    Ok(ScopedCollectionResponse {
        database_name: reply.database_name,
        collection_name: reply.collection_name,
        response: CommitAck {
            last_seq_no: reply.last_seq_no,
            applied_ops: reply.applied_ops as usize,
            snapshot: reply.snapshot.map(snapshot_from_proto).ok_or_else(|| {
                ClientError::InvalidResponse("write response missing write snapshot".to_owned())
            })?,
        },
    })
}

fn snapshot_reply_from_proto(reply: SnapshotReply) -> ScopedCollectionResponse<Snapshot> {
    ScopedCollectionResponse {
        database_name: reply.database_name,
        collection_name: reply.collection_name,
        response: Snapshot {
            manifest_generation: reply.manifest_generation,
            visible_seq_no: reply.visible_seq_no,
        },
    }
}

fn query_match_from_proto(candidate: proto::QueryMatch) -> Result<QueryMatch> {
    let metadata = match candidate.metadata {
        Some(metadata) => JsonValue::Object(json_object_from_proto(metadata, "metadata")?),
        None => JsonValue::Object(serde_json::Map::new()),
    };
    Ok(QueryMatch {
        id: RecordId::new(candidate.id),
        value: candidate.value,
        metadata,
    })
}

fn runtime_status_from_proto(reply: proto::GetRuntimeStatusReply) -> Result<NodeRuntimeStatus> {
    let metadata = reply.metadata.ok_or_else(|| {
        ClientError::InvalidResponse("runtime status missing metadata".to_owned())
    })?;

    Ok(NodeRuntimeStatus {
        metadata: NodeMetadata {
            product: metadata.product,
            node_name: metadata.node_name,
            version: metadata.version,
            git_sha: metadata.git_sha,
            profile: metadata.profile,
        },
        role: node_role_from_proto(reply.role)?,
        rest_endpoint: reply.rest_endpoint,
        grpc_endpoint: reply.grpc_endpoint,
        storage_engine: reply.storage_engine,
        control_plane_ready: reply.control_plane_ready,
        data_plane_ready: reply.data_plane_ready,
        collection_count: reply.collection_count as usize,
        collections: reply
            .collections
            .into_iter()
            .map(collection_placement_from_proto)
            .collect::<Result<Vec<_>>>()?,
        coordination: reply
            .coordination
            .map(coordination_status_from_proto)
            .transpose()?,
        maintenance: maintenance_backlog_from_proto(reply.maintenance.ok_or_else(|| {
            ClientError::InvalidResponse("runtime status missing maintenance".to_owned())
        })?),
    })
}

fn coordination_status_from_proto(reply: CoordinationStatusReply) -> Result<CoordinationStatus> {
    Ok(CoordinationStatus {
        cluster_name: reply.cluster_name,
        membership_registered: reply.membership_registered,
        membership_lease_id: reply.membership_lease_id,
        registered_members: reply.registered_members,
        leader_node: reply.leader_node,
        is_local_leader: reply.is_local_leader,
        leadership_lease_id: reply.leadership_lease_id,
        last_error: reply.last_error,
    })
}

fn collection_placement_from_proto(reply: CollectionPlacementReply) -> Result<CollectionPlacement> {
    Ok(CollectionPlacement {
        collection_id: parse_collection_id(&reply.collection_id)?,
        database_name: reply.database_name,
        collection_name: reply.collection_name,
        assigned_node: reply.assigned_node,
        assigned_role: node_role_from_proto(reply.assigned_role)?,
        owner_node: reply.owner_node,
        ownership_epoch: reply.ownership_epoch,
        route_kind: reply.route_kind,
        route_reason: reply.route_reason,
    })
}

fn parse_collection_id(value: &str) -> Result<CollectionId> {
    value.parse().map(CollectionId).map_err(|error| {
        ClientError::InvalidResponse(format!("invalid collection id '{value}': {error}"))
    })
}

fn node_role_from_proto(role: i32) -> Result<NodeRole> {
    match proto::NodeRole::try_from(role)
        .map_err(|_| ClientError::InvalidResponse(format!("unknown node role '{role}'")))?
    {
        proto::NodeRole::Unspecified => Err(ClientError::InvalidResponse(
            "node role must be set".to_owned(),
        )),
        proto::NodeRole::Combined => Ok(NodeRole::Combined),
        proto::NodeRole::Control => Ok(NodeRole::Control),
        proto::NodeRole::Data => Ok(NodeRole::Data),
    }
}

fn authentication_mode_from_proto(mode: i32) -> Result<AuthenticationMode> {
    match proto::AuthenticationMode::try_from(mode).map_err(|_| {
        ClientError::InvalidResponse(format!("unknown authentication mode '{mode}'"))
    })? {
        proto::AuthenticationMode::Disabled => Ok(AuthenticationMode::Disabled),
        proto::AuthenticationMode::Password => Ok(AuthenticationMode::Password),
        proto::AuthenticationMode::MutualTls => Ok(AuthenticationMode::MutualTls),
        proto::AuthenticationMode::ExternalToken => Ok(AuthenticationMode::ExternalToken),
        proto::AuthenticationMode::Unspecified => Err(ClientError::InvalidResponse(
            "authentication mode must be set".to_owned(),
        )),
    }
}

fn database_role_from_proto(role: i32) -> Result<DatabaseRole> {
    match proto::DatabaseRole::try_from(role)
        .map_err(|_| ClientError::InvalidResponse(format!("unknown database role '{role}'")))?
    {
        proto::DatabaseRole::Owner => Ok(DatabaseRole::Owner),
        proto::DatabaseRole::ReadWrite => Ok(DatabaseRole::ReadWrite),
        proto::DatabaseRole::ReadOnly => Ok(DatabaseRole::ReadOnly),
        proto::DatabaseRole::Unspecified => Err(ClientError::InvalidResponse(
            "database role must be set".to_owned(),
        )),
    }
}

fn metadata_filter_to_proto(filter: MetadataFilter) -> Result<proto::MetadataFilter> {
    Ok(proto::MetadataFilter {
        field: filter.field,
        value: Some(scalar_value_to_proto(filter.value)?),
    })
}

fn predicate_to_proto(predicate: FilterExpr) -> Result<proto::Predicate> {
    let node = match predicate {
        FilterExpr::And { children } => proto::predicate::Node::And(proto::PredicateList {
            children: children
                .into_iter()
                .map(predicate_to_proto)
                .collect::<Result<Vec<_>>>()?,
        }),
        FilterExpr::Or { children } => proto::predicate::Node::Or(proto::PredicateList {
            children: children
                .into_iter()
                .map(predicate_to_proto)
                .collect::<Result<Vec<_>>>()?,
        }),
        FilterExpr::Not { child } => proto::predicate::Node::Not(Box::new(proto::PredicateNot {
            child: Some(Box::new(predicate_to_proto(*child)?)),
        })),
        FilterExpr::Comparison(comparison) => {
            proto::predicate::Node::Comparison(predicate_comparison_to_proto(comparison)?)
        }
    };
    Ok(proto::Predicate { node: Some(node) })
}

fn predicate_comparison_to_proto(
    comparison: FilterComparison,
) -> Result<proto::PredicateComparison> {
    Ok(proto::PredicateComparison {
        field: comparison.field,
        operator: predicate_operator_to_proto(comparison.operator) as i32,
        value: comparison.value.map(scalar_value_to_proto).transpose()?,
    })
}

fn predicate_operator_to_proto(operator: FilterOperator) -> proto::PredicateOperator {
    match operator {
        FilterOperator::Eq => proto::PredicateOperator::Eq,
        FilterOperator::Ne => proto::PredicateOperator::Ne,
        FilterOperator::Lt => proto::PredicateOperator::Lt,
        FilterOperator::Lte => proto::PredicateOperator::Lte,
        FilterOperator::Gt => proto::PredicateOperator::Gt,
        FilterOperator::Gte => proto::PredicateOperator::Gte,
        FilterOperator::Exists => proto::PredicateOperator::Exists,
        FilterOperator::IsNull => proto::PredicateOperator::IsNull,
    }
}

fn explain_mode_to_proto(mode: ExplainMode) -> proto::ExplainMode {
    match mode {
        ExplainMode::None => proto::ExplainMode::None,
        ExplainMode::Plan => proto::ExplainMode::Plan,
        ExplainMode::Profile => proto::ExplainMode::Profile,
    }
}

fn scalar_value_to_proto(value: ScalarMetadataValue) -> Result<ScalarValue> {
    let kind = match value {
        ScalarMetadataValue::String(value) => proto::scalar_value::Kind::StringValue(value),
        ScalarMetadataValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                proto::scalar_value::Kind::Int64Value(value)
            } else if let Some(value) = value.as_u64() {
                proto::scalar_value::Kind::Uint64Value(value)
            } else if let Some(value) = value.as_f64() {
                proto::scalar_value::Kind::DoubleValue(value)
            } else {
                return Err(ClientError::InvalidResponse(
                    "numeric scalar value must be finite".to_owned(),
                ));
            }
        }
        ScalarMetadataValue::Bool(value) => proto::scalar_value::Kind::BoolValue(value),
        ScalarMetadataValue::Null => proto::scalar_value::Kind::NullValue(true),
    };

    Ok(ScalarValue { kind: Some(kind) })
}

fn scalar_value_from_proto(value: ScalarValue) -> Result<ScalarMetadataValue> {
    match value.kind {
        Some(proto::scalar_value::Kind::StringValue(value)) => {
            Ok(ScalarMetadataValue::String(value))
        }
        Some(proto::scalar_value::Kind::Int64Value(value)) => {
            Ok(ScalarMetadataValue::Number(value.into()))
        }
        Some(proto::scalar_value::Kind::Uint64Value(value)) => {
            Ok(ScalarMetadataValue::Number(value.into()))
        }
        Some(proto::scalar_value::Kind::DoubleValue(value)) => serde_json::Number::from_f64(value)
            .map(ScalarMetadataValue::Number)
            .ok_or_else(|| {
                ClientError::InvalidResponse("numeric scalar value must be finite".to_owned())
            }),
        Some(proto::scalar_value::Kind::BoolValue(value)) => Ok(ScalarMetadataValue::Bool(value)),
        Some(proto::scalar_value::Kind::NullValue(_)) => Ok(ScalarMetadataValue::Null),
        None => Err(ClientError::InvalidResponse(
            "scalar value kind is required".to_owned(),
        )),
    }
}

fn query_diagnostics_from_proto(diagnostics: proto::QueryDiagnostics) -> Result<QueryDiagnostics> {
    Ok(QueryDiagnostics {
        chosen_plan: query_plan_kind_from_proto(diagnostics.chosen_plan)?,
        planner_reason: diagnostics.planner_reason,
        estimated_selectivity: diagnostics.estimated_selectivity,
        units_considered: diagnostics.units_considered as usize,
        units_pruned: diagnostics.units_pruned as usize,
        units_scanned: diagnostics.units_scanned as usize,
        candidates_before_filter: diagnostics.candidates_before_filter as usize,
        candidates_after_filter: diagnostics.candidates_after_filter as usize,
        candidates_reranked: diagnostics.candidates_reranked as usize,
        candidates_merged: diagnostics.candidates_merged as usize,
        rerank_count: diagnostics.rerank_count as usize,
        fallback_reason: diagnostics.fallback_reason,
        unit_scan_mix: diagnostics
            .unit_scan_mix
            .into_iter()
            .map(|(key, value)| (key, value as usize))
            .collect(),
        stage_timings: diagnostics
            .stage_timings
            .map(query_stage_timings_from_proto),
    })
}

fn query_plan_kind_from_proto(kind: i32) -> Result<QueryPlanKind> {
    match proto::QueryPlanKind::try_from(kind)
        .map_err(|_| ClientError::InvalidResponse(format!("unknown query plan kind '{kind}'")))?
    {
        proto::QueryPlanKind::Unspecified => Err(ClientError::InvalidResponse(
            "query plan kind must be set".to_owned(),
        )),
        proto::QueryPlanKind::UnfilteredExactScan => Ok(QueryPlanKind::UnfilteredExactScan),
        proto::QueryPlanKind::PredicateFirstExact => Ok(QueryPlanKind::PredicateFirstExact),
        proto::QueryPlanKind::VectorFirstExact => Ok(QueryPlanKind::VectorFirstExact),
        proto::QueryPlanKind::TinyPopulationExactFallback => {
            Ok(QueryPlanKind::TinyPopulationExactFallback)
        }
        proto::QueryPlanKind::VectorFirstAnn => Ok(QueryPlanKind::VectorFirstAnn),
        proto::QueryPlanKind::CooperativeFilteredAnn => Ok(QueryPlanKind::CooperativeFilteredAnn),
        proto::QueryPlanKind::HybridExactAnnMerge => Ok(QueryPlanKind::HybridExactAnnMerge),
    }
}

fn query_stage_timings_from_proto(timings: proto::QueryStageTimings) -> QueryStageTimings {
    QueryStageTimings {
        planning_micros: timings.planning_micros,
        prefilter_micros: timings.prefilter_micros,
        candidate_generation_micros: timings.candidate_generation_micros,
        postfilter_micros: timings.postfilter_micros,
        rerank_micros: timings.rerank_micros,
        merge_micros: timings.merge_micros,
    }
}

fn maintenance_status_from_proto(status: proto::MaintenanceStatus) -> Result<MaintenanceStatus> {
    Ok(MaintenanceStatus {
        pending: status.pending,
        in_progress: status.in_progress,
        last_error: status.last_error,
        completed_runs: status.completed_runs as usize,
    })
}

fn maintenance_backlog_from_proto(maintenance: MaintenanceBacklogReply) -> MaintenanceBacklog {
    MaintenanceBacklog {
        collections_with_pending: maintenance.collections_with_pending as usize,
        pending_operations: maintenance.pending_operations as usize,
        collections_in_progress: maintenance.collections_in_progress as usize,
        collections_with_errors: maintenance.collections_with_errors as usize,
    }
}

fn query_unit_stats_from_proto(stats: proto::QueryUnitStats) -> Result<QueryUnitStats> {
    Ok(QueryUnitStats {
        unit_id: stats.unit_id,
        tier: stats.tier,
        index_kind: stats.index_kind,
        min_seq_no: stats.min_seq_no,
        max_seq_no: stats.max_seq_no,
        put_count: stats.put_count as usize,
        delete_count: stats.delete_count as usize,
        approx_bytes: stats.approx_bytes as usize,
        scalar_fields: stats
            .scalar_fields
            .into_iter()
            .map(|(field, stats)| scalar_field_stats_from_proto(stats).map(|stats| (field, stats)))
            .collect::<Result<_>>()?,
        artifact_stats: stats
            .artifact_stats
            .into_iter()
            .map(|artifact| logpose_types::QueryUnitArtifactStats {
                kind: artifact.kind,
                file_name: artifact.file_name,
                approx_bytes: artifact.approx_bytes as usize,
            })
            .collect(),
        component_bytes: stats
            .component_bytes
            .into_iter()
            .map(|(key, value)| (key, value as usize))
            .collect(),
    })
}

fn scalar_field_stats_from_proto(stats: proto::ScalarFieldStats) -> Result<ScalarFieldStats> {
    Ok(ScalarFieldStats {
        present_count: stats.present_count as usize,
        null_count: stats.null_count as usize,
        value_counts: stats
            .value_counts
            .into_iter()
            .map(|(value, count)| (value, count as usize))
            .collect(),
        min: stats.min.map(scalar_value_from_proto).transpose()?,
        max: stats.max.map(scalar_value_from_proto).transpose()?,
        distinct_count: stats.distinct_count as usize,
    })
}

fn inspect_target_to_proto(target: &InspectTarget) -> proto::InspectTarget {
    match target {
        InspectTarget::Manifest => proto::InspectTarget::Manifest,
        InspectTarget::Wal => proto::InspectTarget::Wal,
        InspectTarget::Segment(_) => proto::InspectTarget::Segment,
        InspectTarget::Maintenance => proto::InspectTarget::Maintenance,
    }
}

fn inspect_segment_id(target: &InspectTarget) -> String {
    match target {
        InspectTarget::Segment(segment_id) => segment_id.clone(),
        InspectTarget::Manifest | InspectTarget::Wal | InspectTarget::Maintenance => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn scoped_collection_responses_serialize_a_flattened_payload() {
        let response = ScopedCollectionResponse {
            database_name: "analytics".to_owned(),
            collection_name: "documents".to_owned(),
            response: CommitAck {
                last_seq_no: 7,
                applied_ops: 2,
                snapshot: Snapshot {
                    manifest_generation: 3,
                    visible_seq_no: 7,
                },
            },
        };

        let json = serde_json::to_value(response).expect("response should serialize");
        assert_eq!(json["database_name"], "analytics");
        assert_eq!(json["collection_name"], "documents");
        assert_eq!(json["last_seq_no"], 7);
        assert_eq!(json["applied_ops"], 2);
        assert_eq!(json["snapshot"]["visible_seq_no"], 7);
        assert!(json.get("response").is_none());
    }

    #[test]
    fn query_diagnostics_from_proto_preserves_ann_fields() {
        let diagnostics = query_diagnostics_from_proto(proto::QueryDiagnostics {
            chosen_plan: proto::QueryPlanKind::CooperativeFilteredAnn as i32,
            planner_reason:
                "filtered ann traversal is cheaper than exact scan for this selectivity".to_owned(),
            estimated_selectivity: 0.25,
            units_considered: 2,
            units_pruned: 1,
            units_scanned: 1,
            candidates_before_filter: 17,
            candidates_after_filter: 13,
            candidates_reranked: 7,
            candidates_merged: 5,
            rerank_count: 1,
            fallback_reason: Some("fallback".to_owned()),
            unit_scan_mix: [
                ("immutable_ann".to_owned(), 1),
                ("mutable_exact".to_owned(), 2),
            ]
            .into_iter()
            .collect(),
            stage_timings: Some(proto::QueryStageTimings {
                planning_micros: 11,
                prefilter_micros: 22,
                candidate_generation_micros: 33,
                postfilter_micros: 44,
                rerank_micros: 55,
                merge_micros: 66,
            }),
        })
        .expect("conversion should succeed");

        assert_eq!(
            diagnostics,
            QueryDiagnostics {
                chosen_plan: QueryPlanKind::CooperativeFilteredAnn,
                planner_reason:
                    "filtered ann traversal is cheaper than exact scan for this selectivity"
                        .to_owned(),
                estimated_selectivity: 0.25,
                units_considered: 2,
                units_pruned: 1,
                units_scanned: 1,
                candidates_before_filter: 17,
                candidates_after_filter: 13,
                candidates_reranked: 7,
                candidates_merged: 5,
                rerank_count: 1,
                fallback_reason: Some("fallback".to_owned()),
                unit_scan_mix: BTreeMap::from([
                    ("immutable_ann".to_owned(), 1),
                    ("mutable_exact".to_owned(), 2),
                ]),
                stage_timings: Some(QueryStageTimings {
                    planning_micros: 11,
                    prefilter_micros: 22,
                    candidate_generation_micros: 33,
                    postfilter_micros: 44,
                    rerank_micros: 55,
                    merge_micros: 66,
                }),
            }
        );
    }

    #[test]
    fn runtime_status_from_proto_reads_coordination_fields() {
        let status = runtime_status_from_proto(proto::GetRuntimeStatusReply {
            metadata: Some(proto::GetMetadataReply {
                product: "LogPose".to_owned(),
                node_name: "client-node".to_owned(),
                version: "test".to_owned(),
                git_sha: "sha".to_owned(),
                profile: "debug".to_owned(),
            }),
            role: proto::NodeRole::Combined as i32,
            rest_endpoint: "http://127.0.0.1:8080".to_owned(),
            grpc_endpoint: "http://127.0.0.1:50051".to_owned(),
            storage_engine: "local+etcd-metadata".to_owned(),
            control_plane_ready: true,
            data_plane_ready: true,
            collection_count: 0,
            collections: Vec::new(),
            coordination: Some(proto::CoordinationStatusReply {
                cluster_name: "prod-cluster".to_owned(),
                membership_registered: true,
                membership_lease_id: Some(17),
                registered_members: vec!["client-node".to_owned(), "client-peer".to_owned()],
                leader_node: Some("client-node".to_owned()),
                is_local_leader: true,
                leadership_lease_id: Some(23),
                last_error: Some("warn".to_owned()),
            }),
            maintenance: Some(proto::MaintenanceBacklogReply {
                collections_with_pending: 0,
                pending_operations: 0,
                collections_in_progress: 0,
                collections_with_errors: 0,
            }),
        })
        .expect("runtime status should decode");

        let coordination = status
            .coordination
            .expect("coordination should be populated");
        assert_eq!(coordination.cluster_name, "prod-cluster");
        assert_eq!(coordination.membership_lease_id, Some(17));
        assert_eq!(coordination.leadership_lease_id, Some(23));
        assert_eq!(coordination.leader_node.as_deref(), Some("client-node"));
        assert_eq!(
            coordination.registered_members,
            vec!["client-node".to_owned(), "client-peer".to_owned()]
        );
        assert!(coordination.is_local_leader);
        assert_eq!(coordination.last_error.as_deref(), Some("warn"));
    }

    #[test]
    fn collection_placement_from_proto_reads_owner_fields() {
        let placement = collection_placement_from_proto(proto::CollectionPlacementReply {
            collection_id: "11111111-1111-1111-1111-111111111111".to_owned(),
            database_name: "analytics".to_owned(),
            collection_name: "documents".to_owned(),
            assigned_node: "owner-a".to_owned(),
            assigned_role: proto::NodeRole::Data as i32,
            owner_node: Some("owner-b".to_owned()),
            ownership_epoch: Some(2),
            route_kind: "recorded".to_owned(),
            route_reason: "ownership epoch 2 is assigned to node 'owner-b'".to_owned(),
        })
        .expect("placement should decode");

        assert_eq!(placement.owner_node.as_deref(), Some("owner-b"));
        assert_eq!(placement.ownership_epoch, Some(2));
    }
}
