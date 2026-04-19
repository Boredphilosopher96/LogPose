//! gRPC-backed client helpers for LogPose operator workflows.

use logpose_api_grpc::proto::{
    self, CollectionDescriptorReply, CollectionPlacementReply, CompactCollectionRequest,
    CoordinationStatusReply, CreateCollectionRequest as ProtoCreateCollectionRequest,
    FlushCollectionRequest, GetCollectionPlacementRequest, GetCollectionRequest,
    GetCollectionStatsRequest, GetDatabasePolicyRequest, GetDatabaseRequest, GetMetadataRequest,
    GetRuntimeStatusRequest, InspectCollectionRequest, ListDatabasesRequest,
    MaintenanceBacklogReply, PutDatabasePolicyRequest, PutDatabaseRequest, QueryCollectionRequest,
    ScalarValue, WriteCollectionRequest, log_pose_service_client::LogPoseServiceClient,
};
use logpose_auth::{AuthenticationMode, DatabaseRole};
pub use logpose_auth::{DatabaseAccessPolicy, DatabaseRoleBinding};
pub use logpose_catalog::{CollectionDescriptor, DatabaseDescriptor};
#[cfg(test)]
use logpose_config as _;
#[cfg(test)]
use logpose_core as _;
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator,
    QueryDiagnostics, QueryMatch, QueryPlanKind, QueryRequest, QueryResponse, QueryStageTimings,
    ScalarMetadataValue,
};
pub use logpose_storage::{CreateCollectionRequest, InspectReport, InspectTarget};
use logpose_types::{
    CollectionId, CollectionPlacement, CollectionRef, CollectionStats, CommitAck,
    CoordinationStatus, DEFAULT_DATABASE_NAME, DistanceMetric, LogPoseError, MaintenanceBacklog,
    MaintenanceStatus, NodeMetadata, NodeRole, NodeRuntimeStatus, QueryUnitStats, RecordId,
    RemoteBlobConfig, ScalarFieldStats, Snapshot, WriteOperation,
};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use thiserror::Error;
#[cfg(test)]
use tokio as _;
use tonic::{
    Request,
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

/// Client-scoped result type.
pub type Result<T> = std::result::Result<T, ClientError>;

/// Errors returned by the gRPC-backed client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// gRPC transport bootstrap failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// The server returned a gRPC status error.
    #[error(transparent)]
    Status(#[from] tonic::Status),
    /// The server returned an invalid or incomplete payload.
    #[error("{0}")]
    InvalidResponse(String),
    /// The caller supplied an invalid bearer token for client transport metadata.
    #[error("{0}")]
    InvalidAuthToken(String),
    /// The server returned malformed JSON payloads.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
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

/// Thin gRPC client over the shared LogPose server contract.
#[derive(Clone)]
pub struct LogPoseClient {
    inner: LogPoseServiceClient<InterceptedService<Channel, AuthInterceptor>>,
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
        let inner =
            LogPoseServiceClient::with_interceptor(channel, AuthInterceptor::new(auth_token)?);
        Ok(Self { inner })
    }

    /// Connect using a shared client configuration.
    pub async fn from_config(config: &ClientConfig) -> Result<Self> {
        Self::connect_with_auth(config.grpc_endpoint.clone(), config.auth_token.as_deref()).await
    }

    /// Fetch canonical node metadata from the server.
    pub async fn metadata(&self) -> Result<NodeMetadata> {
        let response = self
            .inner
            .clone()
            .get_metadata(Request::new(GetMetadataRequest {}))
            .await?
            .into_inner();
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
            .inner
            .clone()
            .get_runtime_status(Request::new(GetRuntimeStatusRequest {}))
            .await?
            .into_inner();
        runtime_status_from_proto(response)
    }

    /// Create or replace one database descriptor.
    pub async fn set_database(&self, descriptor: DatabaseDescriptor) -> Result<DatabaseDescriptor> {
        let response = self
            .inner
            .clone()
            .put_database(Request::new(PutDatabaseRequest {
                descriptor: Some(database_descriptor_to_proto(descriptor)),
            }))
            .await?
            .into_inner();
        database_descriptor_from_proto(response)
    }

    /// Read one database descriptor.
    pub async fn database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        let response = self
            .inner
            .clone()
            .get_database(Request::new(GetDatabaseRequest {
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();
        database_descriptor_from_proto(response)
    }

    /// List every database descriptor.
    pub async fn databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        let response = self
            .inner
            .clone()
            .list_databases(Request::new(ListDatabasesRequest {}))
            .await?
            .into_inner();
        response
            .databases
            .into_iter()
            .map(database_descriptor_from_proto)
            .collect()
    }

    /// Create a collection through the shared service contract.
    pub async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        let response = self
            .inner
            .clone()
            .create_collection(Request::new(ProtoCreateCollectionRequest {
                name: request.name,
                dimensions: request.dimensions as u64,
                metric: proto_metric(request.metric) as i32,
                database_name: request.database_name,
            }))
            .await?
            .into_inner();
        collection_descriptor_from_proto(response)
    }

    /// Fetch placement metadata for one collection.
    pub async fn collection_placement(&self, collection_name: &str) -> Result<CollectionPlacement> {
        let (database_name, collection_name) = split_collection_lookup_key(collection_name);
        self.collection_placement_in_database(&database_name, &collection_name)
            .await
    }

    /// Fetch placement metadata for one collection in an explicit database.
    pub async fn collection_placement_in_database(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionPlacement> {
        let response = self
            .inner
            .clone()
            .get_collection_placement(Request::new(GetCollectionPlacementRequest {
                collection_name: collection_name.to_owned(),
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();
        collection_placement_from_proto(response)
    }

    /// Fetch collection metadata by name.
    pub async fn get_collection(&self, collection_name: &str) -> Result<CollectionDescriptor> {
        let (database_name, collection_name) = split_collection_lookup_key(collection_name);
        self.get_collection_in_database(&database_name, &collection_name)
            .await
    }

    /// Fetch collection metadata by database-qualified identity.
    pub async fn get_collection_in_database(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionDescriptor> {
        let response = self
            .inner
            .clone()
            .get_collection(Request::new(GetCollectionRequest {
                collection_name: collection_name.to_owned(),
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();
        collection_descriptor_from_proto(response)
    }

    /// Create or replace one database access policy.
    pub async fn set_database_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        let database_name = policy.database_name.clone();
        let response = self
            .inner
            .clone()
            .put_database_policy(Request::new(PutDatabasePolicyRequest {
                policy: Some(database_policy_to_proto(policy)),
            }))
            .await?
            .into_inner();
        database_policy_from_proto(response, &database_name)
    }

    /// Read one database access policy.
    pub async fn database_policy(&self, database_name: &str) -> Result<DatabaseAccessPolicy> {
        let response = self
            .inner
            .clone()
            .get_database_policy(Request::new(GetDatabasePolicyRequest {
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();
        database_policy_from_proto(response, database_name)
    }

    /// Persist a write batch durably.
    pub async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        let (database_name, collection_name) = split_collection_lookup_key(collection_name);
        Ok(self
            .write_in_database(&database_name, &collection_name, operations)
            .await?
            .response)
    }

    /// Persist a write batch durably in an explicit database.
    pub async fn write_in_database(
        &self,
        database_name: &str,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<ScopedCollectionResponse<CommitAck>> {
        let response = self
            .inner
            .clone()
            .write_collection(Request::new(WriteCollectionRequest {
                collection_name: collection_name.to_owned(),
                operations: operations
                    .into_iter()
                    .map(write_operation_to_proto)
                    .collect::<Vec<_>>(),
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(scoped_collection_response(
            response.database_name,
            response.collection_name,
            database_name,
            collection_name,
            CommitAck {
                last_seq_no: response.last_seq_no,
                applied_ops: response.applied_ops as usize,
                snapshot: response.snapshot.map(snapshot_from_proto).ok_or_else(|| {
                    ClientError::InvalidResponse("write response missing write snapshot".to_owned())
                })?,
            },
        ))
    }

    /// Execute an exact query through the shared service contract.
    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse> {
        let (database_name, collection_name) =
            split_collection_lookup_key(&request.collection_name);
        let mut request = request;
        request.collection_name = collection_name;
        Ok(self
            .query_in_database(&database_name, request)
            .await?
            .response)
    }

    /// Execute an exact query through the shared service contract in an explicit database.
    pub async fn query_in_database(
        &self,
        database_name: &str,
        request: QueryRequest,
    ) -> Result<ScopedCollectionResponse<QueryResponse>> {
        let collection_name = request.collection_name.clone();
        let response = self
            .inner
            .clone()
            .query_collection(Request::new(QueryCollectionRequest {
                collection_name: collection_name.clone(),
                vector: request.vector,
                top_k: request.top_k as u64,
                snapshot: request.snapshot.map(snapshot_to_proto),
                filters: request
                    .filters
                    .into_iter()
                    .map(metadata_filter_to_proto)
                    .collect::<Result<Vec<_>>>()?,
                predicate: request.predicate.map(predicate_to_proto).transpose()?,
                explain: explain_mode_to_proto(request.explain) as i32,
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();

        Ok(scoped_collection_response(
            response.database_name,
            response.collection_name,
            database_name,
            &collection_name,
            QueryResponse {
                metric: metric_from_proto(response.metric)?,
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
            },
        ))
    }

    /// Fetch collection-level statistics.
    pub async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        let (database_name, collection_name) = split_collection_lookup_key(collection_name);
        self.stats_in_database(&database_name, &collection_name)
            .await
    }

    /// Fetch collection-level statistics in an explicit database.
    pub async fn stats_in_database(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionStats> {
        self.stats_in_database_at_snapshot(database_name, collection_name, None)
            .await
    }

    /// Fetch collection-level statistics in an explicit database for one snapshot.
    pub async fn stats_in_database_at_snapshot(
        &self,
        database_name: &str,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let response = self
            .inner
            .clone()
            .get_collection_stats(Request::new(GetCollectionStatsRequest {
                collection_name: collection_name.to_owned(),
                database_name: database_name.to_owned(),
                snapshot: snapshot.map(snapshot_to_proto),
            }))
            .await?
            .into_inner();
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
    pub async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        let (database_name, collection_name) = split_collection_lookup_key(collection_name);
        Ok(self
            .flush_in_database(&database_name, &collection_name)
            .await?
            .response)
    }

    /// Flush the mutable delta into a new segment in an explicit database.
    pub async fn flush_in_database(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<ScopedCollectionResponse<Snapshot>> {
        let response = self
            .inner
            .clone()
            .flush_collection(Request::new(FlushCollectionRequest {
                collection_name: collection_name.to_owned(),
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();
        let snapshot = snapshot_reply_from_proto(response.clone());
        Ok(scoped_collection_response(
            response.database_name,
            response.collection_name,
            database_name,
            collection_name,
            snapshot,
        ))
    }

    /// Compact immutable segments.
    pub async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        let (database_name, collection_name) = split_collection_lookup_key(collection_name);
        Ok(self
            .compact_in_database(&database_name, &collection_name)
            .await?
            .response)
    }

    /// Compact immutable segments in an explicit database.
    pub async fn compact_in_database(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<ScopedCollectionResponse<Snapshot>> {
        let response = self
            .inner
            .clone()
            .compact_collection(Request::new(CompactCollectionRequest {
                collection_name: collection_name.to_owned(),
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();
        let snapshot = snapshot_reply_from_proto(response.clone());
        Ok(scoped_collection_response(
            response.database_name,
            response.collection_name,
            database_name,
            collection_name,
            snapshot,
        ))
    }

    /// Inspect operator-visible storage state.
    pub async fn inspect(
        &self,
        collection_name: &str,
        target: InspectTarget,
    ) -> Result<InspectReport> {
        let (database_name, collection_name) = split_collection_lookup_key(collection_name);
        Ok(self
            .inspect_in_database(&database_name, &collection_name, target)
            .await?
            .response)
    }

    /// Inspect operator-visible storage state in an explicit database.
    pub async fn inspect_in_database(
        &self,
        database_name: &str,
        collection_name: &str,
        target: InspectTarget,
    ) -> Result<ScopedCollectionResponse<InspectReport>> {
        let response = self
            .inner
            .clone()
            .inspect_collection(Request::new(InspectCollectionRequest {
                collection_name: collection_name.to_owned(),
                target: inspect_target_to_proto(&target) as i32,
                segment_id: inspect_segment_id(&target),
                database_name: database_name.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(scoped_collection_response(
            response.database_name,
            response.collection_name,
            database_name,
            collection_name,
            InspectReport {
                target: response.target,
                payload: serde_json::from_str(&response.payload_json)?,
            },
        ))
    }
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

fn database_descriptor_to_proto(descriptor: DatabaseDescriptor) -> proto::DatabaseDescriptorReply {
    proto::DatabaseDescriptorReply {
        database_id: descriptor.database_id.to_string(),
        name: descriptor.name,
        is_default: descriptor.is_default,
    }
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

fn collection_descriptor_from_proto(
    reply: CollectionDescriptorReply,
) -> Result<CollectionDescriptor> {
    Ok(CollectionDescriptor {
        collection_id: parse_collection_id(&reply.collection_id)?,
        database_name: reply.database_name,
        name: reply.name,
        dimensions: reply.dimensions as usize,
        metric: metric_from_proto(reply.metric)?,
        root_path: reply.root_path.into(),
        remote_blob: reply.remote_blob.map(|remote| RemoteBlobConfig {
            endpoint: remote.endpoint,
            bucket: remote.bucket,
            prefix: remote.prefix,
        }),
        flush_threshold_ops: reply.flush_threshold_ops as usize,
        flush_threshold_bytes: reply.flush_threshold_bytes as usize,
        compaction_threshold_segments: reply.compaction_threshold_segments as usize,
    })
}

fn database_policy_to_proto(policy: DatabaseAccessPolicy) -> proto::DatabaseAccessPolicyReply {
    proto::DatabaseAccessPolicyReply {
        database_name: policy.database_name,
        authentication_mode: authentication_mode_to_proto(policy.authentication_mode) as i32,
        role_bindings: policy
            .role_bindings
            .into_iter()
            .map(database_role_binding_to_proto)
            .collect(),
    }
}

fn database_policy_from_proto(
    reply: proto::DatabaseAccessPolicyReply,
    fallback_database: &str,
) -> Result<DatabaseAccessPolicy> {
    let database_name = if reply.database_name.trim().is_empty() {
        fallback_database.to_owned()
    } else {
        reply.database_name
    };

    Ok(DatabaseAccessPolicy {
        database_name: database_name.clone(),
        authentication_mode: authentication_mode_from_proto(reply.authentication_mode)?,
        role_bindings: reply
            .role_bindings
            .into_iter()
            .map(|binding| database_role_binding_from_proto(binding, &database_name))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn database_role_binding_to_proto(binding: DatabaseRoleBinding) -> proto::DatabaseRoleBindingReply {
    proto::DatabaseRoleBindingReply {
        database_name: binding.database_name,
        principal_name: binding.principal_name,
        role: database_role_to_proto(binding.role) as i32,
    }
}

fn database_role_binding_from_proto(
    binding: proto::DatabaseRoleBindingReply,
    fallback_database: &str,
) -> Result<DatabaseRoleBinding> {
    Ok(DatabaseRoleBinding {
        database_name: if binding.database_name.trim().is_empty() {
            fallback_database.to_owned()
        } else {
            binding.database_name
        },
        principal_name: binding.principal_name,
        role: database_role_from_proto(binding.role)?,
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
        route_kind: reply.route_kind,
        route_reason: reply.route_reason,
    })
}

fn parse_collection_id(value: &str) -> Result<CollectionId> {
    value.parse().map(CollectionId).map_err(|error| {
        ClientError::InvalidResponse(format!("invalid collection id '{value}': {error}"))
    })
}

fn split_collection_lookup_key(value: &str) -> (String, String) {
    let parts = value.split('/').collect::<Vec<_>>();
    if parts.len() == 2 && parts.iter().all(|part| !part.trim().is_empty()) {
        (parts[0].to_owned(), parts[1].to_owned())
    } else {
        (DEFAULT_DATABASE_NAME.to_owned(), value.to_owned())
    }
}

fn scoped_collection_response<T>(
    database_name: String,
    collection_name: String,
    fallback_database: &str,
    fallback_collection: &str,
    response: T,
) -> ScopedCollectionResponse<T> {
    ScopedCollectionResponse {
        database_name: if database_name.trim().is_empty() {
            fallback_database.to_owned()
        } else {
            database_name
        },
        collection_name: if collection_name.trim().is_empty() {
            fallback_collection.to_owned()
        } else {
            collection_name
        },
        response,
    }
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

fn authentication_mode_to_proto(mode: AuthenticationMode) -> proto::AuthenticationMode {
    match mode {
        AuthenticationMode::Disabled => proto::AuthenticationMode::Disabled,
        AuthenticationMode::Password => proto::AuthenticationMode::Password,
        AuthenticationMode::MutualTls => proto::AuthenticationMode::MutualTls,
        AuthenticationMode::ExternalToken => proto::AuthenticationMode::ExternalToken,
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

fn database_role_to_proto(role: DatabaseRole) -> proto::DatabaseRole {
    match role {
        DatabaseRole::Owner => proto::DatabaseRole::Owner,
        DatabaseRole::ReadWrite => proto::DatabaseRole::ReadWrite,
        DatabaseRole::ReadOnly => proto::DatabaseRole::ReadOnly,
    }
}

fn metric_from_proto(metric: i32) -> Result<DistanceMetric> {
    match proto::DistanceMetric::try_from(metric)
        .map_err(|_| ClientError::InvalidResponse(format!("unknown distance metric '{metric}'")))?
    {
        proto::DistanceMetric::Cosine => Ok(DistanceMetric::Cosine),
        proto::DistanceMetric::Dot => Ok(DistanceMetric::Dot),
        proto::DistanceMetric::L2 => Ok(DistanceMetric::L2),
        proto::DistanceMetric::Unspecified => Err(ClientError::InvalidResponse(
            "distance metric must be set".to_owned(),
        )),
    }
}

fn proto_metric(metric: DistanceMetric) -> proto::DistanceMetric {
    match metric {
        DistanceMetric::Cosine => proto::DistanceMetric::Cosine,
        DistanceMetric::Dot => proto::DistanceMetric::Dot,
        DistanceMetric::L2 => proto::DistanceMetric::L2,
    }
}

fn write_operation_to_proto(operation: WriteOperation) -> proto::WriteOperation {
    match operation {
        WriteOperation::Put(record) => proto::WriteOperation {
            operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                id: record.id.to_string(),
                vector: record.vector,
                metadata_json: serde_json::to_string(&record.metadata)
                    .expect("put record metadata should serialize"),
            })),
        },
        WriteOperation::Delete(record) => proto::WriteOperation {
            operation: Some(proto::write_operation::Operation::Delete(
                proto::DeleteRecord {
                    id: record.id.to_string(),
                },
            )),
        },
    }
}

fn metadata_filter_to_proto(filter: MetadataFilter) -> Result<proto::MetadataFilter> {
    Ok(proto::MetadataFilter {
        field: filter.field,
        value: Some(scalar_value_to_proto(filter.value)?),
    })
}

fn predicate_to_proto(predicate: Predicate) -> Result<proto::Predicate> {
    let node = match predicate {
        Predicate::And { children } => proto::predicate::Node::And(proto::PredicateList {
            children: children
                .into_iter()
                .map(predicate_to_proto)
                .collect::<Result<Vec<_>>>()?,
        }),
        Predicate::Or { children } => proto::predicate::Node::Or(proto::PredicateList {
            children: children
                .into_iter()
                .map(predicate_to_proto)
                .collect::<Result<Vec<_>>>()?,
        }),
        Predicate::Not { child } => proto::predicate::Node::Not(Box::new(proto::PredicateNot {
            child: Some(Box::new(predicate_to_proto(*child)?)),
        })),
        Predicate::Comparison(comparison) => {
            proto::predicate::Node::Comparison(predicate_comparison_to_proto(comparison)?)
        }
    };
    Ok(proto::Predicate { node: Some(node) })
}

fn predicate_comparison_to_proto(
    comparison: PredicateComparison,
) -> Result<proto::PredicateComparison> {
    Ok(proto::PredicateComparison {
        field: comparison.field,
        operator: predicate_operator_to_proto(comparison.operator) as i32,
        value: comparison.value.map(scalar_value_to_proto).transpose()?,
    })
}

fn predicate_operator_to_proto(operator: PredicateOperator) -> proto::PredicateOperator {
    match operator {
        PredicateOperator::Eq => proto::PredicateOperator::Eq,
        PredicateOperator::Ne => proto::PredicateOperator::Ne,
        PredicateOperator::Lt => proto::PredicateOperator::Lt,
        PredicateOperator::Lte => proto::PredicateOperator::Lte,
        PredicateOperator::Gt => proto::PredicateOperator::Gt,
        PredicateOperator::Gte => proto::PredicateOperator::Gte,
        PredicateOperator::Exists => proto::PredicateOperator::Exists,
        PredicateOperator::IsNull => proto::PredicateOperator::IsNull,
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

fn snapshot_to_proto(snapshot: Snapshot) -> proto::Snapshot {
    proto::Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
    }
}

fn snapshot_from_proto(snapshot: proto::Snapshot) -> Snapshot {
    Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
    }
}

fn snapshot_reply_from_proto(snapshot: proto::SnapshotReply) -> Snapshot {
    Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
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

fn query_match_from_proto(candidate: proto::QueryMatch) -> Result<QueryMatch> {
    Ok(QueryMatch {
        id: RecordId::new(candidate.id),
        value: candidate.value,
        metadata: serde_json::from_str(&candidate.metadata_json)?,
    })
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

impl From<LogPoseError> for ClientError {
    fn from(error: LogPoseError) -> Self {
        Self::InvalidResponse(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn split_collection_lookup_key_parses_explicit_database_scope() {
        let (database_name, collection_name) = split_collection_lookup_key("analytics/documents");

        assert_eq!(database_name, "analytics");
        assert_eq!(collection_name, "documents");
    }

    #[test]
    fn scoped_collection_response_falls_back_to_requested_database_scope() {
        let response = scoped_collection_response(
            String::new(),
            String::new(),
            "analytics",
            "documents",
            CommitAck {
                last_seq_no: 7,
                applied_ops: 2,
                snapshot: Snapshot {
                    manifest_generation: 3,
                    visible_seq_no: 7,
                },
            },
        );

        assert_eq!(response.database_name, "analytics");
        assert_eq!(response.collection_name, "documents");
        assert_eq!(response.last_seq_no, 7);
        assert_eq!(response.snapshot.visible_seq_no, 7);
    }

    #[test]
    fn scoped_collection_response_serializes_flattened_payload() {
        let response = scoped_collection_response(
            "analytics".to_owned(),
            "documents".to_owned(),
            "analytics",
            "documents",
            CommitAck {
                last_seq_no: 7,
                applied_ops: 2,
                snapshot: Snapshot {
                    manifest_generation: 3,
                    visible_seq_no: 7,
                },
            },
        );

        let json = serde_json::to_value(response).expect("response should serialize");
        assert_eq!(json["database_name"], "analytics");
        assert_eq!(json["collection_name"], "documents");
        assert_eq!(json["last_seq_no"], 7);
        assert_eq!(json["applied_ops"], 2);
        assert_eq!(json["snapshot"]["manifest_generation"], 3);
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
}
