//! gRPC API surface for LogPose.

mod bulk;
pub mod convert;
mod error;
#[cfg(test)]
mod test_support;

pub use error::{
    ERROR_DOMAIN, RETRY_AFTER_METADATA_KEY, code_from_grpc, code_to_grpc, grpc_code,
    status_from_error,
};

use convert::{
    collection_to_proto, create_spec_from_proto, database_policy_from_proto,
    database_policy_to_proto, json_object_to_proto, metric_to_proto, primary_keys_from_proto,
    records_from_proto, required_name, schema_change_from_proto, snapshot_from_proto,
    snapshot_to_proto, split_lookups, updates_from_proto,
};
use error::{MessageLimitLayer, respond, unauthenticated};
use logpose_core::{AppState, RequestAuth};
use logpose_query::{
    ExplainMode, FilterComparison, FilterExpr, FilterOperator, MetadataFilter, QueryDiagnostics,
    QueryPlanKind, QueryRequest, QueryStageTimings, ScalarMetadataValue,
};
use logpose_storage::CreateCollectionRequest as StorageCreateCollectionRequest;
use logpose_types::{
    CollectionPlacement, CommitAck, CoordinationStatus, LogPoseError, MaintenanceBacklog,
    MaintenanceStatus, NodeRole, NodeRuntimeStatus, QueryUnitStats, ScalarFieldStats, Snapshot,
};
use serde_json::{Number, Value};
use std::{net::SocketAddr, sync::Arc};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};
use tonic_health::server::health_reporter;
use tracing::info;

#[allow(missing_docs)]
/// Generated protobuf interfaces.
pub mod proto {
    tonic::include_proto!("logpose.v2");
}

use proto::log_pose_service_server::{LogPoseService, LogPoseServiceServer};
use proto::{
    AlterCollectionRequest, BulkUpsertRecordsReply, BulkUpsertRecordsRequest,
    CollectionPlacementReply, CollectionReply, CollectionStatsReply, CommitAckReply,
    CompactCollectionRequest, CoordinationStatusReply, CreateCollectionRequest,
    DatabaseAccessPolicyReply, DatabaseDescriptorReply, DeleteRecordsRequest, DropCollectionReply,
    DropCollectionRequest, DropDatabaseReply, DropDatabaseRequest, FlushCollectionRequest,
    GetCollectionPlacementRequest, GetCollectionRequest, GetCollectionStatsRequest,
    GetDatabasePolicyRequest, GetDatabaseRequest, GetMetadataReply, GetMetadataRequest,
    GetRecordsReply, GetRecordsRequest, GetRuntimeStatusReply, GetRuntimeStatusRequest,
    InspectCollectionReply, InspectCollectionRequest, InspectTarget, ListCollectionsReply,
    ListCollectionsRequest, ListDatabasesReply, ListDatabasesRequest, MaintenanceBacklogReply,
    NodeRole as ProtoNodeRole, PutDatabasePolicyRequest, PutDatabaseRequest, QueryCollectionReply,
    QueryCollectionRequest, QueryMatch, ScalarValue, SnapshotReply, UpdateRecordsRequest,
    UpsertRecordsRequest,
};

/// Serve the gRPC API until shutdown.
pub async fn serve(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = SocketAddr::from((
        state.config.grpc_host.parse::<std::net::IpAddr>()?,
        state.config.grpc_port,
    ));

    let listener = tokio::net::TcpListener::bind(address).await?;
    serve_with_listener(state, listener).await
}

/// Serve the gRPC API over an existing listener.
///
/// Request messages above `limits.max_grpc_message_bytes` are rejected with
/// `RESOURCE_EXHAUSTED` and a typed `TOO_LARGE` error.
pub async fn serve_with_listener(
    state: Arc<AppState>,
    listener: tokio::net::TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = listener.local_addr()?;
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<LogPoseServiceServer<GrpcLogPoseService>>()
        .await;

    info!(%address, "starting gRPC listener");

    let message_limit = state.config.limits.max_grpc_message_bytes;
    Server::builder()
        .layer(MessageLimitLayer::new(message_limit))
        .add_service(health_service)
        .add_service(
            LogPoseServiceServer::new(GrpcLogPoseService::new(state))
                .max_decoding_message_size(message_limit),
        )
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

/// gRPC service implementation over the shared application state.
#[derive(Clone)]
pub struct GrpcLogPoseService {
    state: Arc<AppState>,
}

impl GrpcLogPoseService {
    /// Construct a gRPC service wrapper from shared application state.
    #[must_use]
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl LogPoseService for GrpcLogPoseService {
    async fn get_metadata(
        &self,
        request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataReply>, Status> {
        respond(self.get_metadata_inner(request).await)
    }

    async fn get_runtime_status(
        &self,
        request: Request<GetRuntimeStatusRequest>,
    ) -> Result<Response<GetRuntimeStatusReply>, Status> {
        respond(self.get_runtime_status_inner(request).await)
    }

    async fn put_database(
        &self,
        request: Request<PutDatabaseRequest>,
    ) -> Result<Response<DatabaseDescriptorReply>, Status> {
        respond(self.put_database_inner(request).await)
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<DatabaseDescriptorReply>, Status> {
        respond(self.get_database_inner(request).await)
    }

    async fn list_databases(
        &self,
        request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesReply>, Status> {
        respond(self.list_databases_inner(request).await)
    }

    async fn drop_database(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseReply>, Status> {
        respond(self.drop_database_inner(request).await)
    }

    async fn put_database_policy(
        &self,
        request: Request<PutDatabasePolicyRequest>,
    ) -> Result<Response<DatabaseAccessPolicyReply>, Status> {
        respond(self.put_database_policy_inner(request).await)
    }

    async fn get_database_policy(
        &self,
        request: Request<GetDatabasePolicyRequest>,
    ) -> Result<Response<DatabaseAccessPolicyReply>, Status> {
        respond(self.get_database_policy_inner(request).await)
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CollectionReply>, Status> {
        respond(self.create_collection_inner(request).await)
    }

    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<CollectionReply>, Status> {
        respond(self.get_collection_inner(request).await)
    }

    async fn list_collections(
        &self,
        request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsReply>, Status> {
        respond(self.list_collections_inner(request).await)
    }

    async fn alter_collection(
        &self,
        request: Request<AlterCollectionRequest>,
    ) -> Result<Response<CollectionReply>, Status> {
        respond(self.alter_collection_inner(request).await)
    }

    async fn drop_collection(
        &self,
        request: Request<DropCollectionRequest>,
    ) -> Result<Response<DropCollectionReply>, Status> {
        respond(self.drop_collection_inner(request).await)
    }

    async fn get_collection_placement(
        &self,
        request: Request<GetCollectionPlacementRequest>,
    ) -> Result<Response<CollectionPlacementReply>, Status> {
        respond(self.get_collection_placement_inner(request).await)
    }

    async fn upsert_records(
        &self,
        request: Request<UpsertRecordsRequest>,
    ) -> Result<Response<CommitAckReply>, Status> {
        respond(self.upsert_records_inner(request).await)
    }

    async fn update_records(
        &self,
        request: Request<UpdateRecordsRequest>,
    ) -> Result<Response<CommitAckReply>, Status> {
        respond(self.update_records_inner(request).await)
    }

    async fn delete_records(
        &self,
        request: Request<DeleteRecordsRequest>,
    ) -> Result<Response<CommitAckReply>, Status> {
        respond(self.delete_records_inner(request).await)
    }

    async fn get_records(
        &self,
        request: Request<GetRecordsRequest>,
    ) -> Result<Response<GetRecordsReply>, Status> {
        respond(self.get_records_inner(request).await)
    }

    async fn query_collection(
        &self,
        request: Request<QueryCollectionRequest>,
    ) -> Result<Response<QueryCollectionReply>, Status> {
        respond(self.query_collection_inner(request).await)
    }

    async fn get_collection_stats(
        &self,
        request: Request<GetCollectionStatsRequest>,
    ) -> Result<Response<CollectionStatsReply>, Status> {
        respond(self.get_collection_stats_inner(request).await)
    }

    async fn flush_collection(
        &self,
        request: Request<FlushCollectionRequest>,
    ) -> Result<Response<SnapshotReply>, Status> {
        respond(self.flush_collection_inner(request).await)
    }

    async fn compact_collection(
        &self,
        request: Request<CompactCollectionRequest>,
    ) -> Result<Response<SnapshotReply>, Status> {
        respond(self.compact_collection_inner(request).await)
    }

    async fn inspect_collection(
        &self,
        request: Request<InspectCollectionRequest>,
    ) -> Result<Response<InspectCollectionReply>, Status> {
        respond(self.inspect_collection_inner(request).await)
    }

    async fn bulk_upsert_records(
        &self,
        request: Request<Streaming<BulkUpsertRecordsRequest>>,
    ) -> Result<Response<BulkUpsertRecordsReply>, Status> {
        respond(bulk::bulk_upsert_records(Arc::clone(&self.state), request).await)
    }
}

/// A collection named by a request: its database and collection names, both required.
struct Target {
    database_name: String,
    collection_name: String,
}

impl Target {
    fn new(database_name: String, collection_name: String) -> Result<Self, LogPoseError> {
        Ok(Self {
            database_name: required_name("database_name", database_name)?,
            collection_name: required_name("collection_name", collection_name)?,
        })
    }

    /// The `database/collection` key the application layer resolves.
    fn key(&self) -> String {
        format!("{}/{}", self.database_name, self.collection_name)
    }

    fn commit_ack(self, ack: CommitAck) -> CommitAckReply {
        CommitAckReply {
            database_name: self.database_name,
            collection_name: self.collection_name,
            last_seq_no: ack.last_seq_no,
            applied_ops: ack.applied_ops as u64,
            snapshot: Some(snapshot_to_proto(ack.snapshot)),
        }
    }

    fn snapshot(self, snapshot: Snapshot) -> SnapshotReply {
        SnapshotReply {
            database_name: self.database_name,
            collection_name: self.collection_name,
            manifest_generation: snapshot.manifest_generation,
            visible_seq_no: snapshot.visible_seq_no,
        }
    }
}

impl GrpcLogPoseService {
    async fn get_metadata_inner(
        &self,
        _request: Request<GetMetadataRequest>,
    ) -> Result<GetMetadataReply, LogPoseError> {
        Ok(metadata_reply_from_domain(self.state.metadata()))
    }

    async fn get_runtime_status_inner(
        &self,
        request: Request<GetRuntimeStatusRequest>,
    ) -> Result<GetRuntimeStatusReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let status = self.state.runtime_status_with_auth(&auth).await?;
        Ok(runtime_status_reply_from_domain(status))
    }

    async fn put_database_inner(
        &self,
        request: Request<PutDatabaseRequest>,
    ) -> Result<DatabaseDescriptorReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let database_name = required_name("database_name", request.into_inner().database_name)?;
        let stored = self
            .state
            .put_database_with_auth(
                &auth,
                logpose_catalog::DatabaseDescriptor::new(database_name),
            )
            .await?;
        Ok(database_descriptor_to_proto(stored))
    }

    async fn get_database_inner(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<DatabaseDescriptorReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let database_name = required_name("database_name", request.into_inner().database_name)?;
        let descriptor = self.state.database_with_auth(&auth, &database_name).await?;
        Ok(database_descriptor_to_proto(descriptor))
    }

    async fn list_databases_inner(
        &self,
        request: Request<ListDatabasesRequest>,
    ) -> Result<ListDatabasesReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let descriptors = self.state.databases_with_auth(&auth).await?;
        Ok(ListDatabasesReply {
            databases: descriptors
                .into_iter()
                .map(database_descriptor_to_proto)
                .collect(),
        })
    }

    async fn drop_database_inner(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> Result<DropDatabaseReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let database_name = required_name("database_name", request.into_inner().database_name)?;
        self.state
            .drop_database_with_auth(&auth, &database_name)
            .await?;
        Ok(DropDatabaseReply { database_name })
    }

    async fn put_database_policy_inner(
        &self,
        request: Request<PutDatabasePolicyRequest>,
    ) -> Result<DatabaseAccessPolicyReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let policy = database_policy_from_proto(
            required_name("database_name", request.database_name)?,
            request.authentication_mode,
            request.role_bindings,
        )?;
        let stored = self
            .state
            .set_database_access_policy_with_auth(&auth, policy)
            .await?;
        Ok(database_policy_to_proto(stored))
    }

    async fn get_database_policy_inner(
        &self,
        request: Request<GetDatabasePolicyRequest>,
    ) -> Result<DatabaseAccessPolicyReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let database_name = required_name("database_name", request.into_inner().database_name)?;
        let policy = self
            .state
            .database_access_policy_with_auth(&auth, &database_name)
            .await?;
        Ok(database_policy_to_proto(policy))
    }

    async fn create_collection_inner(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<CollectionReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = required_name("database_name", request.database_name.clone())?;
        let spec = create_spec_from_proto(request)?;
        let descriptor = self
            .state
            .create_collection_with_auth(
                &auth,
                StorageCreateCollectionRequest::from_spec(database_name, spec),
            )
            .await?;
        Ok(collection_to_proto(descriptor))
    }

    async fn get_collection_inner(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<CollectionReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let descriptor = self
            .state
            .get_collection_with_auth(&auth, &target.key())
            .await?;
        Ok(collection_to_proto(descriptor))
    }

    async fn list_collections_inner(
        &self,
        request: Request<ListCollectionsRequest>,
    ) -> Result<ListCollectionsReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let database_name = required_name("database_name", request.into_inner().database_name)?;
        let collections = self
            .state
            .list_collections_with_auth(&auth, &database_name)
            .await?;
        Ok(ListCollectionsReply {
            collections: collections.into_iter().map(collection_to_proto).collect(),
        })
    }

    async fn alter_collection_inner(
        &self,
        request: Request<AlterCollectionRequest>,
    ) -> Result<CollectionReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let change = schema_change_from_proto(request.change)?;
        let descriptor = self
            .state
            .alter_collection_with_auth(&auth, &target.key(), change)
            .await?;
        Ok(collection_to_proto(descriptor))
    }

    async fn drop_collection_inner(
        &self,
        request: Request<DropCollectionRequest>,
    ) -> Result<DropCollectionReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        self.state
            .drop_collection_with_auth(&auth, &target.key())
            .await?;
        Ok(DropCollectionReply {
            database_name: target.database_name,
            collection_name: target.collection_name,
        })
    }

    async fn get_collection_placement_inner(
        &self,
        request: Request<GetCollectionPlacementRequest>,
    ) -> Result<CollectionPlacementReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let placement = self
            .state
            .collection_placement_with_auth(&auth, &target.key())
            .await?;
        Ok(collection_placement_reply_from_domain(placement))
    }

    async fn upsert_records_inner(
        &self,
        request: Request<UpsertRecordsRequest>,
    ) -> Result<CommitAckReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let records = records_from_proto(request.records, "records")?;
        let ack = self
            .state
            .upsert_records_with_auth(&auth, &target.key(), records)
            .await?;
        Ok(target.commit_ack(ack))
    }

    async fn update_records_inner(
        &self,
        request: Request<UpdateRecordsRequest>,
    ) -> Result<CommitAckReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let updates = updates_from_proto(request.records, "records")?;
        let ack = self
            .state
            .update_records_with_auth(&auth, &target.key(), updates)
            .await?;
        Ok(target.commit_ack(ack))
    }

    async fn delete_records_inner(
        &self,
        request: Request<DeleteRecordsRequest>,
    ) -> Result<CommitAckReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let keys = primary_keys_from_proto(request.keys, "keys")?;
        let ack = self
            .state
            .delete_records_with_auth(&auth, &target.key(), keys)
            .await?;
        Ok(target.commit_ack(ack))
    }

    async fn get_records_inner(
        &self,
        request: Request<GetRecordsRequest>,
    ) -> Result<GetRecordsReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let keys = primary_keys_from_proto(request.keys, "keys")?;
        let fetched = self
            .state
            .get_records_with_auth(&auth, &target.key(), keys.clone(), request.output_fields)
            .await?;
        let (records, missing_keys) = split_lookups(keys, fetched.records);
        Ok(GetRecordsReply {
            database_name: target.database_name,
            collection_name: target.collection_name,
            records,
            missing_keys,
            snapshot: Some(snapshot_to_proto(fetched.snapshot)),
        })
    }

    async fn query_collection_inner(
        &self,
        request: Request<QueryCollectionRequest>,
    ) -> Result<QueryCollectionReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        if request.top_k == 0 {
            return Err(LogPoseError::invalid_field(
                "top_k",
                "top_k must be greater than 0",
            ));
        }
        let filters = request
            .filters
            .into_iter()
            .map(metadata_filter_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let predicate = request.predicate.map(predicate_from_proto).transpose()?;
        let response = self
            .state
            .query_with_auth(
                &auth,
                QueryRequest {
                    collection_name: target.key(),
                    vector: request.vector,
                    top_k: request.top_k as usize,
                    snapshot: request.snapshot.map(snapshot_from_proto),
                    read_barrier: request.read_barrier.map(snapshot_from_proto),
                    filters,
                    predicate,
                    explain: explain_mode_from_proto(request.explain)?,
                },
            )
            .await?;
        Ok(QueryCollectionReply {
            database_name: target.database_name,
            collection_name: target.collection_name,
            metric: metric_to_proto(response.metric) as i32,
            top_k: response.top_k as u64,
            returned: response.returned as u64,
            snapshot: Some(snapshot_to_proto(response.snapshot)),
            matches: response
                .matches
                .into_iter()
                .map(|candidate| QueryMatch {
                    id: candidate.id.to_string(),
                    value: candidate.value,
                    metadata: match &candidate.metadata {
                        Value::Object(metadata) => Some(json_object_to_proto(metadata)),
                        _ => None,
                    },
                })
                .collect(),
            diagnostics: response
                .diagnostics
                .map(query_diagnostics_to_proto)
                .transpose()?,
        })
    }

    async fn get_collection_stats_inner(
        &self,
        request: Request<GetCollectionStatsRequest>,
    ) -> Result<CollectionStatsReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let stats = self
            .state
            .stats_for_read_with_auth(
                &auth,
                &target.key(),
                request.snapshot.map(snapshot_from_proto),
                request.read_barrier.map(snapshot_from_proto),
            )
            .await?;
        collection_stats_reply_from_domain(stats)
    }

    async fn flush_collection_inner(
        &self,
        request: Request<FlushCollectionRequest>,
    ) -> Result<SnapshotReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let snapshot = self.state.flush_with_auth(&auth, &target.key()).await?;
        Ok(target.snapshot(snapshot))
    }

    async fn compact_collection_inner(
        &self,
        request: Request<CompactCollectionRequest>,
    ) -> Result<SnapshotReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let snapshot = self.state.compact_with_auth(&auth, &target.key()).await?;
        Ok(target.snapshot(snapshot))
    }

    async fn inspect_collection_inner(
        &self,
        request: Request<InspectCollectionRequest>,
    ) -> Result<InspectCollectionReply, LogPoseError> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let target = Target::new(request.database_name, request.collection_name)?;
        let inspect_target = inspect_target_from_proto(request.target, request.segment_id)?;
        let report = self
            .state
            .inspect_with_auth(&auth, &target.key(), inspect_target)
            .await?;
        let payload_json = serde_json::to_string(&report.payload).map_err(|error| {
            LogPoseError::internal(format!("failed to serialize inspect payload: {error}"))
        })?;
        Ok(InspectCollectionReply {
            database_name: target.database_name,
            collection_name: target.collection_name,
            target: report.target,
            payload_json,
        })
    }
}

fn metadata_filter_from_proto(
    filter: proto::MetadataFilter,
) -> Result<MetadataFilter, LogPoseError> {
    let value = filter
        .value
        .ok_or_else(|| LogPoseError::invalid_argument("metadata filter value is required"))?;
    Ok(MetadataFilter {
        field: filter.field,
        value: scalar_value_from_proto(value)?,
    })
}

fn predicate_from_proto(predicate: proto::Predicate) -> Result<FilterExpr, LogPoseError> {
    match predicate
        .node
        .ok_or_else(|| LogPoseError::invalid_argument("predicate node is required"))?
    {
        proto::predicate::Node::And(list) => Ok(FilterExpr::And {
            children: list
                .children
                .into_iter()
                .map(predicate_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        proto::predicate::Node::Or(list) => Ok(FilterExpr::Or {
            children: list
                .children
                .into_iter()
                .map(predicate_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        proto::predicate::Node::Not(node) => Ok(FilterExpr::Not {
            child: Box::new(predicate_from_proto(*node.child.ok_or_else(|| {
                LogPoseError::invalid_argument("not predicate child is required")
            })?)?),
        }),
        proto::predicate::Node::Comparison(comparison) => Ok(FilterExpr::Comparison(
            predicate_comparison_from_proto(comparison)?,
        )),
    }
}

fn predicate_comparison_from_proto(
    comparison: proto::PredicateComparison,
) -> Result<FilterComparison, LogPoseError> {
    Ok(FilterComparison {
        field: comparison.field,
        operator: predicate_operator_from_proto(comparison.operator)?,
        value: comparison.value.map(scalar_value_from_proto).transpose()?,
    })
}

fn predicate_operator_from_proto(operator: i32) -> Result<FilterOperator, LogPoseError> {
    match proto::PredicateOperator::try_from(operator)
        .unwrap_or(proto::PredicateOperator::Unspecified)
    {
        proto::PredicateOperator::Eq => Ok(FilterOperator::Eq),
        proto::PredicateOperator::Ne => Ok(FilterOperator::Ne),
        proto::PredicateOperator::Lt => Ok(FilterOperator::Lt),
        proto::PredicateOperator::Lte => Ok(FilterOperator::Lte),
        proto::PredicateOperator::Gt => Ok(FilterOperator::Gt),
        proto::PredicateOperator::Gte => Ok(FilterOperator::Gte),
        proto::PredicateOperator::Exists => Ok(FilterOperator::Exists),
        proto::PredicateOperator::IsNull => Ok(FilterOperator::IsNull),
        proto::PredicateOperator::Unspecified => Err(LogPoseError::invalid_argument(
            "predicate comparison operator must be set",
        )),
    }
}

fn explain_mode_from_proto(mode: i32) -> Result<ExplainMode, LogPoseError> {
    match proto::ExplainMode::try_from(mode)
        .map_err(|_| LogPoseError::invalid_argument("explain mode must be a valid enum value"))?
    {
        proto::ExplainMode::None => Ok(ExplainMode::None),
        proto::ExplainMode::Plan => Ok(ExplainMode::Plan),
        proto::ExplainMode::Profile => Ok(ExplainMode::Profile),
    }
}

fn scalar_value_from_proto(value: ScalarValue) -> Result<ScalarMetadataValue, LogPoseError> {
    match value.kind {
        Some(proto::scalar_value::Kind::StringValue(value)) => {
            Ok(ScalarMetadataValue::String(value))
        }
        Some(proto::scalar_value::Kind::Int64Value(value)) => {
            Ok(ScalarMetadataValue::Number(Number::from(value)))
        }
        Some(proto::scalar_value::Kind::Uint64Value(value)) => {
            Ok(ScalarMetadataValue::Number(Number::from(value)))
        }
        Some(proto::scalar_value::Kind::DoubleValue(value)) => Number::from_f64(value)
            .map(ScalarMetadataValue::Number)
            .ok_or_else(|| LogPoseError::invalid_argument("double scalar value must be finite")),
        Some(proto::scalar_value::Kind::BoolValue(value)) => Ok(ScalarMetadataValue::Bool(value)),
        Some(proto::scalar_value::Kind::NullValue(_)) => Ok(ScalarMetadataValue::Null),
        None => Err(LogPoseError::invalid_argument(
            "scalar value kind is required",
        )),
    }
}

fn scalar_value_to_proto(value: ScalarMetadataValue) -> Result<ScalarValue, LogPoseError> {
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
                return Err(LogPoseError::internal(
                    "numeric scalar value must be finite",
                ));
            }
        }
        ScalarMetadataValue::Bool(value) => proto::scalar_value::Kind::BoolValue(value),
        ScalarMetadataValue::Null => proto::scalar_value::Kind::NullValue(true),
    };
    Ok(ScalarValue { kind: Some(kind) })
}

fn inspect_target_from_proto(
    target: i32,
    segment_id: String,
) -> Result<logpose_storage::InspectTarget, LogPoseError> {
    match InspectTarget::try_from(target).map_err(|_| {
        LogPoseError::invalid_argument(format!("unsupported inspect target '{target}'"))
    })? {
        InspectTarget::Manifest => Ok(logpose_storage::InspectTarget::Manifest),
        InspectTarget::Wal => Ok(logpose_storage::InspectTarget::Wal),
        InspectTarget::Segment => {
            if segment_id.is_empty() {
                Err(LogPoseError::invalid_field(
                    "segment_id",
                    "segment_id is required when inspect target is SEGMENT",
                ))
            } else {
                Ok(logpose_storage::InspectTarget::Segment(segment_id))
            }
        }
        InspectTarget::Maintenance => Ok(logpose_storage::InspectTarget::Maintenance),
    }
}

fn query_diagnostics_to_proto(
    diagnostics: QueryDiagnostics,
) -> Result<proto::QueryDiagnostics, LogPoseError> {
    Ok(proto::QueryDiagnostics {
        chosen_plan: query_plan_kind_to_proto(diagnostics.chosen_plan) as i32,
        planner_reason: diagnostics.planner_reason,
        estimated_selectivity: diagnostics.estimated_selectivity,
        units_considered: diagnostics.units_considered as u64,
        units_pruned: diagnostics.units_pruned as u64,
        units_scanned: diagnostics.units_scanned as u64,
        candidates_before_filter: diagnostics.candidates_before_filter as u64,
        candidates_after_filter: diagnostics.candidates_after_filter as u64,
        candidates_reranked: diagnostics.candidates_reranked as u64,
        candidates_merged: diagnostics.candidates_merged as u64,
        rerank_count: diagnostics.rerank_count as u64,
        fallback_reason: diagnostics.fallback_reason,
        unit_scan_mix: diagnostics
            .unit_scan_mix
            .into_iter()
            .map(|(key, value)| (key, value as u64))
            .collect(),
        stage_timings: diagnostics.stage_timings.map(query_stage_timings_to_proto),
    })
}

fn query_plan_kind_to_proto(plan: QueryPlanKind) -> proto::QueryPlanKind {
    match plan {
        QueryPlanKind::UnfilteredExactScan => proto::QueryPlanKind::UnfilteredExactScan,
        QueryPlanKind::PredicateFirstExact => proto::QueryPlanKind::PredicateFirstExact,
        QueryPlanKind::VectorFirstExact => proto::QueryPlanKind::VectorFirstExact,
        QueryPlanKind::TinyPopulationExactFallback => {
            proto::QueryPlanKind::TinyPopulationExactFallback
        }
        QueryPlanKind::VectorFirstAnn => proto::QueryPlanKind::VectorFirstAnn,
        QueryPlanKind::CooperativeFilteredAnn => proto::QueryPlanKind::CooperativeFilteredAnn,
        QueryPlanKind::HybridExactAnnMerge => proto::QueryPlanKind::HybridExactAnnMerge,
    }
}

fn query_stage_timings_to_proto(timings: QueryStageTimings) -> proto::QueryStageTimings {
    proto::QueryStageTimings {
        planning_micros: timings.planning_micros,
        prefilter_micros: timings.prefilter_micros,
        candidate_generation_micros: timings.candidate_generation_micros,
        postfilter_micros: timings.postfilter_micros,
        rerank_micros: timings.rerank_micros,
        merge_micros: timings.merge_micros,
    }
}

fn metadata_reply_from_domain(metadata: logpose_types::NodeMetadata) -> GetMetadataReply {
    GetMetadataReply {
        product: metadata.product,
        node_name: metadata.node_name,
        version: metadata.version,
        git_sha: metadata.git_sha,
        profile: metadata.profile,
    }
}

fn database_descriptor_to_proto(
    descriptor: logpose_catalog::DatabaseDescriptor,
) -> DatabaseDescriptorReply {
    DatabaseDescriptorReply {
        database_id: descriptor.database_id.to_string(),
        name: descriptor.name,
        is_default: descriptor.is_default,
    }
}

fn runtime_status_reply_from_domain(status: NodeRuntimeStatus) -> GetRuntimeStatusReply {
    GetRuntimeStatusReply {
        metadata: Some(metadata_reply_from_domain(status.metadata)),
        role: node_role_to_proto(status.role) as i32,
        rest_endpoint: status.rest_endpoint,
        grpc_endpoint: status.grpc_endpoint,
        storage_engine: status.storage_engine,
        control_plane_ready: status.control_plane_ready,
        data_plane_ready: status.data_plane_ready,
        collection_count: status.collection_count as u64,
        collections: status
            .collections
            .into_iter()
            .map(collection_placement_reply_from_domain)
            .collect(),
        coordination: status.coordination.map(coordination_status_to_proto),
        maintenance: Some(maintenance_backlog_to_proto(status.maintenance)),
    }
}

fn collection_placement_reply_from_domain(
    placement: CollectionPlacement,
) -> CollectionPlacementReply {
    CollectionPlacementReply {
        collection_id: placement.collection_id.to_string(),
        collection_name: placement.collection_name,
        assigned_node: placement.assigned_node,
        assigned_role: node_role_to_proto(placement.assigned_role) as i32,
        owner_node: placement.owner_node,
        ownership_epoch: placement.ownership_epoch,
        route_kind: placement.route_kind,
        route_reason: placement.route_reason,
        database_name: placement.database_name,
    }
}

fn maintenance_backlog_to_proto(maintenance: MaintenanceBacklog) -> MaintenanceBacklogReply {
    MaintenanceBacklogReply {
        collections_with_pending: maintenance.collections_with_pending as u64,
        pending_operations: maintenance.pending_operations as u64,
        collections_in_progress: maintenance.collections_in_progress as u64,
        collections_with_errors: maintenance.collections_with_errors as u64,
    }
}

fn coordination_status_to_proto(status: CoordinationStatus) -> CoordinationStatusReply {
    CoordinationStatusReply {
        cluster_name: status.cluster_name,
        membership_registered: status.membership_registered,
        membership_lease_id: status.membership_lease_id,
        registered_members: status.registered_members,
        leader_node: status.leader_node,
        is_local_leader: status.is_local_leader,
        leadership_lease_id: status.leadership_lease_id,
        last_error: status.last_error,
    }
}

fn collection_stats_reply_from_domain(
    stats: logpose_types::CollectionStats,
) -> Result<CollectionStatsReply, LogPoseError> {
    Ok(CollectionStatsReply {
        collection_id: stats.collection_id.to_string(),
        collection_name: stats.collection_name,
        manifest_generation: stats.manifest_generation,
        visible_seq_no: stats.visible_seq_no,
        mutable_op_count: stats.mutable_op_count as u64,
        segment_count: stats.segment_count as u64,
        live_record_count: stats.live_record_count as u64,
        deleted_record_count: stats.deleted_record_count as u64,
        maintenance: Some(maintenance_status_to_proto(stats.maintenance)),
        query_units: stats
            .query_units
            .into_iter()
            .map(query_unit_stats_to_proto)
            .collect::<Result<Vec<_>, _>>()?,
        database_name: stats.database_name,
    })
}

fn maintenance_status_to_proto(status: MaintenanceStatus) -> proto::MaintenanceStatus {
    proto::MaintenanceStatus {
        pending: status.pending,
        in_progress: status.in_progress,
        last_error: status.last_error,
        completed_runs: status.completed_runs as u64,
    }
}

fn node_role_to_proto(role: NodeRole) -> ProtoNodeRole {
    match role {
        NodeRole::Combined => ProtoNodeRole::Combined,
        NodeRole::Control => ProtoNodeRole::Control,
        NodeRole::Data => ProtoNodeRole::Data,
    }
}

fn query_unit_stats_to_proto(stats: QueryUnitStats) -> Result<proto::QueryUnitStats, LogPoseError> {
    Ok(proto::QueryUnitStats {
        unit_id: stats.unit_id,
        tier: stats.tier,
        index_kind: stats.index_kind,
        min_seq_no: stats.min_seq_no,
        max_seq_no: stats.max_seq_no,
        put_count: stats.put_count as u64,
        delete_count: stats.delete_count as u64,
        approx_bytes: stats.approx_bytes as u64,
        scalar_fields: stats
            .scalar_fields
            .into_iter()
            .map(|(field, stats)| scalar_field_stats_to_proto(stats).map(|stats| (field, stats)))
            .collect::<Result<_, _>>()?,
        artifact_stats: stats
            .artifact_stats
            .into_iter()
            .map(|artifact| proto::QueryUnitArtifactStats {
                kind: artifact.kind,
                file_name: artifact.file_name,
                approx_bytes: artifact.approx_bytes as u64,
            })
            .collect(),
        component_bytes: stats
            .component_bytes
            .into_iter()
            .map(|(key, value)| (key, value as u64))
            .collect(),
    })
}

fn scalar_field_stats_to_proto(
    stats: ScalarFieldStats,
) -> Result<proto::ScalarFieldStats, LogPoseError> {
    Ok(proto::ScalarFieldStats {
        present_count: stats.present_count as u64,
        null_count: stats.null_count as u64,
        value_counts: stats
            .value_counts
            .into_iter()
            .map(|(value, count)| (value, count as u64))
            .collect(),
        min: stats.min.map(scalar_value_to_proto).transpose()?,
        max: stats.max.map(scalar_value_to_proto).transpose()?,
        distinct_count: stats.distinct_count as u64,
    })
}

fn request_auth_from_metadata<T>(request: &Request<T>) -> Result<RequestAuth, LogPoseError> {
    let value = match request.metadata().get("authorization") {
        Some(value) => value,
        None => return Ok(RequestAuth::default()),
    };
    let value = value
        .to_str()
        .map_err(|_| unauthenticated("authorization metadata must be valid ASCII"))?;
    let (scheme, token) = value
        .split_once(' ')
        .ok_or_else(|| unauthenticated("authorization metadata must use the Bearer scheme"))?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.trim().is_empty() {
        return Err(unauthenticated(
            "authorization metadata must use the Bearer scheme",
        ));
    }
    Ok(RequestAuth::bearer_token(token.trim()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_auth::{
        AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
        Principal, PrincipalKind,
    };
    use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
    use logpose_query::{QueryDiagnostics, QueryPlanKind, QueryStageTimings};
    use logpose_types::{DEFAULT_DATABASE_NAME, DistanceMetric};
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tonic::metadata::MetadataValue;
    use tonic_types::StatusExt;

    #[test]
    fn query_diagnostics_to_proto_preserves_ann_fields() {
        let diagnostics = QueryDiagnostics {
            chosen_plan: QueryPlanKind::CooperativeFilteredAnn,
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
        };

        let proto = query_diagnostics_to_proto(diagnostics).expect("conversion should succeed");
        assert_eq!(
            proto::QueryPlanKind::try_from(proto.chosen_plan).expect("plan should decode"),
            proto::QueryPlanKind::CooperativeFilteredAnn
        );
        assert_eq!(
            proto.planner_reason,
            "filtered ann traversal is cheaper than exact scan for this selectivity"
        );
        assert!((proto.estimated_selectivity - 0.25).abs() <= f32::EPSILON);
        assert_eq!(proto.units_considered, 2);
        assert_eq!(proto.units_pruned, 1);
        assert_eq!(proto.units_scanned, 1);
        assert_eq!(proto.candidates_before_filter, 17);
        assert_eq!(proto.candidates_after_filter, 13);
        assert_eq!(proto.candidates_reranked, 7);
        assert_eq!(proto.candidates_merged, 5);
        assert_eq!(proto.rerank_count, 1);
        assert_eq!(proto.fallback_reason.as_deref(), Some("fallback"));
        assert_eq!(proto.unit_scan_mix.get("immutable_ann"), Some(&1));
        assert_eq!(proto.unit_scan_mix.get("mutable_exact"), Some(&2));
        let timings = proto.stage_timings.expect("timings should be present");
        assert_eq!(timings.planning_micros, 11);
        assert_eq!(timings.prefilter_micros, 22);
        assert_eq!(timings.candidate_generation_micros, 33);
        assert_eq!(timings.postfilter_micros, 44);
        assert_eq!(timings.rerank_micros, 55);
        assert_eq!(timings.merge_micros, 66);
    }

    #[tokio::test]
    async fn grpc_service_runs_collection_workflow() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-workflow"))));

        let create = service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed")
            .into_inner();
        assert_eq!(create.name, "documents");

        let write = service
            .upsert_records(Request::new(upsert_request(
                "documents",
                vec![
                    record(
                        "alpha",
                        vec![1.0, 0.0],
                        json!({"kind":"keep","color":"red"}),
                    ),
                    record(
                        "beta",
                        vec![3.0, 0.0],
                        json!({"kind":"drop","color":"blue"}),
                    ),
                    record(
                        "gamma",
                        vec![2.0, 0.0],
                        json!({"kind":"keep","color":"red"}),
                    ),
                ],
            )))
            .await
            .expect("write should succeed")
            .into_inner();
        let write_snapshot = write
            .snapshot
            .expect("write reply should include a write snapshot");
        assert_eq!(write_snapshot.manifest_generation, 0);
        assert_eq!(write_snapshot.visible_seq_no, 3);

        let stats = service
            .get_collection_stats(Request::new(GetCollectionStatsRequest {
                snapshot: Some(write_snapshot),
                read_barrier: None,
                ..get_collection_stats_request("documents")
            }))
            .await
            .expect("stats at write snapshot should succeed")
            .into_inner();
        assert_eq!(stats.visible_seq_no, 3);
        assert_eq!(stats.live_record_count, 3);

        let query = service
            .query_collection(Request::new(QueryCollectionRequest {
                filters: vec![proto::MetadataFilter {
                    field: "kind".to_owned(),
                    value: Some(proto::ScalarValue {
                        kind: Some(proto::scalar_value::Kind::StringValue("keep".to_owned())),
                    }),
                }],
                ..query_collection_request("documents", vec![1.0, 0.0], 3)
            }))
            .await
            .expect("query should succeed")
            .into_inner();
        assert_eq!(
            query
                .matches
                .iter()
                .map(|candidate| candidate.id.as_str())
                .collect::<Vec<_>>(),
            vec!["gamma", "alpha"]
        );

        let stats = service
            .get_collection_stats(Request::new(get_collection_stats_request("documents")))
            .await
            .expect("stats should succeed")
            .into_inner();
        assert_eq!(stats.live_record_count, 3);
        assert_eq!(stats.deleted_record_count, 0);
        assert_eq!(stats.mutable_op_count, 3);
        assert_eq!(stats.segment_count, 0);
        assert_eq!(
            stats
                .maintenance
                .expect("maintenance should be present")
                .completed_runs,
            0
        );
        assert_eq!(stats.query_units.len(), 1);

        let flush = service
            .flush_collection(Request::new(flush_collection_request("documents")))
            .await
            .expect("flush should succeed")
            .into_inner();
        assert!(flush.manifest_generation >= 1);

        let compact = service
            .compact_collection(Request::new(compact_collection_request("documents")))
            .await
            .expect("compact should succeed")
            .into_inner();
        assert!(compact.manifest_generation >= flush.manifest_generation);

        let inspect = service
            .inspect_collection(Request::new(inspect_collection_request(
                "documents",
                proto::InspectTarget::Manifest,
                String::new(),
            )))
            .await
            .expect("inspect should succeed")
            .into_inner();
        assert_eq!(inspect.target, "manifest");
    }

    #[tokio::test]
    async fn grpc_service_supports_read_barriers() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-read-barrier"))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        let write = service
            .upsert_records(Request::new(upsert_request(
                "documents",
                vec![record("alpha", vec![1.0, 0.0], json!({"kind":"keep"}))],
            )))
            .await
            .expect("write should succeed")
            .into_inner();
        let write_snapshot = write
            .snapshot
            .expect("write reply should include a snapshot");

        let flush = service
            .flush_collection(Request::new(flush_collection_request("documents")))
            .await
            .expect("flush should succeed")
            .into_inner();

        let query = service
            .query_collection(Request::new(QueryCollectionRequest {
                read_barrier: Some(write_snapshot),
                ..query_collection_request("documents", vec![1.0, 0.0], 1)
            }))
            .await
            .expect("query with a satisfied read barrier should succeed")
            .into_inner();
        let query_snapshot = query
            .snapshot
            .expect("query reply should include a snapshot");
        assert_eq!(
            query_snapshot.manifest_generation,
            flush.manifest_generation
        );
        assert_eq!(query_snapshot.visible_seq_no, flush.visible_seq_no);
        assert_eq!(query.matches[0].id, "alpha");

        let stats = service
            .get_collection_stats(Request::new(GetCollectionStatsRequest {
                read_barrier: Some(write_snapshot),
                ..get_collection_stats_request("documents")
            }))
            .await
            .expect("stats with a satisfied read barrier should succeed")
            .into_inner();
        assert_eq!(stats.manifest_generation, flush.manifest_generation);
        assert_eq!(stats.visible_seq_no, flush.visible_seq_no);

        let error = service
            .query_collection(Request::new(QueryCollectionRequest {
                read_barrier: Some(proto::Snapshot {
                    manifest_generation: 0,
                    visible_seq_no: 2,
                }),
                ..query_collection_request("documents", vec![1.0, 0.0], 1)
            }))
            .await
            .expect_err("unsatisfied read barrier should fail");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.get_details_retry_info().is_none());
        assert_eq!(
            error
                .get_details_error_info()
                .map(|info| info.reason)
                .as_deref(),
            Some("READ_BARRIER_NOT_SATISFIED")
        );
    }

    #[tokio::test]
    async fn grpc_service_supports_wal_and_segment_inspection_targets() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-inspect-targets"))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        service
            .upsert_records(Request::new(upsert_request(
                "documents",
                vec![
                    record("alpha", vec![1.0, 0.0], json!({"kind":"keep"})),
                    record("beta", vec![0.0, 1.0], json!({"kind":"drop"})),
                ],
            )))
            .await
            .expect("write should succeed");

        service
            .flush_collection(Request::new(flush_collection_request("documents")))
            .await
            .expect("flush should succeed");

        service
            .delete_records(Request::new(delete_request("documents", &["alpha"])))
            .await
            .expect("delete should succeed");

        let manifest = service
            .inspect_collection(Request::new(inspect_collection_request(
                "documents",
                proto::InspectTarget::Manifest,
                String::new(),
            )))
            .await
            .expect("manifest inspect should succeed")
            .into_inner();
        assert_eq!(manifest.target, "manifest");
        let manifest_segments = manifest
            .payload_json
            .parse::<Value>()
            .expect("manifest payload should be valid json");
        let segment_id = manifest_segments["segments"][0]["segment_id"]
            .as_str()
            .expect("segment id should be a string")
            .to_owned();

        let wal = service
            .inspect_collection(Request::new(inspect_collection_request(
                "documents",
                proto::InspectTarget::Wal,
                String::new(),
            )))
            .await
            .expect("wal inspect should succeed")
            .into_inner();
        assert_eq!(wal.target, "wal");
        let wal_payload = wal
            .payload_json
            .parse::<Value>()
            .expect("wal payload should be valid json");
        // The delete after the flush only set a deletion bit on the segment's row, so the
        // memtable holds no record, but one operation sits above the checkpoint.
        assert!(
            wal_payload["records"]
                .as_array()
                .expect("wal records should be an array")
                .is_empty()
        );
        assert_eq!(
            wal_payload["visible_seq_no"].as_u64(),
            wal_payload["checkpoint_seq_no"]
                .as_u64()
                .map(|seq_no| seq_no + 1)
        );

        let segment = service
            .inspect_collection(Request::new(inspect_collection_request(
                "documents",
                proto::InspectTarget::Segment,
                segment_id.clone(),
            )))
            .await
            .expect("segment inspect should succeed")
            .into_inner();
        assert_eq!(segment.target, format!("segment:{segment_id}"));
        let segment_payload = segment
            .payload_json
            .parse::<Value>()
            .expect("segment payload should be valid json");
        assert_eq!(
            segment_payload["records"]
                .as_array()
                .expect("segment records should be an array")
                .len(),
            2
        );

        let maintenance = service
            .inspect_collection(Request::new(inspect_collection_request(
                "documents",
                proto::InspectTarget::Maintenance,
                String::new(),
            )))
            .await
            .expect("maintenance inspect should succeed")
            .into_inner();
        assert_eq!(maintenance.target, "maintenance");
    }

    #[tokio::test]
    async fn grpc_metadata_reports_build_identity_fields() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-metadata"))));

        let metadata = service
            .get_metadata(Request::new(GetMetadataRequest {}))
            .await
            .expect("metadata should succeed")
            .into_inner();

        assert_eq!(metadata.product, "LogPose");
        assert_eq!(metadata.node_name, "grpc-metadata");
        assert!(!metadata.version.is_empty());
        assert!(!metadata.git_sha.is_empty());
        assert_eq!(metadata.profile, "debug");
    }

    #[tokio::test]
    async fn grpc_runtime_status_requires_bearer_token_when_auth_is_configured() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(auth_test_config(
            "grpc-auth-runtime",
        ))));

        let unauthorized = service
            .get_runtime_status(Request::new(GetRuntimeStatusRequest {}))
            .await
            .expect_err("missing token should be rejected");
        assert_eq!(unauthorized.code(), tonic::Code::Unauthenticated);

        let authorized = service
            .get_runtime_status(authorized_request(
                GetRuntimeStatusRequest {},
                "operator-secret",
            ))
            .await
            .expect("operator token should be accepted")
            .into_inner();
        assert_eq!(
            authorized
                .metadata
                .expect("metadata should be present")
                .node_name,
            "grpc-auth-runtime"
        );
    }

    #[tokio::test]
    async fn grpc_database_rpcs_round_trip_with_operator_auth() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(auth_test_config(
            "grpc-namespace-auth",
        ))));

        let unauthorized = service
            .list_databases(Request::new(ListDatabasesRequest {}))
            .await
            .expect_err("missing token should be rejected");
        assert_eq!(unauthorized.code(), tonic::Code::Unauthenticated);

        let put_database = service
            .put_database(authorized_request(
                put_database_request("analytics"),
                "operator-secret",
            ))
            .await
            .expect("operator token should create database")
            .into_inner();
        assert_eq!(put_database.name, "analytics");

        let get_database = service
            .get_database(authorized_request(
                get_database_request("analytics"),
                "operator-secret",
            ))
            .await
            .expect("operator token should read database")
            .into_inner();
        assert_eq!(get_database.name, "analytics");

        let databases = service
            .list_databases(authorized_request(
                list_databases_request(),
                "operator-secret",
            ))
            .await
            .expect("operator token should list databases")
            .into_inner();
        assert_eq!(databases.databases.len(), 2);
        assert!(
            databases
                .databases
                .iter()
                .any(|database| database.name == "default" && database.is_default),
            "default database should be bootstrapped lazily"
        );
        assert!(
            databases
                .databases
                .iter()
                .any(|database| database.name == "analytics" && !database.is_default),
            "created database should still be listed"
        );
    }

    #[tokio::test]
    async fn grpc_database_policy_rpcs_round_trip_and_map_service_errors() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-policy"))));

        let put = service
            .put_database_policy(Request::new(put_database_policy_request("default")))
            .await
            .expect("put policy should succeed")
            .into_inner();
        assert_eq!(put.database_name, "default");
        assert_eq!(
            put.authentication_mode,
            proto::AuthenticationMode::ExternalToken as i32
        );
        assert_eq!(put.role_bindings.len(), 2);

        let get = service
            .get_database_policy(Request::new(get_database_policy_request("default")))
            .await
            .expect("get policy should succeed")
            .into_inner();
        assert_eq!(get, put);

        let data_only = GrpcLogPoseService::new(Arc::new(AppState::new(test_config_with_role(
            "grpc-policy-data-only",
            NodeRole::Data,
        ))));
        let error = data_only
            .put_database_policy(Request::new(put_database_policy_request("default")))
            .await
            .expect_err("data-only node should reject policy mutation");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error
                .message()
                .contains("cannot accept control-plane database mutations")
        );
    }

    #[tokio::test]
    async fn grpc_read_only_principals_can_read_but_not_write_when_auth_is_configured() {
        let state = Arc::new(AppState::new(auth_test_config("grpc-auth-readonly")));
        state
            .control
            .set_database_access_policy(read_only_policy("default", "reader"))
            .await
            .expect("database policy should persist");
        state
            .control
            .create_collection(storage_create_collection_request(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        let service = GrpcLogPoseService::new(state);

        service
            .get_collection_stats(authorized_request(
                get_collection_stats_request("documents"),
                "reader-secret",
            ))
            .await
            .expect("read-only principal should read stats");

        let error = service
            .upsert_records(authorized_request(
                upsert_request(
                    "documents",
                    vec![record("alpha", vec![1.0, 0.0], json!({}))],
                ),
                "reader-secret",
            ))
            .await
            .expect_err("read-only principal should not write");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn grpc_runtime_status_reports_control_plane_summary() {
        let state = Arc::new(AppState::new(test_config("grpc-runtime-status")));
        state
            .control
            .create_collection(storage_create_collection_request(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        let service = GrpcLogPoseService::new(state);

        let status = service
            .get_runtime_status(Request::new(GetRuntimeStatusRequest {}))
            .await
            .expect("runtime status should succeed")
            .into_inner();

        assert_eq!(status.role, proto::NodeRole::Combined as i32);
        assert_eq!(status.storage_engine, "local");
        assert_eq!(status.collection_count, 1);
        assert_eq!(status.collections.len(), 1);
        assert_eq!(status.collections[0].collection_name, "documents");
        assert_eq!(
            status.collections[0].assigned_role,
            proto::NodeRole::Data as i32
        );
        assert_eq!(status.collections[0].route_kind, "local");
        assert!(status.coordination.is_none());
    }

    #[test]
    fn grpc_runtime_status_reply_includes_coordination_when_present() {
        let reply = runtime_status_reply_from_domain(NodeRuntimeStatus {
            metadata: logpose_types::NodeMetadata {
                product: "LogPose".to_owned(),
                node_name: "grpc-node".to_owned(),
                version: "test".to_owned(),
                git_sha: "sha".to_owned(),
                profile: "debug".to_owned(),
            },
            role: NodeRole::Combined,
            rest_endpoint: "http://127.0.0.1:8080".to_owned(),
            grpc_endpoint: "http://127.0.0.1:50051".to_owned(),
            storage_engine: "local+etcd-metadata".to_owned(),
            control_plane_ready: true,
            data_plane_ready: true,
            collection_count: 0,
            collections: Vec::new(),
            coordination: Some(logpose_types::CoordinationStatus {
                cluster_name: "prod-cluster".to_owned(),
                membership_registered: true,
                membership_lease_id: Some(17),
                registered_members: vec!["grpc-node".to_owned(), "grpc-peer".to_owned()],
                leader_node: Some("grpc-node".to_owned()),
                is_local_leader: true,
                leadership_lease_id: Some(23),
                last_error: Some("warn".to_owned()),
            }),
            maintenance: MaintenanceBacklog::default(),
        });

        let coordination = reply
            .coordination
            .expect("coordination should be serialized");
        assert_eq!(coordination.cluster_name, "prod-cluster");
        assert_eq!(coordination.membership_lease_id, Some(17));
        assert_eq!(coordination.leadership_lease_id, Some(23));
        assert_eq!(
            coordination.registered_members,
            vec!["grpc-node".to_owned(), "grpc-peer".to_owned()]
        );
        assert_eq!(coordination.leader_node.as_deref(), Some("grpc-node"));
        assert!(coordination.is_local_leader);
        assert_eq!(coordination.last_error.as_deref(), Some("warn"));
    }

    #[tokio::test]
    async fn grpc_collection_placement_reports_local_assignment() {
        let state = Arc::new(AppState::new(test_config("grpc-placement")));
        state
            .control
            .create_collection(storage_create_collection_request(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        let service = GrpcLogPoseService::new(state);

        let placement = service
            .get_collection_placement(Request::new(get_collection_placement_request("documents")))
            .await
            .expect("placement should succeed")
            .into_inner();

        assert_eq!(placement.collection_name, "documents");
        assert_eq!(placement.assigned_node, "grpc-placement");
        assert_eq!(placement.assigned_role, proto::NodeRole::Data as i32);
        assert_eq!(placement.route_kind, "local");
    }

    #[test]
    fn collection_placement_reply_serializes_owner_fields_when_present() {
        let reply = collection_placement_reply_from_domain(CollectionPlacement {
            collection_id: logpose_types::CollectionId::default(),
            database_name: "analytics".to_owned(),
            collection_name: "documents".to_owned(),
            assigned_node: "owner-a".to_owned(),
            assigned_role: NodeRole::Data,
            owner_node: Some("owner-b".to_owned()),
            ownership_epoch: Some(2),
            route_kind: "recorded".to_owned(),
            route_reason: "ownership epoch 2 is assigned to node 'owner-b'".to_owned(),
        });

        assert_eq!(reply.owner_node.as_deref(), Some("owner-b"));
        assert_eq!(reply.ownership_epoch, Some(2));
    }

    #[tokio::test]
    async fn data_only_nodes_reject_control_plane_collection_creation() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config_with_role(
            "grpc-data-only",
            NodeRole::Data,
        ))));

        let error = service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect_err("data-only node should reject collection creation");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains(
            "is running as 'data' and cannot accept control-plane collection lifecycle mutations"
        ));
    }

    #[tokio::test]
    async fn control_only_nodes_reject_control_plane_collection_creation() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config_with_role(
            "grpc-control-create",
            NodeRole::Control,
        ))));

        let error = service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect_err("control-only node should reject collection creation");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains(
            "is running as 'control' and cannot accept control-plane collection lifecycle mutations"
        ));
    }

    #[tokio::test]
    async fn control_only_nodes_reject_data_plane_grpc_operations() {
        let root = unique_temp_dir("grpc-control-only");
        let initial = Arc::new(AppState::new(test_config_with_root(
            "grpc-control-only",
            NodeRole::Combined,
            root.clone(),
        )));
        initial
            .control
            .create_collection(storage_create_collection_request(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        drop(initial);

        let state = Arc::new(AppState::new(test_config_with_root(
            "grpc-control-only",
            NodeRole::Control,
            root,
        )));
        let service = GrpcLogPoseService::new(state);

        let errors = vec![
            (
                "write",
                service
                    .upsert_records(Request::new(upsert_request(
                        "documents",
                        vec![record("alpha", vec![1.0, 0.0], json!({"kind":"keep"}))],
                    )))
                    .await
                    .expect_err("control-only node should reject writes"),
            ),
            (
                "query",
                service
                    .query_collection(Request::new(query_collection_request(
                        "documents",
                        vec![1.0, 0.0],
                        1,
                    )))
                    .await
                    .expect_err("control-only node should reject queries"),
            ),
            (
                "stats",
                service
                    .get_collection_stats(Request::new(get_collection_stats_request("documents")))
                    .await
                    .expect_err("control-only node should reject stats"),
            ),
            (
                "flush",
                service
                    .flush_collection(Request::new(flush_collection_request("documents")))
                    .await
                    .expect_err("control-only node should reject flush"),
            ),
            (
                "compact",
                service
                    .compact_collection(Request::new(compact_collection_request("documents")))
                    .await
                    .expect_err("control-only node should reject compact"),
            ),
            (
                "inspect",
                service
                    .inspect_collection(Request::new(inspect_collection_request(
                        "documents",
                        InspectTarget::Manifest,
                        String::new(),
                    )))
                    .await
                    .expect_err("control-only node should reject inspect"),
            ),
        ];

        for (operation, error) in errors {
            assert_eq!(
                error.code(),
                tonic::Code::FailedPrecondition,
                "{operation} should be rejected on control-only nodes"
            );
            assert!(
                error.message().contains("data-plane operations"),
                "{operation} should explain the role mismatch"
            );
        }
    }

    #[tokio::test]
    async fn recorded_remote_assignments_reject_data_plane_grpc_operations() {
        let root = unique_temp_dir("grpc-recorded-route");
        let initial = Arc::new(AppState::new(test_config_with_root(
            "grpc-recorded-node-a",
            NodeRole::Combined,
            root.clone(),
        )));
        initial
            .control
            .create_collection(storage_create_collection_request(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        drop(initial);

        let state = Arc::new(AppState::new(test_config_with_root(
            "grpc-recorded-node-b",
            NodeRole::Combined,
            root,
        )));
        let service = GrpcLogPoseService::new(state);

        let errors = vec![
            (
                "write",
                service
                    .upsert_records(Request::new(upsert_request(
                        "documents",
                        vec![record("alpha", vec![1.0, 0.0], json!({"kind":"keep"}))],
                    )))
                    .await
                    .expect_err("recorded remote writes should be rejected"),
            ),
            (
                "query",
                service
                    .query_collection(Request::new(query_collection_request(
                        "documents",
                        vec![1.0, 0.0],
                        1,
                    )))
                    .await
                    .expect_err("recorded remote queries should be rejected"),
            ),
            (
                "stats",
                service
                    .get_collection_stats(Request::new(get_collection_stats_request("documents")))
                    .await
                    .expect_err("recorded remote stats should be rejected"),
            ),
            (
                "flush",
                service
                    .flush_collection(Request::new(flush_collection_request("documents")))
                    .await
                    .expect_err("recorded remote flush should be rejected"),
            ),
            (
                "compact",
                service
                    .compact_collection(Request::new(compact_collection_request("documents")))
                    .await
                    .expect_err("recorded remote compaction should be rejected"),
            ),
            (
                "inspect",
                service
                    .inspect_collection(Request::new(inspect_collection_request(
                        "documents",
                        InspectTarget::Manifest,
                        String::new(),
                    )))
                    .await
                    .expect_err("recorded remote inspect should be rejected"),
            ),
        ];

        for (operation, error) in errors {
            assert_eq!(
                error.code(),
                tonic::Code::Unavailable,
                "{operation} should be rejected for recorded remote assignments"
            );
            assert_eq!(
                error
                    .get_details_error_info()
                    .map(|info| info.reason)
                    .as_deref(),
                Some("NOT_OWNER"),
                "{operation} should carry the NOT_OWNER reason"
            );
            assert!(
                error.message().contains("not locally served"),
                "{operation} should explain the recorded placement mismatch"
            );
        }
    }

    #[tokio::test]
    async fn grpc_service_maps_missing_collections_to_not_found() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-missing"))));

        let error = service
            .get_collection(Request::new(get_collection_request("missing")))
            .await
            .expect_err("missing collection should error");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn grpc_service_maps_missing_collection_placement_to_not_found() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config(
            "grpc-missing-placement",
        ))));

        let error = service
            .get_collection_placement(Request::new(get_collection_placement_request("missing")))
            .await
            .expect_err("missing collection placement should error");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn grpc_service_rejects_zero_dimensions_for_collection_creation() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-zero-dimensions"))));

        let error = service
            .create_collection(Request::new(create_collection_request(
                "documents",
                0,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect_err("zero dimensions should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("has 0 dimensions"), "{error:?}");
        assert_eq!(violation_fields(&error), vec!["vectors[0].dimensions"]);
    }

    #[tokio::test]
    async fn grpc_service_rejects_unknown_inspect_targets() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-invalid-target"))));

        let error = service
            .inspect_collection(Request::new(InspectCollectionRequest {
                target: 999,
                ..inspect_collection_request(
                    "documents",
                    proto::InspectTarget::Manifest,
                    String::new(),
                )
            }))
            .await
            .expect_err("unknown inspect target should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn grpc_query_filters_preserve_large_integer_precision() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-large-integers"))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        service
            .upsert_records(Request::new(upsert_request(
                "documents",
                vec![
                    record(
                        "lower",
                        vec![1.0, 0.0],
                        json!({"score": 9_007_199_254_740_992_u64}),
                    ),
                    record(
                        "higher",
                        vec![2.0, 0.0],
                        json!({"score": 9_007_199_254_740_993_u64}),
                    ),
                ],
            )))
            .await
            .expect("write should succeed");

        let query = service
            .query_collection(Request::new(QueryCollectionRequest {
                filters: vec![proto::MetadataFilter {
                    field: "score".to_owned(),
                    value: Some(proto::ScalarValue {
                        kind: Some(proto::scalar_value::Kind::Uint64Value(9007199254740993)),
                    }),
                }],
                ..query_collection_request("documents", vec![1.0, 0.0], 5)
            }))
            .await
            .expect("query should succeed")
            .into_inner();

        assert_eq!(
            query
                .matches
                .iter()
                .map(|candidate| candidate.id.as_str())
                .collect::<Vec<_>>(),
            vec!["higher"]
        );
    }

    #[tokio::test]
    async fn grpc_query_supports_predicate_and_profile_diagnostics() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config(
            "grpc-predicate-profile",
        ))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        service
            .upsert_records(Request::new(upsert_request(
                "documents",
                vec![
                    record("alpha", vec![1.0, 0.0], json!({"kind":"keep","version":1})),
                    record("beta", vec![2.0, 0.0], json!({"kind":"drop","version":2})),
                    record("gamma", vec![3.0, 0.0], json!({"kind":"drop","version":3})),
                    record("delta", vec![4.0, 0.0], json!({"kind":"drop","version":4})),
                    record(
                        "epsilon",
                        vec![5.0, 0.0],
                        json!({"kind":"keep","version":5}),
                    ),
                ],
            )))
            .await
            .expect("write should succeed");

        let query = service
            .query_collection(Request::new(QueryCollectionRequest {
                predicate: Some(proto::Predicate {
                    node: Some(proto::predicate::Node::Comparison(
                        proto::PredicateComparison {
                            field: "kind".to_owned(),
                            operator: proto::PredicateOperator::Eq as i32,
                            value: Some(proto::ScalarValue {
                                kind: Some(proto::scalar_value::Kind::StringValue(
                                    "keep".to_owned(),
                                )),
                            }),
                        },
                    )),
                }),
                explain: proto::ExplainMode::Profile as i32,
                ..query_collection_request("documents", vec![1.0, 0.0], 1)
            }))
            .await
            .expect("query should succeed")
            .into_inner();

        assert_eq!(
            query
                .matches
                .iter()
                .map(|candidate| candidate.id.as_str())
                .collect::<Vec<_>>(),
            vec!["epsilon"]
        );
        let diagnostics = query.diagnostics.expect("diagnostics should be present");
        assert_eq!(
            proto::QueryPlanKind::try_from(diagnostics.chosen_plan).expect("plan should decode"),
            proto::QueryPlanKind::PredicateFirstExact
        );
        assert!(diagnostics.stage_timings.is_some());
    }

    #[tokio::test]
    async fn grpc_query_rejects_malformed_predicates() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config(
            "grpc-invalid-predicate",
        ))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        let error = service
            .query_collection(Request::new(QueryCollectionRequest {
                predicate: Some(proto::Predicate {
                    node: Some(proto::predicate::Node::Comparison(
                        proto::PredicateComparison {
                            field: "kind".to_owned(),
                            operator: proto::PredicateOperator::Eq as i32,
                            value: None,
                        },
                    )),
                }),
                ..query_collection_request("documents", vec![1.0, 0.0], 1)
            }))
            .await
            .expect_err("malformed predicate should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn grpc_query_rejects_empty_logical_predicates() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config(
            "grpc-empty-logical-predicate",
        ))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        let error = service
            .query_collection(Request::new(QueryCollectionRequest {
                predicate: Some(proto::Predicate {
                    node: Some(proto::predicate::Node::And(proto::PredicateList {
                        children: Vec::new(),
                    })),
                }),
                ..query_collection_request("documents", vec![1.0, 0.0], 1)
            }))
            .await
            .expect_err("empty logical predicate should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn grpc_query_rejects_unknown_explain_modes() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-invalid-explain"))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        let error = service
            .query_collection(Request::new(QueryCollectionRequest {
                explain: 99,
                ..query_collection_request("documents", vec![1.0, 0.0], 1)
            }))
            .await
            .expect_err("unknown explain mode should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn grpc_query_rejects_zero_top_k() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-zero-top-k"))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        let error = service
            .query_collection(Request::new(query_collection_request(
                "documents",
                vec![1.0, 0.0],
                0,
            )))
            .await
            .expect_err("zero top_k should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("top_k must be greater than 0"));
    }

    #[tokio::test]
    async fn grpc_requests_must_name_their_database_and_collection() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-selector"))));

        let error = service
            .upsert_records(Request::new(UpsertRecordsRequest {
                database_name: String::new(),
                ..upsert_request("documents", Vec::new())
            }))
            .await
            .expect_err("an empty database name is rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(violation_fields(&error), vec!["database_name"]);

        let error = service
            .get_collection(Request::new(GetCollectionRequest {
                collection_name: " ".to_owned(),
                ..get_collection_request("documents")
            }))
            .await
            .expect_err("a blank collection name is rejected");
        assert_eq!(violation_fields(&error), vec!["collection_name"]);

        let error = service
            .list_collections(Request::new(ListCollectionsRequest {
                database_name: String::new(),
            }))
            .await
            .expect_err("listing needs a database");
        assert_eq!(violation_fields(&error), vec!["database_name"]);
    }

    #[tokio::test]
    async fn grpc_creates_describes_lists_and_drops_typed_collections() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-collections"))));
        service
            .put_database(Request::new(put_database_request("shop")))
            .await
            .expect("database should be created");

        let created = service
            .create_collection(Request::new(products_request("shop")))
            .await
            .expect("typed collection should be created")
            .into_inner();
        assert_eq!(created.database_name, "shop");
        assert_eq!(created.name, "products");
        let schema = created.schema.clone().expect("schema is returned");
        assert_eq!(schema.schema_version, 1);
        assert!(schema.dynamic_fields);
        let primary_key = schema.primary_key.clone().expect("primary key");
        assert_eq!(
            (
                primary_key.id,
                primary_key.name.as_str(),
                primary_key.r#type
            ),
            (0, "sku", proto::PrimaryKeyType::Int64 as i32)
        );
        assert_eq!(schema.vectors[0].name, "embedding");
        assert_eq!(schema.vectors[0].dimensions, 3);
        assert_eq!(
            schema.vectors[0].metric,
            proto::DistanceMetric::Cosine as i32
        );
        let tenant = &schema.fields[0];
        assert_eq!(tenant.name, "tenant");
        assert!(!tenant.nullable);
        assert_eq!(tenant.index, proto::FieldIndex::Inverted as i32);
        assert_eq!(
            schema.fields[1].index,
            proto::FieldIndex::InvertedAndSorted as i32,
            "auto picks inverted and sorted for numbers"
        );
        assert_eq!(schema.fields[4].index, proto::FieldIndex::None as i32);
        assert_eq!(
            convert::schema_from_proto(schema.clone())
                .expect("the reply is a valid schema")
                .fields()
                .len(),
            5
        );

        let described = service
            .get_collection(Request::new(GetCollectionRequest {
                database_name: "shop".to_owned(),
                collection_name: "products".to_owned(),
            }))
            .await
            .expect("describe should succeed")
            .into_inner();
        assert_eq!(described, created);

        let listed = service
            .list_collections(Request::new(ListCollectionsRequest {
                database_name: "shop".to_owned(),
            }))
            .await
            .expect("list should succeed")
            .into_inner();
        assert_eq!(listed.collections, vec![created]);
        let other = service
            .list_collections(Request::new(ListCollectionsRequest {
                database_name: "default".to_owned(),
            }))
            .await
            .expect("list should succeed")
            .into_inner();
        assert!(other.collections.is_empty());
        let error = service
            .list_collections(Request::new(ListCollectionsRequest {
                database_name: "nowhere".to_owned(),
            }))
            .await
            .expect_err("a missing database has no collections to list");
        assert_eq!(error.code(), tonic::Code::NotFound);

        let error = service
            .drop_database(Request::new(DropDatabaseRequest {
                database_name: "shop".to_owned(),
            }))
            .await
            .expect_err("a database with a collection cannot be dropped");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("products"), "{error:?}");

        let dropped = service
            .drop_collection(Request::new(DropCollectionRequest {
                database_name: "shop".to_owned(),
                collection_name: "products".to_owned(),
            }))
            .await
            .expect("drop should succeed")
            .into_inner();
        assert_eq!(dropped.collection_name, "products");
        let error = service
            .get_collection(Request::new(GetCollectionRequest {
                database_name: "shop".to_owned(),
                collection_name: "products".to_owned(),
            }))
            .await
            .expect_err("a dropped collection is gone");
        assert_eq!(error.code(), tonic::Code::NotFound);
        let error = service
            .drop_collection(Request::new(DropCollectionRequest {
                database_name: "shop".to_owned(),
                collection_name: "products".to_owned(),
            }))
            .await
            .expect_err("dropping twice fails");
        assert_eq!(error.code(), tonic::Code::NotFound);

        service
            .drop_database(Request::new(DropDatabaseRequest {
                database_name: "shop".to_owned(),
            }))
            .await
            .expect("an empty database can be dropped");
        let error = service
            .get_database(Request::new(get_database_request("shop")))
            .await
            .expect_err("a dropped database is gone");
        assert_eq!(error.code(), tonic::Code::NotFound);
        let error = service
            .drop_database(Request::new(DropDatabaseRequest {
                database_name: "default".to_owned(),
            }))
            .await
            .expect_err("the default database cannot be dropped");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn grpc_create_collection_names_the_invalid_schema_field() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-bad-schema"))));
        type Mutation = Box<dyn Fn(&mut CreateCollectionRequest)>;
        let cases: Vec<(&str, Mutation)> = vec![
            (
                "vectors[0].dimensions",
                Box::new(|request| request.vectors[0].dimensions = 0),
            ),
            (
                "fields[1].name",
                Box::new(|request| request.fields[1].name = "tenant".to_owned()),
            ),
            (
                "fields[0].index",
                Box::new(|request| {
                    request.fields[0].r#type = proto::FieldType::Bool as i32;
                    request.fields[0].index = proto::FieldIndex::Sorted as i32;
                }),
            ),
            (
                "fields[2].type",
                Box::new(|request| request.fields[2].r#type = 0),
            ),
            (
                "primary_key.type",
                Box::new(|request| {
                    if let Some(primary_key) = request.primary_key.as_mut() {
                        primary_key.r#type = 0;
                    }
                }),
            ),
            (
                "primary_key",
                Box::new(|request| request.primary_key = None),
            ),
            ("vectors", Box::new(|request| request.vectors.clear())),
            (
                "primary_key.name",
                Box::new(|request| {
                    if let Some(primary_key) = request.primary_key.as_mut() {
                        primary_key.name = "$extra".to_owned();
                    }
                }),
            ),
        ];
        for (field, change) in cases {
            let mut request = products_request("default");
            change(&mut request);
            let error = service
                .create_collection(Request::new(request))
                .await
                .expect_err("an invalid schema is rejected");
            assert_eq!(error.code(), tonic::Code::InvalidArgument, "{field}");
            assert_eq!(violation_fields(&error), vec![field], "{error:?}");
        }
    }

    #[tokio::test]
    async fn grpc_typed_records_round_trip_through_upsert_get_update_and_delete() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-typed"))));
        service
            .create_collection(Request::new(products_request("default")))
            .await
            .expect("collection should be created");

        let timestamp = 1_790_000_000_000_000_i64;
        let widget = proto::Record {
            pk: Some(int_key(1)),
            vectors: [(
                "embedding".to_owned(),
                proto::Vector {
                    values: vec![3.0, 0.0, 4.0],
                },
            )]
            .into_iter()
            .collect(),
            fields: [
                ("tenant".to_owned(), string_value("acme")),
                (
                    "price".to_owned(),
                    proto::Value {
                        kind: Some(proto::value::Kind::Float64Value(9.5)),
                    },
                ),
                (
                    "tags".to_owned(),
                    proto::Value {
                        kind: Some(proto::value::Kind::ArrayValue(proto::ValueArray {
                            values: vec![string_value("a"), string_value("b")],
                        })),
                    },
                ),
                (
                    "updated_at".to_owned(),
                    proto::Value {
                        kind: Some(proto::value::Kind::TimestampMicros(timestamp)),
                    },
                ),
                (
                    "attrs".to_owned(),
                    proto::Value {
                        kind: Some(proto::value::Kind::JsonValue(convert::json_to_proto(
                            &json!({"size": 3, "big": u64::MAX}),
                        ))),
                    },
                ),
                // Undeclared, so it is kept in `$extra` as JSON.
                ("color".to_owned(), string_value("red")),
            ]
            .into_iter()
            .collect(),
            extra: Some(convert::json_object_to_proto(
                json!({"origin": "eu"}).as_object().expect("object"),
            )),
        };
        let gadget = proto::Record {
            pk: Some(int_key(2)),
            vectors: [(
                "embedding".to_owned(),
                proto::Vector {
                    values: vec![0.0, 1.0, 0.0],
                },
            )]
            .into_iter()
            .collect(),
            fields: [("tenant".to_owned(), string_value("acme"))]
                .into_iter()
                .collect(),
            extra: None,
        };
        let ack = service
            .upsert_records(Request::new(UpsertRecordsRequest {
                database_name: default_database_name(),
                collection_name: "products".to_owned(),
                records: vec![widget, gadget],
            }))
            .await
            .expect("typed upsert should succeed")
            .into_inner();
        assert_eq!(ack.applied_ops, 2);
        assert_eq!(ack.database_name, "default");
        assert_eq!(ack.collection_name, "products");

        let fetched = service
            .get_records(Request::new(get_request(
                "products",
                vec![int_key(1), int_key(3), int_key(2)],
            )))
            .await
            .expect("get should succeed")
            .into_inner();
        assert_eq!(fetched.missing_keys, vec![int_key(3)]);
        assert_eq!(
            fetched.snapshot.expect("snapshot").visible_seq_no,
            ack.last_seq_no
        );
        let records = convert::records_from_proto(fetched.records, "records").expect("decode");
        assert_eq!(records.len(), 2);
        let widget = &records[0];
        assert_eq!(
            widget.vectors["embedding"],
            vec![0.6, 0.0, 0.8],
            "cosine vectors are normalized when written"
        );
        assert_eq!(
            widget.fields["price"],
            logpose_types::value::Value::Float64(9.5)
        );
        assert_eq!(
            widget.fields["tags"],
            logpose_types::value::Value::Array(vec![
                logpose_types::value::Value::String("a".to_owned()),
                logpose_types::value::Value::String("b".to_owned()),
            ])
        );
        assert_eq!(
            widget.fields["updated_at"],
            logpose_types::value::Value::Timestamp(
                logpose_types::value::Timestamp::from_micros(timestamp).expect("timestamp")
            )
        );
        assert_eq!(
            widget.fields["attrs"],
            logpose_types::value::Value::Json(json!({"size": 3, "big": u64::MAX}))
        );
        assert_eq!(
            Value::Object(widget.extra.clone()),
            json!({"color": "red", "origin": "eu"})
        );
        assert_eq!(records[1].pk, logpose_types::record::PrimaryKey::Int64(2));

        let projected = service
            .get_records(Request::new(GetRecordsRequest {
                output_fields: vec!["price".to_owned(), "$extra".to_owned()],
                ..get_request("products", vec![int_key(1)])
            }))
            .await
            .expect("projected get should succeed")
            .into_inner();
        let projected = convert::records_from_proto(projected.records, "records").expect("decode");
        assert!(projected[0].vectors.is_empty());
        assert_eq!(
            projected[0].fields.keys().collect::<Vec<_>>(),
            vec!["price"]
        );
        assert_eq!(projected[0].extra.len(), 2);

        service
            .update_records(Request::new(UpdateRecordsRequest {
                database_name: default_database_name(),
                collection_name: "products".to_owned(),
                records: vec![proto::RecordUpdate {
                    pk: Some(int_key(1)),
                    vectors: Default::default(),
                    fields: [
                        (
                            "price".to_owned(),
                            proto::Value {
                                kind: Some(proto::value::Kind::NullValue(0)),
                            },
                        ),
                        (
                            "tags".to_owned(),
                            proto::Value {
                                kind: Some(proto::value::Kind::ArrayValue(proto::ValueArray {
                                    values: vec![string_value("c")],
                                })),
                            },
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    extra: Some(convert::json_object_to_proto(
                        json!({"origin": null}).as_object().expect("object"),
                    )),
                }],
            }))
            .await
            .expect("partial update should succeed");
        let updated = service
            .get_records(Request::new(get_request("products", vec![int_key(1)])))
            .await
            .expect("get should succeed")
            .into_inner();
        let updated = convert::records_from_proto(updated.records, "records").expect("decode");
        assert!(!updated[0].fields.contains_key("price"), "null clears");
        assert_eq!(
            updated[0].fields["tags"],
            logpose_types::value::Value::Array(vec![logpose_types::value::Value::String(
                "c".to_owned()
            )])
        );
        assert_eq!(
            Value::Object(updated[0].extra.clone()),
            json!({"color": "red"})
        );
        assert_eq!(
            updated[0].vectors["embedding"],
            vec![0.6, 0.0, 0.8],
            "an update keeps the vector it does not send"
        );

        let deleted = service
            .delete_records(Request::new(DeleteRecordsRequest {
                database_name: default_database_name(),
                collection_name: "products".to_owned(),
                keys: vec![int_key(2), int_key(99)],
            }))
            .await
            .expect("delete should succeed")
            .into_inner();
        assert_eq!(deleted.applied_ops, 2, "a missing key is a no-op delete");
        let after = service
            .get_records(Request::new(get_request("products", vec![int_key(2)])))
            .await
            .expect("get should succeed")
            .into_inner();
        assert!(after.records.is_empty());
        assert_eq!(after.missing_keys, vec![int_key(2)]);
    }

    #[tokio::test]
    async fn grpc_record_validation_errors_name_the_record_field() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-record-errors"))));
        service
            .create_collection(Request::new(products_request("default")))
            .await
            .expect("collection should be created");
        let valid = || proto::Record {
            pk: Some(int_key(1)),
            vectors: [(
                "embedding".to_owned(),
                proto::Vector {
                    values: vec![1.0, 0.0, 0.0],
                },
            )]
            .into_iter()
            .collect(),
            fields: [("tenant".to_owned(), string_value("acme"))]
                .into_iter()
                .collect(),
            extra: None,
        };
        let upsert = |record: proto::Record| {
            let service = service.clone();
            async move {
                service
                    .upsert_records(Request::new(UpsertRecordsRequest {
                        database_name: default_database_name(),
                        collection_name: "products".to_owned(),
                        records: vec![valid(), record],
                    }))
                    .await
                    .expect_err("the batch is rejected")
            }
        };

        let mut record = valid();
        record.pk = Some(int_key(2));
        record
            .fields
            .insert("price".to_owned(), proto::Value { kind: None });
        let error = upsert(record).await;
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(violation_fields(&error), vec!["records[1].price"]);

        let mut record = valid();
        record.pk = Some(int_key(2));
        record.fields.insert(
            "price".to_owned(),
            proto::Value {
                kind: Some(proto::value::Kind::Int64Value(5)),
            },
        );
        let error = upsert(record).await;
        assert_eq!(violation_fields(&error), vec!["records[1].price"]);
        assert!(error.message().contains("expected float64"), "{error:?}");

        let mut record = valid();
        record.pk = Some(int_key(2));
        record.fields.insert(
            "tags".to_owned(),
            proto::Value {
                kind: Some(proto::value::Kind::ArrayValue(proto::ValueArray {
                    values: vec![
                        string_value("a"),
                        proto::Value {
                            kind: Some(proto::value::Kind::Int64Value(1)),
                        },
                    ],
                })),
            },
        );
        let error = upsert(record).await;
        assert_eq!(violation_fields(&error), vec!["records[1].tags[1]"]);

        let mut record = valid();
        record.pk = Some(int_key(2));
        record.vectors.insert(
            "embedding".to_owned(),
            proto::Vector {
                values: vec![1.0, 0.0],
            },
        );
        let error = upsert(record).await;
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(reason(&error).as_deref(), Some("DIMENSION_MISMATCH"));
        assert_eq!(violation_fields(&error), vec!["records[1].embedding"]);

        let mut record = valid();
        record.pk = Some(int_key(2));
        record.fields.clear();
        let error = upsert(record).await;
        assert_eq!(violation_fields(&error), vec!["records[1].tenant"]);

        let mut record = valid();
        record.pk = Some(string_key("two"));
        let error = upsert(record).await;
        assert_eq!(violation_fields(&error), vec!["records[1].sku"]);

        let mut record = valid();
        record.pk = None;
        let error = upsert(record).await;
        assert_eq!(violation_fields(&error), vec!["records[1].pk"]);

        let error = upsert(valid()).await;
        assert_eq!(violation_fields(&error), vec!["records[1]"]);
        assert!(error.message().contains("more than once"), "{error:?}");

        let mut record = valid();
        record.pk = Some(int_key(2));
        record.extra = Some(convert::json_object_to_proto(
            json!({"price": 1}).as_object().expect("object"),
        ));
        let error = upsert(record).await;
        assert_eq!(violation_fields(&error), vec!["records[1].price"]);

        let error = service
            .update_records(Request::new(UpdateRecordsRequest {
                database_name: default_database_name(),
                collection_name: "products".to_owned(),
                records: vec![proto::RecordUpdate {
                    pk: Some(int_key(404)),
                    vectors: Default::default(),
                    fields: [("price".to_owned(), string_value("x"))]
                        .into_iter()
                        .collect(),
                    extra: None,
                }],
            }))
            .await
            .expect_err("a type error is reported before the key is looked up");
        assert_eq!(violation_fields(&error), vec!["records[0].price"]);

        let error = service
            .update_records(Request::new(UpdateRecordsRequest {
                database_name: default_database_name(),
                collection_name: "products".to_owned(),
                records: vec![proto::RecordUpdate {
                    pk: Some(int_key(404)),
                    vectors: Default::default(),
                    fields: [(
                        "price".to_owned(),
                        proto::Value {
                            kind: Some(proto::value::Kind::Float64Value(1.0)),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    extra: None,
                }],
            }))
            .await
            .expect_err("an update of a missing key fails");
        assert_eq!(error.code(), tonic::Code::NotFound);
        assert_eq!(reason(&error).as_deref(), Some("RESOURCE_NOT_FOUND"));
        assert_eq!(
            error
                .get_details_error_info()
                .and_then(|info| info.metadata.get("resource_type").cloned())
                .as_deref(),
            Some("record")
        );

        let error = service
            .delete_records(Request::new(DeleteRecordsRequest {
                database_name: default_database_name(),
                collection_name: "products".to_owned(),
                keys: vec![int_key(1), string_key("one")],
            }))
            .await
            .expect_err("a string key does not fit an int64 primary key");
        assert_eq!(violation_fields(&error), vec!["keys[1]"]);

        let error = service
            .get_records(Request::new(get_request(
                "products",
                vec![proto::PrimaryKey { kind: None }],
            )))
            .await
            .expect_err("a key must be set");
        assert_eq!(violation_fields(&error), vec!["keys[0]"]);

        let error = service
            .get_records(Request::new(get_request("products", vec![string_key("x")])))
            .await
            .expect_err("a string key does not fit an int64 primary key");
        assert_eq!(violation_fields(&error), vec!["keys[0]"]);
    }

    #[tokio::test]
    async fn grpc_alter_collection_changes_the_schema_and_shadows_dynamic_keys() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-alter"))));
        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("collection should be created");
        service
            .upsert_records(Request::new(upsert_request(
                "documents",
                vec![record(
                    "alpha",
                    vec![1.0, 0.0],
                    json!({"color": "red", "size": 1}),
                )],
            )))
            .await
            .expect("upsert should succeed");

        let altered = service
            .alter_collection(Request::new(alter_request(
                "default",
                "documents",
                proto::alter_collection_request::Change::AddField(proto::ScalarFieldSpec {
                    name: "color".to_owned(),
                    r#type: proto::FieldType::String as i32,
                    index: proto::FieldIndex::Auto as i32,
                    nullable: None,
                }),
            )))
            .await
            .expect("add field should succeed")
            .into_inner();
        let schema = altered.schema.expect("schema");
        assert_eq!(schema.schema_version, 2);
        assert_eq!(schema.fields[0].name, "color");

        let before = service
            .get_records(Request::new(get_request(
                "documents",
                vec![string_key("alpha")],
            )))
            .await
            .expect("get should succeed")
            .into_inner();
        let before = convert::records_from_proto(before.records, "records").expect("decode");
        assert!(
            before[0].fields.is_empty(),
            "an added field reads null on rows written before it"
        );
        assert_eq!(
            Value::Object(before[0].extra.clone()),
            json!({"size": 1}),
            "the dynamic key the new field declares is shadowed"
        );

        service
            .upsert_records(Request::new(upsert_request(
                "documents",
                vec![proto::Record {
                    fields: [("color".to_owned(), string_value("blue"))]
                        .into_iter()
                        .collect(),
                    ..record("beta", vec![0.0, 1.0], json!({}))
                }],
            )))
            .await
            .expect("typed upsert should succeed");

        let renamed = service
            .alter_collection(Request::new(alter_request(
                "default",
                "documents",
                proto::alter_collection_request::Change::RenameField(proto::RenameField {
                    from: "color".to_owned(),
                    to: "colour".to_owned(),
                }),
            )))
            .await
            .expect("rename should succeed")
            .into_inner()
            .schema
            .expect("schema");
        assert_eq!(renamed.retired_names, vec!["color".to_owned()]);
        let beta = service
            .get_records(Request::new(get_request(
                "documents",
                vec![string_key("beta")],
            )))
            .await
            .expect("get should succeed")
            .into_inner();
        let beta = convert::records_from_proto(beta.records, "records").expect("decode");
        assert_eq!(
            beta[0].fields["colour"],
            logpose_types::value::Value::String("blue".to_owned())
        );

        let error = service
            .upsert_records(Request::new(upsert_request(
                "documents",
                vec![record("gamma", vec![1.0, 1.0], json!({"color": "green"}))],
            )))
            .await
            .expect_err("a retired name cannot be stored dynamically");
        assert_eq!(violation_fields(&error), vec!["records[0].color"]);

        let error = service
            .get_records(Request::new(GetRecordsRequest {
                output_fields: vec!["color".to_owned()],
                ..get_request("documents", vec![string_key("beta")])
            }))
            .await
            .expect_err("a retired name cannot be projected");
        assert_eq!(violation_fields(&error), vec!["output_fields[0]"]);

        service
            .alter_collection(Request::new(alter_request(
                "default",
                "documents",
                proto::alter_collection_request::Change::DropField(proto::DropField {
                    name: "colour".to_owned(),
                }),
            )))
            .await
            .expect("drop should succeed");
        let beta = service
            .get_records(Request::new(get_request(
                "documents",
                vec![string_key("beta")],
            )))
            .await
            .expect("get should succeed")
            .into_inner();
        let beta = convert::records_from_proto(beta.records, "records").expect("decode");
        assert!(beta[0].fields.is_empty(), "a dropped field is hidden");

        for (change, field) in [
            (
                proto::alter_collection_request::Change::AddField(proto::ScalarFieldSpec {
                    name: "required".to_owned(),
                    r#type: proto::FieldType::Bool as i32,
                    index: proto::FieldIndex::Auto as i32,
                    nullable: Some(false),
                }),
                "add_field.nullable",
            ),
            (
                proto::alter_collection_request::Change::DropField(proto::DropField {
                    name: "id".to_owned(),
                }),
                "drop_field.name",
            ),
            (
                proto::alter_collection_request::Change::RenameField(proto::RenameField {
                    from: "missing".to_owned(),
                    to: "other".to_owned(),
                }),
                "rename_field.from",
            ),
            (
                proto::alter_collection_request::Change::RenameField(proto::RenameField {
                    from: "vector".to_owned(),
                    to: "id".to_owned(),
                }),
                "rename_field.to",
            ),
        ] {
            let error = service
                .alter_collection(Request::new(alter_request("default", "documents", change)))
                .await
                .expect_err("an invalid change is rejected");
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert_eq!(violation_fields(&error), vec![field], "{error:?}");
        }
        let error = service
            .alter_collection(Request::new(AlterCollectionRequest {
                change: None,
                ..alter_request(
                    "default",
                    "documents",
                    proto::alter_collection_request::Change::DropField(proto::DropField {
                        name: "x".to_owned(),
                    }),
                )
            }))
            .await
            .expect_err("an alter needs a change");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn grpc_schema_changes_and_typed_records_survive_a_restart() {
        let root = unique_temp_dir("grpc-restart");
        let config = test_config_with_root("grpc-restart", NodeRole::Combined, root.clone());
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(config.clone())));
        service
            .create_collection(Request::new(products_request("default")))
            .await
            .expect("collection should be created");
        service
            .alter_collection(Request::new(alter_request(
                "default",
                "products",
                proto::alter_collection_request::Change::AddField(proto::ScalarFieldSpec {
                    name: "stock".to_owned(),
                    r#type: proto::FieldType::Int64 as i32,
                    index: proto::FieldIndex::Auto as i32,
                    nullable: None,
                }),
            )))
            .await
            .expect("add field should succeed");
        service
            .upsert_records(Request::new(UpsertRecordsRequest {
                database_name: default_database_name(),
                collection_name: "products".to_owned(),
                records: vec![proto::Record {
                    pk: Some(int_key(7)),
                    vectors: [(
                        "embedding".to_owned(),
                        proto::Vector {
                            values: vec![0.0, 0.0, 2.0],
                        },
                    )]
                    .into_iter()
                    .collect(),
                    fields: [
                        ("tenant".to_owned(), string_value("acme")),
                        (
                            "stock".to_owned(),
                            proto::Value {
                                kind: Some(proto::value::Kind::Int64Value(12)),
                            },
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    extra: None,
                }],
            }))
            .await
            .expect("upsert should succeed");
        service
            .alter_collection(Request::new(alter_request(
                "default",
                "products",
                proto::alter_collection_request::Change::RenameField(proto::RenameField {
                    from: "stock".to_owned(),
                    to: "inventory".to_owned(),
                }),
            )))
            .await
            .expect("rename should succeed");
        drop(service);

        let reopened = GrpcLogPoseService::new(Arc::new(AppState::new(config)));
        let described = reopened
            .get_collection(Request::new(GetCollectionRequest {
                database_name: "default".to_owned(),
                collection_name: "products".to_owned(),
            }))
            .await
            .expect("the collection is recovered")
            .into_inner()
            .schema
            .expect("schema");
        assert_eq!(described.schema_version, 3);
        assert_eq!(
            described.fields.last().map(|field| field.name.as_str()),
            Some("inventory")
        );
        let fetched = reopened
            .get_records(Request::new(get_request("products", vec![int_key(7)])))
            .await
            .expect("get should succeed")
            .into_inner();
        let fetched = convert::records_from_proto(fetched.records, "records").expect("decode");
        assert_eq!(
            fetched[0].fields["inventory"],
            logpose_types::value::Value::Int64(12)
        );
        assert_eq!(fetched[0].vectors["embedding"], vec![0.0, 0.0, 1.0]);
        let _ = fs::remove_dir_all(root);
    }

    fn default_database_name() -> String {
        DEFAULT_DATABASE_NAME.to_owned()
    }

    /// A collection of the single-vector shape: string key `id`, vector `vector`, dynamic
    /// fields on.
    fn create_collection_request(
        name: &str,
        dimensions: u32,
        metric: proto::DistanceMetric,
    ) -> CreateCollectionRequest {
        CreateCollectionRequest {
            database_name: default_database_name(),
            collection_name: name.to_owned(),
            primary_key: Some(proto::PrimaryKeySpec {
                name: "id".to_owned(),
                r#type: proto::PrimaryKeyType::String as i32,
            }),
            vectors: vec![proto::VectorFieldSpec {
                name: "vector".to_owned(),
                dimensions,
                metric: metric as i32,
            }],
            fields: Vec::new(),
            dynamic_fields: Some(true),
        }
    }

    /// The typed `products` collection of the engine plan's example: an int64 key `sku`, a
    /// three-dimension cosine vector, typed scalar fields, and dynamic fields on.
    fn products_request(database_name: &str) -> CreateCollectionRequest {
        let field = |name: &str, field_type: proto::FieldType, nullable: Option<bool>| {
            proto::ScalarFieldSpec {
                name: name.to_owned(),
                r#type: field_type as i32,
                index: proto::FieldIndex::Auto as i32,
                nullable,
            }
        };
        CreateCollectionRequest {
            database_name: database_name.to_owned(),
            collection_name: "products".to_owned(),
            primary_key: Some(proto::PrimaryKeySpec {
                name: "sku".to_owned(),
                r#type: proto::PrimaryKeyType::Int64 as i32,
            }),
            vectors: vec![proto::VectorFieldSpec {
                name: "embedding".to_owned(),
                dimensions: 3,
                metric: proto::DistanceMetric::Cosine as i32,
            }],
            fields: vec![
                field("tenant", proto::FieldType::String, Some(false)),
                field("price", proto::FieldType::Float64, None),
                field("tags", proto::FieldType::ArrayString, None),
                field("updated_at", proto::FieldType::Timestamp, None),
                field("attrs", proto::FieldType::Json, None),
            ],
            dynamic_fields: None,
        }
    }

    fn storage_create_collection_request(
        name: &str,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> StorageCreateCollectionRequest {
        StorageCreateCollectionRequest::in_database(
            default_database_name(),
            name.to_owned(),
            dimensions,
            metric,
        )
    }

    fn get_collection_request(collection_name: &str) -> GetCollectionRequest {
        GetCollectionRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
        }
    }

    fn get_collection_placement_request(collection_name: &str) -> GetCollectionPlacementRequest {
        GetCollectionPlacementRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
        }
    }

    fn string_value(value: &str) -> proto::Value {
        proto::Value {
            kind: Some(proto::value::Kind::StringValue(value.to_owned())),
        }
    }

    fn string_key(value: &str) -> proto::PrimaryKey {
        proto::PrimaryKey {
            kind: Some(proto::primary_key::Kind::StringValue(value.to_owned())),
        }
    }

    fn int_key(value: i64) -> proto::PrimaryKey {
        proto::PrimaryKey {
            kind: Some(proto::primary_key::Kind::Int64Value(value)),
        }
    }

    /// A record of the single-vector shape whose `metadata` keys go to `$extra`.
    fn record(id: &str, vector: Vec<f32>, metadata: Value) -> proto::Record {
        let metadata = metadata
            .as_object()
            .cloned()
            .expect("record metadata must be an object");
        proto::Record {
            pk: Some(string_key(id)),
            vectors: [("vector".to_owned(), proto::Vector { values: vector })]
                .into_iter()
                .collect(),
            fields: Default::default(),
            extra: Some(convert::json_object_to_proto(&metadata)),
        }
    }

    fn upsert_request(collection_name: &str, records: Vec<proto::Record>) -> UpsertRecordsRequest {
        UpsertRecordsRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
            records,
        }
    }

    fn delete_request(collection_name: &str, ids: &[&str]) -> DeleteRecordsRequest {
        DeleteRecordsRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
            keys: ids.iter().map(|id| string_key(id)).collect(),
        }
    }

    fn get_request(collection_name: &str, keys: Vec<proto::PrimaryKey>) -> GetRecordsRequest {
        GetRecordsRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
            keys,
            output_fields: Vec::new(),
        }
    }

    fn alter_request(
        database_name: &str,
        collection_name: &str,
        change: proto::alter_collection_request::Change,
    ) -> AlterCollectionRequest {
        AlterCollectionRequest {
            database_name: database_name.to_owned(),
            collection_name: collection_name.to_owned(),
            change: Some(change),
        }
    }

    fn query_collection_request(
        collection_name: &str,
        vector: Vec<f32>,
        top_k: u64,
    ) -> QueryCollectionRequest {
        QueryCollectionRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
            vector,
            top_k,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: proto::ExplainMode::None as i32,
        }
    }

    fn get_collection_stats_request(collection_name: &str) -> GetCollectionStatsRequest {
        GetCollectionStatsRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
            snapshot: None,
            read_barrier: None,
        }
    }

    fn flush_collection_request(collection_name: &str) -> FlushCollectionRequest {
        FlushCollectionRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
        }
    }

    fn compact_collection_request(collection_name: &str) -> CompactCollectionRequest {
        CompactCollectionRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
        }
    }

    fn inspect_collection_request(
        collection_name: &str,
        target: proto::InspectTarget,
        segment_id: impl Into<String>,
    ) -> InspectCollectionRequest {
        InspectCollectionRequest {
            database_name: default_database_name(),
            collection_name: collection_name.to_owned(),
            target: target as i32,
            segment_id: segment_id.into(),
        }
    }

    fn put_database_policy_request(database_name: &str) -> proto::PutDatabasePolicyRequest {
        proto::PutDatabasePolicyRequest {
            database_name: database_name.to_owned(),
            authentication_mode: proto::AuthenticationMode::ExternalToken as i32,
            role_bindings: vec![
                proto::DatabaseRoleBinding {
                    principal_name: "ops-admin".to_owned(),
                    role: proto::DatabaseRole::Owner as i32,
                },
                proto::DatabaseRoleBinding {
                    principal_name: "reader-service".to_owned(),
                    role: proto::DatabaseRole::ReadOnly as i32,
                },
            ],
        }
    }

    fn get_database_policy_request(database_name: &str) -> proto::GetDatabasePolicyRequest {
        proto::GetDatabasePolicyRequest {
            database_name: database_name.to_owned(),
        }
    }

    fn put_database_request(database_name: &str) -> proto::PutDatabaseRequest {
        proto::PutDatabaseRequest {
            database_name: database_name.to_owned(),
        }
    }

    fn get_database_request(database_name: &str) -> proto::GetDatabaseRequest {
        proto::GetDatabaseRequest {
            database_name: database_name.to_owned(),
        }
    }

    fn list_databases_request() -> proto::ListDatabasesRequest {
        proto::ListDatabasesRequest {}
    }

    /// The request fields a status's `BadRequest` detail names.
    fn violation_fields(status: &Status) -> Vec<String> {
        status
            .get_details_bad_request()
            .map(|bad_request| {
                bad_request
                    .field_violations
                    .into_iter()
                    .map(|violation| violation.field)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn reason(status: &Status) -> Option<String> {
        status.get_details_error_info().map(|info| info.reason)
    }

    fn test_config(label: &str) -> LogPoseConfig {
        test_config_with_role(label, NodeRole::Combined)
    }

    fn test_config_with_role(label: &str, node_role: NodeRole) -> LogPoseConfig {
        test_config_with_root(label, node_role, unique_temp_dir(label))
    }

    fn test_config_with_root(
        label: &str,
        node_role: NodeRole,
        storage_root: PathBuf,
    ) -> LogPoseConfig {
        LogPoseConfig {
            node_name: label.to_owned(),
            node_role,
            storage_root,
            ..LogPoseConfig::default()
        }
    }

    fn auth_test_config(label: &str) -> LogPoseConfig {
        let mut config = test_config(label);
        config.auth.bootstrap_tokens = vec![
            BootstrapTokenConfig {
                token: "operator-secret".to_owned(),
                principal: Principal::new_with_access_tier(
                    "ops-admin",
                    PrincipalKind::User,
                    AccessTier::Operator,
                ),
            },
            BootstrapTokenConfig {
                token: "reader-secret".to_owned(),
                principal: Principal::new_with_access_tier(
                    "reader",
                    PrincipalKind::User,
                    AccessTier::Service,
                ),
            },
        ];
        config
    }

    fn read_only_policy(database_name: &str, principal_name: &str) -> DatabaseAccessPolicy {
        DatabaseAccessPolicy {
            database_name: database_name.to_owned(),
            authentication_mode: AuthenticationMode::ExternalToken,
            role_bindings: vec![DatabaseRoleBinding {
                database_name: database_name.to_owned(),
                principal_name: principal_name.to_owned(),
                role: DatabaseRole::ReadOnly,
            }],
        }
    }

    fn authorized_request<T>(message: T, token: &str) -> Request<T> {
        let mut request = Request::new(message);
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}"))
                .expect("authorization metadata should be valid"),
        );
        request
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("logpose-api-grpc-{label}-{suffix}"));
        fs::create_dir_all(&path).expect("temp dir should be created");
        path
    }
}
