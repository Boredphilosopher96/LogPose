//! gRPC API surface for LogPose.

use logpose_auth::{AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding};
use logpose_core::{AppState, RequestAuth};
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator,
    QueryDiagnostics, QueryPlanKind, QueryRequest, QueryStageTimings, ScalarMetadataValue,
};
use logpose_service::ServiceError;
use logpose_storage::CreateCollectionRequest as StorageCreateCollectionRequest;
use logpose_types::{
    CollectionPlacement, CoordinationStatus, DEFAULT_DATABASE_NAME, DeleteRecord, DistanceMetric,
    MaintenanceBacklog, MaintenanceStatus, NodeRole, NodeRuntimeStatus, PutRecord, QueryUnitStats,
    RecordId, ScalarFieldStats, Snapshot, WriteOperation,
};
use serde_json::{Number, Value};
use std::{net::SocketAddr, sync::Arc};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use tonic_health::server::health_reporter;
use tracing::info;

#[allow(missing_docs)]
/// Generated protobuf interfaces.
pub mod proto {
    tonic::include_proto!("logpose.v1");
}

use proto::log_pose_service_server::{LogPoseService, LogPoseServiceServer};
use proto::{
    CollectionDescriptorReply, CollectionPlacementReply, CollectionStatsReply, CommitAckReply,
    CompactCollectionRequest, CoordinationStatusReply, CreateCollectionRequest,
    DatabaseAccessPolicyReply, DatabaseDescriptorReply, DatabaseRoleBindingReply,
    FlushCollectionRequest, GetCollectionPlacementRequest, GetCollectionRequest,
    GetCollectionStatsRequest, GetDatabasePolicyRequest, GetDatabaseRequest, GetMetadataReply,
    GetMetadataRequest, GetRuntimeStatusReply, GetRuntimeStatusRequest, InspectCollectionReply,
    InspectCollectionRequest, InspectTarget, ListDatabasesReply, ListDatabasesRequest,
    MaintenanceBacklogReply, NodeRole as ProtoNodeRole, PutDatabasePolicyRequest,
    PutDatabaseRequest, QueryCollectionReply, QueryCollectionRequest, QueryMatch, RemoteBlobConfig,
    ScalarValue, SnapshotReply, WriteCollectionRequest,
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

    Server::builder()
        .add_service(health_service)
        .add_service(LogPoseServiceServer::new(GrpcLogPoseService::new(state)))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

/// gRPC service implementation scaffold.
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
        _request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataReply>, Status> {
        Ok(Response::new(metadata_reply_from_domain(
            self.state.metadata(),
        )))
    }

    async fn get_runtime_status(
        &self,
        request: Request<GetRuntimeStatusRequest>,
    ) -> Result<Response<GetRuntimeStatusReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let status = self
            .state
            .runtime_status_with_auth(&auth)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(runtime_status_reply_from_domain(status)))
    }

    async fn put_database(
        &self,
        request: Request<PutDatabaseRequest>,
    ) -> Result<Response<DatabaseDescriptorReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let descriptor = request
            .descriptor
            .ok_or_else(|| Status::invalid_argument("database descriptor payload is required"))?;
        let stored = self
            .state
            .put_database_with_auth(&auth, database_descriptor_from_proto(descriptor)?)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(database_descriptor_to_proto(stored)))
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<DatabaseDescriptorReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = normalize_database_name(&request.database_name);
        let descriptor = self
            .state
            .database_with_auth(&auth, &database_name)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(database_descriptor_to_proto(descriptor)))
    }

    async fn list_databases(
        &self,
        request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let descriptors = self
            .state
            .databases_with_auth(&auth)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(ListDatabasesReply {
            databases: descriptors
                .into_iter()
                .map(database_descriptor_to_proto)
                .collect(),
        }))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CollectionDescriptorReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let descriptor = self
            .state
            .create_collection_with_auth(
                &auth,
                StorageCreateCollectionRequest::in_database(
                    normalize_database_name(&request.database_name),
                    request.name,
                    request.dimensions as usize,
                    metric_from_proto(request.metric)?,
                ),
            )
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(collection_descriptor_reply(descriptor)))
    }

    async fn get_collection_placement(
        &self,
        request: Request<GetCollectionPlacementRequest>,
    ) -> Result<Response<CollectionPlacementReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let placement = self
            .state
            .collection_placement_with_auth(
                &auth,
                &collection_lookup_key(&request.database_name, &request.collection_name),
            )
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(collection_placement_reply_from_domain(
            placement,
        )))
    }

    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<CollectionDescriptorReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let descriptor = self
            .state
            .get_collection_with_auth(
                &auth,
                &collection_lookup_key(&request.database_name, &request.collection_name),
            )
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(collection_descriptor_reply(descriptor)))
    }

    async fn write_collection(
        &self,
        request: Request<WriteCollectionRequest>,
    ) -> Result<Response<CommitAckReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = normalize_database_name(&request.database_name);
        let operations = request
            .operations
            .into_iter()
            .map(write_operation_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let ack = self
            .state
            .write_with_auth(
                &auth,
                &collection_lookup_key(&database_name, &request.collection_name),
                operations,
            )
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(CommitAckReply {
            last_seq_no: ack.last_seq_no,
            applied_ops: ack.applied_ops as u64,
            database_name,
            collection_name: request.collection_name,
            snapshot: Some(snapshot_message_from_domain(ack.snapshot)),
        }))
    }

    async fn query_collection(
        &self,
        request: Request<QueryCollectionRequest>,
    ) -> Result<Response<QueryCollectionReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = normalize_database_name(&request.database_name);
        if request.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be greater than 0"));
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
                    collection_name: collection_lookup_key(
                        &database_name,
                        &request.collection_name,
                    ),
                    vector: request.vector,
                    top_k: request.top_k as usize,
                    snapshot: request.snapshot.map(snapshot_from_proto),
                    filters,
                    predicate,
                    explain: explain_mode_from_proto(request.explain)?,
                },
            )
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(QueryCollectionReply {
            metric: proto_metric(response.metric) as i32,
            top_k: response.top_k as u64,
            returned: response.returned as u64,
            snapshot: Some(snapshot_message_from_domain(response.snapshot)),
            matches: response
                .matches
                .into_iter()
                .map(|candidate| {
                    let metadata_json =
                        serde_json::to_string(&candidate.metadata).map_err(|error| {
                            Status::internal(format!(
                                "failed to serialize query match metadata: {error}"
                            ))
                        })?;
                    Ok(QueryMatch {
                        id: candidate.id.to_string(),
                        value: candidate.value,
                        metadata_json,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?,
            diagnostics: response
                .diagnostics
                .map(query_diagnostics_to_proto)
                .transpose()?,
            database_name,
            collection_name: request.collection_name,
        }))
    }

    async fn get_collection_stats(
        &self,
        request: Request<GetCollectionStatsRequest>,
    ) -> Result<Response<CollectionStatsReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = normalize_database_name(&request.database_name);
        let stats = self
            .state
            .stats_at_snapshot_with_auth(
                &auth,
                &collection_lookup_key(&database_name, &request.collection_name),
                request.snapshot.map(snapshot_from_proto),
            )
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(collection_stats_reply_from_domain(stats)?))
    }

    async fn flush_collection(
        &self,
        request: Request<FlushCollectionRequest>,
    ) -> Result<Response<SnapshotReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = normalize_database_name(&request.database_name);
        let snapshot = self
            .state
            .flush_with_auth(
                &auth,
                &collection_lookup_key(&database_name, &request.collection_name),
            )
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(snapshot_reply_from_domain(
            snapshot,
            database_name,
            request.collection_name,
        )))
    }

    async fn compact_collection(
        &self,
        request: Request<CompactCollectionRequest>,
    ) -> Result<Response<SnapshotReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = normalize_database_name(&request.database_name);
        let snapshot = self
            .state
            .compact_with_auth(
                &auth,
                &collection_lookup_key(&database_name, &request.collection_name),
            )
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(snapshot_reply_from_domain(
            snapshot,
            database_name,
            request.collection_name,
        )))
    }

    async fn inspect_collection(
        &self,
        request: Request<InspectCollectionRequest>,
    ) -> Result<Response<InspectCollectionReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = normalize_database_name(&request.database_name);
        let target = inspect_target_from_proto(request.target, request.segment_id)?;
        let report = self
            .state
            .inspect_with_auth(
                &auth,
                &collection_lookup_key(&database_name, &request.collection_name),
                target,
            )
            .await
            .map_err(status_from_service_error)?;
        let payload_json = serde_json::to_string(&report.payload).map_err(|error| {
            Status::internal(format!("failed to serialize inspect payload: {error}"))
        })?;
        Ok(Response::new(InspectCollectionReply {
            target: report.target,
            payload_json,
            database_name,
            collection_name: request.collection_name,
        }))
    }

    async fn put_database_policy(
        &self,
        request: Request<PutDatabasePolicyRequest>,
    ) -> Result<Response<DatabaseAccessPolicyReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let policy = request
            .policy
            .ok_or_else(|| Status::invalid_argument("database policy payload is required"))?;
        let policy = database_access_policy_from_proto(policy)?;
        let stored = self
            .state
            .set_database_access_policy_with_auth(&auth, policy)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(database_access_policy_to_proto(stored)))
    }

    async fn get_database_policy(
        &self,
        request: Request<GetDatabasePolicyRequest>,
    ) -> Result<Response<DatabaseAccessPolicyReply>, Status> {
        let auth = request_auth_from_metadata(&request)?;
        let request = request.into_inner();
        let database_name = normalize_database_name(&request.database_name);
        let policy = self
            .state
            .database_access_policy_with_auth(&auth, &database_name)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(database_access_policy_to_proto(policy)))
    }
}

fn metric_from_proto(metric: i32) -> Result<DistanceMetric, Status> {
    match proto::DistanceMetric::try_from(metric).unwrap_or(proto::DistanceMetric::Unspecified) {
        proto::DistanceMetric::Cosine => Ok(DistanceMetric::Cosine),
        proto::DistanceMetric::Dot => Ok(DistanceMetric::Dot),
        proto::DistanceMetric::L2 => Ok(DistanceMetric::L2),
        proto::DistanceMetric::Unspecified => {
            Err(Status::invalid_argument("distance metric must be set"))
        }
    }
}

fn proto_metric(metric: DistanceMetric) -> proto::DistanceMetric {
    match metric {
        DistanceMetric::Cosine => proto::DistanceMetric::Cosine,
        DistanceMetric::Dot => proto::DistanceMetric::Dot,
        DistanceMetric::L2 => proto::DistanceMetric::L2,
    }
}

fn write_operation_from_proto(operation: proto::WriteOperation) -> Result<WriteOperation, Status> {
    match operation.operation {
        Some(proto::write_operation::Operation::Put(put)) => {
            if put.id.is_empty() {
                return Err(Status::invalid_argument(
                    "put operation record id must not be empty",
                ));
            }
            let metadata = serde_json::from_str::<Value>(&put.metadata_json).map_err(|error| {
                Status::invalid_argument(format!("invalid metadata_json: {error}"))
            })?;
            Ok(WriteOperation::Put(PutRecord {
                id: RecordId::new(put.id),
                vector: put.vector,
                metadata,
            }))
        }
        Some(proto::write_operation::Operation::Delete(delete)) => {
            if delete.id.is_empty() {
                return Err(Status::invalid_argument(
                    "delete operation record id must not be empty",
                ));
            }
            Ok(WriteOperation::Delete(DeleteRecord {
                id: RecordId::new(delete.id),
            }))
        }
        None => Err(Status::invalid_argument(
            "write operation must include put or delete",
        )),
    }
}

fn metadata_filter_from_proto(filter: proto::MetadataFilter) -> Result<MetadataFilter, Status> {
    let value = filter
        .value
        .ok_or_else(|| Status::invalid_argument("metadata filter value is required"))?;
    Ok(MetadataFilter {
        field: filter.field,
        value: scalar_value_from_proto(value)?,
    })
}

fn predicate_from_proto(predicate: proto::Predicate) -> Result<Predicate, Status> {
    match predicate
        .node
        .ok_or_else(|| Status::invalid_argument("predicate node is required"))?
    {
        proto::predicate::Node::And(list) => Ok(Predicate::And {
            children: list
                .children
                .into_iter()
                .map(predicate_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        proto::predicate::Node::Or(list) => Ok(Predicate::Or {
            children: list
                .children
                .into_iter()
                .map(predicate_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        proto::predicate::Node::Not(node) => Ok(Predicate::Not {
            child: Box::new(predicate_from_proto(*node.child.ok_or_else(|| {
                Status::invalid_argument("not predicate child is required")
            })?)?),
        }),
        proto::predicate::Node::Comparison(comparison) => Ok(Predicate::Comparison(
            predicate_comparison_from_proto(comparison)?,
        )),
    }
}

fn predicate_comparison_from_proto(
    comparison: proto::PredicateComparison,
) -> Result<PredicateComparison, Status> {
    Ok(PredicateComparison {
        field: comparison.field,
        operator: predicate_operator_from_proto(comparison.operator)?,
        value: comparison.value.map(scalar_value_from_proto).transpose()?,
    })
}

fn predicate_operator_from_proto(operator: i32) -> Result<PredicateOperator, Status> {
    match proto::PredicateOperator::try_from(operator)
        .unwrap_or(proto::PredicateOperator::Unspecified)
    {
        proto::PredicateOperator::Eq => Ok(PredicateOperator::Eq),
        proto::PredicateOperator::Ne => Ok(PredicateOperator::Ne),
        proto::PredicateOperator::Lt => Ok(PredicateOperator::Lt),
        proto::PredicateOperator::Lte => Ok(PredicateOperator::Lte),
        proto::PredicateOperator::Gt => Ok(PredicateOperator::Gt),
        proto::PredicateOperator::Gte => Ok(PredicateOperator::Gte),
        proto::PredicateOperator::Exists => Ok(PredicateOperator::Exists),
        proto::PredicateOperator::IsNull => Ok(PredicateOperator::IsNull),
        proto::PredicateOperator::Unspecified => Err(Status::invalid_argument(
            "predicate comparison operator must be set",
        )),
    }
}

fn explain_mode_from_proto(mode: i32) -> Result<ExplainMode, Status> {
    match proto::ExplainMode::try_from(mode)
        .map_err(|_| Status::invalid_argument("explain mode must be a valid enum value"))?
    {
        proto::ExplainMode::None => Ok(ExplainMode::None),
        proto::ExplainMode::Plan => Ok(ExplainMode::Plan),
        proto::ExplainMode::Profile => Ok(ExplainMode::Profile),
    }
}

fn scalar_value_from_proto(value: ScalarValue) -> Result<ScalarMetadataValue, Status> {
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
            .ok_or_else(|| Status::invalid_argument("double scalar value must be finite")),
        Some(proto::scalar_value::Kind::BoolValue(value)) => Ok(ScalarMetadataValue::Bool(value)),
        Some(proto::scalar_value::Kind::NullValue(_)) => Ok(ScalarMetadataValue::Null),
        None => Err(Status::invalid_argument("scalar value kind is required")),
    }
}

fn scalar_value_to_proto(value: ScalarMetadataValue) -> Result<ScalarValue, Status> {
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
                return Err(Status::internal("numeric scalar value must be finite"));
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
) -> Result<logpose_storage::InspectTarget, Status> {
    match InspectTarget::try_from(target)
        .map_err(|_| Status::invalid_argument(format!("unsupported inspect target '{target}'")))?
    {
        InspectTarget::Manifest => Ok(logpose_storage::InspectTarget::Manifest),
        InspectTarget::Wal => Ok(logpose_storage::InspectTarget::Wal),
        InspectTarget::Segment => {
            if segment_id.is_empty() {
                Err(Status::invalid_argument(
                    "segment_id is required when inspect target is SEGMENT",
                ))
            } else {
                Ok(logpose_storage::InspectTarget::Segment(segment_id))
            }
        }
        InspectTarget::Maintenance => Ok(logpose_storage::InspectTarget::Maintenance),
    }
}

fn snapshot_from_proto(snapshot: proto::Snapshot) -> Snapshot {
    Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
    }
}

fn snapshot_message_from_domain(snapshot: Snapshot) -> proto::Snapshot {
    proto::Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
    }
}

fn snapshot_reply_from_domain(
    snapshot: Snapshot,
    database_name: String,
    collection_name: String,
) -> SnapshotReply {
    SnapshotReply {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
        database_name,
        collection_name,
    }
}

fn query_diagnostics_to_proto(
    diagnostics: QueryDiagnostics,
) -> Result<proto::QueryDiagnostics, Status> {
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

fn database_descriptor_from_proto(
    descriptor: DatabaseDescriptorReply,
) -> Result<logpose_catalog::DatabaseDescriptor, Status> {
    Ok(logpose_catalog::DatabaseDescriptor {
        database_id: descriptor.database_id.parse().map_err(
            |error: logpose_types::LogPoseError| {
                Status::invalid_argument(format!("invalid database_id: {error}"))
            },
        )?,
        name: descriptor.name,
        is_default: descriptor.is_default,
    })
}

fn collection_descriptor_reply(
    descriptor: logpose_catalog::CollectionDescriptor,
) -> CollectionDescriptorReply {
    CollectionDescriptorReply {
        collection_id: descriptor.collection_id.to_string(),
        name: descriptor.name,
        dimensions: descriptor.dimensions as u64,
        metric: proto_metric(descriptor.metric) as i32,
        root_path: descriptor.root_path.display().to_string(),
        flush_threshold_ops: descriptor.flush_threshold_ops as u64,
        flush_threshold_bytes: descriptor.flush_threshold_bytes as u64,
        compaction_threshold_segments: descriptor.compaction_threshold_segments as u64,
        remote_blob: descriptor.remote_blob.map(|remote_blob| RemoteBlobConfig {
            endpoint: remote_blob.endpoint,
            bucket: remote_blob.bucket,
            prefix: remote_blob.prefix,
        }),
        database_name: descriptor.database_name,
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
) -> Result<CollectionStatsReply, Status> {
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

fn database_access_policy_to_proto(policy: DatabaseAccessPolicy) -> DatabaseAccessPolicyReply {
    DatabaseAccessPolicyReply {
        database_name: policy.database_name,
        authentication_mode: authentication_mode_to_proto(policy.authentication_mode) as i32,
        role_bindings: policy
            .role_bindings
            .into_iter()
            .map(database_role_binding_to_proto)
            .collect(),
    }
}

fn database_access_policy_from_proto(
    policy: DatabaseAccessPolicyReply,
) -> Result<DatabaseAccessPolicy, Status> {
    Ok(DatabaseAccessPolicy {
        database_name: normalize_database_name(&policy.database_name),
        authentication_mode: authentication_mode_from_proto(policy.authentication_mode)?,
        role_bindings: policy
            .role_bindings
            .into_iter()
            .map(database_role_binding_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn database_role_binding_to_proto(binding: DatabaseRoleBinding) -> DatabaseRoleBindingReply {
    DatabaseRoleBindingReply {
        database_name: binding.database_name,
        principal_name: binding.principal_name,
        role: database_role_to_proto(binding.role) as i32,
    }
}

fn database_role_binding_from_proto(
    binding: DatabaseRoleBindingReply,
) -> Result<DatabaseRoleBinding, Status> {
    Ok(DatabaseRoleBinding {
        database_name: normalize_database_name(&binding.database_name),
        principal_name: binding.principal_name,
        role: database_role_from_proto(binding.role)?,
    })
}

fn normalize_database_name(database_name: &str) -> String {
    if database_name.trim().is_empty() {
        DEFAULT_DATABASE_NAME.to_owned()
    } else {
        database_name.to_owned()
    }
}

fn collection_lookup_key(database_name: &str, collection_name: &str) -> String {
    format!(
        "{}/{}",
        normalize_database_name(database_name),
        collection_name
    )
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

fn authentication_mode_to_proto(mode: AuthenticationMode) -> proto::AuthenticationMode {
    match mode {
        AuthenticationMode::Disabled => proto::AuthenticationMode::Disabled,
        AuthenticationMode::Password => proto::AuthenticationMode::Password,
        AuthenticationMode::MutualTls => proto::AuthenticationMode::MutualTls,
        AuthenticationMode::ExternalToken => proto::AuthenticationMode::ExternalToken,
    }
}

fn authentication_mode_from_proto(mode: i32) -> Result<AuthenticationMode, Status> {
    match proto::AuthenticationMode::try_from(mode).map_err(|_| {
        Status::invalid_argument(format!("unsupported authentication mode '{mode}'"))
    })? {
        proto::AuthenticationMode::Disabled => Ok(AuthenticationMode::Disabled),
        proto::AuthenticationMode::Password => Ok(AuthenticationMode::Password),
        proto::AuthenticationMode::MutualTls => Ok(AuthenticationMode::MutualTls),
        proto::AuthenticationMode::ExternalToken => Ok(AuthenticationMode::ExternalToken),
        proto::AuthenticationMode::Unspecified => {
            Err(Status::invalid_argument("authentication mode is required"))
        }
    }
}

fn database_role_to_proto(role: DatabaseRole) -> proto::DatabaseRole {
    match role {
        DatabaseRole::Owner => proto::DatabaseRole::Owner,
        DatabaseRole::ReadWrite => proto::DatabaseRole::ReadWrite,
        DatabaseRole::ReadOnly => proto::DatabaseRole::ReadOnly,
    }
}

fn database_role_from_proto(role: i32) -> Result<DatabaseRole, Status> {
    match proto::DatabaseRole::try_from(role)
        .map_err(|_| Status::invalid_argument(format!("unsupported database role '{role}'")))?
    {
        proto::DatabaseRole::Owner => Ok(DatabaseRole::Owner),
        proto::DatabaseRole::ReadWrite => Ok(DatabaseRole::ReadWrite),
        proto::DatabaseRole::ReadOnly => Ok(DatabaseRole::ReadOnly),
        proto::DatabaseRole::Unspecified => {
            Err(Status::invalid_argument("database role is required"))
        }
    }
}

fn query_unit_stats_to_proto(stats: QueryUnitStats) -> Result<proto::QueryUnitStats, Status> {
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

fn scalar_field_stats_to_proto(stats: ScalarFieldStats) -> Result<proto::ScalarFieldStats, Status> {
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

fn request_auth_from_metadata<T>(request: &Request<T>) -> Result<RequestAuth, Status> {
    let value = match request.metadata().get("authorization") {
        Some(value) => value,
        None => return Ok(RequestAuth::default()),
    };
    let value = value
        .to_str()
        .map_err(|_| Status::unauthenticated("authorization metadata must be valid ASCII"))?;
    let (scheme, token) = value.split_once(' ').ok_or_else(|| {
        Status::unauthenticated("authorization metadata must use the Bearer scheme")
    })?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.trim().is_empty() {
        return Err(Status::unauthenticated(
            "authorization metadata must use the Bearer scheme",
        ));
    }
    Ok(RequestAuth::bearer_token(token.trim()))
}

fn status_from_service_error(error: ServiceError) -> Status {
    match error {
        ServiceError::AlreadyExists(message) => Status::already_exists(message),
        ServiceError::NotFound(message) => Status::not_found(message),
        ServiceError::InvalidArgument(message) => Status::invalid_argument(message),
        ServiceError::Unauthenticated(message) => Status::unauthenticated(message),
        ServiceError::PermissionDenied(message) => Status::permission_denied(message),
        ServiceError::Internal(message) => Status::internal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_auth::{
        AccessTier, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding, Principal,
        PrincipalKind,
    };
    use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
    use logpose_query::{QueryDiagnostics, QueryPlanKind, QueryStageTimings};
    use serde_json::Value;
    use std::{
        collections::BTreeMap,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tonic::metadata::MetadataValue;

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
            .write_collection(Request::new(write_collection_request(
                "documents",
                vec![
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "alpha".to_owned(),
                            vector: vec![1.0, 0.0],
                            metadata_json: r#"{"kind":"keep","color":"red"}"#.to_owned(),
                        })),
                    },
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "beta".to_owned(),
                            vector: vec![3.0, 0.0],
                            metadata_json: r#"{"kind":"drop","color":"blue"}"#.to_owned(),
                        })),
                    },
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "gamma".to_owned(),
                            vector: vec![2.0, 0.0],
                            metadata_json: r#"{"kind":"keep","color":"red"}"#.to_owned(),
                        })),
                    },
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
            .write_collection(Request::new(write_collection_request(
                "documents",
                vec![
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "alpha".to_owned(),
                            vector: vec![1.0, 0.0],
                            metadata_json: r#"{"kind":"keep"}"#.to_owned(),
                        })),
                    },
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "beta".to_owned(),
                            vector: vec![0.0, 1.0],
                            metadata_json: r#"{"kind":"drop"}"#.to_owned(),
                        })),
                    },
                ],
            )))
            .await
            .expect("write should succeed");

        service
            .flush_collection(Request::new(flush_collection_request("documents")))
            .await
            .expect("flush should succeed");

        service
            .write_collection(Request::new(write_collection_request(
                "documents",
                vec![proto::WriteOperation {
                    operation: Some(proto::write_operation::Operation::Delete(
                        proto::DeleteRecord {
                            id: "alpha".to_owned(),
                        },
                    )),
                }],
            )))
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
        assert_eq!(
            wal_payload["records"]
                .as_array()
                .expect("wal records should be an array")
                .len(),
            1
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

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error
                .message()
                .contains("data-only nodes cannot accept control-plane database mutations")
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
            .write_collection(authorized_request(
                write_collection_request(
                    "documents",
                    vec![proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "alpha".to_owned(),
                            vector: vec![1.0, 0.0],
                            metadata_json: "{}".to_owned(),
                        })),
                    }],
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

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains(
            "data-only nodes cannot accept control-plane collection lifecycle mutations"
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

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("without a local data plane"));
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
                    .write_collection(Request::new(write_collection_request(
                        "documents",
                        vec![proto::WriteOperation {
                            operation: Some(proto::write_operation::Operation::Put(
                                proto::PutRecord {
                                    id: "alpha".to_owned(),
                                    vector: vec![1.0, 0.0],
                                    metadata_json: r#"{"kind":"keep"}"#.to_owned(),
                                },
                            )),
                        }],
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
                tonic::Code::InvalidArgument,
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
                    .write_collection(Request::new(write_collection_request(
                        "documents",
                        vec![proto::WriteOperation {
                            operation: Some(proto::write_operation::Operation::Put(
                                proto::PutRecord {
                                    id: "alpha".to_owned(),
                                    vector: vec![1.0, 0.0],
                                    metadata_json: r#"{"kind":"keep"}"#.to_owned(),
                                },
                            )),
                        }],
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
                tonic::Code::InvalidArgument,
                "{operation} should be rejected for recorded remote assignments"
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
        assert!(
            error
                .message()
                .contains("dimensions must be greater than 0")
        );
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
            .write_collection(Request::new(write_collection_request(
                "documents",
                vec![
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "lower".to_owned(),
                            vector: vec![1.0, 0.0],
                            metadata_json: r#"{"score":9007199254740992}"#.to_owned(),
                        })),
                    },
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "higher".to_owned(),
                            vector: vec![2.0, 0.0],
                            metadata_json: r#"{"score":9007199254740993}"#.to_owned(),
                        })),
                    },
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
            .write_collection(Request::new(write_collection_request(
                "documents",
                vec![
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "alpha".to_owned(),
                            vector: vec![1.0, 0.0],
                            metadata_json: r#"{"kind":"keep","version":1}"#.to_owned(),
                        })),
                    },
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "beta".to_owned(),
                            vector: vec![2.0, 0.0],
                            metadata_json: r#"{"kind":"drop","version":2}"#.to_owned(),
                        })),
                    },
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "gamma".to_owned(),
                            vector: vec![3.0, 0.0],
                            metadata_json: r#"{"kind":"drop","version":3}"#.to_owned(),
                        })),
                    },
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "delta".to_owned(),
                            vector: vec![4.0, 0.0],
                            metadata_json: r#"{"kind":"drop","version":4}"#.to_owned(),
                        })),
                    },
                    proto::WriteOperation {
                        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                            id: "epsilon".to_owned(),
                            vector: vec![5.0, 0.0],
                            metadata_json: r#"{"kind":"keep","version":5}"#.to_owned(),
                        })),
                    },
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
    async fn grpc_write_rejects_empty_put_record_id() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-empty-put-id"))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        let error = service
            .write_collection(Request::new(write_collection_request(
                "documents",
                vec![proto::WriteOperation {
                    operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
                        id: String::new(),
                        vector: vec![1.0, 0.0],
                        metadata_json: "{}".to_owned(),
                    })),
                }],
            )))
            .await
            .expect_err("empty put record id should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error
                .message()
                .contains("put operation record id must not be empty")
        );
    }

    #[tokio::test]
    async fn grpc_write_rejects_empty_delete_record_id() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-empty-delete-id"))));

        service
            .create_collection(Request::new(create_collection_request(
                "documents",
                2,
                proto::DistanceMetric::Dot,
            )))
            .await
            .expect("create should succeed");

        let error = service
            .write_collection(Request::new(write_collection_request(
                "documents",
                vec![proto::WriteOperation {
                    operation: Some(proto::write_operation::Operation::Delete(
                        proto::DeleteRecord { id: String::new() },
                    )),
                }],
            )))
            .await
            .expect_err("empty delete record id should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error
                .message()
                .contains("delete operation record id must not be empty")
        );
    }

    fn default_database_name() -> String {
        DEFAULT_DATABASE_NAME.to_owned()
    }

    fn create_collection_request(
        name: &str,
        dimensions: u64,
        metric: proto::DistanceMetric,
    ) -> CreateCollectionRequest {
        CreateCollectionRequest {
            name: name.to_owned(),
            dimensions,
            metric: metric as i32,
            database_name: default_database_name(),
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
            collection_name: collection_name.to_owned(),
            database_name: default_database_name(),
        }
    }

    fn get_collection_placement_request(collection_name: &str) -> GetCollectionPlacementRequest {
        GetCollectionPlacementRequest {
            collection_name: collection_name.to_owned(),
            database_name: default_database_name(),
        }
    }

    fn write_collection_request(
        collection_name: &str,
        operations: Vec<proto::WriteOperation>,
    ) -> WriteCollectionRequest {
        WriteCollectionRequest {
            collection_name: collection_name.to_owned(),
            operations,
            database_name: default_database_name(),
        }
    }

    fn query_collection_request(
        collection_name: &str,
        vector: Vec<f32>,
        top_k: u64,
    ) -> QueryCollectionRequest {
        QueryCollectionRequest {
            collection_name: collection_name.to_owned(),
            vector,
            top_k,
            snapshot: None,
            filters: Vec::new(),
            predicate: None,
            explain: proto::ExplainMode::None as i32,
            database_name: default_database_name(),
        }
    }

    fn get_collection_stats_request(collection_name: &str) -> GetCollectionStatsRequest {
        GetCollectionStatsRequest {
            collection_name: collection_name.to_owned(),
            database_name: default_database_name(),
            snapshot: None,
        }
    }

    fn flush_collection_request(collection_name: &str) -> FlushCollectionRequest {
        FlushCollectionRequest {
            collection_name: collection_name.to_owned(),
            database_name: default_database_name(),
        }
    }

    fn compact_collection_request(collection_name: &str) -> CompactCollectionRequest {
        CompactCollectionRequest {
            collection_name: collection_name.to_owned(),
            database_name: default_database_name(),
        }
    }

    fn inspect_collection_request(
        collection_name: &str,
        target: proto::InspectTarget,
        segment_id: impl Into<String>,
    ) -> InspectCollectionRequest {
        InspectCollectionRequest {
            collection_name: collection_name.to_owned(),
            target: target as i32,
            segment_id: segment_id.into(),
            database_name: default_database_name(),
        }
    }

    fn put_database_policy_request(database_name: &str) -> proto::PutDatabasePolicyRequest {
        proto::PutDatabasePolicyRequest {
            policy: Some(proto::DatabaseAccessPolicyReply {
                database_name: database_name.to_owned(),
                authentication_mode: proto::AuthenticationMode::ExternalToken as i32,
                role_bindings: vec![
                    proto::DatabaseRoleBindingReply {
                        database_name: database_name.to_owned(),
                        principal_name: "ops-admin".to_owned(),
                        role: proto::DatabaseRole::Owner as i32,
                    },
                    proto::DatabaseRoleBindingReply {
                        database_name: database_name.to_owned(),
                        principal_name: "reader-service".to_owned(),
                        role: proto::DatabaseRole::ReadOnly as i32,
                    },
                ],
            }),
        }
    }

    fn get_database_policy_request(database_name: &str) -> proto::GetDatabasePolicyRequest {
        proto::GetDatabasePolicyRequest {
            database_name: database_name.to_owned(),
        }
    }

    fn put_database_request(database_name: &str) -> proto::PutDatabaseRequest {
        proto::PutDatabaseRequest {
            descriptor: Some(proto::DatabaseDescriptorReply {
                database_id: logpose_catalog::DatabaseDescriptor::new(database_name)
                    .database_id
                    .to_string(),
                name: database_name.to_owned(),
                is_default: database_name == DEFAULT_DATABASE_NAME,
            }),
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
