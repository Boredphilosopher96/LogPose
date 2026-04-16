//! gRPC API surface for LogPose.

use logpose_core::AppState;
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator,
    QueryDiagnostics, QueryPlanKind, QueryRequest, QueryStageTimings, ScalarMetadataValue,
};
use logpose_service::ServiceError;
use logpose_storage::CreateCollectionRequest as StorageCreateCollectionRequest;
use logpose_types::{
    CollectionPlacement, DeleteRecord, DistanceMetric, MaintenanceBacklog, MaintenanceStatus,
    NodeRole, NodeRuntimeStatus, PutRecord, QueryUnitStats, RecordId, ScalarFieldStats, Snapshot,
    WriteOperation,
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
    CompactCollectionRequest, CreateCollectionRequest, FlushCollectionRequest,
    GetCollectionPlacementRequest, GetCollectionRequest, GetCollectionStatsRequest,
    GetMetadataReply, GetMetadataRequest, GetRuntimeStatusReply, GetRuntimeStatusRequest,
    InspectCollectionReply, InspectCollectionRequest, InspectTarget, MaintenanceBacklogReply,
    NodeRole as ProtoNodeRole, QueryCollectionReply, QueryCollectionRequest, QueryMatch,
    RemoteBlobConfig, ScalarValue, SnapshotReply, WriteCollectionRequest,
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
        _request: Request<GetRuntimeStatusRequest>,
    ) -> Result<Response<GetRuntimeStatusReply>, Status> {
        let status = self
            .state
            .control
            .runtime_status()
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(runtime_status_reply_from_domain(status)))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CollectionDescriptorReply>, Status> {
        let request = request.into_inner();
        let descriptor = self
            .state
            .control
            .create_collection(StorageCreateCollectionRequest {
                name: request.name,
                dimensions: request.dimensions as usize,
                metric: metric_from_proto(request.metric)?,
            })
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(collection_descriptor_reply(descriptor)))
    }

    async fn get_collection_placement(
        &self,
        request: Request<GetCollectionPlacementRequest>,
    ) -> Result<Response<CollectionPlacementReply>, Status> {
        let placement = self
            .state
            .control
            .collection_placement(&request.into_inner().collection_name)
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
        let descriptor = self
            .state
            .get_collection(&request.into_inner().collection_name)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(collection_descriptor_reply(descriptor)))
    }

    async fn write_collection(
        &self,
        request: Request<WriteCollectionRequest>,
    ) -> Result<Response<CommitAckReply>, Status> {
        let request = request.into_inner();
        let operations = request
            .operations
            .into_iter()
            .map(write_operation_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let ack = self
            .state
            .write(&request.collection_name, operations)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(CommitAckReply {
            last_seq_no: ack.last_seq_no,
            applied_ops: ack.applied_ops as u64,
        }))
    }

    async fn query_collection(
        &self,
        request: Request<QueryCollectionRequest>,
    ) -> Result<Response<QueryCollectionReply>, Status> {
        let request = request.into_inner();
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
            .query(QueryRequest {
                collection_name: request.collection_name,
                vector: request.vector,
                top_k: request.top_k as usize,
                snapshot: request.snapshot.map(snapshot_from_proto),
                filters,
                predicate,
                explain: explain_mode_from_proto(request.explain)?,
            })
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
        }))
    }

    async fn get_collection_stats(
        &self,
        request: Request<GetCollectionStatsRequest>,
    ) -> Result<Response<CollectionStatsReply>, Status> {
        let stats = self
            .state
            .stats(&request.into_inner().collection_name)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(collection_stats_reply_from_domain(stats)?))
    }

    async fn flush_collection(
        &self,
        request: Request<FlushCollectionRequest>,
    ) -> Result<Response<SnapshotReply>, Status> {
        let snapshot = self
            .state
            .flush(&request.into_inner().collection_name)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(snapshot_reply_from_domain(snapshot)))
    }

    async fn compact_collection(
        &self,
        request: Request<CompactCollectionRequest>,
    ) -> Result<Response<SnapshotReply>, Status> {
        let snapshot = self
            .state
            .compact(&request.into_inner().collection_name)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(snapshot_reply_from_domain(snapshot)))
    }

    async fn inspect_collection(
        &self,
        request: Request<InspectCollectionRequest>,
    ) -> Result<Response<InspectCollectionReply>, Status> {
        let request = request.into_inner();
        let target = inspect_target_from_proto(request.target, request.segment_id)?;
        let report = self
            .state
            .inspect(&request.collection_name, target)
            .await
            .map_err(status_from_service_error)?;
        let payload_json = serde_json::to_string(&report.payload).map_err(|error| {
            Status::internal(format!("failed to serialize inspect payload: {error}"))
        })?;
        Ok(Response::new(InspectCollectionReply {
            target: report.target,
            payload_json,
        }))
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

fn snapshot_reply_from_domain(snapshot: Snapshot) -> SnapshotReply {
    SnapshotReply {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
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
        route_kind: placement.route_kind,
        route_reason: placement.route_reason,
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

fn status_from_service_error(error: ServiceError) -> Status {
    match error {
        ServiceError::AlreadyExists(message) => Status::already_exists(message),
        ServiceError::NotFound(message) => Status::not_found(message),
        ServiceError::InvalidArgument(message) => Status::invalid_argument(message),
        ServiceError::Internal(message) => Status::internal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_config::LogPoseConfig;
    use logpose_query::{QueryDiagnostics, QueryPlanKind, QueryStageTimings};
    use serde_json::Value;
    use std::{
        collections::BTreeMap,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

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
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
            .await
            .expect("create should succeed")
            .into_inner();
        assert_eq!(create.name, "documents");

        service
            .write_collection(Request::new(WriteCollectionRequest {
                collection_name: "documents".to_owned(),
                operations: vec![
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
            }))
            .await
            .expect("write should succeed");

        let query = service
            .query_collection(Request::new(QueryCollectionRequest {
                collection_name: "documents".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 3,
                snapshot: None,
                filters: vec![proto::MetadataFilter {
                    field: "kind".to_owned(),
                    value: Some(proto::ScalarValue {
                        kind: Some(proto::scalar_value::Kind::StringValue("keep".to_owned())),
                    }),
                }],
                predicate: None,
                explain: proto::ExplainMode::None as i32,
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
            .get_collection_stats(Request::new(GetCollectionStatsRequest {
                collection_name: "documents".to_owned(),
            }))
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
            .flush_collection(Request::new(FlushCollectionRequest {
                collection_name: "documents".to_owned(),
            }))
            .await
            .expect("flush should succeed")
            .into_inner();
        assert!(flush.manifest_generation >= 1);

        let compact = service
            .compact_collection(Request::new(CompactCollectionRequest {
                collection_name: "documents".to_owned(),
            }))
            .await
            .expect("compact should succeed")
            .into_inner();
        assert!(compact.manifest_generation >= flush.manifest_generation);

        let inspect = service
            .inspect_collection(Request::new(InspectCollectionRequest {
                collection_name: "documents".to_owned(),
                target: proto::InspectTarget::Manifest as i32,
                segment_id: String::new(),
            }))
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
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
            .await
            .expect("create should succeed");

        service
            .write_collection(Request::new(WriteCollectionRequest {
                collection_name: "documents".to_owned(),
                operations: vec![
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
            }))
            .await
            .expect("write should succeed");

        service
            .flush_collection(Request::new(FlushCollectionRequest {
                collection_name: "documents".to_owned(),
            }))
            .await
            .expect("flush should succeed");

        service
            .write_collection(Request::new(WriteCollectionRequest {
                collection_name: "documents".to_owned(),
                operations: vec![proto::WriteOperation {
                    operation: Some(proto::write_operation::Operation::Delete(
                        proto::DeleteRecord {
                            id: "alpha".to_owned(),
                        },
                    )),
                }],
            }))
            .await
            .expect("delete should succeed");

        let manifest = service
            .inspect_collection(Request::new(InspectCollectionRequest {
                collection_name: "documents".to_owned(),
                target: proto::InspectTarget::Manifest as i32,
                segment_id: String::new(),
            }))
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
            .inspect_collection(Request::new(InspectCollectionRequest {
                collection_name: "documents".to_owned(),
                target: proto::InspectTarget::Wal as i32,
                segment_id: String::new(),
            }))
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
            .inspect_collection(Request::new(InspectCollectionRequest {
                collection_name: "documents".to_owned(),
                target: proto::InspectTarget::Segment as i32,
                segment_id: segment_id.clone(),
            }))
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
            .inspect_collection(Request::new(InspectCollectionRequest {
                collection_name: "documents".to_owned(),
                target: proto::InspectTarget::Maintenance as i32,
                segment_id: String::new(),
            }))
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
    async fn grpc_runtime_status_reports_control_plane_summary() {
        let state = Arc::new(AppState::new(test_config("grpc-runtime-status")));
        state
            .control
            .create_collection(StorageCreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Dot,
            })
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
    }

    #[tokio::test]
    async fn grpc_collection_placement_reports_local_assignment() {
        let state = Arc::new(AppState::new(test_config("grpc-placement")));
        state
            .control
            .create_collection(StorageCreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Dot,
            })
            .await
            .expect("collection should be created");
        let service = GrpcLogPoseService::new(state);

        let placement = service
            .get_collection_placement(Request::new(GetCollectionPlacementRequest {
                collection_name: "documents".to_owned(),
            }))
            .await
            .expect("placement should succeed")
            .into_inner();

        assert_eq!(placement.collection_name, "documents");
        assert_eq!(placement.assigned_node, "grpc-placement");
        assert_eq!(placement.assigned_role, proto::NodeRole::Data as i32);
        assert_eq!(placement.route_kind, "local");
    }

    #[tokio::test]
    async fn data_only_nodes_reject_control_plane_collection_creation() {
        let service = GrpcLogPoseService::new(Arc::new(AppState::new(test_config_with_role(
            "grpc-data-only",
            NodeRole::Data,
        ))));

        let error = service
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
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
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
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
            .create_collection(StorageCreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Dot,
            })
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
                    .write_collection(Request::new(WriteCollectionRequest {
                        collection_name: "documents".to_owned(),
                        operations: vec![proto::WriteOperation {
                            operation: Some(proto::write_operation::Operation::Put(
                                proto::PutRecord {
                                    id: "alpha".to_owned(),
                                    vector: vec![1.0, 0.0],
                                    metadata_json: r#"{"kind":"keep"}"#.to_owned(),
                                },
                            )),
                        }],
                    }))
                    .await
                    .expect_err("control-only node should reject writes"),
            ),
            (
                "query",
                service
                    .query_collection(Request::new(QueryCollectionRequest {
                        collection_name: "documents".to_owned(),
                        vector: vec![1.0, 0.0],
                        top_k: 1,
                        snapshot: None,
                        filters: Vec::new(),
                        predicate: None,
                        explain: proto::ExplainMode::None as i32,
                    }))
                    .await
                    .expect_err("control-only node should reject queries"),
            ),
            (
                "stats",
                service
                    .get_collection_stats(Request::new(GetCollectionStatsRequest {
                        collection_name: "documents".to_owned(),
                    }))
                    .await
                    .expect_err("control-only node should reject stats"),
            ),
            (
                "flush",
                service
                    .flush_collection(Request::new(FlushCollectionRequest {
                        collection_name: "documents".to_owned(),
                    }))
                    .await
                    .expect_err("control-only node should reject flush"),
            ),
            (
                "compact",
                service
                    .compact_collection(Request::new(CompactCollectionRequest {
                        collection_name: "documents".to_owned(),
                    }))
                    .await
                    .expect_err("control-only node should reject compact"),
            ),
            (
                "inspect",
                service
                    .inspect_collection(Request::new(InspectCollectionRequest {
                        collection_name: "documents".to_owned(),
                        target: InspectTarget::Manifest as i32,
                        segment_id: String::new(),
                    }))
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
            .create_collection(StorageCreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Dot,
            })
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
                    .write_collection(Request::new(WriteCollectionRequest {
                        collection_name: "documents".to_owned(),
                        operations: vec![proto::WriteOperation {
                            operation: Some(proto::write_operation::Operation::Put(
                                proto::PutRecord {
                                    id: "alpha".to_owned(),
                                    vector: vec![1.0, 0.0],
                                    metadata_json: r#"{"kind":"keep"}"#.to_owned(),
                                },
                            )),
                        }],
                    }))
                    .await
                    .expect_err("recorded remote writes should be rejected"),
            ),
            (
                "query",
                service
                    .query_collection(Request::new(QueryCollectionRequest {
                        collection_name: "documents".to_owned(),
                        vector: vec![1.0, 0.0],
                        top_k: 1,
                        snapshot: None,
                        filters: Vec::new(),
                        predicate: None,
                        explain: proto::ExplainMode::None as i32,
                    }))
                    .await
                    .expect_err("recorded remote queries should be rejected"),
            ),
            (
                "stats",
                service
                    .get_collection_stats(Request::new(GetCollectionStatsRequest {
                        collection_name: "documents".to_owned(),
                    }))
                    .await
                    .expect_err("recorded remote stats should be rejected"),
            ),
            (
                "flush",
                service
                    .flush_collection(Request::new(FlushCollectionRequest {
                        collection_name: "documents".to_owned(),
                    }))
                    .await
                    .expect_err("recorded remote flush should be rejected"),
            ),
            (
                "compact",
                service
                    .compact_collection(Request::new(CompactCollectionRequest {
                        collection_name: "documents".to_owned(),
                    }))
                    .await
                    .expect_err("recorded remote compaction should be rejected"),
            ),
            (
                "inspect",
                service
                    .inspect_collection(Request::new(InspectCollectionRequest {
                        collection_name: "documents".to_owned(),
                        target: InspectTarget::Manifest as i32,
                        segment_id: String::new(),
                    }))
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
            .get_collection(Request::new(GetCollectionRequest {
                collection_name: "missing".to_owned(),
            }))
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
            .get_collection_placement(Request::new(GetCollectionPlacementRequest {
                collection_name: "missing".to_owned(),
            }))
            .await
            .expect_err("missing collection placement should error");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn grpc_service_rejects_zero_dimensions_for_collection_creation() {
        let service =
            GrpcLogPoseService::new(Arc::new(AppState::new(test_config("grpc-zero-dimensions"))));

        let error = service
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 0,
                metric: proto::DistanceMetric::Dot as i32,
            }))
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
                collection_name: "documents".to_owned(),
                target: 999,
                segment_id: String::new(),
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
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
            .await
            .expect("create should succeed");

        service
            .write_collection(Request::new(WriteCollectionRequest {
                collection_name: "documents".to_owned(),
                operations: vec![
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
            }))
            .await
            .expect("write should succeed");

        let query = service
            .query_collection(Request::new(QueryCollectionRequest {
                collection_name: "documents".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 5,
                snapshot: None,
                filters: vec![proto::MetadataFilter {
                    field: "score".to_owned(),
                    value: Some(proto::ScalarValue {
                        kind: Some(proto::scalar_value::Kind::Uint64Value(9007199254740993)),
                    }),
                }],
                predicate: None,
                explain: proto::ExplainMode::None as i32,
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
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
            .await
            .expect("create should succeed");

        service
            .write_collection(Request::new(WriteCollectionRequest {
                collection_name: "documents".to_owned(),
                operations: vec![
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
            }))
            .await
            .expect("write should succeed");

        let query = service
            .query_collection(Request::new(QueryCollectionRequest {
                collection_name: "documents".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
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
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
            .await
            .expect("create should succeed");

        let error = service
            .query_collection(Request::new(QueryCollectionRequest {
                collection_name: "documents".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
                predicate: Some(proto::Predicate {
                    node: Some(proto::predicate::Node::Comparison(
                        proto::PredicateComparison {
                            field: "kind".to_owned(),
                            operator: proto::PredicateOperator::Eq as i32,
                            value: None,
                        },
                    )),
                }),
                explain: proto::ExplainMode::None as i32,
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
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
            .await
            .expect("create should succeed");

        let error = service
            .query_collection(Request::new(QueryCollectionRequest {
                collection_name: "documents".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
                predicate: Some(proto::Predicate {
                    node: Some(proto::predicate::Node::And(proto::PredicateList {
                        children: Vec::new(),
                    })),
                }),
                explain: proto::ExplainMode::None as i32,
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
            .create_collection(Request::new(CreateCollectionRequest {
                name: "documents".to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
            }))
            .await
            .expect("create should succeed");

        let error = service
            .query_collection(Request::new(QueryCollectionRequest {
                collection_name: "documents".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
                predicate: None,
                explain: 99,
            }))
            .await
            .expect_err("unknown explain mode should error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
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
