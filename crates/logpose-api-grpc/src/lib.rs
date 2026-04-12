//! gRPC API surface for LogPose.

use logpose_core::AppState;
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator,
    QueryDiagnostics, QueryPlanKind, QueryRequest, QueryStageTimings, ScalarMetadataValue,
};
use logpose_service::ServiceError;
use logpose_storage::CreateCollectionRequest as StorageCreateCollectionRequest;
use logpose_types::{
    DeleteRecord, DistanceMetric, MaintenanceStatus, PutRecord, QueryUnitStats, RecordId,
    ScalarFieldStats, Snapshot, WriteOperation,
};
use serde_json::{Number, Value};
use std::{net::SocketAddr, sync::Arc};
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
    CollectionDescriptorReply, CollectionStatsReply, CommitAckReply, CompactCollectionRequest,
    CreateCollectionRequest, FlushCollectionRequest, GetCollectionRequest,
    GetCollectionStatsRequest, GetMetadataReply, GetMetadataRequest, InspectCollectionReply,
    InspectCollectionRequest, InspectTarget, QueryCollectionReply, QueryCollectionRequest,
    QueryMatch, RemoteBlobConfig, ScalarValue, SnapshotReply, WriteCollectionRequest,
};

/// Serve the gRPC API until shutdown.
pub async fn serve(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = SocketAddr::from((
        state.config.grpc_host.parse::<std::net::IpAddr>()?,
        state.config.grpc_port,
    ));

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<LogPoseServiceServer<GrpcLogPoseService>>()
        .await;

    info!(%address, "starting gRPC listener");

    Server::builder()
        .add_service(health_service)
        .add_service(LogPoseServiceServer::new(GrpcLogPoseService::new(state)))
        .serve(address)
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
        let metadata = self.state.metadata();
        Ok(Response::new(GetMetadataReply {
            product: metadata.product,
            node_name: metadata.node_name,
            version: metadata.version,
            git_sha: metadata.git_sha,
            profile: metadata.profile,
        }))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CollectionDescriptorReply>, Status> {
        let request = request.into_inner();
        let descriptor = self
            .state
            .service
            .create_collection(StorageCreateCollectionRequest {
                name: request.name,
                dimensions: request.dimensions as usize,
                metric: metric_from_proto(request.metric)?,
            })
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(collection_descriptor_reply(descriptor)))
    }

    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<CollectionDescriptorReply>, Status> {
        let descriptor = self
            .state
            .service
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
            .service
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
        let filters = request
            .filters
            .into_iter()
            .map(metadata_filter_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let predicate = request.predicate.map(predicate_from_proto).transpose()?;
        let response = self
            .state
            .service
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
                .map(|candidate| QueryMatch {
                    id: candidate.id.to_string(),
                    value: candidate.value,
                    metadata_json: serde_json::to_string(&candidate.metadata)
                        .expect("query match metadata should serialize"),
                })
                .collect(),
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
            .service
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
            .service
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
            .service
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
            .service
            .inspect(&request.collection_name, target)
            .await
            .map_err(status_from_service_error)?;
        Ok(Response::new(InspectCollectionReply {
            target: report.target,
            payload_json: serde_json::to_string(&report.payload)
                .expect("inspect payload should serialize"),
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
        rerank_count: diagnostics.rerank_count as u64,
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
    }
}

fn query_stage_timings_to_proto(timings: QueryStageTimings) -> proto::QueryStageTimings {
    proto::QueryStageTimings {
        planning_micros: timings.planning_micros,
        predicate_micros: timings.predicate_micros,
        ranking_micros: timings.ranking_micros,
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

fn query_unit_stats_to_proto(stats: QueryUnitStats) -> Result<proto::QueryUnitStats, Status> {
    Ok(proto::QueryUnitStats {
        unit_id: stats.unit_id,
        tier: stats.tier,
        index_kind: stats.index_kind,
        index_file_name: stats.index_file_name,
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
    use serde_json::Value;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

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
        LogPoseConfig {
            node_name: label.to_owned(),
            storage_root: unique_temp_dir(label),
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
