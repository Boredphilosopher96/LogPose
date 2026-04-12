//! gRPC API surface for LogPose.

use logpose_core::AppState;
use logpose_query::{MetadataFilter, QueryRequest, ScalarMetadataValue};
use logpose_service::ServiceError;
use logpose_storage::CreateCollectionRequest as StorageCreateCollectionRequest;
use logpose_types::{DeleteRecord, DistanceMetric, PutRecord, RecordId, Snapshot, WriteOperation};
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
    QueryMatch, ScalarValue, SnapshotReply, WriteCollectionRequest,
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
        Ok(Response::new(GetMetadataReply {
            product: "LogPose".to_owned(),
            node_name: self.state.config.node_name.clone(),
            version: self.state.build.version.clone(),
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
        let response = self
            .state
            .service
            .query(QueryRequest {
                collection_name: request.collection_name,
                vector: request.vector,
                top_k: request.top_k as usize,
                snapshot: request.snapshot.map(snapshot_from_proto),
                filters,
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
        Ok(Response::new(CollectionStatsReply {
            collection_id: stats.collection_id.to_string(),
            collection_name: stats.collection_name,
            manifest_generation: stats.manifest_generation,
            visible_seq_no: stats.visible_seq_no,
            mutable_op_count: stats.mutable_op_count as u64,
            segment_count: stats.segment_count as u64,
            live_record_count: stats.live_record_count as u64,
            deleted_record_count: stats.deleted_record_count as u64,
        }))
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
    }
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

    fn test_config(label: &str) -> LogPoseConfig {
        LogPoseConfig {
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
