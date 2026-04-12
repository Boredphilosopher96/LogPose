//! gRPC-backed client helpers for LogPose operator workflows.

use logpose_api_grpc::proto::{
    self, CollectionDescriptorReply, CompactCollectionRequest,
    CreateCollectionRequest as ProtoCreateCollectionRequest, FlushCollectionRequest,
    GetCollectionRequest, GetCollectionStatsRequest, GetMetadataRequest, InspectCollectionRequest,
    QueryCollectionRequest, ScalarValue, WriteCollectionRequest,
    log_pose_service_client::LogPoseServiceClient,
};
use logpose_catalog::CollectionDescriptor;
#[cfg(test)]
use logpose_config as _;
#[cfg(test)]
use logpose_core as _;
use logpose_query::{MetadataFilter, QueryMatch, QueryRequest, QueryResponse, ScalarMetadataValue};
use logpose_storage::{CreateCollectionRequest, InspectReport, InspectTarget};
use logpose_types::{
    CollectionId, CollectionStats, CommitAck, DistanceMetric, LogPoseError, NodeMetadata, RecordId,
    RemoteBlobConfig, Snapshot, WriteOperation,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
#[cfg(test)]
use tokio as _;
use tonic::{Request, transport::Channel};

/// Client connection settings shared across tools and SDKs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    /// gRPC endpoint URL.
    pub grpc_endpoint: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            grpc_endpoint: "http://127.0.0.1:50051".to_owned(),
        }
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
    /// The server returned malformed JSON payloads.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Thin gRPC client over the shared LogPose server contract.
#[derive(Clone)]
pub struct LogPoseClient {
    inner: LogPoseServiceClient<Channel>,
}

impl LogPoseClient {
    /// Connect to a LogPose gRPC endpoint.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let inner = LogPoseServiceClient::connect(endpoint.into()).await?;
        Ok(Self { inner })
    }

    /// Connect using a shared client configuration.
    pub async fn from_config(config: &ClientConfig) -> Result<Self> {
        Self::connect(config.grpc_endpoint.clone()).await
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
            }))
            .await?
            .into_inner();
        collection_descriptor_from_proto(response)
    }

    /// Fetch collection metadata by name.
    pub async fn get_collection(&self, collection_name: &str) -> Result<CollectionDescriptor> {
        let response = self
            .inner
            .clone()
            .get_collection(Request::new(GetCollectionRequest {
                collection_name: collection_name.to_owned(),
            }))
            .await?
            .into_inner();
        collection_descriptor_from_proto(response)
    }

    /// Persist a write batch durably.
    pub async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        let response = self
            .inner
            .clone()
            .write_collection(Request::new(WriteCollectionRequest {
                collection_name: collection_name.to_owned(),
                operations: operations
                    .into_iter()
                    .map(write_operation_to_proto)
                    .collect::<Vec<_>>(),
            }))
            .await?
            .into_inner();
        Ok(CommitAck {
            last_seq_no: response.last_seq_no,
            applied_ops: response.applied_ops as usize,
        })
    }

    /// Execute an exact query through the shared service contract.
    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse> {
        let response = self
            .inner
            .clone()
            .query_collection(Request::new(QueryCollectionRequest {
                collection_name: request.collection_name,
                vector: request.vector,
                top_k: request.top_k as u64,
                snapshot: request.snapshot.map(snapshot_to_proto),
                filters: request
                    .filters
                    .into_iter()
                    .map(metadata_filter_to_proto)
                    .collect::<Result<Vec<_>>>()?,
            }))
            .await?
            .into_inner();

        Ok(QueryResponse {
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
        })
    }

    /// Fetch collection-level statistics.
    pub async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        let response = self
            .inner
            .clone()
            .get_collection_stats(Request::new(GetCollectionStatsRequest {
                collection_name: collection_name.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(CollectionStats {
            collection_id: parse_collection_id(&response.collection_id)?,
            collection_name: response.collection_name,
            manifest_generation: response.manifest_generation,
            visible_seq_no: response.visible_seq_no,
            mutable_op_count: response.mutable_op_count as usize,
            segment_count: response.segment_count as usize,
            live_record_count: response.live_record_count as usize,
            deleted_record_count: response.deleted_record_count as usize,
        })
    }

    /// Flush the mutable delta into a new segment.
    pub async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        let response = self
            .inner
            .clone()
            .flush_collection(Request::new(FlushCollectionRequest {
                collection_name: collection_name.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(snapshot_reply_from_proto(response))
    }

    /// Compact immutable segments.
    pub async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        let response = self
            .inner
            .clone()
            .compact_collection(Request::new(CompactCollectionRequest {
                collection_name: collection_name.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(snapshot_reply_from_proto(response))
    }

    /// Inspect operator-visible storage state.
    pub async fn inspect(
        &self,
        collection_name: &str,
        target: InspectTarget,
    ) -> Result<InspectReport> {
        let response = self
            .inner
            .clone()
            .inspect_collection(Request::new(InspectCollectionRequest {
                collection_name: collection_name.to_owned(),
                target: inspect_target_to_proto(&target) as i32,
                segment_id: inspect_segment_id(&target),
            }))
            .await?
            .into_inner();
        Ok(InspectReport {
            target: response.target,
            payload: serde_json::from_str(&response.payload_json)?,
        })
    }
}

fn collection_descriptor_from_proto(
    reply: CollectionDescriptorReply,
) -> Result<CollectionDescriptor> {
    Ok(CollectionDescriptor {
        collection_id: parse_collection_id(&reply.collection_id)?,
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

fn parse_collection_id(value: &str) -> Result<CollectionId> {
    value.parse().map(CollectionId).map_err(|error| {
        ClientError::InvalidResponse(format!("invalid collection id '{value}': {error}"))
    })
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

fn query_match_from_proto(candidate: proto::QueryMatch) -> Result<QueryMatch> {
    Ok(QueryMatch {
        id: RecordId::new(candidate.id),
        value: candidate.value,
        metadata: serde_json::from_str(&candidate.metadata_json)?,
    })
}

fn inspect_target_to_proto(target: &InspectTarget) -> proto::InspectTarget {
    match target {
        InspectTarget::Manifest => proto::InspectTarget::Manifest,
        InspectTarget::Wal => proto::InspectTarget::Wal,
        InspectTarget::Segment(_) => proto::InspectTarget::Segment,
    }
}

fn inspect_segment_id(target: &InspectTarget) -> String {
    match target {
        InspectTarget::Segment(segment_id) => segment_id.clone(),
        InspectTarget::Manifest | InspectTarget::Wal => String::new(),
    }
}

impl From<LogPoseError> for ClientError {
    fn from(error: LogPoseError) -> Self {
        Self::InvalidResponse(error.to_string())
    }
}
