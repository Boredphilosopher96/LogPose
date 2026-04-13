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
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator,
    QueryDiagnostics, QueryMatch, QueryPlanKind, QueryRequest, QueryResponse, QueryStageTimings,
    ScalarMetadataValue,
};
use logpose_storage::{CreateCollectionRequest, InspectReport, InspectTarget};
use logpose_types::{
    CollectionId, CollectionStats, CommitAck, DistanceMetric, LogPoseError, MaintenanceStatus,
    NodeMetadata, QueryUnitStats, RecordId, RemoteBlobConfig, ScalarFieldStats, Snapshot,
    WriteOperation,
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
                predicate: request.predicate.map(predicate_to_proto).transpose()?,
                explain: explain_mode_to_proto(request.explain) as i32,
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
            diagnostics: response
                .diagnostics
                .map(query_diagnostics_from_proto)
                .transpose()?,
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
}
