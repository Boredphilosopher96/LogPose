//! The single-vector query shape these tests were written against, served through the API
//! query (`query_collection`) on collections of the legacy layout: key `id`, one vector field,
//! and metadata kept in `$extra`. Equality `filters` and the `predicate` combine with AND.

#![allow(dead_code)]

use logpose_core::AppState;
use logpose_query::{
    ExplainMode, FilterExpr, QueryDiagnostics, ReadConsistency, VectorQuery, WithSchema,
};
use logpose_service::LogPoseDataService;
use logpose_types::{
    DistanceMetric, LogPoseError, RecordId, ScalarMetadataValue, Snapshot, value::Value,
};
use serde_json::Value as Json;
use std::future::Future;

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataFilter {
    pub field: String,
    pub value: ScalarMetadataValue,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryRequest {
    pub collection_name: String,
    pub vector: Vec<f32>,
    pub top_k: usize,
    pub snapshot: Option<Snapshot>,
    pub read_barrier: Option<Snapshot>,
    pub filters: Vec<MetadataFilter>,
    pub predicate: Option<FilterExpr>,
    pub explain: ExplainMode,
    pub snapshot_token: Option<String>,
    pub pin: bool,
}

/// One hit: its key, metric value, and metadata (`$extra`).
#[derive(Clone, Debug, PartialEq)]
pub struct QueryMatch {
    pub id: RecordId,
    pub value: f32,
    pub metadata: Json,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryResponse {
    pub metric: DistanceMetric,
    pub top_k: usize,
    pub returned: usize,
    pub snapshot: Snapshot,
    pub matches: Vec<QueryMatch>,
    pub diagnostics: Option<QueryDiagnostics>,
    pub snapshot_token: Option<String>,
}

/// `field == value` for a legacy equality filter.
pub fn equality(filter: &MetadataFilter) -> FilterExpr {
    let value = match &filter.value {
        ScalarMetadataValue::String(value) => Value::String(value.clone()),
        ScalarMetadataValue::Bool(value) => Value::Bool(*value),
        ScalarMetadataValue::Number(number) => number.as_i64().map_or_else(
            || Value::Float64(number.as_f64().unwrap_or_default()),
            Value::Int64,
        ),
        ScalarMetadataValue::Null => Value::Null,
    };
    FilterExpr::eq(filter.field.clone(), value)
}

impl QueryRequest {
    /// The collection key and the API request.
    pub fn split(self) -> (String, logpose_query::QueryRequest) {
        let mut filters = self.filters.iter().map(equality).collect::<Vec<_>>();
        filters.extend(self.predicate);
        let filter = match filters.len() {
            0 => None,
            1 => filters.pop(),
            _ => Some(FilterExpr::And(filters)),
        };
        (
            self.collection_name,
            logpose_query::QueryRequest {
                vector: Some(VectorQuery {
                    field: None,
                    values: self.vector,
                }),
                filter,
                top_k: self.top_k,
                output_fields: vec!["$extra".to_owned()],
                explain: self.explain,
                read: ReadConsistency {
                    snapshot: self.snapshot,
                    read_barrier: self.read_barrier,
                    snapshot_token: self.snapshot_token,
                    pin: self.pin,
                },
                ..logpose_query::QueryRequest::default()
            },
        )
    }
}

/// The legacy response of an API query result.
pub fn legacy_response(result: WithSchema<logpose_query::QueryResponse>) -> QueryResponse {
    let response = result.value;
    let matches = response
        .hits
        .into_iter()
        .map(|hit| QueryMatch {
            id: RecordId::new(hit.record.pk.label()),
            value: hit.score.unwrap_or_default(),
            metadata: Json::Object(hit.record.extra),
        })
        .collect::<Vec<_>>();
    QueryResponse {
        metric: response.metric.unwrap_or(DistanceMetric::Cosine),
        top_k: response.top_k,
        returned: matches.len(),
        snapshot: response.snapshot,
        matches,
        diagnostics: response.diagnostics,
        snapshot_token: response.snapshot_token,
    }
}

/// The legacy query on the service surfaces.
pub trait LegacyQuery {
    fn query(
        &self,
        request: QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, LogPoseError>>;
}

impl LegacyQuery for AppState {
    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, LogPoseError> {
        let (collection, request) = request.split();
        self.query_collection(&collection, request)
            .await
            .map(legacy_response)
    }
}

impl LegacyQuery for LogPoseDataService {
    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, LogPoseError> {
        let (collection, request) = request.split();
        self.query_collection(&collection, request)
            .await
            .map(legacy_response)
    }
}
