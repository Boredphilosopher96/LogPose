//! The single-vector query shape the storage harness compares with its model, served through
//! the API query (`logpose_query::query`) on collections of the legacy layout: key `id`, one
//! vector field, and metadata kept in `$extra`.

use logpose_query::{ExplainMode, QueryDiagnostics, QueryError, ReadConsistency, VectorQuery};
use logpose_storage::CollectionReader;
use logpose_types::{CollectionRef, DistanceMetric, RecordId, Snapshot};
use serde_json::Value;

/// A search of the collection's vector field, reading the current state or `snapshot`.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRequest {
    pub collection_name: String,
    pub vector: Vec<f32>,
    pub top_k: usize,
    pub snapshot: Option<Snapshot>,
    pub explain: ExplainMode,
}

/// One hit: its key, metric value, and metadata (`$extra`).
#[derive(Clone, Debug, PartialEq)]
pub struct QueryMatch {
    pub id: RecordId,
    pub value: f32,
    pub metadata: Value,
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

pub async fn query(
    reader: &dyn CollectionReader,
    request: QueryRequest,
) -> Result<QueryResponse, QueryError> {
    let collection = CollectionRef::parse(&request.collection_name)?;
    let response = logpose_query::query(
        reader,
        &collection,
        logpose_query::QueryRequest {
            vector: Some(VectorQuery {
                field: None,
                values: request.vector,
            }),
            top_k: request.top_k,
            output_fields: vec!["$extra".to_owned()],
            explain: request.explain,
            read: ReadConsistency {
                snapshot: request.snapshot,
                ..ReadConsistency::default()
            },
            ..logpose_query::QueryRequest::default()
        },
    )
    .await?
    .value;
    let matches = response
        .hits
        .into_iter()
        .map(|hit| QueryMatch {
            id: RecordId::new(hit.record.pk.label()),
            value: hit.score.unwrap_or_default(),
            metadata: Value::Object(hit.record.extra),
        })
        .collect::<Vec<_>>();
    Ok(QueryResponse {
        metric: response.metric.unwrap_or(DistanceMetric::Cosine),
        top_k: response.top_k,
        returned: matches.len(),
        snapshot: response.snapshot,
        matches,
        diagnostics: response.diagnostics,
        snapshot_token: response.snapshot_token,
    })
}
