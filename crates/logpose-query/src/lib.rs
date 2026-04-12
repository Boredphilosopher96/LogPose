//! Query planning abstractions.

#[cfg(test)]
use async_trait as _;
#[cfg(test)]
use logpose_catalog as _;
use logpose_storage::StorageEngine;
use logpose_types::{DistanceMetric, LogPoseError, RecordId, Snapshot, VisibleRecord};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use thiserror::Error;
#[cfg(test)]
use tokio as _;

/// Narrow request payload for a single-vector exact search.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    /// Target collection name.
    pub collection_name: String,
    /// Query embedding vector.
    pub vector: Vec<f32>,
    /// Maximum number of matches to return.
    pub top_k: usize,
    /// Optional caller-selected read snapshot.
    pub snapshot: Option<Snapshot>,
    /// Optional top-level metadata equality filters combined with AND semantics.
    #[serde(default)]
    pub filters: Vec<MetadataFilter>,
}

/// Top-level metadata equality filter for exact queries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataFilter {
    /// Top-level metadata field to match.
    pub field: String,
    /// Required scalar value for the field.
    pub value: ScalarMetadataValue,
}

/// Scalar metadata value supported by the first filtered-query slice.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScalarMetadataValue {
    /// Match a string metadata value.
    String(String),
    /// Match a numeric metadata value.
    Number(f64),
    /// Match a boolean metadata value.
    Bool(bool),
    /// Match a null metadata value.
    Null,
}

/// A single query match returned to callers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryMatch {
    /// External record identifier.
    pub id: RecordId,
    /// Raw metric value for the match.
    pub value: f32,
    /// Opaque user metadata carried through from storage.
    pub metadata: Value,
}

/// Response payload for a single-vector query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    /// Metric used to rank results.
    pub metric: DistanceMetric,
    /// Requested top-k limit.
    pub top_k: usize,
    /// Number of matches actually returned.
    pub returned: usize,
    /// Effective snapshot used for the read.
    pub snapshot: Snapshot,
    /// Ranked matches.
    pub matches: Vec<QueryMatch>,
}

/// Query-scoped error returned when a request cannot be ranked.
#[derive(Debug, Error)]
pub enum QueryError {
    /// Query vector dimensionality must match the collection descriptor.
    #[error("query vector dimension mismatch: expected {expected}, found {actual}")]
    RequestVectorDimensionMismatch {
        /// Expected collection dimensionality.
        expected: usize,
        /// Actual query dimensionality.
        actual: usize,
    },
    /// Query and candidate vectors must have matching dimensions.
    #[error("vector dimension mismatch: expected {expected}, found {actual}")]
    VectorDimensionMismatch {
        /// Expected query dimensionality.
        expected: usize,
        /// Actual candidate dimensionality.
        actual: usize,
    },
    /// Stored vectors that do not match the collection descriptor are rejected.
    #[error(
        "stored vector dimension mismatch for record '{record_id}': expected {expected}, found {actual}"
    )]
    StoredVectorDimensionMismatch {
        /// Identifier for the malformed record.
        record_id: RecordId,
        /// Expected collection dimensionality.
        expected: usize,
        /// Actual stored dimensionality.
        actual: usize,
    },
    /// Storage failures are surfaced directly from the read path.
    #[error(transparent)]
    Storage(#[from] LogPoseError),
}

/// Result type for query helpers.
pub type Result<T> = std::result::Result<T, QueryError>;

/// Execute a storage-backed exact query for a single vector.
pub async fn query_exact<S>(storage: &S, request: QueryRequest) -> Result<QueryResponse>
where
    S: StorageEngine + ?Sized,
{
    let descriptor = storage.open_collection(&request.collection_name).await?;
    if request.vector.len() != descriptor.dimensions {
        return Err(QueryError::RequestVectorDimensionMismatch {
            expected: descriptor.dimensions,
            actual: request.vector.len(),
        });
    }

    let snapshot = match request.snapshot {
        Some(snapshot) => snapshot,
        None => storage.snapshot(&request.collection_name).await?,
    };
    let records = storage
        .scan_exact(&request.collection_name, Some(snapshot.clone()))
        .await?;
    let filtered = filter_records(records, &request.filters);

    let matches = rank_matches_with(
        descriptor.metric,
        &request.vector,
        filtered,
        request.top_k,
        |record, error| match error {
            QueryError::VectorDimensionMismatch { expected, actual } => {
                QueryError::StoredVectorDimensionMismatch {
                    record_id: record.id.clone(),
                    expected,
                    actual,
                }
            }
            other => other,
        },
    )?;

    Ok(build_query_response(
        descriptor.metric,
        request.top_k,
        snapshot,
        matches,
    ))
}

/// Filter visible records using top-level metadata equality semantics.
pub fn filter_records<I>(records: I, filters: &[MetadataFilter]) -> Vec<VisibleRecord>
where
    I: IntoIterator<Item = VisibleRecord>,
{
    records
        .into_iter()
        .filter(|record| record_matches_filters(record, filters))
        .collect()
}

/// Compute the raw metric value for two vectors.
pub fn metric_value(metric: DistanceMetric, query: &[f32], candidate: &[f32]) -> Result<f32> {
    ensure_dimensions(query, candidate)?;

    Ok(match metric {
        DistanceMetric::Dot => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| lhs * rhs)
            .sum(),
        DistanceMetric::Cosine => {
            let dot: f32 = query
                .iter()
                .zip(candidate)
                .map(|(lhs, rhs)| lhs * rhs)
                .sum();
            let query_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
            let candidate_norm = candidate
                .iter()
                .map(|value| value * value)
                .sum::<f32>()
                .sqrt();

            if query_norm == 0.0 || candidate_norm == 0.0 {
                0.0
            } else {
                dot / (query_norm * candidate_norm)
            }
        }
        DistanceMetric::L2 => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| {
                let delta = lhs - rhs;
                delta * delta
            })
            .sum::<f32>()
            .sqrt(),
    })
}

/// Rank visible records using the shared exact-search semantics.
pub fn rank_matches<I>(
    metric: DistanceMetric,
    query: &[f32],
    records: I,
    top_k: usize,
) -> Result<Vec<QueryMatch>>
where
    I: IntoIterator<Item = VisibleRecord>,
{
    rank_matches_with(metric, query, records, top_k, |_record, error| error)
}

/// Build a query response from ranked matches and an effective snapshot.
#[must_use]
pub fn build_query_response(
    metric: DistanceMetric,
    top_k: usize,
    snapshot: Snapshot,
    matches: Vec<QueryMatch>,
) -> QueryResponse {
    let returned = matches.len();

    QueryResponse {
        metric,
        top_k,
        returned,
        snapshot,
        matches,
    }
}

fn ensure_dimensions(query: &[f32], candidate: &[f32]) -> Result<()> {
    if query.len() == candidate.len() {
        Ok(())
    } else {
        Err(QueryError::VectorDimensionMismatch {
            expected: query.len(),
            actual: candidate.len(),
        })
    }
}

fn compare_matches(metric: DistanceMetric, left: &QueryMatch, right: &QueryMatch) -> Ordering {
    let value_order = match metric {
        DistanceMetric::Cosine | DistanceMetric::Dot => right.value.total_cmp(&left.value),
        DistanceMetric::L2 => left.value.total_cmp(&right.value),
    };

    value_order.then_with(|| left.id.cmp(&right.id))
}

fn record_matches_filters(record: &VisibleRecord, filters: &[MetadataFilter]) -> bool {
    filters
        .iter()
        .all(|filter| filter_matches_metadata(&record.metadata, filter))
}

fn filter_matches_metadata(metadata: &Value, filter: &MetadataFilter) -> bool {
    match metadata {
        Value::Object(fields) => fields
            .get(&filter.field)
            .is_some_and(|value| scalar_value_matches_json(&filter.value, value)),
        _ => false,
    }
}

fn scalar_value_matches_json(expected: &ScalarMetadataValue, actual: &Value) -> bool {
    match (expected, actual) {
        (ScalarMetadataValue::String(expected), Value::String(actual)) => expected == actual,
        (ScalarMetadataValue::Number(expected), Value::Number(actual)) => {
            actual.as_f64().is_some_and(|actual| actual == *expected)
        }
        (ScalarMetadataValue::Bool(expected), Value::Bool(actual)) => expected == actual,
        (ScalarMetadataValue::Null, Value::Null) => true,
        _ => false,
    }
}

fn rank_matches_with<I, F>(
    metric: DistanceMetric,
    query: &[f32],
    records: I,
    top_k: usize,
    map_error: F,
) -> Result<Vec<QueryMatch>>
where
    I: IntoIterator<Item = VisibleRecord>,
    F: Fn(&VisibleRecord, QueryError) -> QueryError,
{
    let mut matches = Vec::new();

    for record in records {
        let value = metric_value(metric, query, &record.vector)
            .map_err(|error| map_error(&record, error))?;
        matches.push(QueryMatch {
            value,
            id: record.id,
            metadata: record.metadata,
        });
    }

    matches.sort_by(|left, right| compare_matches(metric, left, right));
    matches.truncate(top_k);
    Ok(matches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use logpose_catalog::CollectionDescriptor;
    use logpose_storage::{CreateCollectionRequest, InspectReport, InspectTarget, StorageEngine};
    use logpose_types::{CollectionStats, CommitAck, LogPoseError, WriteOperation};
    use serde_json::json;
    use std::path::Path;

    fn record(id: &str, vector: Vec<f32>) -> VisibleRecord {
        VisibleRecord {
            id: RecordId::from(id),
            vector,
            metadata: json!({ "id": id }),
            seq_no: 1,
        }
    }

    #[test]
    fn computes_raw_metric_values_for_supported_metrics() {
        let query = vec![1.0, 2.0];
        let candidate = vec![3.0, 4.0];

        let cosine = metric_value(DistanceMetric::Cosine, &query, &candidate);
        let dot = metric_value(DistanceMetric::Dot, &query, &candidate);
        let l2 = metric_value(DistanceMetric::L2, &query, &candidate);

        assert!(matches!(cosine, Ok(value) if (value - 0.983_869_9).abs() < 1e-6));
        assert!(matches!(dot, Ok(value) if (value - 11.0).abs() < 1e-6));
        assert!(matches!(l2, Ok(value) if (value - 2.828_427).abs() < 1e-6));
    }

    #[test]
    fn rejects_mismatched_vector_dimensions() {
        let query = vec![1.0, 2.0];
        let candidate = vec![3.0];

        let value = metric_value(DistanceMetric::Dot, &query, &candidate);

        assert!(matches!(
            value,
            Err(QueryError::VectorDimensionMismatch {
                expected: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn orders_results_by_metric_and_breaks_ties_by_record_id() {
        let request = QueryRequest {
            collection_name: "alpha".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 3,
            snapshot: Some(Snapshot {
                manifest_generation: 7,
                visible_seq_no: 11,
            }),
            filters: Vec::new(),
        };

        let matches = rank_matches(
            DistanceMetric::Dot,
            &request.vector,
            vec![
                record("b", vec![1.0, 0.0]),
                record("a", vec![1.0, 0.0]),
                record("c", vec![0.25, 0.0]),
            ],
            request.top_k,
        );

        let Ok(matches) = matches else {
            unreachable!("unexpected error in dot-ordering test")
        };

        assert_eq!(
            matches
                .iter()
                .map(|match_| match_.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert!((matches[0].value - 1.0).abs() < 1e-6);
        assert!((matches[2].value - 0.25).abs() < 1e-6);

        let l2_matches = rank_matches(
            DistanceMetric::L2,
            &[0.0, 0.0],
            vec![
                record("b", vec![0.0, 0.0]),
                record("a", vec![1.0, 0.0]),
                record("c", vec![2.0, 0.0]),
            ],
            request.top_k,
        );

        let Ok(l2_matches) = l2_matches else {
            unreachable!("unexpected error in l2-ordering test")
        };

        assert_eq!(
            l2_matches
                .iter()
                .map(|match_| match_.id.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "a", "c"]
        );
        assert!((l2_matches[0].value - 0.0).abs() < 1e-6);
        assert!((l2_matches[2].value - 2.0).abs() < 1e-6);
    }

    #[test]
    fn truncates_to_top_k_and_preserves_empty_results() {
        let snapshot = Snapshot {
            manifest_generation: 9,
            visible_seq_no: 22,
        };

        let request = QueryRequest {
            collection_name: "alpha".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: Some(snapshot.clone()),
            filters: Vec::new(),
        };

        let truncated = rank_matches(
            DistanceMetric::Dot,
            &request.vector,
            vec![
                record("a", vec![3.0, 0.0]),
                record("b", vec![2.0, 0.0]),
                record("c", vec![1.0, 0.0]),
            ],
            request.top_k,
        );

        let Ok(truncated_matches) = truncated else {
            unreachable!("unexpected error in truncation test")
        };
        let truncated = build_query_response(
            DistanceMetric::Dot,
            request.top_k,
            snapshot.clone(),
            truncated_matches,
        );

        assert_eq!(truncated.top_k, 2);
        assert_eq!(truncated.returned, 2);
        assert_eq!(truncated.snapshot, snapshot);
        assert_eq!(
            truncated
                .matches
                .iter()
                .map(|match_| match_.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        let empty_request = QueryRequest {
            collection_name: "alpha".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 4,
            snapshot: None,
            filters: Vec::new(),
        };

        let empty = rank_matches(
            DistanceMetric::L2,
            &empty_request.vector,
            Vec::new(),
            empty_request.top_k,
        );

        let Ok(empty_matches) = empty else {
            unreachable!("unexpected error in empty-results test")
        };
        let empty = build_query_response(
            DistanceMetric::L2,
            empty_request.top_k,
            snapshot.clone(),
            empty_matches,
        );

        assert_eq!(empty.top_k, 4);
        assert_eq!(empty.returned, 0);
        assert_eq!(empty.snapshot, snapshot);
        assert_eq!(empty.matches, Vec::<QueryMatch>::new());
    }

    #[test]
    fn rejects_mismatched_dimensions_during_ranking() {
        let result = rank_matches(
            DistanceMetric::Cosine,
            &[1.0, 2.0],
            vec![record("a", vec![3.0])],
            1,
        );

        assert!(matches!(
            result,
            Err(QueryError::VectorDimensionMismatch {
                expected: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn filters_records_by_top_level_scalar_equality_before_ranking() {
        let matches = rank_matches(
            DistanceMetric::Dot,
            &[1.0, 0.0],
            filter_records(
                vec![
                    VisibleRecord {
                        id: RecordId::from("matching"),
                        vector: vec![2.0, 0.0],
                        metadata: json!({
                            "color": "red",
                            "active": true,
                            "score": 7,
                            "missing": null,
                        }),
                        seq_no: 1,
                    },
                    VisibleRecord {
                        id: RecordId::from("wrong-color"),
                        vector: vec![9.0, 0.0],
                        metadata: json!({
                            "color": "blue",
                            "active": true,
                            "score": 7,
                            "missing": null,
                        }),
                        seq_no: 2,
                    },
                    VisibleRecord {
                        id: RecordId::from("nested"),
                        vector: vec![8.0, 0.0],
                        metadata: json!({
                            "color": { "name": "red" },
                            "active": true,
                            "score": 7,
                            "missing": null,
                        }),
                        seq_no: 3,
                    },
                ],
                &[
                    MetadataFilter {
                        field: "color".to_owned(),
                        value: ScalarMetadataValue::String("red".to_owned()),
                    },
                    MetadataFilter {
                        field: "active".to_owned(),
                        value: ScalarMetadataValue::Bool(true),
                    },
                    MetadataFilter {
                        field: "score".to_owned(),
                        value: ScalarMetadataValue::Number(7.0),
                    },
                    MetadataFilter {
                        field: "missing".to_owned(),
                        value: ScalarMetadataValue::Null,
                    },
                ],
            ),
            3,
        )
        .expect("ranking should succeed");

        assert_eq!(
            matches
                .iter()
                .map(|match_| match_.id.as_str())
                .collect::<Vec<_>>(),
            vec!["matching"]
        );
    }

    #[tokio::test]
    async fn preserves_storage_errors_without_string_flattening() {
        let result = query_exact(
            &MissingCollectionStorage,
            QueryRequest {
                collection_name: "missing".to_owned(),
                vector: vec![1.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(QueryError::Storage(LogPoseError::Message(message)))
                if message.contains("does not exist")
        ));
    }

    #[tokio::test]
    async fn remaps_stored_dimension_mismatch_during_storage_queries() {
        let result = query_exact(
            &MalformedStorageEngine,
            QueryRequest {
                collection_name: "broken".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(QueryError::StoredVectorDimensionMismatch {
                record_id,
                expected: 2,
                actual: 1
            }) if record_id.as_str() == "bad-record"
        ));
    }

    struct MissingCollectionStorage;

    #[async_trait]
    impl StorageEngine for MissingCollectionStorage {
        async fn engine_name(&self) -> &'static str {
            "missing"
        }

        async fn create_collection(
            &self,
            _request: CreateCollectionRequest,
        ) -> logpose_types::Result<CollectionDescriptor> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn open_collection(
            &self,
            _name: &str,
        ) -> logpose_types::Result<CollectionDescriptor> {
            Err(LogPoseError::Message(
                "collection 'missing' does not exist".to_owned(),
            ))
        }

        async fn write(
            &self,
            _collection_name: &str,
            _operations: Vec<WriteOperation>,
        ) -> logpose_types::Result<CommitAck> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn snapshot(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn scan_exact(
            &self,
            _collection_name: &str,
            _snapshot: Option<Snapshot>,
        ) -> logpose_types::Result<Vec<VisibleRecord>> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn flush(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn compact(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn stats(&self, _collection_name: &str) -> logpose_types::Result<CollectionStats> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn inspect(
            &self,
            _collection_name: &str,
            _target: InspectTarget,
        ) -> logpose_types::Result<InspectReport> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }
    }

    #[tokio::test]
    async fn query_exact_applies_filters_and_preserves_snapshot() {
        let result = query_exact(
            &FilteredStorageEngine,
            QueryRequest {
                collection_name: "filtered".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 2,
                snapshot: Some(Snapshot {
                    manifest_generation: 3,
                    visible_seq_no: 8,
                }),
                filters: vec![MetadataFilter {
                    field: "kind".to_owned(),
                    value: ScalarMetadataValue::String("keep".to_owned()),
                }],
            },
        )
        .await
        .expect("query should succeed");

        assert_eq!(result.snapshot.manifest_generation, 3);
        assert_eq!(
            result
                .matches
                .iter()
                .map(|match_| match_.id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "gamma"]
        );
    }

    struct MalformedStorageEngine;

    #[async_trait]
    impl StorageEngine for MalformedStorageEngine {
        async fn engine_name(&self) -> &'static str {
            "malformed"
        }

        async fn create_collection(
            &self,
            _request: CreateCollectionRequest,
        ) -> logpose_types::Result<CollectionDescriptor> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn open_collection(&self, name: &str) -> logpose_types::Result<CollectionDescriptor> {
            Ok(CollectionDescriptor::new(
                name,
                2,
                DistanceMetric::Dot,
                Path::new("/tmp"),
            ))
        }

        async fn write(
            &self,
            _collection_name: &str,
            _operations: Vec<WriteOperation>,
        ) -> logpose_types::Result<CommitAck> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn snapshot(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Ok(Snapshot {
                manifest_generation: 4,
                visible_seq_no: 9,
            })
        }

        async fn scan_exact(
            &self,
            _collection_name: &str,
            _snapshot: Option<Snapshot>,
        ) -> logpose_types::Result<Vec<VisibleRecord>> {
            Ok(vec![VisibleRecord {
                id: RecordId::new("bad-record"),
                vector: vec![1.0],
                metadata: json!(null),
                seq_no: 9,
            }])
        }

        async fn flush(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn compact(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn stats(&self, _collection_name: &str) -> logpose_types::Result<CollectionStats> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn inspect(
            &self,
            _collection_name: &str,
            _target: InspectTarget,
        ) -> logpose_types::Result<InspectReport> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }
    }

    struct FilteredStorageEngine;

    #[async_trait]
    impl StorageEngine for FilteredStorageEngine {
        async fn engine_name(&self) -> &'static str {
            "filtered"
        }

        async fn create_collection(
            &self,
            _request: CreateCollectionRequest,
        ) -> logpose_types::Result<CollectionDescriptor> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn open_collection(&self, name: &str) -> logpose_types::Result<CollectionDescriptor> {
            Ok(CollectionDescriptor::new(
                name,
                2,
                DistanceMetric::Dot,
                Path::new("/tmp"),
            ))
        }

        async fn write(
            &self,
            _collection_name: &str,
            _operations: Vec<WriteOperation>,
        ) -> logpose_types::Result<CommitAck> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn snapshot(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Ok(Snapshot {
                manifest_generation: 99,
                visible_seq_no: 101,
            })
        }

        async fn scan_exact(
            &self,
            _collection_name: &str,
            _snapshot: Option<Snapshot>,
        ) -> logpose_types::Result<Vec<VisibleRecord>> {
            Ok(vec![
                VisibleRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![3.0, 0.0],
                    metadata: json!({ "kind": "keep" }),
                    seq_no: 4,
                },
                VisibleRecord {
                    id: RecordId::new("beta"),
                    vector: vec![9.0, 0.0],
                    metadata: json!({ "kind": "drop" }),
                    seq_no: 5,
                },
                VisibleRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({ "kind": "keep" }),
                    seq_no: 6,
                },
            ])
        }

        async fn flush(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn compact(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn stats(&self, _collection_name: &str) -> logpose_types::Result<CollectionStats> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn inspect(
            &self,
            _collection_name: &str,
            _target: InspectTarget,
        ) -> logpose_types::Result<InspectReport> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }
    }
}
