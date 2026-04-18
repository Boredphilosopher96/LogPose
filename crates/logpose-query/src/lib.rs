//! Query planning abstractions.

#[cfg(test)]
use async_trait as _;
#[cfg(test)]
use criterion as _;
use logpose_catalog as _;
use logpose_storage::StorageEngine;
pub use logpose_types::ScalarMetadataValue;
use logpose_types::{
    AnnSearchRequest, CollectionRef, CollectionStats, DistanceMetric, LogPoseError, QueryUnitStats,
    RecordId, ScalarFieldStats, Snapshot, VisibleRecord,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::Number;
use serde_json::Value;
use std::{cmp::Ordering, collections::BTreeMap, sync::Arc, time::Instant};
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
    /// Optional structured predicate tree over top-level scalar metadata.
    #[serde(default)]
    pub predicate: Option<Predicate>,
    /// Optional explain/profile mode for planner diagnostics.
    #[serde(default)]
    pub explain: ExplainMode,
}

/// Top-level metadata equality filter for exact queries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataFilter {
    /// Top-level metadata field to match.
    pub field: String,
    /// Required scalar value for the field.
    pub value: ScalarMetadataValue,
}

/// Structured predicate tree used for planner-aware metadata filtering.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Predicate {
    /// Conjunction over child predicates.
    And {
        /// Child predicates.
        children: Vec<Predicate>,
    },
    /// Disjunction over child predicates.
    Or {
        /// Child predicates.
        children: Vec<Predicate>,
    },
    /// Negation over a child predicate.
    Not {
        /// Child predicate.
        child: Box<Predicate>,
    },
    /// Comparison over a top-level scalar field.
    Comparison(PredicateComparison),
}

/// Single field comparison inside a predicate tree.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredicateComparison {
    /// Target top-level field.
    pub field: String,
    /// Comparison operator.
    pub operator: PredicateOperator,
    /// Optional scalar value for operators that need one.
    #[serde(default)]
    pub value: Option<ScalarMetadataValue>,
}

/// Operator used by a predicate comparison.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOperator {
    /// Exact scalar equality.
    Eq,
    /// Scalar inequality.
    Ne,
    /// Strictly less-than comparison.
    Lt,
    /// Less-than-or-equal comparison.
    Lte,
    /// Strictly greater-than comparison.
    Gt,
    /// Greater-than-or-equal comparison.
    Gte,
    /// Field existence check.
    Exists,
    /// Explicit null check.
    IsNull,
}

/// Diagnostics verbosity requested by the caller.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainMode {
    /// Do not emit plan diagnostics.
    #[default]
    None,
    /// Emit chosen plan and planner estimates.
    Plan,
    /// Emit chosen plan plus per-stage timings.
    Profile,
}

/// Planner-selected physical plan for an exact, ANN, or hybrid query.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryPlanKind {
    /// Scan all visible records without predicate pruning.
    UnfilteredExactScan,
    /// Apply predicates before ranking candidate vectors.
    PredicateFirstExact,
    /// Rank all candidate vectors before applying the predicate.
    VectorFirstExact,
    /// Use a small exact fallback when the predicate is highly selective.
    TinyPopulationExactFallback,
    /// Use ANN candidate generation over immutable units without exact mutable merge.
    VectorFirstAnn,
    /// Use ANN with predicate-aware candidate rejection before final rerank.
    CooperativeFilteredAnn,
    /// Merge exact mutable candidates with immutable ANN candidates before rerank.
    HybridExactAnnMerge,
}

/// Optional per-stage timings reported for profile mode.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryStageTimings {
    /// Time spent planning the query.
    pub planning_micros: u64,
    /// Time spent doing exact prefilter work before candidate generation.
    pub prefilter_micros: u64,
    /// Time spent generating ANN or exact candidates.
    pub candidate_generation_micros: u64,
    /// Time spent applying postfilters after candidate generation.
    pub postfilter_micros: u64,
    /// Time spent reranking exact vectors for final ordering.
    pub rerank_micros: u64,
    /// Time spent merging mutable and immutable candidate sets.
    pub merge_micros: u64,
}

/// Planner and execution diagnostics surfaced to operators.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryDiagnostics {
    /// Physical plan selected for execution.
    pub chosen_plan: QueryPlanKind,
    /// Short human-readable reason for the plan choice.
    pub planner_reason: String,
    /// Estimated predicate selectivity in the visible snapshot.
    pub estimated_selectivity: f32,
    /// Total mutable plus immutable units considered by the planner.
    pub units_considered: usize,
    /// Units proven impossible and pruned before scan.
    pub units_pruned: usize,
    /// Units actually scanned for this query.
    pub units_scanned: usize,
    /// Candidate count before predicate application.
    pub candidates_before_filter: usize,
    /// Candidate count after predicate application.
    pub candidates_after_filter: usize,
    /// Candidate count entering the final rerank step.
    pub candidates_reranked: usize,
    /// Candidate count after mutable plus immutable merge.
    pub candidates_merged: usize,
    /// Number of rerank passes used by the plan.
    pub rerank_count: usize,
    /// Optional reason the planner fell back to an exact path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
    /// Count of units scanned by execution role.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub unit_scan_mix: BTreeMap<String, usize>,
    /// Optional stage timings when profile mode is requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_timings: Option<QueryStageTimings>,
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
    /// Optional planner and execution diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<QueryDiagnostics>,
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
    /// Predicate structure is malformed for the requested operators.
    #[error("{0}")]
    InvalidPredicate(String),
    /// Storage failures are surfaced directly from the read path.
    #[error(transparent)]
    Storage(#[from] LogPoseError),
}

/// Result type for query helpers.
pub type Result<T> = std::result::Result<T, QueryError>;

struct ResolvedCollectionDescriptor {
    database_name: String,
    collection_name: String,
    dimensions: usize,
    metric: DistanceMetric,
}

impl ResolvedCollectionDescriptor {
    fn lookup_name(&self) -> String {
        CollectionRef::new(self.database_name.clone(), self.collection_name.clone()).lookup_name()
    }
}

/// Execute a storage-backed exact query for a single vector.
pub async fn query_exact<S>(storage: &S, request: QueryRequest) -> Result<QueryResponse>
where
    S: StorageEngine + ?Sized,
{
    let planning_started = Instant::now();
    let descriptor = resolve_collection_descriptor(storage, &request.collection_name).await?;
    let collection_name = descriptor.lookup_name();
    if request.vector.len() != descriptor.dimensions {
        return Err(QueryError::RequestVectorDimensionMismatch {
            expected: descriptor.dimensions,
            actual: request.vector.len(),
        });
    }

    let predicate = combined_predicate(&request);
    if let Some(predicate) = predicate.as_ref() {
        validate_predicate(predicate)?;
    }
    let snapshot = match request.snapshot.clone() {
        Some(snapshot) => snapshot,
        None => storage.snapshot(&collection_name).await?,
    };
    let stats = storage
        .stats_snapshot(&collection_name, Some(snapshot.clone()))
        .await?;
    let unit_selection = select_query_units(&stats, predicate.as_ref());
    let estimated_selectivity = predicate
        .as_ref()
        .map(|predicate| estimate_selectivity(predicate, &stats))
        .unwrap_or(1.0);
    let plan = choose_plan(
        predicate.as_ref(),
        estimated_selectivity,
        request.top_k,
        unit_selection.scanned_put_count,
        unit_selection.has_full_ann_coverage(),
        unit_selection.include_mutable,
    );
    let planning_micros = planning_started.elapsed().as_micros() as u64;
    let filter_hook = predicate.as_ref().map(|predicate| {
        let predicate = Arc::new(predicate.clone());
        Arc::new(move |metadata: &Value| predicate_matches_metadata(metadata, predicate.as_ref()))
            as Arc<dyn for<'a> Fn(&'a Value) -> bool + Send + Sync>
    });

    let (matches, measurements) = match plan {
        QueryPlanKind::UnfilteredExactScan
        | QueryPlanKind::PredicateFirstExact
        | QueryPlanKind::VectorFirstExact
        | QueryPlanKind::TinyPopulationExactFallback => {
            let records = storage
                .scan_exact_selected(
                    &collection_name,
                    Some(snapshot.clone()),
                    unit_selection.include_mutable,
                    unit_selection.exact_immutable_unit_ids.clone(),
                )
                .await?;
            let candidates_before_filter = records.len();

            match plan {
                QueryPlanKind::UnfilteredExactScan => {
                    let ranking_started = Instant::now();
                    let matches = rank_matches_with(
                        descriptor.metric,
                        &request.vector,
                        records,
                        request.top_k,
                        stored_dimension_error,
                    )?;
                    (
                        matches,
                        DiagnosticMeasurements {
                            candidates_before_filter,
                            candidates_after_filter: candidates_before_filter,
                            candidates_reranked: candidates_before_filter,
                            candidates_merged: candidates_before_filter,
                            planning_micros,
                            candidate_generation_micros: ranking_started.elapsed().as_micros()
                                as u64,
                            ..DiagnosticMeasurements::default()
                        },
                    )
                }
                QueryPlanKind::PredicateFirstExact | QueryPlanKind::TinyPopulationExactFallback => {
                    let prefilter_started = Instant::now();
                    let filtered = filter_records_by_predicate(records, predicate.as_ref());
                    let prefilter_micros = prefilter_started.elapsed().as_micros() as u64;
                    let candidates_after_filter = filtered.len();

                    let rerank_started = Instant::now();
                    let matches = rank_matches_with(
                        descriptor.metric,
                        &request.vector,
                        filtered,
                        request.top_k,
                        stored_dimension_error,
                    )?;
                    (
                        matches,
                        DiagnosticMeasurements {
                            candidates_before_filter,
                            candidates_after_filter,
                            candidates_reranked: candidates_after_filter,
                            candidates_merged: candidates_after_filter,
                            planning_micros,
                            prefilter_micros,
                            rerank_micros: rerank_started.elapsed().as_micros() as u64,
                            ..DiagnosticMeasurements::default()
                        },
                    )
                }
                QueryPlanKind::VectorFirstExact => {
                    let candidate_generation_started = Instant::now();
                    let ranked = rank_matches_with(
                        descriptor.metric,
                        &request.vector,
                        records,
                        candidates_before_filter,
                        stored_dimension_error,
                    )?;
                    let candidate_generation_micros =
                        candidate_generation_started.elapsed().as_micros() as u64;

                    let postfilter_started = Instant::now();
                    let mut filtered = ranked
                        .into_iter()
                        .filter(|candidate| {
                            predicate.as_ref().is_none_or(|predicate| {
                                predicate_matches_metadata(&candidate.metadata, predicate)
                            })
                        })
                        .collect::<Vec<_>>();
                    let postfilter_micros = postfilter_started.elapsed().as_micros() as u64;
                    let candidates_after_filter = filtered.len();
                    filtered.truncate(request.top_k);
                    (
                        filtered,
                        DiagnosticMeasurements {
                            candidates_before_filter,
                            candidates_after_filter,
                            candidates_reranked: candidates_before_filter,
                            candidates_merged: candidates_after_filter,
                            planning_micros,
                            candidate_generation_micros,
                            postfilter_micros,
                            ..DiagnosticMeasurements::default()
                        },
                    )
                }
                QueryPlanKind::VectorFirstAnn
                | QueryPlanKind::CooperativeFilteredAnn
                | QueryPlanKind::HybridExactAnnMerge => unreachable!("exact branch gated by plan"),
            }
        }
        QueryPlanKind::VectorFirstAnn
        | QueryPlanKind::CooperativeFilteredAnn
        | QueryPlanKind::HybridExactAnnMerge => {
            let request_budget = ann_candidate_budget(request.top_k, estimated_selectivity);
            let candidate_generation_started = Instant::now();
            let ann_candidates = storage
                .ann_search_selected(
                    &collection_name,
                    Some(snapshot.clone()),
                    unit_selection.ann_immutable_unit_ids.clone(),
                    AnnSearchRequest {
                        vector: request.vector.clone(),
                        top_k: request.top_k,
                        candidate_budget: request_budget,
                    },
                    filter_hook.clone(),
                )
                .await?;
            let candidate_generation_micros =
                candidate_generation_started.elapsed().as_micros() as u64;

            let postfilter_started = Instant::now();
            let ann_records = if ann_candidates.is_empty() {
                Vec::new()
            } else {
                storage
                    .latest_visible_selected(
                        &collection_name,
                        Some(snapshot.clone()),
                        ann_candidates
                            .iter()
                            .map(|candidate| candidate.record_id.clone())
                            .collect(),
                        true,
                        unit_selection.exact_immutable_unit_ids.clone(),
                    )
                    .await?
            };
            let ann_filtered = filter_records_by_predicate(ann_records, predicate.as_ref());
            let postfilter_micros = postfilter_started.elapsed().as_micros() as u64;

            let mutable_exact = if matches!(plan, QueryPlanKind::HybridExactAnnMerge)
                && unit_selection.include_mutable
            {
                let prefilter_started = Instant::now();
                let mutable_records = storage
                    .scan_exact_selected(&collection_name, Some(snapshot.clone()), true, Vec::new())
                    .await?;
                let filtered = filter_records_by_predicate(mutable_records, predicate.as_ref());
                let prefilter_micros = prefilter_started.elapsed().as_micros() as u64;
                (filtered, prefilter_micros)
            } else {
                (Vec::new(), 0)
            };

            let merge_started = Instant::now();
            let merged_records = dedupe_latest_records(
                mutable_exact
                    .0
                    .iter()
                    .cloned()
                    .chain(ann_filtered.iter().cloned())
                    .collect(),
            );
            let merge_micros = merge_started.elapsed().as_micros() as u64;

            let rerank_started = Instant::now();
            let matches = rank_matches_with(
                descriptor.metric,
                &request.vector,
                merged_records.clone(),
                request.top_k,
                stored_dimension_error,
            )?;
            let rerank_micros = rerank_started.elapsed().as_micros() as u64;
            let candidates_after_filter = ann_filtered.len() + mutable_exact.0.len();
            (
                matches,
                DiagnosticMeasurements {
                    candidates_before_filter: ann_candidates.len() + mutable_exact.0.len(),
                    candidates_after_filter,
                    candidates_reranked: merged_records.len(),
                    candidates_merged: merged_records.len(),
                    planning_micros,
                    prefilter_micros: mutable_exact.1,
                    candidate_generation_micros,
                    postfilter_micros,
                    rerank_micros,
                    merge_micros,
                    rerank_count: 1,
                },
            )
        }
    };
    let diagnostics = build_diagnostics(
        &request,
        plan,
        estimated_selectivity,
        &unit_selection,
        measurements,
    );

    Ok(build_query_response_with_diagnostics(
        descriptor.metric,
        request.top_k,
        snapshot,
        matches,
        diagnostics,
    ))
}

async fn resolve_collection_descriptor<S>(
    storage: &S,
    collection_name: &str,
) -> Result<ResolvedCollectionDescriptor>
where
    S: StorageEngine + ?Sized,
{
    let reference = parse_collection_reference(collection_name)?;
    let descriptor = storage
        .open_collection(collection_name)
        .await
        .map_err(|error| qualify_collection_error(error, collection_name))?;
    let resolved = ResolvedCollectionDescriptor {
        database_name: descriptor.database_name,
        collection_name: descriptor.name,
        dimensions: descriptor.dimensions,
        metric: descriptor.metric,
    };
    ensure_collection_reference_matches_descriptor(&reference, &resolved, collection_name)?;
    Ok(resolved)
}

fn parse_collection_reference(collection_name: &str) -> Result<CollectionRef> {
    let reference = match collection_name
        .trim()
        .split('/')
        .collect::<Vec<_>>()
        .as_slice()
    {
        [collection_name] => CollectionRef::new_default(*collection_name),
        [database_name, collection_name] => CollectionRef::new(*database_name, *collection_name),
        _ => {
            return Err(QueryError::Storage(LogPoseError::Message(format!(
                "unsupported collection reference '{collection_name}': expected 'collection' or 'database/collection'"
            ))));
        }
    };
    reference.validate().map_err(QueryError::Storage)?;
    Ok(reference)
}

fn ensure_collection_reference_matches_descriptor(
    reference: &CollectionRef,
    descriptor: &ResolvedCollectionDescriptor,
    original_name: &str,
) -> Result<()> {
    if reference.database_name != descriptor.database_name
        || reference.collection_name != descriptor.collection_name
    {
        return Err(QueryError::Storage(LogPoseError::Message(format!(
            "collection '{original_name}' does not exist"
        ))));
    }
    Ok(())
}

fn qualify_collection_error(error: LogPoseError, collection_name: &str) -> LogPoseError {
    match error {
        LogPoseError::Message(message) if message.contains("does not exist") => {
            LogPoseError::Message(format!("collection '{collection_name}' does not exist"))
        }
        other => other,
    }
}

fn ann_candidate_budget(top_k: usize, estimated_selectivity: f32) -> usize {
    let multiplier = if estimated_selectivity <= 0.2 {
        10
    } else if estimated_selectivity <= 0.45 {
        8
    } else {
        6
    };
    top_k.max(1).saturating_mul(multiplier).max(16)
}

fn stored_dimension_error(record: &VisibleRecord, error: QueryError) -> QueryError {
    match error {
        QueryError::VectorDimensionMismatch { expected, actual } => {
            QueryError::StoredVectorDimensionMismatch {
                record_id: record.id.clone(),
                expected,
                actual,
            }
        }
        other => other,
    }
}

fn dedupe_latest_records(records: Vec<VisibleRecord>) -> Vec<VisibleRecord> {
    let mut deduped = BTreeMap::<RecordId, VisibleRecord>::new();
    for record in records {
        let should_replace = deduped
            .get(&record.id)
            .is_none_or(|current| record.seq_no >= current.seq_no);
        if should_replace {
            deduped.insert(record.id.clone(), record);
        }
    }
    deduped.into_values().collect()
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
        diagnostics: None,
    }
}

/// Build a query response with optional diagnostics.
#[must_use]
pub fn build_query_response_with_diagnostics(
    metric: DistanceMetric,
    top_k: usize,
    snapshot: Snapshot,
    matches: Vec<QueryMatch>,
    diagnostics: Option<QueryDiagnostics>,
) -> QueryResponse {
    let mut response = build_query_response(metric, top_k, snapshot, matches);
    response.diagnostics = diagnostics;
    response
}

fn combined_predicate(request: &QueryRequest) -> Option<Predicate> {
    let legacy = filters_to_predicate(&request.filters);
    match (legacy, request.predicate.clone()) {
        (None, None) => None,
        (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
        (Some(left), Some(right)) => Some(Predicate::And {
            children: vec![left, right],
        }),
    }
}

fn filters_to_predicate(filters: &[MetadataFilter]) -> Option<Predicate> {
    if filters.is_empty() {
        None
    } else {
        Some(Predicate::And {
            children: filters
                .iter()
                .map(|filter| {
                    Predicate::Comparison(PredicateComparison {
                        field: filter.field.clone(),
                        operator: PredicateOperator::Eq,
                        value: Some(filter.value.clone()),
                    })
                })
                .collect(),
        })
    }
}

fn validate_predicate(predicate: &Predicate) -> Result<()> {
    match predicate {
        Predicate::And { children } | Predicate::Or { children } => {
            if children.is_empty() {
                return Err(QueryError::InvalidPredicate(
                    "logical predicates must include at least one child".to_owned(),
                ));
            }
            for child in children {
                validate_predicate(child)?;
            }
        }
        Predicate::Not { child } => validate_predicate(child)?,
        Predicate::Comparison(comparison) => match comparison.operator {
            PredicateOperator::Exists | PredicateOperator::IsNull => {
                if comparison.value.is_some() {
                    return Err(QueryError::InvalidPredicate(format!(
                        "predicate operator '{}' does not accept a value",
                        predicate_operator_name(comparison.operator)
                    )));
                }
            }
            PredicateOperator::Eq
            | PredicateOperator::Ne
            | PredicateOperator::Lt
            | PredicateOperator::Lte
            | PredicateOperator::Gt
            | PredicateOperator::Gte => {
                let Some(value) = comparison.value.as_ref() else {
                    return Err(QueryError::InvalidPredicate(format!(
                        "predicate operator '{}' requires a value",
                        predicate_operator_name(comparison.operator)
                    )));
                };
                if matches!(
                    comparison.operator,
                    PredicateOperator::Lt
                        | PredicateOperator::Lte
                        | PredicateOperator::Gt
                        | PredicateOperator::Gte
                ) && !supports_ordered_comparison(value)
                {
                    return Err(QueryError::InvalidPredicate(format!(
                        "predicate operator '{}' requires a string or number value",
                        predicate_operator_name(comparison.operator)
                    )));
                }
            }
        },
    }
    Ok(())
}

fn predicate_operator_name(operator: PredicateOperator) -> &'static str {
    match operator {
        PredicateOperator::Eq => "eq",
        PredicateOperator::Ne => "ne",
        PredicateOperator::Lt => "lt",
        PredicateOperator::Lte => "lte",
        PredicateOperator::Gt => "gt",
        PredicateOperator::Gte => "gte",
        PredicateOperator::Exists => "exists",
        PredicateOperator::IsNull => "is_null",
    }
}

fn choose_plan(
    predicate: Option<&Predicate>,
    estimated_selectivity: f32,
    top_k: usize,
    scanned_put_count: usize,
    has_ann_units: bool,
    include_mutable: bool,
) -> QueryPlanKind {
    match predicate {
        None if has_ann_units && include_mutable => QueryPlanKind::HybridExactAnnMerge,
        None if has_ann_units => QueryPlanKind::VectorFirstAnn,
        None => QueryPlanKind::UnfilteredExactScan,
        Some(_) if (scanned_put_count as f32 * estimated_selectivity).ceil() <= top_k as f32 => {
            QueryPlanKind::TinyPopulationExactFallback
        }
        Some(_) if has_ann_units && include_mutable => QueryPlanKind::HybridExactAnnMerge,
        Some(_) if has_ann_units && estimated_selectivity <= 0.45 => {
            QueryPlanKind::CooperativeFilteredAnn
        }
        Some(_) if has_ann_units => QueryPlanKind::VectorFirstAnn,
        Some(_) if estimated_selectivity <= 0.45 => QueryPlanKind::PredicateFirstExact,
        Some(_) => QueryPlanKind::VectorFirstExact,
    }
}

fn select_query_units(stats: &CollectionStats, predicate: Option<&Predicate>) -> UnitSelection {
    let mut selection = UnitSelection {
        units_considered: stats.query_units.len(),
        ..UnitSelection::default()
    };
    selection.include_mutable = stats
        .query_units
        .iter()
        .any(|unit| unit.tier == "mutable" && (unit.put_count > 0 || unit.delete_count > 0));

    let mut immutable_units = stats
        .query_units
        .iter()
        .filter(|unit| unit.tier != "mutable")
        .collect::<Vec<_>>();
    immutable_units.sort_by(|left, right| {
        right
            .max_seq_no
            .cmp(&left.max_seq_no)
            .then(right.min_seq_no.cmp(&left.min_seq_no))
            .then(left.unit_id.cmp(&right.unit_id))
    });

    let mut included_immutable_count = immutable_units.len();
    while included_immutable_count > 0 {
        let unit = immutable_units[included_immutable_count - 1];
        let can_match = unit.delete_count > 0
            || predicate.is_none_or(|predicate| predicate_may_match_unit(predicate, unit));
        if can_match {
            break;
        }
        included_immutable_count -= 1;
    }

    for (index, unit) in immutable_units.into_iter().enumerate() {
        if index < included_immutable_count {
            selection.immutable_unit_ids.push(unit.unit_id.clone());
            selection
                .exact_immutable_unit_ids
                .push(unit.unit_id.clone());
            if unit.index_kind == "hnsw" {
                selection.ann_immutable_unit_ids.push(unit.unit_id.clone());
            }
        } else {
            selection.units_pruned += 1;
        }
    }

    selection.units_scanned =
        selection.immutable_unit_ids.len() + usize::from(selection.include_mutable);
    selection.scanned_put_count = stats
        .query_units
        .iter()
        .filter(|unit| {
            (unit.tier == "mutable" && selection.include_mutable)
                || selection
                    .immutable_unit_ids
                    .iter()
                    .any(|unit_id| unit_id == &unit.unit_id)
        })
        .map(|unit| unit.put_count)
        .sum();
    selection
}

fn estimate_selectivity(predicate: &Predicate, stats: &CollectionStats) -> f32 {
    let total_records = stats
        .query_units
        .iter()
        .map(|unit| unit.put_count)
        .sum::<usize>()
        .max(1) as f32;

    let estimated_matches = stats
        .query_units
        .iter()
        .map(|unit| estimate_unit_selectivity(predicate, unit) * unit.put_count as f32)
        .sum::<f32>();

    (estimated_matches / total_records).clamp(0.0, 1.0)
}

fn estimate_unit_selectivity(predicate: &Predicate, unit: &QueryUnitStats) -> f32 {
    match predicate {
        Predicate::And { children } => children.iter().fold(1.0, |current, child| {
            (current * estimate_unit_selectivity(child, unit)).clamp(0.0, 1.0)
        }),
        Predicate::Or { children } => {
            1.0 - children.iter().fold(1.0, |current, child| {
                current * (1.0 - estimate_unit_selectivity(child, unit))
            })
        }
        Predicate::Not { child } => 1.0 - estimate_unit_selectivity(child, unit),
        Predicate::Comparison(comparison) => estimate_comparison_selectivity(comparison, unit),
    }
}

fn estimate_comparison_selectivity(comparison: &PredicateComparison, unit: &QueryUnitStats) -> f32 {
    let Some(field_stats) = unit.scalar_fields.get(&comparison.field) else {
        return 0.0;
    };
    let total = unit.put_count.max(1) as f32;

    match comparison.operator {
        PredicateOperator::Exists => (field_stats.present_count as f32 / total).clamp(0.0, 1.0),
        PredicateOperator::IsNull => (field_stats.null_count as f32 / total).clamp(0.0, 1.0),
        PredicateOperator::Eq => comparison
            .value
            .as_ref()
            .map(|value| {
                field_stats
                    .value_counts
                    .get(&value.summary_key())
                    .copied()
                    .unwrap_or_default() as f32
                    / total
            })
            .unwrap_or(0.0),
        PredicateOperator::Ne => {
            let scalar_present = scalar_present_count(field_stats);
            if scalar_present == 0 {
                return 0.0;
            }
            let eq_count = comparison
                .value
                .as_ref()
                .and_then(|value| field_stats.value_counts.get(&value.summary_key()))
                .copied()
                .unwrap_or_default();
            scalar_present.saturating_sub(eq_count) as f32 / total
        }
        PredicateOperator::Lt
        | PredicateOperator::Lte
        | PredicateOperator::Gt
        | PredicateOperator::Gte => {
            let Some(value) = comparison.value.as_ref() else {
                return 0.0;
            };
            let comparable_count = comparable_value_count(field_stats, value);
            if comparable_count == 0 {
                return 0.0;
            }
            let comparable_share = comparable_count as f32 / total;
            match (
                field_stats
                    .min
                    .as_ref()
                    .and_then(|min| compare_ordered_scalars(value, min)),
                field_stats
                    .max
                    .as_ref()
                    .and_then(|max| compare_ordered_scalars(value, max)),
            ) {
                (Some(min_ordering), Some(max_ordering)) => {
                    let estimate = match comparison.operator {
                        PredicateOperator::Lt if min_ordering != Ordering::Greater => 0.0,
                        PredicateOperator::Lt if max_ordering == Ordering::Greater => 1.0,
                        PredicateOperator::Lte if min_ordering == Ordering::Less => 0.0,
                        PredicateOperator::Lte if max_ordering != Ordering::Less => 1.0,
                        PredicateOperator::Gt if max_ordering != Ordering::Less => 0.0,
                        PredicateOperator::Gt if min_ordering == Ordering::Less => 1.0,
                        PredicateOperator::Gte if max_ordering == Ordering::Greater => 0.0,
                        PredicateOperator::Gte if min_ordering != Ordering::Greater => 1.0,
                        _ => 0.6,
                    };
                    (estimate * comparable_share).clamp(0.0, 1.0)
                }
                _ => (0.6 * comparable_share).clamp(0.0, 1.0),
            }
        }
    }
}

fn filter_records_by_predicate<I>(records: I, predicate: Option<&Predicate>) -> Vec<VisibleRecord>
where
    I: IntoIterator<Item = VisibleRecord>,
{
    records
        .into_iter()
        .filter(|record| {
            predicate
                .is_none_or(|predicate| predicate_matches_metadata(&record.metadata, predicate))
        })
        .collect()
}

fn predicate_matches_metadata(metadata: &Value, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::And { children } => children
            .iter()
            .all(|child| predicate_matches_metadata(metadata, child)),
        Predicate::Or { children } => children
            .iter()
            .any(|child| predicate_matches_metadata(metadata, child)),
        Predicate::Not { child } => !predicate_matches_metadata(metadata, child),
        Predicate::Comparison(comparison) => comparison_matches_metadata(metadata, comparison),
    }
}

fn comparison_matches_metadata(metadata: &Value, comparison: &PredicateComparison) -> bool {
    let field_value = metadata
        .as_object()
        .and_then(|fields| fields.get(&comparison.field));

    match comparison.operator {
        PredicateOperator::Exists => field_value.is_some(),
        PredicateOperator::IsNull => matches!(field_value, Some(Value::Null)),
        PredicateOperator::Eq => field_value
            .and_then(ScalarMetadataValue::from_json)
            .zip(comparison.value.clone())
            .is_some_and(|(actual, expected)| actual == expected),
        PredicateOperator::Ne => field_value
            .and_then(ScalarMetadataValue::from_json)
            .zip(comparison.value.clone())
            .is_some_and(|(actual, expected)| actual != expected),
        PredicateOperator::Lt => {
            compare_field_value(field_value, comparison.value.as_ref(), Ordering::Less)
        }
        PredicateOperator::Lte => {
            compare_field_value(field_value, comparison.value.as_ref(), Ordering::Less)
                || compare_field_value(field_value, comparison.value.as_ref(), Ordering::Equal)
        }
        PredicateOperator::Gt => {
            compare_field_value(field_value, comparison.value.as_ref(), Ordering::Greater)
        }
        PredicateOperator::Gte => {
            compare_field_value(field_value, comparison.value.as_ref(), Ordering::Greater)
                || compare_field_value(field_value, comparison.value.as_ref(), Ordering::Equal)
        }
    }
}

fn compare_field_value(
    actual: Option<&Value>,
    expected: Option<&ScalarMetadataValue>,
    ordering: Ordering,
) -> bool {
    actual
        .and_then(ScalarMetadataValue::from_json)
        .zip(expected.cloned())
        .and_then(|(actual, expected)| compare_ordered_scalars(&actual, &expected))
        .is_some_and(|actual_ordering| actual_ordering == ordering)
}

fn build_diagnostics(
    request: &QueryRequest,
    chosen_plan: QueryPlanKind,
    estimated_selectivity: f32,
    unit_selection: &UnitSelection,
    measurements: DiagnosticMeasurements,
) -> Option<QueryDiagnostics> {
    match request.explain {
        ExplainMode::None => None,
        ExplainMode::Plan | ExplainMode::Profile => Some(QueryDiagnostics {
            chosen_plan,
            planner_reason: planner_reason(chosen_plan, estimated_selectivity).to_owned(),
            estimated_selectivity,
            units_considered: unit_selection.units_considered,
            units_pruned: unit_selection.units_pruned,
            units_scanned: unit_selection.units_scanned,
            candidates_before_filter: measurements.candidates_before_filter,
            candidates_after_filter: measurements.candidates_after_filter,
            candidates_reranked: measurements.candidates_reranked,
            candidates_merged: measurements.candidates_merged,
            rerank_count: measurements.rerank_count,
            fallback_reason: fallback_reason(chosen_plan, estimated_selectivity).map(str::to_owned),
            unit_scan_mix: unit_scan_mix(chosen_plan, unit_selection),
            stage_timings: match request.explain {
                ExplainMode::Profile => Some(QueryStageTimings {
                    planning_micros: measurements.planning_micros,
                    prefilter_micros: measurements.prefilter_micros,
                    candidate_generation_micros: measurements.candidate_generation_micros,
                    postfilter_micros: measurements.postfilter_micros,
                    rerank_micros: measurements.rerank_micros,
                    merge_micros: measurements.merge_micros,
                }),
                ExplainMode::None | ExplainMode::Plan => None,
            },
        }),
    }
}

fn planner_reason(plan: QueryPlanKind, estimated_selectivity: f32) -> &'static str {
    match plan {
        QueryPlanKind::UnfilteredExactScan => "no predicate supplied",
        QueryPlanKind::PredicateFirstExact if estimated_selectivity <= 0.45 => {
            "predicate is selective enough to filter before ranking"
        }
        QueryPlanKind::VectorFirstExact => {
            "predicate is broad enough that ranking first is cheaper"
        }
        QueryPlanKind::TinyPopulationExactFallback => {
            "estimated predicate population is small enough for an exact fallback"
        }
        QueryPlanKind::VectorFirstAnn => {
            "immutable hnsw units can generate candidates more cheaply than exact ranking"
        }
        QueryPlanKind::CooperativeFilteredAnn => {
            "filtered ann traversal is cheaper than exact scan for this selectivity"
        }
        QueryPlanKind::HybridExactAnnMerge => {
            "mutable exact candidates and immutable ann candidates must be merged before rerank"
        }
        QueryPlanKind::PredicateFirstExact => "predicate-first exact scan selected",
    }
}

fn fallback_reason(plan: QueryPlanKind, estimated_selectivity: f32) -> Option<&'static str> {
    match plan {
        QueryPlanKind::TinyPopulationExactFallback => {
            Some("estimated predicate population is below the requested top-k")
        }
        QueryPlanKind::PredicateFirstExact if estimated_selectivity <= 0.45 => {
            Some("no immutable ann units are available for the selective predicate path")
        }
        QueryPlanKind::VectorFirstExact => {
            Some("no immutable ann units are available for the broad predicate path")
        }
        QueryPlanKind::UnfilteredExactScan => Some("no immutable ann units are available"),
        QueryPlanKind::VectorFirstAnn
        | QueryPlanKind::CooperativeFilteredAnn
        | QueryPlanKind::HybridExactAnnMerge
        | QueryPlanKind::PredicateFirstExact => None,
    }
}

fn unit_scan_mix(plan: QueryPlanKind, unit_selection: &UnitSelection) -> BTreeMap<String, usize> {
    let mut mix = BTreeMap::new();
    if unit_selection.include_mutable {
        mix.insert("mutable_exact".to_owned(), 1);
    }
    match plan {
        QueryPlanKind::UnfilteredExactScan
        | QueryPlanKind::PredicateFirstExact
        | QueryPlanKind::VectorFirstExact
        | QueryPlanKind::TinyPopulationExactFallback => {
            if !unit_selection.exact_immutable_unit_ids.is_empty() {
                mix.insert(
                    "immutable_exact".to_owned(),
                    unit_selection.exact_immutable_unit_ids.len(),
                );
            }
        }
        QueryPlanKind::VectorFirstAnn | QueryPlanKind::CooperativeFilteredAnn => {
            if !unit_selection.ann_immutable_unit_ids.is_empty() {
                mix.insert(
                    "immutable_ann".to_owned(),
                    unit_selection.ann_immutable_unit_ids.len(),
                );
            }
        }
        QueryPlanKind::HybridExactAnnMerge => {
            if !unit_selection.ann_immutable_unit_ids.is_empty() {
                mix.insert(
                    "immutable_ann".to_owned(),
                    unit_selection.ann_immutable_unit_ids.len(),
                );
            }
        }
    }
    mix
}

fn predicate_may_match_unit(predicate: &Predicate, unit: &QueryUnitStats) -> bool {
    match predicate {
        Predicate::And { children } => children
            .iter()
            .all(|child| predicate_may_match_unit(child, unit)),
        Predicate::Or { children } => children
            .iter()
            .any(|child| predicate_may_match_unit(child, unit)),
        Predicate::Not { .. } => true,
        Predicate::Comparison(comparison) => comparison_may_match_unit(comparison, unit),
    }
}

fn comparison_may_match_unit(comparison: &PredicateComparison, unit: &QueryUnitStats) -> bool {
    let Some(field_stats) = unit.scalar_fields.get(&comparison.field) else {
        return false;
    };

    match comparison.operator {
        PredicateOperator::Exists => field_stats.present_count > 0,
        PredicateOperator::IsNull => field_stats.null_count > 0,
        PredicateOperator::Eq => comparison
            .value
            .as_ref()
            .is_some_and(|value| field_stats.value_counts.contains_key(&value.summary_key())),
        PredicateOperator::Ne => comparison.value.as_ref().is_none_or(|value| {
            field_stats
                .value_counts
                .get(&value.summary_key())
                .is_none_or(|count| *count < scalar_present_count(field_stats))
        }),
        PredicateOperator::Lt | PredicateOperator::Lte => {
            let Some(value) = comparison.value.as_ref() else {
                return false;
            };
            if comparable_value_count(field_stats, value) == 0 {
                return false;
            }
            field_stats
                .min
                .as_ref()
                .and_then(|min| compare_ordered_scalars(min, value))
                .is_none_or(|ordering| match comparison.operator {
                    PredicateOperator::Lt => ordering == Ordering::Less,
                    PredicateOperator::Lte => ordering != Ordering::Greater,
                    _ => false,
                })
        }
        PredicateOperator::Gt | PredicateOperator::Gte => {
            let Some(value) = comparison.value.as_ref() else {
                return false;
            };
            if comparable_value_count(field_stats, value) == 0 {
                return false;
            }
            field_stats
                .max
                .as_ref()
                .and_then(|max| compare_ordered_scalars(max, value))
                .is_none_or(|ordering| match comparison.operator {
                    PredicateOperator::Gt => ordering == Ordering::Greater,
                    PredicateOperator::Gte => ordering != Ordering::Less,
                    _ => false,
                })
        }
    }
}

fn supports_ordered_comparison(value: &ScalarMetadataValue) -> bool {
    matches!(
        value,
        ScalarMetadataValue::String(_) | ScalarMetadataValue::Number(_)
    )
}

fn ordered_summary_prefix(value: &ScalarMetadataValue) -> Option<&'static str> {
    match value {
        ScalarMetadataValue::String(_) => Some("string:"),
        ScalarMetadataValue::Number(_) => Some("number:"),
        ScalarMetadataValue::Bool(_) | ScalarMetadataValue::Null => None,
    }
}

fn comparable_value_count(field_stats: &ScalarFieldStats, value: &ScalarMetadataValue) -> usize {
    let Some(prefix) = ordered_summary_prefix(value) else {
        return 0;
    };
    field_stats
        .value_counts
        .iter()
        .filter(|(summary_key, _)| summary_key.starts_with(prefix))
        .map(|(_, count)| *count)
        .sum()
}

fn scalar_present_count(field_stats: &ScalarFieldStats) -> usize {
    field_stats.value_counts.values().sum()
}

fn compare_ordered_scalars(
    left: &ScalarMetadataValue,
    right: &ScalarMetadataValue,
) -> Option<Ordering> {
    match (left, right) {
        (ScalarMetadataValue::String(left), ScalarMetadataValue::String(right)) => {
            Some(left.cmp(right))
        }
        (ScalarMetadataValue::Number(left), ScalarMetadataValue::Number(right)) => {
            Some(compare_numbers(left, right))
        }
        _ => None,
    }
}

fn compare_numbers(left: &serde_json::Number, right: &serde_json::Number) -> Ordering {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return left.cmp(&right);
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return left.cmp(&right);
    }
    left.as_f64()
        .unwrap_or_default()
        .partial_cmp(&right.as_f64().unwrap_or_default())
        .unwrap_or(Ordering::Equal)
}

#[derive(Default)]
struct UnitSelection {
    include_mutable: bool,
    immutable_unit_ids: Vec<String>,
    ann_immutable_unit_ids: Vec<String>,
    exact_immutable_unit_ids: Vec<String>,
    units_considered: usize,
    units_pruned: usize,
    units_scanned: usize,
    scanned_put_count: usize,
}

impl UnitSelection {
    fn has_full_ann_coverage(&self) -> bool {
        !self.immutable_unit_ids.is_empty()
            && self.immutable_unit_ids.len() == self.ann_immutable_unit_ids.len()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct DiagnosticMeasurements {
    candidates_before_filter: usize,
    candidates_after_filter: usize,
    candidates_reranked: usize,
    candidates_merged: usize,
    planning_micros: u64,
    prefilter_micros: u64,
    candidate_generation_micros: u64,
    postfilter_micros: u64,
    rerank_micros: u64,
    merge_micros: u64,
    rerank_count: usize,
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
        (ScalarMetadataValue::Number(expected), Value::Number(actual)) => expected == actual,
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
    use logpose_types::{
        CollectionStats, CommitAck, LogPoseError, MaintenanceStatus, QueryUnitStats, WriteOperation,
    };
    use serde_json::json;
    use std::{collections::BTreeMap, path::Path, sync::Mutex};

    fn record(id: &str, vector: Vec<f32>) -> VisibleRecord {
        VisibleRecord {
            id: RecordId::from(id),
            vector,
            metadata: json!({ "id": id }),
            seq_no: 1,
        }
    }

    #[test]
    fn parse_collection_reference_accepts_database_collection() {
        let reference = parse_collection_reference("analytics/documents")
            .expect("database-qualified collection name should parse");

        assert_eq!(reference.database_name, "analytics");
        assert_eq!(reference.collection_name, "documents");
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
            predicate: None,
            explain: ExplainMode::None,
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
            predicate: None,
            explain: ExplainMode::None,
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
            predicate: None,
            explain: ExplainMode::None,
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
                        value: ScalarMetadataValue::Number(Number::from(7)),
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

    #[test]
    fn large_integer_filters_do_not_collapse_distinct_values() {
        let matches = rank_matches(
            DistanceMetric::Dot,
            &[1.0, 0.0],
            filter_records(
                vec![
                    VisibleRecord {
                        id: RecordId::from("lower"),
                        vector: vec![1.0, 0.0],
                        metadata: json!({ "score": 9007199254740992u64 }),
                        seq_no: 1,
                    },
                    VisibleRecord {
                        id: RecordId::from("higher"),
                        vector: vec![2.0, 0.0],
                        metadata: json!({ "score": 9007199254740993u64 }),
                        seq_no: 2,
                    },
                ],
                &[MetadataFilter {
                    field: "score".to_owned(),
                    value: ScalarMetadataValue::Number(Number::from(9007199254740993u64)),
                }],
            ),
            5,
        )
        .expect("ranking should succeed");

        assert_eq!(
            matches
                .iter()
                .map(|match_| match_.id.as_str())
                .collect::<Vec<_>>(),
            vec!["higher"]
        );
    }

    #[test]
    fn ann_candidate_budget_saturates_without_overflow() {
        assert_eq!(ann_candidate_budget(usize::MAX, 0.1), usize::MAX);
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
                predicate: None,
                explain: ExplainMode::None,
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
                predicate: None,
                explain: ExplainMode::None,
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

    #[tokio::test]
    async fn rejects_missing_values_for_binary_predicates() {
        let result = query_exact(
            &FilteredStorageEngine,
            QueryRequest {
                collection_name: "filtered".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
                predicate: Some(Predicate::Comparison(PredicateComparison {
                    field: "kind".to_owned(),
                    operator: PredicateOperator::Eq,
                    value: None,
                })),
                explain: ExplainMode::None,
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(QueryError::InvalidPredicate(message))
                if message.contains("requires a value")
        ));
    }

    #[tokio::test]
    async fn rejects_values_for_unary_predicates() {
        let result = query_exact(
            &FilteredStorageEngine,
            QueryRequest {
                collection_name: "filtered".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
                predicate: Some(Predicate::Comparison(PredicateComparison {
                    field: "kind".to_owned(),
                    operator: PredicateOperator::Exists,
                    value: Some(ScalarMetadataValue::String("keep".to_owned())),
                })),
                explain: ExplainMode::None,
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(QueryError::InvalidPredicate(message))
                if message.contains("does not accept a value")
        ));
    }

    #[tokio::test]
    async fn rejects_empty_logical_predicates() {
        let result = query_exact(
            &FilteredStorageEngine,
            QueryRequest {
                collection_name: "filtered".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
                predicate: Some(Predicate::And {
                    children: Vec::new(),
                }),
                explain: ExplainMode::None,
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(QueryError::InvalidPredicate(message))
                if message.contains("at least one child")
        ));
    }

    #[tokio::test]
    async fn rejects_unordered_values_for_range_predicates() {
        let result = query_exact(
            &FilteredStorageEngine,
            QueryRequest {
                collection_name: "filtered".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
                predicate: Some(Predicate::Comparison(PredicateComparison {
                    field: "kind".to_owned(),
                    operator: PredicateOperator::Gt,
                    value: Some(ScalarMetadataValue::Bool(true)),
                })),
                explain: ExplainMode::None,
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(QueryError::InvalidPredicate(message))
                if message.contains("string or number value")
        ));
    }

    #[tokio::test]
    async fn ordered_predicates_do_not_match_incompatible_actual_types() {
        let result = query_exact(
            &FilteredStorageEngine,
            QueryRequest {
                collection_name: "filtered".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 3,
                snapshot: None,
                filters: Vec::new(),
                predicate: Some(Predicate::Comparison(PredicateComparison {
                    field: "kind".to_owned(),
                    operator: PredicateOperator::Gt,
                    value: Some(ScalarMetadataValue::Number(Number::from(1))),
                })),
                explain: ExplainMode::None,
            },
        )
        .await
        .expect("query should succeed");

        assert!(result.matches.is_empty());
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

        async fn stats_snapshot(
            &self,
            _collection_name: &str,
            _snapshot: Option<Snapshot>,
        ) -> logpose_types::Result<CollectionStats> {
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
                predicate: None,
                explain: ExplainMode::None,
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

    #[tokio::test]
    async fn query_exact_resolves_database_qualified_collection_refs_before_storage_calls() {
        let storage = QualifiedReferenceStorage {
            seen_names: Mutex::new(Vec::new()),
        };

        let result = query_exact(
            &storage,
            QueryRequest {
                collection_name: "analytics/profiles".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                filters: Vec::new(),
                predicate: None,
                explain: ExplainMode::None,
            },
        )
        .await
        .expect("query should succeed");

        assert_eq!(result.returned, 1);
        assert_eq!(result.matches[0].id.as_str(), "alpha");
        assert_eq!(
            storage
                .seen_names
                .lock()
                .expect("seen names should be readable")
                .as_slice(),
            &[
                "analytics/profiles",
                "analytics/profiles",
                "analytics/profiles",
                "analytics/profiles",
            ]
        );
    }

    struct MalformedStorageEngine;

    struct QualifiedReferenceStorage {
        seen_names: Mutex<Vec<String>>,
    }

    impl QualifiedReferenceStorage {
        fn record_name(&self, collection_name: &str) -> logpose_types::Result<()> {
            if collection_name != "analytics/profiles" {
                return Err(LogPoseError::Message(format!(
                    "expected qualified collection name 'analytics/profiles', got '{collection_name}'"
                )));
            }
            self.seen_names
                .lock()
                .expect("seen names lock should not be poisoned")
                .push(collection_name.to_owned());
            Ok(())
        }
    }

    #[async_trait]
    impl StorageEngine for QualifiedReferenceStorage {
        async fn engine_name(&self) -> &'static str {
            "qualified"
        }

        async fn create_collection(
            &self,
            _request: CreateCollectionRequest,
        ) -> logpose_types::Result<CollectionDescriptor> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn open_collection(&self, name: &str) -> logpose_types::Result<CollectionDescriptor> {
            self.record_name(name)?;
            Ok(CollectionDescriptor::new_in_database(
                "analytics",
                "profiles",
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

        async fn snapshot(&self, collection_name: &str) -> logpose_types::Result<Snapshot> {
            self.record_name(collection_name)?;
            Ok(Snapshot {
                manifest_generation: 5,
                visible_seq_no: 9,
            })
        }

        async fn scan_exact(
            &self,
            collection_name: &str,
            _snapshot: Option<Snapshot>,
        ) -> logpose_types::Result<Vec<VisibleRecord>> {
            self.record_name(collection_name)?;
            Ok(vec![VisibleRecord {
                id: RecordId::new("alpha"),
                vector: vec![2.0, 0.0],
                metadata: json!({ "kind": "keep" }),
                seq_no: 9,
            }])
        }

        async fn flush(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn compact(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }

        async fn stats(&self, collection_name: &str) -> logpose_types::Result<CollectionStats> {
            self.record_name(collection_name)?;
            Ok(CollectionStats {
                collection_id: CollectionDescriptor::new_in_database(
                    "analytics",
                    "profiles",
                    2,
                    DistanceMetric::Dot,
                    Path::new("/tmp"),
                )
                .collection_id,
                database_name: "analytics".to_owned(),
                collection_name: "profiles".to_owned(),
                manifest_generation: 5,
                visible_seq_no: 9,
                mutable_op_count: 1,
                segment_count: 0,
                live_record_count: 1,
                deleted_record_count: 0,
                maintenance: MaintenanceStatus::default(),
                query_units: vec![QueryUnitStats {
                    unit_id: "mutable-delta".to_owned(),
                    tier: "mutable".to_owned(),
                    index_kind: "raw".to_owned(),
                    min_seq_no: 9,
                    max_seq_no: 9,
                    put_count: 1,
                    delete_count: 0,
                    approx_bytes: 32,
                    scalar_fields: BTreeMap::new(),
                    artifact_stats: vec![logpose_types::QueryUnitArtifactStats {
                        kind: "mutable_delta".to_owned(),
                        file_name: String::new(),
                        approx_bytes: 32,
                    }],
                    component_bytes: BTreeMap::from([("mutable_delta".to_owned(), 32)]),
                }],
            })
        }

        async fn stats_snapshot(
            &self,
            collection_name: &str,
            snapshot: Option<Snapshot>,
        ) -> logpose_types::Result<CollectionStats> {
            let mut stats = self.stats(collection_name).await?;
            if let Some(snapshot) = snapshot {
                stats.manifest_generation = snapshot.manifest_generation;
                stats.visible_seq_no = snapshot.visible_seq_no;
            }
            Ok(stats)
        }

        async fn inspect(
            &self,
            _collection_name: &str,
            _target: InspectTarget,
        ) -> logpose_types::Result<InspectReport> {
            Err(LogPoseError::Message("not implemented".to_owned()))
        }
    }

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
            Ok(CollectionStats {
                collection_id: CollectionDescriptor::new(
                    "broken",
                    2,
                    DistanceMetric::Dot,
                    Path::new("/tmp"),
                )
                .collection_id,
                database_name: "default".to_owned(),
                collection_name: "broken".to_owned(),
                manifest_generation: 4,
                visible_seq_no: 9,
                mutable_op_count: 1,
                segment_count: 0,
                live_record_count: 1,
                deleted_record_count: 0,
                maintenance: MaintenanceStatus::default(),
                query_units: vec![QueryUnitStats {
                    unit_id: "mutable-delta".to_owned(),
                    tier: "mutable".to_owned(),
                    index_kind: "raw".to_owned(),
                    min_seq_no: 9,
                    max_seq_no: 9,
                    put_count: 1,
                    delete_count: 0,
                    approx_bytes: 32,
                    scalar_fields: BTreeMap::new(),
                    artifact_stats: vec![logpose_types::QueryUnitArtifactStats {
                        kind: "mutable_delta".to_owned(),
                        file_name: String::new(),
                        approx_bytes: 32,
                    }],
                    component_bytes: BTreeMap::from([("mutable_delta".to_owned(), 32)]),
                }],
            })
        }

        async fn stats_snapshot(
            &self,
            collection_name: &str,
            snapshot: Option<Snapshot>,
        ) -> logpose_types::Result<CollectionStats> {
            self.stats(collection_name).await.map(|mut stats| {
                if let Some(snapshot) = snapshot {
                    stats.manifest_generation = snapshot.manifest_generation;
                    stats.visible_seq_no = snapshot.visible_seq_no;
                }
                stats
            })
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
            Ok(CollectionStats {
                collection_id: CollectionDescriptor::new(
                    "filtered",
                    2,
                    DistanceMetric::Dot,
                    Path::new("/tmp"),
                )
                .collection_id,
                database_name: "default".to_owned(),
                collection_name: "filtered".to_owned(),
                manifest_generation: 3,
                visible_seq_no: 8,
                mutable_op_count: 3,
                segment_count: 0,
                live_record_count: 3,
                deleted_record_count: 0,
                maintenance: MaintenanceStatus::default(),
                query_units: vec![QueryUnitStats {
                    unit_id: "mutable-delta".to_owned(),
                    tier: "mutable".to_owned(),
                    index_kind: "raw".to_owned(),
                    min_seq_no: 4,
                    max_seq_no: 6,
                    put_count: 3,
                    delete_count: 0,
                    approx_bytes: 64,
                    scalar_fields: BTreeMap::from([(
                        "kind".to_owned(),
                        logpose_types::ScalarFieldStats {
                            present_count: 3,
                            null_count: 0,
                            distinct_count: 2,
                            min: Some(ScalarMetadataValue::String("drop".to_owned())),
                            max: Some(ScalarMetadataValue::String("keep".to_owned())),
                            value_counts: BTreeMap::from([
                                ("string:keep".to_owned(), 2),
                                ("string:drop".to_owned(), 1),
                            ]),
                        },
                    )]),
                    artifact_stats: vec![logpose_types::QueryUnitArtifactStats {
                        kind: "mutable_delta".to_owned(),
                        file_name: String::new(),
                        approx_bytes: 64,
                    }],
                    component_bytes: BTreeMap::from([("mutable_delta".to_owned(), 64)]),
                }],
            })
        }

        async fn stats_snapshot(
            &self,
            collection_name: &str,
            snapshot: Option<Snapshot>,
        ) -> logpose_types::Result<CollectionStats> {
            self.stats(collection_name).await.map(|mut stats| {
                if let Some(snapshot) = snapshot {
                    stats.manifest_generation = snapshot.manifest_generation;
                    stats.visible_seq_no = snapshot.visible_seq_no;
                }
                stats
            })
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
