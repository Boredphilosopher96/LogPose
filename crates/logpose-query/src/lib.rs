//! Query execution over the engine's read interfaces.
//!
//! Every read opens one [`ReadView`] through a [`CollectionReader`] and runs against it, so a
//! query sees one consistent snapshot from its first stage to its last, and a flush or
//! compaction that lands while it runs cannot expire it. The modules:
//!
//! - [`compile`]: filters to per-unit bitmaps, through scalar indexes where a unit has one and
//!   column or `$extra` scans otherwise.
//! - [`search`](mod@search): staged vector search: per-unit strategy from the exact filter cardinality
//!   (exact scan, ACORN-1 style walk, or admit-only walk over SQ8 codes), a resumable cursor
//!   that widens `ef` for short or anti-correlated results, an exact f32 rerank, and a global
//!   top-k merge.
//! - [`ops`]: get, count, scroll by key, and order by a field.
//! - [`resolver`](mod@resolver): the [`RowSetResolver`](logpose_storage::RowSetResolver) the engine uses for
//!   delete-by-filter and update-by-filter.
//!
//! [`query`] serves the API's single-vector query shape ([`QueryRequest`]) on top of them.

pub mod compile;
pub mod ops;
pub mod resolver;
pub mod search;

#[cfg(test)]
mod tests;

pub use compile::CompiledFilter;
pub use logpose_types::ScalarMetadataValue;
pub use logpose_types::filter::{FilterComparison, FilterExpr, FilterOperator};
pub use ops::{
    Cursor, CursorKey, ScrollOrder, ScrollPage, ScrollRequest, count, count_view, get,
    resolve_view, scroll, scroll_view,
};
pub use resolver::{QueryResolver, resolver};
pub use search::{
    SearchHit, SearchOutcome, SearchRequest, SearchTuning, UnitReport, UnitStrategy, metric_value,
    search,
};

use logpose_catalog as _;
use logpose_storage::{
    CollectionReader, Projection, ReadOptions, ReadView, RowData, SnapshotToken,
};
use logpose_types::{
    CollectionRef, DistanceMetric, LogPoseError, RecordId, ResourceKind, Snapshot, VisibleRecord,
    record::PrimaryKey, schema::CollectionSchema,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use thiserror::Error;

/// Narrow request payload for a single-vector search.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    /// Target collection name.
    pub collection_name: String,
    /// Query embedding vector.
    pub vector: Vec<f32>,
    /// Maximum number of matches to return.
    pub top_k: usize,
    /// Read exactly this snapshot (the current state, one of the latest states of the current
    /// manifest generation, or a token-pinned one).
    pub snapshot: Option<Snapshot>,
    /// Optional lower-bound read barrier that the server must satisfy or reject.
    #[serde(default)]
    pub read_barrier: Option<Snapshot>,
    /// Optional top-level metadata equality filters combined with AND semantics.
    #[serde(default)]
    pub filters: Vec<MetadataFilter>,
    /// Optional structured predicate tree over top-level scalar metadata.
    #[serde(default)]
    pub predicate: Option<FilterExpr>,
    /// Optional explain/profile mode for planner diagnostics.
    #[serde(default)]
    pub explain: ExplainMode,
    /// Read the state this snapshot token pins (and extend its expiry).
    #[serde(default)]
    pub snapshot_token: Option<String>,
    /// Pin the state the query reads; the response carries the token.
    #[serde(default)]
    pub pin: bool,
}

/// Top-level metadata equality filter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataFilter {
    /// Top-level metadata field to match.
    pub field: String,
    /// Required scalar value for the field.
    pub value: ScalarMetadataValue,
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

/// The plan a search used, summarized over its units.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryPlanKind {
    /// Every unit was scanned exactly and no filter was given.
    UnfilteredExactScan,
    /// Every unit was scanned exactly over its filter bitmap.
    PredicateFirstExact,
    /// Not produced by the staged planner; kept for wire compatibility.
    VectorFirstExact,
    /// Not produced by the staged planner; kept for wire compatibility.
    TinyPopulationExactFallback,
    /// Graph walks over segments, no filter, no memtable rows.
    VectorFirstAnn,
    /// Filtered graph walks (ACORN-1 style or admit-only) over segments.
    CooperativeFilteredAnn,
    /// Graph walks over segments merged with exact memtable scans.
    HybridExactAnnMerge,
}

/// Per-stage timings reported for profile mode.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryStageTimings {
    /// Planning and the first fetch (filter sections and vector indexes).
    pub planning_micros: u64,
    /// Not measured separately: filters compile inside candidate generation.
    pub prefilter_micros: u64,
    /// Per-unit filter bitmaps and candidate generation.
    pub candidate_generation_micros: u64,
    /// Reading the result rows.
    pub postfilter_micros: u64,
    /// The second fetch (f32 pages) and the exact rerank.
    pub rerank_micros: u64,
    /// Not measured separately: the global merge is part of the rerank.
    pub merge_micros: u64,
}

/// Planner and execution diagnostics surfaced to operators.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryDiagnostics {
    /// Plan summary.
    pub chosen_plan: QueryPlanKind,
    /// Short human-readable reason for the plan.
    pub planner_reason: String,
    /// Exact filter selectivity over the view's live rows (1 without a filter).
    pub estimated_selectivity: f32,
    /// Units in the view.
    pub units_considered: usize,
    /// Units whose zone maps excluded the filter.
    pub units_pruned: usize,
    /// Units that produced candidates.
    pub units_scanned: usize,
    /// Live rows of the units searched.
    pub candidates_before_filter: usize,
    /// Live rows matching the filter.
    pub candidates_after_filter: usize,
    /// Candidates scored exactly in the rerank.
    pub candidates_reranked: usize,
    /// Candidates entering the global merge.
    pub candidates_merged: usize,
    /// 1 when a rerank ran.
    pub rerank_count: usize,
    /// Why an approximate path was not used, when none was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
    /// Units per strategy (`memtable_scan`, `exact_sq8`, `exact_f32`, `graph_admit`,
    /// `graph_acorn`, `pruned`, `empty`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub unit_scan_mix: BTreeMap<String, usize>,
    /// Stage timings when profile mode is requested.
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
    /// The record's fields: typed scalar fields and visible `$extra` keys.
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
    /// Snapshot naming the state the query read.
    pub snapshot: Snapshot,
    /// Ranked matches.
    pub matches: Vec<QueryMatch>,
    /// Optional planner and execution diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<QueryDiagnostics>,
    /// The token pinning the state read, when the request pinned one or read through one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_token: Option<String>,
}

/// Query-scoped error returned when a request cannot be served.
#[derive(Debug, Error)]
pub enum QueryError {
    /// Query vector dimensionality must match the collection's vector field.
    #[error("query vector dimension mismatch: expected {expected}, found {actual}")]
    RequestVectorDimensionMismatch {
        /// Expected dimensionality.
        expected: usize,
        /// Actual query dimensionality.
        actual: usize,
    },
    /// Filter structure is malformed for the requested operators.
    #[error("{0}")]
    InvalidPredicate(String),
    /// Storage failures are surfaced directly from the read path.
    #[error(transparent)]
    Storage(#[from] LogPoseError),
}

/// Result type for query helpers.
pub type Result<T> = std::result::Result<T, QueryError>;

impl From<QueryError> for LogPoseError {
    fn from(error: QueryError) -> Self {
        match error {
            QueryError::RequestVectorDimensionMismatch { expected, actual } => {
                LogPoseError::DimensionMismatch {
                    field: "vector".to_owned(),
                    record_id: None,
                    expected,
                    actual,
                }
            }
            QueryError::InvalidPredicate(message) => {
                LogPoseError::invalid_field("predicate", message)
            }
            QueryError::Storage(error) => error,
        }
    }
}

/// Serve one [`QueryRequest`]: open one view (current, token-pinned, or an exact snapshot,
/// pinning it when asked), search it, and return the matches with the snapshot read.
///
/// The view is held for the whole query, so a query that names no snapshot never fails because
/// a flush or compaction published while it ran.
///
/// # Errors
///
/// `NotFound` for an unknown collection, invalid requests, `SnapshotExpired` for an expired
/// token or snapshot, `ReadBarrierNotSatisfied`, I/O, and typed corruption.
pub async fn query(reader: &dyn CollectionReader, request: QueryRequest) -> Result<QueryResponse> {
    let collection = &request.collection_name;
    let reference = CollectionRef::parse(collection)?;
    let token = request
        .snapshot_token
        .as_deref()
        .map(|token| {
            token.parse::<SnapshotToken>().map_err(|error| {
                LogPoseError::invalid_field(
                    "snapshot_token",
                    format!("invalid snapshot token: {error}"),
                )
            })
        })
        .transpose()?;
    let options = ReadOptions {
        token,
        snapshot: request.snapshot.clone(),
        read_barrier: request.read_barrier.clone(),
        pin: request.pin,
    };
    let view = reader
        .read_view(&reference, options)
        .await
        .map_err(|error| qualify_collection_error(error, collection))?;
    let filter = combined_predicate(&request);
    let search_request = SearchRequest {
        filter,
        ..SearchRequest::new(request.vector.clone(), request.top_k)
    };
    let outcome = search::search(&view, &search_request).await?;
    let metric = view
        .schema()
        .vectors()
        .first()
        .map_or(DistanceMetric::Cosine, |field| field.metric);
    let matches = outcome
        .hits
        .iter()
        .map(|hit| QueryMatch {
            id: legacy_id(&hit.row.record.pk),
            value: hit.value,
            metadata: Value::Object(metadata(&hit.row)),
        })
        .collect::<Vec<_>>();
    let diagnostics = match request.explain {
        ExplainMode::None => None,
        explain => Some(diagnostics(
            &outcome,
            search_request.filter.is_some(),
            explain,
        )),
    };
    Ok(QueryResponse {
        metric,
        top_k: request.top_k,
        returned: matches.len(),
        snapshot: view.snapshot(),
        matches,
        diagnostics,
        snapshot_token: view.token().map(ToString::to_string),
    })
}

/// Report any missing resource on the way to a collection (its database, say) as the
/// collection the caller named.
fn qualify_collection_error(error: LogPoseError, collection_name: &str) -> LogPoseError {
    match error {
        LogPoseError::NotFound { .. } => {
            LogPoseError::not_found(ResourceKind::Collection, collection_name)
        }
        other => other,
    }
}

/// The request's equality filters and predicate, as one filter.
fn combined_predicate(request: &QueryRequest) -> Option<FilterExpr> {
    let equalities = (!request.filters.is_empty()).then(|| FilterExpr::And {
        children: request
            .filters
            .iter()
            .map(|filter| {
                FilterExpr::Comparison(FilterComparison {
                    field: filter.field.clone(),
                    operator: FilterOperator::Eq,
                    value: Some(filter.value.clone()),
                })
            })
            .collect(),
    });
    match (equalities, request.predicate.clone()) {
        (None, None) => None,
        (Some(filter), None) | (None, Some(filter)) => Some(filter),
        (Some(left), Some(right)) => Some(FilterExpr::And {
            children: vec![left, right],
        }),
    }
}

fn diagnostics(outcome: &SearchOutcome, filtered: bool, explain: ExplainMode) -> QueryDiagnostics {
    let mut mix = BTreeMap::new();
    let mut graph = false;
    let mut memtable_rows = false;
    let mut live = 0_u64;
    let mut matched = 0_u64;
    let mut merged = 0;
    for unit in &outcome.units {
        *mix.entry(unit.strategy.name().to_owned()).or_insert(0) += 1;
        graph |= unit.strategy.is_graph();
        memtable_rows |= unit.memtable && unit.matched > 0;
        live += unit.live;
        matched += unit.matched;
        merged += unit.candidates;
    }
    let pruned = mix.get(UnitStrategy::Pruned.name()).copied().unwrap_or(0);
    let empty = mix.get(UnitStrategy::Empty.name()).copied().unwrap_or(0);
    let chosen_plan = match (graph, filtered, memtable_rows) {
        (true, true, _) => QueryPlanKind::CooperativeFilteredAnn,
        (true, false, true) => QueryPlanKind::HybridExactAnnMerge,
        (true, false, false) => QueryPlanKind::VectorFirstAnn,
        (false, true, _) => QueryPlanKind::PredicateFirstExact,
        (false, false, _) => QueryPlanKind::UnfilteredExactScan,
    };
    let planner_reason = outcome
        .units
        .iter()
        .filter(|unit| !unit.reason.is_empty())
        .map(|unit| {
            format!(
                "unit {:08x} {}: {}",
                unit.unit,
                unit.strategy.name(),
                unit.reason
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    #[allow(clippy::cast_precision_loss)]
    let estimated_selectivity = if filtered && live > 0 {
        matched as f32 / live as f32
    } else {
        1.0
    };
    QueryDiagnostics {
        chosen_plan,
        planner_reason,
        estimated_selectivity,
        units_considered: outcome.units.len(),
        units_pruned: pruned,
        units_scanned: outcome.units.len() - pruned - empty,
        candidates_before_filter: usize::try_from(live).unwrap_or(usize::MAX),
        candidates_after_filter: usize::try_from(matched).unwrap_or(usize::MAX),
        candidates_reranked: outcome.reranked,
        candidates_merged: merged,
        rerank_count: usize::from(outcome.reranked > 0),
        fallback_reason: (!graph).then(|| "no segment has a graph large enough to walk".to_owned()),
        unit_scan_mix: mix,
        stage_timings: (explain == ExplainMode::Profile).then(|| QueryStageTimings {
            planning_micros: outcome.micros[0],
            prefilter_micros: 0,
            candidate_generation_micros: outcome.micros[1],
            postfilter_micros: outcome.micros[3],
            rerank_micros: outcome.micros[2],
            merge_micros: 0,
        }),
    }
}

/// The v1 record id of a key.
#[must_use]
pub fn legacy_id(pk: &PrimaryKey) -> RecordId {
    match pk {
        PrimaryKey::String(value) => RecordId::new(value.clone()),
        PrimaryKey::Int64(value) => RecordId::new(value.to_string()),
    }
}

/// A row's fields as one JSON object: visible `$extra` keys, then typed scalar fields under
/// their names.
#[must_use]
pub fn metadata(row: &RowData) -> Map<String, Value> {
    let mut metadata = row.record.extra.clone();
    for (name, value) in &row.record.fields {
        metadata.insert(name.clone(), value.to_json());
    }
    metadata
}

/// A row in the v1 record shape: the key as id, the first vector field as the vector, and
/// [`metadata`].
#[must_use]
pub fn legacy_record(schema: &CollectionSchema, row: &RowData) -> VisibleRecord {
    let vector = schema
        .vectors()
        .first()
        .and_then(|field| row.record.vectors.get(&field.name))
        .cloned()
        .unwrap_or_default();
    VisibleRecord {
        id: legacy_id(&row.record.pk),
        vector,
        metadata: Value::Object(metadata(row)),
        seq_no: row.seq_no,
    }
}

/// Every live row of `view` in the v1 record shape, ordered by key.
///
/// # Errors
///
/// I/O and typed corruption.
pub async fn scan_view(view: &ReadView) -> Result<Vec<VisibleRecord>> {
    let (rows, _) = ops::scroll_view(
        view,
        None,
        &ScrollOrder::Pk,
        u32::MAX,
        Projection::full(),
        None,
    )
    .await?;
    Ok(rows
        .iter()
        .map(|row| legacy_record(view.schema(), row))
        .collect())
}

/// Every live row of `collection` as `options` select the state, in the v1 record shape,
/// ordered by key.
///
/// # Errors
///
/// As [`CollectionReader::read_view`] and [`scan_view`].
pub async fn scan_records<R: CollectionReader + ?Sized>(
    reader: &R,
    collection: &CollectionRef,
    options: ReadOptions,
) -> Result<Vec<VisibleRecord>> {
    let view = reader.read_view(collection, options).await?;
    scan_view(&view).await
}
