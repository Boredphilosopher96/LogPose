//! Query execution over the engine's read interfaces.
//!
//! Every read opens one [`ReadView`] through a [`CollectionReader`] and runs against it, so a
//! query sees one consistent snapshot from its first stage to its last, and a flush or
//! compaction that lands while it runs cannot expire it. The modules:
//!
//! - [`compile`]: filters checked against the schema and compiled to per-unit bitmaps, through
//!   scalar indexes where a unit has one and column or `$extra` scans otherwise.
//! - [`search`](mod@search): planned, staged vector search: per-unit strategy priced by the
//!   [`cost`] model from the exact filter cardinality (exact scan, ACORN-1 style walk, or
//!   admit-only walk over SQ8 codes), units searched in parallel, a resumable cursor that widens
//!   `ef` for short or anti-correlated results and yields to an exact scan past its price, an
//!   exact f32 rerank, and a global heap merge.
//! - [`cost`]: the cost model, in distance computations, graph hops, and bytes touched.
//! - [`explain`]: the plan as an operator tree with estimated and actual work, which
//!   `EXPLAIN` returns.
//! - [`ops`]: get, count, scroll by key, and order by a field, with opaque scroll cursors.
//! - [`resolver`](mod@resolver): the [`RowSetResolver`](logpose_storage::RowSetResolver) the
//!   engine uses for delete-by-filter and update-by-filter.
//!
//! [`query`], [`count_records`], and [`scroll_records`] serve the API's requests (engine plan
//! decision D11) on top of them.

pub mod compile;
pub mod cost;
pub mod explain;
pub mod ops;
pub mod resolver;
pub mod search;

#[cfg(test)]
mod tests;

pub use compile::CompiledFilter;
pub use cost::{CostModel, ExactCause, Force};
pub use explain::{Operator, OperatorStats, PlanNode};
pub use logpose_storage::read::Direction;
pub use logpose_types::filter::{FilterExpr, RangeBounds};
pub use ops::{
    Cursor, CursorKey, InvalidCursor, ScrollOrder, ScrollPage, ScrollRequest, count, count_view,
    filter_digest, get, resolve_view, scroll, scroll_view,
};
pub use resolver::{QueryResolver, resolver};
pub use search::{
    MAX_RERANK_FACTOR, SearchHit, SearchOutcome, SearchRequest, SearchTimings, SearchTuning,
    UnitReport, UnitStrategy, metric_value, search,
};

use logpose_catalog as _;
use logpose_storage::{
    CollectionReader, Projection as RowProjection, ReadOptions, ReadView, SnapshotToken,
};
use logpose_types::{
    CollectionRef, DistanceMetric, LogPoseError, ResourceKind, Snapshot,
    record::{Projection, Record},
    schema::CollectionSchema,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use thiserror::Error;

/// Largest `top_k` of a query and `page_size` of a scroll.
pub const MAX_RESULTS: usize = 10_000;
/// Rows per scroll page when the request names no page size.
pub const DEFAULT_PAGE_SIZE: u32 = 100;
/// Largest `ef` a query may ask for.
pub const MAX_EF: usize = 4_096;

/// Which state a read sees: at most one of `snapshot` and `snapshot_token`, and at most one of
/// `snapshot` and `read_barrier`. A token with a read barrier fails unless the pinned state is at
/// or past the barrier.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadConsistency {
    /// Read exactly this snapshot (the current state, or one of the latest states of the
    /// current manifest generation).
    pub snapshot: Option<Snapshot>,
    /// Fail unless the current state is at or past this snapshot.
    pub read_barrier: Option<Snapshot>,
    /// Read the state this snapshot token pins, and extend its expiry.
    pub snapshot_token: Option<String>,
    /// Pin the state read under a snapshot token, which the response returns.
    pub pin: bool,
}

impl ReadConsistency {
    /// The view options these settings select.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` for a malformed token, or for `snapshot` with `snapshot_token` or
    /// `read_barrier`.
    pub fn options(&self) -> Result<ReadOptions> {
        let token = parse_token(self.snapshot_token.as_deref())?;
        if token.is_some() && self.snapshot.is_some() {
            return Err(invalid(
                "snapshot_token",
                "snapshot and snapshot_token cannot be provided together",
            ));
        }
        if self.snapshot.is_some() && self.read_barrier.is_some() {
            return Err(invalid(
                "read_barrier",
                "snapshot and read_barrier cannot be provided together",
            ));
        }
        Ok(ReadOptions {
            token,
            snapshot: self.snapshot.clone(),
            read_barrier: self.read_barrier.clone(),
            pin: self.pin,
        })
    }
}

/// Parse a snapshot token named in a request field `snapshot_token`.
///
/// # Errors
///
/// `InvalidArgument` at `snapshot_token` for text that is not a token.
pub fn parse_token(token: Option<&str>) -> Result<Option<SnapshotToken>> {
    token
        .filter(|token| !token.is_empty())
        .map(|token| {
            token.parse::<SnapshotToken>().map_err(|error| {
                invalid("snapshot_token", format!("invalid snapshot token: {error}"))
            })
        })
        .transpose()
}

fn invalid(field: &str, message: impl Into<String>) -> QueryError {
    QueryError::Storage(LogPoseError::invalid_field(field, message))
}

/// The vector half of a query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorQuery {
    /// The vector field to search; `None` searches the collection's only vector field.
    #[serde(default)]
    pub field: Option<String>,
    /// The query vector, of the field's dimensions.
    pub values: Vec<f32>,
}

/// Sort direction of an [`OrderBy`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    /// Smallest first.
    #[default]
    Asc,
    /// Largest first.
    Desc,
}

impl From<SortDirection> for Direction {
    fn from(direction: SortDirection) -> Self {
        match direction {
            SortDirection::Asc => Self::Ascending,
            SortDirection::Desc => Self::Descending,
        }
    }
}

/// Order results by a declared scalar field (not an array or JSON). Ties are broken by primary
/// key ascending, and rows without a value come last in both directions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderBy {
    /// The field.
    pub field: String,
    /// The direction; ascending by default.
    #[serde(default)]
    pub direction: SortDirection,
}

/// One search request (engine plan decision D11): a vector search, or without a vector an
/// ordered scan, with a filter, an order, a limit, and a projection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryRequest {
    /// Search this vector; `None` reads rows in `order_by` order (primary key by default).
    pub vector: Option<VectorQuery>,
    /// Only rows matching this filter.
    pub filter: Option<FilterExpr>,
    /// At most one entry. With a vector it reorders the `top_k` hits; without one it is the
    /// scan order.
    pub order_by: Vec<OrderBy>,
    /// Results wanted, 1 to [`MAX_RESULTS`].
    pub top_k: usize,
    /// Fields each hit returns, as `GetRecords` projects them; empty returns every scalar field
    /// and `$extra` key, no vectors.
    pub output_fields: Vec<String>,
    /// Beam width of graph walks, 1 to [`MAX_EF`]; only with a vector.
    pub ef: Option<usize>,
    /// Candidates per result each unit reranks exactly in f32, 1 to [`MAX_RERANK_FACTOR`]
    /// (default 4); only with a vector.
    pub rerank_factor: Option<usize>,
    /// Planner diagnostics to return.
    pub explain: ExplainMode,
    /// Which state the query reads.
    pub read: ReadConsistency,
}

/// Diagnostics verbosity requested by the caller.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainMode {
    /// Do not emit plan diagnostics.
    #[default]
    None,
    /// Emit the plan tree with estimated and actual work per operator.
    Plan,
    /// Emit the plan plus measured times per operator and per stage.
    Profile,
}

/// The plan a query used, summarized over its units.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryPlanKind {
    /// Every unit was scanned exactly and no filter was given.
    UnfilteredExactScan,
    /// Every unit was scanned exactly over its filter bitmap.
    PredicateFirstExact,
    /// Graph walks over segments, no filter, no memtable rows.
    VectorFirstAnn,
    /// Filtered graph walks (ACORN-1 style or admit-only) over segments.
    CooperativeFilteredAnn,
    /// Graph walks over segments merged with exact memtable scans.
    HybridExactAnnMerge,
    /// No vector: rows read in `order_by` order through the filter.
    OrderedScan,
}

/// Per-stage timings reported for profile mode.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryStageTimings {
    /// Planning and the first fetch (filter sections and vector indexes).
    pub planning_micros: u64,
    /// Filter bitmaps (`BitmapProbe` and `MaskDeletes`), CPU time summed over units.
    pub prefilter_micros: u64,
    /// The per-unit stage: filter bitmaps, scans, and walks, units in parallel (an ordered
    /// scan: the whole scan).
    pub candidate_generation_micros: u64,
    /// Reading the result rows.
    pub postfilter_micros: u64,
    /// The exact f32 rerank, with any fetch of vector pages.
    pub rerank_micros: u64,
    /// The global heap merge of the units' results.
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
    /// The plan as an operator tree, with estimated and actual work per operator (times only
    /// in profile mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<Box<PlanNode>>,
    /// The plan tree rendered as text, one operator per line.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plan_text: String,
}

/// One query result.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryHit {
    /// The record, projected to the request's output fields.
    pub record: Record,
    /// The metric value of a vector search (similarity for dot and cosine, distance for L2);
    /// `None` for an ordered scan.
    pub score: Option<f32>,
}

/// The result of a [`QueryRequest`].
#[derive(Clone, Debug, PartialEq)]
pub struct QueryResponse {
    /// The vector field searched; `None` for an ordered scan.
    pub vector_field: Option<String>,
    /// That field's metric.
    pub metric: Option<DistanceMetric>,
    /// The requested `top_k`.
    pub top_k: usize,
    /// The state the query read.
    pub snapshot: Snapshot,
    /// The hits, best first (or in scan order).
    pub hits: Vec<QueryHit>,
    /// Planner diagnostics, when requested.
    pub diagnostics: Option<QueryDiagnostics>,
    /// The token pinning the state read, when the request pinned one or read through one.
    pub snapshot_token: Option<String>,
}

impl QueryResponse {
    /// The response as natural JSON, the REST form: records as documents typed by `schema`
    /// (the schema of the state read), each hit `{"score": ..., "record": {...}}`.
    #[must_use]
    pub fn to_json(&self, schema: &CollectionSchema) -> Value {
        let mut object = Map::new();
        if let Some(field) = &self.vector_field {
            object.insert("vector_field".to_owned(), Value::from(field.as_str()));
        }
        if let Some(metric) = self.metric {
            object.insert(
                "metric".to_owned(),
                serde_json::to_value(metric).unwrap_or(Value::Null),
            );
        }
        object.insert("top_k".to_owned(), Value::from(self.top_k));
        object.insert("returned".to_owned(), Value::from(self.hits.len()));
        object.insert(
            "snapshot".to_owned(),
            serde_json::to_value(&self.snapshot).unwrap_or(Value::Null),
        );
        object.insert(
            "hits".to_owned(),
            Value::Array(
                self.hits
                    .iter()
                    .map(|hit| {
                        let mut entry = Map::new();
                        if let Some(score) = hit.score {
                            entry.insert("score".to_owned(), Value::from(score));
                        }
                        entry.insert("record".to_owned(), hit.record.to_json(schema));
                        Value::Object(entry)
                    })
                    .collect(),
            ),
        );
        if let Some(diagnostics) = &self.diagnostics {
            object.insert(
                "diagnostics".to_owned(),
                serde_json::to_value(diagnostics).unwrap_or(Value::Null),
            );
        }
        if let Some(token) = &self.snapshot_token {
            object.insert("snapshot_token".to_owned(), Value::from(token.as_str()));
        }
        Value::Object(object)
    }
}

/// A count of the records matching a filter.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CountRecordsRequest {
    /// Only rows matching this filter; `None` counts every live record.
    pub filter: Option<FilterExpr>,
    /// Which state the count reads.
    pub read: ReadConsistency,
}

/// The result of a [`CountRecordsRequest`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CountRecordsResponse {
    /// Live records matching the filter.
    pub count: u64,
    /// The state the count read.
    pub snapshot: Snapshot,
    /// The token pinning that state, when the request pinned one or read through one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_token: Option<String>,
}

/// One page of a scroll through the records matching a filter.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScrollRecordsRequest {
    /// Only rows matching this filter. Every page of a scroll must send the same filter.
    pub filter: Option<FilterExpr>,
    /// At most one entry; primary key ascending when empty. Every page must send the same
    /// order.
    pub order_by: Vec<OrderBy>,
    /// Rows per page, 1 to [`MAX_RESULTS`]; [`DEFAULT_PAGE_SIZE`] when `None`.
    pub page_size: Option<u32>,
    /// Fields each record returns; empty returns every scalar field and `$extra` key, no
    /// vectors.
    pub output_fields: Vec<String>,
    /// Continue after the page that returned this cursor.
    pub cursor: Option<String>,
    /// A first page reads the state this token pins (from a pinned count or query, say)
    /// instead of the current state. Not allowed with a cursor, which carries its own.
    pub snapshot_token: Option<String>,
}

/// The result of a [`ScrollRecordsRequest`].
#[derive(Clone, Debug, PartialEq)]
pub struct ScrollRecordsResponse {
    /// The page's records, in order.
    pub records: Vec<Record>,
    /// The cursor of the next page; `None` after the last page.
    pub next_cursor: Option<String>,
    /// The state the page read; every page of one scroll reads the same state.
    pub snapshot: Snapshot,
}

impl ScrollRecordsResponse {
    /// The page as natural JSON, the REST form: records as documents typed by `schema`, and
    /// `next_cursor` (null after the last page).
    #[must_use]
    pub fn to_json(&self, schema: &CollectionSchema) -> Value {
        let mut object = Map::new();
        object.insert(
            "records".to_owned(),
            Value::Array(
                self.records
                    .iter()
                    .map(|record| record.to_json(schema))
                    .collect(),
            ),
        );
        object.insert(
            "next_cursor".to_owned(),
            self.next_cursor.as_deref().map_or(Value::Null, Value::from),
        );
        object.insert(
            "snapshot".to_owned(),
            serde_json::to_value(&self.snapshot).unwrap_or(Value::Null),
        );
        Value::Object(object)
    }
}

/// A read result with the schema of the state it read, which renders its records.
#[derive(Clone, Debug, PartialEq)]
pub struct WithSchema<T> {
    /// The schema of the state read.
    pub schema: Arc<CollectionSchema>,
    /// The result.
    pub value: T,
}

/// Query-scoped error returned when a request cannot be served.
#[derive(Debug, Error)]
pub enum QueryError {
    /// Query vector dimensionality must match the searched vector field.
    #[error("query vector dimension mismatch: expected {expected}, found {actual}")]
    RequestVectorDimensionMismatch {
        /// Expected dimensionality.
        expected: usize,
        /// Actual query dimensionality.
        actual: usize,
    },
    /// Storage and validation failures, surfaced as they are.
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
                    field: "vector.values".to_owned(),
                    record_id: None,
                    expected,
                    actual,
                }
            }
            QueryError::Storage(error) => error,
        }
    }
}

/// Open the view `read` selects, reporting a missing database as the missing collection.
async fn open_view(
    reader: &dyn CollectionReader,
    collection: &CollectionRef,
    read: &ReadConsistency,
) -> Result<ReadView> {
    reader
        .read_view(collection, read.options()?)
        .await
        .map_err(|error| qualify_collection_error(error, collection).into())
}

/// The single order of a request's `order_by`.
fn single_order(order_by: &[OrderBy]) -> Result<Option<&OrderBy>> {
    match order_by {
        [] => Ok(None),
        [order] => Ok(Some(order)),
        _ => Err(invalid(
            "order_by[1]",
            "order_by takes one field; ties are broken by primary key",
        )),
    }
}

/// Serve one [`QueryRequest`]: open one view (current, token-pinned, or an exact snapshot,
/// pinning it when asked), search it (or scan it in order), and return the hits with the
/// snapshot read.
///
/// The view is held for the whole query, so a query that names no snapshot never fails because
/// a flush or compaction published while it ran.
///
/// # Errors
///
/// `NotFound` for an unknown collection, `InvalidArgument` naming the request field (filter
/// errors at their node's path), `DimensionMismatch` at `vector.values`, `SnapshotExpired` for
/// an expired token or snapshot, `ReadBarrierNotSatisfied`, I/O, and typed corruption.
pub async fn query(
    reader: &dyn CollectionReader,
    collection: &CollectionRef,
    request: QueryRequest,
) -> Result<WithSchema<QueryResponse>> {
    let started = Instant::now();
    if request.top_k == 0 || request.top_k > MAX_RESULTS {
        return Err(invalid(
            "top_k",
            format!("top_k must be 1 to {MAX_RESULTS}"),
        ));
    }
    let order = single_order(&request.order_by)?;
    if let Some(ef) = request.ef {
        if request.vector.is_none() {
            return Err(invalid("ef", "ef applies only to a vector search"));
        }
        if ef == 0 || ef > MAX_EF {
            return Err(invalid("ef", format!("ef must be 1 to {MAX_EF}")));
        }
    }
    if let Some(factor) = request.rerank_factor {
        if request.vector.is_none() {
            return Err(invalid(
                "rerank_factor",
                "rerank_factor applies only to a vector search",
            ));
        }
        if factor == 0 || factor > MAX_RERANK_FACTOR {
            return Err(invalid(
                "rerank_factor",
                format!("rerank_factor must be 1 to {MAX_RERANK_FACTOR}"),
            ));
        }
    }
    let view = open_view(reader, collection, &request.read).await?;
    let schema = Arc::clone(view.schema());
    let projection = Projection::resolve(&schema, &request.output_fields)?;
    let order = order
        .map(|order| {
            ops::order_field(&schema, &order.field, "order_by[0].field")
                .map(|_| (order.field.clone(), Direction::from(order.direction)))
        })
        .transpose()?;
    let rows = RowProjection {
        vectors: projection.selects_vectors(&schema),
        seq_no: false,
    };
    let response = match request.vector {
        Some(vector) => {
            let field = match vector.field {
                Some(field) => field,
                None => match schema.vectors() {
                    [only] => only.name.clone(),
                    fields => {
                        return Err(invalid(
                            "vector.field",
                            format!(
                                "the collection has {} vector fields; name the one to search",
                                fields.len()
                            ),
                        ));
                    }
                },
            };
            let search_request = SearchRequest {
                field: Some(field.clone()),
                vector: vector.values,
                top_k: request.top_k,
                filter: request.filter.clone(),
                ef: request.ef,
                projection: rows,
                tuning: SearchTuning {
                    rerank_factor: request
                        .rerank_factor
                        .unwrap_or(SearchTuning::default().rerank_factor),
                    ..SearchTuning::default()
                },
            };
            let outcome = search::search(&view, &search_request).await?;
            let metric = schema.vector_field(&field).map(|field| field.metric);
            let mut hits = outcome.hits.clone();
            if let Some((name, direction)) = &order {
                hits.sort_by(|left, right| {
                    ops::compare_ordered(
                        (
                            ops::order_key(left.row.record.fields.get(name)).as_ref(),
                            &left.row.record.pk,
                        ),
                        (
                            ops::order_key(right.row.record.fields.get(name)).as_ref(),
                            &right.row.record.pk,
                        ),
                        *direction,
                    )
                });
            }
            QueryResponse {
                vector_field: Some(field),
                metric,
                top_k: request.top_k,
                snapshot: view.snapshot(),
                hits: hits
                    .into_iter()
                    .map(|hit| QueryHit {
                        record: projection.apply(hit.row.record),
                        score: Some(hit.value),
                    })
                    .collect(),
                diagnostics: match request.explain {
                    ExplainMode::None => None,
                    explain => Some(diagnostics(&outcome, request.filter.is_some(), explain)),
                },
                snapshot_token: view.token().map(ToString::to_string),
            }
        }
        None => {
            let scroll_order = match &order {
                None => ScrollOrder::Pk,
                Some((field, direction)) => ScrollOrder::Field {
                    field: field.clone(),
                    direction: *direction,
                },
            };
            let limit = u32::try_from(request.top_k).unwrap_or(u32::MAX);
            let (rows, _, plan) = ops::scroll_view_explained(
                &view,
                request.filter.as_ref(),
                &scroll_order,
                limit,
                rows,
                None,
            )
            .await?;
            let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            QueryResponse {
                vector_field: None,
                metric: None,
                top_k: request.top_k,
                snapshot: view.snapshot(),
                hits: rows
                    .into_iter()
                    .map(|row| QueryHit {
                        record: projection.apply(row.record),
                        score: None,
                    })
                    .collect(),
                diagnostics: match request.explain {
                    ExplainMode::None => None,
                    explain => Some(scan_diagnostics(
                        &view,
                        &scroll_order,
                        explain,
                        micros,
                        plan,
                    )),
                },
                snapshot_token: view.token().map(ToString::to_string),
            }
        }
    };
    Ok(WithSchema {
        schema,
        value: response,
    })
}

/// Count the live records matching a filter, in the state `request.read` selects.
///
/// # Errors
///
/// As [`query`].
pub async fn count_records(
    reader: &dyn CollectionReader,
    collection: &CollectionRef,
    request: CountRecordsRequest,
) -> Result<CountRecordsResponse> {
    let view = open_view(reader, collection, &request.read).await?;
    let count = ops::count_view(&view, request.filter.as_ref()).await?;
    Ok(CountRecordsResponse {
        count,
        snapshot: view.snapshot(),
        snapshot_token: view.token().map(ToString::to_string),
    })
}

/// One page of a scroll. A first page reads the current state (or the snapshot its token
/// pins), and when records are left after it, pins that state under a token its cursor carries,
/// so later pages read exactly that state: every record matching the filter then appears
/// exactly once across the pages, whatever is written meanwhile. A scroll that fits in one
/// page pins nothing.
///
/// # Errors
///
/// `InvalidArgument` naming the request field: a malformed cursor or token, a cursor with
/// another filter or order, or with a token, and the checks of [`query`]. `SnapshotExpired`
/// when the cursor's snapshot expired (after the token TTL without a page, 5 minutes by
/// default): restart the scroll. `TooManySnapshots` when the collection holds too many pins.
pub async fn scroll_records(
    reader: &dyn CollectionReader,
    collection: &CollectionRef,
    request: ScrollRecordsRequest,
) -> Result<WithSchema<ScrollRecordsResponse>> {
    let page_size = request.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    if page_size == 0 || page_size as usize > MAX_RESULTS {
        return Err(invalid(
            "page_size",
            format!("page_size must be 1 to {MAX_RESULTS}"),
        ));
    }
    let order = match single_order(&request.order_by)? {
        None => ScrollOrder::Pk,
        Some(order) => ScrollOrder::Field {
            field: order.field.clone(),
            direction: order.direction.into(),
        },
    };
    let cursor = request
        .cursor
        .as_deref()
        .filter(|cursor| !cursor.is_empty())
        .map(|cursor| {
            cursor
                .parse::<Cursor>()
                .map_err(|error| invalid("cursor", error.to_string()))
        })
        .transpose()?;
    let token = parse_token(request.snapshot_token.as_deref())?;
    let mut scroll = ScrollRequest {
        filter: request.filter,
        order,
        limit: page_size,
        projection: RowProjection::default(),
        cursor,
        token,
    };
    let view = reader
        .read_view(collection, ops::scroll_options(&scroll)?)
        .await
        .map_err(|error| qualify_collection_error(error, collection))?;
    let schema = Arc::clone(view.schema());
    let projection = Projection::resolve(&schema, &request.output_fields)?;
    scroll.projection = RowProjection {
        vectors: projection.selects_vectors(&schema),
        seq_no: false,
    };
    let page = ops::scroll_page(&view, scroll).await?;
    Ok(WithSchema {
        schema,
        value: ScrollRecordsResponse {
            records: page
                .rows
                .into_iter()
                .map(|row| projection.apply(row.record))
                .collect(),
            next_cursor: page.next.map(|cursor| cursor.to_string()),
            snapshot: page.snapshot,
        },
    })
}

/// Report any missing resource on the way to a collection (its database, say) as the
/// collection the caller named.
fn qualify_collection_error(error: LogPoseError, collection: &CollectionRef) -> LogPoseError {
    match error {
        LogPoseError::NotFound { .. } => {
            LogPoseError::not_found(ResourceKind::Collection, collection.lookup_name())
        }
        other => other,
    }
}

fn scan_diagnostics(
    view: &ReadView,
    order: &ScrollOrder,
    explain: ExplainMode,
    micros: u64,
    plan: PlanNode,
) -> QueryDiagnostics {
    let order = match order {
        ScrollOrder::Pk => "primary key".to_owned(),
        ScrollOrder::Field { field, direction } => format!("{field} {direction:?}").to_lowercase(),
    };
    QueryDiagnostics {
        chosen_plan: QueryPlanKind::OrderedScan,
        planner_reason: format!("no query vector: rows read in {order} order"),
        estimated_selectivity: 1.0,
        units_considered: view.units().len(),
        units_pruned: 0,
        units_scanned: 0,
        candidates_before_filter: 0,
        candidates_after_filter: 0,
        candidates_reranked: 0,
        candidates_merged: 0,
        rerank_count: 0,
        fallback_reason: None,
        unit_scan_mix: BTreeMap::new(),
        stage_timings: (explain == ExplainMode::Profile).then(|| QueryStageTimings {
            candidate_generation_micros: micros,
            ..QueryStageTimings::default()
        }),
        plan_text: String::new(),
        plan: Some(Box::new(plan)),
    }
    .with_rendered_plan(explain)
}

impl QueryDiagnostics {
    /// Render the plan into `plan_text`; outside profile mode, drop measured times so the plan
    /// depends only on the data.
    fn with_rendered_plan(mut self, explain: ExplainMode) -> Self {
        let profile = explain == ExplainMode::Profile;
        if let Some(plan) = &mut self.plan {
            if !profile {
                clear_times(plan);
            }
            self.plan_text = plan.render(profile);
        }
        self
    }
}

fn clear_times(node: &mut PlanNode) {
    node.actual.micros = 0.0;
    node.estimated.micros = 0.0;
    for child in &mut node.children {
        clear_times(child);
    }
}

/// Why no unit walked a graph, from the causes of the units' exact scans.
fn fallback_reason(units: &[search::UnitReport], filtered: bool) -> String {
    let mut causes = units
        .iter()
        .filter_map(|unit| unit.exact_cause)
        .collect::<Vec<_>>();
    causes.sort_unstable_by_key(|cause| *cause as u8);
    causes.dedup();
    if causes.is_empty() {
        return "no unit had live rows matching the filter".to_owned();
    }
    causes
        .into_iter()
        .map(|cause| match cause {
            ExactCause::Memtable => "memtable rows are scanned exactly",
            ExactCause::NoGraph => {
                "a segment has no graph yet (its index build has not run, or it is too small \
                 for one)"
            }
            ExactCause::FitsBudget => "a segment's matching rows fit the candidates it contributes",
            ExactCause::Cheaper if filtered => {
                "an exact scan was cheaper than a walk at the filter's selectivity"
            }
            ExactCause::Cheaper => "an exact scan was cheaper than a walk for the segment's size",
            ExactCause::WalkAbandoned => {
                "a walk reached the exact scan's price and the segment was scanned exactly"
            }
            ExactCause::Forced => "exact scans were forced",
        })
        .collect::<Vec<_>>()
        .join("; ")
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
        fallback_reason: (!graph).then(|| fallback_reason(&outcome.units, filtered)),
        unit_scan_mix: mix,
        stage_timings: (explain == ExplainMode::Profile).then_some(QueryStageTimings {
            planning_micros: outcome.timings.planning,
            prefilter_micros: outcome.timings.prefilter,
            candidate_generation_micros: outcome.timings.candidates,
            postfilter_micros: outcome.timings.project,
            rerank_micros: outcome.timings.rerank,
            merge_micros: outcome.timings.merge,
        }),
        plan: Some(Box::new(outcome.plan.clone())),
        plan_text: String::new(),
    }
    .with_rendered_plan(explain)
}
