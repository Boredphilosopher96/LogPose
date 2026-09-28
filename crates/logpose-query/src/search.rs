//! Staged vector search over one [`ReadView`].
//!
//! 1. **Plan.** Prune units whose zone maps exclude the filter. For the rest, list the sections
//!    the filter needs and the vector index (graph and SQ8 codes).
//! 2. **Fetch 1** on the I/O pool.
//! 3. **Compute 1**, one `rayon` task per unit on the query pool: compile the filter to the
//!    unit's bitmap `B` of live matching rows, then pick a strategy from `n = |B|` against the
//!    live rows `N` (D6):
//!    - a memtable scans `B` exactly in f32;
//!    - a segment without a usable graph, or with `n` at most the exact-scan threshold (and at
//!      most the candidate budget), scans `B` exactly: over SQ8 codes when it has them, else in
//!      f32 after a second fetch;
//!    - otherwise a graph walk over SQ8 codes: ACORN-1 style when `n / N` is below the ACORN
//!      threshold, else a walk that admits only rows in `B`. The walk is a resumable cursor:
//!      when it returns fewer candidates than the budget, or the rows it visits match the filter
//!      far less often than `n / N` predicts (an anti-correlated filter), it widens `ef` and
//!      continues the same traversal instead of restarting.
//! 4. **Fetch 2**: f32 pages of the SQ8 candidates and of the f32 scans.
//! 5. **Compute 2**: exact f32 distances, then a global top-k heap over every unit.
//! 6. **Project**: read the final rows and order ties by primary key.

use crate::{QueryError, Result, compile::CompiledFilter};
use logpose_index::{
    graph::{FilterStrategy, QueryDistance, RowFilter, SearchScratch},
    kernels,
    sq8::{Sq8Metric, Sq8Query},
};
use logpose_storage::{
    FetchPlan, Projection, ReadView, RowData, SectionNeed, UnitView,
    cache::{FetchReport, PinSet},
    segment_v2::NodeMap,
};
use logpose_types::{
    DistanceMetric, LogPoseError, RowAddr, RowId, UnitId,
    filter::FilterExpr,
    record::PrimaryKey,
    schema::{FieldId, VectorField},
};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    time::Instant,
};

/// Default beam width of a graph walk.
pub const DEFAULT_EF: usize = 64;
/// Candidates each unit contributes per requested result, before the exact rerank.
pub const RERANK_FACTOR: usize = 4;
/// The widest a walk escalates, as a multiple of its starting beam.
pub const MAX_EF_MULTIPLIER: usize = 8;
/// An admit-only walk whose visited rows match the filter less often than this share of the
/// filter's selectivity is treated as anti-correlated and widens its beam (at most twice).
pub const ANTI_CORRELATION_RATIO: f64 = 0.75;

/// Strategy thresholds. The defaults come from `logpose-index` and are provisional until the
/// Phase 5 benchmark calibrates them.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchTuning {
    /// A segment whose filter matches at most this many live rows (or at most the candidate
    /// budget) is scanned exactly instead of walked.
    pub exact_max_matches: usize,
    /// Below this filter selectivity (`n / N`) a walk is ACORN-1 style; at or above it the walk
    /// admits only matching rows.
    pub acorn_selectivity: f64,
    /// Candidates per result each unit contributes to the exact rerank.
    pub rerank_factor: usize,
    /// Whether walks widen `ef` for short or anti-correlated results.
    pub ef_escalation: bool,
}

impl Default for SearchTuning {
    fn default() -> Self {
        Self {
            exact_max_matches: FilterStrategy::EXACT_SCAN_MAX_MATCHES,
            acorn_selectivity: FilterStrategy::ACORN_SELECTIVITY_THRESHOLD,
            rerank_factor: RERANK_FACTOR,
            ef_escalation: true,
        }
    }
}

/// One vector search over a view.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchRequest {
    /// The vector field; `None` searches the schema's first vector field.
    pub field: Option<String>,
    /// The query vector.
    pub vector: Vec<f32>,
    /// Results wanted.
    pub top_k: usize,
    /// Rows must match this filter.
    pub filter: Option<FilterExpr>,
    /// Beam width of graph walks; `None` uses [`DEFAULT_EF`].
    pub ef: Option<usize>,
    /// What to read of each result row.
    pub projection: Projection,
    /// Strategy thresholds.
    pub tuning: SearchTuning,
}

impl SearchRequest {
    /// A top-`top_k` search for `vector` with default settings.
    #[must_use]
    pub fn new(vector: Vec<f32>, top_k: usize) -> Self {
        Self {
            field: None,
            vector,
            top_k,
            filter: None,
            ef: None,
            projection: Projection::scalars(),
            tuning: SearchTuning::default(),
        }
    }
}

/// How one unit was searched.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitStrategy {
    /// Zone maps excluded the filter; nothing was read.
    Pruned,
    /// No live row matched.
    Empty,
    /// A memtable: exact f32 scan.
    MemtableScan,
    /// Exact scan over SQ8 codes, reranked in f32.
    ExactSq8,
    /// Exact f32 scan.
    ExactF32,
    /// Graph walk admitting only matching rows.
    GraphAdmit,
    /// ACORN-1 style filtered graph walk.
    GraphAcorn,
}

impl UnitStrategy {
    /// The stable name used in plan diagnostics.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Pruned => "pruned",
            Self::Empty => "empty",
            Self::MemtableScan => "memtable_scan",
            Self::ExactSq8 => "exact_sq8",
            Self::ExactF32 => "exact_f32",
            Self::GraphAdmit => "graph_admit",
            Self::GraphAcorn => "graph_acorn",
        }
    }

    /// Whether the strategy is approximate.
    #[must_use]
    pub fn is_graph(self) -> bool {
        matches!(self, Self::GraphAdmit | Self::GraphAcorn)
    }
}

/// What `EXPLAIN` records for one unit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnitReport {
    /// The unit.
    pub unit: u32,
    /// Whether it is a memtable.
    pub memtable: bool,
    /// The strategy.
    pub strategy: UnitStrategy,
    /// Live rows matching the filter (`n`).
    pub matched: u64,
    /// Live rows (`N`).
    pub live: u64,
    /// Candidates the unit contributed to the rerank.
    pub candidates: usize,
    /// Final beam width of a walk (0 otherwise).
    pub ef: usize,
    /// Rows a walk visited (0 otherwise).
    pub visited: u64,
    /// Times a walk widened its beam.
    pub escalations: u32,
    /// Why the strategy was chosen.
    pub reason: String,
}

/// One search result.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    /// The row's metric value (similarity for dot and cosine, distance for L2).
    pub value: f32,
    /// The row, as projected.
    pub row: RowData,
}

/// Results and diagnostics of a search.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchOutcome {
    /// Up to `top_k` hits, best first; ties by primary key ascending.
    pub hits: Vec<SearchHit>,
    /// Per unit strategy and counts.
    pub units: Vec<UnitReport>,
    /// Cold reads of the fetch stages.
    pub fetch: FetchReport,
    /// Candidates entering the exact rerank.
    pub reranked: usize,
    /// Time spent planning and in the first fetch, compute, second fetch plus rerank, and
    /// projection stages, in microseconds.
    pub micros: [u64; 4],
}

/// A candidate: its unit, row, distance (lower is closer), and key, when the unit's keys are
/// at hand.
///
/// Results with equal values are ordered by key, so every cut over exact distances orders
/// rows tied in distance by key too: the kept set is then the one a global sort by
/// `(distance, key)` would keep, however many rows tie, and it never grows past the cut's
/// limit. Keys are read for memtables and for segments scanned exactly in f32 (whose key
/// sections fetch 2 loads), and only for rows a cut may keep. Candidates of SQ8 and graph
/// stages, whose order is approximate anyway, carry no key and order after keyed ones at an
/// equal distance, then by unit and row.
#[derive(Clone, Debug)]
struct Candidate {
    unit: UnitId,
    row: RowId,
    distance: f32,
    key: Option<PrimaryKey>,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| match (&self.key, &other.key) {
                (Some(left), Some(right)) => compare_keys(left, right),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            })
            .then(self.unit.cmp(&other.unit))
            .then(self.row.cmp(&other.row))
    }
}

/// Keeps the `limit` smallest candidates in [`Candidate`] order.
struct TopK {
    limit: usize,
    heap: BinaryHeap<Candidate>,
}

impl TopK {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit + 1),
        }
    }

    /// Offer `row` of `unit` at `distance`. `key` is called only when the row may be kept, so
    /// a scan reads the keys of the rows that enter the heap, not of every row it scores.
    fn offer(
        &mut self,
        unit: UnitId,
        row: RowId,
        distance: f32,
        key: impl FnOnce(RowId) -> Option<PrimaryKey>,
    ) {
        if self.limit == 0 || !distance.is_finite() {
            return;
        }
        if self.heap.len() >= self.limit
            && self
                .heap
                .peek()
                .is_some_and(|worst| distance.total_cmp(&worst.distance).is_gt())
        {
            return;
        }
        self.push(Candidate {
            unit,
            row,
            distance,
            key: key(row),
        });
    }

    fn push(&mut self, candidate: Candidate) {
        if self.limit == 0 || !candidate.distance.is_finite() {
            return;
        }
        if self.heap.len() < self.limit {
            self.heap.push(candidate);
        } else if self.heap.peek().is_some_and(|worst| candidate < *worst) {
            self.heap.pop();
            self.heap.push(candidate);
        }
    }

    fn into_sorted(self) -> Vec<Candidate> {
        self.heap.into_sorted_vec()
    }
}

/// What compute 1 produced for one unit.
enum UnitOutput {
    /// Exact distances, final.
    Exact(Vec<Candidate>),
    /// Approximate distances to rerank in f32.
    Approx(Vec<Candidate>),
    /// Rows to scan exactly in f32 after a second fetch.
    Scan(RoaringBitmap),
    /// Nothing.
    None,
}

/// Distances in one metric, lower is closer.
#[derive(Clone, Copy, Debug)]
struct Metric {
    metric: DistanceMetric,
}

impl Metric {
    /// Distance between the (normalized, for cosine) query and a stored vector.
    fn distance(self, query: &[f32], vector: &[f32]) -> f32 {
        if query.len() != vector.len() {
            return f32::INFINITY;
        }
        match self.metric {
            DistanceMetric::L2 => kernels::l2_squared(query, vector),
            DistanceMetric::Cosine | DistanceMetric::Dot => -kernels::dot(query, vector),
        }
    }

    fn sq8(self) -> Sq8Metric {
        match self.metric {
            DistanceMetric::L2 => Sq8Metric::L2Squared,
            DistanceMetric::Cosine | DistanceMetric::Dot => Sq8Metric::Dot,
        }
    }
}

/// The metric value the API reports, computed exactly as the v1 read path did: similarity
/// for dot and cosine (cosine divides by both norms), Euclidean distance for L2.
#[must_use]
pub fn metric_value(metric: DistanceMetric, query: &[f32], candidate: &[f32]) -> f32 {
    match metric {
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
    }
}

/// Order two metric values best first.
#[must_use]
pub fn compare_values(metric: DistanceMetric, left: f32, right: f32) -> Ordering {
    match metric {
        DistanceMetric::Cosine | DistanceMetric::Dot => right.total_cmp(&left),
        DistanceMetric::L2 => left.total_cmp(&right),
    }
}

/// The key order of results with equal values.
#[must_use]
pub fn compare_keys(left: &PrimaryKey, right: &PrimaryKey) -> Ordering {
    left.cmp(right)
}

/// Resolve the searched vector field.
fn vector_field<'a>(view: &'a ReadView, request: &SearchRequest) -> Result<&'a VectorField> {
    let schema = view.schema();
    let field = match &request.field {
        Some(name) => schema.vector_field(name).ok_or_else(|| {
            QueryError::Storage(LogPoseError::invalid_field(
                "vector.field",
                format!("'{name}' is not a vector field of the collection"),
            ))
        })?,
        None => schema.vectors().first().ok_or_else(|| {
            QueryError::Storage(LogPoseError::invalid_field(
                "vector",
                "the collection has no vector field",
            ))
        })?,
    };
    if request.vector.len() != field.dimensions as usize {
        return Err(QueryError::RequestVectorDimensionMismatch {
            expected: field.dimensions as usize,
            actual: request.vector.len(),
        });
    }
    if request.vector.iter().any(|value| !value.is_finite()) {
        return Err(QueryError::Storage(LogPoseError::invalid_field(
            "vector",
            "query vector components must be finite",
        )));
    }
    Ok(field)
}

/// Shared inputs of the compute stages.
struct Context {
    field: FieldId,
    metric: Metric,
    /// The query, normalized for cosine.
    query: Vec<f32>,
    /// The query as given, for reported values.
    raw_query: Vec<f32>,
    top_k: usize,
    budget: usize,
    ef: usize,
    tuning: SearchTuning,
    filter: Option<CompiledFilter>,
}

/// Run a vector search over `view`.
///
/// # Errors
///
/// Invalid requests ([`QueryError::RequestVectorDimensionMismatch`],
/// [`QueryError::InvalidPredicate`], `InvalidArgument`), I/O and typed corruption.
pub async fn search(view: &ReadView, request: &SearchRequest) -> Result<SearchOutcome> {
    let started = Instant::now();
    let field = vector_field(view, request)?;
    let metric = Metric {
        metric: field.metric,
    };
    let mut query = request.vector.clone();
    if field.metric == DistanceMetric::Cosine && !kernels::normalize_in_place(&mut query) {
        query.fill(0.0);
    }
    let filter = request
        .filter
        .as_ref()
        .map(|filter| CompiledFilter::compile(view.schema(), filter))
        .transpose()?;
    let top_k = request.top_k;
    let budget = top_k
        .saturating_mul(request.tuning.rerank_factor.max(1))
        .max(top_k);
    let context = std::sync::Arc::new(Context {
        field: field.id,
        metric,
        query,
        raw_query: request.vector.clone(),
        top_k,
        budget,
        ef: request.ef.unwrap_or(DEFAULT_EF).max(budget).max(1),
        tuning: request.tuning,
        filter,
    });
    if top_k == 0 {
        return Ok(SearchOutcome {
            hits: Vec::new(),
            units: Vec::new(),
            fetch: FetchReport::default(),
            reranked: 0,
            micros: [0; 4],
        });
    }

    // Plan and fetch 1.
    let mut plan = FetchPlan::default();
    let mut pruned = Vec::new();
    for unit in view.units() {
        if unit.is_memtable() {
            continue;
        }
        if let Some(filter) = &context.filter {
            if !filter.may_match(&unit) {
                pruned.push(unit.id());
                continue;
            }
            for need in filter.needs(&unit) {
                plan.push(unit.id(), need);
            }
        }
        if unit.has_vector_index(context.field, false) || unit.has_vector_index(context.field, true)
        {
            plan.push(unit.id(), SectionNeed::VectorIndex(context.field));
        }
    }
    let (pins, mut fetch) = view.fetch(&plan).await?;
    let plan_micros = elapsed(started);

    // Compute 1: per unit, in parallel on the query pool.
    let compute_started = Instant::now();
    let first_context = std::sync::Arc::clone(&context);
    let first_pins = std::sync::Arc::new(pins);
    let pins_for_compute = std::sync::Arc::clone(&first_pins);
    let pruned_units = pruned.clone();
    let outputs = view
        .run(move |view| {
            view.units()
                .into_par_iter()
                .map(|unit| {
                    if pruned_units.contains(&unit.id()) {
                        return Ok((pruned_report(&unit), UnitOutput::None));
                    }
                    unit_first_stage(&unit, &pins_for_compute, &first_context)
                })
                .collect::<Vec<logpose_types::Result<_>>>()
        })
        .await?
        .into_iter()
        .collect::<logpose_types::Result<Vec<_>>>()?;
    let compute_micros = elapsed(compute_started);

    // Fetch 2: pages for reranking and for exact f32 scans.
    let rerank_started = Instant::now();
    let mut plan = FetchPlan::default();
    let mut reports = Vec::with_capacity(outputs.len());
    let mut stage_two = Vec::new();
    for (report, output) in outputs {
        let unit = UnitId(report.unit);
        match &output {
            UnitOutput::Approx(candidates) => {
                let rows: RoaringBitmap =
                    candidates.iter().map(|candidate| candidate.row).collect();
                plan.push(unit, SectionNeed::VectorRows(context.field, rows));
            }
            UnitOutput::Scan(rows) => {
                plan.push(unit, SectionNeed::VectorRows(context.field, rows.clone()));
                // Keys order the rows an exact cut keeps at its boundary.
                plan.push(unit, SectionNeed::Pk);
            }
            UnitOutput::Exact(_) | UnitOutput::None => {}
        }
        reports.push(report);
        stage_two.push((unit, output));
    }
    let (rerank_pins, rerank_fetch) = view.fetch(&plan).await?;
    fetch.merge(&rerank_fetch);
    drop(first_pins);

    // Compute 2: exact distances and the global top-k.
    let second_context = std::sync::Arc::clone(&context);
    let limit = context.top_k.saturating_mul(2).max(context.top_k + 8);
    let (finalists, reranked) = view
        .run(
            move |view| -> logpose_types::Result<(Vec<(Candidate, f32)>, usize)> {
                let units: HashMap<UnitId, UnitView<'_>> = view
                    .units()
                    .into_iter()
                    .map(|unit| (unit.id(), unit))
                    .collect();
                let per_unit = stage_two
                    .into_par_iter()
                    .map(|(unit, output)| {
                        let Some(unit) = units.get(&unit) else {
                            return Ok((Vec::new(), 0));
                        };
                        exact_stage(unit, output, &rerank_pins, &second_context)
                    })
                    .collect::<Vec<logpose_types::Result<_>>>();
                let mut top = TopK::new(limit);
                let mut reranked = 0;
                let mut vectors: HashMap<(UnitId, RowId), f32> = HashMap::new();
                for result in per_unit {
                    let (candidates, count) = result?;
                    reranked += count;
                    for (candidate, value) in candidates {
                        vectors.insert((candidate.unit, candidate.row), value);
                        top.push(candidate);
                    }
                }
                Ok((
                    top.into_sorted()
                        .into_iter()
                        .map(|candidate| {
                            let value = vectors
                                .get(&(candidate.unit, candidate.row))
                                .copied()
                                .unwrap_or_default();
                            (candidate, value)
                        })
                        .collect(),
                    reranked,
                ))
            },
        )
        .await??;
    let rerank_micros = elapsed(rerank_started);

    // Project, then order ties by key.
    let project_started = Instant::now();
    let addrs = finalists
        .iter()
        .map(|(candidate, _)| RowAddr {
            unit: candidate.unit,
            row: candidate.row,
        })
        .collect::<Vec<_>>();
    let rows = view.rows(&addrs, request.projection).await?;
    let mut hits = finalists
        .into_iter()
        .zip(rows)
        .map(|((_, value), row)| SearchHit { value, row })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        compare_values(field.metric, left.value, right.value)
            .then_with(|| compare_keys(&left.row.record.pk, &right.row.record.pk))
    });
    hits.truncate(top_k);
    let project_micros = elapsed(project_started);
    Ok(SearchOutcome {
        hits,
        units: reports,
        fetch,
        reranked,
        micros: [plan_micros, compute_micros, rerank_micros, project_micros],
    })
}

fn elapsed(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn pruned_report(unit: &UnitView<'_>) -> UnitReport {
    UnitReport {
        unit: unit.id().0,
        memtable: unit.is_memtable(),
        strategy: UnitStrategy::Pruned,
        matched: 0,
        live: u64::from(unit.live_count()),
        candidates: 0,
        ef: 0,
        visited: 0,
        escalations: 0,
        reason: "zone maps exclude the filter".to_owned(),
    }
}

/// The live rows of `unit` the search may return.
fn allowed_rows(
    unit: &UnitView<'_>,
    pins: &PinSet,
    context: &Context,
) -> logpose_types::Result<RoaringBitmap> {
    match &context.filter {
        Some(filter) => filter.evaluate(unit, pins),
        None => Ok(unit.live()),
    }
}

/// Compute 1 for one unit.
fn unit_first_stage(
    unit: &UnitView<'_>,
    pins: &PinSet,
    context: &Context,
) -> logpose_types::Result<(UnitReport, UnitOutput)> {
    let allowed = allowed_rows(unit, pins, context)?;
    let matched = allowed.len();
    let live = u64::from(unit.live_count());
    let mut report = UnitReport {
        unit: unit.id().0,
        memtable: unit.is_memtable(),
        strategy: UnitStrategy::Empty,
        matched,
        live,
        candidates: 0,
        ef: 0,
        visited: 0,
        escalations: 0,
        reason: String::new(),
    };
    if allowed.is_empty() {
        report.reason = "no live row matches".to_owned();
        return Ok((report, UnitOutput::None));
    }
    if unit.is_memtable() {
        let vectors = unit.vector_rows(context.field, pins)?;
        let pks = unit.pks(pins)?;
        let mut top = TopK::new(context.budget);
        for row in &allowed {
            if let Some(vector) = vectors.get(row)? {
                top.offer(
                    unit.id(),
                    row,
                    context.metric.distance(&context.query, &vector),
                    |row| pks.pk_at(row),
                );
            }
        }
        let candidates = top.into_sorted();
        report.strategy = UnitStrategy::MemtableScan;
        report.candidates = candidates.len();
        report.reason = "memtables are scanned exactly".to_owned();
        return Ok((report, UnitOutput::Exact(candidates)));
    }
    let index = unit.vector_index(context.field, pins)?;
    let exact_limit = context.tuning.exact_max_matches.max(context.budget) as u64;
    let usable_graph = index.graph.as_ref().filter(|_| index.sq8.is_some());
    let Some(sq8) = index.sq8.as_ref() else {
        report.strategy = UnitStrategy::ExactF32;
        report.candidates = usize::try_from(matched).unwrap_or(usize::MAX);
        report.reason = "no SQ8 codes (small segment)".to_owned();
        return Ok((report, UnitOutput::Scan(allowed)));
    };
    let section = sq8.section();
    let dims = section.params().dims();
    let sq8_query = section
        .params()
        .query(context.metric.sq8(), &context.query)
        .map_err(|error| LogPoseError::internal(format!("sq8 query: {error}")))?;
    let codes = sq8.codes();
    let estimate = |row: RowId| -> f32 {
        let start = row as usize * dims;
        codes
            .get(start..start + dims)
            .map_or(f32::INFINITY, |code| sq8_distance(&sq8_query, code))
    };

    match usable_graph {
        Some(graph) if matched > exact_limit => {
            let selectivity = matched as f64 / live.max(1) as f64;
            let filtered = context.filter.is_some() && matched < live;
            let strategy = if filtered && selectivity < context.tuning.acorn_selectivity {
                report.strategy = UnitStrategy::GraphAcorn;
                FilterStrategy::acorn()
            } else {
                report.strategy = UnitStrategy::GraphAdmit;
                FilterStrategy::Admit
            };
            let admit_only = strategy == FilterStrategy::Admit;
            report.reason = format!(
                "{matched} matching rows exceed the exact-scan limit {exact_limit}; selectivity \
                 {selectivity:.3}"
            );
            let node_filter = NodeFilter {
                rows: &allowed,
                nodes: &graph.nodes,
                all: matched == u64::from(unit.row_count()),
                count: matched as usize,
            };
            let distance = NodeDistance {
                nodes: &graph.nodes,
                estimate: &estimate,
            };
            let (candidates, ef, visited, escalations) = SCRATCH.with(|scratch| {
                let mut scratch = scratch.borrow_mut();
                let mut cursor =
                    graph
                        .graph
                        .cursor(&distance, &node_filter, strategy, &mut scratch);
                let mut ef = context.ef;
                cursor.advance(ef);
                let max_ef = context.ef.saturating_mul(MAX_EF_MULTIPLIER);
                let mut escalations = 0_u32;
                // A filtered ACORN walk probes convergence: it widens once and keeps widening
                // while its best `k` still change. Over an anti-correlated filter the first
                // beam settles on matching rows far from the query and wider beams keep finding
                // closer ones; over a filter independent of the query the probe changes nothing.
                let mut probing = filtered && !admit_only;
                while context.tuning.ef_escalation && ef < max_ef && !cursor.is_exhausted() {
                    let found = cursor.top_k(context.budget).len();
                    let stats = cursor.stats();
                    let short = found < context.budget;
                    // An admit-only walk over an anti-correlated filter meets matching rows
                    // far less often than the filter's selectivity predicts: the query's
                    // neighborhood holds few of them, so the first beam settles on distant ones.
                    let anti_correlated = admit_only && filtered && stats.visited > 0 && {
                        let admitted = stats.visited.saturating_sub(stats.filtered_out);
                        let local = admitted as f64 / stats.visited as f64;
                        local < ANTI_CORRELATION_RATIO * selectivity
                    };
                    let widen = short || probing || (anti_correlated && escalations < 2);
                    if !widen {
                        break;
                    }
                    let before = probing.then(|| best_rows(&cursor.top_k(context.top_k)));
                    ef = ef.saturating_mul(2).min(max_ef);
                    escalations += 1;
                    cursor.advance(ef);
                    if let Some(before) = before
                        && before == best_rows(&cursor.top_k(context.top_k))
                    {
                        probing = false;
                    }
                }
                // A node stands for every row with its vector; the cut bounds the candidates
                // when very many rows share one.
                let mut top = TopK::new(context.budget);
                for neighbor in cursor.top_k(context.budget) {
                    for row in graph.nodes.rows(neighbor.row) {
                        if allowed.contains(row) {
                            top.offer(unit.id(), row, neighbor.distance, |_| None);
                        }
                    }
                }
                (top.into_sorted(), ef, cursor.stats().visited, escalations)
            });
            report.candidates = candidates.len();
            report.ef = ef;
            report.visited = visited;
            report.escalations = escalations;
            Ok((report, UnitOutput::Approx(candidates)))
        }
        _ => {
            let mut top = TopK::new(context.budget);
            for row in &allowed {
                top.offer(unit.id(), row, estimate(row), |_| None);
            }
            let candidates = top.into_sorted();
            report.strategy = UnitStrategy::ExactSq8;
            report.candidates = candidates.len();
            report.reason = if index.graph.is_some() {
                format!("{matched} matching rows are within the exact-scan limit {exact_limit}")
            } else {
                "no graph (below the graph threshold)".to_owned()
            };
            Ok((report, UnitOutput::Approx(candidates)))
        }
    }
}

/// The node ids of a walk's best results, for convergence checks.
fn best_rows(neighbors: &[logpose_index::graph::Neighbor]) -> Vec<u32> {
    let mut rows = neighbors
        .iter()
        .map(|neighbor| neighbor.row)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

/// SQ8 estimate as a distance, clamped: an estimate that overflowed is "far".
fn sq8_distance(query: &Sq8Query, code: &[u8]) -> f32 {
    let estimate = query.estimate(code);
    let distance = match query.metric() {
        Sq8Metric::Dot => -estimate,
        Sq8Metric::L2Squared => estimate,
    };
    if distance.is_finite() {
        distance
    } else {
        f32::MAX
    }
}

/// Compute 2 for one unit: exact distances, with each candidate's reported metric value.
fn exact_stage(
    unit: &UnitView<'_>,
    output: UnitOutput,
    pins: &PinSet,
    context: &Context,
) -> logpose_types::Result<(Vec<(Candidate, f32)>, usize)> {
    let rows_to_score: Vec<RowId> = match &output {
        UnitOutput::Exact(candidates) | UnitOutput::Approx(candidates) => {
            candidates.iter().map(|candidate| candidate.row).collect()
        }
        UnitOutput::Scan(rows) => rows.iter().collect(),
        UnitOutput::None => return Ok((Vec::new(), 0)),
    };
    let reranked = if matches!(output, UnitOutput::Exact(_)) {
        0
    } else {
        rows_to_score.len()
    };
    let vectors = unit.vector_rows(context.field, pins)?;
    // A memtable's keys are always at hand, a segment's when fetch 2 loaded them (exact f32
    // scans); SQ8 and graph candidates rerank without them.
    let pks = unit.pks(pins).ok();
    let mut top = TopK::new(context.budget);
    for row in rows_to_score {
        let Some(vector) = vectors.get(row)? else {
            continue;
        };
        let distance = context.metric.distance(&context.query, &vector);
        top.offer(unit.id(), row, distance, |row| {
            pks.as_ref().and_then(|pks| pks.pk_at(row))
        });
    }
    // The reported values of the kept rows only.
    let kept = top.into_sorted();
    let mut candidates = Vec::with_capacity(kept.len());
    for candidate in kept {
        let value = vectors.get(candidate.row)?.map_or(0.0, |vector| {
            metric_value(context.metric.metric, &context.raw_query, &vector)
        });
        candidates.push((candidate, value));
    }
    Ok((candidates, reranked))
}

thread_local! {
    /// Walk scratch reused by every search on a query-pool thread.
    static SCRATCH: RefCell<SearchScratch> = RefCell::new(SearchScratch::new());
}

/// Admits graph nodes with at least one row in the allowed set.
struct NodeFilter<'a> {
    rows: &'a RoaringBitmap,
    nodes: &'a NodeMap,
    all: bool,
    count: usize,
}

impl RowFilter for NodeFilter<'_> {
    fn contains(&self, node: u32) -> bool {
        self.all || self.nodes.rows(node).any(|row| self.rows.contains(row))
    }

    fn cardinality_hint(&self) -> Option<usize> {
        Some(self.count)
    }
}

/// SQ8 distance from the query to a node's vector (its first row's code).
struct NodeDistance<'a, F: Fn(RowId) -> f32> {
    nodes: &'a NodeMap,
    estimate: &'a F,
}

impl<F: Fn(RowId) -> f32> QueryDistance for NodeDistance<'_, F> {
    fn distance(&self, node: u32) -> f32 {
        self.nodes
            .first_row(node)
            .map_or(f32::MAX, |row| (self.estimate)(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyed(row: RowId) -> Option<PrimaryKey> {
        Some(PrimaryKey::from(format!("k{:05}", 99_999 - row).as_str()))
    }

    #[test]
    fn a_cut_over_tied_rows_keeps_its_limit_by_key() {
        let mut top = TopK::new(3);
        for row in 0..10_000 {
            top.offer(UnitId(1), row, 1.0, keyed);
        }
        let keys = top
            .into_sorted()
            .into_iter()
            .map(|candidate| candidate.key)
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![keyed(9_999), keyed(9_998), keyed(9_997)]);
    }

    #[test]
    fn a_cut_reads_keys_only_of_rows_it_may_keep() {
        let mut top = TopK::new(2);
        let mut reads = 0;
        for row in 0..1_000 {
            // Rows get farther, so after the first two none can enter the heap.
            top.offer(UnitId(1), row, row as f32, |row| {
                reads += 1;
                keyed(row)
            });
        }
        assert_eq!(reads, 2);
        let rows = top
            .into_sorted()
            .iter()
            .map(|candidate| candidate.row)
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![0, 1]);
    }

    #[test]
    fn keyed_candidates_order_before_unkeyed_ones_at_an_equal_distance() {
        let mut top = TopK::new(2);
        top.offer(UnitId(1), 0, 1.0, |_| None);
        top.offer(UnitId(2), 5, 1.0, keyed);
        top.offer(UnitId(0), 9, 1.0, |_| None);
        top.offer(UnitId(3), 1, 0.5, |_| None);
        let kept = top
            .into_sorted()
            .iter()
            .map(|candidate| (candidate.unit, candidate.row))
            .collect::<Vec<_>>();
        assert_eq!(kept, vec![(UnitId(3), 1), (UnitId(2), 5)]);
    }
}
