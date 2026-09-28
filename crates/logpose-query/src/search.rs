//! Planned, staged vector search over one [`ReadView`] (engine plan D6, Phase 5).
//!
//! 1. **Plan.** Prune units whose zone maps exclude the filter. For the rest, list the sections
//!    the filter needs and the vector index (graph and SQ8 codes), and note which of them are
//!    cold. **Fetch 1** loads them on the I/O pool (a resident unit is pinned without a hop).
//! 2. **Candidates**, one `rayon` task per unit on the query pool, in parallel: compile the
//!    filter to the unit's bitmap `B` of live matching rows (`BitmapProbe`, `MaskDeletes`),
//!    then price the strategies with the [cost model](crate::cost) from the exact `n = |B|`
//!    and run the cheapest: an exact scan of `B` over SQ8 codes (split into morsels scanned in
//!    parallel when `B` is large), or a graph walk over the codes, admit-only or ACORN-1
//!    style. A walk is a resumable cursor that widens `ef` when it comes back short or its
//!    visits contradict the filter's selectivity (an anti-correlated filter); once its work
//!    reaches the exact scan's price it stops and the unit is scanned exactly instead.
//!    Memtables are scanned exactly in f32. Each unit keeps its best `k * rerank_factor`
//!    candidates (`TopK`).
//! 3. **Rerank.** Exact f32 scores and reported values of each unit's candidates, read from
//!    their vector pages, and each unit's best `k` by (value, key). Segments without SQ8
//!    codes are scanned exactly in f32 here.
//! 4. **Merge.** A global k-way heap merge of the units' sorted results to the top `k`.
//! 5. **Project.** Read the `k` final rows.
//!
//! Stages 2 to 5 run as one task on the query pool: each stage first pins what it reads from
//! the buffer cache without I/O ([`ReadView::fetch_resident`]) and hands control back to fetch
//! on the I/O pool only when something is cold, so a query over resident data crosses to the
//! query pool once. The plan, with the planner's estimates and execution's counts per
//! operator, is the [`SearchOutcome::plan`] tree that `EXPLAIN` renders.

use crate::{
    QueryError, Result,
    compile::CompiledFilter,
    cost::{Choice, CostModel, Decision, Force, SegmentShape, Work},
    explain::{Operator, OperatorStats, PlanNode},
};
use logpose_index::{
    graph::{FilterStrategy, QueryDistance, RowFilter, SearchScratch, SearchStats},
    kernels,
    sq8::{Sq8Metric, Sq8Query},
};
use logpose_storage::{
    FetchPlan, Projection, ReadView, Residency, RowData, SectionNeed, UnitView,
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
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    sync::Arc,
    time::Instant,
};

/// Default beam width of a graph walk.
pub const DEFAULT_EF: usize = 64;
/// Candidates each unit contributes per requested result, before the exact rerank.
pub const RERANK_FACTOR: usize = 4;
/// Largest rerank factor a query may ask for.
pub const MAX_RERANK_FACTOR: usize = 64;
/// The widest a walk escalates, as a multiple of its starting beam.
pub const MAX_EF_MULTIPLIER: usize = 8;
/// Rows of `B` per morsel of a parallel exact scan: a scan of more than two morsels splits
/// into morsels scanned in parallel on the query pool.
pub const SCAN_MORSEL_ROWS: u32 = 8_192;
/// Units run their candidates stage in parallel when more than one has more live rows than
/// this; smaller units run inline, where a hand-off to another worker would cost more than
/// they do.
pub const PARALLEL_UNIT_ROWS: u32 = 4_096;
/// Units rerank in parallel when they have more candidates than this in all.
pub const PARALLEL_RERANK_CANDIDATES: usize = 2_048;

/// How a search chooses and runs its strategies.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchTuning {
    /// Candidates per result each unit contributes to the exact rerank.
    pub rerank_factor: usize,
    /// Whether walks widen `ef` for short or anti-correlated results.
    pub ef_escalation: bool,
    /// Restrict the planner to exact scans or walks (tests and benchmarks).
    pub force: Force,
    /// The cost model.
    pub cost: CostModel,
    /// Whether units run in parallel and large exact scans split into morsels scanned in
    /// parallel; `false` runs everything sequentially (same results, for tests).
    pub parallel: bool,
}

impl Default for SearchTuning {
    fn default() -> Self {
        Self {
            rerank_factor: RERANK_FACTOR,
            ef_escalation: true,
            force: Force::Auto,
            cost: CostModel::default(),
            parallel: true,
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
    /// Strategy selection and execution settings.
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
    /// The strategy that produced the unit's candidates.
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
    /// Distance computations of the unit's scan or walk (SQ8 or f32, before the rerank).
    pub distances: u64,
    /// Adjacency lists a walk read (expansions plus two-hop lists).
    pub hops: u64,
    /// Two-hop lists an ACORN walk read (part of `hops`).
    pub two_hops: u64,
    /// Whether a walk stopped at the exact scan's price and the unit was scanned exactly.
    pub walk_abandoned: bool,
    /// Why the strategy was chosen.
    pub reason: String,
}

impl UnitReport {
    fn new(unit: &UnitView<'_>, strategy: UnitStrategy, matched: u64, reason: &str) -> Self {
        Self {
            unit: unit.id().0,
            memtable: unit.is_memtable(),
            strategy,
            matched,
            live: u64::from(unit.live_count()),
            candidates: 0,
            ef: 0,
            visited: 0,
            escalations: 0,
            distances: 0,
            hops: 0,
            two_hops: 0,
            walk_abandoned: false,
            reason: reason.to_owned(),
        }
    }
}

/// Wall time of each stage, in microseconds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchTimings {
    /// Planning and the first fetch.
    pub planning: u64,
    /// Filter bitmaps (`BitmapProbe` and `MaskDeletes`), summed over units (CPU time; units
    /// run in parallel).
    pub prefilter: u64,
    /// The candidates stage (filters, scans, and walks of every unit, in parallel).
    pub candidates: u64,
    /// Exact f32 scores, including any fetch of vector pages.
    pub rerank: u64,
    /// The global merge.
    pub merge: u64,
    /// Reading the final rows, including any fetch.
    pub project: u64,
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
    /// Candidates scored exactly in the rerank.
    pub reranked: usize,
    /// The plan: operators with estimated and actual work.
    pub plan: PlanNode,
    /// Stage timings.
    pub timings: SearchTimings,
}

/// A candidate of an approximate stage: its unit, row, distance (lower is closer), and key,
/// when the unit's keys are at hand.
///
/// Candidates with equal distances are ordered by key, so every cut over exact distances
/// orders rows tied in distance by key too: the kept set is then the one a global sort by
/// `(distance, key)` would keep, however many rows tie, and it never grows past the cut's
/// limit. Keys are read for memtables and for segments scanned exactly in f32, and only for
/// rows a cut may keep. Candidates of SQ8 and graph stages, whose order is approximate
/// anyway, carry no key and order after keyed ones at an equal distance, then by unit and row.
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
            heap: BinaryHeap::with_capacity(limit.min(4_096) + 1),
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

/// An exactly scored row: its rank (the metric value oriented so lower is better), its key
/// when read, and its reported value. Ordered by rank, then key (keyed rows first), then
/// address.
///
/// Keys only break ties, and reading one costs a cache miss or two, so a row's key is read
/// only when its rank ties another's at the final cut ([`Execution::resolve_ties`]).
#[derive(Clone, Debug)]
struct Scored {
    rank: f32,
    key: Option<PrimaryKey>,
    addr: RowAddr,
    value: f32,
}

impl PartialEq for Scored {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Scored {}

impl PartialOrd for Scored {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Scored {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank
            .total_cmp(&other.rank)
            .then_with(|| match (&self.key, &other.key) {
                (Some(left), Some(right)) => compare_keys(left, right),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            })
            .then(self.addr.unit.cmp(&other.addr.unit))
            .then(self.addr.row.cmp(&other.addr.row))
    }
}

/// The best `k` of `scored`, sorted, plus every row whose rank ties the `k`th: which of the tied
/// rows are best depends on keys not read yet.
fn best_k(scored: impl IntoIterator<Item = Scored>, k: usize) -> Vec<Scored> {
    let mut scored = scored.into_iter().collect::<Vec<_>>();
    scored.sort_unstable();
    cut_with_ties(&mut scored, k);
    scored
}

/// Truncate sorted `items` to `k`, keeping every item whose rank ties the `k`th.
fn cut_with_ties(items: &mut Vec<Scored>, k: usize) {
    if k == 0 {
        items.clear();
        return;
    }
    let Some(kth) = items.get(k - 1).map(|item| item.rank) else {
        return;
    };
    let end = items
        .iter()
        .skip(k)
        .position(|item| item.rank.total_cmp(&kth).is_ne())
        .map_or(items.len(), |extra| k + extra);
    items.truncate(end);
}

/// A k-way heap merge of sorted lists to their first `k` items, plus every item whose rank ties
/// the `k`th.
fn merge_sorted(lists: Vec<Vec<Scored>>, k: usize) -> Vec<Scored> {
    let mut iters = lists
        .into_iter()
        .map(std::iter::IntoIterator::into_iter)
        .collect::<Vec<_>>();
    let mut heap = BinaryHeap::with_capacity(iters.len());
    for (index, iter) in iters.iter_mut().enumerate() {
        if let Some(first) = iter.next() {
            heap.push(Reverse((first, index)));
        }
    }
    let mut out: Vec<Scored> = Vec::with_capacity(k);
    loop {
        let Some(Reverse((item, index))) = heap.pop() else {
            break;
        };
        if out.len() >= k
            && out
                .last()
                .is_none_or(|last| last.rank.total_cmp(&item.rank).is_ne())
        {
            break;
        }
        if let Some(next) = iters[index].next() {
            heap.push(Reverse((next, index)));
        }
        out.push(item);
    }
    out
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

    /// A reported value as a rank, lower is better.
    fn rank(self, value: f32) -> f32 {
        match self.metric {
            DistanceMetric::L2 => value,
            DistanceMetric::Cosine | DistanceMetric::Dot => -value,
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
            "vector.values",
            "query vector components must be finite",
        )));
    }
    Ok(field)
}

/// Shared inputs of the compute stages.
struct Context {
    field: FieldId,
    dims: usize,
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
    projection: Projection,
    /// Units whose zone maps exclude the filter.
    pruned: Vec<UnitId>,
    /// Per segment: bytes the first fetch read cold.
    cold: HashMap<UnitId, u64>,
}

/// Run a vector search over `view`.
///
/// # Errors
///
/// Invalid requests ([`QueryError::RequestVectorDimensionMismatch`],
/// `InvalidArgument` naming the request field), I/O and typed corruption.
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
    if top_k == 0 {
        return Ok(SearchOutcome {
            hits: Vec::new(),
            units: Vec::new(),
            fetch: FetchReport::default(),
            reranked: 0,
            plan: PlanNode::new(Operator::Project, "k=0"),
            timings: SearchTimings::default(),
        });
    }

    // Plan and fetch 1: the filter's sections and the vector indexes.
    let mut plan = FetchPlan::default();
    let mut pruned = Vec::new();
    let mut cold = HashMap::new();
    for unit in view.units() {
        if unit.is_memtable() {
            continue;
        }
        let mut needs = Vec::new();
        if let Some(filter) = &filter {
            if !filter.may_match(&unit) {
                pruned.push(unit.id());
                continue;
            }
            needs.extend(filter.needs(&unit));
        }
        if unit.has_vector_index(field.id, false) || unit.has_vector_index(field.id, true) {
            needs.push(SectionNeed::VectorIndex(field.id));
        }
        let mut unit_cold = 0;
        for need in needs {
            if let Residency::Cold { bytes } = view.residency(unit.id(), &need) {
                unit_cold += bytes;
            }
            plan.push(unit.id(), need);
        }
        cold.insert(unit.id(), unit_cold);
    }
    let (pins, fetch) = view.fetch(&plan).await?;
    let context = Arc::new(Context {
        field: field.id,
        dims: field.dimensions as usize,
        metric,
        query,
        raw_query: request.vector.clone(),
        top_k,
        budget,
        ef: request.ef.unwrap_or(DEFAULT_EF).max(budget).max(1),
        tuning: request.tuning,
        filter,
        projection: request.projection,
        pruned,
        cold,
    });
    let mut execution = Execution {
        context,
        pins,
        fetch,
        phase: Phase::Candidates,
        fetched: false,
        rows: None,
        timings: SearchTimings {
            planning: elapsed(started),
            ..SearchTimings::default()
        },
        reranked: 0,
    };

    // Stages 2 to 5 on the query pool, handing back only to fetch what is cold.
    loop {
        let (returned, step) = view
            .run(move |view| {
                let step = execution.advance(view);
                (execution, step)
            })
            .await?;
        execution = returned;
        match step? {
            Step::Done(outcome) => return Ok(*outcome),
            Step::Fetch(plan) => {
                let (pins, report) = view.fetch(&plan).await?;
                execution.pins.extend(pins);
                execution.fetch.merge(&report);
                execution.fetched = true;
            }
            Step::Rows(addrs) => {
                execution.rows = Some(view.rows(&addrs, execution.context.projection).await?);
            }
        }
    }
}

fn elapsed(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn micros_since(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1e6
}

/// What a unit's candidates stage produced.
enum Output {
    /// Exactly scored rows, the unit's best `k`, final.
    Exact(Vec<Scored>),
    /// Approximate candidates to rerank in f32.
    Approx(Vec<Candidate>),
    /// Rows to scan exactly in f32 once their pages are fetched.
    ScanF32(RoaringBitmap),
    /// Nothing.
    None,
}

/// One unit through the candidates stage.
struct UnitResult {
    unit: UnitId,
    report: UnitReport,
    /// The unit's plan from its source up to its candidate cut.
    node: PlanNode,
    output: Output,
    prefilter_micros: u64,
}

/// A stage's request back to the async driver.
enum Step {
    /// The search finished.
    Done(Box<SearchOutcome>),
    /// Fetch these sections, then continue.
    Fetch(FetchPlan),
    /// Read these rows with [`ReadView::rows`], then continue.
    Rows(Vec<RowAddr>),
}

enum Phase {
    Candidates,
    Rerank(Vec<UnitResult>),
    Project {
        finalists: Vec<Scored>,
        units: Vec<UnitResult>,
        merge: PlanNode,
    },
    Done,
}

/// The state of one search between its compute and fetch stages.
struct Execution {
    context: Arc<Context>,
    pins: PinSet,
    fetch: FetchReport,
    phase: Phase,
    /// Whether the current phase's inputs were fetched (so it reads them from the pins
    /// without asking the cache again).
    fetched: bool,
    /// Rows read by the driver for a projection the pins cannot serve.
    rows: Option<Vec<RowData>>,
    timings: SearchTimings,
    reranked: usize,
}

impl Execution {
    /// Run stages until one needs I/O or the search is done.
    fn advance(&mut self, view: &ReadView) -> logpose_types::Result<Step> {
        loop {
            match std::mem::replace(&mut self.phase, Phase::Done) {
                Phase::Candidates => {
                    let started = Instant::now();
                    let context = &self.context;
                    let pins = &self.pins;
                    let units = view.units();
                    // Units run in parallel only when more than one has real work: handing a
                    // task to another worker and joining it costs tens of microseconds, more
                    // than a small unit (an empty memtable, a pruned segment) takes.
                    let heavy = units
                        .iter()
                        .filter(|unit| {
                            unit.live_count() > PARALLEL_UNIT_ROWS
                                && !context.pruned.contains(&unit.id())
                        })
                        .count();
                    let results = if context.tuning.parallel && heavy > 1 {
                        units
                            .into_par_iter()
                            .map(|unit| unit_candidates(&unit, pins, context))
                            .collect::<Vec<_>>()
                    } else {
                        units
                            .iter()
                            .map(|unit| unit_candidates(unit, pins, context))
                            .collect::<Vec<_>>()
                    }
                    .into_iter()
                    .collect::<logpose_types::Result<Vec<_>>>()?;
                    self.timings.prefilter = results.iter().map(|unit| unit.prefilter_micros).sum();
                    self.timings.candidates = elapsed(started);
                    self.phase = Phase::Rerank(results);
                    self.fetched = false;
                }
                Phase::Rerank(units) => {
                    let started = Instant::now();
                    if !self.fetched {
                        let plan = rerank_needs(&units, self.context.field);
                        let (pins, report, missing) = view.fetch_resident(&plan)?;
                        self.pins.extend(pins);
                        self.fetch.merge(&report);
                        if !missing.is_empty() {
                            self.timings.rerank += elapsed(started);
                            self.phase = Phase::Rerank(units);
                            return Ok(Step::Fetch(missing));
                        }
                    }
                    let (units, lists) = self.rerank(view, units)?;
                    self.timings.rerank += elapsed(started);
                    let merge_started = Instant::now();
                    let merged_from = lists.iter().map(Vec::len).sum::<usize>();
                    let unit_count = lists.len();
                    let mut finalists = merge_sorted(lists, self.context.top_k);
                    self.resolve_ties(view, &mut finalists)?;
                    let merge_micros = micros_since(merge_started);
                    self.timings.merge = elapsed(merge_started);
                    let merge = PlanNode::new(
                        Operator::Merge,
                        format!("units={unit_count} k={}", self.context.top_k),
                    )
                    .with_stats(
                        OperatorStats::rows(self.context.top_k as u64),
                        OperatorStats {
                            rows: finalists.len() as u64,
                            micros: merge_micros,
                            ..OperatorStats::rows(0)
                        },
                    );
                    let _ = merged_from;
                    self.phase = Phase::Project {
                        finalists,
                        units,
                        merge,
                    };
                    self.fetched = false;
                }
                Phase::Project {
                    finalists,
                    units,
                    merge,
                } => {
                    let started = Instant::now();
                    let addrs = finalists
                        .iter()
                        .map(|scored| scored.addr)
                        .collect::<Vec<_>>();
                    let rows = match self.rows.take() {
                        Some(rows) => rows,
                        None => match view.projection_needs(&addrs, self.context.projection) {
                            None => {
                                self.phase = Phase::Project {
                                    finalists,
                                    units,
                                    merge,
                                };
                                self.timings.project += elapsed(started);
                                return Ok(Step::Rows(addrs));
                            }
                            Some(plan) => {
                                if !self.fetched {
                                    let (pins, report, missing) = view.fetch_resident(&plan)?;
                                    self.pins.extend(pins);
                                    self.fetch.merge(&report);
                                    if !missing.is_empty() {
                                        self.phase = Phase::Project {
                                            finalists,
                                            units,
                                            merge,
                                        };
                                        self.timings.project += elapsed(started);
                                        return Ok(Step::Fetch(missing));
                                    }
                                }
                                view.project(&addrs, self.context.projection, &self.pins)?
                            }
                        },
                    };
                    let hits = finalists
                        .into_iter()
                        .zip(rows)
                        .map(|(scored, row)| SearchHit {
                            value: scored.value,
                            row,
                        })
                        .collect::<Vec<_>>();
                    self.timings.project += elapsed(started);
                    let outcome = self.finish(units, merge, hits);
                    return Ok(Step::Done(Box::new(outcome)));
                }
                Phase::Done => {
                    return Err(LogPoseError::internal("a finished search was advanced"));
                }
            }
        }
    }

    /// Exact scores of every unit's candidates and each unit's best `k`, in parallel.
    fn rerank(
        &mut self,
        view: &ReadView,
        units: Vec<UnitResult>,
    ) -> logpose_types::Result<(Vec<UnitResult>, Vec<Vec<Scored>>)> {
        let context = &self.context;
        let pins = &self.pins;
        let views: HashMap<UnitId, UnitView<'_>> = view
            .units()
            .into_iter()
            .map(|unit| (unit.id(), unit))
            .collect();
        let rerank_one =
            |mut result: UnitResult| -> logpose_types::Result<(UnitResult, Vec<Scored>, usize)> {
                let output = std::mem::replace(&mut result.output, Output::None);
                let Some(unit) = views.get(&result.unit) else {
                    return Ok((result, Vec::new(), 0));
                };
                let started = Instant::now();
                let (scored, count, node) = match output {
                    Output::Exact(scored) => (scored, 0, None),
                    Output::None => (Vec::new(), 0, None),
                    Output::Approx(candidates) => {
                        let count = candidates.len();
                        let scored = rerank_candidates(unit, &candidates, pins, context)?;
                        let node =
                            PlanNode::new(Operator::Rerank, format!("f32 keep={}", context.top_k))
                                .with_stats(
                                    OperatorStats {
                                        rows: count.min(context.top_k) as u64,
                                        distances: count as u64,
                                        ..OperatorStats::default()
                                    },
                                    OperatorStats {
                                        rows: scored.len() as u64,
                                        distances: count as u64,
                                        micros: micros_since(started),
                                        ..OperatorStats::default()
                                    },
                                );
                        (scored, count, Some(node))
                    }
                    Output::ScanF32(rows) => {
                        let scanned = rows.len();
                        let scored = scan_f32(unit, &rows, pins, context)?;
                        result.report.candidates = scored.len();
                        let node = PlanNode::new(
                            Operator::ExactScan,
                            format!("unit={:08x} f32 keep={}", result.unit.0, context.top_k),
                        )
                        .with_reason(result.report.reason.clone())
                        .with_stats(
                            OperatorStats {
                                rows: scanned.min(context.top_k as u64),
                                distances: scanned,
                                ..OperatorStats::default()
                            },
                            OperatorStats {
                                rows: scored.len() as u64,
                                distances: scanned,
                                micros: micros_since(started),
                                ..OperatorStats::default()
                            },
                        );
                        result.report.distances = scanned;
                        (scored, 0, Some(node))
                    }
                };
                if let Some(node) = node {
                    let below = std::mem::replace(
                        &mut result.node,
                        PlanNode::new(Operator::SegmentSource, ""),
                    );
                    result.node = node.over(below);
                }
                Ok((result, scored, count))
            };
        // A unit reranks at most its `k * rerank_factor` candidates, microseconds of work, so
        // units rerank in parallel only when there are many candidates in all.
        let candidates = units
            .iter()
            .map(|unit| match &unit.output {
                Output::Approx(candidates) => candidates.len(),
                Output::ScanF32(rows) => usize::try_from(rows.len()).unwrap_or(usize::MAX),
                Output::Exact(_) | Output::None => 0,
            })
            .sum::<usize>();
        let reranked = if context.tuning.parallel && candidates > PARALLEL_RERANK_CANDIDATES {
            units.into_par_iter().map(rerank_one).collect::<Vec<_>>()
        } else {
            units.into_iter().map(rerank_one).collect::<Vec<_>>()
        };
        let mut out_units = Vec::with_capacity(reranked.len());
        let mut lists = Vec::with_capacity(reranked.len());
        for item in reranked {
            let (result, scored, count) = item?;
            self.reranked += count;
            out_units.push(result);
            lists.push(scored);
        }
        Ok((out_units, lists))
    }

    /// Read the keys of finalists whose ranks tie, order them by (rank, key), and cut to `k`:
    /// results with equal values are ordered by key, however many rows tie.
    fn resolve_ties(
        &self,
        view: &ReadView,
        finalists: &mut Vec<Scored>,
    ) -> logpose_types::Result<()> {
        let tied = |index: usize| {
            let rank = finalists[index].rank;
            let equal = |other: Option<&Scored>| {
                other.is_some_and(|other| other.rank.total_cmp(&rank).is_eq())
            };
            equal(
                index
                    .checked_sub(1)
                    .and_then(|before| finalists.get(before)),
            ) || equal(finalists.get(index + 1))
        };
        let missing = (0..finalists.len())
            .filter(|index| finalists[*index].key.is_none() && tied(*index))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            let units: HashMap<UnitId, UnitView<'_>> = view
                .units()
                .into_iter()
                .map(|unit| (unit.id(), unit))
                .collect();
            for index in missing {
                let addr = finalists[index].addr;
                let unit = units.get(&addr.unit).ok_or_else(|| {
                    LogPoseError::internal(format!("unit {} left the view", addr.unit))
                })?;
                let key = unit
                    .pks(&self.pins)?
                    .pk_at(addr.row)
                    .ok_or_else(|| missing_key(unit, addr.row))?;
                finalists[index].key = Some(key);
            }
            finalists.sort_unstable();
        }
        finalists.truncate(self.context.top_k);
        Ok(())
    }

    fn finish(
        &mut self,
        units: Vec<UnitResult>,
        merge: PlanNode,
        hits: Vec<SearchHit>,
    ) -> SearchOutcome {
        let mut reports = Vec::with_capacity(units.len());
        let mut merge = merge;
        for unit in units {
            reports.push(unit.report);
            merge.children.push(unit.node);
        }
        let project = PlanNode::new(Operator::Project, format!("k={}", self.context.top_k))
            .with_stats(
                OperatorStats::rows(self.context.top_k as u64),
                OperatorStats {
                    rows: hits.len() as u64,
                    micros: self.timings.project as f64,
                    ..OperatorStats::default()
                },
            )
            .over(merge);
        SearchOutcome {
            hits,
            units: reports,
            fetch: std::mem::take(&mut self.fetch),
            reranked: self.reranked,
            plan: project,
            timings: self.timings,
        }
    }
}

/// What the rerank reads: the vector pages and keys of approximate candidates, and the pages
/// of the rows segments without codes scan in f32.
fn rerank_needs(units: &[UnitResult], field: FieldId) -> FetchPlan {
    let mut plan = FetchPlan::default();
    for unit in units {
        match &unit.output {
            Output::Approx(candidates) => {
                let rows: RoaringBitmap =
                    candidates.iter().map(|candidate| candidate.row).collect();
                plan.push(unit.unit, SectionNeed::VectorRows(field, rows));
                plan.push(unit.unit, SectionNeed::Pk);
            }
            Output::ScanF32(rows) => {
                plan.push(unit.unit, SectionNeed::VectorRows(field, rows.clone()));
                plan.push(unit.unit, SectionNeed::Pk);
            }
            Output::Exact(_) | Output::None => {}
        }
    }
    plan
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

/// The unit's source node.
fn source_node(unit: &UnitView<'_>, context: &Context, what: &str) -> PlanNode {
    let live = u64::from(unit.live_count());
    let cold = context.cold.get(&unit.id()).copied().unwrap_or(0);
    let kind = if unit.is_memtable() {
        "memtable"
    } else {
        "segment"
    };
    let stats = OperatorStats {
        rows: live,
        cold_bytes: cold,
        ..OperatorStats::default()
    };
    PlanNode::new(
        Operator::SegmentSource,
        format!(
            "unit={:08x} {kind} rows={}{}{what}",
            unit.id().0,
            unit.row_count(),
            if what.is_empty() { "" } else { " " }
        ),
    )
    .with_stats(stats, stats)
}

/// The unit's filter nodes over its source: `BitmapProbe` (with a filter) and `MaskDeletes`.
fn filter_nodes(
    unit: &UnitView<'_>,
    source: PlanNode,
    matched: u64,
    context: &Context,
    micros: f64,
) -> PlanNode {
    let live = u64::from(unit.live_count());
    let deleted = u64::from(unit.row_count()).saturating_sub(live);
    let mut node = source;
    if let Some(filter) = &context.filter {
        node = PlanNode::new(Operator::BitmapProbe, filter.describe(unit))
            .with_reason("exact cardinality from the bitmap")
            .with_stats(
                OperatorStats::rows(matched),
                OperatorStats {
                    rows: matched,
                    micros,
                    ..OperatorStats::default()
                },
            )
            .over(node);
    }
    PlanNode::new(Operator::MaskDeletes, format!("deleted={deleted}"))
        .with_stats(OperatorStats::rows(matched), OperatorStats::rows(matched))
        .over(node)
}

/// Stats of estimated work.
fn estimated(work: &Work, rows: u64, micros: f64) -> OperatorStats {
    OperatorStats {
        rows,
        distances: work.distances().round() as u64,
        hops: work.hops.round() as u64,
        resident_bytes: work.resident_bytes.round() as u64,
        cold_bytes: work.cold_bytes.round() as u64,
        micros,
    }
}

/// The candidates stage for one unit: filter, strategy, and scan or walk.
fn unit_candidates(
    unit: &UnitView<'_>,
    pins: &PinSet,
    context: &Context,
) -> logpose_types::Result<UnitResult> {
    if context.pruned.contains(&unit.id()) {
        return Ok(UnitResult {
            unit: unit.id(),
            report: UnitReport::new(
                unit,
                UnitStrategy::Pruned,
                0,
                "zone maps exclude the filter",
            ),
            node: source_node(unit, context, "pruned").with_reason("zone maps exclude the filter"),
            output: Output::None,
            prefilter_micros: 0,
        });
    }
    let probe_started = Instant::now();
    let allowed = allowed_rows(unit, pins, context)?;
    let probe_micros = micros_since(probe_started);
    let matched = allowed.len();
    let source = source_node(unit, context, "");
    let below = filter_nodes(unit, source, matched, context, probe_micros);
    let prefilter_micros = probe_micros.round() as u64;
    if allowed.is_empty() {
        return Ok(UnitResult {
            unit: unit.id(),
            report: UnitReport::new(unit, UnitStrategy::Empty, 0, "no live row matches"),
            node: below,
            output: Output::None,
            prefilter_micros,
        });
    }
    if unit.is_memtable() {
        return memtable_scan(unit, pins, context, allowed, below, prefilter_micros);
    }
    let index = unit.vector_index(context.field, pins)?;
    let usable_graph = index.graph.as_ref().filter(|_| index.sq8.is_some());
    let shape = SegmentShape {
        rows: u64::from(unit.row_count()),
        matched,
        dims: context.dims,
        graph_nodes: usable_graph.map(|graph| graph.graph.len() as u64),
        links: usable_graph.map_or(0, |graph| graph.graph.params().max_links(0)),
        sq8: index.sq8.is_some(),
        index_cold_bytes: context.cold.get(&unit.id()).copied().unwrap_or(0),
        ef: context.ef,
        budget: context.budget,
        filtered: context.filter.is_some() && matched < u64::from(unit.live_count()),
        probe: context.tuning.ef_escalation,
    };
    let decision = context.tuning.cost.decide(&shape, context.tuning.force);
    let Some(sq8) = index.sq8.as_ref() else {
        // Scanned in f32 in the rerank stage, once the rows' pages are fetched.
        let mut report = UnitReport::new(unit, UnitStrategy::ExactF32, matched, &decision.reason);
        report.candidates = usize::try_from(matched).unwrap_or(usize::MAX);
        return Ok(UnitResult {
            unit: unit.id(),
            report,
            node: below,
            output: Output::ScanF32(allowed),
            prefilter_micros,
        });
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

    let mut report = UnitReport::new(unit, UnitStrategy::ExactSq8, matched, &decision.reason);
    let (candidates, scan_node) = match (decision.chosen.choice, usable_graph) {
        (Choice::GraphAdmit | Choice::GraphAcorn, Some(graph)) => {
            let started = Instant::now();
            let walked = walk(graph, &allowed, unit, &estimate, &decision, &shape, context);
            let walk_micros = micros_since(started);
            report.strategy = if decision.chosen.choice == Choice::GraphAcorn {
                UnitStrategy::GraphAcorn
            } else {
                UnitStrategy::GraphAdmit
            };
            report.ef = walked.ef;
            report.visited = walked.stats.visited;
            report.escalations = walked.escalations;
            report.distances = walked.stats.distance_computations;
            report.hops = walked.stats.expansions + walked.stats.two_hop_expansions;
            report.two_hops = walked.stats.two_hop_expansions;
            let mode = if decision.chosen.choice == Choice::GraphAcorn {
                "acorn"
            } else {
                "admit"
            };
            let widened = if walked.ef == context.ef {
                String::new()
            } else {
                format!("->{} escalations={}", walked.ef, walked.escalations)
            };
            let walk_node = PlanNode::new(
                Operator::GraphScan,
                format!("unit={:08x} {mode} ef={}{widened}", unit.id().0, context.ef),
            )
            .with_reason(decision.reason.clone())
            .with_stats(
                estimated(
                    &decision.chosen.work,
                    context.budget as u64,
                    decision.chosen.micros,
                ),
                OperatorStats {
                    rows: walked.candidates.len() as u64,
                    distances: walked.stats.distance_computations,
                    hops: report.hops,
                    micros: walk_micros,
                    ..OperatorStats::default()
                },
            )
            .over(below);
            match walked.abandoned {
                None => (walked.candidates, walk_node),
                Some(why) => {
                    // The walk spent the exact scan's price: scan exactly instead.
                    report.strategy = UnitStrategy::ExactSq8;
                    report.walk_abandoned = true;
                    report.reason = format!("{}; {why}", decision.reason);
                    let started = Instant::now();
                    let candidates = sq8_scan(&allowed, unit.id(), &estimate, context);
                    report.distances += matched;
                    let node = PlanNode::new(
                        Operator::ExactScan,
                        format!("unit={:08x} sq8 after the walk", unit.id().0),
                    )
                    .with_reason(why)
                    .with_stats(
                        estimated(&decision.exact.work, matched, decision.exact.micros),
                        OperatorStats {
                            rows: matched,
                            distances: matched,
                            micros: micros_since(started),
                            ..OperatorStats::default()
                        },
                    )
                    .over(walk_node);
                    (candidates, node)
                }
            }
        }
        _ => {
            let started = Instant::now();
            let candidates = sq8_scan(&allowed, unit.id(), &estimate, context);
            report.distances = matched;
            let morsels = if parallel(context, matched) {
                format!(" morsels={}", unit.row_count().div_ceil(SCAN_MORSEL_ROWS))
            } else {
                String::new()
            };
            let node = PlanNode::new(
                Operator::ExactScan,
                format!("unit={:08x} sq8{morsels}", unit.id().0),
            )
            .with_reason(decision.reason.clone())
            .with_stats(
                estimated(&decision.exact.work, matched, decision.exact.micros),
                OperatorStats {
                    rows: matched,
                    distances: matched,
                    micros: micros_since(started),
                    ..OperatorStats::default()
                },
            )
            .over(below);
            (candidates, node)
        }
    };
    report.candidates = candidates.len();
    let top = PlanNode::new(Operator::TopK, format!("k={}", context.budget))
        .with_stats(
            OperatorStats::rows(matched.min(context.budget as u64)),
            OperatorStats::rows(candidates.len() as u64),
        )
        .over(scan_node);
    Ok(UnitResult {
        unit: unit.id(),
        report,
        node: top,
        output: Output::Approx(candidates),
        prefilter_micros,
    })
}

/// A memtable's exact f32 scan: the best `budget` rows by distance, then their values and
/// the best `k` by value.
fn memtable_scan(
    unit: &UnitView<'_>,
    pins: &PinSet,
    context: &Context,
    allowed: RoaringBitmap,
    below: PlanNode,
    prefilter_micros: u64,
) -> logpose_types::Result<UnitResult> {
    let started = Instant::now();
    let matched = allowed.len();
    let scored = scan_f32(unit, &allowed, pins, context)?;
    let mut report = UnitReport::new(
        unit,
        UnitStrategy::MemtableScan,
        matched,
        "memtables are scanned exactly",
    );
    report.candidates = scored.len();
    report.distances = matched;
    let work = Work {
        f32_distances: matched as f64,
        ..Work::default()
    };
    let node = PlanNode::new(
        Operator::ExactScan,
        format!("unit={:08x} f32 keep={}", unit.id().0, context.top_k),
    )
    .with_reason("memtables are scanned exactly")
    .with_stats(
        estimated(
            &work,
            matched.min(context.top_k as u64),
            context.tuning.cost.micros(&work, context.dims, 0),
        ),
        OperatorStats {
            rows: scored.len() as u64,
            distances: matched,
            micros: micros_since(started),
            ..OperatorStats::default()
        },
    )
    .over(below);
    Ok(UnitResult {
        unit: unit.id(),
        report,
        node,
        output: Output::Exact(scored),
        prefilter_micros,
    })
}

/// Whether an exact scan of `rows` rows runs in parallel morsels.
fn parallel(context: &Context, rows: u64) -> bool {
    context.tuning.parallel && rows > 2 * u64::from(SCAN_MORSEL_ROWS)
}

/// The best `budget` rows of `allowed` by their SQ8 estimates, scanned in parallel morsels
/// when there are many.
fn sq8_scan(
    allowed: &RoaringBitmap,
    unit: UnitId,
    estimate: &(impl Fn(RowId) -> f32 + Sync),
    context: &Context,
) -> Vec<Candidate> {
    let scan = |rows: roaring::bitmap::Iter<'_>| {
        let mut top = TopK::new(context.budget);
        for row in rows {
            top.offer(unit, row, estimate(row), |_| None);
        }
        top
    };
    if !parallel(context, allowed.len()) {
        return scan(allowed.iter()).into_sorted();
    }
    let last = allowed.max().unwrap_or(0);
    let morsels = (0..=last / SCAN_MORSEL_ROWS).collect::<Vec<_>>();
    let tops = morsels
        .into_par_iter()
        .map(|morsel| {
            let start = morsel * SCAN_MORSEL_ROWS;
            let end = start.saturating_add(SCAN_MORSEL_ROWS);
            scan(allowed.range(start..end))
        })
        .collect::<Vec<_>>();
    let mut top = TopK::new(context.budget);
    for part in tops {
        for candidate in part.heap {
            top.push(candidate);
        }
    }
    top.into_sorted()
}

/// Exact f32 scan of `rows`: the best `budget` by distance (ties by key), then the best `k` by
/// reported value.
fn scan_f32(
    unit: &UnitView<'_>,
    rows: &RoaringBitmap,
    pins: &PinSet,
    context: &Context,
) -> logpose_types::Result<Vec<Scored>> {
    let vectors = unit.vector_rows(context.field, pins)?;
    let pks = unit.pks(pins)?;
    let mut top = TopK::new(context.budget);
    for row in rows {
        if let Some(vector) = vectors.get(row)? {
            top.offer(
                unit.id(),
                row,
                context.metric.distance(&context.query, &vector),
                |row| pks.pk_at(row),
            );
        }
    }
    let mut scored = Vec::with_capacity(top.heap.len());
    for candidate in top.into_sorted() {
        let Some(vector) = vectors.get(candidate.row)? else {
            continue;
        };
        let value = metric_value(context.metric.metric, &context.raw_query, &vector);
        scored.push(Scored {
            rank: context.metric.rank(value),
            key: candidate.key,
            addr: RowAddr {
                unit: unit.id(),
                row: candidate.row,
            },
            value,
        });
    }
    Ok(best_k(scored, context.top_k))
}

fn missing_key(unit: &UnitView<'_>, row: RowId) -> LogPoseError {
    LogPoseError::internal(format!("row {row} of unit {} has no key", unit.id()))
}

/// Exact values and keys of approximate candidates; the unit's best `k`.
fn rerank_candidates(
    unit: &UnitView<'_>,
    candidates: &[Candidate],
    pins: &PinSet,
    context: &Context,
) -> logpose_types::Result<Vec<Scored>> {
    let vectors = unit.vector_rows(context.field, pins)?;
    let mut scored = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let Some(vector) = vectors.get(candidate.row)? else {
            continue;
        };
        let value = metric_value(context.metric.metric, &context.raw_query, &vector);
        if !value.is_finite() {
            continue;
        }
        scored.push(Scored {
            rank: context.metric.rank(value),
            key: None,
            addr: RowAddr {
                unit: unit.id(),
                row: candidate.row,
            },
            value,
        });
    }
    Ok(best_k(scored, context.top_k))
}

/// What a walk produced.
struct Walked {
    candidates: Vec<Candidate>,
    ef: usize,
    stats: SearchStats,
    escalations: u32,
    /// Why the walk stopped widening in favor of an exact scan.
    abandoned: Option<String>,
}

/// The work a walk's counters represent, for its cost bound: every distance a random read
/// and a queue offer, every list read with its links, and a filter check per visited row.
fn walk_work(stats: &SearchStats, dims: usize) -> Work {
    let evaluated = stats.distance_computations as f64;
    Work {
        sq8_distances: evaluated,
        hops: (stats.expansions + stats.two_hop_expansions) as f64,
        checks: stats.visited as f64,
        offers: evaluated,
        random_reads: evaluated,
        resident_bytes: evaluated * dims as f64,
        ..Work::default()
    }
}

/// Walk a segment's graph over `allowed`: a resumable cursor that widens `ef` while its
/// results are short, while an admit-only walk meets matching rows far less often than the
/// filter's selectivity predicts, and, for a filtered ACORN walk, while widening still
/// changes its best rows. It stops widening once its work reaches the exact scan's price
/// (unless walks are forced).
fn walk(
    graph: &logpose_storage::segment_v2::SegmentGraph,
    allowed: &RoaringBitmap,
    unit: &UnitView<'_>,
    estimate: &impl Fn(RowId) -> f32,
    decision: &Decision,
    shape: &SegmentShape,
    context: &Context,
) -> Walked {
    let matched = allowed.len();
    let selectivity = shape.selectivity();
    let filtered = shape.filtered;
    let (strategy, admit_only) = if decision.chosen.choice == Choice::GraphAcorn {
        (FilterStrategy::acorn(), false)
    } else {
        (FilterStrategy::Admit, true)
    };
    let node_filter = NodeFilter {
        rows: allowed,
        nodes: &graph.nodes,
        all: matched == u64::from(unit.row_count()),
        count: usize::try_from(matched).unwrap_or(usize::MAX),
    };
    let distance = NodeDistance {
        nodes: &graph.nodes,
        estimate,
    };
    let cost = &context.tuning.cost;
    let bound = (!context.tuning.force.walks()).then(|| cost.walk_budget(decision));
    SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        let mut cursor = graph
            .graph
            .cursor(&distance, &node_filter, strategy, &mut scratch);
        let mut ef = context.ef;
        cursor.advance(ef);
        let max_ef = context.ef.saturating_mul(MAX_EF_MULTIPLIER);
        let mut escalations = 0_u32;
        let mut abandoned = None;
        // A filtered ACORN walk probes convergence: it widens once and keeps widening while
        // its best `k` still change. Over an anti-correlated filter the first beam settles on
        // matching rows far from the query and wider beams keep finding closer ones; over a
        // filter independent of the query the probe changes nothing.
        let mut probing = filtered && !admit_only;
        while context.tuning.ef_escalation && ef < max_ef && !cursor.is_exhausted() {
            let found = cursor.top_k(context.budget).len();
            let stats = cursor.stats();
            let short = found < context.budget;
            let anti_correlated = admit_only && filtered && stats.visited > 0 && {
                let admitted = stats.visited.saturating_sub(stats.filtered_out);
                let local = admitted as f64 / stats.visited as f64;
                local < CostModel::ANTI_CORRELATION_RATIO * selectivity
            };
            let widen = short || probing || (anti_correlated && escalations < 2);
            if !widen {
                break;
            }
            // Widening about doubles the work spent so far; past the exact scan's price the
            // exact scan is both cheaper and exact.
            if let Some(bound) = bound {
                let spent = cost.micros(&walk_work(&stats, shape.dims), shape.dims, shape.links);
                if spent * 2.0 > bound {
                    abandoned = Some(format!(
                        "walk abandoned at ef={ef}: {spent:.0}us spent, widening would pass the \
                         exact scan's {bound:.0}us"
                    ));
                    break;
                }
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
        // A node stands for every row with its vector; the cut bounds the candidates when
        // very many rows share one.
        let mut top = TopK::new(context.budget);
        if abandoned.is_none() {
            for neighbor in cursor.top_k(context.budget) {
                for row in graph.nodes.rows(neighbor.row) {
                    if allowed.contains(row) {
                        top.offer(unit.id(), row, neighbor.distance, |_| None);
                    }
                }
            }
        }
        Walked {
            candidates: top.into_sorted(),
            ef,
            stats: cursor.stats(),
            escalations,
            abandoned,
        }
    })
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

    fn scored(rank: f32, key: i64, unit: u32) -> Scored {
        Scored {
            rank,
            key: Some(PrimaryKey::Int64(key)),
            addr: RowAddr {
                unit: UnitId(unit),
                row: 0,
            },
            value: rank,
        }
    }

    #[test]
    fn the_heap_merge_equals_a_full_sort() {
        let mut rng = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for case in 0..200 {
            let units = 1 + next() % 6;
            let mut lists = Vec::new();
            let mut all = Vec::new();
            for unit in 0..units {
                let len = next() % 12;
                let items = (0..len)
                    .map(|index| {
                        scored(
                            (next() % 5) as f32,
                            i64::try_from(unit * 100 + index).unwrap_or_default(),
                            u32::try_from(unit).unwrap_or_default(),
                        )
                    })
                    .collect::<Vec<_>>();
                let sorted = best_k(items.clone(), items.len());
                all.extend(items);
                lists.push(sorted);
            }
            let k = usize::try_from(next() % 15).unwrap_or_default();
            let mut expected = all;
            expected.sort();
            cut_with_ties(&mut expected, k);
            let merged = merge_sorted(lists, k);
            let keys = |items: &[Scored]| {
                items
                    .iter()
                    .map(|item| (item.rank.to_bits(), item.key.clone()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(keys(&merged), keys(&expected), "case {case}");
        }
    }

    #[test]
    fn best_k_keeps_every_row_tied_with_the_kth() {
        let items = (0..50)
            .rev()
            .map(|key| scored(1.0, key, 0))
            .chain([scored(0.5, 99, 0)]);
        let best = best_k(items, 3);
        assert_eq!(
            best.len(),
            51,
            "one better row and all fifty tied at the third"
        );
        let keys = best
            .iter()
            .take(3)
            .map(|item| item.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            [99, 0, 1].map(|key| Some(PrimaryKey::Int64(key))).to_vec()
        );
        let distinct = best_k((0..10).map(|key| scored(key as f32, key, 0)), 3);
        assert_eq!(distinct.len(), 3);
        assert!(best_k((0..10).map(|key| scored(1.0, key, 0)), 0).is_empty());
    }
}
