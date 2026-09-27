//! Tests for the row-id HNSW graph.

use std::collections::{HashSet, VecDeque};
use std::sync::OnceLock;
use std::time::Instant;

use super::build::select_neighbors;
use super::search::Scored;
use super::{
    AllRows, F32Metric, F32Vectors, FilterStrategy, GraphError, HnswGraph, HnswParams, Neighbor,
    RowBitset, RowFilter, SearchScratch, SearchStatus, VectorSource,
};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

struct TestRng(u64);

impl TestRng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1_u64 << 24) as f32
    }

    fn gaussian(&mut self) -> f32 {
        let u1 = self.unit().max(f32::MIN_POSITIVE);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f32::consts::TAU * u2).cos()
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// Gaussian blobs around uniform centers in `[-1, 1]^dim`.
struct Clustered {
    vectors: F32Vectors,
    centers: Vec<Vec<f32>>,
    cluster_of: Vec<usize>,
    queries: Vec<Vec<f32>>,
}

fn clustered(rows: usize, dim: usize, clusters: usize, queries: usize, seed: u64) -> Clustered {
    clustered_with_spread(rows, dim, clusters, queries, 0.25, seed)
}

/// Like [`clustered`] with a per-dimension standard deviation of `spread`;
/// at 0.6 the blobs overlap heavily.
fn clustered_with_spread(
    rows: usize,
    dim: usize,
    clusters: usize,
    queries: usize,
    spread: f32,
    seed: u64,
) -> Clustered {
    let mut rng = TestRng(seed);
    let centers: Vec<Vec<f32>> = (0..clusters)
        .map(|_| (0..dim).map(|_| rng.unit() * 2.0 - 1.0).collect())
        .collect();
    let mut data = Vec::with_capacity(rows * dim);
    let mut cluster_of = Vec::with_capacity(rows);
    for _ in 0..rows {
        let cluster = rng.below(clusters);
        cluster_of.push(cluster);
        data.extend(centers[cluster].iter().map(|c| c + spread * rng.gaussian()));
    }
    let queries = (0..queries)
        .map(|_| {
            let cluster = rng.below(clusters);
            centers[cluster]
                .iter()
                .map(|c| c + spread * rng.gaussian())
                .collect()
        })
        .collect();
    Clustered {
        vectors: F32Vectors::new(dim, data, F32Metric::L2Squared).expect("valid vectors"),
        centers,
        cluster_of,
        queries,
    }
}

fn brute_force<F: RowFilter + ?Sized>(
    vectors: &F32Vectors,
    query: &[f32],
    k: usize,
    filter: &F,
) -> Vec<u32> {
    let mut all: Vec<Scored> = (0..vectors.len() as u32)
        .filter(|row| filter.contains(*row))
        .map(|row| Scored {
            dist: vectors.metric().distance(query, vectors.row(row)),
            row,
        })
        .collect();
    all.sort_unstable();
    all.truncate(k);
    all.into_iter().map(|hit| hit.row).collect()
}

fn recall(hits: &[Neighbor], truth: &[u32]) -> f64 {
    if truth.is_empty() {
        return 1.0;
    }
    let truth: HashSet<u32> = truth.iter().copied().collect();
    hits.iter().filter(|hit| truth.contains(&hit.row)).count() as f64 / truth.len() as f64
}

fn params(m: usize, ef_construction: usize, seed: u64) -> HnswParams {
    HnswParams {
        m,
        ef_construction,
        seed,
    }
}

/// Structural invariants every graph must satisfy.
fn assert_valid(graph: &HnswGraph) {
    let rows = graph.len();
    if rows == 0 {
        assert_eq!(graph.entry_point(), None);
        return;
    }
    let entry = graph
        .entry_point()
        .expect("non-empty graph has an entry point");
    let top = (0..rows as u32)
        .filter_map(|row| graph.level(row))
        .max()
        .expect("levels");
    assert_eq!(graph.max_level(), top);
    assert_eq!(graph.level(entry), Some(top));
    for row in 0..rows as u32 {
        let level = graph.level(row).expect("row level");
        for layer in 0..=level {
            let links = graph.neighbors(row, layer);
            assert!(links.len() <= graph.params().max_links(layer));
            let unique: HashSet<u32> = links.iter().copied().collect();
            assert_eq!(unique.len(), links.len(), "row {row} has duplicate links");
            for &neighbor in links {
                assert_ne!(neighbor, row, "row {row} links to itself");
                assert!(graph.level(neighbor).is_some_and(|l| l >= layer));
            }
        }
        assert!(graph.neighbors(row, level + 1).is_empty());
    }
}

/// Rows reachable from the entry point on layer 0.
fn reachable(graph: &HnswGraph) -> usize {
    let Some(entry) = graph.entry_point() else {
        return 0;
    };
    let mut seen = vec![false; graph.len()];
    let mut queue = VecDeque::from([entry]);
    seen[entry as usize] = true;
    let mut count = 1;
    while let Some(row) = queue.pop_front() {
        for &neighbor in graph.neighbors(row, 0) {
            if !seen[neighbor as usize] {
                seen[neighbor as usize] = true;
                count += 1;
                queue.push_back(neighbor);
            }
        }
    }
    count
}

fn random_filter(rows: usize, selectivity: f64, seed: u64) -> RowBitset {
    let mut rng = TestRng(seed);
    let mut set = RowBitset::new(rows);
    for row in 0..rows as u32 {
        if f64::from(rng.unit()) < selectivity {
            set.insert(row);
        }
    }
    set
}

/// Rows of the clusters whose centers are farthest from `query`, adding
/// clusters until at least `selectivity` of the rows are covered.
fn anti_correlated_filter(data: &Clustered, query: &[f32], selectivity: f64) -> RowBitset {
    let rows = data.cluster_of.len();
    let mut order: Vec<(f32, usize)> = data
        .centers
        .iter()
        .enumerate()
        .map(|(cluster, center)| (F32Metric::L2Squared.distance(query, center), cluster))
        .collect();
    order.sort_by(|a, b| b.0.total_cmp(&a.0));
    let mut sizes = vec![0_usize; data.centers.len()];
    for cluster in &data.cluster_of {
        sizes[*cluster] += 1;
    }
    let target = (rows as f64 * selectivity).ceil() as usize;
    let mut chosen = vec![false; data.centers.len()];
    let mut covered = 0;
    for (_, cluster) in order {
        if covered >= target {
            break;
        }
        chosen[cluster] = true;
        covered += sizes[cluster];
    }
    let mut set = RowBitset::new(rows);
    for (row, cluster) in data.cluster_of.iter().enumerate() {
        if chosen[*cluster] {
            set.insert(row as u32);
        }
    }
    set
}

struct Fixture {
    data: Clustered,
    graph: HnswGraph,
}

/// 10k x 32 clustered rows with a parallel-built graph, shared by tests.
fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let data = clustered(10_000, 32, 64, 100, 7);
        let graph =
            HnswGraph::build_parallel(&data.vectors, params(16, 128, 42)).expect("parallel build");
        Fixture { data, graph }
    })
}

struct FilteredRun {
    recall: f64,
    distance_computations: f64,
    two_hop: f64,
    qps: f64,
}

/// Per-query filters and their exact filtered top-k.
struct Workload {
    filters: Vec<RowBitset>,
    truths: Vec<Vec<u32>>,
}

fn workload<M>(fixture: &Fixture, k: usize, make_filter: M) -> Workload
where
    M: Fn(usize, &[f32]) -> RowBitset,
{
    let filters: Vec<RowBitset> = fixture
        .data
        .queries
        .iter()
        .enumerate()
        .map(|(index, query)| make_filter(index, query))
        .collect();
    let truths = fixture
        .data
        .queries
        .iter()
        .zip(&filters)
        .map(|(query, filter)| brute_force(&fixture.data.vectors, query, k, filter))
        .collect();
    Workload { filters, truths }
}

fn run_filtered(
    fixture: &Fixture,
    workload: &Workload,
    strategy: FilterStrategy,
    k: usize,
    ef: usize,
) -> FilteredRun {
    let Workload { filters, truths } = workload;
    let mut scratch = SearchScratch::new();
    let started = Instant::now();
    let mut total_recall = 0.0;
    let mut distance_computations = 0;
    let mut two_hop = 0;
    for ((query, filter), truth) in fixture.data.queries.iter().zip(filters).zip(truths) {
        let distance = fixture.data.vectors.query(query).expect("query");
        let output =
            fixture
                .graph
                .search_filtered(&distance, filter, strategy, k, ef, &mut scratch);
        assert!(output.neighbors.iter().all(|hit| filter.contains(hit.row)));
        total_recall += recall(&output.neighbors, truth);
        distance_computations += output.stats.distance_computations;
        two_hop += output.stats.two_hop_expansions;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let queries = fixture.data.queries.len() as f64;
    FilteredRun {
        recall: total_recall / queries,
        distance_computations: distance_computations as f64 / queries,
        two_hop: two_hop as f64 / queries,
        qps: queries / elapsed.max(f64::EPSILON),
    }
}

// ---------------------------------------------------------------------------
// Parameters and tiny graphs
// ---------------------------------------------------------------------------

#[test]
fn rejects_invalid_params() {
    for bad in [params(1, 64, 0), params(2000, 64, 0), params(16, 0, 0)] {
        assert!(matches!(
            HnswGraph::new(bad),
            Err(GraphError::InvalidParams(_))
        ));
    }
    assert!(HnswGraph::new(HnswParams::default()).is_ok());
}

#[test]
fn rejects_malformed_vectors_and_queries() {
    assert!(F32Vectors::new(0, vec![], F32Metric::L2Squared).is_err());
    assert!(F32Vectors::new(3, vec![1.0; 4], F32Metric::L2Squared).is_err());
    let vectors = F32Vectors::new(2, vec![0.0; 4], F32Metric::L2Squared).expect("vectors");
    assert!(vectors.query(&[1.0]).is_err());
}

#[test]
fn empty_graph_search_is_exhausted() {
    let vectors = F32Vectors::new(2, vec![], F32Metric::L2Squared).expect("vectors");
    let graph = HnswGraph::build(&vectors, HnswParams::default()).expect("build");
    assert!(graph.is_empty());
    let query = vectors.query(&[0.0, 0.0]).expect("query");
    let output = graph.search(&query, 5, 16, &mut SearchScratch::new());
    assert!(output.neighbors.is_empty());
    assert_eq!(output.status, SearchStatus::Exhausted);
}

#[test]
fn single_row_graph_returns_that_row() {
    let vectors = F32Vectors::new(2, vec![3.0, 4.0], F32Metric::L2Squared).expect("vectors");
    let graph = HnswGraph::build(&vectors, HnswParams::default()).expect("build");
    let query = vectors.query(&[0.0, 0.0]).expect("query");
    let output = graph.search(&query, 3, 8, &mut SearchScratch::new());
    assert_eq!(
        output.neighbors,
        vec![Neighbor {
            row: 0,
            distance: 25.0
        }]
    );
    assert_eq!(output.status, SearchStatus::Exhausted);
    let complete = graph.search(&query, 1, 8, &mut SearchScratch::new());
    assert_eq!(complete.status, SearchStatus::Complete);
}

#[test]
fn tiny_graph_search_matches_brute_force() {
    let data = clustered(200, 4, 5, 20, 11);
    let graph = HnswGraph::build(&data.vectors, params(4, 32, 3)).expect("build");
    assert_valid(&graph);
    let mut scratch = SearchScratch::new();
    for query in &data.queries {
        let distance = data.vectors.query(query).expect("query");
        let output = graph.search(&distance, 10, 200, &mut scratch);
        let truth = brute_force(&data.vectors, query, 10, &AllRows);
        let rows: Vec<u32> = output.neighbors.iter().map(|hit| hit.row).collect();
        assert_eq!(rows, truth);
        assert!(
            output
                .neighbors
                .windows(2)
                .all(|w| w[0].distance <= w[1].distance)
        );
    }
}

#[test]
fn negative_dot_metric_ranks_largest_inner_product_first() {
    let vectors = F32Vectors::new(
        2,
        vec![1.0, 0.0, 0.0, 1.0, 3.0, 3.0, -1.0, -1.0],
        F32Metric::NegativeDot,
    )
    .expect("vectors");
    let graph = HnswGraph::build(&vectors, params(2, 8, 1)).expect("build");
    let query = vectors.query(&[1.0, 1.0]).expect("query");
    let output = graph.search(&query, 2, 8, &mut SearchScratch::new());
    let rows: Vec<u32> = output.neighbors.iter().map(|hit| hit.row).collect();
    assert_eq!(rows[0], 2);
    assert_eq!(output.neighbors[0].distance, -6.0);
}

#[test]
fn heuristic_prefers_diverse_neighbors_and_keeps_pruned_ones() {
    // Base row 0 at the origin; rows 1 and 2 sit on the same side, row 3 on
    // the opposite side but farther away.
    let vectors =
        F32Vectors::new(1, vec![0.0, 1.0, 1.1, -1.5], F32Metric::L2Squared).expect("vectors");
    let candidates: Vec<Scored> = [1_u32, 2, 3]
        .iter()
        .map(|&row| Scored {
            dist: vectors.distance_between(0, row),
            row,
        })
        .collect();
    let (mut keep, mut pruned) = (Vec::new(), Vec::new());
    select_neighbors(&vectors, &candidates, 2, &mut keep, &mut pruned);
    let rows: Vec<u32> = keep.iter().map(|hit| hit.row).collect();
    assert_eq!(rows, vec![1, 3], "row 2 is shadowed by row 1");
    select_neighbors(&vectors, &candidates, 3, &mut keep, &mut pruned);
    let rows: Vec<u32> = keep.iter().map(|hit| hit.row).collect();
    assert_eq!(rows, vec![1, 3, 2], "keepPrunedConnections refills");
}

#[test]
fn level_distribution_follows_ml_without_a_cap() {
    let params = params(4, 16, 99);
    let levels: Vec<u8> = (0..200_000_u32).map(|row| params.draw_level(row)).collect();
    let above = |level: u8| levels.iter().filter(|l| **l >= level).count() as f64;
    // P(level >= l) = M^-l.
    let ratio1 = above(1) / levels.len() as f64;
    let ratio2 = above(2) / levels.len() as f64;
    assert!((ratio1 - 0.25).abs() < 0.01, "P(level >= 1) = {ratio1}");
    assert!((ratio2 - 0.0625).abs() < 0.005, "P(level >= 2) = {ratio2}");
    let top = levels.iter().copied().max().unwrap_or(0);
    assert!(top >= 6, "no artificial cap: top level {top}");
}

// ---------------------------------------------------------------------------
// Build variants
// ---------------------------------------------------------------------------

#[test]
fn sequential_build_is_valid_and_connected() {
    let data = clustered(1_500, 8, 12, 0, 5);
    let graph = HnswGraph::build(&data.vectors, params(8, 64, 9)).expect("build");
    assert_valid(&graph);
    assert_eq!(reachable(&graph), graph.len());
}

#[test]
fn sequential_build_is_deterministic_for_a_seed() {
    let data = clustered(800, 8, 8, 0, 21);
    let first = HnswGraph::build(&data.vectors, params(8, 48, 1234)).expect("build");
    let second = HnswGraph::build(&data.vectors, params(8, 48, 1234)).expect("build");
    assert_eq!(first, second);
    assert_eq!(first.to_bytes(), second.to_bytes());
    let other = HnswGraph::build(&data.vectors, params(8, 48, 4321)).expect("build");
    assert_ne!(first.to_bytes(), other.to_bytes());
}

#[test]
fn parallel_build_is_valid_connected_and_accurate() {
    let fixture = fixture();
    assert_valid(&fixture.graph);
    assert_eq!(reachable(&fixture.graph), fixture.graph.len());
}

#[test]
fn incremental_insert_extends_a_graph() {
    let data = clustered(2_000, 16, 16, 50, 31);
    let params = params(12, 96, 77);
    let mut graph = HnswGraph::new(params).expect("graph");
    let mut scratch = SearchScratch::new();
    for row in 0..1_000 {
        graph
            .insert(&data.vectors, row, &mut scratch)
            .expect("insert");
    }
    assert_eq!(
        graph.insert(&data.vectors, 1_500, &mut scratch),
        Err(GraphError::OutOfOrderInsert {
            row: 1_500,
            expected: 1_000
        })
    );
    for row in 1_000..2_000 {
        graph
            .insert(&data.vectors, row, &mut scratch)
            .expect("insert");
    }
    assert_eq!(
        graph.insert(&data.vectors, 2_000, &mut scratch),
        Err(GraphError::RowOutOfRange {
            row: 2_000,
            len: 2_000
        })
    );
    assert_valid(&graph);
    assert_eq!(
        graph,
        HnswGraph::build(&data.vectors, params).expect("build")
    );
    let mut total = 0.0;
    for query in &data.queries {
        let distance = data.vectors.query(query).expect("query");
        let output = graph.search(&distance, 10, 64, &mut scratch);
        total += recall(
            &output.neighbors,
            &brute_force(&data.vectors, query, 10, &AllRows),
        );
    }
    let mean = total / data.queries.len() as f64;
    assert!(mean >= 0.95, "recall@10 after incremental inserts: {mean}");
}

// ---------------------------------------------------------------------------
// Recall
// ---------------------------------------------------------------------------

#[test]
fn recall_at_10_on_clustered_10k_by_32() {
    let fixture = fixture();
    let mut scratch = SearchScratch::new();
    let mut total = 0.0;
    for query in &fixture.data.queries {
        let distance = fixture.data.vectors.query(query).expect("query");
        let output = fixture.graph.search(&distance, 10, 64, &mut scratch);
        assert_eq!(output.status, SearchStatus::Complete);
        total += recall(
            &output.neighbors,
            &brute_force(&fixture.data.vectors, query, 10, &AllRows),
        );
    }
    let mean = total / fixture.data.queries.len() as f64;
    eprintln!("unfiltered recall@10 (ef=64): {mean:.4}");
    assert!(mean >= 0.95, "recall@10 = {mean}");
}

#[test]
fn acorn_filtered_recall_across_selectivities() {
    let fixture = fixture();
    let rows = fixture.graph.len();
    for (selectivity, ef) in [(0.01, 64), (0.10, 64), (0.50, 64)] {
        let work = workload(fixture, 10, |index, _| {
            random_filter(rows, selectivity, 1_000 + index as u64)
        });
        let acorn = run_filtered(fixture, &work, FilterStrategy::acorn(), 10, ef);
        let admit = run_filtered(fixture, &work, FilterStrategy::Admit, 10, ef);
        eprintln!(
            "selectivity {selectivity}: acorn recall {:.4} ({:.0} dist, {:.0} two-hop), \
             admit recall {:.4} ({:.0} dist)",
            acorn.recall,
            acorn.distance_computations,
            acorn.two_hop,
            admit.recall,
            admit.distance_computations
        );
        assert!(
            acorn.recall >= 0.9,
            "acorn at {selectivity}: {}",
            acorn.recall
        );
        assert!(
            admit.recall >= 0.9,
            "admit at {selectivity}: {}",
            admit.recall
        );
    }
}

#[test]
fn acorn_handles_anti_correlated_filters() {
    let fixture = fixture();
    for selectivity in [0.02, 0.10] {
        let work = workload(fixture, 10, |_, query| {
            anti_correlated_filter(&fixture.data, query, selectivity)
        });
        let run = run_filtered(fixture, &work, FilterStrategy::acorn(), 10, 64);
        eprintln!(
            "anti-correlated {selectivity}: acorn recall {:.4} ({:.0} dist)",
            run.recall, run.distance_computations
        );
        assert!(
            run.recall >= 0.9,
            "anti-correlated {selectivity}: {}",
            run.recall
        );
    }
}

#[test]
fn deleted_rows_are_excluded_through_the_filter() {
    let fixture = fixture();
    let rows = fixture.graph.len();
    let mut live = RowBitset::full(rows);
    let mut rng = TestRng(404);
    for _ in 0..rows * 3 / 10 {
        live.remove(rng.below(rows) as u32);
    }
    // Also delete every row's nearest neighbor for the first queries, so the
    // unfiltered answer is always wrong.
    for query in fixture.data.queries.iter().take(20) {
        for row in brute_force(&fixture.data.vectors, query, 3, &AllRows) {
            live.remove(row);
        }
    }
    assert_eq!(live.cardinality_hint(), Some(live.count()));
    let work = workload(fixture, 10, |_, _| live.clone());
    let run = run_filtered(fixture, &work, FilterStrategy::Admit, 10, 64);
    assert!(run.recall >= 0.95, "recall with deletions: {}", run.recall);
    let acorn = run_filtered(fixture, &work, FilterStrategy::acorn(), 10, 64);
    assert!(
        acorn.recall >= 0.95,
        "acorn recall with deletions: {}",
        acorn.recall
    );
}

#[test]
fn suggest_picks_acorn_for_selective_filters() {
    let sparse = random_filter(1_000, 0.05, 1);
    let dense = random_filter(1_000, 0.8, 1);
    assert_eq!(
        FilterStrategy::suggest(&sparse, 1_000),
        FilterStrategy::acorn()
    );
    assert_eq!(
        FilterStrategy::suggest(&dense, 1_000),
        FilterStrategy::Admit
    );
    assert_eq!(
        FilterStrategy::suggest(&|row: u32| row.is_multiple_of(2), 1_000),
        FilterStrategy::Admit
    );
}

// ---------------------------------------------------------------------------
// Resumable search
// ---------------------------------------------------------------------------

#[test]
fn resumed_search_improves_on_the_first_pass() {
    let fixture = fixture();
    let mut scratch = SearchScratch::new();
    let (mut before, mut after) = (0.0, 0.0);
    for query in &fixture.data.queries {
        let distance = fixture.data.vectors.query(query).expect("query");
        let truth = brute_force(&fixture.data.vectors, query, 10, &AllRows);
        let mut cursor =
            fixture
                .graph
                .cursor(&distance, &AllRows, FilterStrategy::Admit, &mut scratch);
        let first = cursor.advance(10).output(10);
        let first_stats = cursor.stats();
        let second = cursor.advance(128).output(10);
        assert_eq!(cursor.ef(), 128);
        // Every rank is at least as close as before.
        for (old, new) in first.neighbors.iter().zip(&second.neighbors) {
            assert!(new.distance <= old.distance);
        }
        assert!(second.stats.distance_computations >= first_stats.distance_computations);
        // Shrinking ef keeps the wider state.
        let third = cursor.advance(16).output(10);
        assert_eq!(third.neighbors, second.neighbors);
        before += recall(&first.neighbors, &truth);
        after += recall(&second.neighbors, &truth);
    }
    let queries = fixture.data.queries.len() as f64;
    eprintln!(
        "resume: recall {:.4} at ef=10 -> {:.4} after extending to ef=128",
        before / queries,
        after / queries
    );
    assert!(after > before);
    assert!(after / queries >= 0.98);
}

#[test]
fn resuming_costs_no_more_than_restarting() {
    let fixture = fixture();
    let mut scratch = SearchScratch::new();
    for query in fixture.data.queries.iter().take(20) {
        let distance = fixture.data.vectors.query(query).expect("query");
        let narrow = fixture.graph.search(&distance, 10, 16, &mut scratch);
        let wide = fixture.graph.search(&distance, 10, 64, &mut scratch);
        let mut cursor =
            fixture
                .graph
                .cursor(&distance, &AllRows, FilterStrategy::Admit, &mut scratch);
        cursor.advance(16);
        let resumed = cursor.advance(64).output(10);
        // Every layer-0 row is evaluated at most once across both passes.
        assert!(resumed.stats.visited <= fixture.graph.len() as u64);
        assert!(
            resumed.stats.distance_computations
                <= narrow.stats.distance_computations + wide.stats.distance_computations
        );
        let truth = brute_force(&fixture.data.vectors, query, 10, &AllRows);
        assert!(recall(&resumed.neighbors, &truth) + 0.1 >= recall(&wide.neighbors, &truth));
    }
}

#[test]
fn fewer_than_k_results_are_distinguished_from_exhaustion() {
    let data = clustered(500, 8, 6, 5, 91);
    let graph = HnswGraph::build(&data.vectors, params(6, 48, 3)).expect("build");
    let chosen = [5_u32, 17, 300];
    let filter = |row: u32| chosen.contains(&row);
    let mut scratch = SearchScratch::new();
    for strategy in [FilterStrategy::Admit, FilterStrategy::acorn()] {
        for query in &data.queries {
            let distance = data.vectors.query(query).expect("query");
            let output = graph.search_filtered(&distance, &filter, strategy, 10, 10, &mut scratch);
            assert_eq!(output.status, SearchStatus::Exhausted, "{strategy:?}");
            let mut rows: Vec<u32> = output.neighbors.iter().map(|hit| hit.row).collect();
            rows.sort_unstable();
            assert_eq!(rows, chosen, "{strategy:?}");
        }
    }
    // Unfiltered: a beam wider than the graph visits every row.
    let distance = data.vectors.query(&data.queries[0]).expect("query");
    let mut cursor = graph.cursor(&distance, &AllRows, FilterStrategy::Admit, &mut scratch);
    assert_eq!(cursor.advance(10).output(10).status, SearchStatus::Complete);
    assert!(!cursor.is_exhausted());
    let all = cursor.advance(1_000).output(1_000);
    assert!(cursor.is_exhausted());
    assert_eq!(all.neighbors.len(), 500);
    assert_eq!(all.status, SearchStatus::Exhausted);
}

#[test]
fn narrow_acorn_pass_is_improved_by_widening() {
    let fixture = fixture();
    let rows = fixture.graph.len();
    let filter = random_filter(rows, 0.02, 55);
    let k = 40;
    let strategy = FilterStrategy::Acorn {
        candidate_budget: Some(1),
    };
    let mut scratch = SearchScratch::new();
    let (mut first_total, mut widened_total) = (0.0, 0.0);
    for query in fixture.data.queries.iter().take(20) {
        let distance = fixture.data.vectors.query(query).expect("query");
        let truth = brute_force(&fixture.data.vectors, query, k, &filter);
        let mut cursor = fixture
            .graph
            .cursor(&distance, &filter, strategy, &mut scratch);
        let first = recall(&cursor.advance(k).output(k).neighbors, &truth);
        let widened = recall(&cursor.advance(4 * k).output(k).neighbors, &truth);
        assert!(widened >= first);
        first_total += first;
        widened_total += widened;
    }
    eprintln!(
        "budget-1 acorn at 2%: recall@{k} {:.3} at ef={k}, {:.3} after widening to ef={}",
        first_total / 20.0,
        widened_total / 20.0,
        4 * k
    );
    assert!(widened_total / 20.0 >= 0.95);
}

#[test]
fn partial_status_is_reported_until_the_walk_is_exhausted() {
    let fixture = fixture();
    let rows = fixture.graph.len();
    let filter = random_filter(rows, 0.02, 56);
    // More results than rows match: the search can only end exhausted.
    let k = filter.count() + 10;
    let strategy = FilterStrategy::Acorn {
        candidate_budget: Some(1),
    };
    let mut scratch = SearchScratch::new();
    let mut partial_passes = 0;
    for query in fixture.data.queries.iter().take(10) {
        let distance = fixture.data.vectors.query(query).expect("query");
        let mut cursor = fixture
            .graph
            .cursor(&distance, &filter, strategy, &mut scratch);
        let mut ef = 16;
        let mut output = cursor.advance(ef).output(k);
        let mut found = output.neighbors.len();
        while output.status == SearchStatus::Partial {
            partial_passes += 1;
            assert!(!cursor.is_exhausted());
            ef *= 2;
            output = cursor.advance(ef).output(k);
            assert!(output.neighbors.len() >= found, "resuming never loses rows");
            found = output.neighbors.len();
        }
        assert_eq!(output.status, SearchStatus::Exhausted);
        assert!(cursor.is_exhausted());
        assert!(output.neighbors.iter().all(|hit| filter.contains(hit.row)));
        let coverage = found as f64 / filter.count() as f64;
        assert!(
            coverage >= 0.9,
            "exhausted walk reached {coverage} of matches"
        );
    }
    assert!(partial_passes > 0, "a budget of one defers two-hop bridges");
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

fn small_graph() -> (Clustered, HnswGraph) {
    let data = clustered(300, 6, 5, 10, 61);
    let graph = HnswGraph::build(&data.vectors, params(4, 32, 8)).expect("build");
    (data, graph)
}

fn reseal(bytes: &mut [u8]) {
    let body = bytes.len() - 4;
    let crc = crc32fast::hash(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn serialization_round_trips_graph_and_results() {
    let (data, graph) = small_graph();
    let bytes = graph.to_bytes();
    let loaded = HnswGraph::from_bytes(&bytes).expect("load");
    assert_eq!(loaded, graph);
    assert_eq!(loaded.to_bytes(), bytes);
    let mut scratch = SearchScratch::new();
    for query in &data.queries {
        let distance = data.vectors.query(query).expect("query");
        assert_eq!(
            loaded.search(&distance, 5, 32, &mut scratch),
            graph.search(&distance, 5, 32, &mut scratch)
        );
    }
    let empty = HnswGraph::new(HnswParams::default()).expect("graph");
    assert_eq!(HnswGraph::from_bytes(&empty.to_bytes()), Ok(empty));
}

#[test]
fn loaded_graph_accepts_incremental_inserts() {
    let data = clustered(400, 6, 5, 0, 62);
    let params = params(4, 32, 8);
    let mut scratch = SearchScratch::new();
    let mut graph = HnswGraph::new(params).expect("graph");
    for row in 0..200 {
        graph
            .insert(&data.vectors, row, &mut scratch)
            .expect("insert");
    }
    let mut loaded = HnswGraph::from_bytes(&graph.to_bytes()).expect("load");
    for row in 200..400 {
        loaded
            .insert(&data.vectors, row, &mut scratch)
            .expect("insert");
    }
    assert_eq!(
        loaded,
        HnswGraph::build(&data.vectors, params).expect("build")
    );
}

#[test]
fn deserialization_rejects_truncated_input() {
    let (_, graph) = small_graph();
    let bytes = graph.to_bytes();
    for len in 0..bytes.len() {
        assert!(
            HnswGraph::from_bytes(&bytes[..len]).is_err(),
            "prefix of {len} bytes"
        );
    }
}

#[test]
fn deserialization_rejects_every_single_byte_corruption() {
    let (_, graph) = small_graph();
    let bytes = graph.to_bytes();
    for index in 0..bytes.len() {
        let mut corrupted = bytes.clone();
        corrupted[index] ^= 0x5a;
        assert!(
            HnswGraph::from_bytes(&corrupted).is_err(),
            "flip at byte {index}"
        );
    }
    let mut extended = bytes.clone();
    extended.push(0);
    assert!(HnswGraph::from_bytes(&extended).is_err());
}

#[test]
fn deserialization_reports_specific_errors() {
    let (_, graph) = small_graph();
    let bytes = graph.to_bytes();

    let mut magic = bytes.clone();
    magic[0] = b'X';
    assert_eq!(HnswGraph::from_bytes(&magic), Err(GraphError::BadMagic));

    let mut version = bytes.clone();
    version[8] = 9;
    assert_eq!(
        HnswGraph::from_bytes(&version),
        Err(GraphError::UnsupportedVersion(9))
    );

    let mut checksum = bytes.clone();
    let last = checksum.len() - 1;
    checksum[last] ^= 1;
    assert!(matches!(
        HnswGraph::from_bytes(&checksum),
        Err(GraphError::ChecksumMismatch { .. })
    ));

    // Entry point out of range, with a valid checksum.
    let mut entry = bytes.clone();
    entry[36..40].copy_from_slice(&10_000_u32.to_le_bytes());
    reseal(&mut entry);
    assert!(matches!(
        HnswGraph::from_bytes(&entry),
        Err(GraphError::Corrupt(_))
    ));

    // First layer-0 neighbor out of range, with a valid checksum.
    let rows = graph.len();
    let layer0 = 40 + rows + 4 + 8 + 2 * rows;
    let mut neighbor = bytes.clone();
    neighbor[layer0..layer0 + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    reseal(&mut neighbor);
    assert!(matches!(
        HnswGraph::from_bytes(&neighbor),
        Err(GraphError::Corrupt(_))
    ));

    // A degree above the layer cap.
    let mut degree = bytes.clone();
    let degrees = 40 + rows + 4 + 8;
    degree[degrees..degrees + 2].copy_from_slice(&9_u16.to_le_bytes());
    reseal(&mut degree);
    assert!(matches!(
        HnswGraph::from_bytes(&degree),
        Err(GraphError::Corrupt(_))
    ));
}

#[test]
fn deserialization_never_panics_on_resealed_mutations() {
    let (data, graph) = small_graph();
    let bytes = graph.to_bytes();
    let mut rng = TestRng(2024);
    let mut scratch = SearchScratch::new();
    let distance = data.vectors.query(&data.queries[0]).expect("query");
    let mut accepted = 0;
    for _ in 0..3_000 {
        let mut mutated = bytes.clone();
        for _ in 0..1 + rng.below(3) {
            let index = rng.below(mutated.len() - 4);
            mutated[index] = rng.next_u64() as u8;
        }
        if rng.below(8) == 0 {
            let cut = rng.below(mutated.len() - 4);
            mutated.drain(cut..mutated.len() - 4);
        }
        reseal(&mut mutated);
        if let Ok(loaded) = HnswGraph::from_bytes(&mutated) {
            accepted += 1;
            assert_valid_bounds(&loaded);
            if loaded.len() <= data.vectors.len() {
                let _ = loaded.search(&distance, 5, 16, &mut scratch);
            }
        }
    }
    eprintln!("accepted {accepted} of 3000 resealed mutations");
}

/// The subset of invariants validation guarantees (no duplicate check).
fn assert_valid_bounds(graph: &HnswGraph) {
    for row in 0..graph.len() as u32 {
        let level = graph.level(row).expect("level");
        for layer in 0..=level {
            for &neighbor in graph.neighbors(row, layer) {
                assert!((neighbor as usize) < graph.len());
                assert!(graph.level(neighbor).is_some_and(|l| l >= layer));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Release-mode benchmarks (run with --release -- --ignored --nocapture)
// ---------------------------------------------------------------------------

fn unfiltered_sweep(data: &Clustered, graph: &HnswGraph, label: &str) {
    let truths: Vec<Vec<u32>> = data
        .queries
        .iter()
        .map(|query| brute_force(&data.vectors, query, 10, &AllRows))
        .collect();
    let mut scratch = SearchScratch::new();
    for ef in [16, 32, 64, 128, 256] {
        let started = Instant::now();
        let mut total = 0.0;
        let mut distances = 0;
        for (query, truth) in data.queries.iter().zip(&truths) {
            let distance = data.vectors.query(query).expect("query");
            let output = graph.search(&distance, 10, ef, &mut scratch);
            total += recall(&output.neighbors, truth);
            distances += output.stats.distance_computations;
        }
        let elapsed = started.elapsed().as_secs_f64();
        let queries = data.queries.len() as f64;
        eprintln!(
            "{label} ef={ef:>3}: recall@10 {:.4}, {:>8.0} QPS (1 thread), {:>6.0} dist/query",
            total / queries,
            queries / elapsed,
            distances as f64 / queries
        );
    }
}

#[test]
#[ignore = "release benchmark"]
fn release_unfiltered_recall_and_qps() {
    for (rows, dim, spread, sequential) in [
        (10_000, 32, 0.25, true),
        (100_000, 32, 0.25, true),
        (100_000, 32, 0.6, false),
        (100_000, 128, 0.25, false),
    ] {
        let data = clustered_with_spread(rows, dim, 256, 1_000, spread, 17);
        let label = format!("{rows}x{dim} spread {spread}");
        let started = Instant::now();
        let parallel =
            HnswGraph::build_parallel(&data.vectors, HnswParams::default()).expect("build");
        eprintln!(
            "{label}: parallel build {:.2}s ({} threads), graph {:.1} MiB, max level {}",
            started.elapsed().as_secs_f64(),
            rayon::current_num_threads(),
            parallel.memory_bytes() as f64 / (1024.0 * 1024.0),
            parallel.max_level()
        );
        unfiltered_sweep(&data, &parallel, &format!("{label} parallel"));
        if sequential {
            let started = Instant::now();
            let graph = HnswGraph::build(&data.vectors, HnswParams::default()).expect("build");
            eprintln!(
                "{label}: sequential build {:.2}s",
                started.elapsed().as_secs_f64()
            );
            unfiltered_sweep(&data, &graph, &format!("{label} sequential"));
        }
    }
}

#[test]
#[ignore = "release benchmark"]
fn release_filtered_recall_and_qps() {
    filtered_benchmark(0.25, &[0.001, 0.01, 0.1, 0.5, 0.9], &[0.01, 0.1, 0.5]);
}

#[test]
#[ignore = "release benchmark"]
fn release_filtered_overlapping_clusters() {
    filtered_benchmark(0.6, &[0.01, 0.1], &[0.01, 0.1, 0.5]);
}

fn filtered_benchmark(spread: f32, random: &[f64], anti_correlated: &[f64]) {
    eprintln!("100000x32, 256 clusters, spread {spread}");
    let data = clustered_with_spread(100_000, 32, 256, 200, spread, 23);
    let graph = HnswGraph::build_parallel(&data.vectors, HnswParams::default()).expect("build");
    let fixture = Fixture { data, graph };
    let rows = fixture.graph.len();
    let strategies = [
        ("acorn", FilterStrategy::acorn()),
        ("admit", FilterStrategy::Admit),
    ];
    let report = |label: &str, work: &Workload| {
        for ef in [32, 64, 128] {
            for (name, strategy) in strategies {
                if name == "admit" && label.contains("0.001") {
                    continue; // exact-scan territory; admit visits the whole graph
                }
                let run = run_filtered(&fixture, work, strategy, 10, ef);
                eprintln!(
                    "{label}: {name} ef={ef:>3} recall@10 {:.4}, {:>7.0} QPS, \
                     {:>7.0} dist/query, {:>6.0} two-hop/query",
                    run.recall, run.qps, run.distance_computations, run.two_hop
                );
            }
        }
    };
    for &selectivity in random {
        let work = workload(&fixture, 10, |index, _| {
            random_filter(rows, selectivity, 9_000 + index as u64)
        });
        report(&format!("random {selectivity:>5}"), &work);
    }
    for &selectivity in anti_correlated {
        let work = workload(&fixture, 10, |_, query| {
            anti_correlated_filter(&fixture.data, query, selectivity)
        });
        report(&format!("anti-correlated {selectivity:>4}"), &work);
    }
}

#[test]
#[ignore = "release benchmark"]
fn release_parallel_build_scaling() {
    let data = clustered_with_spread(50_000, 32, 256, 0, 0.25, 29);
    let started = Instant::now();
    let sequential = HnswGraph::build(&data.vectors, HnswParams::default()).expect("build");
    eprintln!(
        "50000x32 sequential build: {:.2}s",
        started.elapsed().as_secs_f64()
    );
    drop(sequential);
    for threads in [1, 2, 4] {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .expect("pool");
        let started = Instant::now();
        let graph = pool
            .install(|| HnswGraph::build_parallel(&data.vectors, HnswParams::default()))
            .expect("build");
        eprintln!(
            "50000x32 parallel build, {threads} threads: {:.2}s",
            started.elapsed().as_secs_f64()
        );
        assert_valid(&graph);
        // Lock-free reference workload, to separate machine scaling from
        // build contention.
        let started = Instant::now();
        let total: f64 = pool.install(|| {
            use rayon::prelude::*;
            (0..2_000_u32)
                .into_par_iter()
                .map(|a| {
                    (0..20_000_u32)
                        .map(|b| f64::from(data.vectors.distance_between(a, b)))
                        .sum::<f64>()
                })
                .sum()
        });
        eprintln!(
            "  reference scan, {threads} threads: {:.2}s ({total:.0})",
            started.elapsed().as_secs_f64()
        );
    }
}
