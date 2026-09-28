//! The planner's cost model (engine plan D6, Phase 5).
//!
//! A strategy's cost is the work it does, counted in the units the engine spends time on, and
//! priced in nanoseconds with constants measured on the benchmark harness
//! (`calibrate_cost_model` in `tests/recall.rs` prints a fresh fit):
//!
//! - **distance computations** over SQ8 codes or in f32 (a fixed part per call; the per-byte
//!   part is priced as resident bytes);
//! - **graph hops**: adjacency lists read, each link with its visited mark, and the filter
//!   checks and queue offers of a walk;
//! - **bytes touched**, split into resident bytes (streamed from memory), random reads (a
//!   resident row read at a random position, which costs a cache miss chain), and cold bytes (a
//!   read through the buffer cache from disk, from
//!   [`Residency`](logpose_storage::Residency)).
//!
//! On the calibration host, walks are memory-latency bound: an evaluated node costs about
//! 240 ns (a random read of its code, its visited mark, and its queue offers) and an expanded
//! list about 1 µs (32 visited marks), while an exact scan, scoring 64 rows per kernel call,
//! costs about 40 ns per row when the filter is dense (the codes stream) and about 100 ns when
//! it is sparse (each row is a random read).
//!
//! Per segment the planner prices an exact scan of the filter bitmap `B` (`n` rows) and, when
//! the segment has a graph, an admit-only walk and an ACORN-1 style walk, and runs the
//! cheapest, so the boundaries between strategies sit where the prices cross rather than at
//! fixed thresholds. The walk shapes ([`WalkShape`]) are fitted to the counters walks report:
//!
//! - an unfiltered walk expands about `ef` nodes, and evaluates a share of their links that
//!   shrinks as it expands more (the neighborhood saturates): `ν · links · x₀ · (x / x₀)^β`
//!   for `x` expansions against a reference `x₀`;
//! - an admit-only walk must expand about `ef / s` nodes to fill its beam at selectivity `s`
//!   (measured within 3 percent from 1 to 50 percent selectivity);
//! - an ACORN walk expands about `ef` matching nodes while two hops find enough matches
//!   (`s` above `knee / links`), and superlinearly more below; each expansion reads about
//!   `0.35 · (1 - s) · links` two-hop lists and checks every link against the filter.
//!
//! The walk estimates assume the filter is independent of the query. When it is not (an
//! anti-correlated filter), the walk's observed work exceeds its estimate; execution compares
//! the two and, once widening would take the walk past the exact scan's price, scans exactly
//! instead ([`CostModel::walk_budget`]).

use serde::{Deserialize, Serialize};

/// Work an operator does, in the model's units.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Work {
    /// Distance computations over SQ8 codes.
    pub sq8_distances: f64,
    /// Exact f32 distance computations.
    pub f32_distances: f64,
    /// Graph adjacency lists read (expansions and two-hop lists).
    pub hops: f64,
    /// Filter-bitmap membership tests.
    pub checks: f64,
    /// Candidates offered to a walk's frontier and result queues.
    pub offers: f64,
    /// Resident rows read at random positions (each costs a cache miss chain).
    pub random_reads: f64,
    /// Bytes streamed from resident sections.
    pub resident_bytes: f64,
    /// Bytes a fetch reads from disk first.
    pub cold_bytes: f64,
}

impl Work {
    /// All distance computations.
    #[must_use]
    pub fn distances(&self) -> f64 {
        self.sq8_distances + self.f32_distances
    }

    /// Every count scaled by `factor`.
    #[must_use]
    pub fn scaled(self, factor: f64) -> Self {
        Self {
            sq8_distances: self.sq8_distances * factor,
            f32_distances: self.f32_distances * factor,
            hops: self.hops * factor,
            checks: self.checks * factor,
            offers: self.offers * factor,
            random_reads: self.random_reads * factor,
            resident_bytes: self.resident_bytes * factor,
            cold_bytes: self.cold_bytes,
        }
    }
}

/// The shape of graph walks, fitted to the counters walks report.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalkShape {
    /// Expansions of an unfiltered walk per unit of beam width `ef`.
    pub expansions_per_ef: f64,
    /// Expansions of an unfiltered walk independent of `ef`.
    pub expansions_base: f64,
    /// Share `ν` of an expanded node's links that are new at the reference expansion count.
    pub novelty: f64,
    /// The reference expansion count `x₀` (an unfiltered walk at `ef = 64`).
    pub reference_expansions: f64,
    /// Exponent `β` of the saturation: evaluated nodes grow as `x^β` in the expansions.
    pub saturation: f64,
    /// ACORN walks expand about `ef` nodes above selectivity `knee / links`.
    pub acorn_knee: f64,
    /// Below the knee, ACORN expansions grow as `(knee / (links · s))^γ`.
    pub acorn_growth: f64,
    /// Two-hop lists an ACORN expansion reads, as a share of its non-matching links.
    pub acorn_two_hops: f64,
    /// Matching links an ACORN expansion evaluates, as a share of `links · s`.
    pub acorn_matches: f64,
    /// Work of a filtered ACORN walk's convergence probe (one doubling of `ef`), as a factor.
    pub acorn_probe: f64,
}

impl Default for WalkShape {
    fn default() -> Self {
        CALIBRATED.walk
    }
}

/// Prices of the units of [`Work`], in nanoseconds, and the walk shape.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct CostModel {
    /// One SQ8 distance, fixed part (dispatch, code lookup, heap offer).
    pub sq8_ns: f64,
    /// One f32 distance, fixed part.
    pub f32_ns: f64,
    /// One resident byte streamed (and computed on).
    pub resident_ns_per_byte: f64,
    /// One resident row read at a random position: the first cache miss.
    pub random_read_ns: f64,
    /// Each further 64-byte line of a randomly read row.
    pub random_line_ns: f64,
    /// One candidate offered to a walk's queues (visited mark, frontier and result heaps).
    pub offer_ns: f64,
    /// One adjacency list read, fixed part.
    pub hop_ns: f64,
    /// Each link of an adjacency list read (its visited mark).
    pub link_ns: f64,
    /// One filter-bitmap membership test.
    pub check_ns: f64,
    /// One cold byte read (disk bandwidth through the buffer cache).
    pub cold_ns_per_byte: f64,
    /// The walk shape.
    pub walk: WalkShape,
}

/// The constants measured on the benchmark host (4 cores, 100,000 x 128 clustered vectors
/// in one segment, `M = 16`, release build). `cold_ns_per_byte` is not measured (the harness
/// runs warm); it assumes about 1 GB/s through the buffer cache.
pub const CALIBRATED: CostModel = CostModel {
    sq8_ns: 25.0,
    f32_ns: 30.0,
    resident_ns_per_byte: 0.12,
    random_read_ns: 45.0,
    random_line_ns: 15.0,
    offer_ns: 140.0,
    hop_ns: 50.0,
    link_ns: 30.0,
    check_ns: 15.0,
    cold_ns_per_byte: 1.0,
    walk: WalkShape {
        expansions_per_ef: 1.0,
        expansions_base: 1.0,
        novelty: 0.43,
        reference_expansions: 65.0,
        saturation: 0.64,
        acorn_knee: 3.0,
        acorn_growth: 1.6,
        acorn_two_hops: 0.35,
        acorn_matches: 0.6,
        acorn_probe: 1.6,
    },
};

impl Default for CostModel {
    fn default() -> Self {
        CALIBRATED
    }
}

/// What the planner knows about one segment before choosing how to search it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SegmentShape {
    /// Rows stored, live or not (graph nodes stand for rows).
    pub rows: u64,
    /// Live rows matching the filter (`n`, exact).
    pub matched: u64,
    /// Vector dimensions.
    pub dims: usize,
    /// Nodes of the graph, when the segment has one usable for walks.
    pub graph_nodes: Option<u64>,
    /// Layer-0 links per node of the graph (`2M`).
    pub links: usize,
    /// Whether the segment has SQ8 codes.
    pub sq8: bool,
    /// Bytes of the segment's filter and vector index sections that are cold.
    pub index_cold_bytes: u64,
    /// Beam width `ef`.
    pub ef: usize,
    /// Candidates the unit contributes to the rerank.
    pub budget: usize,
    /// Whether a filter excludes live rows.
    pub filtered: bool,
    /// Whether a filtered ACORN walk probes convergence by widening once.
    pub probe: bool,
}

impl SegmentShape {
    /// Filter selectivity over the graph's nodes (deleted rows are nodes too).
    #[must_use]
    pub fn selectivity(&self) -> f64 {
        let rows = self.graph_nodes.unwrap_or(self.rows).max(1) as f64;
        (self.matched as f64 / rows).clamp(0.0, 1.0)
    }

    /// Share of the segment's rows that match.
    #[must_use]
    pub fn density(&self) -> f64 {
        (self.matched as f64 / self.rows.max(1) as f64).clamp(0.0, 1.0)
    }
}

/// A way to search one segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Choice {
    /// Score every row of `B` over SQ8 codes (or in f32 without codes).
    ExactScan,
    /// Walk the graph admitting only rows of `B`.
    GraphAdmit,
    /// Walk the graph ACORN-1 style over `B`.
    GraphAcorn,
}

impl Choice {
    /// The stable name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::ExactScan => "exact_scan",
            Self::GraphAdmit => "graph_admit",
            Self::GraphAcorn => "graph_acorn",
        }
    }
}

/// A priced alternative.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Priced {
    /// The alternative.
    pub choice: Choice,
    /// Its estimated work.
    pub work: Work,
    /// Its estimated cost in microseconds.
    pub micros: f64,
}

/// The planner's decision for one segment.
#[derive(Clone, Debug, PartialEq)]
pub struct Decision {
    /// The cheapest alternative (or the one a forced strategy picks).
    pub chosen: Priced,
    /// The exact scan's price, which also bounds an anti-correlated walk.
    pub exact: Priced,
    /// Every alternative considered, cheapest first.
    pub alternatives: Vec<Priced>,
    /// Why, in words.
    pub reason: String,
}

/// Restricts the planner to one family of strategies (for tests and benchmarks).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Force {
    /// The cheapest strategy.
    #[default]
    Auto,
    /// Always scan exactly.
    Exact,
    /// Always walk when the segment has a graph and more rows match than the budget (the
    /// cheaper walk); the walk never falls back to an exact scan.
    Walk,
    /// As [`Force::Walk`], always admit-only.
    Admit,
    /// As [`Force::Walk`], always ACORN-1 style (admit-only without a filter).
    Acorn,
}

impl Force {
    /// Whether walks are forced (and so never fall back to an exact scan).
    #[must_use]
    pub fn walks(self) -> bool {
        matches!(self, Self::Walk | Self::Admit | Self::Acorn)
    }
}

impl CostModel {
    /// Microseconds `work` costs over vectors of `dims` dimensions in a graph of `links`
    /// layer-0 links per node.
    #[must_use]
    pub fn micros(&self, work: &Work, dims: usize, links: usize) -> f64 {
        let lines = (dims as f64 / 64.0).ceil().max(1.0) - 1.0;
        let ns = work.sq8_distances * self.sq8_ns
            + work.f32_distances * self.f32_ns
            + work.hops * (self.hop_ns + self.link_ns * links as f64)
            + work.checks * self.check_ns
            + work.offers * self.offer_ns
            + work.random_reads * (self.random_read_ns + self.random_line_ns * lines)
            + work.resident_bytes * self.resident_ns_per_byte
            + work.cold_bytes * self.cold_ns_per_byte;
        ns / 1_000.0
    }

    fn priced(&self, choice: Choice, work: Work, shape: &SegmentShape) -> Priced {
        Priced {
            choice,
            work,
            micros: self.micros(&work, shape.dims, shape.links),
        }
    }

    /// The exact scan of `B`: one distance per matching row (SQ8 when the segment has codes),
    /// streaming the codes when `B` is dense and reading them at random when it is sparse.
    #[must_use]
    pub fn exact_scan(&self, shape: &SegmentShape) -> Work {
        let n = shape.matched as f64;
        let dims = shape.dims as f64;
        let sparse = (1.0 - shape.density()).powi(2);
        if shape.sq8 {
            Work {
                sq8_distances: n,
                random_reads: n * sparse,
                resident_bytes: n * dims,
                cold_bytes: shape.index_cold_bytes as f64,
                ..Work::default()
            }
        } else {
            Work {
                f32_distances: n,
                random_reads: n * sparse,
                resident_bytes: n * dims * 4.0,
                cold_bytes: shape.index_cold_bytes as f64,
                ..Work::default()
            }
        }
    }

    /// Expansions of an unfiltered walk with beam `ef`.
    fn expansions(&self, ef: usize) -> f64 {
        self.walk.expansions_base + self.walk.expansions_per_ef * ef as f64
    }

    /// Nodes a walk evaluates in `expansions` expansions: `ν · links · x₀ · (x / x₀)^β`, at
    /// most `cap`.
    fn evaluated(&self, expansions: f64, links: usize, cap: f64) -> f64 {
        let reference = self.walk.reference_expansions.max(1.0);
        let evaluated = self.walk.novelty
            * links as f64
            * reference
            * (expansions / reference).powf(self.walk.saturation);
        evaluated.min(cap)
    }

    /// Work a walk that expands `expansions` nodes, reads `hops` lists, and evaluates
    /// `evaluated` nodes does.
    fn walk_work(&self, shape: &SegmentShape, hops: f64, checks: f64, evaluated: f64) -> Work {
        Work {
            sq8_distances: evaluated,
            hops,
            checks,
            offers: evaluated,
            random_reads: evaluated,
            resident_bytes: evaluated * shape.dims as f64,
            cold_bytes: shape.index_cold_bytes as f64,
            ..Work::default()
        }
    }

    /// An admit-only walk: it expands non-matching nodes too, so filling a beam of `ef`
    /// matches takes about `1 / s` times the expansions of an unfiltered walk.
    #[must_use]
    pub fn admit_walk(&self, shape: &SegmentShape) -> Work {
        let nodes = shape.graph_nodes.unwrap_or(shape.rows).max(1) as f64;
        let s = shape.selectivity().max(1.0 / nodes);
        let expansions = (self.expansions(shape.ef) / s).min(nodes);
        let evaluated = self.evaluated(expansions, shape.links, nodes);
        self.walk_work(shape, expansions, evaluated, evaluated)
    }

    /// An ACORN-1 style walk: it expands matching nodes only, checks every link it reads
    /// against the filter, and reads the lists of non-matching neighbors (two hops) to reach
    /// matches beyond them. Below the knee two hops find too few matches and it expands
    /// superlinearly more.
    #[must_use]
    pub fn acorn_walk(&self, shape: &SegmentShape) -> Work {
        let shape_walk = &self.walk;
        let n = shape.matched.max(1) as f64;
        let s = shape.selectivity().max(1e-9);
        let links = shape.links.max(1) as f64;
        let knee = shape_walk.acorn_knee / links;
        let growth = (knee / s).max(1.0).powf(shape_walk.acorn_growth);
        let probe = if shape.probe {
            shape_walk.acorn_probe
        } else {
            1.0
        };
        let expansions = (self.expansions(shape.ef) * growth).min(n);
        let two_hops =
            expansions * shape_walk.acorn_two_hops * (1.0 - s) * links * (s / knee).min(1.0);
        let evaluated = (expansions * (1.0 + shape_walk.acorn_matches * links * s)).min(n);
        let hops = expansions + two_hops;
        self.walk_work(shape, hops, hops * links, evaluated)
            .scaled(probe)
    }

    /// Choose how to search a segment. A walk is considered only with a graph and SQ8 codes,
    /// and only when more rows match than the unit contributes candidates (a walk cannot
    /// return more rows than match, so below that it would visit the whole matching set
    /// anyway); an ACORN walk only under a filter that excludes rows.
    #[must_use]
    pub fn decide(&self, shape: &SegmentShape, force: Force) -> Decision {
        let exact = self.priced(Choice::ExactScan, self.exact_scan(shape), shape);
        let walkable =
            shape.graph_nodes.is_some() && shape.sq8 && shape.matched > shape.budget as u64;
        let mut alternatives = vec![exact];
        if walkable {
            alternatives.push(self.priced(Choice::GraphAdmit, self.admit_walk(shape), shape));
            if shape.filtered {
                alternatives.push(self.priced(Choice::GraphAcorn, self.acorn_walk(shape), shape));
            }
        }
        alternatives.sort_by(|left, right| left.micros.total_cmp(&right.micros));
        let named = |choice: Choice| {
            alternatives
                .iter()
                .copied()
                .find(|priced| priced.choice == choice)
        };
        let cheapest_walk = alternatives
            .iter()
            .copied()
            .find(|priced| priced.choice != Choice::ExactScan);
        let forced = match force {
            Force::Auto => None,
            Force::Exact => Some((Some(exact), "exact scan forced")),
            Force::Walk => Some((cheapest_walk, "walk forced")),
            Force::Admit => Some((named(Choice::GraphAdmit), "admit-only walk forced")),
            Force::Acorn => Some((
                named(Choice::GraphAcorn).or_else(|| named(Choice::GraphAdmit)),
                "ACORN walk forced",
            )),
        };
        let (chosen, reason) = match forced {
            Some((Some(chosen), why)) => (chosen, why.to_owned()),
            _ if !walkable => {
                let why = if shape.graph_nodes.is_none() || !shape.sq8 {
                    "no graph".to_owned()
                } else {
                    format!(
                        "{} matching rows fit the {} candidates",
                        shape.matched, shape.budget
                    )
                };
                (exact, why)
            }
            _ => {
                let chosen = alternatives.first().copied().unwrap_or(exact);
                let others = alternatives
                    .iter()
                    .skip(1)
                    .map(|other| format!("{} {:.0}us", other.choice.name(), other.micros))
                    .collect::<Vec<_>>()
                    .join(", ");
                (
                    chosen,
                    format!(
                        "cheapest at selectivity {:.4}: {} {:.0}us vs {others}",
                        shape.selectivity(),
                        chosen.choice.name(),
                        chosen.micros
                    ),
                )
            }
        };
        Decision {
            chosen,
            exact,
            alternatives,
            reason,
        }
    }

    /// The most a walk may spend (in microseconds of estimated work) before it stops widening
    /// and the unit is scanned exactly instead: the exact scan's price.
    #[must_use]
    pub fn walk_budget(&self, decision: &Decision) -> f64 {
        decision.exact.micros
    }

    /// An admit-only walk whose visited rows match the filter less often than this share of
    /// the filter's selectivity is treated as anti-correlated and widens its beam: the
    /// independence the walk estimates assume does not hold.
    pub const ANTI_CORRELATION_RATIO: f64 = 0.75;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(rows: u64, matched: u64) -> SegmentShape {
        SegmentShape {
            rows,
            matched,
            dims: 128,
            graph_nodes: Some(rows),
            links: 32,
            sq8: true,
            index_cold_bytes: 0,
            ef: 64,
            budget: 40,
            filtered: matched < rows,
            probe: matched < rows,
        }
    }

    fn choice(rows: u64, matched: u64) -> Choice {
        CostModel::default()
            .decide(&shape(rows, matched), Force::Auto)
            .chosen
            .choice
    }

    fn price(decision: &Decision, choice: Choice) -> f64 {
        decision
            .alternatives
            .iter()
            .find(|priced| priced.choice == choice)
            .map(|priced| priced.micros)
            .unwrap_or(f64::INFINITY)
    }

    /// The smallest matching count at which the choice stops being `below`, by bisection.
    fn boundary(rows: u64, mut low: u64, mut high: u64, below: Choice) -> u64 {
        assert_eq!(choice(rows, low), below, "rows {rows} at {low}");
        assert_ne!(choice(rows, high), below, "rows {rows} at {high}");
        while high - low > 1 {
            let mid = low + (high - low) / 2;
            if choice(rows, mid) == below {
                low = mid;
            } else {
                high = mid;
            }
        }
        high
    }

    #[test]
    fn calibrated_prices_match_the_measured_work() {
        // Measured on the calibration host at 100,000 x 128, ef 64 (see the module docs).
        let model = CostModel::default();
        let unfiltered = model.decide(&shape(100_000, 100_000), Force::Auto);
        assert_eq!(unfiltered.chosen.choice, Choice::GraphAdmit);
        let walk = unfiltered.chosen.micros;
        assert!(
            (250.0..360.0).contains(&walk),
            "unfiltered walk {walk}us, measured 290"
        );
        let scan = unfiltered.exact.micros;
        assert!((3_500.0..5_000.0).contains(&scan), "full scan {scan}us");
        let price = |matched| {
            model
                .decide(&shape(100_000, matched), Force::Auto)
                .exact
                .micros
        };
        let sparse = price(1_000);
        assert!(
            (80.0..140.0).contains(&sparse),
            "1% scan {sparse}us, measured 100"
        );
        let tenth = price(10_000);
        assert!(
            (750.0..1_100.0).contains(&tenth),
            "10% scan {tenth}us, measured 800 to 950"
        );
    }

    #[test]
    fn unfiltered_segments_scan_exactly_up_to_a_size_then_walk() {
        let crossover = (1_000..1_000_000)
            .step_by(500)
            .find(|rows| choice(*rows, *rows) != Choice::ExactScan)
            .expect("large segments walk");
        assert!(
            (4_000..10_000).contains(&crossover),
            "crossover {crossover}"
        );
        assert_eq!(choice(crossover, crossover), Choice::GraphAdmit);
        let decision = CostModel::default().decide(&shape(crossover, crossover), Force::Auto);
        let (exact, walk) = (
            price(&decision, Choice::ExactScan),
            price(&decision, Choice::GraphAdmit),
        );
        assert!(walk <= exact && exact < walk * 1.1, "{walk} vs {exact}");
    }

    #[test]
    fn filters_switch_from_exact_scans_to_walks_where_the_prices_cross() {
        let model = CostModel::default();
        for (rows, lowest, highest) in [
            (100_000_u64, 0.05, 0.3),
            (1_000_000, 0.01, 0.08),
            (10_000_000, 0.002, 0.03),
        ] {
            let switch = boundary(rows, 41, rows, Choice::ExactScan);
            let selectivity = switch as f64 / rows as f64;
            assert!(
                (lowest..highest).contains(&selectivity),
                "rows {rows}: walks from selectivity {selectivity}"
            );
            // Just below the boundary the exact scan is cheaper, at it a walk; the prices are
            // continuous, so they are within a hair of each other there.
            let at = model.decide(&shape(rows, switch), Force::Auto);
            let below = model.decide(&shape(rows, switch - 1), Force::Auto);
            assert_eq!(below.chosen.choice, Choice::ExactScan);
            assert_ne!(at.chosen.choice, Choice::ExactScan);
            let (exact, walk) = (at.exact.micros, at.chosen.micros);
            assert!(
                (exact - walk).abs() / exact < 0.01,
                "rows {rows}: {exact} vs {walk}"
            );
            // Every selectivity past the boundary keeps walking.
            for percent in [2_u64, 5, 10, 25, 50, 90] {
                let matched = rows * percent / 100;
                if matched > switch {
                    assert_ne!(
                        choice(rows, matched),
                        Choice::ExactScan,
                        "rows {rows} {percent}%"
                    );
                }
            }
        }
    }

    #[test]
    fn acorn_is_priced_under_a_filter_and_grows_below_its_knee() {
        let model = CostModel::default();
        let decision = model.decide(&shape(1_000_000, 100_000), Force::Auto);
        assert!(price(&decision, Choice::GraphAcorn).is_finite());
        let unfiltered = model.decide(&shape(1_000_000, 1_000_000), Force::Auto);
        assert!(price(&unfiltered, Choice::GraphAcorn).is_infinite());
        // Above the knee (3 / 32 links) the expansions stay near ef; below it they grow.
        let at = |percent: f64| {
            let matched = (1_000_000.0 * percent) as u64;
            let mut shape = shape(1_000_000, matched);
            shape.probe = false;
            model.acorn_walk(&shape).hops
        };
        assert!(at(0.01) > at(0.2));
        let forced = model.decide(&shape(1_000_000, 20_000), Force::Acorn);
        assert_eq!(forced.chosen.choice, Choice::GraphAcorn);
        assert_eq!(forced.reason, "ACORN walk forced");
    }

    /// ACORN never wins on the calibration workload's shape (128 dimensions, `M = 16`,
    /// `ef = 64`) but does where a scanned row costs more next to a hop: at 768 dimensions over
    /// a million rows, a 5 percent filter walks ACORN-1 style.
    #[test]
    fn acorn_wins_at_higher_dimensions_but_not_at_the_calibration_shape() {
        let model = CostModel::default();
        let choice_at = |dims: usize, matched: u64| {
            let mut shape = shape(1_000_000, matched);
            shape.dims = dims;
            model.decide(&shape, Force::Auto).chosen.choice
        };
        for matched in (1..=100).map(|percent| percent * 10_000) {
            assert_ne!(
                choice_at(128, matched),
                Choice::GraphAcorn,
                "{matched} rows"
            );
        }
        assert_eq!(choice_at(768, 50_000), Choice::GraphAcorn);
    }

    #[test]
    fn a_walk_needs_more_matches_than_candidates() {
        let decision = CostModel::default().decide(&shape(1_000_000, 40), Force::Walk);
        assert_eq!(decision.chosen.choice, Choice::ExactScan);
        assert!(
            decision.reason.contains("fit the 40 candidates"),
            "{}",
            decision.reason
        );
        let decision = CostModel::default().decide(&shape(1_000_000, 41), Force::Walk);
        assert_ne!(decision.chosen.choice, Choice::ExactScan);
    }

    #[test]
    fn cold_sections_are_charged_to_every_alternative() {
        let mut cold = shape(100_000, 100_000);
        cold.index_cold_bytes = 20_000_000;
        let decision = CostModel::default().decide(&cold, Force::Auto);
        for priced in &decision.alternatives {
            assert!(priced.work.cold_bytes >= 20_000_000.0);
            assert!(priced.micros > 20_000.0);
        }
        assert_eq!(decision.chosen.choice, Choice::GraphAdmit);
    }

    #[test]
    fn segments_without_codes_scan_exactly_in_f32() {
        let mut small = shape(900, 900);
        small.graph_nodes = None;
        small.sq8 = false;
        let decision = CostModel::default().decide(&small, Force::Walk);
        assert_eq!(decision.chosen.choice, Choice::ExactScan);
        assert_eq!(decision.chosen.work.f32_distances, 900.0);
        assert_eq!(decision.reason, "no graph");
    }
}
