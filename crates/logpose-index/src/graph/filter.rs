//! Row filters and filtered-traversal strategies.

/// A set of admissible rows, typically a predicate bitmap minus deletions.
pub trait RowFilter {
    /// Returns `true` when `row` may appear in results.
    fn contains(&self, row: u32) -> bool;

    /// Number of admissible rows, when cheaply known.
    fn cardinality_hint(&self) -> Option<usize> {
        None
    }
}

/// A filter that admits every row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AllRows;

impl RowFilter for AllRows {
    #[inline]
    fn contains(&self, _row: u32) -> bool {
        true
    }
}

impl<F: Fn(u32) -> bool> RowFilter for F {
    #[inline]
    fn contains(&self, row: u32) -> bool {
        self(row)
    }
}

/// A fixed-size bitset over row ids that tracks its cardinality.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowBitset {
    words: Vec<u64>,
    len: usize,
    count: usize,
}

impl RowBitset {
    /// An empty set over rows `0..len`.
    pub fn new(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
            len,
            count: 0,
        }
    }

    /// A set containing every row in `0..len`.
    pub fn full(len: usize) -> Self {
        let mut words = vec![u64::MAX; len.div_ceil(64)];
        if let Some(last) = words.last_mut()
            && !len.is_multiple_of(64)
        {
            *last = (1_u64 << (len % 64)) - 1;
        }
        Self {
            words,
            len,
            count: len,
        }
    }

    /// Number of rows the set ranges over.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when the set ranges over zero rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of rows in the set.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Adds `row`; returns `true` when it was absent. Out-of-range rows are
    /// ignored.
    pub fn insert(&mut self, row: u32) -> bool {
        let (word, bit) = Self::position(row);
        match self.words.get_mut(word) {
            Some(slot) if (row as usize) < self.len && *slot & bit == 0 => {
                *slot |= bit;
                self.count += 1;
                true
            }
            _ => false,
        }
    }

    /// Removes `row`; returns `true` when it was present.
    pub fn remove(&mut self, row: u32) -> bool {
        let (word, bit) = Self::position(row);
        match self.words.get_mut(word) {
            Some(slot) if *slot & bit != 0 => {
                *slot &= !bit;
                self.count -= 1;
                true
            }
            _ => false,
        }
    }

    fn position(row: u32) -> (usize, u64) {
        ((row / 64) as usize, 1_u64 << (row % 64))
    }
}

impl RowFilter for RowBitset {
    #[inline]
    fn contains(&self, row: u32) -> bool {
        let (word, bit) = Self::position(row);
        self.words.get(word).is_some_and(|slot| slot & bit != 0)
    }

    fn cardinality_hint(&self) -> Option<usize> {
        Some(self.count)
    }
}

/// How a filtered search treats rows that fail the filter.
///
/// Neither strategy is exact. Both lose recall when the matching rows sit
/// far from the query in regions the graph barely links (anti-correlated
/// filters): on 50k clustered rows with 10 percent of rows matching,
/// recall@10 at `ef = 64` fell to about 0.86 (ACORN) and 0.90 (admit),
/// recovering to 0.99 at `ef = 256`. Use [`Self::suggest`], which sends
/// small filters to an exact scan, and widen a [`crate::graph::SearchCursor`]
/// when a result must be trusted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FilterStrategy {
    /// Walk the full graph, using non-matching rows for navigation but
    /// admitting only matching rows into the results. Best when most rows
    /// match; its cost grows as the filter gets more selective.
    #[default]
    Admit,
    /// ACORN-1 style walk for selective filters: expand only matching rows,
    /// and bridge through non-matching neighbors by looking at their
    /// neighbors (two hops). Non-matching rows are evaluated only to
    /// navigate: before the first match is found, and at dead ends where two
    /// hops reveal no new match while the results are still short.
    Acorn {
        /// Newly discovered matching candidates collected per expansion
        /// before remaining two-hop bridges are deferred. `None` uses `2M`.
        candidate_budget: Option<usize>,
    },
}

/// What [`FilterStrategy::suggest`] recommends for one filtered search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterPlan {
    /// Score every matching row directly. Few enough rows match that this
    /// costs less than any graph walk, and it is exact. The graph does not
    /// do this; the caller scans its filter.
    ExactScan,
    /// Walk the graph with this strategy.
    Graph(FilterStrategy),
}

impl FilterStrategy {
    /// Selectivity (`matching / total`) below which [`Self::suggest`] picks
    /// [`FilterStrategy::Acorn`] over [`FilterStrategy::Admit`].
    /// Provisional; the planner's benchmark harness (design PR 12, plan
    /// Phase 5) calibrates it together with
    /// [`Self::EXACT_SCAN_MAX_MATCHES`].
    pub const ACORN_SELECTIVITY_THRESHOLD: f64 = 0.3;

    /// Matching-row count at or below which [`Self::suggest`] picks
    /// [`FilterPlan::ExactScan`]. Provisional. At 50k to 100k rows a graph
    /// walk over a filter of a few thousand rows computes as many distances
    /// as scanning them all (and tens of times more for 0.1 percent or
    /// anti-correlated filters), without being exact.
    pub const EXACT_SCAN_MAX_MATCHES: usize = 2_048;

    /// ACORN with the default candidate budget.
    pub const fn acorn() -> Self {
        Self::Acorn {
            candidate_budget: None,
        }
    }

    /// Picks a plan from the filter's cardinality hint over `rows` rows: an
    /// exact scan for at most [`Self::EXACT_SCAN_MAX_MATCHES`] matches,
    /// [`FilterStrategy::Acorn`] below [`Self::ACORN_SELECTIVITY_THRESHOLD`],
    /// and [`FilterStrategy::Admit`] otherwise. Without a hint it walks with
    /// [`FilterStrategy::Admit`].
    pub fn suggest<F: RowFilter + ?Sized>(filter: &F, rows: usize) -> FilterPlan {
        let Some(count) = filter.cardinality_hint() else {
            return FilterPlan::Graph(Self::Admit);
        };
        if count <= Self::EXACT_SCAN_MAX_MATCHES || rows == 0 {
            return FilterPlan::ExactScan;
        }
        let selectivity = count as f64 / rows as f64;
        if selectivity < Self::ACORN_SELECTIVITY_THRESHOLD {
            FilterPlan::Graph(Self::acorn())
        } else {
            FilterPlan::Graph(Self::Admit)
        }
    }
}
