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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FilterStrategy {
    /// Walk the full graph, using non-matching rows for navigation but
    /// admitting only matching rows into the results. Best when most rows
    /// match.
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

impl FilterStrategy {
    /// Selectivity (`matching / total`) below which [`Self::suggest`] picks
    /// [`FilterStrategy::Acorn`]. Provisional; the benchmark harness
    /// calibrates it together with the exact-scan threshold.
    pub const ACORN_SELECTIVITY_THRESHOLD: f64 = 0.3;

    /// ACORN with the default candidate budget.
    pub const fn acorn() -> Self {
        Self::Acorn {
            candidate_budget: None,
        }
    }

    /// Picks a strategy from the filter's cardinality hint over `rows` rows.
    /// Without a hint it returns [`FilterStrategy::Admit`].
    pub fn suggest<F: RowFilter + ?Sized>(filter: &F, rows: usize) -> Self {
        match filter.cardinality_hint() {
            Some(count) if rows > 0 => {
                let selectivity = count as f64 / rows as f64;
                if selectivity < Self::ACORN_SELECTIVITY_THRESHOLD {
                    Self::acorn()
                } else {
                    Self::Admit
                }
            }
            _ => Self::Admit,
        }
    }
}
