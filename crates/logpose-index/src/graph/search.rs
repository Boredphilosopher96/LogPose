//! Beam search, filtered traversal and resumable cursors.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::ops::RangeInclusive;

use super::{FilterStrategy, HnswGraph, QueryDistance, RowFilter};

/// Read access to adjacency lists, shared by the frozen graph and the
/// locked lists used during parallel builds.
pub(super) trait Links {
    /// Replaces `out` with the links of `row` on `level`.
    fn neighbors_into(&self, row: u32, level: usize, out: &mut Vec<u32>);
}

/// A row with its distance, ordered by distance then row id.
#[derive(Clone, Copy, Debug)]
pub(super) struct Scored {
    pub(super) dist: f32,
    pub(super) row: u32,
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
        self.dist
            .total_cmp(&other.dist)
            .then(self.row.cmp(&other.row))
    }
}

/// A search hit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Neighbor {
    /// Row id.
    pub row: u32,
    /// Distance from the query; lower is closer.
    pub distance: f32,
}

/// Work counters for one search (summed across cursor extensions).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SearchStats {
    /// Rows marked visited on layer 0, including two-hop bridge rows.
    pub visited: u64,
    /// Query-to-row distance computations on every layer.
    pub distance_computations: u64,
    /// Filter checks that rejected a row.
    pub filtered_out: u64,
    /// Candidates popped from the frontier and expanded.
    pub expansions: u64,
    /// Non-matching rows whose links were scanned to reach matching rows
    /// two hops away ([`FilterStrategy::Acorn`] only).
    pub two_hop_expansions: u64,
}

/// Why a search returned what it did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchStatus {
    /// `k` results were found.
    Complete,
    /// Fewer than `k` results so far, but unexplored candidates remain;
    /// extending the cursor with a larger `ef` can find more.
    Partial,
    /// Fewer than `k` results, and the traversal has nothing left to
    /// explore. The caller should fall back to an exact scan if it needs
    /// more.
    Exhausted,
}

/// Results of a search.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchOutput {
    /// Up to `k` hits, closest first.
    pub neighbors: Vec<Neighbor>,
    /// Work counters.
    pub stats: SearchStats,
    /// Completion state.
    pub status: SearchStatus,
}

/// Generation-stamped visited set: resetting is O(1) except on wrap-around.
#[derive(Clone, Debug, Default)]
pub(super) struct VisitedSet {
    stamps: Vec<u32>,
    generation: u32,
}

impl VisitedSet {
    /// Starts a new traversal over rows `0..len`.
    pub(super) fn reset(&mut self, len: usize) {
        if self.stamps.len() < len {
            self.stamps.resize(len, 0);
        }
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.stamps.fill(0);
            self.generation = 1;
        }
    }

    /// Marks `row`; returns `true` when it was not yet visited. Rows outside
    /// the reset range count as visited so they are never explored.
    #[inline]
    pub(super) fn insert(&mut self, row: u32) -> bool {
        match self.stamps.get_mut(row as usize) {
            Some(stamp) if *stamp != self.generation => {
                *stamp = self.generation;
                true
            }
            _ => false,
        }
    }

    #[inline]
    fn contains(&self, row: u32) -> bool {
        self.stamps
            .get(row as usize)
            .is_none_or(|stamp| *stamp == self.generation)
    }
}

/// Frontier, results and the rows pruned so far.
#[derive(Clone, Debug, Default)]
pub(super) struct Queues {
    /// Unexpanded candidates, closest first.
    pub(super) frontier: BinaryHeap<Reverse<Scored>>,
    /// Admitted rows, worst on top, at most `ef`.
    pub(super) results: BinaryHeap<Scored>,
    /// Evaluated rows pruned by the result bound, never expanded.
    deferred: Vec<Scored>,
    /// Admitted rows pushed out of `results` by closer ones.
    evicted: Vec<Scored>,
}

impl Queues {
    pub(super) fn clear(&mut self) {
        self.frontier.clear();
        self.results.clear();
        self.deferred.clear();
        self.evicted.clear();
    }

    /// Offers an evaluated row: it joins the frontier (and the results when
    /// admitted) if it beats the current bound, otherwise it is deferred.
    #[inline]
    pub(super) fn offer(&mut self, candidate: Scored, admitted: bool, ef: usize) {
        let within_bound = self.results.len() < ef
            || self
                .results
                .peek()
                .is_some_and(|worst| candidate.dist < worst.dist);
        if !within_bound {
            self.deferred.push(candidate);
            return;
        }
        self.frontier.push(Reverse(candidate));
        if admitted {
            self.results.push(candidate);
            self.trim(ef);
        }
    }

    fn trim(&mut self, ef: usize) {
        while self.results.len() > ef {
            if let Some(worst) = self.results.pop() {
                self.evicted.push(worst);
            }
        }
    }

    #[inline]
    fn should_stop(&self, candidate: Scored, ef: usize) -> bool {
        self.results.len() >= ef
            && self
                .results
                .peek()
                .is_some_and(|worst| candidate.dist > worst.dist)
    }
}

/// Traversal state reused across searches and insertions.
#[derive(Clone, Debug, Default)]
pub(super) struct BeamState {
    pub(super) visited: VisitedSet,
    pub(super) queues: Queues,
    pub(super) neighbors: Vec<u32>,
    hop_neighbors: Vec<u32>,
    hops: Vec<u32>,
    /// An expansion's newly visited neighbors and their distances, scored in one call.
    fresh: Vec<u32>,
    fresh_distances: Vec<f32>,
    /// Two-hop bridges skipped because an expansion hit its budget.
    deferred_hops: Vec<u32>,
}

impl BeamState {
    /// Starts a traversal over rows `0..len`.
    pub(super) fn reset(&mut self, len: usize) {
        self.visited.reset(len);
        self.queues.clear();
        self.deferred_hops.clear();
    }

    /// Seeds the traversal with an already evaluated row.
    pub(super) fn seed<F: RowFilter + ?Sized>(
        &mut self,
        seed: Scored,
        filter: &F,
        stats: &mut SearchStats,
    ) {
        if !self.visited.insert(seed.row) {
            return;
        }
        stats.visited += 1;
        self.queues.frontier.push(Reverse(seed));
        if filter.contains(seed.row) {
            self.queues.results.push(seed);
        } else {
            stats.filtered_out += 1;
        }
    }

    fn is_exhausted(&self) -> bool {
        self.queues.frontier.is_empty()
            && self.queues.deferred.is_empty()
            && self
                .deferred_hops
                .iter()
                .all(|hop| self.visited.contains(*hop))
    }
}

/// Buffers used by insertion, kept here so a single scratch serves both.
#[derive(Clone, Debug, Default)]
pub(super) struct BuildBuffers {
    pub(super) layer: Vec<Scored>,
    pub(super) selected: Vec<Vec<u32>>,
    pub(super) candidates: Vec<Scored>,
    pub(super) keep: Vec<Scored>,
    pub(super) pruned: Vec<Scored>,
    pub(super) links: Vec<u32>,
}

/// Reusable per-thread search state.
///
/// Holds the visited array (one `u32` stamp per row, reset in O(1) by
/// bumping a generation), the heaps and the neighbor buffers. Keep one per
/// worker thread and pass it to every search to avoid per-query allocation.
#[derive(Clone, Debug, Default)]
pub struct SearchScratch {
    pub(super) beam: BeamState,
    pub(super) build: BuildBuffers,
}

impl SearchScratch {
    /// Empty scratch; buffers grow to the graph size on first use.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Internal traversal mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    Admit,
    Acorn { budget: usize },
}

/// One layer's beam search over some adjacency.
pub(super) struct Walk<'a, L: ?Sized, Q: ?Sized, F: ?Sized> {
    pub(super) links: &'a L,
    pub(super) level: usize,
    pub(super) query: &'a Q,
    pub(super) filter: &'a F,
    pub(super) mode: Mode,
    pub(super) ef: usize,
}

impl<L, Q, F> Walk<'_, L, Q, F>
where
    L: Links + ?Sized,
    Q: QueryDistance + ?Sized,
    F: RowFilter + ?Sized,
{
    /// Expands frontier candidates until the closest one is farther than the
    /// worst of `ef` results, or the frontier is empty.
    pub(super) fn run(&self, state: &mut BeamState, stats: &mut SearchStats) {
        let BeamState {
            visited,
            queues,
            neighbors,
            hop_neighbors,
            hops,
            fresh,
            fresh_distances,
            deferred_hops,
        } = state;
        while let Some(&Reverse(current)) = queues.frontier.peek() {
            if queues.should_stop(current, self.ef) {
                break;
            }
            queues.frontier.pop();
            stats.expansions += 1;
            self.links
                .neighbors_into(current.row, self.level, neighbors);
            match self.mode {
                Mode::Admit => {
                    // Score the new neighbors in one call, then offer them in link order:
                    // the same offers as scoring each in turn.
                    fresh.clear();
                    fresh.extend(neighbors.iter().copied().filter(|row| visited.insert(*row)));
                    fresh_distances.clear();
                    fresh_distances.resize(fresh.len(), 0.0);
                    self.query.distances(fresh, fresh_distances);
                    stats.distance_computations += fresh.len() as u64;
                    for (&row, &dist) in fresh.iter().zip(fresh_distances.iter()) {
                        stats.visited += 1;
                        let admitted = self.filter.contains(row);
                        if !admitted {
                            stats.filtered_out += 1;
                        }
                        queues.offer(Scored { dist, row }, admitted, self.ef);
                    }
                }
                Mode::Acorn { budget } => {
                    // Until the first match is found, walk the unfiltered
                    // graph so a non-matching entry region cannot strand the
                    // search.
                    let bootstrap = queues.results.is_empty();
                    let mut found = 0_usize;
                    hops.clear();
                    for &row in neighbors.iter() {
                        if self.filter.contains(row) {
                            if visited.insert(row) {
                                stats.visited += 1;
                                self.evaluate(row, true, queues, stats);
                                found += 1;
                            }
                            continue;
                        }
                        stats.filtered_out += 1;
                        if bootstrap {
                            if visited.insert(row) {
                                stats.visited += 1;
                                self.evaluate(row, false, queues, stats);
                            }
                        } else if !visited.contains(row) {
                            hops.push(row);
                        }
                    }
                    for &hop in hops.iter() {
                        if found >= budget {
                            deferred_hops.push(hop);
                            continue;
                        }
                        found += self.expand_hop(hop, visited, queues, hop_neighbors, stats);
                    }
                    // Dead end: two hops found no new match while the results
                    // are still short. The matching subgraph is locally
                    // disconnected, so keep the bridges as navigation-only
                    // candidates; the distance-ordered frontier then crosses
                    // the gap toward the next matching region. Doing this
                    // once the results are full costs far more distance
                    // work for little recall, so it stops there.
                    if found == 0 && queues.results.len() < self.ef {
                        for &hop in hops.iter() {
                            self.evaluate(hop, false, queues, stats);
                        }
                    }
                }
            }
        }
    }

    /// Scans the links of the non-matching row `hop` and evaluates matching
    /// rows among them. Returns how many new matches it found.
    pub(super) fn expand_hop(
        &self,
        hop: u32,
        visited: &mut VisitedSet,
        queues: &mut Queues,
        buffer: &mut Vec<u32>,
        stats: &mut SearchStats,
    ) -> usize {
        if !visited.insert(hop) {
            return 0;
        }
        stats.visited += 1;
        stats.two_hop_expansions += 1;
        self.links.neighbors_into(hop, self.level, buffer);
        let mut found = 0;
        for &row in buffer.iter() {
            if !self.filter.contains(row) {
                stats.filtered_out += 1;
                continue;
            }
            if visited.insert(row) {
                stats.visited += 1;
                self.evaluate(row, true, queues, stats);
                found += 1;
            }
        }
        found
    }

    #[inline]
    fn evaluate(&self, row: u32, admitted: bool, queues: &mut Queues, stats: &mut SearchStats) {
        let dist = self.query.distance(row);
        stats.distance_computations += 1;
        queues.offer(Scored { dist, row }, admitted, self.ef);
    }
}

/// Greedy descent over `levels`, highest first. Level 0 is never visited.
///
/// Returns the closest row found and, when `filter` is given, the closest
/// matching row seen on the way down, which seeds filtered layer-0 searches
/// whose matches lie far from the query.
pub(super) fn descend<L, Q, F>(
    links: &L,
    query: &Q,
    filter: Option<&F>,
    start: Scored,
    levels: RangeInclusive<usize>,
    buffer: &mut Vec<u32>,
    stats: &mut SearchStats,
) -> (Scored, Option<Scored>)
where
    L: Links + ?Sized,
    Q: QueryDistance + ?Sized,
    F: RowFilter + ?Sized,
{
    let mut current = start;
    let mut best_match = filter
        .filter(|filter| filter.contains(start.row))
        .map(|_| start);
    let (low, top) = levels.into_inner();
    for level in (low.max(1)..=top).rev() {
        loop {
            let mut improved = false;
            links.neighbors_into(current.row, level, buffer);
            for &row in buffer.iter() {
                let candidate = Scored {
                    dist: query.distance(row),
                    row,
                };
                stats.distance_computations += 1;
                if let Some(filter) = filter
                    && best_match.is_none_or(|best| candidate < best)
                    && filter.contains(row)
                {
                    best_match = Some(candidate);
                }
                if candidate < current {
                    current = candidate;
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }
    }
    (current, best_match)
}

/// A search that can be extended to a larger `ef` without restarting.
///
/// The cursor keeps the frontier, the rows the result bound pruned, rows
/// evicted from the result set, deferred two-hop bridges and the visited
/// set. [`SearchCursor::advance`] with a larger `ef` re-offers the pruned
/// rows under the looser bound and continues the same traversal, so the
/// best `k` can only improve and no distance is computed twice.
pub struct SearchCursor<'a, Q: ?Sized, F: ?Sized> {
    graph: &'a HnswGraph,
    query: &'a Q,
    filter: &'a F,
    mode: Mode,
    state: &'a mut BeamState,
    ef: usize,
    stats: SearchStats,
}

impl<'a, Q, F> SearchCursor<'a, Q, F>
where
    Q: QueryDistance + ?Sized,
    F: RowFilter + ?Sized,
{
    pub(super) fn start(
        graph: &'a HnswGraph,
        query: &'a Q,
        filter: &'a F,
        strategy: FilterStrategy,
        scratch: &'a mut SearchScratch,
    ) -> Self {
        let mode = match strategy {
            FilterStrategy::Admit => Mode::Admit,
            FilterStrategy::Acorn { candidate_budget } => Mode::Acorn {
                budget: candidate_budget
                    .unwrap_or(graph.params().max_links(0))
                    .max(1),
            },
        };
        let state = &mut scratch.beam;
        state.reset(graph.len());
        let mut stats = SearchStats::default();
        if let Some(entry) = graph.entry_point() {
            let start = Scored {
                dist: query.distance(entry),
                row: entry,
            };
            stats.distance_computations += 1;
            let (closest, closest_match) = descend(
                graph,
                query,
                Some(filter),
                start,
                1..=graph.max_level(),
                &mut state.neighbors,
                &mut stats,
            );
            state.seed(closest, filter, &mut stats);
            if let Some(seed) = closest_match {
                state.seed(seed, filter, &mut stats);
            }
        }
        Self {
            graph,
            query,
            filter,
            mode,
            state,
            ef: 0,
            stats,
        }
    }

    /// Runs (or continues) the layer-0 search with beam width `ef`.
    ///
    /// Growing `ef` resumes from the saved frontier and re-offers every row
    /// that an earlier, tighter bound pruned. A smaller or equal `ef` keeps
    /// the current width.
    pub fn advance(&mut self, ef: usize) -> &mut Self {
        let ef = ef.max(1);
        let walk = Walk {
            links: self.graph,
            level: 0,
            query: self.query,
            filter: self.filter,
            mode: self.mode,
            ef: ef.max(self.ef),
        };
        if ef > self.ef {
            self.ef = ef;
            let state = &mut *self.state;
            let queues = &mut state.queues;
            let evicted = std::mem::take(&mut queues.evicted);
            queues.results.extend(evicted.iter().copied());
            queues.trim(ef);
            let mut deferred = std::mem::take(&mut queues.deferred);
            deferred.sort_unstable();
            for candidate in deferred.drain(..) {
                let admitted = self.filter.contains(candidate.row);
                queues.offer(candidate, admitted, ef);
            }
            if queues.deferred.is_empty() {
                queues.deferred = deferred;
            }
            let hops = std::mem::take(&mut state.deferred_hops);
            for &hop in &hops {
                walk.expand_hop(
                    hop,
                    &mut state.visited,
                    &mut state.queues,
                    &mut state.hop_neighbors,
                    &mut self.stats,
                );
            }
        }
        walk.run(self.state, &mut self.stats);
        self
    }

    /// The best `k` rows found so far, closest first.
    pub fn top_k(&self, k: usize) -> Vec<Neighbor> {
        let mut hits: Vec<Neighbor> = self
            .state
            .queues
            .results
            .iter()
            .map(|hit| Neighbor {
                row: hit.row,
                distance: hit.dist,
            })
            .collect();
        hits.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance).then(a.row.cmp(&b.row)));
        hits.truncate(k);
        hits
    }

    /// Current beam width.
    pub fn ef(&self) -> usize {
        self.ef
    }

    /// Accumulated work counters.
    pub fn stats(&self) -> SearchStats {
        self.stats
    }

    /// Returns `true` when nothing is left to explore: no frontier, no
    /// pruned rows and no deferred two-hop bridges.
    pub fn is_exhausted(&self) -> bool {
        self.state.is_exhausted()
    }

    /// Best `k` rows with stats and completion status.
    pub fn output(&self, k: usize) -> SearchOutput {
        let neighbors = self.top_k(k);
        let status = if neighbors.len() >= k {
            SearchStatus::Complete
        } else if self.is_exhausted() {
            SearchStatus::Exhausted
        } else {
            SearchStatus::Partial
        };
        SearchOutput {
            neighbors,
            stats: self.stats,
            status,
        }
    }
}

impl HnswGraph {
    /// Approximate `k` nearest rows with beam width `max(ef, k)`.
    pub fn search<Q: QueryDistance + ?Sized>(
        &self,
        query: &Q,
        k: usize,
        ef: usize,
        scratch: &mut SearchScratch,
    ) -> SearchOutput {
        self.search_filtered(
            query,
            &super::AllRows,
            FilterStrategy::Admit,
            k,
            ef,
            scratch,
        )
    }

    /// Approximate `k` nearest rows admitted by `filter`, with beam width
    /// `max(ef, k)`.
    pub fn search_filtered<Q, F>(
        &self,
        query: &Q,
        filter: &F,
        strategy: FilterStrategy,
        k: usize,
        ef: usize,
        scratch: &mut SearchScratch,
    ) -> SearchOutput
    where
        Q: QueryDistance + ?Sized,
        F: RowFilter + ?Sized,
    {
        if k == 0 {
            return SearchOutput {
                neighbors: Vec::new(),
                stats: SearchStats::default(),
                status: SearchStatus::Complete,
            };
        }
        let mut cursor = self.cursor(query, filter, strategy, scratch);
        cursor.advance(ef.max(k));
        cursor.output(k)
    }

    /// Starts a resumable search. Upper layers are descended immediately;
    /// call [`SearchCursor::advance`] to search layer 0.
    pub fn cursor<'a, Q, F>(
        &'a self,
        query: &'a Q,
        filter: &'a F,
        strategy: FilterStrategy,
        scratch: &'a mut SearchScratch,
    ) -> SearchCursor<'a, Q, F>
    where
        Q: QueryDistance + ?Sized,
        F: RowFilter + ?Sized,
    {
        SearchCursor::start(self, query, filter, strategy, scratch)
    }
}

#[cfg(test)]
mod tests {
    use super::VisitedSet;

    #[test]
    fn visited_generation_wraparound_clears_old_stamps() {
        let mut visited = VisitedSet::default();
        visited.reset(4);
        assert!(visited.insert(1));
        // Jump to the last generation, stamp a row, then wrap.
        visited.generation = u32::MAX - 1;
        visited.reset(4);
        assert_eq!(visited.generation, u32::MAX);
        assert!(visited.insert(2));
        assert!(!visited.insert(2));
        visited.reset(4);
        assert_eq!(visited.generation, 1);
        // Neither the row stamped with u32::MAX nor a stale stamp equal to
        // the restarted generation may read as visited.
        assert!(visited.stamps.iter().all(|stamp| *stamp == 0));
        for row in 0..4 {
            assert!(!visited.contains(row));
            assert!(visited.insert(row));
        }
        // Rows beyond the reset range always count as visited.
        assert!(!visited.insert(4));
        assert!(visited.contains(4));
    }
}
