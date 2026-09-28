//! Insertion, the neighbor-selection heuristic and the parallel builder.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Mutex, PoisonError};

use rayon::prelude::*;

use super::distance::RowQuery;
use super::search::{BuildBuffers, Descent, Links, Mode, Scored, SearchStats, Walk};
use super::{AllRows, GraphError, HnswGraph, HnswParams, SearchScratch, VectorSource};

/// Rows inserted on one thread before the parallel phase, so that early
/// concurrent inserts find a connected graph.
const PARALLEL_WARMUP_ROWS: usize = 256;

/// Read-modify-write access to adjacency lists during insertion.
pub(super) trait LinkWriter {
    /// Calls `edit(current, out)` on the links of `row` at `level`; when it
    /// returns `true` the links are replaced by `out`. Implementations hold
    /// the row's lock for the whole call, so the edit is atomic.
    fn update<F>(&mut self, row: u32, level: usize, out: &mut Vec<u32>, edit: F)
    where
        F: FnOnce(&[u32], &mut Vec<u32>) -> bool;
}

/// Where a new row enters the graph.
#[derive(Clone, Copy, Debug)]
pub(super) struct InsertTarget {
    /// Row being inserted.
    pub(super) row: u32,
    /// Its level.
    pub(super) level: usize,
    /// Current entry point.
    pub(super) entry: u32,
    /// Level of the entry point.
    pub(super) top: usize,
    /// Rows addressable in the graph (visited-set size).
    pub(super) rows: usize,
}

/// Finds and selects neighbors for `target.row` on every level it shares
/// with the graph, storing them in `scratch.build.selected`. Returns the
/// number of levels to link (`min(level, top) + 1`).
pub(super) fn plan_links<L, V>(
    links: &L,
    source: &V,
    params: &HnswParams,
    target: InsertTarget,
    scratch: &mut SearchScratch,
) -> usize
where
    L: Links + ?Sized,
    V: VectorSource + ?Sized,
{
    let query = RowQuery {
        source,
        row: target.row,
    };
    let mut stats = SearchStats::default();
    let SearchScratch { beam, build, .. } = scratch;
    let start = Scored {
        dist: source.distance_between(target.row, target.entry),
        row: target.entry,
    };
    let descent = Descent {
        links,
        query: &query,
        filter: None::<&AllRows>,
        low: target.level + 1,
        top: target.top,
        rows: target.rows,
    };
    descent.run(start, beam, &mut build.layer, &mut stats);
    let linked = target.level.min(target.top) + 1;
    if build.selected.len() < linked {
        build.selected.resize_with(linked, Vec::new);
    }
    let ef = params.build_ef();
    for level in (0..linked).rev() {
        beam.reset(target.rows);
        for &seed in &build.layer {
            beam.seed(seed, &AllRows, &mut stats);
        }
        let walk = Walk {
            links,
            level,
            query: &query,
            filter: &AllRows,
            mode: Mode::Admit,
            ef,
        };
        walk.run(beam, &mut stats);
        build.layer.clear();
        build.layer.extend(
            beam.queues
                .results
                .drain()
                .filter(|hit| hit.row != target.row),
        );
        build.layer.sort_unstable();
        let BuildBuffers {
            layer,
            selected,
            keep,
            pruned,
            ..
        } = &mut *build;
        select_neighbors(source, layer, params.m, keep, pruned);
        if let Some(list) = selected.get_mut(level) {
            list.clear();
            list.extend(keep.iter().map(|hit| hit.row));
        }
    }
    linked
}

/// Writes the planned links of `row` and the reverse links on each
/// neighbor, pruning neighbors that overflow with the heuristic.
pub(super) fn commit_links<W, V>(
    writer: &mut W,
    source: &V,
    params: &HnswParams,
    row: u32,
    linked: usize,
    scratch: &mut SearchScratch,
) where
    W: LinkWriter + ?Sized,
    V: VectorSource + ?Sized,
{
    let BuildBuffers {
        selected,
        candidates,
        keep,
        pruned,
        links,
        ..
    } = &mut scratch.build;
    for (level, list) in selected.iter().enumerate().take(linked) {
        writer.update(row, level, links, |_, out| {
            out.clear();
            out.extend_from_slice(list);
            true
        });
    }
    for (level, list) in selected.iter().enumerate().take(linked) {
        let cap = params.max_links(level);
        for &neighbor in list {
            writer.update(neighbor, level, links, |current, out| {
                add_reverse_link(
                    source, neighbor, row, current, cap, out, candidates, keep, pruned,
                )
            });
        }
    }
}

/// Adds `new` to `base`'s links, re-selecting with the heuristic when the
/// list is full. Returns `false` when nothing changes.
#[allow(clippy::too_many_arguments)]
fn add_reverse_link<V: VectorSource + ?Sized>(
    source: &V,
    base: u32,
    new: u32,
    current: &[u32],
    cap: usize,
    out: &mut Vec<u32>,
    candidates: &mut Vec<Scored>,
    keep: &mut Vec<Scored>,
    pruned: &mut Vec<Scored>,
) -> bool {
    if base == new || current.contains(&new) {
        return false;
    }
    out.clear();
    if current.len() < cap {
        out.extend_from_slice(current);
        out.push(new);
        return true;
    }
    candidates.clear();
    candidates.extend(
        current
            .iter()
            .chain(std::iter::once(&new))
            .map(|&row| Scored {
                dist: source.distance_between(base, row),
                row,
            }),
    );
    candidates.sort_unstable();
    select_neighbors(source, candidates, cap, keep, pruned);
    out.extend(keep.iter().map(|hit| hit.row));
    true
}

/// Neighbor-selection heuristic (Malkov and Yashunin, algorithm 4) with
/// `keepPrunedConnections`.
///
/// `candidates` must be sorted closest first by distance to the base row. A
/// candidate is kept when it is closer to the base than to every row kept
/// so far, which spreads links across directions instead of clustering them.
/// Pruned candidates then fill any remaining room, closest first.
pub(super) fn select_neighbors<V: VectorSource + ?Sized>(
    source: &V,
    candidates: &[Scored],
    m: usize,
    keep: &mut Vec<Scored>,
    pruned: &mut Vec<Scored>,
) {
    keep.clear();
    pruned.clear();
    for &candidate in candidates {
        if keep.len() >= m {
            break;
        }
        let diverse = keep
            .iter()
            .all(|kept| source.distance_between(candidate.row, kept.row) >= candidate.dist);
        if diverse {
            keep.push(candidate);
        } else {
            pruned.push(candidate);
        }
    }
    for &candidate in pruned.iter() {
        if keep.len() >= m {
            break;
        }
        keep.push(candidate);
    }
}

/// Adjacency shared by parallel build workers.
///
/// Slots use the frozen graph's flat layout (`[count, links..]`) but hold
/// atomics, so readers never lock: a read racing with a write may see a mix
/// of old and new links, all of which are valid row ids, which only perturbs
/// the search order. Writers serialize per row on a mutex so every
/// read-modify-write of a link list is atomic.
struct SharedLinks {
    /// Graph whose layout (levels, upper blocks) addresses the slots; its
    /// link storage is moved into the atomics below until [`Self::freeze`].
    layout: HnswGraph,
    layer0: Vec<AtomicU32>,
    upper: Vec<AtomicU32>,
    locks: Vec<Mutex<()>>,
}

impl SharedLinks {
    fn new(mut layout: HnswGraph) -> Self {
        let (layer0, upper) = layout.take_storage();
        let rows = layout.len();
        Self {
            layout,
            layer0: layer0.into_iter().map(AtomicU32::new).collect(),
            upper: upper.into_iter().map(AtomicU32::new).collect(),
            locks: (0..rows).map(|_| Mutex::new(())).collect(),
        }
    }

    fn slot(&self, row: u32, level: usize) -> Option<&[AtomicU32]> {
        match self.layout.slot_range(row, level)? {
            (true, range) => self.layer0.get(range),
            (false, range) => self.upper.get(range),
        }
    }

    fn read(&self, row: u32, level: usize, out: &mut Vec<u32>) {
        out.clear();
        if let Some((count, links)) = self.slot(row, level).and_then(<[_]>::split_first) {
            let count = (count.load(Ordering::Acquire) as usize).min(links.len());
            out.extend(
                links
                    .iter()
                    .take(count)
                    .map(|link| link.load(Ordering::Relaxed)),
            );
        }
        // A read racing a write may mix old and new links, but every value
        // ever stored in a slot is a valid row id other than the slot's own.
        debug_assert!(
            out.iter()
                .all(|link| (*link as usize) < self.locks.len() && *link != row),
            "lock-free read of row {row} level {level} saw an invalid link"
        );
    }

    fn freeze(self) -> HnswGraph {
        let Self {
            mut layout,
            layer0,
            upper,
            ..
        } = self;
        layout.restore_storage(
            layer0.into_iter().map(AtomicU32::into_inner).collect(),
            upper.into_iter().map(AtomicU32::into_inner).collect(),
        );
        layout
    }
}

impl Links for SharedLinks {
    fn neighbors_into(&self, row: u32, level: usize, out: &mut Vec<u32>) {
        self.read(row, level, out);
    }
}

/// A worker's write handle, with a buffer for the list being edited.
struct SharedWriter<'a> {
    links: &'a SharedLinks,
    current: Vec<u32>,
}

impl LinkWriter for SharedWriter<'_> {
    fn update<F>(&mut self, row: u32, level: usize, out: &mut Vec<u32>, edit: F)
    where
        F: FnOnce(&[u32], &mut Vec<u32>) -> bool,
    {
        let Some(lock) = self.links.locks.get(row as usize) else {
            return;
        };
        let _guard = lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.links.read(row, level, &mut self.current);
        if !edit(&self.current, out) {
            return;
        }
        if let Some((count, links)) = self.links.slot(row, level).and_then(<[_]>::split_first) {
            let written = out.len().min(links.len());
            for (link, value) in links.iter().zip(out.iter()) {
                link.store(*value, Ordering::Relaxed);
            }
            count.store(written as u32, Ordering::Release);
        }
    }
}

fn insert_shared<V: VectorSource + ?Sized>(
    links: &SharedLinks,
    source: &V,
    params: &HnswParams,
    target: InsertTarget,
    scratch: &mut SearchScratch,
) {
    let linked = plan_links(links, source, params, target, scratch);
    let mut writer = SharedWriter {
        links,
        current: Vec::with_capacity(params.max_links(0)),
    };
    commit_links(&mut writer, source, params, target.row, linked, scratch);
}

/// Parallel build: levels are drawn up front, the first row with the top
/// level becomes the fixed entry point, a short prefix is inserted on this
/// thread, and the rest are inserted on the `rayon` pool.
pub(super) fn build_parallel<V: VectorSource + ?Sized>(
    source: &V,
    params: HnswParams,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<HnswGraph, GraphError> {
    params.validate()?;
    let rows = source.len();
    if rows >= u32::MAX as usize {
        return Err(GraphError::TooLarge);
    }
    let mut layout = HnswGraph::with_capacity(params, rows)?;
    let levels: Vec<u8> = (0..rows as u32).map(|row| params.draw_level(row)).collect();
    let Some((entry, top)) = levels
        .iter()
        .enumerate()
        .rev()
        .max_by_key(|(_, level)| **level)
        .map(|(row, level)| (row as u32, usize::from(*level)))
    else {
        return Ok(layout);
    };
    for &level in &levels {
        layout.push_node(level)?;
    }
    let shared = SharedLinks::new(layout);
    let target = |row: u32| InsertTarget {
        row,
        level: levels
            .get(row as usize)
            .map_or(0, |level| usize::from(*level)),
        entry,
        top,
        rows,
    };

    let mut scratch = SearchScratch::new();
    let warmup = rows.min(PARALLEL_WARMUP_ROWS) as u32;
    for row in (0..warmup).filter(|row| *row != entry) {
        if cancelled() {
            return Err(GraphError::Cancelled);
        }
        insert_shared(&shared, source, &params, target(row), &mut scratch);
    }

    let stop = AtomicBool::new(false);
    let pool: Vec<Mutex<SearchScratch>> = (0..=rayon::current_num_threads())
        .map(|_| Mutex::new(SearchScratch::new()))
        .collect();
    (warmup..rows as u32)
        .into_par_iter()
        .filter(|row| *row != entry)
        .for_each(|row| {
            // Once cancelled, the remaining rows are skipped (each costs one poll).
            if stop.load(Ordering::Relaxed) || cancelled() {
                stop.store(true, Ordering::Relaxed);
                return;
            }
            let slot = rayon::current_thread_index()
                .and_then(|index| pool.get(index))
                .or_else(|| pool.last());
            if let Some(slot) = slot {
                let mut scratch = slot.lock().unwrap_or_else(PoisonError::into_inner);
                insert_shared(&shared, source, &params, target(row), &mut scratch);
            }
        });

    if stop.load(Ordering::Relaxed) {
        return Err(GraphError::Cancelled);
    }
    let mut graph = shared.freeze();
    graph.set_entry(Some(entry), top as u8);
    Ok(graph)
}
