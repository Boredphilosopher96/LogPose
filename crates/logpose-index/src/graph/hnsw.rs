//! The frozen, search-optimized graph layout.

use std::ops::Range;

use super::build::{self, LinkWriter};
use super::search::Links;
use super::{GraphError, SearchScratch, VectorSource};

/// Default `M`: links per node on upper layers (`2M` on layer 0).
pub const DEFAULT_M: usize = 16;
/// Default beam width while building.
pub const DEFAULT_EF_CONSTRUCTION: usize = 128;
/// Largest supported `M`.
pub const MAX_M: usize = 1024;

/// Sentinel for "no upper-layer block" in [`HnswGraph::upper_block`].
const NO_BLOCK: u32 = u32::MAX;

/// Graph construction parameters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HnswParams {
    /// Links per node on upper layers; layer 0 allows `2 * m`.
    pub m: usize,
    /// Beam width used to find neighbor candidates during insertion.
    pub ef_construction: usize,
    /// Seed for level assignment. Sequential builds with the same seed and
    /// source produce identical graphs.
    pub seed: u64,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: DEFAULT_M,
            ef_construction: DEFAULT_EF_CONSTRUCTION,
            seed: 0x5eed_1095_e000_0001,
        }
    }
}

impl HnswParams {
    /// Checks that the parameters are in range.
    pub fn validate(&self) -> Result<(), GraphError> {
        if !(2..=MAX_M).contains(&self.m) {
            return Err(GraphError::InvalidParams(format!(
                "m must be in 2..={MAX_M}, got {}",
                self.m
            )));
        }
        if self.ef_construction == 0 || self.ef_construction > u32::MAX as usize {
            return Err(GraphError::InvalidParams(format!(
                "ef_construction must be in 1..={}, got {}",
                u32::MAX,
                self.ef_construction
            )));
        }
        Ok(())
    }

    /// Maximum links per node on `level`.
    pub fn max_links(&self, level: usize) -> usize {
        if level == 0 { 2 * self.m } else { self.m }
    }

    /// Level multiplier `mL = 1 / ln(M)`.
    pub fn level_multiplier(&self) -> f64 {
        1.0 / (self.m as f64).ln()
    }

    /// Beam width actually used while building (`max(ef_construction, M)`).
    pub(super) fn build_ef(&self) -> usize {
        self.ef_construction.max(self.m)
    }

    /// Level of `row`, drawn from a counter-based RNG keyed by the seed.
    ///
    /// `level = floor(-ln(u) * mL)` for `u` uniform in `(0, 1]`. With 53-bit
    /// `u` and `M >= 2` the level never exceeds 53, so it always fits `u8`.
    pub(super) fn draw_level(&self, row: u32) -> u8 {
        let bits = splitmix64(self.seed ^ splitmix64(u64::from(row)));
        let unit = ((bits >> 11) as f64 + 1.0) / (1_u64 << 53) as f64;
        let level = (-unit.ln() * self.level_multiplier()).floor();
        level.clamp(0.0, f64::from(u8::MAX)) as u8
    }
}

/// SplitMix64 finalizer, used as a stateless seeded RNG.
pub(super) fn splitmix64(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// An HNSW graph over dense row ids.
///
/// Layer 0 is a flat array of fixed-size slots, `[count, link_0 .. link_2M)`
/// per row, so a node's links sit in one or two cache lines. Rows with
/// level `>= 1` own a run of `level` upper-layer slots of `M + 1` words.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HnswGraph {
    params: HnswParams,
    levels: Vec<u8>,
    entry_point: Option<u32>,
    max_level: u8,
    layer0: Vec<u32>,
    /// First upper-layer block of each row, or [`NO_BLOCK`] at level 0.
    upper_block: Vec<u32>,
    upper: Vec<u32>,
    upper_blocks: u32,
}

impl HnswGraph {
    /// An empty graph.
    pub fn new(params: HnswParams) -> Result<Self, GraphError> {
        Self::with_capacity(params, 0)
    }

    /// An empty graph with room for `rows` rows on layer 0.
    pub fn with_capacity(params: HnswParams, rows: usize) -> Result<Self, GraphError> {
        params.validate()?;
        let mut graph = Self {
            params,
            levels: Vec::new(),
            entry_point: None,
            max_level: 0,
            layer0: Vec::new(),
            upper_block: Vec::new(),
            upper: Vec::new(),
            upper_blocks: 0,
        };
        graph.reserve(rows)?;
        Ok(graph)
    }

    /// Builds a graph over every row of `source` by inserting rows in order
    /// on the calling thread. Deterministic for a given seed and source.
    pub fn build<V: VectorSource + ?Sized>(
        source: &V,
        params: HnswParams,
    ) -> Result<Self, GraphError> {
        let rows = source.len();
        if rows >= u32::MAX as usize {
            return Err(GraphError::TooLarge);
        }
        let mut graph = Self::with_capacity(params, rows)?;
        let mut scratch = SearchScratch::new();
        for row in 0..rows as u32 {
            graph.insert(source, row, &mut scratch)?;
        }
        Ok(graph)
    }

    /// Builds a graph over every row of `source` on the current `rayon`
    /// pool: link lists are atomics read without locks, and each node has a
    /// mutex that serializes edits to its lists. Not deterministic across
    /// runs.
    pub fn build_parallel<V: VectorSource + ?Sized>(
        source: &V,
        params: HnswParams,
    ) -> Result<Self, GraphError> {
        build::build_parallel(source, params, &|| false)
    }

    /// [`build_parallel`](Self::build_parallel) that stops early once `cancelled` returns
    /// `true`. It is polled before every insert, so a build stops within about one insert per
    /// thread of being cancelled, and then returns [`GraphError::Cancelled`].
    pub fn build_parallel_cancellable<V: VectorSource + ?Sized>(
        source: &V,
        params: HnswParams,
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<Self, GraphError> {
        build::build_parallel(source, params, cancelled)
    }

    /// Inserts `row`, which must equal [`Self::len`] (rows are dense and
    /// appended in order) and be covered by `source`.
    pub fn insert<V: VectorSource + ?Sized>(
        &mut self,
        source: &V,
        row: u32,
        scratch: &mut SearchScratch,
    ) -> Result<(), GraphError> {
        let expected = u32::try_from(self.len()).map_err(|_| GraphError::TooLarge)?;
        if row != expected {
            return Err(GraphError::OutOfOrderInsert { row, expected });
        }
        if row as usize >= source.len() {
            return Err(GraphError::RowOutOfRange {
                row,
                len: source.len(),
            });
        }
        let level = self.params.draw_level(row);
        self.push_node(level)?;
        let Some(entry) = self.entry_point else {
            self.entry_point = Some(row);
            self.max_level = level;
            return Ok(());
        };
        let params = self.params;
        let target = build::InsertTarget {
            row,
            level: usize::from(level),
            entry,
            top: usize::from(self.max_level),
            rows: self.len(),
        };
        let linked = build::plan_links(&*self, source, &params, target, scratch);
        build::commit_links(self, source, &params, row, linked, scratch);
        if level > self.max_level {
            self.entry_point = Some(row);
            self.max_level = level;
        }
        Ok(())
    }

    /// Number of rows in the graph.
    pub fn len(&self) -> usize {
        self.levels.len()
    }

    /// Returns `true` when the graph has no rows.
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Build parameters.
    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    /// Entry point for searches: a row on the top layer.
    pub fn entry_point(&self) -> Option<u32> {
        self.entry_point
    }

    /// Highest layer in the graph.
    pub fn max_level(&self) -> usize {
        usize::from(self.max_level)
    }

    /// Top layer of `row`, or `None` when out of range.
    pub fn level(&self, row: u32) -> Option<usize> {
        self.levels
            .get(row as usize)
            .map(|level| usize::from(*level))
    }

    /// Links of `row` on `level`; empty when either is out of range.
    #[inline]
    pub fn neighbors(&self, row: u32, level: usize) -> &[u32] {
        let (storage, range) = match self.slot_range(row, level) {
            Some((true, range)) => (&self.layer0, range),
            Some((false, range)) => (&self.upper, range),
            None => return &[],
        };
        let Some(slot) = storage.get(range) else {
            return &[];
        };
        match slot.split_first() {
            Some((count, links)) => links.get(..*count as usize).unwrap_or(links),
            None => &[],
        }
    }

    /// Heap bytes used by the adjacency and level arrays.
    pub fn memory_bytes(&self) -> usize {
        self.levels.capacity()
            + 4 * (self.layer0.capacity() + self.upper_block.capacity() + self.upper.capacity())
    }

    pub(super) fn levels(&self) -> &[u8] {
        &self.levels
    }

    /// Moves the link storage out, leaving the layout (levels and upper
    /// blocks) in place; see [`Self::restore_storage`].
    pub(super) fn take_storage(&mut self) -> (Vec<u32>, Vec<u32>) {
        (
            std::mem::take(&mut self.layer0),
            std::mem::take(&mut self.upper),
        )
    }

    /// Puts back storage taken by [`Self::take_storage`].
    pub(super) fn restore_storage(&mut self, layer0: Vec<u32>, upper: Vec<u32>) {
        self.layer0 = layer0;
        self.upper = upper;
    }

    pub(super) fn set_entry(&mut self, entry: Option<u32>, max_level: u8) {
        self.entry_point = entry;
        self.max_level = max_level;
    }

    /// Reserves layer-0 room for `additional` rows without aborting on
    /// allocation failure.
    pub(super) fn reserve(&mut self, additional: usize) -> Result<(), GraphError> {
        let slot = self.params.max_links(0) + 1;
        let words = additional.checked_mul(slot).ok_or(GraphError::TooLarge)?;
        self.levels
            .try_reserve(additional)
            .map_err(|_| GraphError::TooLarge)?;
        self.upper_block
            .try_reserve(additional)
            .map_err(|_| GraphError::TooLarge)?;
        self.layer0
            .try_reserve(words)
            .map_err(|_| GraphError::TooLarge)?;
        Ok(())
    }

    /// Appends a row at `level` with empty link lists and returns its id.
    pub(super) fn push_node(&mut self, level: u8) -> Result<u32, GraphError> {
        let row = u32::try_from(self.levels.len()).map_err(|_| GraphError::TooLarge)?;
        if row == u32::MAX {
            return Err(GraphError::TooLarge);
        }
        let slot0 = self.params.max_links(0) + 1;
        let new_len = self
            .layer0
            .len()
            .checked_add(slot0)
            .ok_or(GraphError::TooLarge)?;
        self.layer0
            .try_reserve(slot0)
            .map_err(|_| GraphError::TooLarge)?;
        self.layer0.resize(new_len, 0);
        if level == 0 {
            self.upper_block.push(NO_BLOCK);
        } else {
            let first = self.upper_blocks;
            let next = first
                .checked_add(u32::from(level))
                .filter(|next| *next != NO_BLOCK)
                .ok_or(GraphError::TooLarge)?;
            let words = usize::from(level) * (self.params.m + 1);
            self.upper
                .try_reserve(words)
                .map_err(|_| GraphError::TooLarge)?;
            self.upper.resize(self.upper.len() + words, 0);
            self.upper_blocks = next;
            self.upper_block.push(first);
        }
        self.levels.push(level);
        Ok(row)
    }

    /// Overwrites the links of `row` on `level`, truncating to the layer
    /// capacity. Ignores out-of-range rows and levels.
    pub(super) fn set_links(&mut self, row: u32, level: usize, links: &[u32]) {
        let cap = self.params.max_links(level);
        let count = links.len().min(cap);
        let (storage, range) = match self.slot_range(row, level) {
            Some((true, range)) => (&mut self.layer0, range),
            Some((false, range)) => (&mut self.upper, range),
            None => return,
        };
        if let Some(slot) = storage.get_mut(range)
            && let Some((head, tail)) = slot.split_first_mut()
        {
            *head = count as u32;
            if let (Some(dst), Some(src)) = (tail.get_mut(..count), links.get(..count)) {
                dst.copy_from_slice(src);
            }
        }
    }

    /// Storage (`true` = layer 0) and word range of a slot.
    #[inline]
    pub(super) fn slot_range(&self, row: u32, level: usize) -> Option<(bool, Range<usize>)> {
        let index = row as usize;
        let node_level = usize::from(*self.levels.get(index)?);
        if level == 0 {
            let width = self.params.max_links(0) + 1;
            let start = index * width;
            return Some((true, start..start + width));
        }
        if level > node_level {
            return None;
        }
        let block = *self.upper_block.get(index)?;
        let width = self.params.m + 1;
        let start = (block as usize + level - 1) * width;
        Some((false, start..start + width))
    }
}

impl Links for HnswGraph {
    #[inline]
    fn neighbors_into(&self, row: u32, level: usize, out: &mut Vec<u32>) {
        out.clear();
        out.extend_from_slice(self.neighbors(row, level));
    }
}

impl LinkWriter for HnswGraph {
    fn update<F>(&mut self, row: u32, level: usize, out: &mut Vec<u32>, edit: F)
    where
        F: FnOnce(&[u32], &mut Vec<u32>) -> bool,
    {
        if edit(self.neighbors(row, level), out) {
            self.set_links(row, level, out);
        }
    }
}
