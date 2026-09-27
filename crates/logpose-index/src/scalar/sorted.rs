//! Immutable sorted index: distinct sorted keys, per-key row runs in one flat
//! array, for ranges, prefixes, ordered scans, zone maps and histograms.

use super::{
    Direction, KeyKind, OrderedScalarIndex, RoaringBitmap, ScalarError, ScalarIndex,
    ScalarIndexStats, ScalarKey, ScalarKeyRef, key::KeyColumn,
};
use std::ops::{Bound, Range};

/// Immutable per-segment sorted index.
///
/// The layout is columnar, in compressed sparse row form: `keys` holds the
/// distinct keys in ascending order, and the rows of key `i` are
/// `rows[offsets[i]..offsets[i + 1]]`, ascending. Reading `rows` front to back
/// therefore yields entries in `(key, row)` order. Ranges and counts are
/// binary searches over `keys`, and a range bitmap is built from one
/// contiguous slice of `rows`.
#[derive(Clone, Debug, PartialEq)]
pub struct SortedIndex {
    keys: KeyColumn,
    offsets: Vec<u32>,
    rows: Vec<u32>,
    nulls: RoaringBitmap,
    present: RoaringBitmap,
}

/// One bucket of an equi-depth histogram.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistogramBucket {
    /// Smallest key in the bucket.
    pub lower: ScalarKey,
    /// Largest key in the bucket, inclusive.
    pub upper: ScalarKey,
    /// Number of `(key, row)` entries in the bucket.
    pub entries: u64,
    /// Number of distinct keys in the bucket.
    pub distinct_keys: u64,
}

impl SortedIndex {
    /// An empty index of `kind`.
    #[must_use]
    pub fn empty(kind: KeyKind) -> Self {
        Self {
            keys: KeyColumn::new(kind),
            offsets: vec![0],
            rows: Vec::new(),
            nulls: RoaringBitmap::new(),
            present: RoaringBitmap::new(),
        }
    }

    /// Build from `(row, key)` pairs. Arrays contribute one pair per element.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::KindMismatch`] for a key of another kind and
    /// [`ScalarError::TooManyEntries`] past `u32::MAX` entries.
    pub fn from_entries(
        kind: KeyKind,
        entries: impl IntoIterator<Item = (u32, ScalarKey)>,
    ) -> Result<Self, ScalarError> {
        let mut builder = super::ScalarIndexBuilder::new(kind);
        builder.extend(entries)?;
        builder.build_sorted()
    }

    /// Assemble from validated parts and derive the present bitmap.
    ///
    /// Callers guarantee: keys strictly ascending, `offsets` has
    /// `keys.len() + 1` strictly ascending entries from 0 to `rows.len()`, and
    /// each run of `rows` is strictly ascending.
    pub(crate) fn from_parts(
        keys: KeyColumn,
        offsets: Vec<u32>,
        rows: Vec<u32>,
        nulls: RoaringBitmap,
    ) -> Result<Self, ScalarError> {
        let present = if keys.len() == 1 {
            RoaringBitmap::from_sorted_iter(rows.iter().copied())
                .map_err(|_| ScalarError::Corrupt("row run is not ascending"))?
        } else {
            sorted_bitmap(rows.clone())
        };
        if let Some(row) = (&present & &nulls).min() {
            return Err(ScalarError::NullConflict { row });
        }
        Ok(Self {
            keys,
            offsets,
            rows,
            nulls,
            present,
        })
    }

    pub(crate) fn key_column(&self) -> &KeyColumn {
        &self.keys
    }

    pub(crate) fn offsets(&self) -> &[u32] {
        &self.offsets
    }

    pub(crate) fn rows(&self) -> &[u32] {
        &self.rows
    }

    pub(crate) fn nulls(&self) -> &RoaringBitmap {
        &self.nulls
    }

    /// Number of `(key, row)` entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the index holds no entries (null rows may still exist).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Number of distinct keys.
    #[must_use]
    pub fn distinct_keys(&self) -> usize {
        self.keys.len()
    }

    /// Whether every row holds at most one key.
    #[must_use]
    pub fn is_single_valued(&self) -> bool {
        self.present.len() == self.rows.len() as u64
    }

    /// Smallest key (zone map minimum).
    #[must_use]
    pub fn min(&self) -> Option<ScalarKeyRef<'_>> {
        (self.keys.len() > 0).then(|| self.keys.get(0))
    }

    /// Largest key (zone map maximum).
    #[must_use]
    pub fn max(&self) -> Option<ScalarKeyRef<'_>> {
        self.keys
            .len()
            .checked_sub(1)
            .map(|last| self.keys.get(last))
    }

    /// Rows of the keys at positions `keys`, as positions into `rows`.
    fn positions(&self, keys: Range<usize>) -> Range<usize> {
        if keys.is_empty() {
            return 0..0;
        }
        self.offsets[keys.start] as usize..self.offsets[keys.end] as usize
    }

    /// Bitmap of the rows of the keys at positions `keys`.
    fn bitmap_for_keys(&self, keys: Range<usize>) -> RoaringBitmap {
        let positions = self.positions(keys.clone());
        if positions.is_empty() {
            return RoaringBitmap::new();
        }
        if keys.len() == 1 {
            // A single run is already ascending and duplicate-free.
            return RoaringBitmap::from_sorted_iter(self.rows[positions].iter().copied())
                .unwrap_or_default();
        }
        // With one key per row, a wide interval is cheaper as a complement:
        // sort the rows outside it instead.
        if self.is_single_valued() && positions.len() * 2 > self.rows.len() {
            let mut outside = Vec::with_capacity(self.rows.len() - positions.len());
            outside.extend_from_slice(&self.rows[..positions.start]);
            outside.extend_from_slice(&self.rows[positions.end..]);
            let mut result = self.present.clone();
            result -= sorted_bitmap(outside);
            return result;
        }
        sorted_bitmap(self.rows[positions].to_vec())
    }

    /// Lazily yield `(key, row)` entries in key order, keeping only rows in
    /// `allow` when given. See [`OrderedScalarIndex::scan_range`].
    #[must_use]
    pub fn iter_ordered<'a>(
        &'a self,
        direction: Direction,
        allow: Option<&'a RoaringBitmap>,
    ) -> OrderedIter<'a> {
        self.iter_range_ordered(Bound::Unbounded, Bound::Unbounded, direction, allow)
    }

    /// Lazily yield `(key, row)` entries inside the interval in key order,
    /// keeping only rows in `allow` when given.
    #[must_use]
    pub fn iter_range_ordered<'a>(
        &'a self,
        lower: Bound<&ScalarKey>,
        upper: Bound<&ScalarKey>,
        direction: Direction,
        allow: Option<&'a RoaringBitmap>,
    ) -> OrderedIter<'a> {
        let keys = self.keys.range(lower, upper);
        let positions = self.positions(keys.clone());
        let key = match direction {
            Direction::Ascending => keys.start,
            Direction::Descending => keys.end.saturating_sub(1),
        };
        OrderedIter {
            index: self,
            allow,
            direction,
            front: positions.start,
            back: positions.end,
            key,
        }
    }

    /// Equi-depth histogram with at most `buckets` buckets.
    ///
    /// Bucket boundaries fall on key boundaries, so a heavy key stays in one
    /// bucket and the result can have fewer buckets than requested. Buckets
    /// are ascending and disjoint, and their entry counts sum to
    /// [`SortedIndex::len`].
    #[must_use]
    pub fn equi_depth_histogram(&self, buckets: usize) -> Vec<HistogramBucket> {
        let total = self.rows.len() as u64;
        if buckets == 0 || total == 0 {
            return Vec::new();
        }
        let key_count = self.keys.len();
        let buckets = buckets.min(key_count);
        let mut result = Vec::with_capacity(buckets);
        let mut start_key = 0usize;
        for bucket in 1..=buckets as u64 {
            if start_key >= key_count {
                break;
            }
            // Close the bucket at the first key whose cumulative entry count
            // reaches this bucket's quantile target.
            let target = (total * bucket).div_ceil(buckets as u64);
            let end_key = self.offsets[1..]
                .partition_point(|&end| u64::from(end) < target)
                .max(start_key)
                .min(key_count - 1);
            result.push(HistogramBucket {
                lower: self.keys.get(start_key).to_key(),
                upper: self.keys.get(end_key).to_key(),
                entries: u64::from(self.offsets[end_key + 1] - self.offsets[start_key]),
                distinct_keys: (end_key + 1 - start_key) as u64,
            });
            start_key = end_key + 1;
        }
        result
    }
}

impl ScalarIndex for SortedIndex {
    fn kind(&self) -> KeyKind {
        self.keys.kind()
    }

    fn equals(&self, key: &ScalarKey) -> RoaringBitmap {
        self.keys
            .find(key)
            .map_or_else(RoaringBitmap::new, |index| {
                self.bitmap_for_keys(index..index + 1)
            })
    }

    fn in_set(&self, keys: &[ScalarKey]) -> RoaringBitmap {
        let mut rows = Vec::new();
        let mut found = 0usize;
        for key in keys {
            if let Some(index) = self.keys.find(key) {
                rows.extend_from_slice(&self.rows[self.positions(index..index + 1)]);
                found += 1;
            }
        }
        if found <= 1 {
            // Zero or one run: already ascending and duplicate-free.
            return RoaringBitmap::from_sorted_iter(rows).unwrap_or_default();
        }
        sorted_bitmap(rows)
    }

    fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> RoaringBitmap {
        self.bitmap_for_keys(self.keys.range(lower, upper))
    }

    fn prefix(&self, prefix: &str) -> RoaringBitmap {
        self.bitmap_for_keys(self.keys.prefix_range(prefix))
    }

    fn is_null(&self) -> &RoaringBitmap {
        &self.nulls
    }

    fn exists(&self) -> &RoaringBitmap {
        &self.present
    }

    fn cardinality(&self, key: &ScalarKey) -> u64 {
        self.keys
            .find(key)
            .map_or(0, |index| self.positions(index..index + 1).len() as u64)
    }

    fn count_range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> u64 {
        self.positions(self.keys.range(lower, upper)).len() as u64
    }

    fn stats(&self) -> ScalarIndexStats {
        ScalarIndexStats {
            kind: self.keys.kind(),
            entries: self.rows.len() as u64,
            distinct_keys: self.keys.len() as u64,
            rows_with_values: self.present.len(),
            null_rows: self.nulls.len(),
        }
    }
}

impl OrderedScalarIndex for SortedIndex {
    fn min_key(&self) -> Option<ScalarKeyRef<'_>> {
        self.min()
    }

    fn max_key(&self) -> Option<ScalarKeyRef<'_>> {
        self.max()
    }

    fn scan_range<'a>(
        &'a self,
        lower: Bound<&ScalarKey>,
        upper: Bound<&ScalarKey>,
        direction: Direction,
        allow: Option<&'a RoaringBitmap>,
    ) -> Box<dyn Iterator<Item = (ScalarKeyRef<'a>, u32)> + 'a> {
        Box::new(self.iter_range_ordered(lower, upper, direction, allow))
    }
}

/// Ordered `(key, row)` iterator over a [`SortedIndex`].
#[derive(Clone, Debug)]
pub struct OrderedIter<'a> {
    index: &'a SortedIndex,
    allow: Option<&'a RoaringBitmap>,
    direction: Direction,
    /// Next position to yield when ascending.
    front: usize,
    /// One past the next position to yield when descending.
    back: usize,
    /// Key owning the most recently visited position.
    key: usize,
}

impl<'a> Iterator for OrderedIter<'a> {
    type Item = (ScalarKeyRef<'a>, u32);

    fn next(&mut self) -> Option<Self::Item> {
        let offsets = &self.index.offsets;
        while self.front < self.back {
            let position = match self.direction {
                Direction::Ascending => {
                    let position = self.front;
                    self.front += 1;
                    while offsets[self.key + 1] as usize <= position {
                        self.key += 1;
                    }
                    position
                }
                Direction::Descending => {
                    self.back -= 1;
                    let position = self.back;
                    while offsets[self.key] as usize > position {
                        self.key -= 1;
                    }
                    position
                }
            };
            let row = self.index.rows[position];
            if self.allow.is_none_or(|allow| allow.contains(row)) {
                return Some((self.index.keys.get(self.key), row));
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back - self.front;
        match self.allow {
            None => (remaining, Some(remaining)),
            Some(_) => (0, Some(remaining)),
        }
    }
}

/// Sort, deduplicate and bulk-load row ids.
pub(crate) fn sorted_bitmap(mut rows: Vec<u32>) -> RoaringBitmap {
    rows.sort_unstable();
    rows.dedup();
    RoaringBitmap::from_sorted_iter(rows).unwrap_or_default()
}
