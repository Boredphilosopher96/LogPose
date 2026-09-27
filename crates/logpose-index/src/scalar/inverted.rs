//! Immutable inverted index: one roaring bitmap per distinct key.

use super::{
    KeyKind, RoaringBitmap, ScalarError, ScalarIndex, ScalarIndexStats, ScalarKey, SortedIndex,
    key::KeyColumn, union_bitmaps,
};
use roaring::MultiOps;
use std::ops::{Bound, Range};

/// Immutable per-segment inverted index.
///
/// Distinct keys are kept sorted in a typed column next to their bitmaps, so
/// equality is a binary search and returns a clone of one bitmap, `IN` is a
/// union, and ranges and prefixes are unions over a contiguous key run.
/// Cumulative entry counts make [`ScalarIndex::cardinality`] and
/// [`ScalarIndex::count_range`] O(log n) without touching any bitmap.
#[derive(Clone, Debug, PartialEq)]
pub struct InvertedIndex {
    keys: KeyColumn,
    bitmaps: Vec<RoaringBitmap>,
    /// `cumulative[i]` is the number of entries of keys `0..i`.
    cumulative: Vec<u64>,
    nulls: RoaringBitmap,
    present: RoaringBitmap,
}

impl InvertedIndex {
    /// An empty index of `kind`.
    #[must_use]
    pub fn empty(kind: KeyKind) -> Self {
        Self {
            keys: KeyColumn::new(kind),
            bitmaps: Vec::new(),
            cumulative: vec![0],
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
        builder.build_inverted()
    }

    /// Convert a sorted index, which holds the same entries.
    #[must_use]
    pub fn from_sorted(sorted: &SortedIndex) -> Self {
        let offsets = sorted.offsets();
        let rows = sorted.rows();
        let bitmaps = offsets
            .windows(2)
            .map(|run| {
                RoaringBitmap::from_sorted_iter(
                    rows[run[0] as usize..run[1] as usize].iter().copied(),
                )
                .unwrap_or_default()
            })
            .collect();
        Self {
            keys: sorted.key_column().clone(),
            bitmaps,
            cumulative: offsets.iter().map(|&offset| u64::from(offset)).collect(),
            nulls: sorted.nulls().clone(),
            present: sorted.exists().clone(),
        }
    }

    /// Assemble from parts whose keys are strictly ascending and whose
    /// bitmaps are non-empty, deriving counts and the present bitmap.
    pub(crate) fn from_parts(
        keys: KeyColumn,
        bitmaps: Vec<RoaringBitmap>,
        nulls: RoaringBitmap,
    ) -> Result<Self, ScalarError> {
        let mut cumulative = Vec::with_capacity(bitmaps.len() + 1);
        let mut total = 0u64;
        cumulative.push(total);
        for bitmap in &bitmaps {
            total += bitmap.len();
            cumulative.push(total);
        }
        if total > u64::from(u32::MAX) {
            return Err(ScalarError::TooManyEntries);
        }
        let present = union_bitmaps(bitmaps.iter(), bitmaps.len() as u64, total);
        if let Some(row) = (&present & &nulls).min() {
            return Err(ScalarError::NullConflict { row });
        }
        Ok(Self {
            keys,
            bitmaps,
            cumulative,
            nulls,
            present,
        })
    }

    pub(crate) fn key_column(&self) -> &KeyColumn {
        &self.keys
    }

    pub(crate) fn bitmaps(&self) -> &[RoaringBitmap] {
        &self.bitmaps
    }

    pub(crate) fn nulls(&self) -> &RoaringBitmap {
        &self.nulls
    }

    /// Number of `(key, row)` entries.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.total()
    }

    /// Whether the index holds no entries (null rows may still exist).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bitmaps.is_empty()
    }

    /// Number of distinct keys.
    #[must_use]
    pub fn distinct_keys(&self) -> usize {
        self.keys.len()
    }

    /// Distinct keys with their bitmaps, in ascending key order.
    pub fn terms(&self) -> impl Iterator<Item = (super::ScalarKeyRef<'_>, &RoaringBitmap)> {
        self.bitmaps
            .iter()
            .enumerate()
            .map(|(index, bitmap)| (self.keys.get(index), bitmap))
    }

    fn total(&self) -> u64 {
        self.cumulative.last().copied().unwrap_or(0)
    }

    fn entries_of(&self, keys: &Range<usize>) -> u64 {
        if keys.is_empty() {
            return 0;
        }
        self.cumulative[keys.end] - self.cumulative[keys.start]
    }

    fn union_of_keys(&self, keys: Range<usize>) -> RoaringBitmap {
        let entries = self.entries_of(&keys);
        if entries == 0 {
            return RoaringBitmap::new();
        }
        // With one key per row, a wide interval is cheaper as a complement.
        let total = self.total();
        if self.present.len() == total && entries * 2 > total {
            let outside = union_bitmaps(
                self.bitmaps[..keys.start]
                    .iter()
                    .chain(&self.bitmaps[keys.end..]),
                (self.bitmaps.len() - keys.len()) as u64,
                total - entries,
            );
            let mut result = self.present.clone();
            result -= outside;
            return result;
        }
        let count = keys.len() as u64;
        union_bitmaps(self.bitmaps[keys].iter(), count, entries)
    }
}

impl ScalarIndex for InvertedIndex {
    fn kind(&self) -> KeyKind {
        self.keys.kind()
    }

    fn equals(&self, key: &ScalarKey) -> RoaringBitmap {
        self.keys
            .find(key)
            .map_or_else(RoaringBitmap::new, |index| self.bitmaps[index].clone())
    }

    fn in_set(&self, keys: &[ScalarKey]) -> RoaringBitmap {
        keys.iter()
            .filter_map(|key| self.keys.find(key))
            .map(|index| &self.bitmaps[index])
            .union()
    }

    fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> RoaringBitmap {
        self.union_of_keys(self.keys.range(lower, upper))
    }

    fn prefix(&self, prefix: &str) -> RoaringBitmap {
        self.union_of_keys(self.keys.prefix_range(prefix))
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
            .map_or(0, |index| self.entries_of(&(index..index + 1)))
    }

    fn count_range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> u64 {
        self.entries_of(&self.keys.range(lower, upper))
    }

    fn stats(&self) -> ScalarIndexStats {
        ScalarIndexStats {
            kind: self.keys.kind(),
            entries: self.total(),
            distinct_keys: self.keys.len() as u64,
            rows_with_values: self.present.len(),
            null_rows: self.nulls.len(),
        }
    }
}
