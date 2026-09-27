//! Per-segment scalar indexes over dense `u32` row ids.
//!
//! Every query returns a [`RoaringBitmap`] of row ids, because bitmaps are
//! where scalar filtering and vector search meet: a segment's filter result is
//! `bitmap(predicate) AND NOT deletion_vector`, and its cardinality picks the
//! vector search strategy.
//!
//! # Structures
//!
//! - [`InvertedIndex`]: immutable, one bitmap per distinct key. Best for
//!   equality, `IN`, and array membership.
//! - [`SortedIndex`]: immutable, distinct sorted keys plus the rows of each
//!   key in one flat array. Best for ranges, prefixes, `ORDER BY ... LIMIT`
//!   scans, zone maps, and histograms.
//! - [`MutableInvertedIndex`] (`BTreeMap<ScalarKey, RoaringBitmap>`) and
//!   [`MutableSortedIndex`] (ordered `(key, row)` set): memtable indexes
//!   supporting insert and remove, frozen into the immutable forms on flush
//!   with a row-id remapping (memtable slot to segment row).
//! - [`ScalarIndexBuilder`]: builds the immutable forms from `(row, key)`
//!   pairs.
//!
//! All four indexes implement [`ScalarIndex`], so a predicate compiler can
//! probe mutable and immutable segments uniformly. The sorted variants also
//! implement [`OrderedScalarIndex`].
//!
//! # Data model
//!
//! - An index holds keys of one [`KeyKind`]. Timestamps are
//!   [`KeyKind::Int`] microseconds since the Unix epoch.
//! - Float keys are totally ordered with [`f64::total_cmp`]; NaN is rejected
//!   when the key is built and `-0.0` is normalized to `0.0`.
//! - An array value is indexed as one `(row, key)` entry per distinct element.
//!   Duplicate elements collapse.
//! - Null and missing values are recorded in a separate null bitmap, so
//!   [`ScalarIndex::is_null`] and [`ScalarIndex::exists`] are answerable. A row
//!   cannot be both null and have values. A row with an empty array is neither
//!   null nor existing unless the caller records it as null.
//!
//! # Serialization
//!
//! [`InvertedIndex::to_bytes`] and [`SortedIndex::to_bytes`] write a
//! versioned, CRC32-protected layout (see the `codec` module docs);
//! `write_to` appends the same bytes to an existing buffer. Loading
//! (`from_bytes`) validates the checksum and the structure, returns an error
//! on any corruption, and decodes into an owned index.

mod builder;
mod codec;
mod error;
mod inverted;
mod key;
mod mutable;
mod sorted;
#[cfg(test)]
mod tests;

#[cfg(test)]
use criterion as _;

pub use builder::ScalarIndexBuilder;
pub use error::ScalarError;
pub use inverted::InvertedIndex;
pub use key::{F64Key, KeyKind, ScalarKey, ScalarKeyRef};
pub use mutable::{MutableInvertedIndex, MutableSortedIndex};
pub use roaring::RoaringBitmap;
pub use sorted::{HistogramBucket, OrderedIter, SortedIndex};

use roaring::MultiOps;
use std::ops::Bound;

/// Direction of an ordered scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Smallest key first; rows of equal keys in ascending row order.
    Ascending,
    /// Largest key first; rows of equal keys in descending row order.
    Descending,
}

/// Exact size statistics of one index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ScalarIndexStats {
    /// Kind of the indexed keys.
    pub kind: KeyKind,
    /// Number of distinct `(key, row)` entries. Equals `rows_with_values` for
    /// single-valued fields; larger when arrays hold several elements.
    pub entries: u64,
    /// Number of distinct keys.
    pub distinct_keys: u64,
    /// Number of rows with at least one indexed value.
    pub rows_with_values: u64,
    /// Number of rows recorded as null or missing.
    pub null_rows: u64,
}

/// Query surface shared by mutable and immutable scalar indexes.
///
/// Queries never fail. A key or bound whose kind differs from
/// [`ScalarIndex::kind`] matches nothing, and an inverted interval is empty.
/// Null rows never match a value predicate.
pub trait ScalarIndex {
    /// Kind of the indexed keys.
    fn kind(&self) -> KeyKind;

    /// Rows with a value equal to `key`.
    fn equals(&self, key: &ScalarKey) -> RoaringBitmap;

    /// Rows with a value in `keys` (SQL `IN`).
    fn in_set(&self, keys: &[ScalarKey]) -> RoaringBitmap;

    /// Rows with a value in the interval.
    fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> RoaringBitmap;

    /// Rows with a string value starting with `prefix`. Empty for other kinds.
    fn prefix(&self, prefix: &str) -> RoaringBitmap;

    /// Rows recorded as null or missing.
    fn is_null(&self) -> &RoaringBitmap;

    /// Rows with at least one indexed value.
    fn exists(&self) -> &RoaringBitmap;

    /// Exact number of rows holding `key`, without materializing a bitmap.
    fn cardinality(&self, key: &ScalarKey) -> u64;

    /// Exact number of `(key, row)` entries in the interval, without
    /// materializing a bitmap. Equals the matching row count for
    /// single-valued fields.
    fn count_range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> u64;

    /// Exact size statistics.
    fn stats(&self) -> ScalarIndexStats;

    /// Rows with a value that is not in `keys` (SQL `NOT IN`): existing rows
    /// minus [`ScalarIndex::in_set`]. For arrays this is "at least one
    /// element, and no element is in `keys`", so `[a, b] NOT IN {a}` is
    /// false. Null rows and empty-array rows are never in
    /// [`ScalarIndex::exists`], so they never match; a caller that wants
    /// `NOT (field IN keys)` instead computes `live - in_set(keys)`.
    fn not_in_set(&self, keys: &[ScalarKey]) -> RoaringBitmap {
        let mut rows = self.exists().clone();
        rows -= self.in_set(keys);
        rows
    }

    /// Rows whose array holds `key`. The same as [`ScalarIndex::equals`],
    /// since each element is indexed as its own entry.
    fn contains(&self, key: &ScalarKey) -> RoaringBitmap {
        self.equals(key)
    }

    /// Rows whose array holds any of `keys`. The same as
    /// [`ScalarIndex::in_set`].
    fn contains_any(&self, keys: &[ScalarKey]) -> RoaringBitmap {
        self.in_set(keys)
    }

    /// Rows whose array holds every one of `keys`. With no keys, every
    /// existing row matches; empty-array rows are not existing rows, so they
    /// do not match even then.
    fn contains_all(&self, keys: &[ScalarKey]) -> RoaringBitmap {
        if keys.is_empty() {
            return self.exists().clone();
        }
        keys.iter().map(|key| self.equals(key)).intersection()
    }
}

/// Ordered access for `ORDER BY field LIMIT n` scans and zone maps.
pub trait OrderedScalarIndex: ScalarIndex {
    /// Smallest key, if any.
    fn min_key(&self) -> Option<ScalarKeyRef<'_>>;

    /// Largest key, if any.
    fn max_key(&self) -> Option<ScalarKeyRef<'_>>;

    /// Lazily yield `(key, row)` entries inside the interval in key order,
    /// keeping only rows in `allow` when given. Ties are broken by row id in
    /// the same direction. Stop early by dropping the iterator or with
    /// [`Iterator::take`].
    fn scan_range<'a>(
        &'a self,
        lower: Bound<&ScalarKey>,
        upper: Bound<&ScalarKey>,
        direction: Direction,
        allow: Option<&'a RoaringBitmap>,
    ) -> Box<dyn Iterator<Item = (ScalarKeyRef<'a>, u32)> + 'a>;

    /// [`OrderedScalarIndex::scan_range`] over every entry.
    fn scan<'a>(
        &'a self,
        direction: Direction,
        allow: Option<&'a RoaringBitmap>,
    ) -> Box<dyn Iterator<Item = (ScalarKeyRef<'a>, u32)> + 'a> {
        self.scan_range(Bound::Unbounded, Bound::Unbounded, direction, allow)
    }
}

/// Density, as rows per bitset bit, above which [`bitmap_from_rows`] fills a
/// dense bitset instead of sorting.
const DENSE_ROWS_PER_BIT: u64 = 64;

/// Average bitmap size below which [`union_bitmaps`] gathers rows instead of
/// merging bitmaps one by one.
const SMALL_BITMAP_ROWS: u64 = 32;

/// Build a bitmap from unsorted row ids, which may repeat.
///
/// Dense inputs fill a byte bitset and bulk-load it, which is linear; sparse
/// inputs are sorted.
pub(crate) fn bitmap_from_rows(rows: &[u32]) -> RoaringBitmap {
    let Some(&max) = rows.iter().max() else {
        return RoaringBitmap::new();
    };
    if rows.len() as u64 * DENSE_ROWS_PER_BIT >= u64::from(max) {
        let mut bits = vec![0u8; max as usize / 8 + 1];
        for &row in rows {
            bits[row as usize / 8] |= 1 << (row % 8);
        }
        return RoaringBitmap::from_lsb0_bytes(0, &bits);
    }
    let mut sorted = rows.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    RoaringBitmap::from_sorted_iter(sorted).unwrap_or_default()
}

/// Union `bitmaps`, which hold `entries` rows in total across `count`
/// bitmaps. Many tiny bitmaps (a high-cardinality key range) are gathered
/// into one row list, because merging them one at a time is far slower.
pub(crate) fn union_bitmaps<'a>(
    bitmaps: impl Iterator<Item = &'a RoaringBitmap>,
    count: u64,
    entries: u64,
) -> RoaringBitmap {
    if entries < count.saturating_mul(SMALL_BITMAP_ROWS) {
        let mut rows = Vec::with_capacity(usize::try_from(entries).unwrap_or(0));
        for bitmap in bitmaps {
            rows.extend(bitmap);
        }
        bitmap_from_rows(&rows)
    } else {
        bitmaps.union()
    }
}
