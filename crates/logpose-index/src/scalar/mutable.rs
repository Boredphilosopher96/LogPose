//! Mutable memtable indexes and their freeze into the immutable forms.
//!
//! The two memtable indexes store postings differently, each for its role:
//!
//! - [`MutableInvertedIndex`] keeps `BTreeMap<ScalarKey, RoaringBitmap>`, so
//!   equality and `IN` return (unions of) ready bitmaps.
//! - [`MutableSortedIndex`] keeps an ordered set of `(key, row)` pairs, so a
//!   range or an ordered scan walks contiguous B-tree leaves instead of one
//!   small bitmap allocation per distinct key. On one million distinct keys
//!   this is the difference between gathering rows and merging a million
//!   bitmaps.

use super::{
    Direction, InvertedIndex, KeyKind, OrderedScalarIndex, RoaringBitmap, ScalarError, ScalarIndex,
    ScalarIndexStats, ScalarKey, ScalarKeyRef, SortedIndex, bitmap_from_rows,
    key::{KeyColumn, bounds_usable},
    union_bitmaps,
};
use roaring::MultiOps;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::Bound,
};

/// Row bookkeeping shared by both memtable indexes: nulls, rows with values,
/// and per-row entry counts so that `exists` stays exact under removes.
#[derive(Clone, Debug)]
struct Rows {
    kind: KeyKind,
    nulls: RoaringBitmap,
    present: RoaringBitmap,
    /// Entry counts of rows holding two or more keys. Rows with one key are
    /// only in `present`, so single-valued fields never touch this map.
    multi: HashMap<u32, u32>,
    entries: u64,
    distinct_keys: u64,
}

impl Rows {
    fn new(kind: KeyKind) -> Self {
        Self {
            kind,
            nulls: RoaringBitmap::new(),
            present: RoaringBitmap::new(),
            multi: HashMap::new(),
            entries: 0,
            distinct_keys: 0,
        }
    }

    /// Validate an insert of `key` into `row` before touching postings.
    fn check_insert(&self, row: u32, key: &ScalarKey) -> Result<(), ScalarError> {
        if key.kind() != self.kind {
            return Err(ScalarError::KindMismatch {
                expected: self.kind,
                found: key.kind(),
            });
        }
        if self.nulls.contains(row) {
            return Err(ScalarError::NullConflict { row });
        }
        Ok(())
    }

    /// Record a new `(key, row)` entry.
    fn inserted(&mut self, row: u32, new_key: bool) {
        self.entries += 1;
        self.distinct_keys += u64::from(new_key);
        if !self.present.insert(row) {
            *self.multi.entry(row).or_insert(1) += 1;
        }
    }

    /// Record a removed `(key, row)` entry.
    fn removed(&mut self, row: u32, key_gone: bool) {
        self.entries -= 1;
        self.distinct_keys -= u64::from(key_gone);
        match self.multi.get_mut(&row) {
            Some(count) if *count > 2 => *count -= 1,
            Some(_) => {
                self.multi.remove(&row);
            }
            None => {
                self.present.remove(row);
            }
        }
    }

    fn insert_null(&mut self, row: u32) -> Result<bool, ScalarError> {
        if self.present.contains(row) {
            return Err(ScalarError::NullConflict { row });
        }
        Ok(self.nulls.insert(row))
    }

    /// Check that `remap` is injective over every row the index knows, and
    /// return the remapped null bitmap.
    fn validate_remap(
        &self,
        remap: &impl Fn(u32) -> Option<u32>,
    ) -> Result<RoaringBitmap, ScalarError> {
        if u32::try_from(self.entries).is_err() {
            return Err(ScalarError::TooManyEntries);
        }
        let mut image = RoaringBitmap::new();
        let mut nulls = RoaringBitmap::new();
        for row in &self.present {
            if let Some(target) = remap(row)
                && !image.insert(target)
            {
                return Err(ScalarError::NonInjectiveRemap { row: target });
            }
        }
        for row in &self.nulls {
            if let Some(target) = remap(row) {
                if !image.insert(target) {
                    return Err(ScalarError::NonInjectiveRemap { row: target });
                }
                nulls.insert(target);
            }
        }
        Ok(nulls)
    }

    fn stats(&self) -> ScalarIndexStats {
        ScalarIndexStats {
            kind: self.kind,
            entries: self.entries,
            distinct_keys: self.distinct_keys,
            rows_with_values: self.present.len(),
            null_rows: self.nulls.len(),
        }
    }
}

/// Union bitmaps of unknown sizes, sizing them first so that runs of tiny
/// bitmaps are gathered rather than merged one by one.
fn union_all<'a>(bitmaps: impl Iterator<Item = &'a RoaringBitmap>) -> RoaringBitmap {
    let bitmaps: Vec<&RoaringBitmap> = bitmaps.collect();
    let count = bitmaps.len() as u64;
    let entries = bitmaps.iter().map(|bitmap| bitmap.len()).sum();
    union_bitmaps(bitmaps.into_iter(), count, entries)
}

/// Memtable inverted index: `BTreeMap<ScalarKey, RoaringBitmap>` over
/// memtable slots, frozen into an [`InvertedIndex`] on flush.
#[derive(Clone, Debug)]
pub struct MutableInvertedIndex {
    terms: BTreeMap<ScalarKey, RoaringBitmap>,
    rows: Rows,
}

impl MutableInvertedIndex {
    /// An empty index for keys of `kind`.
    #[must_use]
    pub fn new(kind: KeyKind) -> Self {
        Self {
            terms: BTreeMap::new(),
            rows: Rows::new(kind),
        }
    }

    /// Record that `row` holds `key`. Call once per element for arrays.
    /// Returns whether the pair was new.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::KindMismatch`] for a key of another kind and
    /// [`ScalarError::NullConflict`] when `row` is null.
    pub fn insert(&mut self, row: u32, key: ScalarKey) -> Result<bool, ScalarError> {
        self.rows.check_insert(row, &key)?;
        let bitmap = self.terms.entry(key).or_default();
        let new_key = bitmap.is_empty();
        if !bitmap.insert(row) {
            return Ok(false);
        }
        self.rows.inserted(row, new_key);
        Ok(true)
    }

    /// Record that `row` is null or missing. Returns whether it was newly
    /// recorded.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::NullConflict`] when `row` has values.
    pub fn insert_null(&mut self, row: u32) -> Result<bool, ScalarError> {
        self.rows.insert_null(row)
    }

    /// Forget that `row` holds `key`. Returns whether the pair existed.
    pub fn remove(&mut self, row: u32, key: &ScalarKey) -> bool {
        let Some(bitmap) = self.terms.get_mut(key) else {
            return false;
        };
        if !bitmap.remove(row) {
            return false;
        }
        let key_gone = bitmap.is_empty();
        if key_gone {
            self.terms.remove(key);
        }
        self.rows.removed(row, key_gone);
        true
    }

    /// Forget that `row` is null. Returns whether it was recorded.
    pub fn remove_null(&mut self, row: u32) -> bool {
        self.rows.nulls.remove(row)
    }

    /// Number of `(key, row)` entries.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.rows.entries
    }

    /// Whether the index holds no entries (null rows may still exist).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.entries == 0
    }

    /// Build the immutable index, renumbering rows with `remap` (memtable slot
    /// to segment row id). Slots mapped to `None` are dropped. `remap` must be
    /// a pure function; it is called once per entry. The index stays
    /// readable, so queries can continue while a flush runs.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::NonInjectiveRemap`] when two slots map to the
    /// same row.
    pub fn freeze(&self, remap: impl Fn(u32) -> Option<u32>) -> Result<InvertedIndex, ScalarError> {
        let nulls = self.rows.validate_remap(&remap)?;
        let mut keys = KeyColumn::new(self.rows.kind);
        let mut bitmaps = Vec::with_capacity(self.terms.len());
        for (key, bitmap) in &self.terms {
            let remapped: RoaringBitmap = bitmap.iter().filter_map(&remap).collect();
            if remapped.is_empty() {
                continue;
            }
            keys.push(key.clone())?;
            bitmaps.push(remapped);
        }
        InvertedIndex::from_parts(keys, bitmaps, nulls)
    }

    /// Terms inside the interval; empty when the bounds are unusable, which
    /// `BTreeMap::range` would panic on.
    fn in_range<'s>(
        &'s self,
        lower: Bound<&ScalarKey>,
        upper: Bound<&ScalarKey>,
    ) -> impl DoubleEndedIterator<Item = (&'s ScalarKey, &'s RoaringBitmap)> + use<'s> {
        bounds_usable(self.rows.kind, lower, upper)
            .then(|| self.terms.range::<ScalarKey, _>((lower, upper)))
            .into_iter()
            .flatten()
    }
}

impl ScalarIndex for MutableInvertedIndex {
    fn kind(&self) -> KeyKind {
        self.rows.kind
    }

    fn equals(&self, key: &ScalarKey) -> RoaringBitmap {
        self.terms.get(key).cloned().unwrap_or_default()
    }

    fn in_set(&self, keys: &[ScalarKey]) -> RoaringBitmap {
        keys.iter().filter_map(|key| self.terms.get(key)).union()
    }

    fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> RoaringBitmap {
        union_all(self.in_range(lower, upper).map(|(_, bitmap)| bitmap))
    }

    fn prefix(&self, prefix: &str) -> RoaringBitmap {
        if self.rows.kind != KeyKind::Str {
            return RoaringBitmap::new();
        }
        let start = ScalarKey::string(prefix);
        union_all(
            self.terms
                .range::<ScalarKey, _>((Bound::Included(&start), Bound::Unbounded))
                .take_while(|(key, _)| starts_with(key, prefix))
                .map(|(_, bitmap)| bitmap),
        )
    }

    fn is_null(&self) -> &RoaringBitmap {
        &self.rows.nulls
    }

    fn exists(&self) -> &RoaringBitmap {
        &self.rows.present
    }

    fn cardinality(&self, key: &ScalarKey) -> u64 {
        self.terms.get(key).map_or(0, RoaringBitmap::len)
    }

    fn count_range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> u64 {
        self.in_range(lower, upper)
            .map(|(_, bitmap)| bitmap.len())
            .sum()
    }

    fn stats(&self) -> ScalarIndexStats {
        self.rows.stats()
    }
}

/// Memtable sorted index: an ordered set of `(key, row)` pairs over memtable
/// slots, with ordered scans, frozen into a [`SortedIndex`] on flush.
///
/// Equality and counts walk the key's run, so they cost O(log n + matches)
/// rather than the O(1) of [`MutableInvertedIndex`].
#[derive(Clone, Debug)]
pub struct MutableSortedIndex {
    pairs: BTreeSet<(ScalarKey, u32)>,
    rows: Rows,
}

impl MutableSortedIndex {
    /// An empty index for keys of `kind`.
    #[must_use]
    pub fn new(kind: KeyKind) -> Self {
        Self {
            pairs: BTreeSet::new(),
            rows: Rows::new(kind),
        }
    }

    /// Record that `row` holds `key`. Call once per element for arrays.
    /// Returns whether the pair was new.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::KindMismatch`] for a key of another kind and
    /// [`ScalarError::NullConflict`] when `row` is null.
    pub fn insert(&mut self, row: u32, key: ScalarKey) -> Result<bool, ScalarError> {
        self.rows.check_insert(row, &key)?;
        let new_key = !self.has_key(&key);
        if !self.pairs.insert((key, row)) {
            return Ok(false);
        }
        self.rows.inserted(row, new_key);
        Ok(true)
    }

    /// Record that `row` is null or missing. Returns whether it was newly
    /// recorded.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::NullConflict`] when `row` has values.
    pub fn insert_null(&mut self, row: u32) -> Result<bool, ScalarError> {
        self.rows.insert_null(row)
    }

    /// Forget that `row` holds `key`. Returns whether the pair existed.
    pub fn remove(&mut self, row: u32, key: &ScalarKey) -> bool {
        if key.kind() != self.rows.kind || !self.pairs.remove(&(key.clone(), row)) {
            return false;
        }
        let key_gone = !self.has_key(key);
        self.rows.removed(row, key_gone);
        true
    }

    /// Forget that `row` is null. Returns whether it was recorded.
    pub fn remove_null(&mut self, row: u32) -> bool {
        self.rows.nulls.remove(row)
    }

    /// Number of `(key, row)` entries.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.rows.entries
    }

    /// Whether the index holds no entries (null rows may still exist).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.entries == 0
    }

    /// Smallest key (zone map minimum).
    #[must_use]
    pub fn min(&self) -> Option<ScalarKeyRef<'_>> {
        self.pairs.first().map(|(key, _)| key.as_key_ref())
    }

    /// Largest key (zone map maximum).
    #[must_use]
    pub fn max(&self) -> Option<ScalarKeyRef<'_>> {
        self.pairs.last().map(|(key, _)| key.as_key_ref())
    }

    /// Build the immutable index, renumbering rows with `remap` (memtable slot
    /// to segment row id). Slots mapped to `None` are dropped. `remap` must be
    /// a pure function; it is called once per entry. The index stays
    /// readable, so queries can continue while a flush runs.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::NonInjectiveRemap`] when two slots map to the
    /// same row, and [`ScalarError::TooManyEntries`] past `u32::MAX` entries.
    pub fn freeze(&self, remap: impl Fn(u32) -> Option<u32>) -> Result<SortedIndex, ScalarError> {
        let nulls = self.rows.validate_remap(&remap)?;
        let mut keys = KeyColumn::new(self.rows.kind);
        let mut offsets = vec![0u32];
        let mut rows = Vec::with_capacity(usize::try_from(self.rows.entries).unwrap_or(0));
        let mut current: Option<&ScalarKey> = None;
        // Close the run of `current`: sort its remapped rows and emit the key
        // unless every row was dropped.
        let mut close = |key: &ScalarKey, rows: &mut Vec<u32>| -> Result<(), ScalarError> {
            let start = *offsets.last().unwrap_or(&0) as usize;
            if rows.len() > start {
                rows[start..].sort_unstable();
                keys.push(key.clone())?;
                offsets.push(rows.len() as u32);
            }
            Ok(())
        };
        for (key, row) in &self.pairs {
            if current != Some(key) {
                if let Some(done) = current {
                    close(done, &mut rows)?;
                }
                current = Some(key);
            }
            if let Some(target) = remap(*row) {
                rows.push(target);
            }
        }
        if let Some(done) = current {
            close(done, &mut rows)?;
        }
        SortedIndex::from_parts(keys, offsets, rows, nulls)
    }

    fn has_key(&self, key: &ScalarKey) -> bool {
        self.run(key).next().is_some()
    }

    /// Pairs holding `key`, in row order.
    fn run(&self, key: &ScalarKey) -> std::collections::btree_set::Range<'_, (ScalarKey, u32)> {
        self.pairs.range((key.clone(), 0)..=(key.clone(), u32::MAX))
    }

    /// Pairs inside the key interval; empty when the bounds are unusable,
    /// which `BTreeSet::range` would panic on.
    fn in_range<'s>(
        &'s self,
        lower: Bound<&ScalarKey>,
        upper: Bound<&ScalarKey>,
    ) -> impl DoubleEndedIterator<Item = &'s (ScalarKey, u32)> + use<'s> {
        let bounds = bounds_usable(self.rows.kind, lower, upper).then(|| {
            let lower = match lower {
                Bound::Included(key) => Bound::Included((key.clone(), 0)),
                Bound::Excluded(key) => Bound::Excluded((key.clone(), u32::MAX)),
                Bound::Unbounded => Bound::Unbounded,
            };
            let upper = match upper {
                Bound::Included(key) => Bound::Included((key.clone(), u32::MAX)),
                Bound::Excluded(key) => Bound::Excluded((key.clone(), 0)),
                Bound::Unbounded => Bound::Unbounded,
            };
            (lower, upper)
        });
        bounds
            .map(|bounds| self.pairs.range(bounds))
            .into_iter()
            .flatten()
    }
}

fn starts_with(key: &ScalarKey, prefix: &str) -> bool {
    matches!(key, ScalarKey::Str(key) if key.starts_with(prefix))
}

impl ScalarIndex for MutableSortedIndex {
    fn kind(&self) -> KeyKind {
        self.rows.kind
    }

    fn equals(&self, key: &ScalarKey) -> RoaringBitmap {
        if key.kind() != self.rows.kind {
            return RoaringBitmap::new();
        }
        RoaringBitmap::from_sorted_iter(self.run(key).map(|(_, row)| *row)).unwrap_or_default()
    }

    fn in_set(&self, keys: &[ScalarKey]) -> RoaringBitmap {
        let rows: Vec<u32> = keys
            .iter()
            .filter(|key| key.kind() == self.rows.kind)
            .flat_map(|key| self.run(key).map(|(_, row)| *row))
            .collect();
        bitmap_from_rows(&rows)
    }

    fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> RoaringBitmap {
        let rows: Vec<u32> = self.in_range(lower, upper).map(|(_, row)| *row).collect();
        bitmap_from_rows(&rows)
    }

    fn prefix(&self, prefix: &str) -> RoaringBitmap {
        if self.rows.kind != KeyKind::Str {
            return RoaringBitmap::new();
        }
        let start = (ScalarKey::string(prefix), 0);
        let rows: Vec<u32> = self
            .pairs
            .range(start..)
            .take_while(|(key, _)| starts_with(key, prefix))
            .map(|(_, row)| *row)
            .collect();
        bitmap_from_rows(&rows)
    }

    fn is_null(&self) -> &RoaringBitmap {
        &self.rows.nulls
    }

    fn exists(&self) -> &RoaringBitmap {
        &self.rows.present
    }

    fn cardinality(&self, key: &ScalarKey) -> u64 {
        if key.kind() != self.rows.kind {
            return 0;
        }
        self.run(key).count() as u64
    }

    fn count_range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> u64 {
        self.in_range(lower, upper).count() as u64
    }

    fn stats(&self) -> ScalarIndexStats {
        self.rows.stats()
    }
}

impl OrderedScalarIndex for MutableSortedIndex {
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
        let pairs = self.in_range(lower, upper);
        let visible = move |(key, row): &'a (ScalarKey, u32)| {
            allow
                .is_none_or(|allow| allow.contains(*row))
                .then(|| (key.as_key_ref(), *row))
        };
        match direction {
            Direction::Ascending => Box::new(pairs.filter_map(visible)),
            Direction::Descending => Box::new(pairs.rev().filter_map(visible)),
        }
    }
}
