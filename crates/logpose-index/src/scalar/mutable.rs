//! Mutable memtable indexes and their freeze into the immutable forms.

use super::{
    Direction, InvertedIndex, KeyKind, OrderedScalarIndex, RoaringBitmap, ScalarError, ScalarIndex,
    ScalarIndexStats, ScalarKey, ScalarKeyRef, SortedIndex,
    key::{KeyColumn, bounds_usable},
};
use roaring::MultiOps;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
};

/// Shared state of both memtable indexes.
#[derive(Clone, Debug)]
struct TermIndex {
    kind: KeyKind,
    terms: BTreeMap<ScalarKey, RoaringBitmap>,
    nulls: RoaringBitmap,
    present: RoaringBitmap,
    /// Term counts of rows holding two or more keys. Rows with one key are
    /// only in `present`, so single-valued fields never touch this map.
    multi: HashMap<u32, u32>,
    entries: u64,
}

impl TermIndex {
    fn new(kind: KeyKind) -> Self {
        Self {
            kind,
            terms: BTreeMap::new(),
            nulls: RoaringBitmap::new(),
            present: RoaringBitmap::new(),
            multi: HashMap::new(),
            entries: 0,
        }
    }

    fn insert(&mut self, row: u32, key: ScalarKey) -> Result<bool, ScalarError> {
        if key.kind() != self.kind {
            return Err(ScalarError::KindMismatch {
                expected: self.kind,
                found: key.kind(),
            });
        }
        if self.nulls.contains(row) {
            return Err(ScalarError::NullConflict { row });
        }
        if !self.terms.entry(key).or_default().insert(row) {
            return Ok(false);
        }
        self.entries += 1;
        if !self.present.insert(row) {
            *self.multi.entry(row).or_insert(1) += 1;
        }
        Ok(true)
    }

    fn insert_null(&mut self, row: u32) -> Result<bool, ScalarError> {
        if self.present.contains(row) {
            return Err(ScalarError::NullConflict { row });
        }
        Ok(self.nulls.insert(row))
    }

    fn remove(&mut self, row: u32, key: &ScalarKey) -> bool {
        let Some(bitmap) = self.terms.get_mut(key) else {
            return false;
        };
        if !bitmap.remove(row) {
            return false;
        }
        if bitmap.is_empty() {
            self.terms.remove(key);
        }
        self.entries -= 1;
        match self.multi.get_mut(&row) {
            Some(count) if *count > 2 => *count -= 1,
            Some(_) => {
                self.multi.remove(&row);
            }
            None => {
                self.present.remove(row);
            }
        }
        true
    }

    fn remove_null(&mut self, row: u32) -> bool {
        self.nulls.remove(row)
    }

    /// Check that `remap` is injective over every row the index knows, and
    /// return the remapped null bitmap.
    fn validate_remap(
        &self,
        remap: &impl Fn(u32) -> Option<u32>,
    ) -> Result<RoaringBitmap, ScalarError> {
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

    fn freeze_sorted(
        &self,
        remap: impl Fn(u32) -> Option<u32>,
    ) -> Result<SortedIndex, ScalarError> {
        let nulls = self.validate_remap(&remap)?;
        if u32::try_from(self.entries).is_err() {
            return Err(ScalarError::TooManyEntries);
        }
        let mut keys = KeyColumn::new(self.kind);
        let mut offsets = vec![0u32];
        let mut rows = Vec::with_capacity(self.entries as usize);
        for (key, bitmap) in &self.terms {
            let start = rows.len();
            rows.extend(bitmap.iter().filter_map(&remap));
            if rows.len() == start {
                continue;
            }
            rows[start..].sort_unstable();
            keys.push(key.clone())?;
            offsets.push(rows.len() as u32);
        }
        SortedIndex::from_parts(keys, offsets, rows, nulls)
    }

    fn freeze_inverted(
        &self,
        remap: impl Fn(u32) -> Option<u32>,
    ) -> Result<InvertedIndex, ScalarError> {
        let nulls = self.validate_remap(&remap)?;
        let mut keys = KeyColumn::new(self.kind);
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

    fn equals(&self, key: &ScalarKey) -> RoaringBitmap {
        self.terms.get(key).cloned().unwrap_or_default()
    }

    fn in_set(&self, keys: &[ScalarKey]) -> RoaringBitmap {
        keys.iter().filter_map(|key| self.terms.get(key)).union()
    }

    /// Terms inside the interval; empty when the bounds are unusable, which
    /// `BTreeMap::range` would panic on.
    fn in_range<'s>(
        &'s self,
        lower: Bound<&ScalarKey>,
        upper: Bound<&ScalarKey>,
    ) -> impl DoubleEndedIterator<Item = (&'s ScalarKey, &'s RoaringBitmap)> + use<'s> {
        bounds_usable(self.kind, lower, upper)
            .then(|| self.terms.range::<ScalarKey, _>((lower, upper)))
            .into_iter()
            .flatten()
    }

    fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> RoaringBitmap {
        self.in_range(lower, upper)
            .map(|(_, bitmap)| bitmap)
            .union()
    }

    fn prefix(&self, prefix: &str) -> RoaringBitmap {
        if self.kind != KeyKind::Str {
            return RoaringBitmap::new();
        }
        let start = ScalarKey::string(prefix);
        self.terms
            .range::<ScalarKey, _>((Bound::Included(&start), Bound::Unbounded))
            .take_while(|(key, _)| matches!(key, ScalarKey::Str(key) if key.starts_with(prefix)))
            .map(|(_, bitmap)| bitmap)
            .union()
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
        ScalarIndexStats {
            kind: self.kind,
            entries: self.entries,
            distinct_keys: self.terms.len() as u64,
            rows_with_values: self.present.len(),
            null_rows: self.nulls.len(),
        }
    }
}

macro_rules! mutable_index {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug)]
        pub struct $name {
            inner: TermIndex,
        }

        impl $name {
            /// An empty index for keys of `kind`.
            #[must_use]
            pub fn new(kind: KeyKind) -> Self {
                Self {
                    inner: TermIndex::new(kind),
                }
            }

            /// Record that `row` holds `key`. Call once per element for
            /// arrays. Returns whether the pair was new.
            ///
            /// # Errors
            ///
            /// Returns [`ScalarError::KindMismatch`] for a key of another
            /// kind and [`ScalarError::NullConflict`] when `row` is null.
            pub fn insert(&mut self, row: u32, key: ScalarKey) -> Result<bool, ScalarError> {
                self.inner.insert(row, key)
            }

            /// Record that `row` is null or missing. Returns whether it was
            /// newly recorded.
            ///
            /// # Errors
            ///
            /// Returns [`ScalarError::NullConflict`] when `row` has values.
            pub fn insert_null(&mut self, row: u32) -> Result<bool, ScalarError> {
                self.inner.insert_null(row)
            }

            /// Forget that `row` holds `key`. Returns whether the pair existed.
            pub fn remove(&mut self, row: u32, key: &ScalarKey) -> bool {
                self.inner.remove(row, key)
            }

            /// Forget that `row` is null. Returns whether it was recorded.
            pub fn remove_null(&mut self, row: u32) -> bool {
                self.inner.remove_null(row)
            }

            /// Number of `(key, row)` entries.
            #[must_use]
            pub fn len(&self) -> u64 {
                self.inner.entries
            }

            /// Whether the index holds no entries (null rows may still exist).
            #[must_use]
            pub fn is_empty(&self) -> bool {
                self.inner.entries == 0
            }
        }

        impl ScalarIndex for $name {
            fn kind(&self) -> KeyKind {
                self.inner.kind
            }

            fn equals(&self, key: &ScalarKey) -> RoaringBitmap {
                self.inner.equals(key)
            }

            fn in_set(&self, keys: &[ScalarKey]) -> RoaringBitmap {
                self.inner.in_set(keys)
            }

            fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> RoaringBitmap {
                self.inner.range(lower, upper)
            }

            fn prefix(&self, prefix: &str) -> RoaringBitmap {
                self.inner.prefix(prefix)
            }

            fn is_null(&self) -> &RoaringBitmap {
                &self.inner.nulls
            }

            fn exists(&self) -> &RoaringBitmap {
                &self.inner.present
            }

            fn cardinality(&self, key: &ScalarKey) -> u64 {
                self.inner.cardinality(key)
            }

            fn count_range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> u64 {
                self.inner.count_range(lower, upper)
            }

            fn stats(&self) -> ScalarIndexStats {
                self.inner.stats()
            }
        }
    };
}

mutable_index!(
    /// Memtable inverted index: `BTreeMap<ScalarKey, RoaringBitmap>` over
    /// memtable slots, frozen into an [`InvertedIndex`] on flush.
    MutableInvertedIndex
);

mutable_index!(
    /// Memtable sorted index: `BTreeMap<ScalarKey, RoaringBitmap>` over
    /// memtable slots with ordered scans, frozen into a [`SortedIndex`] on
    /// flush.
    MutableSortedIndex
);

impl MutableInvertedIndex {
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
        self.inner.freeze_inverted(remap)
    }
}

impl MutableSortedIndex {
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
        self.inner.freeze_sorted(remap)
    }

    /// Smallest key (zone map minimum).
    #[must_use]
    pub fn min(&self) -> Option<ScalarKeyRef<'_>> {
        self.inner.terms.keys().next().map(ScalarKey::as_key_ref)
    }

    /// Largest key (zone map maximum).
    #[must_use]
    pub fn max(&self) -> Option<ScalarKeyRef<'_>> {
        self.inner
            .terms
            .keys()
            .next_back()
            .map(ScalarKey::as_key_ref)
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
        let terms = self.inner.in_range(lower, upper);
        let allowed = move |(_, row): &(ScalarKeyRef<'a>, u32)| {
            allow.is_none_or(|allow| allow.contains(*row))
        };
        match direction {
            Direction::Ascending => Box::new(
                terms
                    .flat_map(|(key, bitmap)| bitmap.iter().map(move |row| (key.as_key_ref(), row)))
                    .filter(allowed),
            ),
            Direction::Descending => Box::new(
                terms
                    .rev()
                    .flat_map(|(key, bitmap)| {
                        bitmap.iter().rev().map(move |row| (key.as_key_ref(), row))
                    })
                    .filter(allowed),
            ),
        }
    }
}
