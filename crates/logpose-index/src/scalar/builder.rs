//! Bulk construction of the immutable indexes from `(row, key)` pairs.

use super::{
    InvertedIndex, KeyKind, RoaringBitmap, ScalarError, ScalarKey, SortedIndex, key::KeyColumn,
};

/// Collects `(row, key)` pairs and null rows for one field of one segment,
/// then builds a [`SortedIndex`], an [`InvertedIndex`], or both from a single
/// sort.
#[derive(Clone, Debug)]
pub struct ScalarIndexBuilder {
    kind: KeyKind,
    pairs: Vec<(ScalarKey, u32)>,
    nulls: RoaringBitmap,
}

impl ScalarIndexBuilder {
    /// A builder for keys of `kind`.
    #[must_use]
    pub fn new(kind: KeyKind) -> Self {
        Self {
            kind,
            pairs: Vec::new(),
            nulls: RoaringBitmap::new(),
        }
    }

    /// Record that `row` holds `key`. Call once per element for arrays;
    /// repeated pairs collapse.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::KindMismatch`] when `key` is not of the
    /// builder's kind.
    pub fn insert(&mut self, row: u32, key: ScalarKey) -> Result<(), ScalarError> {
        if key.kind() != self.kind {
            return Err(ScalarError::KindMismatch {
                expected: self.kind,
                found: key.kind(),
            });
        }
        self.pairs.push((key, row));
        Ok(())
    }

    /// Record every pair from `entries`.
    ///
    /// # Errors
    ///
    /// Returns the first error [`ScalarIndexBuilder::insert`] reports.
    pub fn extend(
        &mut self,
        entries: impl IntoIterator<Item = (u32, ScalarKey)>,
    ) -> Result<(), ScalarError> {
        for (row, key) in entries {
            self.insert(row, key)?;
        }
        Ok(())
    }

    /// Record that `row` is null or missing.
    pub fn insert_null(&mut self, row: u32) {
        self.nulls.insert(row);
    }

    /// Build the sorted index.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::NullConflict`] when a row is both null and has a
    /// value, and [`ScalarError::TooManyEntries`] past `u32::MAX` entries.
    pub fn build_sorted(mut self) -> Result<SortedIndex, ScalarError> {
        self.pairs.sort_unstable();
        self.pairs.dedup();
        if u32::try_from(self.pairs.len()).is_err() {
            return Err(ScalarError::TooManyEntries);
        }
        let mut keys = KeyColumn::new(self.kind);
        let mut offsets = vec![0u32];
        let mut rows = Vec::with_capacity(self.pairs.len());
        let mut previous: Option<ScalarKey> = None;
        for (key, row) in self.pairs {
            // A new key closes the previous key's run.
            if previous.as_ref() != Some(&key)
                && let Some(done) = previous.replace(key)
            {
                keys.push(done)?;
                offsets.push(rows.len() as u32);
            }
            rows.push(row);
        }
        if let Some(done) = previous {
            keys.push(done)?;
            offsets.push(rows.len() as u32);
        }
        SortedIndex::from_parts(keys, offsets, rows, self.nulls)
    }

    /// Build the inverted index.
    ///
    /// # Errors
    ///
    /// See [`ScalarIndexBuilder::build_sorted`].
    pub fn build_inverted(self) -> Result<InvertedIndex, ScalarError> {
        self.build_sorted()
            .map(|sorted| InvertedIndex::from_sorted(&sorted))
    }

    /// Build both indexes from one sort, as `auto` indexing does for numbers
    /// and timestamps.
    ///
    /// # Errors
    ///
    /// See [`ScalarIndexBuilder::build_sorted`].
    pub fn build(self) -> Result<(InvertedIndex, SortedIndex), ScalarError> {
        let sorted = self.build_sorted()?;
        Ok((InvertedIndex::from_sorted(&sorted), sorted))
    }
}
