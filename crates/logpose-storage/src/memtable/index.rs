//! [`MemScalarIndex`]: a memtable's live postings over slots for one indexed scalar field.

use crate::dv::CowBitmap;
use logpose_index::scalar::ScalarKey;
use logpose_types::{RowId, schema::FieldIndex, value::Value};
use roaring::RoaringBitmap;
use std::ops::Bound;

/// Which kind of index postings serve.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum IndexFlavor {
    /// Equality and `IN` (bool, string, int64, float64, timestamp, and array elements).
    Inverted,
    /// Ranges and `order_by` (int64, float64, timestamp).
    Sorted,
}

impl IndexFlavor {
    /// The flavors a field's concrete index asks for.
    pub(crate) fn for_index(index: FieldIndex) -> Vec<Self> {
        let mut flavors = Vec::new();
        if index.has_inverted() {
            flavors.push(Self::Inverted);
        }
        if index.has_sorted() {
            flavors.push(Self::Sorted);
        }
        flavors
    }
}

/// Postings over slots: an ordered map from key to the slots holding it, and the slots with no
/// value. Every posting is a two-tier [`CowBitmap`], so a group copies at most the recent tier
/// of each key it touches, never a whole posting. Dead slots stay in their postings; readers
/// always subtract the unit's deletion vector.
#[derive(Clone)]
#[allow(
    dead_code,
    reason = "the read path compiles filters over memtable postings"
)]
pub(crate) struct MemScalarIndex {
    flavor: IndexFlavor,
    /// The first slot the index covers; earlier slots (written before the field was added)
    /// read null.
    first_slot: RowId,
    keys: imbl::OrdMap<ScalarKey, CowBitmap>,
    nulls: CowBitmap,
}

#[allow(
    dead_code,
    reason = "the read path compiles filters over memtable postings"
)]
impl MemScalarIndex {
    pub(crate) fn new(flavor: IndexFlavor, first_slot: RowId) -> Self {
        Self {
            flavor,
            first_slot,
            keys: imbl::OrdMap::new(),
            nulls: CowBitmap::default(),
        }
    }

    pub(crate) fn flavor(&self) -> IndexFlavor {
        self.flavor
    }

    /// Index `slot` with `value` (null when `None`). Returns the number of posting entries
    /// added.
    pub(crate) fn insert(&mut self, slot: RowId, value: Option<&Value>) -> u64 {
        let mut keys = value.map(index_keys).unwrap_or_default();
        keys.sort();
        keys.dedup();
        if keys.is_empty() {
            self.nulls.insert(slot);
            return 1;
        }
        let added = keys.len() as u64;
        for key in keys {
            match self.keys.get_mut(&key) {
                Some(posting) => {
                    posting.insert(slot);
                }
                None => {
                    let mut posting = CowBitmap::default();
                    posting.insert(slot);
                    self.keys.insert(key, posting);
                }
            }
        }
        added
    }

    /// Slots holding `key` (for arrays, holding it as an element).
    pub(crate) fn eq(&self, key: &ScalarKey) -> RoaringBitmap {
        let mut rows = RoaringBitmap::new();
        if let Some(posting) = self.keys.get(key) {
            posting.union_into(&mut rows);
        }
        rows
    }

    /// Slots whose key is in the range; `None` for an inverted index.
    pub(crate) fn range(
        &self,
        low: Bound<&ScalarKey>,
        high: Bound<&ScalarKey>,
    ) -> Option<RoaringBitmap> {
        if self.flavor != IndexFlavor::Sorted {
            return None;
        }
        let mut rows = RoaringBitmap::new();
        for (_, posting) in self.keys.range::<_, ScalarKey>((low, high)) {
            posting.union_into(&mut rows);
        }
        Some(rows)
    }

    /// Slots with no value: the ones indexed as null and every slot before the index existed.
    pub(crate) fn nulls(&self) -> RoaringBitmap {
        let mut rows: RoaringBitmap = (0..self.first_slot).collect();
        self.nulls.union_into(&mut rows);
        rows
    }

    /// Number of distinct keys.
    pub(crate) fn key_count(&self) -> usize {
        self.keys.len()
    }
}

/// The index keys of one value: one per array element, none for JSON or null.
pub(crate) fn index_keys(value: &Value) -> Vec<ScalarKey> {
    match value {
        Value::Array(values) => values.iter().filter_map(scalar_key).collect(),
        other => scalar_key(other).into_iter().collect(),
    }
}

/// The index key of a scalar value. Integers and timestamps share the integer kind; floats are
/// finite by validation.
pub(crate) fn scalar_key(value: &Value) -> Option<ScalarKey> {
    match value {
        Value::Bool(value) => Some(ScalarKey::Bool(*value)),
        Value::Int64(value) => Some(ScalarKey::Int(*value)),
        Value::Timestamp(value) => Some(ScalarKey::timestamp_micros(value.as_micros())),
        Value::Float64(value) => ScalarKey::float(*value).ok(),
        Value::String(value) => Some(ScalarKey::string(value.as_str())),
        Value::Null | Value::Array(_) | Value::Json(_) => None,
    }
}
