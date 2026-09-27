//! Index keys, their total order, and the sorted key column shared by the
//! immutable indexes.

use super::ScalarError;
use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::{Bound, Range},
};

/// Kind of the keys an index holds. Every index holds keys of exactly one kind.
///
/// Timestamps are indexed as [`KeyKind::Int`] holding microseconds since the
/// Unix epoch (see [`ScalarKey::timestamp_micros`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyKind {
    /// Boolean keys.
    Bool,
    /// Signed 64-bit integer keys, including timestamps in microseconds.
    Int,
    /// 64-bit float keys, never NaN.
    Float,
    /// UTF-8 string keys, ordered bytewise.
    Str,
}

impl KeyKind {
    /// Render the kind as a stable lowercase name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int => "int",
            Self::Float => "float",
            Self::Str => "string",
        }
    }

    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Bool => 1,
            Self::Int => 2,
            Self::Float => 3,
            Self::Str => 4,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Bool),
            2 => Some(Self::Int),
            3 => Some(Self::Float),
            4 => Some(Self::Str),
            _ => None,
        }
    }
}

impl fmt::Display for KeyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A totally ordered `f64` key.
///
/// NaN is rejected at construction, and `-0.0` is normalized to `0.0` so that
/// equality matches numeric equality. Ordering is [`f64::total_cmp`], which
/// puts `-inf` first and `+inf` last.
#[derive(Clone, Copy, Debug)]
pub struct F64Key(f64);

impl F64Key {
    /// Wrap a float, rejecting NaN.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::NanKey`] when `value` is NaN.
    pub fn new(value: f64) -> Result<Self, ScalarError> {
        if value.is_nan() {
            Err(ScalarError::NanKey)
        } else if value == 0.0 {
            Ok(Self(0.0))
        } else {
            Ok(Self(value))
        }
    }

    /// The wrapped value.
    #[must_use]
    pub fn get(self) -> f64 {
        self.0
    }
}

impl PartialEq for F64Key {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for F64Key {}

impl PartialOrd for F64Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl Hash for F64Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

/// An owned index key.
///
/// Keys of different kinds order by kind first, but indexes never mix kinds:
/// inserting a key of the wrong kind is an error, and querying with a key of
/// the wrong kind matches nothing. A predicate compiler must coerce literals to
/// the field's kind before probing.
///
/// Arrays of scalars are indexed as one key per element for the same row.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarKey {
    /// Boolean key.
    Bool(bool),
    /// Integer key; timestamps are microseconds since the Unix epoch.
    Int(i64),
    /// Float key, never NaN.
    Float(F64Key),
    /// String key.
    Str(Box<str>),
}

impl ScalarKey {
    /// Build a float key, rejecting NaN.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::NanKey`] when `value` is NaN.
    pub fn float(value: f64) -> Result<Self, ScalarError> {
        F64Key::new(value).map(Self::Float)
    }

    /// Build a timestamp key from microseconds since the Unix epoch.
    ///
    /// Timestamps share [`KeyKind::Int`] with integers.
    #[must_use]
    pub fn timestamp_micros(micros: i64) -> Self {
        Self::Int(micros)
    }

    /// Build a string key.
    #[must_use]
    pub fn string(value: impl Into<Box<str>>) -> Self {
        Self::Str(value.into())
    }

    /// Kind of this key.
    #[must_use]
    pub fn kind(&self) -> KeyKind {
        match self {
            Self::Bool(_) => KeyKind::Bool,
            Self::Int(_) => KeyKind::Int,
            Self::Float(_) => KeyKind::Float,
            Self::Str(_) => KeyKind::Str,
        }
    }

    /// Borrow this key.
    #[must_use]
    pub fn as_key_ref(&self) -> ScalarKeyRef<'_> {
        match self {
            Self::Bool(value) => ScalarKeyRef::Bool(*value),
            Self::Int(value) => ScalarKeyRef::Int(*value),
            Self::Float(value) => ScalarKeyRef::Float(*value),
            Self::Str(value) => ScalarKeyRef::Str(value),
        }
    }
}

impl From<bool> for ScalarKey {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for ScalarKey {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<F64Key> for ScalarKey {
    fn from(value: F64Key) -> Self {
        Self::Float(value)
    }
}

impl From<&str> for ScalarKey {
    fn from(value: &str) -> Self {
        Self::Str(value.into())
    }
}

impl From<String> for ScalarKey {
    fn from(value: String) -> Self {
        Self::Str(value.into_boxed_str())
    }
}

impl TryFrom<f64> for ScalarKey {
    type Error = ScalarError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::float(value)
    }
}

/// A borrowed index key, as yielded by ordered iteration and zone maps.
///
/// Orders exactly like [`ScalarKey`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarKeyRef<'a> {
    /// Boolean key.
    Bool(bool),
    /// Integer key.
    Int(i64),
    /// Float key.
    Float(F64Key),
    /// String key.
    Str(&'a str),
}

impl ScalarKeyRef<'_> {
    /// Kind of this key.
    #[must_use]
    pub fn kind(&self) -> KeyKind {
        match self {
            Self::Bool(_) => KeyKind::Bool,
            Self::Int(_) => KeyKind::Int,
            Self::Float(_) => KeyKind::Float,
            Self::Str(_) => KeyKind::Str,
        }
    }

    /// Copy into an owned key.
    #[must_use]
    pub fn to_key(&self) -> ScalarKey {
        match *self {
            Self::Bool(value) => ScalarKey::Bool(value),
            Self::Int(value) => ScalarKey::Int(value),
            Self::Float(value) => ScalarKey::Float(value),
            Self::Str(value) => ScalarKey::Str(value.into()),
        }
    }
}

/// Whether both bounds hold keys of `kind` and describe a non-empty interval.
///
/// Callers use this before `BTreeMap::range`, which panics on inverted bounds.
pub(crate) fn bounds_usable(
    kind: KeyKind,
    lower: Bound<&ScalarKey>,
    upper: Bound<&ScalarKey>,
) -> bool {
    let kind_ok = |bound: Bound<&ScalarKey>| match bound {
        Bound::Included(key) | Bound::Excluded(key) => key.kind() == kind,
        Bound::Unbounded => true,
    };
    if !kind_ok(lower) || !kind_ok(upper) {
        return false;
    }
    match (lower, upper) {
        (Bound::Included(low), Bound::Included(high)) => low <= high,
        (
            Bound::Included(low) | Bound::Excluded(low),
            Bound::Included(high) | Bound::Excluded(high),
        ) => low < high,
        _ => true,
    }
}

/// Distinct keys of one kind in strictly ascending order.
///
/// Stored as a typed column so binary searches compare plain values.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyColumn {
    Bool(Vec<bool>),
    Int(Vec<i64>),
    Float(Vec<F64Key>),
    Str(Vec<Box<str>>),
}

impl KeyColumn {
    pub(crate) fn new(kind: KeyKind) -> Self {
        match kind {
            KeyKind::Bool => Self::Bool(Vec::new()),
            KeyKind::Int => Self::Int(Vec::new()),
            KeyKind::Float => Self::Float(Vec::new()),
            KeyKind::Str => Self::Str(Vec::new()),
        }
    }

    pub(crate) fn kind(&self) -> KeyKind {
        match self {
            Self::Bool(_) => KeyKind::Bool,
            Self::Int(_) => KeyKind::Int,
            Self::Float(_) => KeyKind::Float,
            Self::Str(_) => KeyKind::Str,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Bool(keys) => keys.len(),
            Self::Int(keys) => keys.len(),
            Self::Float(keys) => keys.len(),
            Self::Str(keys) => keys.len(),
        }
    }

    /// Key at `index`. Callers guarantee `index < len()`.
    pub(crate) fn get(&self, index: usize) -> ScalarKeyRef<'_> {
        match self {
            Self::Bool(keys) => ScalarKeyRef::Bool(keys[index]),
            Self::Int(keys) => ScalarKeyRef::Int(keys[index]),
            Self::Float(keys) => ScalarKeyRef::Float(keys[index]),
            Self::Str(keys) => ScalarKeyRef::Str(&keys[index]),
        }
    }

    /// Append a key. Callers guarantee it is greater than the last key.
    pub(crate) fn push(&mut self, key: ScalarKey) -> Result<(), ScalarError> {
        match (self, key) {
            (Self::Bool(keys), ScalarKey::Bool(key)) => keys.push(key),
            (Self::Int(keys), ScalarKey::Int(key)) => keys.push(key),
            (Self::Float(keys), ScalarKey::Float(key)) => keys.push(key),
            (Self::Str(keys), ScalarKey::Str(key)) => keys.push(key),
            (column, key) => {
                return Err(ScalarError::KindMismatch {
                    expected: column.kind(),
                    found: key.kind(),
                });
            }
        }
        Ok(())
    }

    /// Whether keys are strictly ascending, as every constructor guarantees.
    pub(crate) fn is_strictly_ascending(&self) -> bool {
        fn check<T: Ord>(keys: &[T]) -> bool {
            keys.windows(2).all(|pair| pair[0] < pair[1])
        }
        match self {
            Self::Bool(keys) => check(keys),
            Self::Int(keys) => check(keys),
            Self::Float(keys) => check(keys),
            Self::Str(keys) => check(keys),
        }
    }

    /// Position of `key`, if present.
    pub(crate) fn find(&self, key: &ScalarKey) -> Option<usize> {
        match (self, key) {
            (Self::Bool(keys), ScalarKey::Bool(key)) => keys.binary_search(key).ok(),
            (Self::Int(keys), ScalarKey::Int(key)) => keys.binary_search(key).ok(),
            (Self::Float(keys), ScalarKey::Float(key)) => keys.binary_search(key).ok(),
            (Self::Str(keys), ScalarKey::Str(key)) => keys
                .binary_search_by(|probe| probe.as_ref().cmp(key.as_ref()))
                .ok(),
            _ => None,
        }
    }

    /// Positions of the keys inside the interval; empty on a kind mismatch.
    pub(crate) fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> Range<usize> {
        if !bounds_usable(self.kind(), lower, upper) {
            return 0..0;
        }
        let range = match self {
            Self::Bool(keys) => typed_range(keys, lower, upper, as_bool),
            Self::Int(keys) => typed_range(keys, lower, upper, as_int),
            Self::Float(keys) => typed_range(keys, lower, upper, as_float),
            Self::Str(keys) => typed_range::<Box<str>, str>(keys, lower, upper, as_str),
        };
        if range.start < range.end { range } else { 0..0 }
    }

    /// Positions of the string keys starting with `prefix`; empty for other kinds.
    pub(crate) fn prefix_range(&self, prefix: &str) -> Range<usize> {
        let Self::Str(keys) = self else {
            return 0..0;
        };
        let start = keys.partition_point(|key| key.as_ref() < prefix);
        let len = keys[start..].partition_point(|key| key.starts_with(prefix));
        start..start + len
    }
}

fn as_bool(key: &ScalarKey) -> Option<&bool> {
    match key {
        ScalarKey::Bool(value) => Some(value),
        _ => None,
    }
}

fn as_int(key: &ScalarKey) -> Option<&i64> {
    match key {
        ScalarKey::Int(value) => Some(value),
        _ => None,
    }
}

fn as_float(key: &ScalarKey) -> Option<&F64Key> {
    match key {
        ScalarKey::Float(value) => Some(value),
        _ => None,
    }
}

fn as_str(key: &ScalarKey) -> Option<&str> {
    match key {
        ScalarKey::Str(value) => Some(value),
        _ => None,
    }
}

/// Binary-search a typed, strictly ascending slice for an interval. Kinds were
/// checked by the caller, so a failed extraction widens to unbounded, which is
/// never reached in practice.
fn typed_range<T, Q>(
    keys: &[T],
    lower: Bound<&ScalarKey>,
    upper: Bound<&ScalarKey>,
    extract: fn(&ScalarKey) -> Option<&Q>,
) -> Range<usize>
where
    T: Borrow<Q>,
    Q: Ord + ?Sized,
{
    let start = match lower {
        Bound::Included(key) => {
            extract(key).map_or(0, |key| keys.partition_point(|probe| probe.borrow() < key))
        }
        Bound::Excluded(key) => {
            extract(key).map_or(0, |key| keys.partition_point(|probe| probe.borrow() <= key))
        }
        Bound::Unbounded => 0,
    };
    let end = match upper {
        Bound::Included(key) => extract(key).map_or(keys.len(), |key| {
            keys.partition_point(|probe| probe.borrow() <= key)
        }),
        Bound::Excluded(key) => extract(key).map_or(keys.len(), |key| {
            keys.partition_point(|probe| probe.borrow() < key)
        }),
        Bound::Unbounded => keys.len(),
    };
    start..end
}
