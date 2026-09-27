//! Primary-key sections: `PkColumn`, `PkSorted`, and `PkFilter`.
//!
//! ```text
//! PkColumn  encoding 1 (int64):  i64 keys[row_count]
//!           encoding 2 (string): u32 offsets[row_count + 1], then UTF-8 bytes
//! PkSorted  encoding 1 (int64):  i64 keys[row_count] ascending, then u32 rows[row_count]
//!           encoding 2 (string): u32 rows[row_count], sorted by key bytes
//! PkFilter  encoding 1 (binary fuse, 8-bit fingerprints):
//!   0  8 seed
//!   8  4 segment_length        power of two, at most 2^18
//!  12  4 segment_count
//!  16  4 array_length          (segment_count + 2) * segment_length
//!  20  4 key_count             distinct key hashes in the filter
//!  24  8 reserved
//!  32  n fingerprints          u8[array_length]
//! ```
//!
//! The filter holds `xxh3_64(canonical_pk_bytes, seed 0)`, where the
//! canonical bytes are `0x01 ++ i64 LE` or `0x02 ++ UTF-8`. Keys in one
//! segment are unique.

use super::{
    error::{DecodeResult, Malformed, SegmentError},
    le::{Cursor, check_offsets, put_i64, put_u32, put_u64, range_at, usize_from},
};
use logpose_types::{record::PrimaryKey, schema::PrimaryKeyType};
use std::cmp::Ordering;
use twox_hash::XxHash3_64;

pub(crate) const ENCODING_INT64: u16 = 1;
pub(crate) const ENCODING_STRING: u16 = 2;
pub(crate) const FILTER_ENCODING_FUSE8: u16 = 1;

const FILTER_HEADER_LEN: usize = 32;
const MAX_SEGMENT_LENGTH: u32 = 1 << 18;

/// Encoding code for a key type.
pub(crate) fn encoding_for(key_type: PrimaryKeyType) -> u16 {
    match key_type {
        PrimaryKeyType::Int64 => ENCODING_INT64,
        PrimaryKeyType::String => ENCODING_STRING,
    }
}

/// The canonical hash of a primary key: `xxh3_64` over `0x01 ++ i64 LE` or
/// `0x02 ++ UTF-8`, seed 0. Part of the format.
#[must_use]
pub fn canonical_pk_hash(pk: &PrimaryKey) -> u64 {
    match pk {
        PrimaryKey::Int64(value) => {
            let mut bytes = [0_u8; 9];
            bytes[0] = 0x01;
            bytes[1..].copy_from_slice(&value.to_le_bytes());
            XxHash3_64::oneshot(&bytes)
        }
        PrimaryKey::String(value) => string_pk_hash(value.as_bytes()),
    }
}

fn string_pk_hash(bytes: &[u8]) -> u64 {
    let mut canonical = Vec::with_capacity(bytes.len() + 1);
    canonical.push(0x02);
    canonical.extend_from_slice(bytes);
    XxHash3_64::oneshot(&canonical)
}

/// Primary keys in row order.
#[derive(Clone, Debug, PartialEq)]
pub enum PkColumn {
    /// `int64` keys.
    Int64(Vec<i64>),
    /// `string` keys: `offsets[row..row + 2]` delimit each key in `bytes`.
    String {
        /// `row_count + 1` offsets into `bytes`.
        offsets: Vec<u32>,
        /// Concatenated UTF-8 keys.
        bytes: Vec<u8>,
    },
}

impl PkColumn {
    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Int64(keys) => keys.len(),
            Self::String { offsets, .. } => offsets.len().saturating_sub(1),
        }
    }

    /// Whether there are no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The key of `row`.
    #[must_use]
    pub fn get(&self, row: usize) -> Option<PrimaryKey> {
        match self {
            Self::Int64(keys) => keys.get(row).copied().map(PrimaryKey::Int64),
            Self::String { .. } => self
                .str_at(row)
                .map(|key| PrimaryKey::String(key.to_owned())),
        }
    }

    fn str_bytes(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Int64(_) => None,
            Self::String { offsets, bytes } => bytes.get(range_at(offsets, row)?),
        }
    }

    fn str_at(&self, row: usize) -> Option<&str> {
        // Validated as UTF-8 when decoded.
        std::str::from_utf8(self.str_bytes(row)?).ok()
    }

    pub(crate) fn decode(bytes: &[u8], encoding: u16, row_count: usize) -> DecodeResult<Self> {
        let mut cursor = Cursor::new(bytes);
        let column = match encoding {
            ENCODING_INT64 => Self::Int64(cursor.i64s(row_count)?),
            ENCODING_STRING => {
                let offsets = cursor.u32s(row_count + 1)?;
                let data = cursor.take(cursor.remaining())?.to_vec();
                check_offsets(&offsets, data.len())?;
                for row in 0..row_count {
                    let range =
                        range_at(&offsets, row).ok_or_else(|| Malformed::new("bad key offsets"))?;
                    if std::str::from_utf8(&data[range]).is_err() {
                        return Err(Malformed::new(format!("key of row {row} is not UTF-8")));
                    }
                }
                Self::String {
                    offsets,
                    bytes: data,
                }
            }
            other => return Err(Malformed::new(format!("unknown pk encoding {other}"))),
        };
        cursor.finish()?;
        Ok(column)
    }

    /// Compare the key of `row` with `pk`. Keys of the wrong type compare as
    /// less, which never matches.
    fn cmp_row(&self, row: usize, pk: &PrimaryKey) -> Ordering {
        match (self, pk) {
            (Self::Int64(keys), PrimaryKey::Int64(value)) => {
                keys.get(row).map_or(Ordering::Less, |key| key.cmp(value))
            }
            (Self::String { .. }, PrimaryKey::String(value)) => self
                .str_bytes(row)
                .map_or(Ordering::Less, |key| key.cmp(value.as_bytes())),
            _ => Ordering::Less,
        }
    }

    fn row_hash(&self, row: usize) -> Option<u64> {
        match self {
            Self::Int64(keys) => keys
                .get(row)
                .map(|key| canonical_pk_hash(&PrimaryKey::Int64(*key))),
            Self::String { .. } => self.str_bytes(row).map(string_pk_hash),
        }
    }
}

/// Row ids sorted by primary key.
#[derive(Clone, Debug, PartialEq)]
pub enum PkSorted {
    /// `int64`: keys ascending and their rows, so lookups need nothing else.
    Int64 {
        /// Keys, strictly ascending.
        keys: Vec<i64>,
        /// Row of each key.
        rows: Vec<u32>,
    },
    /// `string`: rows ordered by key bytes; lookups read the [`PkColumn`].
    String {
        /// Rows, ordered by key.
        rows: Vec<u32>,
    },
}

impl PkSorted {
    pub(crate) fn decode(bytes: &[u8], encoding: u16, row_count: usize) -> DecodeResult<Self> {
        let mut cursor = Cursor::new(bytes);
        let sorted = match encoding {
            ENCODING_INT64 => {
                let keys = cursor.i64s(row_count)?;
                let rows = cursor.u32s(row_count)?;
                if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(Malformed::new("sorted keys are not strictly ascending"));
                }
                Self::Int64 { keys, rows }
            }
            ENCODING_STRING => Self::String {
                rows: cursor.u32s(row_count)?,
            },
            other => return Err(Malformed::new(format!("unknown pk encoding {other}"))),
        };
        cursor.finish()?;
        if sorted
            .rows()
            .iter()
            .any(|row| usize_from(*row) >= row_count)
        {
            return Err(Malformed::new("sorted row id out of range"));
        }
        Ok(sorted)
    }

    /// Rows in key order.
    #[must_use]
    pub fn rows(&self) -> &[u32] {
        match self {
            Self::Int64 { rows, .. } | Self::String { rows } => rows,
        }
    }

    /// Find the row holding `pk`, using `column` for string keys.
    #[must_use]
    pub fn find(&self, column: &PkColumn, pk: &PrimaryKey) -> Option<u32> {
        match (self, pk) {
            (Self::Int64 { keys, rows }, PrimaryKey::Int64(value)) => keys
                .binary_search(value)
                .ok()
                .and_then(|index| rows.get(index).copied()),
            (Self::String { rows }, PrimaryKey::String(_)) => {
                let index = rows
                    .binary_search_by(|row| column.cmp_row(usize_from(*row), pk))
                    .ok()?;
                rows.get(index).copied()
            }
            _ => None,
        }
    }

    /// Check that this is a permutation of the column's rows in strictly
    /// ascending key order.
    pub(crate) fn check_against(&self, column: &PkColumn) -> DecodeResult<()> {
        let rows = self.rows();
        if rows.len() != column.len() {
            return Err(Malformed::new("sorted pk and pk column differ in length"));
        }
        let mut seen = vec![false; rows.len()];
        for row in rows {
            let slot = seen
                .get_mut(usize_from(*row))
                .ok_or_else(|| Malformed::new("sorted row id out of range"))?;
            if *slot {
                return Err(Malformed::new("sorted rows repeat a row"));
            }
            *slot = true;
        }
        match (self, column) {
            (Self::Int64 { keys, rows }, PkColumn::Int64(column)) => {
                for (key, row) in keys.iter().zip(rows) {
                    if column.get(usize_from(*row)) != Some(key) {
                        return Err(Malformed::new("sorted key disagrees with the pk column"));
                    }
                }
            }
            (Self::String { rows }, PkColumn::String { .. }) => {
                for pair in rows.windows(2) {
                    let left = column.str_bytes(usize_from(pair[0]));
                    let right = column.str_bytes(usize_from(pair[1]));
                    if left >= right {
                        return Err(Malformed::new("sorted string keys are not ascending"));
                    }
                }
            }
            _ => {
                return Err(Malformed::new(
                    "sorted pk and pk column have different types",
                ));
            }
        }
        Ok(())
    }
}

/// Encode the `PkColumn` and `PkSorted` payloads.
///
/// # Errors
///
/// [`SegmentError::DuplicatePrimaryKey`] if two rows share a key, or
/// [`SegmentError::TooLarge`] if string keys exceed `u32` offsets.
pub(crate) fn encode_pk_sections(column: &PkColumn) -> Result<(Vec<u8>, Vec<u8>), SegmentError> {
    let mut column_bytes = Vec::new();
    let mut sorted_bytes = Vec::new();
    match column {
        PkColumn::Int64(keys) => {
            for key in keys {
                put_i64(&mut column_bytes, *key);
            }
            let mut order: Vec<(i64, u32)> = keys
                .iter()
                .enumerate()
                .map(|(row, key)| (*key, row_u32(row)))
                .collect();
            order.sort_unstable();
            if let Some(pair) = order.windows(2).find(|pair| pair[0].0 == pair[1].0) {
                return Err(SegmentError::DuplicatePrimaryKey {
                    pk: PrimaryKey::Int64(pair[0].0),
                });
            }
            for (key, _) in &order {
                put_i64(&mut sorted_bytes, *key);
            }
            for (_, row) in &order {
                put_u32(&mut sorted_bytes, *row);
            }
        }
        PkColumn::String { offsets, bytes } => {
            for offset in offsets {
                put_u32(&mut column_bytes, *offset);
            }
            column_bytes.extend_from_slice(bytes);
            let mut rows: Vec<u32> = (0..column.len()).map(row_u32).collect();
            rows.sort_unstable_by(|left, right| {
                column
                    .str_bytes(usize_from(*left))
                    .cmp(&column.str_bytes(usize_from(*right)))
            });
            if let Some(pair) = rows.windows(2).find(|pair| {
                column.str_bytes(usize_from(pair[0])) == column.str_bytes(usize_from(pair[1]))
            }) {
                let pk = column
                    .get(usize_from(pair[0]))
                    .unwrap_or(PrimaryKey::String(String::new()));
                return Err(SegmentError::DuplicatePrimaryKey { pk });
            }
            for row in rows {
                put_u32(&mut sorted_bytes, row);
            }
        }
    }
    Ok((column_bytes, sorted_bytes))
}

fn row_u32(row: usize) -> u32 {
    // The builder caps rows below u32::MAX.
    u32::try_from(row).unwrap_or(u32::MAX)
}

/// A binary fuse filter with 8-bit fingerprints over canonical key hashes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkFilter {
    seed: u64,
    segment_length: u32,
    segment_count: u32,
    key_count: u32,
    fingerprints: Vec<u8>,
}

impl PkFilter {
    /// Whether `pk` may be in the segment. False positives occur at about
    /// 0.4 percent; false negatives never.
    #[must_use]
    pub fn may_contain(&self, pk: &PrimaryKey) -> bool {
        self.contains_hash(canonical_pk_hash(pk))
    }

    /// Number of distinct key hashes in the filter.
    #[must_use]
    pub fn key_count(&self) -> u32 {
        self.key_count
    }

    fn contains_hash(&self, key: u64) -> bool {
        if self.key_count == 0 {
            return false;
        }
        let hash = mix(key.wrapping_add(self.seed));
        let [h0, h1, h2] = positions(hash, self.segment_length, self.segment_count);
        let fingerprint = fingerprint(hash);
        let lookup = |index: usize| self.fingerprints.get(index).copied();
        match (lookup(h0), lookup(h1), lookup(h2)) {
            (Some(a), Some(b), Some(c)) => fingerprint ^ a ^ b ^ c == 0,
            _ => false,
        }
    }

    /// Build a filter over the keys of `column`.
    pub(crate) fn build(column: &PkColumn) -> Self {
        let mut hashes: Vec<u64> = (0..column.len())
            .filter_map(|row| column.row_hash(row))
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        let key_count = u32::try_from(hashes.len()).unwrap_or(u32::MAX);
        if hashes.is_empty() {
            return Self {
                seed: 0,
                segment_length: 4,
                segment_count: 1,
                key_count: 0,
                fingerprints: vec![0; 12],
            };
        }
        let (segment_length, mut segment_count) = sizing(hashes.len());
        let mut seed_state = 0_u64;
        let mut attempt = 0_u32;
        loop {
            let seed = splitmix64(&mut seed_state);
            if let Some(fingerprints) = populate(&hashes, seed, segment_length, segment_count) {
                return Self {
                    seed,
                    segment_length,
                    segment_count,
                    key_count,
                    fingerprints,
                };
            }
            attempt += 1;
            if attempt.is_multiple_of(8) {
                segment_count = segment_count.saturating_add(1);
            }
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(FILTER_HEADER_LEN + self.fingerprints.len());
        put_u64(&mut out, self.seed);
        put_u32(&mut out, self.segment_length);
        put_u32(&mut out, self.segment_count);
        put_u32(
            &mut out,
            u32::try_from(self.fingerprints.len()).unwrap_or(u32::MAX),
        );
        put_u32(&mut out, self.key_count);
        out.resize(FILTER_HEADER_LEN, 0);
        out.extend_from_slice(&self.fingerprints);
        out
    }

    pub(crate) fn decode(bytes: &[u8], encoding: u16) -> DecodeResult<Self> {
        if encoding != FILTER_ENCODING_FUSE8 {
            return Err(Malformed::new(format!(
                "unknown pk filter encoding {encoding}"
            )));
        }
        let mut cursor = Cursor::new(bytes);
        let seed = cursor.u64()?;
        let segment_length = cursor.u32()?;
        let segment_count = cursor.u32()?;
        let array_length = cursor.u32()?;
        let key_count = cursor.u32()?;
        cursor.zeros(8)?;
        if !segment_length.is_power_of_two() || segment_length > MAX_SEGMENT_LENGTH {
            return Err(Malformed::new("filter segment length is invalid"));
        }
        let expected = u64::from(segment_count)
            .checked_add(2)
            .and_then(|segments| segments.checked_mul(u64::from(segment_length)));
        if segment_count == 0 || expected != Some(u64::from(array_length)) {
            return Err(Malformed::new("filter array length is inconsistent"));
        }
        let fingerprints = cursor.take(usize_from(array_length))?.to_vec();
        cursor.finish()?;
        Ok(Self {
            seed,
            segment_length,
            segment_count,
            key_count,
            fingerprints,
        })
    }

    /// Check that every key of `column` passes the filter.
    pub(crate) fn check_against(&self, column: &PkColumn) -> DecodeResult<()> {
        for row in 0..column.len() {
            let hash = column
                .row_hash(row)
                .ok_or_else(|| Malformed::new("pk column row is unreadable"))?;
            if !self.contains_hash(hash) {
                return Err(Malformed::new(format!(
                    "pk filter rejects the key of row {row}"
                )));
            }
        }
        Ok(())
    }
}

/// Segment length and count for `size` keys, following the reference
/// binary fuse sizing for arity 3.
fn sizing(size: usize) -> (u32, u32) {
    let size_f = size as f64;
    let segment_length = if size <= 1 {
        4
    } else {
        let exponent = (size_f.ln() / 3.33_f64.ln() + 2.25).floor();
        let exponent = exponent.clamp(2.0, 18.0) as u32;
        1_u32 << exponent
    };
    let size_factor = if size <= 1 {
        0.0
    } else {
        f64::max(1.125, 0.875 + 0.25 * 1_000_000_f64.ln() / size_f.ln())
    };
    let capacity = (size_f * size_factor).round() as u64;
    let segments = capacity.div_ceil(u64::from(segment_length));
    let segment_count = u32::try_from(segments.saturating_sub(2).max(1)).unwrap_or(u32::MAX);
    (segment_length, segment_count)
}

/// Peel the 3-hypergraph and assign fingerprints; `None` if it has a core.
fn populate(hashes: &[u64], seed: u64, segment_length: u32, segment_count: u32) -> Option<Vec<u8>> {
    let array_length = usize_from(segment_count.checked_add(2)?.checked_mul(segment_length)?);
    let mut counts = vec![0_u32; array_length];
    let mut xors = vec![0_u64; array_length];
    let mixed: Vec<u64> = hashes
        .iter()
        .map(|key| mix(key.wrapping_add(seed)))
        .collect();
    for hash in &mixed {
        for position in positions(*hash, segment_length, segment_count) {
            counts[position] += 1;
            xors[position] ^= *hash;
        }
    }
    let mut queue: Vec<usize> = (0..array_length)
        .filter(|index| counts[*index] == 1)
        .collect();
    let mut stack: Vec<(u64, usize)> = Vec::with_capacity(mixed.len());
    while let Some(position) = queue.pop() {
        if counts[position] != 1 {
            continue;
        }
        let hash = xors[position];
        stack.push((hash, position));
        for other in positions(hash, segment_length, segment_count) {
            counts[other] -= 1;
            xors[other] ^= hash;
            if counts[other] == 1 {
                queue.push(other);
            }
        }
    }
    if stack.len() != mixed.len() {
        return None;
    }
    let mut fingerprints = vec![0_u8; array_length];
    for (hash, position) in stack.into_iter().rev() {
        let mut value = fingerprint(hash);
        for other in positions(hash, segment_length, segment_count) {
            if other != position {
                value ^= fingerprints[other];
            }
        }
        fingerprints[position] = value;
    }
    Some(fingerprints)
}

fn positions(hash: u64, segment_length: u32, segment_count: u32) -> [usize; 3] {
    let segment_count_length = u64::from(segment_count) * u64::from(segment_length);
    let mask = u64::from(segment_length - 1);
    let h0 = ((u128::from(hash) * u128::from(segment_count_length)) >> 64) as u64;
    let h1 = (h0 + u64::from(segment_length)) ^ ((hash >> 18) & mask);
    let h2 = (h0 + 2 * u64::from(segment_length)) ^ (hash & mask);
    [h0, h1, h2].map(|position| usize::try_from(position).unwrap_or(usize::MAX))
}

fn fingerprint(hash: u64) -> u8 {
    (hash ^ (hash >> 32)) as u8
}

/// The murmur3 64-bit finalizer.
fn mix(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
