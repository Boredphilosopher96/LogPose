//! Binary layout of the immutable scalar indexes.
//!
//! All integers are little-endian.
//!
//! ```text
//! offset   size  field
//! 0        4     magic "LPSX"
//! 4        2     format version, currently 1
//! 6        1     index type: 1 inverted, 2 sorted
//! 7        1     key kind: 1 bool, 2 int, 3 float, 4 string
//! 8        8     body length n
//! 16       n     body
//! 16 + n   4     CRC32 (IEEE) of bytes 0 .. 16 + n
//! ```
//!
//! Body encodings:
//!
//! ```text
//! bitmap        := u32 byte length, roaring portable serialization
//! key column    := u64 count, then per key:
//!                    bool u8 (0 or 1) | int i64 | float f64 bits | string u32 length + UTF-8
//! inverted body := nulls bitmap, key column, one non-empty bitmap per key
//! sorted body   := nulls bitmap, key column, (count + 1) u32 run offsets,
//!                  u64 row count, u32 rows
//! ```
//!
//! Decoding checks the magic, version, exact length and checksum first, then
//! validates the structure (strictly ascending keys, canonical floats, UTF-8,
//! monotone offsets, ascending runs, disjoint nulls) so that every accepted
//! input satisfies the in-memory invariants. It never panics.

use super::{
    F64Key, InvertedIndex, KeyKind, RoaringBitmap, ScalarError, SortedIndex, key::KeyColumn,
};

const MAGIC: [u8; 4] = *b"LPSX";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 16;
const CRC_LEN: usize = 4;
const TYPE_INVERTED: u8 = 1;
const TYPE_SORTED: u8 = 2;

impl InvertedIndex {
    /// Serialize into the checksummed binary layout.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::KeyTooLong`] for a string key over `u32::MAX`
    /// bytes and [`ScalarError::Encode`] if a bitmap fails to serialize.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ScalarError> {
        let mut bytes = Vec::new();
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }

    /// Append the checksummed binary layout to `out`, for example a segment
    /// section being assembled, without an intermediate buffer.
    ///
    /// On error `out` is left as it was.
    ///
    /// # Errors
    ///
    /// See [`InvertedIndex::to_bytes`].
    pub fn write_to(&self, out: &mut Vec<u8>) -> Result<(), ScalarError> {
        sealed(out, TYPE_INVERTED, self.key_column().kind(), |body| {
            write_bitmap(body, self.nulls())?;
            write_keys(body, self.key_column())?;
            for bitmap in self.bitmaps() {
                write_bitmap(body, bitmap)?;
            }
            Ok(())
        })
    }

    /// Load from bytes written by [`InvertedIndex::to_bytes`].
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are truncated, corrupt, of another
    /// version, or hold a sorted index.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ScalarError> {
        let (kind, body) = open(bytes, TYPE_INVERTED)?;
        let mut reader = Reader::new(body);
        let nulls = reader.bitmap()?;
        let keys = reader.keys(kind)?;
        let mut bitmaps = Vec::with_capacity(keys.len());
        for _ in 0..keys.len() {
            let bitmap = reader.bitmap()?;
            if bitmap.is_empty() {
                return Err(ScalarError::Corrupt("empty key bitmap"));
            }
            bitmaps.push(bitmap);
        }
        reader.finish()?;
        Self::from_parts(keys, bitmaps, nulls)
    }
}

impl SortedIndex {
    /// Serialize into the checksummed binary layout.
    ///
    /// # Errors
    ///
    /// Returns [`ScalarError::KeyTooLong`] for a string key over `u32::MAX`
    /// bytes and [`ScalarError::Encode`] if a bitmap fails to serialize.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ScalarError> {
        let mut bytes = Vec::with_capacity(
            HEADER_LEN + CRC_LEN + self.rows().len() * 4 + self.offsets().len() * 4,
        );
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }

    /// Append the checksummed binary layout to `out`, for example a segment
    /// section being assembled, without an intermediate buffer.
    ///
    /// On error `out` is left as it was.
    ///
    /// # Errors
    ///
    /// See [`SortedIndex::to_bytes`].
    pub fn write_to(&self, out: &mut Vec<u8>) -> Result<(), ScalarError> {
        sealed(out, TYPE_SORTED, self.key_column().kind(), |body| {
            write_bitmap(body, self.nulls())?;
            write_keys(body, self.key_column())?;
            for offset in self.offsets() {
                body.extend_from_slice(&offset.to_le_bytes());
            }
            body.extend_from_slice(&(self.rows().len() as u64).to_le_bytes());
            for row in self.rows() {
                body.extend_from_slice(&row.to_le_bytes());
            }
            Ok(())
        })
    }

    /// Load from bytes written by [`SortedIndex::to_bytes`].
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are truncated, corrupt, of another
    /// version, or hold an inverted index.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ScalarError> {
        let (kind, body) = open(bytes, TYPE_SORTED)?;
        let mut reader = Reader::new(body);
        let nulls = reader.bitmap()?;
        let keys = reader.keys(kind)?;
        let offsets = reader.u32_array(keys.len() + 1)?;
        let row_count = reader.count(4)?;
        let rows = reader.u32_array(row_count)?;
        reader.finish()?;

        if offsets.first() != Some(&0)
            || offsets.last().map(|&end| end as usize) != Some(rows.len())
        {
            return Err(ScalarError::Corrupt("run offsets do not span the rows"));
        }
        // Check every offset before slicing with any of them: with first 0,
        // last rows.len() and strict ascent, every run is in bounds.
        if offsets.windows(2).any(|run| run[0] >= run[1]) {
            return Err(ScalarError::Corrupt(
                "run offsets are not strictly ascending",
            ));
        }
        for run in offsets.windows(2) {
            let slice = rows
                .get(run[0] as usize..run[1] as usize)
                .ok_or(ScalarError::Corrupt("run offset out of range"))?;
            if slice.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(ScalarError::Corrupt(
                    "rows of a key are not strictly ascending",
                ));
            }
        }
        Self::from_parts(keys, offsets, rows, nulls)
    }
}

/// Append the header, the body that `write_body` appends, and the checksum
/// trailer to `out`. On error `out` is truncated back to its original length.
fn sealed(
    out: &mut Vec<u8>,
    index_type: u8,
    kind: KeyKind,
    write_body: impl FnOnce(&mut Vec<u8>) -> Result<(), ScalarError>,
) -> Result<(), ScalarError> {
    let start = out.len();
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.push(index_type);
    out.push(kind.tag());
    // Body length, patched once the body is written.
    out.extend_from_slice(&0u64.to_le_bytes());
    if let Err(error) = write_body(out) {
        out.truncate(start);
        return Err(error);
    }
    let body_len = (out.len() - start - HEADER_LEN) as u64;
    out[start + 8..start + HEADER_LEN].copy_from_slice(&body_len.to_le_bytes());
    let crc = crc32fast::hash(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// Validate the header and checksum; return the key kind and the body.
fn open(bytes: &[u8], expected_type: u8) -> Result<(KeyKind, &[u8]), ScalarError> {
    let mut header = Reader::new(bytes);
    if header.take(4)? != MAGIC {
        return Err(ScalarError::BadMagic);
    }
    let version = header.u16()?;
    if version != VERSION {
        return Err(ScalarError::UnsupportedVersion(version));
    }
    let index_type = header.u8()?;
    let kind_tag = header.u8()?;
    let body_len = header.u64()?;
    let expected_len = usize::try_from(body_len)
        .ok()
        .and_then(|len| len.checked_add(HEADER_LEN + CRC_LEN));
    if expected_len != Some(bytes.len()) {
        return Err(ScalarError::Corrupt("length does not match the header"));
    }
    let (sealed, trailer) = bytes.split_at(bytes.len() - CRC_LEN);
    let stored = Reader::new(trailer).u32()?;
    let computed = crc32fast::hash(sealed);
    if stored != computed {
        return Err(ScalarError::ChecksumMismatch { stored, computed });
    }
    if index_type != expected_type {
        return Err(ScalarError::Corrupt("wrong index type"));
    }
    let kind = KeyKind::from_tag(kind_tag).ok_or(ScalarError::Corrupt("unknown key kind"))?;
    Ok((kind, &sealed[HEADER_LEN..]))
}

fn write_bitmap(out: &mut Vec<u8>, bitmap: &RoaringBitmap) -> Result<(), ScalarError> {
    let len = u32::try_from(bitmap.serialized_size()).map_err(|_| ScalarError::TooManyEntries)?;
    out.extend_from_slice(&len.to_le_bytes());
    bitmap.serialize_into(&mut *out)?;
    Ok(())
}

fn write_keys(out: &mut Vec<u8>, keys: &KeyColumn) -> Result<(), ScalarError> {
    out.extend_from_slice(&(keys.len() as u64).to_le_bytes());
    match keys {
        KeyColumn::Bool(keys) => out.extend(keys.iter().map(|&key| u8::from(key))),
        KeyColumn::Int(keys) => {
            for key in keys {
                out.extend_from_slice(&key.to_le_bytes());
            }
        }
        KeyColumn::Float(keys) => {
            for key in keys {
                out.extend_from_slice(&key.get().to_bits().to_le_bytes());
            }
        }
        KeyColumn::Str(keys) => {
            for key in keys {
                let len = u32::try_from(key.len()).map_err(|_| ScalarError::KeyTooLong)?;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(key.as_bytes());
            }
        }
    }
    Ok(())
}

/// Bounds-checked little-endian reader over a byte slice.
struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ScalarError> {
        if len > self.bytes.len() {
            return Err(ScalarError::Corrupt("truncated"));
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ScalarError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ScalarError::Corrupt("truncated"))
    }

    fn u8(&mut self) -> Result<u8, ScalarError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, ScalarError> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Result<u32, ScalarError> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64, ScalarError> {
        self.array().map(u64::from_le_bytes)
    }

    /// Read a u64 element count and check that `count * min_size` bytes
    /// remain, so a corrupt count cannot trigger a huge allocation.
    fn count(&mut self, min_size: usize) -> Result<usize, ScalarError> {
        let count = usize::try_from(self.u64()?).map_err(|_| ScalarError::Corrupt("count"))?;
        match count.checked_mul(min_size) {
            Some(bytes) if bytes <= self.bytes.len() => Ok(count),
            _ => Err(ScalarError::Corrupt("count exceeds the remaining bytes")),
        }
    }

    fn u32_array(&mut self, count: usize) -> Result<Vec<u32>, ScalarError> {
        let len = count.checked_mul(4).ok_or(ScalarError::Corrupt("count"))?;
        let bytes = self.take(len)?;
        Ok(bytes
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect())
    }

    fn bitmap(&mut self) -> Result<RoaringBitmap, ScalarError> {
        let len = self.u32()? as usize;
        let mut bytes = self.take(len)?;
        let bitmap = RoaringBitmap::deserialize_from(&mut bytes)
            .map_err(|_| ScalarError::Corrupt("invalid bitmap"))?;
        if !bytes.is_empty() {
            return Err(ScalarError::Corrupt("bitmap length mismatch"));
        }
        Ok(bitmap)
    }

    fn keys(&mut self, kind: KeyKind) -> Result<KeyColumn, ScalarError> {
        let keys = match kind {
            KeyKind::Bool => {
                let count = self.count(1)?;
                let keys = self
                    .take(count)?
                    .iter()
                    .map(|&byte| match byte {
                        0 => Ok(false),
                        1 => Ok(true),
                        _ => Err(ScalarError::Corrupt("invalid bool key")),
                    })
                    .collect::<Result<_, _>>()?;
                KeyColumn::Bool(keys)
            }
            KeyKind::Int => {
                let count = self.count(8)?;
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(i64::from_le_bytes(self.array()?));
                }
                KeyColumn::Int(keys)
            }
            KeyKind::Float => {
                let count = self.count(8)?;
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    let value = f64::from_bits(self.u64()?);
                    let key =
                        F64Key::new(value).map_err(|_| ScalarError::Corrupt("NaN float key"))?;
                    if key.get().to_bits() != value.to_bits() {
                        return Err(ScalarError::Corrupt("non-canonical float key"));
                    }
                    keys.push(key);
                }
                KeyColumn::Float(keys)
            }
            KeyKind::Str => {
                let count = self.count(4)?;
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    let len = self.u32()? as usize;
                    let key = std::str::from_utf8(self.take(len)?)
                        .map_err(|_| ScalarError::Corrupt("string key is not UTF-8"))?;
                    keys.push(Box::<str>::from(key));
                }
                KeyColumn::Str(keys)
            }
        };
        if !keys.is_strictly_ascending() {
            return Err(ScalarError::Corrupt("keys are not strictly ascending"));
        }
        Ok(keys)
    }

    fn finish(&self) -> Result<(), ScalarError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(ScalarError::Corrupt("trailing bytes in body"))
        }
    }
}
