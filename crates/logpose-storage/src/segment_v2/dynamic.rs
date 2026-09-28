//! `DynamicJson` sections: the `$extra` dynamic field in blocks of rows.
//!
//! ```text
//! header (64 bytes):
//!      0 4 row_count
//!      4 4 block_rows    4096
//!      8 4 block_count   ceil(row_count / block_rows)
//!     12 4 index_crc     crc32c of header bytes 0..12 and the block index
//!     16 48 reserved
//! block index: block_count x { offset u64 (relative to section), len u32, crc32c u32 }
//! blocks: each 8-byte aligned, back to back:
//!         u32 offsets[rows_in_block + 1], then value bytes
//! ```
//!
//! Each row's value is one JSON object node in the binary value codec, copied
//! unchanged from the row; an empty range means the row has no dynamic
//! keys. A block is the load and verification unit.

use super::{
    error::{DecodeResult, Malformed},
    format::{crc, crc_append},
    le::{Cursor, check_offsets, pad_to, padded, put_u32, put_u64, range_at, usize_from},
};
use crate::segment_v2::column::VarBuf;
use logpose_types::value::codec;
use serde_json::{Map, Value as JsonValue};
use std::{ops::Range, sync::OnceLock};

pub(crate) const DYNAMIC_ENCODING_BLOCKS: u16 = 1;
/// Rows per `DynamicJson` block.
pub const DYNAMIC_BLOCK_ROWS: u32 = 4096;
const HEADER_LEN: usize = 64;
const INDEX_ENTRY_LEN: usize = 16;
const INDEX_CRC_AT: usize = 12;

/// Encode the section payload from one value per row.
pub(crate) fn encode(values: &VarBuf) -> Result<Vec<u8>, super::SegmentError> {
    let too_large = || super::SegmentError::TooLarge {
        what: "dynamic block over 4 GiB",
    };
    let row_count = u32::try_from(values.len()).map_err(|_| too_large())?;
    let block_count = row_count.div_ceil(DYNAMIC_BLOCK_ROWS);
    let mut blocks = Vec::new();
    let mut index = Vec::new();
    let blocks_start = padded(HEADER_LEN + usize_from(block_count) * INDEX_ENTRY_LEN, 8);
    for block in 0..block_count {
        pad_to(&mut blocks, 8);
        let rows = block_row_range(block, row_count);
        let (start, end) = (usize_from(rows.start), usize_from(rows.end));
        let block_start = blocks.len();
        let mut used = 0_usize;
        put_u32(&mut blocks, 0);
        for row in start..end {
            used += values.get(row).len();
            put_u32(&mut blocks, u32::try_from(used).map_err(|_| too_large())?);
        }
        for row in start..end {
            blocks.extend_from_slice(values.get(row));
        }
        let len = blocks.len() - block_start;
        put_u64(&mut index, (blocks_start + block_start) as u64);
        put_u32(&mut index, u32::try_from(len).map_err(|_| too_large())?);
        put_u32(&mut index, crc(&blocks[block_start..]));
    }
    let mut out = Vec::with_capacity(blocks_start + blocks.len());
    put_u32(&mut out, row_count);
    put_u32(&mut out, DYNAMIC_BLOCK_ROWS);
    put_u32(&mut out, block_count);
    let index_crc = crc_append(crc(&out), &index);
    put_u32(&mut out, index_crc);
    out.resize(HEADER_LEN, 0);
    out.extend_from_slice(&index);
    pad_to(&mut out, 8);
    out.extend_from_slice(&blocks);
    Ok(out)
}

/// Rows of `block` in a section of `row_count` rows. The end is computed
/// without `u32` overflow: the last block of a segment near `u32::MAX` rows
/// starts at `u32::MAX - 4095`.
fn block_row_range(block: u32, row_count: u32) -> Range<u32> {
    let start = block.saturating_mul(DYNAMIC_BLOCK_ROWS).min(row_count);
    start..start + (row_count - start).min(DYNAMIC_BLOCK_ROWS)
}

/// Where one block lives inside the section.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DynamicBlockRef {
    /// Offset from the start of the section.
    pub offset: u64,
    /// Length in bytes.
    pub len: u32,
    /// CRC-32C of the block.
    pub crc32c: u32,
}

/// The decoded header and block index of a `DynamicJson` section.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicIndex {
    row_count: u32,
    blocks: Vec<DynamicBlockRef>,
}

/// Why the index failed to decode.
#[derive(Debug)]
pub(crate) enum IndexError {
    Checksum,
    Malformed(Malformed),
}

impl DynamicIndex {
    /// Length of the fixed header.
    pub(crate) const HEADER_LEN: usize = HEADER_LEN;

    /// From the 64-byte header, the length of header plus index, checked
    /// against `section_len`.
    pub(crate) fn index_len(header: &[u8], section_len: u64) -> DecodeResult<usize> {
        let mut cursor = Cursor::new(header);
        cursor.skip(8)?;
        let block_count = cursor.u32()?;
        let len = HEADER_LEN as u64 + u64::from(block_count) * INDEX_ENTRY_LEN as u64;
        if len > section_len {
            return Err(Malformed::new("dynamic block index exceeds the section"));
        }
        Ok(usize_from(
            u32::try_from(len).map_err(|_| Malformed::new("index too long"))?,
        ))
    }

    /// Decode and verify the header and index.
    pub(crate) fn decode(
        bytes: &[u8],
        row_count: u32,
        section_len: u64,
    ) -> Result<Self, IndexError> {
        if bytes.len() < HEADER_LEN {
            return Err(IndexError::Malformed(Malformed::new(
                "dynamic header is truncated",
            )));
        }
        let stored = u32::from_le_bytes([
            bytes[INDEX_CRC_AT],
            bytes[INDEX_CRC_AT + 1],
            bytes[INDEX_CRC_AT + 2],
            bytes[INDEX_CRC_AT + 3],
        ]);
        if crc_append(crc(&bytes[..INDEX_CRC_AT]), &bytes[HEADER_LEN..]) != stored {
            return Err(IndexError::Checksum);
        }
        Self::decode_verified(bytes, row_count, section_len).map_err(IndexError::Malformed)
    }

    /// Decode a header and index whose CRC was already checked (a cached unit).
    pub(crate) fn decode_verified(
        bytes: &[u8],
        row_count: u32,
        section_len: u64,
    ) -> DecodeResult<Self> {
        let mut cursor = Cursor::new(bytes);
        let stored_rows = cursor.u32()?;
        let block_rows = cursor.u32()?;
        let block_count = cursor.u32()?;
        cursor.skip(4)?;
        cursor.zeros(HEADER_LEN - cursor.position())?;
        if stored_rows != row_count {
            return Err(Malformed::new(format!(
                "dynamic section has {stored_rows} rows, expected {row_count}"
            )));
        }
        if block_rows != DYNAMIC_BLOCK_ROWS || block_count != row_count.div_ceil(block_rows) {
            return Err(Malformed::new("dynamic block geometry is inconsistent"));
        }
        let mut blocks = Vec::with_capacity(usize_from(block_count));
        let mut expected_offset = padded(bytes.len(), 8) as u64;
        for _ in 0..block_count {
            let block = DynamicBlockRef {
                offset: cursor.u64()?,
                len: cursor.u32()?,
                crc32c: cursor.u32()?,
            };
            if block.offset != expected_offset {
                return Err(Malformed::new("dynamic blocks are not contiguous"));
            }
            let end = block.offset + u64::from(block.len);
            expected_offset = super::le::align_up(end, 8)
                .ok_or_else(|| Malformed::new("dynamic block overflows"))?;
            blocks.push(block);
        }
        cursor.finish()?;
        let end = blocks
            .last()
            .map_or(padded(bytes.len(), 8) as u64, |block| {
                block.offset + u64::from(block.len)
            });
        if end != section_len {
            return Err(Malformed::new(
                "dynamic blocks do not end at the section end",
            ));
        }
        Ok(Self { row_count, blocks })
    }

    /// Number of blocks.
    #[must_use]
    pub fn block_count(&self) -> u32 {
        u32::try_from(self.blocks.len()).unwrap_or(u32::MAX)
    }

    /// Location of `block`.
    #[must_use]
    pub fn block(&self, block: u32) -> Option<DynamicBlockRef> {
        self.blocks.get(usize_from(block)).copied()
    }

    /// Rows of `block`.
    #[must_use]
    pub fn block_rows(&self, block: u32) -> Option<Range<u32>> {
        if usize_from(block) >= self.blocks.len() {
            return None;
        }
        Some(block_row_range(block, self.row_count))
    }

    /// Offset where the block area starts (after the index and padding).
    pub(crate) fn blocks_start(&self) -> u64 {
        padded(HEADER_LEN + self.blocks.len() * INDEX_ENTRY_LEN, 8) as u64
    }
}

/// One decoded block of dynamic values.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicBlock {
    first_row: u32,
    offsets: Vec<u32>,
    bytes: Vec<u8>,
    /// Every key any row of the block has, sorted, computed on first use; `None` inside when a
    /// row does not parse (then no key can be ruled out).
    keys: OnceLock<Option<Box<[Box<str>]>>>,
}

impl DynamicBlock {
    /// Decode a block holding `rows`, already checked against its CRC.
    pub(crate) fn decode(bytes: &[u8], rows: Range<u32>) -> DecodeResult<Self> {
        let count = usize_from(rows.end - rows.start);
        let mut cursor = Cursor::new(bytes);
        let offsets = cursor.u32s(count + 1)?;
        let data = cursor.take(cursor.remaining())?.to_vec();
        check_offsets(&offsets, data.len())?;
        Ok(Self {
            first_row: rows.start,
            offsets,
            bytes: data,
            keys: OnceLock::new(),
        })
    }

    /// Whether some row of the block may have the top-level key `key`: `false` only when no
    /// row has it, so a filter on the key can skip the block's rows. The block's key set is
    /// read once, from the key names only (values are skipped, not decoded).
    #[must_use]
    pub fn may_have_key(&self, key: &str) -> bool {
        let keys = self.keys.get_or_init(|| {
            let mut keys = std::collections::BTreeSet::new();
            for index in 0..self.offsets.len().saturating_sub(1) {
                let bytes = range_at(&self.offsets, index).and_then(|r| self.bytes.get(r))?;
                if bytes.is_empty() {
                    continue;
                }
                for name in codec::json_object_keys(bytes).ok()? {
                    if !keys.contains(name) {
                        keys.insert(Box::<str>::from(name));
                    }
                }
            }
            Some(keys.into_iter().collect())
        });
        keys.as_ref()
            .is_none_or(|keys| keys.binary_search_by(|name| (**name).cmp(key)).is_ok())
    }

    /// The value of `row`'s top-level key `key`, `None` when the row has no such key (or no
    /// dynamic object), decoding only that member.
    ///
    /// # Errors
    ///
    /// `Malformed` for a row outside the block or bytes that do not parse.
    pub(crate) fn member(&self, row: u32, key: &str) -> DecodeResult<Option<JsonValue>> {
        if !self.rows().contains(&row) {
            return Err(Malformed::new(format!("row {row} is not in this block")));
        }
        let Some(bytes) = self.raw(row) else {
            return Ok(None);
        };
        codec::decode_json_member(bytes, key)
            .map_err(|error| Malformed::new(format!("dynamic value: {error}")))
    }

    /// Heap bytes the decoded block holds, which the buffer cache charges beside its bytes.
    #[must_use]
    pub fn heap_bytes(&self) -> u64 {
        (self.offsets.capacity() * 4 + self.bytes.capacity()) as u64
    }

    /// Rows covered by this block.
    #[must_use]
    pub fn rows(&self) -> Range<u32> {
        let count = u32::try_from(self.offsets.len().saturating_sub(1)).unwrap_or(u32::MAX);
        self.first_row..self.first_row + count
    }

    /// Encoded bytes of `row`'s dynamic object, `None` when it has none.
    #[must_use]
    pub fn raw(&self, row: u32) -> Option<&[u8]> {
        let index = usize_from(row.checked_sub(self.first_row)?);
        let bytes = self.bytes.get(range_at(&self.offsets, index)?)?;
        (!bytes.is_empty()).then_some(bytes)
    }

    /// The decoded dynamic object of `row`, `None` when it has none.
    pub(crate) fn object(&self, row: u32) -> DecodeResult<Option<Map<String, JsonValue>>> {
        if !self.rows().contains(&row) {
            return Err(Malformed::new(format!("row {row} is not in this block")));
        }
        let Some(bytes) = self.raw(row) else {
            return Ok(None);
        };
        decode_object(bytes).map(Some)
    }
}

/// Decode one dynamic value, which must be a JSON object.
pub(crate) fn decode_object(bytes: &[u8]) -> DecodeResult<Map<String, JsonValue>> {
    match codec::decode_json(bytes) {
        Ok(JsonValue::Object(map)) => Ok(map),
        Ok(_) => Err(Malformed::new("dynamic value is not a JSON object")),
        Err(error) => Err(Malformed::new(format!("dynamic value: {error}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::{DYNAMIC_BLOCK_ROWS, block_row_range};

    #[test]
    fn block_rows_do_not_overflow_at_the_largest_segment() {
        let rows = u32::MAX - 1;
        let last = rows.div_ceil(DYNAMIC_BLOCK_ROWS) - 1;
        assert_eq!(block_row_range(last, rows), 4_294_963_200..rows);
        assert_eq!(block_row_range(0, 10), 0..10);
        assert_eq!(block_row_range(1, 8192), 4096..8192);
        assert_eq!(block_row_range(2, 8193), 8192..8193);
    }
}
