//! `VectorF32` sections: raw vectors of one field in CRC-checked pages.
//!
//! ```text
//! offset size field
//!      0    4 dim
//!      4    4 row_count
//!      8    4 page_rows       max(1, 8192 / (dim * 4))
//!     12    4 page_count      ceil(row_count / page_rows)
//!     16    8 nulls_len       bytes of the null bitmap (0 when no nulls)
//!     24    4 prefix_crc      crc32c of bytes 0..24 and 28..prefix_end
//!     28   36 reserved
//!     64    n nulls           RoaringBitmap portable serialization, padded to 8
//!      .  4*p page_crcs       crc32c of each page's bytes
//!      .    . padding to 64   (prefix_end is the end of page_crcs)
//!      .    . data            row_count * dim f32 LE, rows contiguous; null rows are zeros
//! ```
//!
//! The prefix is the load unit for the null bitmap and page CRCs, and a page
//! (`page_rows` consecutive rows) is the load unit for rerank.

use super::{
    error::{DecodeResult, Malformed, SegmentError},
    format::{crc, crc_append},
    le::{Cursor, align_up, pad_to, put_u32, put_u64, usize_from, usize_from_u64},
};
use logpose_types::schema::FieldId;
use roaring::RoaringBitmap;
use std::ops::Range;

pub(crate) const VECTOR_ENCODING_F32: u16 = 1;
const PREFIX_HEADER_LEN: usize = 64;
const PAGE_BYTES: u32 = 8192;
const PREFIX_CRC_AT: usize = 24;

/// Rows per page for `dim` dimensions.
#[must_use]
pub fn page_rows_for(dim: u32) -> u32 {
    (PAGE_BYTES / dim.saturating_mul(4).max(1)).max(1)
}

/// Vectors of one field under construction.
#[derive(Clone, Debug)]
pub(crate) struct VectorBuf {
    pub(crate) field: FieldId,
    pub(crate) dim: u32,
    /// Row-major little-endian `f32` bytes.
    pub(crate) data: Vec<u8>,
    pub(crate) nulls: RoaringBitmap,
    pub(crate) rows: u32,
}

impl VectorBuf {
    pub(crate) fn new(field: FieldId, dim: u32) -> Self {
        Self {
            field,
            dim,
            data: Vec::new(),
            nulls: RoaringBitmap::new(),
            rows: 0,
        }
    }

    fn stride(&self) -> usize {
        usize_from(self.dim) * 4
    }

    pub(crate) fn push_null(&mut self) {
        self.nulls.insert(self.rows);
        self.rows += 1;
        self.data.resize(self.data.len() + self.stride(), 0);
    }

    /// Set the last row from little-endian bytes of exactly `dim` floats.
    pub(crate) fn set_last_le(&mut self, bytes: &[u8]) -> Result<(), SegmentError> {
        let Some(row) = self.rows.checked_sub(1) else {
            return Ok(());
        };
        if bytes.len() != self.stride() {
            return Err(SegmentError::VectorDimensions {
                field: self.field,
                expected: self.dim,
                actual: bytes.len() / 4,
            });
        }
        if !self.nulls.remove(row) {
            return Err(SegmentError::FieldAlreadySet { field: self.field });
        }
        let start = self.data.len() - self.stride();
        self.data[start..].copy_from_slice(bytes);
        Ok(())
    }

    /// Encode the prefix, including the padding before the data. The payload
    /// is the prefix followed by `self.data`.
    pub(crate) fn encode_prefix(&self) -> Result<Vec<u8>, SegmentError> {
        let page_rows = page_rows_for(self.dim);
        let page_count = self.rows.div_ceil(page_rows);
        let nulls = if self.nulls.is_empty() {
            Vec::new()
        } else {
            let mut bytes = Vec::with_capacity(self.nulls.serialized_size());
            self.nulls
                .serialize_into(&mut bytes)
                .map_err(|error| SegmentError::Encode(error.to_string()))?;
            bytes
        };
        let mut out = Vec::new();
        put_u32(&mut out, self.dim);
        put_u32(&mut out, self.rows);
        put_u32(&mut out, page_rows);
        put_u32(&mut out, page_count);
        put_u64(&mut out, nulls.len() as u64);
        put_u32(&mut out, 0);
        out.resize(PREFIX_HEADER_LEN, 0);
        out.extend_from_slice(&nulls);
        pad_to(&mut out, 8);
        let page_bytes = usize_from(page_rows) * self.stride();
        for page in self.data.chunks(page_bytes.max(1)) {
            put_u32(&mut out, crc(page));
        }
        let prefix_crc = crc_append(crc(&out[..PREFIX_CRC_AT]), &out[PREFIX_CRC_AT + 4..]);
        out[PREFIX_CRC_AT..PREFIX_CRC_AT + 4].copy_from_slice(&prefix_crc.to_le_bytes());
        pad_to(&mut out, 64);
        Ok(out)
    }
}

/// The decoded prefix of a `VectorF32` section.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorPrefix {
    dim: u32,
    row_count: u32,
    page_rows: u32,
    nulls: RoaringBitmap,
    page_crcs: Vec<u32>,
    /// Offset of the data from the start of the section.
    data_offset: u64,
}

impl VectorPrefix {
    /// Length of the fixed prefix header.
    pub(crate) const HEADER_LEN: usize = PREFIX_HEADER_LEN;

    /// From the first 64 bytes, the length of the whole prefix (up to the
    /// end of the page CRCs), checked against `section_len`.
    pub(crate) fn prefix_len(header: &[u8], section_len: u64) -> DecodeResult<usize> {
        let mut cursor = Cursor::new(header);
        cursor.skip(12)?;
        let page_count = cursor.u32()?;
        let nulls_len = cursor.u64()?;
        let len = align_up(nulls_len, 8)
            .and_then(|len| len.checked_add(PREFIX_HEADER_LEN as u64))
            .and_then(|len| len.checked_add(u64::from(page_count) * 4))
            .filter(|len| *len <= section_len)
            .ok_or_else(|| Malformed::new("vector prefix exceeds the section"))?;
        usize_from_u64(len)
    }

    /// Decode and verify a whole prefix for a field of `dim` dimensions in a
    /// segment of `row_count` rows.
    pub(crate) fn decode(
        bytes: &[u8],
        dim: u32,
        row_count: u32,
        section_len: u64,
    ) -> Result<Self, PrefixError> {
        if bytes.len() < PREFIX_HEADER_LEN {
            return Err(PrefixError::Malformed(Malformed::new(
                "vector prefix is truncated",
            )));
        }
        let stored = u32::from_le_bytes([
            bytes[PREFIX_CRC_AT],
            bytes[PREFIX_CRC_AT + 1],
            bytes[PREFIX_CRC_AT + 2],
            bytes[PREFIX_CRC_AT + 3],
        ]);
        if crc_append(crc(&bytes[..PREFIX_CRC_AT]), &bytes[PREFIX_CRC_AT + 4..]) != stored {
            return Err(PrefixError::Checksum);
        }
        Self::parse(bytes, dim, row_count, section_len).map_err(PrefixError::Malformed)
    }

    fn parse(bytes: &[u8], dim: u32, row_count: u32, section_len: u64) -> DecodeResult<Self> {
        let mut cursor = Cursor::new(bytes);
        let stored_dim = cursor.u32()?;
        let stored_rows = cursor.u32()?;
        let page_rows = cursor.u32()?;
        let page_count = cursor.u32()?;
        let nulls_len = usize_from_u64(cursor.u64()?)?;
        cursor.skip(4)?;
        cursor.zeros(PREFIX_HEADER_LEN - cursor.position())?;
        if stored_dim != dim || dim == 0 {
            return Err(Malformed::new(format!(
                "vector section has {stored_dim} dimensions, expected {dim}"
            )));
        }
        if stored_rows != row_count {
            return Err(Malformed::new(format!(
                "vector section has {stored_rows} rows, expected {row_count}"
            )));
        }
        if page_rows != page_rows_for(dim) || page_count != row_count.div_ceil(page_rows) {
            return Err(Malformed::new("vector page geometry is inconsistent"));
        }
        let nulls = if nulls_len == 0 {
            RoaringBitmap::new()
        } else {
            let raw = cursor.take(nulls_len)?;
            let nulls = RoaringBitmap::deserialize_from(raw)
                .map_err(|error| Malformed::new(format!("invalid null bitmap: {error}")))?;
            if nulls.serialized_size() != nulls_len {
                return Err(Malformed::new("null bitmap length mismatch"));
            }
            nulls
        };
        if nulls.max().is_some_and(|max| max >= row_count) {
            return Err(Malformed::new("null bitmap names a row out of range"));
        }
        cursor.align(8)?;
        let page_crcs = cursor.u32s(usize_from(page_count))?;
        cursor.finish()?;
        let prefix_end = bytes.len() as u64;
        let data_offset =
            align_up(prefix_end, 64).ok_or_else(|| Malformed::new("vector prefix overflows"))?;
        let data_len = u64::from(row_count)
            .checked_mul(u64::from(dim) * 4)
            .and_then(|len| len.checked_add(data_offset));
        if data_len != Some(section_len) {
            return Err(Malformed::new(
                "vector section length does not match its rows",
            ));
        }
        Ok(Self {
            dim,
            row_count,
            page_rows,
            nulls,
            page_crcs,
            data_offset,
        })
    }

    /// Number of dimensions.
    #[must_use]
    pub fn dim(&self) -> u32 {
        self.dim
    }

    /// Number of rows.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Rows per page.
    #[must_use]
    pub fn page_rows(&self) -> u32 {
        self.page_rows
    }

    /// Number of pages.
    #[must_use]
    pub fn page_count(&self) -> u32 {
        u32::try_from(self.page_crcs.len()).unwrap_or(u32::MAX)
    }

    /// Rows whose vector is null (stored as zeros).
    #[must_use]
    pub fn nulls(&self) -> &RoaringBitmap {
        &self.nulls
    }

    /// Offset of the data from the start of the section.
    #[must_use]
    pub fn data_offset(&self) -> u64 {
        self.data_offset
    }

    fn stride(&self) -> u64 {
        u64::from(self.dim) * 4
    }

    /// Rows of `page`.
    #[must_use]
    pub fn page_row_range(&self, page: u32) -> Option<Range<u32>> {
        if page >= self.page_count() {
            return None;
        }
        let start = page.checked_mul(self.page_rows)?;
        let end = start.saturating_add(self.page_rows).min(self.row_count);
        Some(start..end)
    }

    /// Byte range of `page` relative to the start of the section.
    #[must_use]
    pub fn page_byte_range(&self, page: u32) -> Option<Range<u64>> {
        let rows = self.page_row_range(page)?;
        let start = self.data_offset + u64::from(rows.start) * self.stride();
        let end = self.data_offset + u64::from(rows.end) * self.stride();
        Some(start..end)
    }

    /// Whether `bytes` match the stored CRC of `page`.
    #[must_use]
    pub fn page_matches(&self, page: u32, bytes: &[u8]) -> bool {
        self.page_crcs
            .get(usize_from(page))
            .is_some_and(|expected| crc(bytes) == *expected)
    }

    /// Check that the data holds zeros for null rows and matches every page
    /// CRC. `data` is the section's bytes from the data offset on.
    pub(crate) fn check_data(&self, data: &[u8]) -> Result<(), PrefixError> {
        let stride = usize_from(self.dim) * 4;
        for page in 0..self.page_count() {
            let range = self
                .page_row_range(page)
                .ok_or_else(|| PrefixError::Malformed(Malformed::new("page out of range")))?;
            let bytes = data
                .get(usize_from(range.start) * stride..usize_from(range.end) * stride)
                .ok_or_else(|| PrefixError::Malformed(Malformed::new("page is truncated")))?;
            if !self.page_matches(page, bytes) {
                return Err(PrefixError::Page(page));
            }
        }
        for row in &self.nulls {
            let start = usize_from(row) * stride;
            let is_zero = data
                .get(start..start + stride)
                .is_some_and(|bytes| bytes.iter().all(|byte| *byte == 0));
            if !is_zero {
                return Err(PrefixError::Malformed(Malformed::new(format!(
                    "null vector row {row} is not zeros"
                ))));
            }
        }
        Ok(())
    }
}

/// Why a prefix or page failed to verify.
#[derive(Debug)]
pub(crate) enum PrefixError {
    Checksum,
    Page(u32),
    Malformed(Malformed),
}
