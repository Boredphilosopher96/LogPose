//! Little-endian encoding helpers and a bounds-checked decoding cursor.
//!
//! Every length the cursor is asked for is checked against the bytes that
//! remain before anything is allocated, so a corrupt count can never make a
//! decoder reserve more memory than the input it was given.

use super::error::{DecodeResult, Malformed};

/// Round `value` up to a multiple of `align`, which must be a power of two.
/// Returns `None` on overflow.
pub(crate) fn align_up(value: u64, align: u64) -> Option<u64> {
    let mask = align - 1;
    value.checked_add(mask).map(|value| value & !mask)
}

/// Append zeros until `out.len()` is a multiple of `align`.
pub(crate) fn pad_to(out: &mut Vec<u8>, align: usize) {
    let rem = out.len() % align;
    if rem != 0 {
        out.resize(out.len() + (align - rem), 0);
    }
}

/// Padded length of `len` bytes at `align` alignment, for in-memory sizes.
pub(crate) fn padded(len: usize, align: usize) -> usize {
    len.div_ceil(align) * align
}

pub(crate) fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_f64(out: &mut Vec<u8>, value: f64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Append `count` zero bytes.
pub(crate) fn put_zeros(out: &mut Vec<u8>, count: usize) {
    out.resize(out.len() + count, 0);
}

/// A forward-only reader over a byte slice with checked lengths.
pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// Current position from the start of the slice.
    pub(crate) fn position(&self) -> usize {
        self.pos
    }

    /// Bytes not yet consumed.
    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    /// Take the next `len` bytes.
    pub(crate) fn take(&mut self, len: usize) -> DecodeResult<&'a [u8]> {
        if len > self.remaining() {
            return Err(Malformed::new(format!(
                "needs {len} bytes at offset {}, but only {} remain",
                self.pos,
                self.remaining()
            )));
        }
        let slice = &self.bytes[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    pub(crate) fn skip(&mut self, len: usize) -> DecodeResult<()> {
        self.take(len).map(|_| ())
    }

    /// Skip zero padding up to the next multiple of `align` from the start.
    pub(crate) fn align(&mut self, align: usize) -> DecodeResult<()> {
        let target = padded(self.pos, align);
        let pad = self.take(target - self.pos)?;
        if pad.iter().all(|byte| *byte == 0) {
            Ok(())
        } else {
            Err(Malformed::new("padding bytes are not zero"))
        }
    }

    pub(crate) fn u8(&mut self) -> DecodeResult<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> DecodeResult<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> DecodeResult<[u8; N]> {
        let mut out = [0_u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    /// Require that the next `len` bytes are zero (reserved fields).
    pub(crate) fn zeros(&mut self, len: usize) -> DecodeResult<()> {
        if self.take(len)?.iter().all(|byte| *byte == 0) {
            Ok(())
        } else {
            Err(Malformed::new("reserved bytes are not zero"))
        }
    }

    /// Read `count` little-endian `u32`s.
    pub(crate) fn u32s(&mut self, count: usize) -> DecodeResult<Vec<u32>> {
        let bytes = self.take(checked_mul(count, 4)?)?;
        Ok(bytes
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect())
    }

    /// Read `count` little-endian `u64`s.
    pub(crate) fn u64s(&mut self, count: usize) -> DecodeResult<Vec<u64>> {
        let bytes = self.take(checked_mul(count, 8)?)?;
        Ok(bytes.chunks_exact(8).map(le_u64).collect())
    }

    /// Read `count` little-endian `i64`s.
    pub(crate) fn i64s(&mut self, count: usize) -> DecodeResult<Vec<i64>> {
        let bytes = self.take(checked_mul(count, 8)?)?;
        Ok(bytes
            .chunks_exact(8)
            .map(|chunk| le_u64(chunk).cast_signed())
            .collect())
    }

    /// Read `count` little-endian `f64`s.
    pub(crate) fn f64s(&mut self, count: usize) -> DecodeResult<Vec<f64>> {
        let bytes = self.take(checked_mul(count, 8)?)?;
        Ok(bytes
            .chunks_exact(8)
            .map(|chunk| f64::from_bits(le_u64(chunk)))
            .collect())
    }

    /// Require that every byte was consumed.
    pub(crate) fn finish(&self) -> DecodeResult<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(Malformed::new(format!(
                "{} unexpected trailing bytes",
                self.remaining()
            )))
        }
    }
}

fn le_u64(chunk: &[u8]) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(chunk);
    u64::from_le_bytes(bytes)
}

/// Decode little-endian `f32`s from bytes whose length is a multiple of 4.
pub(crate) fn f32s_from_le(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

pub(crate) fn checked_mul(count: usize, size: usize) -> DecodeResult<usize> {
    count
        .checked_mul(size)
        .ok_or_else(|| Malformed::new(format!("count {count} overflows")))
}

/// Convert a stored `u32` count to `usize`.
pub(crate) fn usize_from(value: u32) -> usize {
    // u32 always fits in usize on the 64-bit targets the engine supports.
    value as usize
}

/// Convert a stored `u64` count to `usize`, failing if it does not fit.
pub(crate) fn usize_from_u64(value: u64) -> DecodeResult<usize> {
    usize::try_from(value)
        .map_err(|_| Malformed::new(format!("length {value} does not fit in memory")))
}

/// Check a `u32` offsets array: starts at 0, never decreases, and ends at
/// `data_len`.
pub(crate) fn check_offsets(offsets: &[u32], data_len: usize) -> DecodeResult<()> {
    if offsets.first() != Some(&0) {
        return Err(Malformed::new("offsets do not start at 0"));
    }
    if offsets.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(Malformed::new("offsets decrease"));
    }
    if offsets.last().map(|last| usize_from(*last)) != Some(data_len) {
        return Err(Malformed::new("offsets do not end at the data length"));
    }
    Ok(())
}

/// The `index`-th range of an offsets array that [`check_offsets`] accepted.
pub(crate) fn range_at(offsets: &[u32], index: usize) -> Option<std::ops::Range<usize>> {
    let start = usize_from(*offsets.get(index)?);
    let end = usize_from(*offsets.get(index + 1)?);
    (start <= end).then_some(start..end)
}
