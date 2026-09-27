//! [`AlignedBytes`]: the 8-byte-aligned buffers the cache holds.

use std::{fmt, ops::Deref};

/// An immutable-once-cached byte buffer whose start is 8-byte aligned.
///
/// Segment sections are 64-byte aligned in the file and every array inside
/// them is aligned to its element size relative to the section start, so a
/// buffer that starts 8-byte aligned admits zero-copy typed views
/// (`bytemuck::try_cast_slice`) of every `u16`, `u32`, `u64`, `i64`, `f32`,
/// and `f64` array it holds.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct AlignedBytes {
    words: Box<[u64]>,
    len: usize,
}

impl AlignedBytes {
    /// A zero-filled buffer of `len` bytes.
    #[must_use]
    pub fn zeroed(len: usize) -> Self {
        Self {
            words: vec![0_u64; len.div_ceil(8)].into_boxed_slice(),
            len,
        }
    }

    /// A buffer holding a copy of `bytes`.
    #[must_use]
    pub fn copy_from(bytes: &[u8]) -> Self {
        let mut out = Self::zeroed(bytes.len());
        out.as_bytes_mut().copy_from_slice(bytes);
        out
    }

    /// The bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        let all: &[u8] = bytemuck::cast_slice(&self.words);
        &all[..self.len]
    }

    /// The bytes, mutably; loaders fill a buffer through this before it is
    /// shared.
    #[must_use]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        let all: &mut [u8] = bytemuck::cast_slice_mut(&mut self.words);
        &mut all[..self.len]
    }

    /// Number of bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bytes allocated for the buffer: `len` rounded up to a multiple of 8.
    #[must_use]
    pub fn allocated(&self) -> u64 {
        self.words.len() as u64 * 8
    }
}

impl Deref for AlignedBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for AlignedBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<&[u8]> for AlignedBytes {
    fn from(bytes: &[u8]) -> Self {
        Self::copy_from(bytes)
    }
}

impl fmt::Debug for AlignedBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlignedBytes")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::AlignedBytes;

    #[test]
    fn buffers_are_eight_byte_aligned_and_keep_their_length() {
        for len in [0_usize, 1, 7, 8, 9, 4096, 8193] {
            let bytes = AlignedBytes::zeroed(len);
            assert_eq!(bytes.len(), len);
            assert_eq!(bytes.as_ptr().align_offset(8), 0, "len {len}");
            assert_eq!(bytes.allocated(), (len as u64).div_ceil(8) * 8);
            assert!(bytes.iter().all(|byte| *byte == 0));
        }
    }

    #[test]
    fn copies_round_trip_and_admit_typed_views() {
        let source: Vec<u8> = (0..24_u8).collect();
        let bytes = AlignedBytes::copy_from(&source);
        assert_eq!(&*bytes, source.as_slice());
        let words: &[u64] = bytemuck::try_cast_slice(&bytes).expect("aligned and sized");
        assert_eq!(words.len(), 3);
        assert_eq!(words[0].to_ne_bytes(), [0, 1, 2, 3, 4, 5, 6, 7]);
    }
}
