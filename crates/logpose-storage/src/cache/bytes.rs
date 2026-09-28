//! [`AlignedBytes`]: the 8-byte-aligned buffers the cache holds.

use std::{
    any::Any,
    fmt,
    ops::Deref,
    sync::{Arc, OnceLock},
};

/// An immutable-once-cached byte buffer whose start is 8-byte aligned.
///
/// Segment sections are 64-byte aligned in the file and every array inside
/// them is aligned to its element size relative to the section start, so a
/// buffer that starts 8-byte aligned admits zero-copy typed views
/// (`bytemuck::try_cast_slice`) of every `u16`, `u32`, `u64`, `i64`, `f32`,
/// and `f64` array it holds.
///
/// A buffer can carry one decoded form of itself (a graph, a scalar index,
/// parsed SQ8 params), attached once with [`AlignedBytes::attach`] or
/// [`AlignedBytes::decoded`]. It lives exactly as long as the buffer, so a
/// cached unit decodes once per cache load instead of once per access. The
/// cache charges an attachment's heap size when it is attached before the
/// insert (loaders do that).
#[derive(Clone, Default)]
pub struct AlignedBytes {
    words: Box<[u64]>,
    len: usize,
    decoded: OnceLock<Decoded>,
}

/// A decoded form of a buffer and the heap bytes it holds.
#[derive(Clone)]
struct Decoded {
    value: Arc<dyn Any + Send + Sync>,
    heap_bytes: u64,
}

impl PartialEq for AlignedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for AlignedBytes {}

impl AlignedBytes {
    /// A zero-filled buffer of `len` bytes.
    #[must_use]
    pub fn zeroed(len: usize) -> Self {
        Self {
            words: vec![0_u64; len.div_ceil(8)].into_boxed_slice(),
            len,
            decoded: OnceLock::new(),
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

    /// Attach `value`, a decoded form of these bytes holding `heap_bytes` of
    /// heap memory. Returns `false` (and drops `value`) if a decoded form is
    /// already attached.
    pub fn attach<T: Any + Send + Sync>(&self, value: T, heap_bytes: u64) -> bool {
        self.decoded
            .set(Decoded {
                value: Arc::new(value),
                heap_bytes,
            })
            .is_ok()
    }

    /// The attached decoded form of type `T`, if one is attached.
    #[must_use]
    pub fn attached<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        let decoded = self.decoded.get()?;
        Arc::clone(&decoded.value).downcast::<T>().ok()
    }

    /// The decoded form of type `T`: the attached one, or `decode(bytes)`,
    /// which is attached for later callers (the first attach wins a race).
    /// A buffer that carries a decoded form of another type decodes on every
    /// call.
    ///
    /// # Errors
    ///
    /// `decode`'s error; nothing is attached then.
    pub fn decoded<T, E>(
        &self,
        decode: impl FnOnce(&[u8]) -> Result<(T, u64), E>,
    ) -> Result<Arc<T>, E>
    where
        T: Any + Send + Sync,
    {
        if let Some(value) = self.attached::<T>() {
            return Ok(value);
        }
        let (value, heap_bytes) = decode(self.as_bytes())?;
        let value = Arc::new(value);
        if self.decoded.get().is_none() {
            let _ = self.decoded.set(Decoded {
                value: Arc::clone(&value) as Arc<dyn Any + Send + Sync>,
                heap_bytes,
            });
        }
        Ok(value)
    }

    /// Heap bytes of the attached decoded form; 0 without one.
    #[must_use]
    pub fn decoded_heap_bytes(&self) -> u64 {
        self.decoded.get().map_or(0, |decoded| decoded.heap_bytes)
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
            .field("decoded", &self.decoded.get().is_some())
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

    #[test]
    fn decoded_forms_are_attached_once_and_typed() {
        let bytes = AlignedBytes::copy_from(&[1, 2, 3]);
        assert_eq!(bytes.decoded_heap_bytes(), 0);
        let mut calls = 0;
        let mut decode = |raw: &[u8]| -> Result<(usize, u64), ()> {
            calls += 1;
            Ok((raw.len(), 16))
        };
        assert_eq!(*bytes.decoded(&mut decode).expect("decodes"), 3);
        assert_eq!(*bytes.decoded(&mut decode).expect("decodes"), 3);
        assert_eq!(calls, 1);
        assert_eq!(bytes.decoded_heap_bytes(), 16);
        assert!(bytes.attached::<String>().is_none());
        assert!(!bytes.attach(7_usize, 8));
        let failed: Result<std::sync::Arc<u8>, &str> = bytes.decoded(|_| Err("bad"));
        assert!(failed.is_err());
        assert_eq!(bytes, AlignedBytes::copy_from(&[1, 2, 3]));
    }
}
