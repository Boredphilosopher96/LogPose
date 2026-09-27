//! Byte-range sources that a [`SegmentReader`](super::SegmentReader) reads
//! from.
//!
//! The trait mirrors the positioned-read half of the engine's planned
//! `VfsFile` (`len` and `read_exact_at`), so a Vfs-backed source is a
//! one-line forwarding impl. The reader checks every range against the
//! validated file length before it allocates a buffer, so a source never
//! sees a request that the file cannot satisfy.

use logpose_vfs::VfsFile;
use std::{fmt, fs::File, io, path::Path, sync::Arc};

/// Positioned, read-only access to an immutable segment file.
///
/// Implementations must be usable from many threads.
pub trait SectionSource: Send + Sync {
    /// Total length of the file in bytes.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error.
    fn len(&self) -> io::Result<u64>;

    /// Whether the file is empty.
    ///
    /// # Errors
    ///
    /// As [`len`](Self::len).
    fn is_empty(&self) -> io::Result<bool> {
        self.len().map(|len| len == 0)
    }

    /// Fill `buf` with the bytes at `offset`, or fail with
    /// [`io::ErrorKind::UnexpectedEof`].
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
}

impl<S: SectionSource + ?Sized> SectionSource for Arc<S> {
    fn len(&self) -> io::Result<u64> {
        (**self).len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }
}

impl<S: SectionSource + ?Sized> SectionSource for &S {
    fn len(&self) -> io::Result<u64> {
        (**self).len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }
}

/// A segment held in memory.
#[derive(Clone, Debug)]
pub struct MemorySource {
    bytes: Arc<[u8]>,
}

impl MemorySource {
    /// Wrap bytes.
    #[must_use]
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self {
            bytes: bytes.into(),
        }
    }

    /// The whole file.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl SectionSource for MemorySource {
    fn len(&self) -> io::Result<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let range = usize::try_from(offset)
            .ok()
            .and_then(|start| Some(start..start.checked_add(buf.len())?))
            .filter(|range| range.end <= self.bytes.len())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "read past the end of the segment",
                )
            })?;
        buf.copy_from_slice(&self.bytes[range]);
        Ok(())
    }
}

/// A segment file read with positioned reads (`pread`).
///
/// The length is captured at open: segment files are immutable once
/// written.
#[derive(Debug)]
pub struct FileSource {
    file: File,
    len: u64,
}

impl FileSource {
    /// Open a segment file read-only.
    ///
    /// # Errors
    ///
    /// Returns the I/O error from opening the file or reading its metadata.
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::from_file(File::open(path)?)
    }

    /// Wrap an open file.
    ///
    /// # Errors
    ///
    /// Returns the I/O error from reading the file's metadata.
    pub fn from_file(file: File) -> io::Result<Self> {
        let len = file.metadata()?.len();
        Ok(Self { file, len })
    }
}

impl SectionSource for FileSource {
    fn len(&self) -> io::Result<u64> {
        Ok(self.len)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, offset)
    }
}

/// A segment file opened through the engine's [`Vfs`](logpose_vfs::Vfs).
/// Cheap to clone, so async fetches can move it into a loader.
#[derive(Clone)]
pub struct VfsSource(pub Arc<dyn VfsFile>);

impl SectionSource for VfsSource {
    fn len(&self) -> io::Result<u64> {
        self.0.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }
}

impl fmt::Debug for VfsSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("VfsSource").finish_non_exhaustive()
    }
}
