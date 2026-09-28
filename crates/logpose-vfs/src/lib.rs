//! Filesystem abstraction used by every engine crate.
//!
//! All engine I/O (WAL, manifests, `CURRENT`, segments, sidecars, descriptors, catalog files and
//! the storage-root lock file) goes through [`Vfs`], so every durability claim can be tested by
//! injecting crashes and I/O errors:
//!
//! - [`StdVfs`] is the real filesystem.
//! - [`FaultVfs`] is an in-memory filesystem that models what a crash can do to a POSIX
//!   filesystem: unsynced data is lost or torn, directory changes are volatile until the
//!   directory is synced, and fsync can fail. It is deterministic given a seed.
//!
//! The trait is deliberately narrow: no seek, no in-place overwrite and no mmap. Immutable files
//! are written once with [`OpenMode::CreateNew`] plus [`VfsFile::append`]; the only mutable file
//! is the WAL tail, which may also be truncated with [`VfsFile::set_len`].

mod fault;
mod std_vfs;

pub use fault::{CrashReport, FaultPlan, FaultVfs, TearMode, TornFile};
pub use std_vfs::{StdVfs, std_vfs};

use std::{fmt, io, path::Path, sync::Arc};

/// Filesystem used by the engine.
///
/// Implementations must be usable from many threads. Every method is blocking.
pub trait Vfs: Send + Sync + 'static {
    /// Open or create a file.
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>>;
    /// Create a directory and its parents. Not durable until `sync_dir` of each parent.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// List entry names (not paths) in a directory, unsorted.
    fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>>;
    /// Atomically replace `to` with `from`. Not durable until `sync_dir(parent)`.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Remove a file. Not durable until `sync_dir(parent)`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    /// Remove a directory tree. Not durable until `sync_dir(parent)`.
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;
    /// Make the directory's entry set (creates, renames, removes) durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// Take an exclusive advisory lock on `path`, failing fast with
    /// [`io::ErrorKind::WouldBlock`] if another holder exists. The lock is released when the
    /// returned guard is dropped.
    fn try_lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn VfsLock>>;
    /// Named crash point. [`StdVfs`] returns `Ok(())`; [`FaultVfs`] may return a
    /// [crashed](is_crashed) error. Engine code calls this at every documented step of its
    /// crash analyses.
    fn crash_point(&self, point: CrashPoint) -> io::Result<()>;
}

/// How [`Vfs::open`] opens a file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenMode {
    /// Read-only; the file must exist.
    Read,
    /// Create a new file for reading and appending; fail with
    /// [`io::ErrorKind::AlreadyExists`] if it exists. Used for every immutable file.
    CreateNew,
    /// Open an existing file for reading and appending (the WAL tail).
    Append,
}

/// One entry returned by [`Vfs::list`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirEntry {
    /// Entry name inside the listed directory.
    pub name: String,
    /// Whether the entry is a directory.
    pub is_dir: bool,
    /// Current length in bytes for files, including unsynced appends; 0 for directories.
    pub len: u64,
}

/// An open file.
pub trait VfsFile: Send + Sync {
    /// Positioned read (`pread`). Returns bytes read; short only at EOF.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    /// Read exactly `buf.len()` bytes or fail with [`io::ErrorKind::UnexpectedEof`].
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    /// Append all slices at the current end of file; returns the new length.
    ///
    /// A single call is one "write" for the torn-write fault model.
    fn append(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<u64>;
    /// `fdatasync`.
    fn sync_data(&self) -> io::Result<()>;
    /// `fsync`.
    fn sync_all(&self) -> io::Result<()>;
    /// Current length, including unsynced appends.
    fn len(&self) -> io::Result<u64>;
    /// Whether the file is currently empty.
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
    /// Truncate. Used only by WAL tail repair, failed-append rollback and WAL truncation.
    fn set_len(&self, len: u64) -> io::Result<()>;
}

/// Guard for a lock taken with [`Vfs::try_lock_exclusive`]; dropping it releases the lock.
pub trait VfsLock: Send + Sync {}

/// Named steps of the engine's crash analyses.
///
/// Every variant that the engine implements appears exactly once in engine code, so a test can
/// crash at a documented step with [`FaultPlan::crash_at`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum CrashPoint {
    /// After a WAL frame is appended, before it is synced.
    WalAfterAppend,
    /// After a WAL frame is synced, before the write is acknowledged.
    WalAfterSync,
    /// After a failed WAL append was rolled back to the last synced frame.
    WalAfterRollback,
    /// After WAL rotation created the new active file and synced its directory.
    WalAfterRotateCreate,
    /// After a flush wrote and synced its segment file.
    FlushAfterSegmentSync,
    /// After a flush wrote and synced its deletion-vector files.
    FlushAfterDvSync,
    /// After a flush synced the segment directories.
    FlushAfterSegmentsDirSync,
    /// After a manifest file was written and synced.
    ManifestAfterFileSync,
    /// After the manifest directory was synced.
    ManifestAfterDirSync,
    /// After the `CURRENT` temp file was written and synced.
    CurrentAfterTempSync,
    /// After the `CURRENT` temp file was renamed over `CURRENT`.
    CurrentAfterRename,
    /// After the directory holding `CURRENT` was synced.
    CurrentAfterDirSync,
    /// After a compaction wrote and synced its output segment and directories.
    CompactionAfterOutputSync,
    /// After a compaction wrote and synced its output deletion-vector file.
    CompactionAfterDvSync,
    /// After an index build wrote and synced its index sidecar.
    IndexAfterSidecarSync,
    /// After an index build synced the segment directory holding its sidecar.
    IndexAfterSegmentsDirSync,
    /// After garbage collection removed a file.
    GcAfterRemove,
    /// After recovery repaired a torn WAL tail.
    RecoveryAfterTailRepair,
    /// After recovery removed orphaned files.
    RecoveryAfterOrphanCleanup,
}

impl CrashPoint {
    /// Every crash point, in declaration order.
    pub const ALL: [CrashPoint; 19] = [
        CrashPoint::WalAfterAppend,
        CrashPoint::WalAfterSync,
        CrashPoint::WalAfterRollback,
        CrashPoint::WalAfterRotateCreate,
        CrashPoint::FlushAfterSegmentSync,
        CrashPoint::FlushAfterDvSync,
        CrashPoint::FlushAfterSegmentsDirSync,
        CrashPoint::ManifestAfterFileSync,
        CrashPoint::ManifestAfterDirSync,
        CrashPoint::CurrentAfterTempSync,
        CrashPoint::CurrentAfterRename,
        CrashPoint::CurrentAfterDirSync,
        CrashPoint::CompactionAfterOutputSync,
        CrashPoint::CompactionAfterDvSync,
        CrashPoint::IndexAfterSidecarSync,
        CrashPoint::IndexAfterSegmentsDirSync,
        CrashPoint::GcAfterRemove,
        CrashPoint::RecoveryAfterTailRepair,
        CrashPoint::RecoveryAfterOrphanCleanup,
    ];
}

/// Payload of the error every [`FaultVfs`] operation returns once a crash has triggered.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Crashed;

impl fmt::Display for Crashed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("simulated crash: the process is halted")
    }
}

impl std::error::Error for Crashed {}

/// Build the error returned by every operation after a simulated crash.
#[must_use]
pub fn crashed_error() -> io::Error {
    io::Error::other(Crashed)
}

/// Whether `error` reports a simulated crash.
#[must_use]
pub fn is_crashed(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.downcast_ref::<Crashed>().is_some())
}

/// Read a whole file.
pub fn read_file(vfs: &dyn Vfs, path: &Path) -> io::Result<Vec<u8>> {
    let file = vfs.open(path, OpenMode::Read)?;
    let len = usize::try_from(file.len()?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file is too large to read"))?;
    let mut bytes = vec![0u8; len];
    file.read_exact_at(&mut bytes, 0)?;
    Ok(bytes)
}

/// Whether `path` names an existing file or directory.
///
/// Implemented by listing the parent directory, so it needs no extra trait method. A missing
/// parent means the path does not exist.
pub fn exists(vfs: &dyn Vfs, path: &Path) -> io::Result<bool> {
    let Some(name) = path.file_name() else {
        // `/` or a path ending in `..`: fall back to listing the path itself.
        return match vfs.list(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        };
    };
    let name = name.to_string_lossy();
    match vfs.list(parent_dir(path)) {
        Ok(entries) => Ok(entries.iter().any(|entry| entry.name == name)),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

/// The directory that contains `path`; `.` for a bare file name.
#[must_use]
pub fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}
