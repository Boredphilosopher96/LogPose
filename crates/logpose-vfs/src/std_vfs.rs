//! [`StdVfs`]: the real filesystem.

use crate::{CrashPoint, DirEntry, OpenMode, Vfs, VfsFile, VfsLock};
use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Write},
    os::unix::fs::FileExt,
    path::Path,
    sync::{Arc, OnceLock},
};

/// The real filesystem, through `std::fs`.
///
/// - `open` maps to [`OpenOptions`]; [`OpenMode::CreateNew`] uses `create_new(true)`.
/// - `read_at` is a safe positioned read ([`FileExt::read_at`]).
/// - `append` writes to a file opened with `append(true)`.
/// - `sync_dir` opens the directory read-only and calls `sync_all`.
/// - `try_lock_exclusive` uses [`File::try_lock`] and records the holder's pid in the file.
/// - `crash_point` does nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct StdVfs;

/// The process-wide shared [`StdVfs`] handle.
///
/// Engines opened through convenience constructors share this handle, so in-process registries
/// keyed by the `Vfs` identity treat them as one filesystem.
#[must_use]
pub fn std_vfs() -> Arc<dyn Vfs> {
    static SHARED: OnceLock<Arc<StdVfs>> = OnceLock::new();
    let shared: Arc<StdVfs> = SHARED.get_or_init(|| Arc::new(StdVfs)).clone();
    shared
}

impl Vfs for StdVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        let mut options = OpenOptions::new();
        options.read(true);
        match mode {
            OpenMode::Read => {}
            OpenMode::CreateNew => {
                options.append(true).create_new(true);
            }
            OpenMode::Append => {
                options.append(true);
            }
        }
        let file = options.open(path)?;
        Ok(Arc::new(StdFile { file }))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        let mut entries = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let len = if file_type.is_dir() {
                0
            } else {
                entry.metadata()?.len()
            };
            entries.push(DirEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                is_dir: file_type.is_dir(),
                len,
            });
        }
        Ok(entries)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    fn try_lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("'{}' is locked by another holder", path.display()),
                ));
            }
            Err(TryLockError::Error(error)) => return Err(error),
        }
        // The pid is diagnostic only; the OS lock is the source of truth, so failing to record it
        // is not fatal.
        let _ = file
            .set_len(0)
            .and_then(|()| file.write_all(format!("{}\n", std::process::id()).as_bytes()));
        Ok(Box::new(StdLock { _file: file }))
    }

    fn crash_point(&self, _point: CrashPoint) -> io::Result<()> {
        Ok(())
    }
}

struct StdFile {
    file: File,
}

impl VfsFile for StdFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let mut filled = 0usize;
        while filled < buf.len() {
            let position = offset
                .checked_add(filled as u64)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;
            match self.file.read_at(&mut buf[filled..], position) {
                Ok(0) => break,
                Ok(read) => filled += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(filled)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, offset)
    }

    fn append(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<u64> {
        let mut writer = &self.file;
        match bufs {
            [] => {}
            [single] => writer.write_all(single)?,
            many => {
                let joined = many
                    .iter()
                    .flat_map(|slice| slice.iter().copied())
                    .collect::<Vec<u8>>();
                writer.write_all(&joined)?;
            }
        }
        self.len()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}

struct StdLock {
    /// Held open for its OS lock; dropping it releases the lock.
    _file: File,
}

impl VfsLock for StdLock {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{exists, read_file};
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn unique_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("temp dir should be created");
        dir
    }

    #[test]
    fn create_append_read_truncate_round_trip() {
        let dir = unique_dir("logpose-std-vfs-file");
        let vfs = StdVfs;
        let path = dir.join("file");

        let file = vfs
            .open(&path, OpenMode::CreateNew)
            .expect("file should be created");
        let len = file
            .append(&[io::IoSlice::new(b"hello "), io::IoSlice::new(b"world")])
            .expect("append should succeed");
        assert_eq!(len, 11);
        file.sync_all().expect("sync should succeed");
        assert!(
            vfs.open(&path, OpenMode::CreateNew).is_err(),
            "create_new must refuse an existing file"
        );

        let mut buf = [0u8; 5];
        file.read_exact_at(&mut buf, 6)
            .expect("read should succeed");
        assert_eq!(&buf, b"world");
        let mut long = [0u8; 32];
        assert_eq!(file.read_at(&mut long, 6).expect("short read"), 5);

        file.set_len(5).expect("truncate should succeed");
        let reopened = vfs
            .open(&path, OpenMode::Append)
            .expect("append open should succeed");
        reopened
            .append(&[io::IoSlice::new(b"!")])
            .expect("append after truncate should succeed");
        assert_eq!(read_file(&vfs, &path).expect("read file"), b"hello!");

        let entries = vfs.list(&dir).expect("list should succeed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "file");
        assert_eq!(entries[0].len, 6);
        assert!(exists(&vfs, &path).expect("exists"));
        assert!(!exists(&vfs, &dir.join("missing")).expect("exists"));
        assert!(!exists(&vfs, &dir.join("missing").join("child")).expect("exists"));
        vfs.sync_dir(&dir).expect("dir sync should succeed");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn lock_is_exclusive_until_dropped() {
        let dir = unique_dir("logpose-std-vfs-lock");
        let vfs = StdVfs;
        let path = dir.join("LOCK");

        let first = vfs.try_lock_exclusive(&path).expect("first lock");
        let error = vfs
            .try_lock_exclusive(&path)
            .err()
            .expect("second lock should fail while held");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(
            fs::read_to_string(&path).expect("lock file should be readable"),
            format!("{}\n", std::process::id())
        );
        drop(first);
        drop(vfs.try_lock_exclusive(&path).expect("lock after release"));

        let _ = fs::remove_dir_all(dir);
    }
}
