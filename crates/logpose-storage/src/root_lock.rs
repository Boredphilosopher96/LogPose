//! Exclusive ownership of a storage root across processes.
//!
//! A storage root may be served by exactly one process at a time. The owning process holds an
//! exclusive OS file lock on `<storage_root>/LOCK` for as long as any engine in that process has
//! the root open.
//!
//! Inside one process, several engine handles legitimately share a root: server bootstrap opens
//! one engine for the data plane and one for the catalog, `EtcdBackedStorageEngine` wraps a local
//! engine, and tests reopen a root while an older handle is alive. Those handles already
//! coordinate through the process-global per-path locks in this crate, so they share a single
//! OS lock through a reference-counted registry instead of excluding each other. The OS lock is
//! released when the last handle for the root is dropped.

use crate::durable_fs::create_dir_all_synced;
use logpose_types::{LogPoseError, Result};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions, TryLockError},
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, OnceLock, PoisonError},
};

/// File name of the lock file inside a storage root.
pub(crate) const STORAGE_ROOT_LOCK_FILE_NAME: &str = "LOCK";

struct RegistryEntry {
    /// Held open for its OS lock; dropping it releases the lock.
    _file: File,
    holders: usize,
}

fn registry() -> MutexGuard<'static, BTreeMap<PathBuf, RegistryEntry>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<PathBuf, RegistryEntry>>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// One process-local claim on a storage root's exclusive lock.
///
/// The OS lock is held while at least one claim for the root is alive.
#[derive(Debug)]
pub(crate) struct StorageRootLock {
    key: PathBuf,
}

impl StorageRootLock {
    /// Claim `root` for this process, creating the directory if needed.
    ///
    /// Fails with a descriptive error if another process holds the root.
    pub(crate) fn acquire(root: &Path) -> Result<Self> {
        create_dir_all_synced(root)?;
        let key = fs::canonicalize(root).map_err(|error| {
            LogPoseError::Message(format!(
                "failed to resolve storage root '{}': {error}",
                root.display()
            ))
        })?;

        let mut registry = registry();
        if let Some(entry) = registry.get_mut(&key) {
            entry.holders += 1;
            return Ok(Self { key });
        }

        let file = lock_file_exclusively(&key)?;
        registry.insert(
            key.clone(),
            RegistryEntry {
                _file: file,
                holders: 1,
            },
        );
        Ok(Self { key })
    }
}

impl Drop for StorageRootLock {
    fn drop(&mut self) {
        let mut registry = registry();
        let release = match registry.get_mut(&self.key) {
            Some(entry) => {
                entry.holders = entry.holders.saturating_sub(1);
                entry.holders == 0
            }
            None => false,
        };
        if release {
            // Dropping the file closes it and releases the OS lock while the registry mutex is
            // still held, so a concurrent `acquire` never observes a half-released root.
            registry.remove(&self.key);
        }
    }
}

fn lock_file_exclusively(root: &Path) -> Result<File> {
    let lock_path = root.join(STORAGE_ROOT_LOCK_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| {
            LogPoseError::Message(format!(
                "failed to open storage root lock '{}': {error}",
                lock_path.display()
            ))
        })?;

    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            let holder = fs::read_to_string(&lock_path)
                .ok()
                .map(|contents| contents.trim().to_owned())
                .filter(|contents| !contents.is_empty())
                .map(|pid| format!(" (held by pid {pid})"))
                .unwrap_or_default();
            return Err(LogPoseError::Message(format!(
                "storage root '{}' is already in use by another process{holder}; \
                 lock file '{}' is held exclusively",
                root.display(),
                lock_path.display()
            )));
        }
        Err(TryLockError::Error(error)) => {
            return Err(LogPoseError::Message(format!(
                "failed to take the exclusive lock on '{}': {error}; storage_root must be on a \
                 filesystem that supports advisory file locks (flock), such as a local disk, so \
                 that two processes cannot serve '{}' at once; move storage_root to such a \
                 filesystem or enable locking on the mount",
                lock_path.display(),
                root.display()
            )));
        }
    }

    // The pid is diagnostic only; the OS lock is the source of truth, so failures to record it
    // are not fatal.
    let _ = file
        .set_len(0)
        .and_then(|_| file.write_all(format!("{}\n", std::process::id()).as_bytes()));
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ))
    }

    /// Open the lock file through an independent handle, which the OS treats exactly like a
    /// lock attempt from another process.
    fn foreign_handle(root: &Path) -> File {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(root.join(STORAGE_ROOT_LOCK_FILE_NAME))
            .expect("lock file should open")
    }

    #[test]
    fn second_lock_attempt_fails_until_the_first_is_released() {
        let root = unique_root("logpose-root-lock-exclusive");
        fs::create_dir_all(&root).expect("root should be created");

        let first = lock_file_exclusively(&root).expect("first lock should succeed");
        let error = lock_file_exclusively(&root).expect_err("second lock should fail while held");
        assert!(
            error
                .to_string()
                .contains("already in use by another process"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .to_string()
                .contains(&format!("held by pid {}", std::process::id())),
            "error should name the holder: {error}"
        );

        drop(first);
        let second = lock_file_exclusively(&root).expect("lock should succeed after release");
        drop(second);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn claims_in_one_process_share_the_lock_and_release_it_with_the_last_claim() {
        let root = unique_root("logpose-root-lock-shared");

        let first = StorageRootLock::acquire(&root).expect("first claim should succeed");
        let second = StorageRootLock::acquire(&root).expect("in-process claims should share");

        let foreign = foreign_handle(&root);
        assert!(
            matches!(foreign.try_lock(), Err(TryLockError::WouldBlock)),
            "root should be locked while claims are alive"
        );

        drop(first);
        assert!(
            matches!(foreign.try_lock(), Err(TryLockError::WouldBlock)),
            "root should stay locked while any claim is alive"
        );

        drop(second);
        foreign
            .try_lock()
            .expect("root should be released after the last claim is dropped");
        drop(foreign);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn claim_fails_while_another_holder_owns_the_root() {
        let root = unique_root("logpose-root-lock-foreign");
        fs::create_dir_all(&root).expect("root should be created");

        let foreign = foreign_handle(&root);
        foreign.lock().expect("foreign holder should lock the root");

        let error = StorageRootLock::acquire(&root)
            .expect_err("claim should fail while another holder owns the root");
        assert!(
            error
                .to_string()
                .contains("already in use by another process"),
            "unexpected error: {error}"
        );

        foreign.unlock().expect("foreign holder should unlock");
        drop(foreign);
        let claim = StorageRootLock::acquire(&root).expect("claim should succeed after release");
        drop(claim);
        let _ = fs::remove_dir_all(root);
    }
}
