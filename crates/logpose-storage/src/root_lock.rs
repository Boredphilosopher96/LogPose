//! Exclusive ownership of a storage root across processes.
//!
//! A storage root may be served by exactly one process at a time. The owning process holds an
//! exclusive lock on `<storage_root>/LOCK` (through [`Vfs::try_lock_exclusive`]) for as long as
//! any engine in that process has the root open.
//!
//! Inside one process, several engine handles legitimately share a root: server bootstrap opens
//! one engine for the data plane and one for the catalog, `EtcdBackedStorageEngine` wraps a local
//! engine, and tests reopen a root while an older handle is alive. Those handles already
//! coordinate through the process-global per-path locks in this crate, so they share a single
//! lock through a reference-counted registry instead of excluding each other. The lock is
//! released when the last handle for the root is dropped.
//!
//! The registry is keyed by the identity of the `Vfs` and the absolute root path. Engines opened
//! through the convenience constructors share one `StdVfs` handle, so they share the claim; an
//! engine on a different `Vfs` (for example a `FaultVfs` process in a test) is a different
//! filesystem and never shares it.

use crate::durable_fs::{create_dir_all_synced, read_file};
use logpose_types::{LogPoseError, Result};
use logpose_vfs::{Vfs, VfsLock};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError},
};

/// File name of the lock file inside a storage root.
pub(crate) const STORAGE_ROOT_LOCK_FILE_NAME: &str = "LOCK";

/// Registry key: the address of the `Vfs` (kept alive by the entry) and the absolute root.
type RegistryKey = (usize, PathBuf);

struct RegistryEntry {
    /// Keeps the `Vfs` alive so its address is not reused while the entry exists.
    _vfs: Arc<dyn Vfs>,
    /// Held for its lock; dropping it releases the lock.
    _lock: Box<dyn VfsLock>,
    holders: usize,
}

fn registry() -> MutexGuard<'static, BTreeMap<RegistryKey, RegistryEntry>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<RegistryKey, RegistryEntry>>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// One process-local claim on a storage root's exclusive lock.
///
/// The lock is held while at least one claim for the root is alive.
pub(crate) struct StorageRootLock {
    key: RegistryKey,
}

impl StorageRootLock {
    /// Claim `root` for this process, creating the directory if needed.
    ///
    /// Fails with a descriptive error if another process holds the root.
    pub(crate) fn acquire(vfs: &Arc<dyn Vfs>, root: &Path) -> Result<Self> {
        create_dir_all_synced(vfs.as_ref(), root)?;
        let absolute = std::path::absolute(root).map_err(|error| {
            LogPoseError::Message(format!(
                "failed to resolve storage root '{}': {error}",
                root.display()
            ))
        })?;
        let key = (Arc::as_ptr(vfs).cast::<()>() as usize, absolute);

        let mut registry = registry();
        if let Some(entry) = registry.get_mut(&key) {
            entry.holders += 1;
            return Ok(Self { key });
        }

        let lock = lock_root_exclusively(vfs.as_ref(), &key.1)?;
        registry.insert(
            key.clone(),
            RegistryEntry {
                _vfs: Arc::clone(vfs),
                _lock: lock,
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
            // Dropping the entry releases the lock while the registry mutex is still held, so a
            // concurrent `acquire` never observes a half-released root.
            registry.remove(&self.key);
        }
    }
}

fn lock_root_exclusively(vfs: &dyn Vfs, root: &Path) -> Result<Box<dyn VfsLock>> {
    let lock_path = root.join(STORAGE_ROOT_LOCK_FILE_NAME);
    match vfs.try_lock_exclusive(&lock_path) {
        Ok(lock) => Ok(lock),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            let holder = read_file(vfs, &lock_path, "failed to read storage root lock")
                .ok()
                .map(|contents| String::from_utf8_lossy(&contents).trim().to_owned())
                .filter(|contents| !contents.is_empty())
                .map(|pid| format!(" (held by pid {pid})"))
                .unwrap_or_default();
            Err(LogPoseError::Message(format!(
                "storage root '{}' is already in use by another process{holder}; \
                 lock file '{}' is held exclusively",
                root.display(),
                lock_path.display()
            )))
        }
        Err(error) => Err(LogPoseError::Message(format!(
            "failed to take the exclusive lock on '{}': {error}; storage_root must be on a \
             filesystem that supports advisory file locks (flock), such as a local disk, so \
             that two processes cannot serve '{}' at once; move storage_root to such a \
             filesystem or enable locking on the mount",
            lock_path.display(),
            root.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_vfs::{FaultVfs, StdVfs, std_vfs};
    use std::{
        fs::{self, File, OpenOptions, TryLockError},
        time::{SystemTime, UNIX_EPOCH},
    };

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

        let first = lock_root_exclusively(&StdVfs, &root).expect("first lock should succeed");
        let error = lock_root_exclusively(&StdVfs, &root)
            .err()
            .expect("second lock should fail while held");
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
        let second =
            lock_root_exclusively(&StdVfs, &root).expect("lock should succeed after release");
        drop(second);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn claims_in_one_process_share_the_lock_and_release_it_with_the_last_claim() {
        let root = unique_root("logpose-root-lock-shared");
        let vfs = std_vfs();

        let first = StorageRootLock::acquire(&vfs, &root).expect("first claim should succeed");
        let second = StorageRootLock::acquire(&vfs, &root).expect("in-process claims should share");

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
        let vfs = std_vfs();

        let foreign = foreign_handle(&root);
        foreign.lock().expect("foreign holder should lock the root");

        let error = StorageRootLock::acquire(&vfs, &root)
            .err()
            .expect("claim should fail while another holder owns the root");
        assert!(
            error
                .to_string()
                .contains("already in use by another process"),
            "unexpected error: {error}"
        );

        foreign.unlock().expect("foreign holder should unlock");
        drop(foreign);
        let claim =
            StorageRootLock::acquire(&vfs, &root).expect("claim should succeed after release");
        drop(claim);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_crashed_process_releases_the_root_for_the_next_one() {
        let fault = FaultVfs::new(0);
        let root = Path::new("/storage");
        let crashed_process = fault.process();
        let claim = StorageRootLock::acquire(&crashed_process, root).expect("first claim");
        let other_process = fault.process();
        assert!(
            StorageRootLock::acquire(&other_process, root).is_err(),
            "a second process must not claim a held root"
        );

        fault.crash();
        let rebooted = fault.process();
        let reclaimed =
            StorageRootLock::acquire(&rebooted, root).expect("the crash released the root");
        drop(claim);
        assert!(
            StorageRootLock::acquire(&fault.process(), root).is_err(),
            "dropping the crashed process's claim must not release the new holder"
        );
        drop(reclaimed);
    }
}
