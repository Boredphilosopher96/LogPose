//! Exclusive ownership of a storage root.
//!
//! An [`Engine`](crate::Engine) keeps every collection's state resident, so a storage root may be
//! served by exactly one engine at a time, in this process or any other. The engine holds an
//! exclusive lock on `<storage_root>/LOCK` (through [`Vfs::try_lock_exclusive`]) for its whole
//! lifetime. A second engine on the same root, even in the same process, fails to open with
//! [`LogPoseError::StorageRootLocked`]; callers that need several views of one root share one
//! engine (it is cheap to clone).

use crate::durable_fs::read_file;
use logpose_types::{LogPoseError, Result};
use logpose_vfs::{Vfs, VfsLock};
use std::{io, path::Path};

/// File name of the lock file inside a storage root.
pub(crate) const STORAGE_ROOT_LOCK_FILE_NAME: &str = "LOCK";

/// Take the exclusive lock on `root`, which must exist. Dropping the guard releases it.
pub(crate) fn lock_root_exclusively(vfs: &dyn Vfs, root: &Path) -> Result<Box<dyn VfsLock>> {
    let lock_path = root.join(STORAGE_ROOT_LOCK_FILE_NAME);
    match vfs.try_lock_exclusive(&lock_path) {
        Ok(lock) => Ok(lock),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            let holder_pid = read_file(vfs, &lock_path, "failed to read storage root lock")
                .ok()
                .map(|contents| String::from_utf8_lossy(&contents).trim().to_owned())
                .filter(|contents| !contents.is_empty());
            Err(LogPoseError::StorageRootLocked {
                root: root.to_path_buf(),
                lock_file: lock_path,
                holder_pid,
            })
        }
        Err(error) => Err(LogPoseError::failed_precondition(format!(
            "failed to take the exclusive lock on '{}': {error}; storage_root must be on a \
             filesystem that supports advisory file locks (flock), such as a local disk, so \
             that two engines cannot serve '{}' at once; move storage_root to such a \
             filesystem or enable locking on the mount",
            lock_path.display(),
            root.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::unique_temp_dir;
    use logpose_vfs::{FaultVfs, StdVfs};
    use std::fs;

    #[test]
    fn second_lock_attempt_fails_until_the_first_is_released() {
        let root = unique_temp_dir("root-lock-exclusive");

        let first = lock_root_exclusively(&StdVfs, &root).expect("first lock should succeed");
        let error = lock_root_exclusively(&StdVfs, &root)
            .err()
            .expect("second lock should fail while held");
        let pid = std::process::id().to_string();
        assert!(
            matches!(
                &error,
                LogPoseError::StorageRootLocked { root: locked_root, holder_pid, .. }
                    if locked_root == &root && holder_pid.as_deref() == Some(pid.as_str())
            ),
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
    fn a_crashed_process_releases_the_root_for_the_next_one() {
        let fault = FaultVfs::new(0);
        let root = Path::new("/storage");
        crate::durable_fs::create_dir_all_synced(fault.as_ref(), root)
            .expect("root directory should be created");
        let crashed_process = fault.process();
        let lock = lock_root_exclusively(crashed_process.as_ref(), root).expect("first lock");
        assert!(
            lock_root_exclusively(fault.process().as_ref(), root).is_err(),
            "a second process must not lock a held root"
        );

        fault.crash();
        let reclaimed = lock_root_exclusively(fault.process().as_ref(), root)
            .expect("the crash released the root");
        drop(lock);
        assert!(
            lock_root_exclusively(fault.process().as_ref(), root).is_err(),
            "dropping the crashed process's lock must not release the new holder"
        );
        drop(reclaimed);
    }
}
