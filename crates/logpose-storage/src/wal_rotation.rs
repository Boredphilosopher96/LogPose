//! WAL rotation protocol shared by writes, flush and recovery: the per-collection rotation lock and the `PENDING_ROTATION` marker.

#[cfg(test)]
use crate::failpoints;
use crate::{LocalStorageEngine, durable_fs::sync_parent_dir, fs_util::remove_file_if_exists};
use logpose_catalog::CollectionDescriptor;
use logpose_types::Result;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

impl LocalStorageEngine {
    /// Remove the pending-rotation marker and make the removal durable.
    pub(crate) fn clear_pending_rotation_marker(descriptor: &CollectionDescriptor) -> Result<()> {
        let marker_path = Self::pending_rotation_file_path(descriptor);
        #[cfg(test)]
        failpoints::check_marker_removal(&marker_path)?;
        remove_file_if_exists(&marker_path, "failed to clear pending WAL rotation marker")?;
        sync_parent_dir(&marker_path)
    }
}

fn wal_rotation_locks() -> &'static Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub(crate) fn wal_rotation_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = wal_rotation_locks()
        .lock()
        .expect("wal rotation lock map should not be poisoned");
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}
