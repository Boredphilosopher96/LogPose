//! Test-only fault injection for failures that cannot be provoked through the filesystem when
//! tests run with elevated privileges.

use logpose_types::{LogPoseError, Result};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock, PoisonError},
};

fn failing_marker_removals() -> &'static Mutex<BTreeSet<PathBuf>> {
    static PATHS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();
    PATHS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

/// Make every removal of `marker_path` fail until [`clear_marker_removal_failure`] runs.
pub(crate) fn fail_marker_removal(marker_path: &Path) {
    failing_marker_removals()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(marker_path.to_path_buf());
}

pub(crate) fn clear_marker_removal_failure(marker_path: &Path) {
    failing_marker_removals()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(marker_path);
}

pub(crate) fn check_marker_removal(marker_path: &Path) -> Result<()> {
    if failing_marker_removals()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(marker_path)
    {
        return Err(LogPoseError::internal(format!(
            "failed to clear pending WAL rotation marker '{}': injected failure",
            marker_path.display()
        )));
    }
    Ok(())
}
