//! File helpers shared by the local engine: JSON reads, atomic replacement, and cleanup.

use crate::{
    durable_fs::{create_dir_all_synced, read_file, sync_parent_dir, write_file_synced},
    error::{io_message, json_corrupt},
};
use logpose_types::{CorruptionKind, LogPoseError, Result};
use logpose_vfs::{CrashPoint, Vfs};
use serde::Deserialize;
use std::{
    path::Path,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

pub(crate) fn read_json<T>(vfs: &dyn Vfs, path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = read_file(vfs, path, "failed to read JSON file")?;
    serde_json::from_slice(&bytes)
        .map_err(|error| json_corrupt(CorruptionKind::Descriptor, path, &error))
}

/// Durably replace `path` with `bytes`: write a temp file, fsync it, rename it into place, and
/// fsync the parent directory so the rename survives power loss.
pub(crate) fn atomic_write(vfs: &dyn Vfs, path: &Path, bytes: Vec<u8>) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all_synced(vfs, parent)?;
    }
    replace_atomically(vfs, path, bytes)
}

fn replace_atomically(vfs: &dyn Vfs, path: &Path, bytes: Vec<u8>) -> Result<()> {
    static ATOMIC_WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);
    let temp_path = path.with_file_name(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".to_owned()),
        std::process::id(),
        ATOMIC_WRITE_COUNTER.fetch_add(1, AtomicOrdering::Relaxed),
    ));
    if let Err(error) = write_file_synced(vfs, &temp_path, &bytes) {
        cleanup_file(vfs, &temp_path);
        return Err(error);
    }
    if let Err(error) = vfs.rename(&temp_path, path) {
        cleanup_file(vfs, &temp_path);
        return Err(io_message("failed to atomically rename file", error));
    }
    sync_parent_dir(vfs, path)
}

/// Report `point` if there is one. A crash there halts the operation with an error.
pub(crate) fn crash_point(vfs: &dyn Vfs, point: Option<CrashPoint>) -> Result<()> {
    match point {
        Some(point) => vfs.crash_point(point).map_err(|error| {
            LogPoseError::io(format!("interrupted at crash point {point:?}"), error)
        }),
        None => Ok(()),
    }
}

pub(crate) fn cleanup_file(vfs: &dyn Vfs, path: &Path) {
    let _ = vfs.remove_file(path);
}

pub(crate) fn cleanup_dir(vfs: &dyn Vfs, path: &Path) {
    let _ = vfs.remove_dir_all(path);
}
