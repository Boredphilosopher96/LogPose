//! File helpers shared by the local engine: JSON reads, atomic replacement, and cleanup.

use crate::{
    durable_fs::{create_dir_all_synced, sync_parent_dir, write_file_synced},
    error::{io_message, json_message},
};
use logpose_types::Result;
use serde::Deserialize;
use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

pub(crate) fn read_json<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = fs::read(path).map_err(|error| io_message("failed to read JSON file", error))?;
    serde_json::from_slice(&bytes).map_err(json_message)
}

/// Durably replace `path` with `bytes`: write a temp file, fsync it, rename it into place, and
/// fsync the parent directory so the rename survives power loss.
pub(crate) fn atomic_write(path: &Path, bytes: Vec<u8>) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all_synced(parent)?;
    }
    static ATOMIC_WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);
    let temp_path = path.with_file_name(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".to_owned()),
        std::process::id(),
        ATOMIC_WRITE_COUNTER.fetch_add(1, AtomicOrdering::Relaxed),
    ));
    if let Err(error) = write_file_synced(&temp_path, &bytes) {
        cleanup_file(&temp_path);
        return Err(error);
    }
    if let Err(error) = fs::rename(&temp_path, path) {
        cleanup_file(&temp_path);
        return Err(io_message("failed to atomically rename file", error));
    }
    sync_parent_dir(path)
}

pub(crate) fn cleanup_file(path: &Path) {
    let _ = fs::remove_file(path);
}

pub(crate) fn cleanup_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

pub(crate) fn remove_file_if_exists(path: &Path, context: &str) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_message(context, error)),
    }
}
