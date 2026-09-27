//! File helpers shared by the local engine: JSON reads, atomic replacement, and cleanup.

use crate::{
    durable_fs::{create_dir_all_synced, read_file, sync_parent_dir, write_file_synced},
    error::{io_message, json_message},
};
use logpose_types::{LogPoseError, Result};
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
    serde_json::from_slice(&bytes).map_err(json_message)
}

/// Crash points [`atomic_write_with_points`] reports after each of its durable steps.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AtomicWritePoints {
    /// After the temp file is written and synced.
    pub(crate) after_temp_sync: Option<CrashPoint>,
    /// After the temp file is renamed over the destination.
    pub(crate) after_rename: Option<CrashPoint>,
    /// After the destination directory is synced.
    pub(crate) after_dir_sync: Option<CrashPoint>,
}

/// Durably replace `path` with `bytes`: write a temp file, fsync it, rename it into place, and
/// fsync the parent directory so the rename survives power loss.
pub(crate) fn atomic_write(vfs: &dyn Vfs, path: &Path, bytes: Vec<u8>) -> Result<()> {
    atomic_write_with_points(vfs, path, bytes, AtomicWritePoints::default())
        .map_err(|failure| failure.error)
}

/// Why [`atomic_write_with_points`] failed, and whether it got as far as the rename.
#[derive(Debug)]
pub(crate) struct AtomicWriteFailure {
    /// What failed.
    pub(crate) error: LogPoseError,
    /// Whether the rename was attempted, so that the destination may hold either version until
    /// its directory is synced.
    pub(crate) renamed: bool,
}

/// [`atomic_write`] that reports a named crash point after each durable step, and on failure
/// whether the destination may have changed.
pub(crate) fn atomic_write_with_points(
    vfs: &dyn Vfs,
    path: &Path,
    bytes: Vec<u8>,
    points: AtomicWritePoints,
) -> std::result::Result<(), AtomicWriteFailure> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all_synced(vfs, parent).map_err(|error| AtomicWriteFailure {
            error,
            renamed: false,
        })?;
    }
    replace_atomically(vfs, path, bytes, points)
}

/// [`atomic_write`] into a directory that must already exist. For files of a collection that
/// may be dropped concurrently, where recreating the directory would resurrect it on disk.
pub(crate) fn atomic_write_in_existing_dir(
    vfs: &dyn Vfs,
    path: &Path,
    bytes: Vec<u8>,
) -> Result<()> {
    replace_atomically(vfs, path, bytes, AtomicWritePoints::default())
        .map_err(|failure| failure.error)
}

fn replace_atomically(
    vfs: &dyn Vfs,
    path: &Path,
    bytes: Vec<u8>,
    points: AtomicWritePoints,
) -> std::result::Result<(), AtomicWriteFailure> {
    let before = |error| AtomicWriteFailure {
        error,
        renamed: false,
    };
    let after = |error| AtomicWriteFailure {
        error,
        renamed: true,
    };
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
        return Err(before(error));
    }
    crash_point(vfs, points.after_temp_sync).map_err(before)?;
    if let Err(error) = vfs.rename(&temp_path, path) {
        cleanup_file(vfs, &temp_path);
        return Err(after(io_message("failed to atomically rename file", error)));
    }
    crash_point(vfs, points.after_rename).map_err(after)?;
    sync_parent_dir(vfs, path).map_err(after)?;
    crash_point(vfs, points.after_dir_sync).map_err(after)
}

/// Report `point` if there is one. A crash there halts the operation with an error.
pub(crate) fn crash_point(vfs: &dyn Vfs, point: Option<CrashPoint>) -> Result<()> {
    match point {
        Some(point) => vfs.crash_point(point).map_err(|error| {
            LogPoseError::Message(format!("interrupted at crash point {point:?}: {error}"))
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
