//! Crash-durable filesystem helpers.
//!
//! A file's contents are durable after `fsync` on the file, but its name is
//! only durable once the directory that holds the entry is fsynced too. Every
//! create, rename and removal that recovery depends on must therefore be
//! followed by a sync of the parent directory.

use logpose_types::{LogPoseError, Result};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

/// Fsync a directory so entries created, renamed or removed inside it survive power loss.
///
/// Directory fsync is only meaningful (and only possible through `std`) on unix; elsewhere it
/// is a no-op.
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                LogPoseError::Message(format!(
                    "failed to fsync directory '{}': {error}",
                    path.display()
                ))
            })
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

/// Fsync the directory that contains `path`.
pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    sync_dir(parent_dir(path))
}

/// Create `path` and any missing ancestors, fsyncing the parent of every directory created.
pub(crate) fn create_dir_all_synced(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.exists() {
        missing.push(cursor);
        match cursor.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => cursor = parent,
            _ => break,
        }
    }
    if missing.is_empty() {
        return Ok(());
    }

    fs::create_dir_all(path).map_err(|error| {
        LogPoseError::Message(format!(
            "failed to create directory '{}': {error}",
            path.display()
        ))
    })?;
    for created in missing.iter().rev() {
        sync_parent_dir(created)?;
    }
    Ok(())
}

/// Write `bytes` to a new file at `path` and fsync the file contents.
///
/// The caller owns publishing the file (renaming it into place and syncing the directory).
pub(crate) fn write_file_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path).map_err(|error| {
        LogPoseError::Message(format!(
            "failed to create file '{}': {error}",
            path.display()
        ))
    })?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| {
            LogPoseError::Message(format!(
                "failed to write file '{}': {error}",
                path.display()
            ))
        })
}

fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn create_dir_all_synced_creates_every_missing_ancestor() {
        let base = std::env::temp_dir().join(format!(
            "logpose-durable-fs-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        let nested = base.join("a").join("b").join("c");

        create_dir_all_synced(&nested).expect("nested directories should be created");
        assert!(nested.is_dir());
        create_dir_all_synced(&nested).expect("existing directories should be a no-op");

        let file = nested.join("payload");
        write_file_synced(&file, b"payload").expect("file should be written");
        sync_parent_dir(&file).expect("parent directory should sync");
        assert_eq!(
            fs::read(&file).expect("file should be readable"),
            b"payload".to_vec()
        );

        let _ = fs::remove_dir_all(base);
    }
}
