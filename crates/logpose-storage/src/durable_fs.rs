//! Crash-durable filesystem helpers over [`Vfs`].
//!
//! A file's contents are durable after `fsync` on the file, but its name is
//! only durable once the directory that holds the entry is fsynced too. Every
//! create, rename and removal that recovery depends on must therefore be
//! followed by a sync of the parent directory.

use logpose_types::{LogPoseError, Result};
use logpose_vfs::{OpenMode, Vfs, parent_dir};
use std::{io::IoSlice, path::Path};

/// Fsync a directory so entries created, renamed or removed inside it survive power loss.
pub(crate) fn sync_dir(vfs: &dyn Vfs, path: &Path) -> Result<()> {
    vfs.sync_dir(path).map_err(|error| {
        LogPoseError::io(
            format!("failed to fsync directory '{}'", path.display()),
            error,
        )
    })
}

/// Fsync the directory that contains `path`.
pub(crate) fn sync_parent_dir(vfs: &dyn Vfs, path: &Path) -> Result<()> {
    sync_dir(vfs, parent_dir(path))
}

/// Whether `path` exists.
pub(crate) fn path_exists(vfs: &dyn Vfs, path: &Path) -> Result<bool> {
    logpose_vfs::exists(vfs, path)
        .map_err(|error| LogPoseError::io(format!("failed to look up '{}'", path.display()), error))
}

/// Create `path` and any missing ancestors, fsyncing the parent of every directory created.
pub(crate) fn create_dir_all_synced(vfs: &dyn Vfs, path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut cursor = path;
    while !path_exists(vfs, cursor)? {
        missing.push(cursor);
        match cursor.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => cursor = parent,
            _ => break,
        }
    }
    if missing.is_empty() {
        return Ok(());
    }

    vfs.create_dir_all(path).map_err(|error| {
        LogPoseError::io(
            format!("failed to create directory '{}'", path.display()),
            error,
        )
    })?;
    for created in missing.iter().rev() {
        sync_parent_dir(vfs, created)?;
    }
    Ok(())
}

/// Write `bytes` to a new file at `path` and fsync the file contents.
///
/// A stale file left at `path` by a crashed process is replaced. The caller owns publishing the
/// file (renaming it into place and syncing the directory).
pub(crate) fn write_file_synced(vfs: &dyn Vfs, path: &Path, bytes: &[u8]) -> Result<()> {
    let context = |error: std::io::Error| {
        LogPoseError::io(format!("failed to write file '{}'", path.display()), error)
    };
    let file = match vfs.open(path, OpenMode::CreateNew) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            vfs.remove_file(path).map_err(context)?;
            vfs.open(path, OpenMode::CreateNew).map_err(|error| {
                LogPoseError::io(format!("failed to create file '{}'", path.display()), error)
            })?
        }
        Err(error) => {
            return Err(LogPoseError::io(
                format!("failed to create file '{}'", path.display()),
                error,
            ));
        }
    };
    file.append(&[IoSlice::new(bytes)])
        .and_then(|_| file.sync_all())
        .map_err(context)
}

/// Read a whole file.
pub(crate) fn read_file(vfs: &dyn Vfs, path: &Path, context: &str) -> Result<Vec<u8>> {
    logpose_vfs::read_file(vfs, path)
        .map_err(|error| LogPoseError::io(format!("{context} '{}'", path.display()), error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_vfs::{FaultVfs, StdVfs};
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

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

        create_dir_all_synced(&StdVfs, &nested).expect("nested directories should be created");
        assert!(nested.is_dir());
        create_dir_all_synced(&StdVfs, &nested).expect("existing directories should be a no-op");

        let file = nested.join("payload");
        write_file_synced(&StdVfs, &file, b"payload").expect("file should be written");
        write_file_synced(&StdVfs, &file, b"replaced").expect("stale file should be replaced");
        sync_parent_dir(&StdVfs, &file).expect("parent directory should sync");
        assert_eq!(
            fs::read(&file).expect("file should be readable"),
            b"replaced".to_vec()
        );

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn synced_directories_and_files_survive_a_crash() {
        let vfs = FaultVfs::new(0);
        let nested = Path::new("/root/a/b");
        create_dir_all_synced(vfs.as_ref(), nested).expect("directories should be created");
        let file = nested.join("payload");
        write_file_synced(vfs.as_ref(), &file, b"payload").expect("file should be written");
        sync_parent_dir(vfs.as_ref(), &file).expect("parent should sync");
        vfs.crash();
        assert_eq!(
            read_file(vfs.as_ref(), &file, "failed to read").expect("file should survive"),
            b"payload".to_vec()
        );
    }
}
