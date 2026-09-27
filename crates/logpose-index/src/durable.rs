//! Crash-durable sidecar publishing.

use std::{
    fs::{self, File},
    io::{self, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

/// Durably replace `path` with `bytes`.
///
/// Writes a temp file in the destination directory, fsyncs it, renames it over `path`, and
/// fsyncs the directory so both the contents and the name survive power loss. Missing parent
/// directories are created and made durable too.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = parent_dir(path);
    create_dir_all_synced(parent)?;

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "sidecar".to_owned());
    let temp_path = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));

    let written = File::create(&temp_path).and_then(|mut file| {
        file.write_all(bytes)?;
        file.sync_all()
    });
    if let Err(error) = written.and_then(|()| fs::rename(&temp_path, path)) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    sync_dir(parent)
}

fn create_dir_all_synced(path: &Path) -> io::Result<()> {
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
    fs::create_dir_all(path)?;
    for created in missing.iter().rev() {
        sync_dir(parent_dir(created))?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
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
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn unique_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ))
    }

    fn entry_names(dir: &Path) -> Vec<String> {
        fs::read_dir(dir)
            .expect("directory should be listable")
            .map(|entry| {
                entry
                    .expect("entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    }

    #[test]
    fn write_atomic_replaces_contents_and_leaves_no_temp_files() {
        let base = unique_dir("logpose-index-durable");
        let path = base.join("indexes").join("segment.hnsw.bin");

        write_atomic(&path, b"first").expect("first write should create missing directories");
        write_atomic(&path, b"second").expect("second write should replace the file");

        assert_eq!(
            fs::read(&path).expect("sidecar should be readable"),
            b"second".to_vec()
        );
        assert_eq!(
            entry_names(&base.join("indexes")),
            vec!["segment.hnsw.bin".to_owned()]
        );
        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn write_atomic_cleans_up_temp_file_when_publish_fails() {
        let base = unique_dir("logpose-index-durable-fail");
        let path = base.join("segment.flat.json");
        fs::create_dir_all(path.join("occupied")).expect("blocking directory should be created");

        write_atomic(&path, b"payload").expect_err("renaming over a non-empty directory fails");

        assert_eq!(entry_names(&base), vec!["segment.flat.json".to_owned()]);
        let _ = fs::remove_dir_all(base);
    }
}
