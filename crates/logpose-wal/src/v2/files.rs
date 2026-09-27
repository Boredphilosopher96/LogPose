//! WAL file naming and directory listing.

use super::{FENCE_FILE_NAME, WalError};
use logpose_types::SeqNo;
use logpose_vfs::Vfs;
use std::{
    io,
    path::{Path, PathBuf},
};

/// Suffix of every WAL file.
pub const WAL_FILE_SUFFIX: &str = ".wal";
const NAME_DIGITS: usize = 20;

/// File name of the WAL file whose first sequence number is `first_seq_no`.
#[must_use]
pub fn wal_file_name(first_seq_no: SeqNo) -> String {
    format!("{first_seq_no:020}{WAL_FILE_SUFFIX}")
}

/// The first sequence number encoded in a WAL file name, or `None` if `name` is not exactly 20
/// decimal digits followed by `.wal`.
#[must_use]
pub fn parse_wal_file_name(name: &str) -> Option<SeqNo> {
    let digits = name.strip_suffix(WAL_FILE_SUFFIX)?;
    if digits.len() != NAME_DIGITS || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// One WAL file, identified by the first sequence number in its name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WalFile {
    pub(super) first_seq_no: SeqNo,
    pub(super) path: PathBuf,
}

/// What a WAL directory holds.
#[derive(Debug, Default)]
pub(super) struct DirListing {
    /// WAL files sorted by first sequence number.
    pub(super) files: Vec<WalFile>,
    /// Length of the fence marker, if one exists.
    pub(super) fence_len: Option<u64>,
    /// Whether the directory exists.
    pub(super) exists: bool,
}

/// List a WAL directory. A missing directory lists as empty. Files that do not end in `.wal`
/// are ignored, except the fence marker; a `.wal` file with a malformed name is an error.
pub(super) fn list_dir(vfs: &dyn Vfs, dir: &Path) -> Result<DirListing, WalError> {
    let entries = match vfs.list(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(DirListing::default()),
        Err(error) => return Err(WalError::io("failed to list WAL directory", dir, error)),
    };
    let mut listing = DirListing {
        exists: true,
        ..DirListing::default()
    };
    for entry in entries {
        if entry.is_dir {
            continue;
        }
        if entry.name == FENCE_FILE_NAME {
            listing.fence_len = Some(entry.len);
            continue;
        }
        if !entry.name.ends_with(WAL_FILE_SUFFIX) {
            continue;
        }
        let path = dir.join(&entry.name);
        let first_seq_no = parse_wal_file_name(&entry.name)
            .ok_or(WalError::UnexpectedFile { path: path.clone() })?;
        listing.files.push(WalFile { first_seq_no, path });
    }
    listing.files.sort_by_key(|file| file.first_seq_no);
    Ok(listing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_round_trip_and_sort_numerically() {
        for seq in [0, 1, 48_213, u64::MAX] {
            let name = wal_file_name(seq);
            assert_eq!(name.len(), 24);
            assert_eq!(parse_wal_file_name(&name), Some(seq));
        }
        assert!(wal_file_name(9) < wal_file_name(10));
    }

    #[test]
    fn rejects_malformed_file_names() {
        for name in [
            "active.wal",
            "1.wal",
            "0000000000000000000a.wal",
            "000000000000000000001.wal",
            "00000000000000000001.log",
            "+0000000000000000001.wal",
            "99999999999999999999.wal",
        ] {
            assert_eq!(parse_wal_file_name(name), None, "{name}");
        }
    }
}
