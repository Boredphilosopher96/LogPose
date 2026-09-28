//! On-disk layout: where the local engine keeps each root, descriptor, WAL, manifest, segment,
//! and deletion-vector file.
//!
//! A collection directory holds `descriptor.json`, `placement.json`, `maintenance.json`,
//! `CURRENT`, `manifests/<generation:020>.mf`, `wal/<first seq no:020>.wal`, and
//! `segments/`: one segment v2 file per segment unit, `<unit:08x>.seg`, and its deletion-vector
//! files, `<unit:08x>.dv.<generation:016x>`. Unit ids and DV generations are never reused, so no
//! file name is ever issued twice.

use crate::engine::EngineCore;
use logpose_catalog::CollectionDescriptor;
use logpose_types::UnitId;
use std::path::{Path, PathBuf};

/// Directory of segment and deletion-vector files.
pub(crate) const SEGMENTS_DIR: &str = "segments";
/// Directory of WAL files.
pub(crate) const WAL_DIR: &str = "wal";
/// Extension of a segment file.
pub(crate) const SEGMENT_EXTENSION: &str = ".seg";

/// `segments/<unit:08x>.seg` of the collection in `dir`.
pub(crate) fn segment_path(dir: &Path, unit: UnitId) -> PathBuf {
    dir.join(SEGMENTS_DIR)
        .join(format!("{unit}{SEGMENT_EXTENSION}"))
}

/// The unit a segment file name belongs to: eight lowercase hex digits and `.seg`.
pub(crate) fn parse_segment_file_name(name: &str) -> Option<UnitId> {
    let (digits, rest) = name.split_at_checked(8)?;
    if rest != SEGMENT_EXTENSION
        || !digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    u32::from_str_radix(digits, 16).ok().map(UnitId)
}

impl EngineCore {
    pub(crate) fn databases_root(&self) -> PathBuf {
        self.root.join("databases")
    }

    pub(crate) fn database_descriptor_path(&self, database_name: &str) -> PathBuf {
        self.databases_root()
            .join(database_name)
            .join("descriptor.json")
    }

    pub(crate) fn database_policy_path(&self, database_name: &str) -> PathBuf {
        self.databases_root()
            .join(database_name)
            .join("policy.json")
    }

    pub(crate) fn principals_root(&self) -> PathBuf {
        self.root.join("principals")
    }

    pub(crate) fn principal_descriptor_path(&self, principal_name: &str) -> PathBuf {
        self.principals_root()
            .join(principal_name)
            .join("descriptor.json")
    }

    /// The collection's WAL directory: `wal/<first seq no:020>.wal` files.
    pub(crate) fn wal_dir(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join(WAL_DIR)
    }

    pub(crate) fn placement_file_path(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("placement.json")
    }

    pub(crate) fn descriptor_path(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("descriptor.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_file_names_round_trip_and_reject_foreign_names() {
        assert_eq!(
            segment_path(Path::new("/c"), UnitId(0x2a)),
            Path::new("/c/segments/0000002a.seg")
        );
        assert_eq!(parse_segment_file_name("0000002a.seg"), Some(UnitId(0x2a)));
        assert_eq!(
            parse_segment_file_name("ffffffff.seg"),
            Some(UnitId(u32::MAX))
        );
        for foreign in [
            "0000002A.seg",
            "2a.seg",
            "0000002a.lps",
            "0000002a.seg.tmp",
            "0000002a.dv.0000000000000001",
        ] {
            assert_eq!(parse_segment_file_name(foreign), None, "{foreign}");
        }
    }
}
