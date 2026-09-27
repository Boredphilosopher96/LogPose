//! On-disk layout: where the local engine keeps each root, descriptor, WAL, manifest and segment
//! file.
//!
//! A collection directory holds `descriptor.json`, `placement.json`, `maintenance.json`,
//! `CURRENT`, `manifests/<generation:020>.mf`, `wal/<first seq no:020>.wal`, and one v1 segment
//! per unit: `segments/<unit:08x>.lps` with the sidecars `indexes/<unit:08x>.flat.json` and
//! `indexes/<unit:08x>.hnsw.bin`, staged under `tmp/` before they are renamed into place. Unit
//! ids are never reused, so no file name is ever issued twice.

use crate::engine::EngineCore;
use logpose_catalog::CollectionDescriptor;
use logpose_types::UnitId;
use std::path::{Path, PathBuf};

/// Directory of segment files.
pub(crate) const SEGMENTS_DIR: &str = "segments";
/// Directory of v1 segment sidecars.
pub(crate) const INDEXES_DIR: &str = "indexes";
/// Directory of files being written before they are renamed into place.
pub(crate) const TMP_DIR: &str = "tmp";
/// Directory of WAL files.
pub(crate) const WAL_DIR: &str = "wal";
/// Extension of a v1 segment file.
pub(crate) const V1_SEGMENT_EXTENSION: &str = ".lps";
/// Extension of a v1 flat index sidecar.
pub(crate) const FLAT_SIDECAR_EXTENSION: &str = ".flat.json";
/// Extension of a v1 HNSW sidecar.
pub(crate) const HNSW_SIDECAR_EXTENSION: &str = ".hnsw.bin";

/// The files of one v1 segment unit, final and staged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnitFiles {
    pub(crate) segment: PathBuf,
    pub(crate) flat: PathBuf,
    pub(crate) hnsw: PathBuf,
    pub(crate) segment_temp: PathBuf,
    pub(crate) flat_temp: PathBuf,
    pub(crate) hnsw_temp: PathBuf,
}

impl UnitFiles {
    /// The files of `unit` in the collection directory `dir`.
    pub(crate) fn new(dir: &Path, unit: UnitId) -> Self {
        let name = |extension: &str| format!("{unit}{extension}");
        let temp = |extension: &str| dir.join(TMP_DIR).join(format!("{unit}{extension}.tmp"));
        Self {
            segment: dir.join(SEGMENTS_DIR).join(name(V1_SEGMENT_EXTENSION)),
            flat: dir.join(INDEXES_DIR).join(name(FLAT_SIDECAR_EXTENSION)),
            hnsw: dir.join(INDEXES_DIR).join(name(HNSW_SIDECAR_EXTENSION)),
            segment_temp: temp(V1_SEGMENT_EXTENSION),
            flat_temp: temp(FLAT_SIDECAR_EXTENSION),
            hnsw_temp: temp(HNSW_SIDECAR_EXTENSION),
        }
    }

    /// The files a durable manifest references.
    pub(crate) fn published(&self) -> Vec<PathBuf> {
        vec![self.segment.clone(), self.flat.clone(), self.hnsw.clone()]
    }

    /// Every file an attempt to write the unit may have left behind.
    pub(crate) fn all(&self) -> Vec<PathBuf> {
        let mut paths = self.published();
        paths.extend([
            self.segment_temp.clone(),
            self.flat_temp.clone(),
            self.hnsw_temp.clone(),
        ]);
        paths
    }
}

/// The unit a segment or sidecar file name belongs to: eight lowercase hex digits followed by
/// one of `extensions`.
pub(crate) fn parse_unit_file_name(name: &str, extensions: &[&str]) -> Option<UnitId> {
    let (digits, rest) = name.split_at_checked(8)?;
    if !extensions.contains(&rest)
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
    fn unit_file_names_round_trip_and_reject_foreign_names() {
        let files = UnitFiles::new(Path::new("/c"), UnitId(0x2a));
        assert_eq!(files.segment, Path::new("/c/segments/0000002a.lps"));
        assert_eq!(files.flat, Path::new("/c/indexes/0000002a.flat.json"));
        assert_eq!(files.hnsw_temp, Path::new("/c/tmp/0000002a.hnsw.bin.tmp"));
        assert_eq!(
            parse_unit_file_name("0000002a.lps", &[V1_SEGMENT_EXTENSION]),
            Some(UnitId(0x2a))
        );
        assert_eq!(
            parse_unit_file_name("ffffffff.hnsw.bin", &[HNSW_SIDECAR_EXTENSION]),
            Some(UnitId(u32::MAX))
        );
        for foreign in [
            "0000002A.lps",
            "2a.lps",
            "0000002a.seg",
            "0000002a.lps.tmp",
            "a7c3e0e4-5b1f-4c7e-9c1e-2f4f1b8a9d10.lps",
        ] {
            assert_eq!(
                parse_unit_file_name(foreign, &[V1_SEGMENT_EXTENSION]),
                None,
                "{foreign}"
            );
        }
    }
}
