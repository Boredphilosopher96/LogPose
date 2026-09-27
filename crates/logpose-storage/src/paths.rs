//! On-disk layout: where the local engine keeps each root, descriptor, WAL, manifest and index file.

use crate::engine::EngineCore;
use logpose_catalog::CollectionDescriptor;
use std::path::PathBuf;

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
        descriptor.root_path.join("wal")
    }

    pub(crate) fn flat_index_file_path(
        descriptor: &CollectionDescriptor,
        segment_id: &str,
    ) -> PathBuf {
        descriptor
            .root_path
            .join("indexes")
            .join(format!("{segment_id}.flat.json"))
    }

    pub(crate) fn hnsw_index_file_path(
        descriptor: &CollectionDescriptor,
        segment_id: &str,
    ) -> PathBuf {
        descriptor
            .root_path
            .join("indexes")
            .join(format!("{segment_id}.hnsw.bin"))
    }

    pub(crate) fn manifest_file_path(
        descriptor: &CollectionDescriptor,
        generation: u64,
    ) -> PathBuf {
        descriptor
            .root_path
            .join("manifests")
            .join(format!("{generation:020}.json"))
    }

    pub(crate) fn placement_file_path(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("placement.json")
    }

    pub(crate) fn current_manifest_pointer(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("CURRENT")
    }

    pub(crate) fn descriptor_path(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("descriptor.json")
    }
}
