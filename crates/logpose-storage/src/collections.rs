//! Collection descriptors on disk: planning, durable creation, placement assignment, and the
//! descriptor-directory listing the catalog uses.

use crate::{
    CreateCollectionRequest,
    durable_fs::{create_dir_all_synced, sync_dir},
    engine::{CoreRef, EngineCore, already_exists},
    error::{io_message, json_message},
    fs_util::{atomic_write, cleanup_dir, read_json},
    handle::{CollectionHandle, CollectionMeta},
    maintenance::MaintenanceState,
    manifest::Manifest,
    version::DeltaLog,
    writer::{LogicalState, checkpoint_frame},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{CollectionAssignment, CollectionRef, MaintenanceStatus, Result};
use logpose_wal::{WalRecovery, WalWriter};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

impl EngineCore {
    /// Build the descriptor that would be persisted for one collection request.
    pub(crate) fn plan_collection_descriptor(
        &self,
        request: &CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        let request = request.clone().with_defaults();
        let collection = CollectionRef::new(request.database_name.clone(), request.name.clone());
        if self.contains(&collection) {
            return Err(already_exists(&collection));
        }

        let descriptor = CollectionDescriptor::new_in_database(
            request.database_name,
            request.name,
            request.dimensions,
            request.metric,
            self.collections_root(),
        );
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// The persisted placement assignment, if the collection has one.
    pub(crate) fn load_collection_assignment(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<Option<CollectionAssignment>> {
        let path = Self::placement_file_path(descriptor);
        if !self.exists(&path)? {
            return Ok(None);
        }
        read_json(self.vfs.as_ref(), &path).map(Some)
    }

    fn persist_collection_assignment(
        &self,
        descriptor: &CollectionDescriptor,
        assignment: &CollectionAssignment,
    ) -> Result<()> {
        atomic_write(
            self.vfs.as_ref(),
            &Self::placement_file_path(descriptor),
            serde_json::to_vec_pretty(assignment).map_err(json_message)?,
        )
    }

    fn create_collection_directories(&self, descriptor: &CollectionDescriptor) -> Result<()> {
        create_dir_all_synced(self.vfs.as_ref(), &descriptor.root_path)?;
        for child in ["manifests", "wal", "segments", "indexes", "tmp"] {
            self.vfs
                .create_dir_all(&descriptor.root_path.join(child))
                .map_err(|error| io_message("failed to create collection directories", error))?;
        }
        sync_dir(self.vfs.as_ref(), &descriptor.root_path)
    }

    /// Write every file of a new collection; `descriptor.json` last, so a crash before it
    /// leaves a directory that the next open removes. Returns manifest 0 and the WAL writer,
    /// whose first file already holds its synced checkpoint group.
    fn write_collection_files(
        &self,
        descriptor: &CollectionDescriptor,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<(Manifest, WalWriter)> {
        self.ensure_database_descriptor(&descriptor.database_name)?;
        self.create_collection_directories(descriptor)?;
        if let Some(assignment) = assignment {
            self.persist_collection_assignment(descriptor, assignment)?;
        }
        let manifest = Manifest::empty(descriptor.schema()?);
        self.publish_manifest(descriptor, &manifest)
            .map_err(|failure| failure.error)?;
        self.persist_maintenance_status(descriptor, &MaintenanceStatus::default())?;
        let wal = WalRecovery::open(
            Arc::clone(&self.vfs),
            Self::wal_dir(descriptor),
            self.wal_config(),
            manifest.checkpoint_seq_no,
        )?
        .into_writer(&checkpoint_frame(&manifest)?)?;
        atomic_write(
            self.vfs.as_ref(),
            &Self::descriptor_path(descriptor),
            serde_json::to_vec_pretty(descriptor).map_err(json_message)?,
        )?;
        Ok((manifest, wal))
    }

    /// Paths of `<root>/<entry>/descriptor.json` for every subdirectory of `root` that has one,
    /// in name order. A missing `root` has none.
    pub(crate) fn descriptor_files_under(&self, root: &Path) -> Result<Vec<PathBuf>> {
        if !self.exists(root)? {
            return Ok(Vec::new());
        }
        let mut entries = self
            .vfs
            .list(root)
            .map_err(|error| io_message("failed to list descriptor directory", error))?;
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        let mut paths = Vec::new();
        for entry in entries.into_iter().filter(|entry| entry.is_dir) {
            let path = root.join(entry.name).join("descriptor.json");
            if self.exists(&path)? {
                paths.push(path);
            }
        }
        Ok(paths)
    }
}

impl CoreRef {
    /// Durably create a collection from a planned descriptor and register it.
    ///
    /// The name is reserved in the collection map first, so concurrent creates of one name
    /// resolve to exactly one success; the files are written outside any lock.
    pub(crate) fn create_collection(
        &self,
        descriptor: CollectionDescriptor,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<Arc<CollectionHandle>> {
        descriptor.validate()?;
        let reservation = self.reserve(&descriptor.collection_ref())?;
        create_dir_all_synced(self.vfs.as_ref(), &self.collections_root())?;
        let (manifest, wal) = match self.write_collection_files(&descriptor, assignment) {
            Ok(created) => created,
            Err(error) => {
                cleanup_dir(self.vfs.as_ref(), &descriptor.root_path);
                return Err(error);
            }
        };
        let meta = Arc::new(CollectionMeta::new(descriptor, assignment.cloned()));
        let state = LogicalState {
            schema: Arc::new(manifest.schema.clone()),
            delta: DeltaLog::default(),
        };
        let handle = self.start_collection(
            meta,
            Arc::new(manifest),
            state,
            wal,
            MaintenanceState::default(),
        )?;
        reservation.commit(Arc::clone(&handle));
        Ok(handle)
    }
}

/// Parse a `database/collection` lookup name; a bare name is in the default database.
pub(crate) fn collection_ref_from_lookup(name: &str) -> CollectionRef {
    let parts = name.split('/').collect::<Vec<_>>();
    if parts.len() == 2 && parts.iter().all(|part| !part.trim().is_empty()) {
        CollectionRef::new(parts[0], parts[1])
    } else {
        CollectionRef::new_default(name)
    }
}
