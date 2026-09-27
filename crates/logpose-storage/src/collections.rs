//! Collection descriptors on disk: planning, creation, placement assignment, lookup and listing.

use crate::{
    CreateCollectionRequest, LocalStorageEngine,
    durable_fs::{create_dir_all_synced, sync_dir, sync_parent_dir},
    error::{io_message, json_message},
    fs_util::{atomic_write, cleanup_dir, read_json},
    manifest::Manifest,
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{CollectionAssignment, CollectionRef, LogPoseError, MaintenanceStatus, Result};
use logpose_wal::WalWriter;
use std::fs;

impl LocalStorageEngine {
    /// Build the descriptor that would be persisted for one collection request.
    pub fn plan_collection_descriptor(
        &self,
        request: &CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        let request = request.clone().with_defaults();
        let collection = CollectionRef::new(request.database_name.clone(), request.name.clone());
        if self.find_collection_descriptor_ref(&collection).is_ok() {
            return Err(LogPoseError::Message(format!(
                "collection '{}/{}' already exists",
                collection.database_name, collection.collection_name
            )));
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

    /// Persist a collection using a previously planned descriptor.
    pub fn create_collection_from_descriptor(
        &self,
        descriptor: CollectionDescriptor,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<CollectionDescriptor> {
        create_dir_all_synced(&self.collections_root())?;
        if self
            .find_collection_descriptor_ref(&descriptor.collection_ref())
            .is_ok()
        {
            return Err(LogPoseError::Message(format!(
                "collection '{}/{}' already exists",
                descriptor.database_name, descriptor.name
            )));
        }

        descriptor.validate()?;
        let result = (|| -> Result<()> {
            self.ensure_database_descriptor(&descriptor.database_name)?;
            self.create_collection_directories(&descriptor)?;
            if let Some(assignment) = assignment {
                self.persist_collection_assignment(&descriptor, assignment)?;
            }
            self.publish_manifest(&descriptor, &Manifest::empty(0))?;
            self.persist_maintenance_status(&descriptor, &MaintenanceStatus::default())?;
            let mut wal_writer = WalWriter::open(Self::active_wal_path(&descriptor))?;
            wal_writer.truncate()?;
            sync_parent_dir(&Self::active_wal_path(&descriptor))?;
            atomic_write(
                &Self::descriptor_path(&descriptor),
                serde_json::to_vec_pretty(&descriptor).map_err(json_message)?,
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(descriptor),
            Err(error) => {
                cleanup_dir(&descriptor.root_path);
                Err(error)
            }
        }
    }

    pub(crate) fn load_collection_assignment(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        let path = Self::placement_file_path(descriptor);
        if !path.exists() {
            return Err(LogPoseError::Message(format!(
                "collection '{}' is missing placement metadata",
                descriptor.name
            )));
        }
        read_json(&path)
    }

    fn persist_collection_assignment(
        &self,
        descriptor: &CollectionDescriptor,
        assignment: &CollectionAssignment,
    ) -> Result<()> {
        atomic_write(
            &Self::placement_file_path(descriptor),
            serde_json::to_vec_pretty(assignment).map_err(json_message)?,
        )
    }

    pub(crate) fn create_collection_internal(
        &self,
        request: CreateCollectionRequest,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<CollectionDescriptor> {
        let descriptor = self.plan_collection_descriptor(&request)?;
        self.create_collection_from_descriptor(descriptor, assignment)
    }

    /// Open a collection descriptor using an explicit database namespace.
    pub async fn open_collection_in_database(
        &self,
        database_name: &str,
        name: &str,
    ) -> Result<CollectionDescriptor> {
        self.find_collection_descriptor_ref(&CollectionRef::new(database_name, name))
    }

    fn create_collection_directories(&self, descriptor: &CollectionDescriptor) -> Result<()> {
        create_dir_all_synced(&descriptor.root_path)?;
        fs::create_dir_all(descriptor.root_path.join("manifests"))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("wal")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("segments")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("indexes")))
            .and_then(|_| fs::create_dir_all(descriptor.root_path.join("tmp")))
            .map_err(|error| io_message("failed to create collection directories", error))?;
        sync_dir(&descriptor.root_path)
    }

    pub(crate) fn find_collection_descriptor(&self, name: &str) -> Result<CollectionDescriptor> {
        self.find_collection_descriptor_ref(&Self::collection_ref_from_lookup(name))
    }

    fn find_collection_descriptor_ref(
        &self,
        collection: &CollectionRef,
    ) -> Result<CollectionDescriptor> {
        let collections_root = self.collections_root();
        if !collections_root.exists() {
            return Err(LogPoseError::Message(format!(
                "collection '{}/{}' does not exist",
                collection.database_name, collection.collection_name
            )));
        }

        for entry in fs::read_dir(&collections_root)
            .map_err(|error| io_message("failed to list collections root", error))?
        {
            let entry =
                entry.map_err(|error| io_message("failed to read collection entry", error))?;
            let path = entry.path().join("descriptor.json");
            if !path.exists() {
                continue;
            }

            let descriptor = read_json::<CollectionDescriptor>(&path)?;
            if descriptor.database_name == collection.database_name
                && descriptor.name == collection.collection_name
            {
                descriptor.validate()?;
                return Ok(descriptor);
            }
        }

        Err(LogPoseError::Message(format!(
            "collection '{}/{}' does not exist",
            collection.database_name, collection.collection_name
        )))
    }

    pub(crate) fn list_collection_descriptors(&self) -> Result<Vec<CollectionDescriptor>> {
        let collections_root = self.collections_root();
        if !collections_root.exists() {
            return Ok(Vec::new());
        }

        let mut descriptors = Vec::new();
        for entry in fs::read_dir(&collections_root)
            .map_err(|error| io_message("failed to list collections root", error))?
        {
            let entry =
                entry.map_err(|error| io_message("failed to read collection entry", error))?;
            let path = entry.path().join("descriptor.json");
            if !path.exists() {
                continue;
            }

            let descriptor = read_json::<CollectionDescriptor>(&path)?;
            descriptor.validate()?;
            descriptors.push(descriptor);
        }

        descriptors.sort_by(|left, right| {
            (&left.database_name, &left.name).cmp(&(&right.database_name, &right.name))
        });
        Ok(descriptors)
    }

    fn collection_ref_from_lookup(name: &str) -> CollectionRef {
        let parts = name.split('/').collect::<Vec<_>>();
        if parts.len() == 2 && parts.iter().all(|part| !part.trim().is_empty()) {
            CollectionRef::new(parts[0], parts[1])
        } else {
            CollectionRef::new_default(name)
        }
    }
}
