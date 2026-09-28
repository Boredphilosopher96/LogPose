//! Collection create requests, and collection descriptors on disk: planning, durable creation,
//! placement assignment, and the descriptor-directory listing the catalog uses.

use crate::{
    durable_fs::{create_dir_all_synced, sync_dir},
    engine::{CoreRef, EngineCore, already_exists},
    error::{io_message, json_message},
    fs_util::{atomic_write, cleanup_dir, read_json},
    handle::{CollectionHandle, CollectionMeta},
    manifest::{Manifest, publish_manifest},
    recovery::{DurableStart, new_state},
    writer::{PkIndex, checkpoint_frame},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    CollectionAssignment, CollectionRef, DEFAULT_DATABASE_NAME, DistanceMetric, Result, UnitId,
    schema::{CreateCollectionSpec, PrimaryKeySpec, PrimaryKeyType, VectorFieldSpec},
};
use logpose_wal::{WalRecovery, WalWriter};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Request payload for creating a collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateCollectionRequest {
    /// Database containing the collection. Blank values default to `default`.
    pub database_name: String,
    /// The collection's name and schema, as the create request carries them (engine plan
    /// decision D4). Validated when the collection is planned, so errors name spec fields
    /// such as `vectors[0].dimensions`.
    pub spec: CreateCollectionSpec,
}

impl CreateCollectionRequest {
    /// Create a collection from a schema-based spec in an explicit database namespace.
    #[must_use]
    pub fn from_spec(database_name: impl Into<String>, spec: CreateCollectionSpec) -> Self {
        Self {
            database_name: database_name.into(),
            spec,
        }
    }

    /// A request in the default database for the single-vector shape: string primary key
    /// `id`, one vector field `vector` with `dimensions` and `metric`, and dynamic fields on.
    #[must_use]
    pub fn new(name: impl Into<String>, dimensions: usize, metric: DistanceMetric) -> Self {
        Self::in_database(DEFAULT_DATABASE_NAME, name, dimensions, metric)
    }

    /// [`CreateCollectionRequest::new`] in an explicit database namespace.
    #[must_use]
    pub fn in_database(
        database_name: impl Into<String>,
        name: impl Into<String>,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> Self {
        Self::from_spec(
            database_name,
            CreateCollectionSpec {
                name: name.into(),
                primary_key: PrimaryKeySpec {
                    name: "id".to_owned(),
                    key_type: PrimaryKeyType::String,
                },
                vectors: vec![VectorFieldSpec {
                    name: "vector".to_owned(),
                    // Out-of-range dimensions fail validation when the collection is planned.
                    dimensions: u32::try_from(dimensions).unwrap_or(u32::MAX),
                    metric,
                }],
                fields: Vec::new(),
                dynamic_fields: true,
            },
        )
    }

    /// The collection name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.spec.name
    }

    /// Return the canonical database/collection reference for this request.
    #[must_use]
    pub fn collection_ref(&self) -> CollectionRef {
        let request = self.clone().with_defaults();
        CollectionRef::new(request.database_name, request.spec.name)
    }

    /// Return the canonical database/collection lookup key for this request.
    #[must_use]
    pub fn lookup_name(&self) -> String {
        self.collection_ref().lookup_name()
    }

    pub(crate) fn with_defaults(self) -> Self {
        let database_name = if self.database_name.trim().is_empty() {
            DEFAULT_DATABASE_NAME.to_owned()
        } else {
            self.database_name
        };
        Self {
            database_name,
            spec: self.spec,
        }
    }
}

impl EngineCore {
    /// Build the descriptor that would be persisted for one collection request.
    pub(crate) fn plan_collection_descriptor(
        &self,
        request: &CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        let request = request.clone().with_defaults();
        let collection = request.collection_ref();
        let schema = request.spec.build_schema()?;
        let descriptor = CollectionDescriptor::new_in_database(
            request.database_name,
            request.spec.name,
            schema,
            self.collections_root(),
        );
        descriptor.validate()?;
        if self.contains(&collection) {
            return Err(already_exists(&collection));
        }
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
        for child in ["manifests", "wal", "segments"] {
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
        let manifest = Manifest::empty(descriptor.collection_id.clone(), descriptor.schema.clone());
        publish_manifest(self.vfs.as_ref(), &descriptor.root_path, &manifest)
            .map_err(|failure| failure.error)?;
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
        // The active memtable takes the first unit id.
        let state = new_state(
            Arc::new(manifest.schema.clone()),
            UnitId(manifest.next_unit_id),
            manifest.checkpoint_seq_no + 1,
            self.tokens.clock.now(),
            Arc::from(Vec::new()),
            Default::default(),
            PkIndex::default(),
            self.strict_invariants,
        );
        let handle = self.start_collection(
            meta,
            DurableStart {
                next_manifest_gen: manifest.generation + 1,
                next_unit_id: manifest.next_unit_id + 1,
                next_dv_gen: manifest.next_dv_gen,
                manifest: Arc::new(manifest),
                previous_generation: None,
            },
            state,
            wal,
            true,
        )?;
        reservation.commit(Arc::clone(&handle));
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_types::{LogPoseError, schema::MAX_VECTOR_DIMENSIONS};

    #[test]
    fn the_single_vector_request_is_a_string_key_one_vector_and_dynamic_fields() {
        let request = CreateCollectionRequest::new("docs", 3, DistanceMetric::L2);
        assert_eq!(request.collection_ref(), CollectionRef::new_default("docs"));
        let schema = request.spec.build_schema().expect("the schema builds");
        assert_eq!(schema.primary_key().name, "id");
        assert_eq!(schema.primary_key_type(), PrimaryKeyType::String);
        let vectors = schema.vectors();
        assert_eq!(vectors.len(), 1);
        assert_eq!(vectors[0].name, "vector");
        assert_eq!(vectors[0].dimensions, 3);
        assert_eq!(vectors[0].metric, DistanceMetric::L2);
        assert!(schema.fields().is_empty());
        assert!(schema.dynamic_fields());
        assert_eq!(schema.schema_version(), 1);
    }

    #[test]
    fn the_single_vector_request_rejects_out_of_range_dimensions() {
        let too_many = usize::try_from(MAX_VECTOR_DIMENSIONS).expect("fits") + 1;
        for dimensions in [0, too_many, usize::MAX] {
            let error = CreateCollectionRequest::new("docs", dimensions, DistanceMetric::Dot)
                .spec
                .build_schema()
                .expect_err("out-of-range dimensions are refused");
            assert!(
                matches!(
                    &error,
                    LogPoseError::InvalidArgument { field: Some(field), .. }
                        if field == "vectors[0].dimensions"
                ),
                "{dimensions}: {error:?}"
            );
        }
    }
}
