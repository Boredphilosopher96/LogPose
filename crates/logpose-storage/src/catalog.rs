//! Database, principal and access-policy descriptor files: the catalog under the engine's root.
//!
//! The descriptor calls do blocking file I/O. [`Engine`] runs them on its I/O pool, so async
//! callers (the service's request handlers) never block a runtime worker on them.

use crate::{
    Engine,
    durable_fs::{create_dir_all_synced, path_exists, sync_dir},
    engine::EngineCore,
    error::{invalid_descriptor, io_message, json_message},
    fs_util::{atomic_write, read_json},
};
use logpose_auth::{DatabaseAccessPolicy, Principal};
use logpose_catalog::DatabaseDescriptor;
use logpose_types::{CorruptionKind, DEFAULT_DATABASE_NAME, LogPoseError, ResourceKind, Result};
use serde::de::DeserializeOwned;
use std::path::Path;

impl EngineCore {
    pub(crate) fn exists(&self, path: &Path) -> Result<bool> {
        path_exists(self.vfs.as_ref(), path)
    }

    pub(crate) fn ensure_database_descriptor(&self, database_name: &str) -> Result<()> {
        if database_name.trim().is_empty() {
            return Err(LogPoseError::invalid_field(
                "database_name",
                "database name must not be empty",
            ));
        }
        let path = self.database_descriptor_path(database_name);
        if self.exists(&path)? {
            read_stored(self, &path, DatabaseDescriptor::validate)?;
            return Ok(());
        }

        let descriptor = DatabaseDescriptor::new(database_name);
        descriptor.validate()?;
        let parent = path.parent().ok_or_else(|| {
            LogPoseError::internal(format!(
                "database descriptor path for '{database_name}' is missing a parent directory"
            ))
        })?;
        create_dir_all_synced(self.vfs.as_ref(), parent)?;
        atomic_write(
            self.vfs.as_ref(),
            &path,
            serde_json::to_vec_pretty(&descriptor).map_err(json_message)?,
        )?;
        Ok(())
    }

    fn list_database_descriptors(&self) -> Result<Vec<DatabaseDescriptor>> {
        let mut descriptors = Vec::new();
        for path in self.descriptor_files_under(&self.databases_root())? {
            descriptors.push(read_stored(self, &path, DatabaseDescriptor::validate)?);
        }

        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(descriptors)
    }

    fn list_principal_descriptors(&self) -> Result<Vec<Principal>> {
        let mut principals = Vec::new();
        for path in self.descriptor_files_under(&self.principals_root())? {
            principals.push(read_stored(self, &path, validate_principal)?);
        }

        principals.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(principals)
    }
}

impl EngineCore {
    pub(crate) fn put_database(
        &self,
        descriptor: DatabaseDescriptor,
    ) -> Result<DatabaseDescriptor> {
        let mut descriptor = descriptor;
        descriptor.is_default = descriptor.name == DEFAULT_DATABASE_NAME;
        match self.get_database(&descriptor.name) {
            Ok(existing) => {
                descriptor.database_id = existing.database_id;
            }
            Err(error) if error.to_string().contains("does not exist") => {}
            Err(error) => return Err(error),
        }
        descriptor.validate()?;
        atomic_write(
            self.vfs.as_ref(),
            &self.database_descriptor_path(&descriptor.name),
            serde_json::to_vec_pretty(&descriptor).map_err(json_message)?,
        )?;
        Ok(descriptor)
    }

    pub(crate) fn get_database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        validate_namespace_segment("database name", database_name)?;
        let path = self.database_descriptor_path(database_name);
        if database_name == DEFAULT_DATABASE_NAME && !self.exists(&path)? {
            self.ensure_database_descriptor(DEFAULT_DATABASE_NAME)?;
        }
        if !self.exists(&path)? {
            return Err(LogPoseError::not_found(
                ResourceKind::Database,
                database_name,
            ));
        }

        read_stored(self, &path, DatabaseDescriptor::validate)
    }

    pub(crate) fn list_databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        self.ensure_database_descriptor(DEFAULT_DATABASE_NAME)?;
        self.list_database_descriptors()
    }

    pub(crate) fn delete_database(&self, database_name: &str) -> Result<()> {
        validate_namespace_segment("database name", database_name)?;
        if database_name == DEFAULT_DATABASE_NAME {
            return Err(LogPoseError::failed_precondition(
                "the default database cannot be dropped",
            ));
        }
        self.with_empty_database(database_name, || {
            let descriptor = self.database_descriptor_path(database_name);
            if !self.exists(&descriptor)? {
                return Err(LogPoseError::not_found(
                    ResourceKind::Database,
                    database_name,
                ));
            }
            let dir = self.databases_root().join(database_name);
            // The policy goes first: a crash after it leaves a database without a policy, which
            // only operators can use, never a policy a later database of this name inherits.
            let policy = self.database_policy_path(database_name);
            if self.exists(&policy)? {
                self.vfs.remove_file(&policy).map_err(|error| {
                    io_message("failed to remove a database access policy", error)
                })?;
                sync_dir(self.vfs.as_ref(), &dir)?;
            }
            self.vfs
                .remove_file(&descriptor)
                .map_err(|error| io_message("failed to remove a database descriptor", error))?;
            sync_dir(self.vfs.as_ref(), &dir)?;
            self.vfs
                .remove_dir_all(&dir)
                .map_err(|error| io_message("failed to remove a database directory", error))?;
            sync_dir(self.vfs.as_ref(), &self.databases_root())
        })
    }

    pub(crate) fn put_principal(&self, principal: Principal) -> Result<Principal> {
        validate_principal(&principal)?;
        atomic_write(
            self.vfs.as_ref(),
            &self.principal_descriptor_path(&principal.name),
            serde_json::to_vec_pretty(&principal).map_err(json_message)?,
        )?;
        Ok(principal)
    }

    pub(crate) fn get_principal(&self, principal_name: &str) -> Result<Principal> {
        validate_principal_name(principal_name)?;
        let path = self.principal_descriptor_path(principal_name);
        if !self.exists(&path)? {
            return Err(LogPoseError::not_found(
                ResourceKind::Principal,
                principal_name,
            ));
        }

        read_stored(self, &path, validate_principal)
    }

    pub(crate) fn list_principals(&self) -> Result<Vec<Principal>> {
        self.list_principal_descriptors()
    }

    pub(crate) fn put_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        policy.validate().map_err(invalid_descriptor)?;
        self.ensure_database_descriptor(&policy.database_name)?;
        atomic_write(
            self.vfs.as_ref(),
            &self.database_policy_path(&policy.database_name),
            serde_json::to_vec_pretty(&policy).map_err(json_message)?,
        )?;
        Ok(policy)
    }

    pub(crate) fn get_database_access_policy(
        &self,
        database_name: &str,
    ) -> Result<DatabaseAccessPolicy> {
        validate_namespace_segment("database name", database_name)?;
        let path = self.database_policy_path(database_name);
        if !self.exists(&path)? {
            return Err(LogPoseError::not_found(
                ResourceKind::DatabasePolicy,
                database_name,
            ));
        }

        read_stored(self, &path, |policy: &DatabaseAccessPolicy| {
            policy.validate().map_err(invalid_descriptor)
        })
    }
}

/// The catalog of database, principal, and access-policy descriptors under the engine's root.
///
/// Each call does blocking file I/O on the engine's I/O pool. The `_blocking` forms are for
/// threads outside any async runtime, such as bootstrap before the server serves requests.
impl Engine {
    /// Create or replace a database descriptor, keeping the stored database id of one that
    /// exists.
    ///
    /// # Errors
    ///
    /// An invalid descriptor, a damaged stored one, or I/O errors.
    pub async fn put_database(&self, descriptor: DatabaseDescriptor) -> Result<DatabaseDescriptor> {
        self.io(move |core| core.put_database(descriptor)).await
    }

    /// Read one database descriptor, creating the default database's on first use.
    ///
    /// # Errors
    ///
    /// `NotFound`, an invalid name, a damaged stored descriptor (`DATA_LOSS`), or I/O errors.
    pub async fn get_database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        let database_name = database_name.to_owned();
        self.io(move |core| core.get_database(&database_name)).await
    }

    /// Every database descriptor, ordered by name, the default database's included.
    ///
    /// # Errors
    ///
    /// A damaged stored descriptor (`DATA_LOSS`), or I/O errors.
    pub async fn list_databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        self.io(|core| core.list_databases()).await
    }

    /// Delete a database descriptor and its access policy.
    ///
    /// # Errors
    ///
    /// `FAILED_PRECONDITION` for the default database and for a database that still holds a
    /// collection, `NOT_FOUND` when the database does not exist, and I/O errors.
    pub async fn delete_database(&self, database_name: &str) -> Result<()> {
        let database_name = database_name.to_owned();
        self.io(move |core| core.delete_database(&database_name))
            .await
    }

    /// Create or replace a principal descriptor.
    ///
    /// # Errors
    ///
    /// An invalid principal, or I/O errors.
    pub async fn put_principal(&self, principal: Principal) -> Result<Principal> {
        self.io(move |core| core.put_principal(principal)).await
    }

    /// [`Engine::put_principal`] for threads outside any async runtime.
    ///
    /// # Errors
    ///
    /// As [`Engine::put_principal`].
    pub fn put_principal_blocking(&self, principal: Principal) -> Result<Principal> {
        self.core().put_principal(principal)
    }

    /// Read one principal descriptor.
    ///
    /// # Errors
    ///
    /// `NotFound`, an invalid name, a damaged stored descriptor (`DATA_LOSS`), or I/O errors.
    pub async fn get_principal(&self, principal_name: &str) -> Result<Principal> {
        let principal_name = principal_name.to_owned();
        self.io(move |core| core.get_principal(&principal_name))
            .await
    }

    /// [`Engine::get_principal`] for threads outside any async runtime.
    ///
    /// # Errors
    ///
    /// As [`Engine::get_principal`].
    pub fn get_principal_blocking(&self, principal_name: &str) -> Result<Principal> {
        self.core().get_principal(principal_name)
    }

    /// Every stored principal descriptor, ordered by name.
    ///
    /// # Errors
    ///
    /// A damaged stored descriptor (`DATA_LOSS`), or I/O errors.
    pub async fn list_principals(&self) -> Result<Vec<Principal>> {
        self.io(|core| core.list_principals()).await
    }

    /// Create or replace one database's access policy, creating the database's descriptor if
    /// it has none.
    ///
    /// # Errors
    ///
    /// An invalid policy, or I/O errors.
    pub async fn put_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        self.io(move |core| core.put_database_access_policy(policy))
            .await
    }

    /// Read one database's access policy.
    ///
    /// # Errors
    ///
    /// `NotFound`, an invalid name, a damaged stored policy (`DATA_LOSS`), or I/O errors.
    pub async fn get_database_access_policy(
        &self,
        database_name: &str,
    ) -> Result<DatabaseAccessPolicy> {
        let database_name = database_name.to_owned();
        self.io(move |core| core.get_database_access_policy(&database_name))
            .await
    }
}

/// Read the descriptor stored at `path` and check it with `validate`.
///
/// Every descriptor is validated before it is written, so one that fails now is damaged
/// stored data (`DATA_LOSS`), not a bad request.
fn read_stored<T>(
    core: &EngineCore,
    path: &Path,
    validate: impl FnOnce(&T) -> Result<()>,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let value = read_json::<T>(core.vfs.as_ref(), path)?;
    validate(&value).map_err(|error| LogPoseError::Corrupt {
        kind: CorruptionKind::Descriptor,
        location: Some(path.display().to_string()),
        message: format!(
            "stored descriptor '{}' fails validation: {error}",
            path.display()
        ),
    })?;
    Ok(value)
}

fn validate_principal(principal: &Principal) -> Result<()> {
    validate_principal_name(&principal.name)?;
    principal.validate().map_err(invalid_descriptor)
}

fn validate_principal_name(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if value.trim().is_empty() {
        return Err(LogPoseError::invalid_field(
            "name",
            "principal name must not be empty",
        ));
    }
    if value.contains('/') {
        return Err(LogPoseError::invalid_field(
            "name",
            "principal name must not contain '/'",
        ));
    }
    if matches!(trimmed, "." | "..") {
        return Err(LogPoseError::invalid_field(
            "name",
            "principal name must not be a relative path component",
        ));
    }
    Ok(())
}

fn validate_namespace_segment(label: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if value.trim().is_empty() {
        return Err(LogPoseError::invalid_field(
            label.replace(' ', "_"),
            format!("{label} must not be empty"),
        ));
    }
    if value.contains('/') {
        return Err(LogPoseError::invalid_field(
            label.replace(' ', "_"),
            format!("{label} must not contain '/'"),
        ));
    }
    if matches!(trimmed, "." | "..") {
        return Err(LogPoseError::invalid_field(
            label.replace(' ', "_"),
            format!("{label} must not be a relative path component"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ControlledVfs, unique_temp_dir};
    use logpose_vfs::FaultVfs;
    use std::{
        sync::{Arc, mpsc},
        time::Duration,
    };

    #[tokio::test]
    async fn list_databases_bootstraps_the_default_database_descriptor() {
        let root_dir = unique_temp_dir("storage-default-database-bootstrap");
        let root = root_dir.path().to_path_buf();
        let engine =
            Engine::open_local(&root, crate::EngineConfig::default()).expect("engine should open");

        let databases = engine
            .list_databases()
            .await
            .expect("database listing should bootstrap the default database");

        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0].name, DEFAULT_DATABASE_NAME);
        assert!(databases[0].is_default);
    }

    /// A catalog write whose file sync blocks does not block the async runtime awaiting it:
    /// the write runs on the engine's I/O pool, so a runtime with a single worker thread keeps
    /// running its other tasks while the sync is held.
    #[test]
    fn catalog_file_io_runs_off_the_async_runtime() {
        let vfs = ControlledVfs::wrap(FaultVfs::new(3).process());
        let engine = Engine::open(vfs.clone(), "/storage", crate::EngineConfig::default())
            .expect("engine should open");
        let (done, finished) = mpsc::channel();
        // The runtime runs on a thread of its own, so a regression fails the test at the
        // deadline below instead of hanging it.
        let runner = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime should build");
            runtime.block_on(async move {
                vfs.hold_syncs();
                let put = tokio::spawn({
                    let engine = engine.clone();
                    async move {
                        engine
                            .put_database(DatabaseDescriptor::new("analytics"))
                            .await
                    }
                });
                // The put reaches its held sync while this runtime's only worker still runs
                // this task.
                let held = tokio::task::spawn_blocking({
                    let vfs = Arc::clone(&vfs);
                    move || vfs.wait_for_held_sync(Duration::from_secs(30))
                })
                .await
                .expect("the wait should not panic");
                assert!(held, "the put should reach its sync");
                assert!(!put.is_finished(), "the put waits for its sync");
                vfs.release_syncs();
                let stored = put
                    .await
                    .expect("the put should not panic")
                    .expect("the put should succeed");
                assert_eq!(
                    engine
                        .get_database("analytics")
                        .await
                        .expect("the database should be stored"),
                    stored
                );
            });
            let _ = done.send(());
        });
        assert!(
            finished.recv_timeout(Duration::from_secs(60)).is_ok(),
            "a catalog call blocked the runtime's only worker thread"
        );
        runner.join().expect("the runtime thread should not panic");
    }

    #[test]
    fn catalog_validation_rejects_relative_path_components() {
        let principal_error =
            validate_principal_name("..").expect_err("relative principal names should fail");
        assert!(principal_error.to_string().contains("relative path"));

        let database_error = validate_namespace_segment("database name", "..")
            .expect_err("relative database names should fail");
        assert!(database_error.to_string().contains("relative path"));
    }
}
