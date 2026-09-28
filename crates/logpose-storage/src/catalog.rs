//! Database, principal and access-policy descriptor files, served through [`CatalogStore`].

use crate::{
    Engine,
    durable_fs::{create_dir_all_synced, path_exists, sync_dir},
    engine::EngineCore,
    error::{invalid_descriptor, io_message, json_message},
    fs_util::{atomic_write, read_json},
};
use logpose_auth::{DatabaseAccessPolicy, Principal};
use logpose_catalog::{CatalogStore, DatabaseDescriptor};
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

impl CatalogStore for EngineCore {
    fn put_database(&self, descriptor: DatabaseDescriptor) -> Result<DatabaseDescriptor> {
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

    fn get_database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
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

    fn list_databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        self.ensure_database_descriptor(DEFAULT_DATABASE_NAME)?;
        self.list_database_descriptors()
    }

    fn delete_database(&self, database_name: &str) -> Result<()> {
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

    fn put_principal(&self, principal: Principal) -> Result<Principal> {
        validate_principal(&principal)?;
        atomic_write(
            self.vfs.as_ref(),
            &self.principal_descriptor_path(&principal.name),
            serde_json::to_vec_pretty(&principal).map_err(json_message)?,
        )?;
        Ok(principal)
    }

    fn get_principal(&self, principal_name: &str) -> Result<Principal> {
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

    fn list_principals(&self) -> Result<Vec<Principal>> {
        self.list_principal_descriptors()
    }

    fn put_database_access_policy(
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

    fn get_database_access_policy(&self, database_name: &str) -> Result<DatabaseAccessPolicy> {
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
impl CatalogStore for Engine {
    fn put_database(&self, descriptor: DatabaseDescriptor) -> Result<DatabaseDescriptor> {
        self.core().put_database(descriptor)
    }

    fn get_database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        self.core().get_database(database_name)
    }

    fn list_databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        self.core().list_databases()
    }

    fn delete_database(&self, database_name: &str) -> Result<()> {
        self.core().delete_database(database_name)
    }

    fn put_principal(&self, principal: Principal) -> Result<Principal> {
        self.core().put_principal(principal)
    }

    fn get_principal(&self, principal_name: &str) -> Result<Principal> {
        self.core().get_principal(principal_name)
    }

    fn list_principals(&self) -> Result<Vec<Principal>> {
        self.core().list_principals()
    }

    fn put_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        self.core().put_database_access_policy(policy)
    }

    fn get_database_access_policy(&self, database_name: &str) -> Result<DatabaseAccessPolicy> {
        self.core().get_database_access_policy(database_name)
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
    use crate::test_support::unique_temp_dir;

    #[test]
    fn list_databases_bootstraps_the_default_database_descriptor() {
        let root = unique_temp_dir("storage-default-database-bootstrap");
        let engine =
            Engine::open_local(&root, crate::EngineConfig::default()).expect("engine should open");

        let databases = engine
            .list_databases()
            .expect("database listing should bootstrap the default database");

        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0].name, DEFAULT_DATABASE_NAME);
        assert!(databases[0].is_default);
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
