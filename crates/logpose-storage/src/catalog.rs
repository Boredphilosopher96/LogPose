//! Database, principal and access-policy descriptor files, served through [`CatalogStore`].

use crate::{
    LocalStorageEngine,
    durable_fs::create_dir_all_synced,
    engine::EngineCore,
    error::{json_message, string_message},
    fs_util::{atomic_write, read_json},
};
use logpose_auth::{DatabaseAccessPolicy, Principal};
use logpose_catalog::{CatalogStore, DatabaseDescriptor};
use logpose_types::{DEFAULT_DATABASE_NAME, LogPoseError, Result};

impl EngineCore {
    pub(crate) fn ensure_database_descriptor(&self, database_name: &str) -> Result<()> {
        if database_name.trim().is_empty() {
            return Err(LogPoseError::Message(
                "database name must not be empty".to_owned(),
            ));
        }
        let path = self.database_descriptor_path(database_name);
        if self.exists(&path)? {
            let descriptor = read_json::<DatabaseDescriptor>(self.vfs.as_ref(), &path)?;
            descriptor.validate()?;
            return Ok(());
        }

        let descriptor = DatabaseDescriptor::new(database_name);
        descriptor.validate()?;
        let parent = path.parent().ok_or_else(|| {
            LogPoseError::Message(format!(
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
            let descriptor = read_json::<DatabaseDescriptor>(self.vfs.as_ref(), &path)?;
            descriptor.validate()?;
            descriptors.push(descriptor);
        }

        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(descriptors)
    }

    fn list_principal_descriptors(&self) -> Result<Vec<Principal>> {
        let mut principals = Vec::new();
        for path in self.descriptor_files_under(&self.principals_root())? {
            let principal = read_json::<Principal>(self.vfs.as_ref(), &path)?;
            validate_principal_name(&principal.name)?;
            principal.validate().map_err(string_message)?;
            principals.push(principal);
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
            return Err(LogPoseError::Message(format!(
                "database '{database_name}' does not exist"
            )));
        }

        let descriptor = read_json::<DatabaseDescriptor>(self.vfs.as_ref(), &path)?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn list_databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        self.ensure_database_descriptor(DEFAULT_DATABASE_NAME)?;
        self.list_database_descriptors()
    }

    fn put_principal(&self, principal: Principal) -> Result<Principal> {
        validate_principal_name(&principal.name)?;
        principal.validate().map_err(string_message)?;
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
            return Err(LogPoseError::Message(format!(
                "principal '{principal_name}' does not exist"
            )));
        }

        let principal = read_json::<Principal>(self.vfs.as_ref(), &path)?;
        validate_principal_name(&principal.name)?;
        principal.validate().map_err(string_message)?;
        Ok(principal)
    }

    fn list_principals(&self) -> Result<Vec<Principal>> {
        self.list_principal_descriptors()
    }

    fn put_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        policy.validate().map_err(string_message)?;
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
            return Err(LogPoseError::Message(format!(
                "database access policy '{database_name}' does not exist"
            )));
        }

        let policy = read_json::<DatabaseAccessPolicy>(self.vfs.as_ref(), &path)?;
        policy.validate().map_err(string_message)?;
        Ok(policy)
    }
}

impl CatalogStore for LocalStorageEngine {
    fn put_database(&self, descriptor: DatabaseDescriptor) -> Result<DatabaseDescriptor> {
        self.engine().core().put_database(descriptor)
    }

    fn get_database(&self, database_name: &str) -> Result<DatabaseDescriptor> {
        self.engine().core().get_database(database_name)
    }

    fn list_databases(&self) -> Result<Vec<DatabaseDescriptor>> {
        self.engine().core().list_databases()
    }

    fn put_principal(&self, principal: Principal) -> Result<Principal> {
        self.engine().core().put_principal(principal)
    }

    fn get_principal(&self, principal_name: &str) -> Result<Principal> {
        self.engine().core().get_principal(principal_name)
    }

    fn list_principals(&self) -> Result<Vec<Principal>> {
        self.engine().core().list_principals()
    }

    fn put_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> Result<DatabaseAccessPolicy> {
        self.engine().core().put_database_access_policy(policy)
    }

    fn get_database_access_policy(&self, database_name: &str) -> Result<DatabaseAccessPolicy> {
        self.engine()
            .core()
            .get_database_access_policy(database_name)
    }
}

fn validate_principal_name(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if value.trim().is_empty() {
        return Err(LogPoseError::Message(
            "principal name must not be empty".to_owned(),
        ));
    }
    if value.contains('/') {
        return Err(LogPoseError::Message(
            "principal name must not contain '/'".to_owned(),
        ));
    }
    if matches!(trimmed, "." | "..") {
        return Err(LogPoseError::Message(
            "principal name must not be a relative path component".to_owned(),
        ));
    }
    Ok(())
}

fn validate_namespace_segment(label: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if value.trim().is_empty() {
        return Err(LogPoseError::Message(format!("{label} must not be empty")));
    }
    if value.contains('/') {
        return Err(LogPoseError::Message(format!(
            "{label} must not contain '/'"
        )));
    }
    if matches!(trimmed, "." | "..") {
        return Err(LogPoseError::Message(format!(
            "{label} must not be a relative path component"
        )));
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
        let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

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
