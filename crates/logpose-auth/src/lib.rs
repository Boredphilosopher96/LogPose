//! Authentication and authorization foundations.

use serde::{Deserialize, Serialize};

/// Coarse legacy access tiers kept for broad runtime splits.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessTier {
    /// Full cluster administration privileges.
    Operator,
    /// Standard application workload privileges.
    Service,
    /// Read-only observability and metadata access.
    Observer,
}

/// Principal kind tracked by authn and authz policies.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// Human operator or end-user identity.
    User,
    /// Non-human application or workload identity.
    Service,
}

/// Supported authentication modes for database-scoped policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationMode {
    /// No caller authentication is enforced.
    Disabled,
    /// Static username/password authentication.
    Password,
    /// Mutual TLS authentication.
    MutualTls,
    /// External token validation such as OIDC or JWT.
    ExternalToken,
}

/// One authenticated principal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    /// Stable principal name.
    pub name: String,
    /// Principal classification.
    pub kind: PrincipalKind,
    /// Cluster-wide operator or service tier.
    #[serde(default = "default_access_tier")]
    pub access_tier: AccessTier,
}

impl Principal {
    /// Build one principal.
    #[must_use]
    pub fn new(name: impl Into<String>, kind: PrincipalKind) -> Self {
        Self::new_with_access_tier(name, kind, default_access_tier())
    }

    /// Build one principal with an explicit access tier.
    #[must_use]
    pub fn new_with_access_tier(
        name: impl Into<String>,
        kind: PrincipalKind,
        access_tier: AccessTier,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            access_tier,
        }
    }

    /// Validate principal shape.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("principal name must not be empty".to_owned());
        }
        Ok(())
    }
}

/// Database-scoped role.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseRole {
    /// Full ownership including policy changes.
    Owner,
    /// Read and write collection data plus inspect metadata.
    ReadWrite,
    /// Read-only access to query and inspect workflows.
    ReadOnly,
}

/// Binding between one principal and one database role.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseRoleBinding {
    /// Database this binding applies to.
    pub database_name: String,
    /// Principal receiving the role.
    pub principal_name: String,
    /// Granted role.
    pub role: DatabaseRole,
}

impl DatabaseRoleBinding {
    /// Validate binding shape.
    pub fn validate(&self) -> Result<(), String> {
        if self.database_name.trim().is_empty() {
            return Err("database_name must not be empty".to_owned());
        }
        if self.principal_name.trim().is_empty() {
            return Err("principal_name must not be empty".to_owned());
        }
        Ok(())
    }
}

/// Database-scoped policy object that combines authn mode and authz bindings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseAccessPolicy {
    /// Stable database name for the policy.
    pub database_name: String,
    /// Authentication mode required for the database.
    pub authentication_mode: AuthenticationMode,
    /// Explicit principal-to-role mappings for the database.
    pub role_bindings: Vec<DatabaseRoleBinding>,
}

impl DatabaseAccessPolicy {
    /// Build a policy with no bindings yet.
    #[must_use]
    pub fn new(database_name: impl Into<String>, authentication_mode: AuthenticationMode) -> Self {
        Self {
            database_name: database_name.into(),
            authentication_mode,
            role_bindings: Vec::new(),
        }
    }

    /// Validate the policy contents.
    pub fn validate(&self) -> Result<(), String> {
        if self.database_name.trim().is_empty() {
            return Err("database_name must not be empty".to_owned());
        }
        for binding in &self.role_bindings {
            binding.validate()?;
            if binding.database_name != self.database_name {
                return Err("role binding database_name must match policy database_name".to_owned());
            }
        }
        Ok(())
    }
}

fn default_access_tier() -> AccessTier {
    AccessTier::Service
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_rejects_blank_name() {
        let error = Principal::new("   ", PrincipalKind::User)
            .validate()
            .expect_err("blank principal name should fail");

        assert!(error.contains("principal name"));
    }

    #[test]
    fn database_binding_rejects_blank_fields() {
        let error = DatabaseRoleBinding {
            database_name: String::new(),
            principal_name: String::new(),
            role: DatabaseRole::ReadOnly,
        }
        .validate()
        .expect_err("blank binding fields should fail");

        assert!(error.contains("database_name"));
    }

    #[test]
    fn database_policy_is_database_scoped() {
        let policy = DatabaseAccessPolicy::new("analytics", AuthenticationMode::Password);

        assert_eq!(policy.database_name, "analytics");
        assert_eq!(
            serde_json::to_value(&policy).expect("policy should serialize"),
            serde_json::json!({
                "database_name": "analytics",
                "authentication_mode": "password",
                "role_bindings": [],
            })
        );
    }

    #[test]
    fn database_policy_validates_nested_bindings() {
        let error = DatabaseAccessPolicy {
            database_name: "default".to_owned(),
            authentication_mode: AuthenticationMode::Password,
            role_bindings: vec![DatabaseRoleBinding {
                database_name: "default".to_owned(),
                principal_name: String::new(),
                role: DatabaseRole::Owner,
            }],
        }
        .validate()
        .expect_err("invalid nested binding should fail");

        assert!(error.contains("principal_name"));
    }

    #[test]
    fn database_policy_rejects_cross_database_binding() {
        let error = DatabaseAccessPolicy {
            database_name: "default".to_owned(),
            authentication_mode: AuthenticationMode::ExternalToken,
            role_bindings: vec![DatabaseRoleBinding {
                database_name: "analytics".to_owned(),
                principal_name: "reader".to_owned(),
                role: DatabaseRole::ReadOnly,
            }],
        }
        .validate()
        .expect_err("cross-database bindings should fail");

        assert!(error.contains("database_name"));
    }
}
