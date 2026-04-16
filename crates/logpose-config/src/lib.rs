//! Configuration loading for LogPose services and tooling.

use logpose_types::{ANONYMOUS_LOCAL_NODE_NAME, LogPoseError, NodeRole, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Runtime configuration for the LogPose platform.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogPoseConfig {
    /// Human-readable deployment name.
    pub node_name: String,
    /// Declared runtime role for this process.
    #[serde(default)]
    pub node_role: NodeRole,
    /// Host address for the REST listener.
    pub rest_host: String,
    /// Port for the REST listener.
    pub rest_port: u16,
    /// Host address for the gRPC listener.
    pub grpc_host: String,
    /// Port for the gRPC listener.
    pub grpc_port: u16,
    /// Default log filter string.
    pub log_filter: String,
    /// Root directory for local storage-engine state.
    pub storage_root: PathBuf,
    /// Optional bearer token required for API authentication.
    ///
    /// When set, all API requests (except health checks) must include an
    /// `Authorization: Bearer <token>` header matching this value.  When
    /// absent the APIs operate in unauthenticated mode.
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for LogPoseConfig {
    fn default() -> Self {
        Self {
            node_name: "logpose-node-1".to_owned(),
            node_role: NodeRole::Combined,
            rest_host: "127.0.0.1".to_owned(),
            rest_port: 8080,
            grpc_host: "127.0.0.1".to_owned(),
            grpc_port: 50051,
            log_filter: "info,logpose=debug".to_owned(),
            storage_root: PathBuf::from(".logpose"),
            auth_token: None,
        }
    }
}

impl LogPoseConfig {
    /// Validate configuration invariants that must hold before runtime bootstrap.
    pub fn validate(&self) -> Result<()> {
        if self.node_name == ANONYMOUS_LOCAL_NODE_NAME {
            return Err(LogPoseError::Message(format!(
                "invalid LOGPOSE_CONFIG: node_name '{}' is reserved for anonymous local placement metadata",
                ANONYMOUS_LOCAL_NODE_NAME
            )));
        }
        if self.auth_token.as_deref().is_some_and(|t| t.is_empty()) {
            return Err(LogPoseError::Message(
                "invalid LOGPOSE_CONFIG: auth_token must not be an empty string".to_owned(),
            ));
        }
        Ok(())
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml_str(value: &str) -> Result<Self> {
        let config: Self = toml::from_str(value)
            .map_err(|error| LogPoseError::Message(format!("invalid LOGPOSE_CONFIG: {error}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from `LOGPOSE_CONFIG` when provided, otherwise use defaults.
    pub fn load() -> Result<Self> {
        match std::env::var("LOGPOSE_CONFIG") {
            Ok(value) if !value.trim().is_empty() => Self::from_toml_str(&value),
            _ => {
                let config = Self::default();
                config.validate()?;
                Ok(config)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_includes_storage_root() {
        let config = LogPoseConfig::default();
        assert_eq!(config.storage_root, PathBuf::from(".logpose"));
    }

    #[test]
    fn from_toml_str_reads_storage_root() {
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
node_role = "data"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data""#,
        )
        .expect("config should load");
        assert_eq!(config.storage_root, PathBuf::from("tmp/logpose-data"));
        assert_eq!(config.node_role, NodeRole::Data);
        assert_eq!(config.rest_port, 18080);
    }

    #[test]
    fn from_toml_str_defaults_node_role_when_omitted() {
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data""#,
        )
        .expect("config should load");

        assert_eq!(config.node_role, NodeRole::Combined);
    }

    #[test]
    fn from_toml_str_rejects_empty_auth_token() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"
auth_token = """#,
        )
        .expect_err("empty auth_token should be rejected");

        assert!(error.to_string().contains("auth_token"));
    }

    #[test]
    fn from_toml_str_rejects_reserved_local_node_name() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "local"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data""#,
        )
        .expect_err("reserved anonymous local node name should be rejected");

        assert!(error.to_string().contains("reserved"));
    }
}
