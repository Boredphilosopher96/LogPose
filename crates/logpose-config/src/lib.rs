//! Configuration loading for LogPose services and tooling.

use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, LogPoseError, MetadataBackend, MetadataConfig, NodeRole, Result,
};
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
    /// Metadata control-plane backend and settings.
    #[serde(default)]
    pub metadata: MetadataConfig,
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
            metadata: MetadataConfig::default(),
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
        if self.metadata.backend == MetadataBackend::Etcd && self.metadata.etcd.endpoints.is_empty()
        {
            return Err(LogPoseError::Message(
                "invalid LOGPOSE_CONFIG: metadata.etcd.endpoints must be non-empty when metadata.backend is 'etcd'".to_owned(),
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
        assert_eq!(config.metadata.backend.to_string(), "local");
    }

    #[test]
    fn from_toml_str_reads_etcd_metadata_backend() {
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://127.0.0.1:2379", "http://127.0.0.1:22379"]
key_prefix = "/logpose/prod"
timeout_ms = 900
membership_ttl_secs = 25
leadership_ttl_secs = 12
cluster_name = "prod-cluster""#,
        )
        .expect("config should load");

        assert_eq!(config.metadata.backend.to_string(), "etcd");
        assert_eq!(
            config.metadata.etcd.endpoints,
            vec![
                "http://127.0.0.1:2379".to_owned(),
                "http://127.0.0.1:22379".to_owned()
            ]
        );
        assert_eq!(config.metadata.etcd.key_prefix, "/logpose/prod");
        assert_eq!(config.metadata.etcd.timeout_ms, 900);
        assert_eq!(config.metadata.etcd.membership_ttl_secs, 25);
        assert_eq!(config.metadata.etcd.leadership_ttl_secs, 12);
        assert_eq!(config.metadata.etcd.cluster_name, "prod-cluster");
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
    fn from_toml_str_rejects_etcd_backend_with_empty_endpoints() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = []"#,
        )
        .expect_err("etcd backend with empty endpoints should be rejected");

        assert!(error.to_string().contains("metadata.etcd.endpoints"));
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
