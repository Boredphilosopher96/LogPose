//! Configuration loading for LogPose services and tooling.

use logpose_types::{LogPoseError, Result};
use serde::{Deserialize, Serialize};

/// Runtime configuration for the LogPose platform.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogPoseConfig {
    /// Human-readable deployment name.
    pub node_name: String,
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
}

impl Default for LogPoseConfig {
    fn default() -> Self {
        Self {
            node_name: "logpose-node-1".to_owned(),
            rest_host: "127.0.0.1".to_owned(),
            rest_port: 8080,
            grpc_host: "127.0.0.1".to_owned(),
            grpc_port: 50051,
            log_filter: "info,logpose=debug".to_owned(),
        }
    }
}

impl LogPoseConfig {
    /// Load configuration from `LOGPOSE_CONFIG` when provided, otherwise use defaults.
    pub fn load() -> Result<Self> {
        match std::env::var("LOGPOSE_CONFIG") {
            Ok(value) if !value.trim().is_empty() => toml::from_str(&value)
                .map_err(|error| LogPoseError::Message(format!("invalid LOGPOSE_CONFIG: {error}"))),
            _ => Ok(Self::default()),
        }
    }
}
