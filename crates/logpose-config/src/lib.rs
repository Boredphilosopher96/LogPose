//! Configuration loading for LogPose services and tooling.

use logpose_auth::Principal;
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, LogPoseError, MetadataBackend, MetadataConfig, NodeRole, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::{net::IpAddr, path::PathBuf};

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
    /// Peer-visible host advertised for the REST endpoint.
    #[serde(default)]
    pub rest_advertise_host: Option<String>,
    /// Port for the REST listener.
    pub rest_port: u16,
    /// Host address for the gRPC listener.
    pub grpc_host: String,
    /// Peer-visible host advertised for the gRPC endpoint.
    #[serde(default)]
    pub grpc_advertise_host: Option<String>,
    /// Port for the gRPC listener.
    pub grpc_port: u16,
    /// Default log filter string.
    pub log_filter: String,
    /// Root directory for local storage-engine state.
    pub storage_root: PathBuf,
    /// Metadata control-plane backend and settings.
    #[serde(default)]
    pub metadata: MetadataConfig,
    /// Authentication bootstrap configuration.
    #[serde(default)]
    pub auth: AuthConfig,
    /// Internal peer-to-peer coordination settings.
    #[serde(default)]
    pub internal: InternalConfig,
}

/// Authentication bootstrap and runtime configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Static bearer tokens bound to explicit principals for bootstrapping.
    #[serde(default)]
    pub bootstrap_tokens: Vec<BootstrapTokenConfig>,
}

/// Internal peer-to-peer coordination settings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InternalConfig {
    /// Shared bearer token required for internal replica transfer.
    #[serde(default)]
    pub replica_token: Option<String>,
    /// Per-request timeout for internal replica transfer over HTTP.
    #[serde(default = "default_replica_transfer_timeout_ms")]
    pub replica_transfer_timeout_ms: u64,
    /// Explicit single-host escape hatch for loopback or otherwise non-routable replica endpoints.
    #[serde(default)]
    pub allow_non_routable_rest_advertise_host: bool,
}

impl Default for InternalConfig {
    fn default() -> Self {
        Self {
            replica_token: None,
            replica_transfer_timeout_ms: default_replica_transfer_timeout_ms(),
            allow_non_routable_rest_advertise_host: false,
        }
    }
}

impl InternalConfig {
    fn validate(&self) -> Result<()> {
        validate_optional_token("internal.replica_token", &self.replica_token)?;
        if self.replica_transfer_timeout_ms == 0 {
            return Err(LogPoseError::Message(
                "invalid LOGPOSE_CONFIG: internal.replica_transfer_timeout_ms must be greater than zero"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl AuthConfig {
    fn validate(&self) -> Result<()> {
        let mut seen_tokens = BTreeSet::new();
        let mut seen_principals = BTreeSet::new();
        for (index, token) in self.bootstrap_tokens.iter().enumerate() {
            token.validate().map_err(|message| {
                LogPoseError::Message(format!(
                    "invalid LOGPOSE_CONFIG: auth.bootstrap_tokens[{index}] {message}"
                ))
            })?;
            if !seen_tokens.insert(token.token.clone()) {
                return Err(LogPoseError::Message(
                    "invalid LOGPOSE_CONFIG: auth.bootstrap_tokens must not contain duplicate token values"
                        .to_owned(),
                ));
            }
            if !seen_principals.insert(token.principal.name.clone()) {
                return Err(LogPoseError::Message(
                    "invalid LOGPOSE_CONFIG: auth.bootstrap_tokens must not contain duplicate principal names"
                        .to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// One bearer token bootstrap binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BootstrapTokenConfig {
    /// Shared secret bearer token value.
    pub token: String,
    /// Principal authenticated by this bootstrap token.
    pub principal: Principal,
}

impl BootstrapTokenConfig {
    fn validate(&self) -> std::result::Result<(), String> {
        let trimmed = self.token.trim();
        if trimmed.is_empty() {
            return Err("token must not be empty".to_owned());
        }
        if trimmed != self.token {
            return Err("token must not include leading or trailing whitespace".to_owned());
        }
        self.principal.validate()?;
        Ok(())
    }
}

impl Default for LogPoseConfig {
    fn default() -> Self {
        Self {
            node_name: "logpose-node-1".to_owned(),
            node_role: NodeRole::Combined,
            rest_host: "127.0.0.1".to_owned(),
            rest_advertise_host: None,
            rest_port: 8080,
            grpc_host: "127.0.0.1".to_owned(),
            grpc_advertise_host: None,
            grpc_port: 50051,
            log_filter: "info,logpose=debug".to_owned(),
            storage_root: PathBuf::from(".logpose"),
            metadata: MetadataConfig::default(),
            auth: AuthConfig::default(),
            internal: InternalConfig::default(),
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
        validate_optional_advertise_host("rest_advertise_host", &self.rest_advertise_host)?;
        validate_optional_advertise_host("grpc_advertise_host", &self.grpc_advertise_host)?;
        self.internal.validate()?;
        if self.metadata.backend == MetadataBackend::Etcd {
            self.metadata.etcd.validate().map_err(|error| match error {
                LogPoseError::Message(message) => {
                    LogPoseError::Message(format!("invalid LOGPOSE_CONFIG: {message}"))
                }
            })?;
            self.validate_etcd_peer_connectivity()?;
        }
        self.auth.validate()?;
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

    /// Return the peer-visible REST host, falling back to the bind host.
    #[must_use]
    pub fn advertised_rest_host(&self) -> &str {
        self.rest_advertise_host
            .as_deref()
            .unwrap_or(&self.rest_host)
    }

    /// Return the peer-visible gRPC host, falling back to the bind host.
    #[must_use]
    pub fn advertised_grpc_host(&self) -> &str {
        self.grpc_advertise_host
            .as_deref()
            .unwrap_or(&self.grpc_host)
    }

    fn validate_etcd_peer_connectivity(&self) -> Result<()> {
        if !matches!(self.node_role, NodeRole::Combined | NodeRole::Data) {
            return Ok(());
        }
        if self.internal.replica_token.is_none() {
            return Err(LogPoseError::Message(
                "invalid LOGPOSE_CONFIG: internal.replica_token must be configured for etcd data-serving nodes"
                    .to_owned(),
            ));
        }
        if self.rest_advertise_host.is_none()
            && listener_host_requires_advertise_host(&self.rest_host)
        {
            return Err(LogPoseError::Message(
                "invalid LOGPOSE_CONFIG: rest_advertise_host must be configured for etcd data-serving nodes when rest_host is not peer-routable"
                    .to_owned(),
            ));
        }
        if self.grpc_advertise_host.is_none()
            && listener_host_requires_advertise_host(&self.grpc_host)
        {
            return Err(LogPoseError::Message(
                "invalid LOGPOSE_CONFIG: grpc_advertise_host must be configured for etcd data-serving nodes when grpc_host is not peer-routable"
                    .to_owned(),
            ));
        }
        if listener_host_requires_advertise_host(self.advertised_rest_host())
            && !self.internal.allow_non_routable_rest_advertise_host
        {
            return Err(LogPoseError::Message(
                "invalid LOGPOSE_CONFIG: advertised REST endpoints for etcd data-serving nodes must be peer-routable unless internal.allow_non_routable_rest_advertise_host = true"
                    .to_owned(),
            ));
        }
        if listener_host_requires_advertise_host(self.advertised_grpc_host())
            && !self.internal.allow_non_routable_rest_advertise_host
        {
            return Err(LogPoseError::Message(
                "invalid LOGPOSE_CONFIG: advertised gRPC endpoints for etcd data-serving nodes must be peer-routable unless internal.allow_non_routable_rest_advertise_host = true"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

fn validate_optional_advertise_host(label: &str, value: &Option<String>) -> Result<()> {
    let Some(value) = value.as_deref() else {
        return Ok(());
    };
    if value.trim().is_empty() {
        return Err(LogPoseError::Message(format!(
            "invalid LOGPOSE_CONFIG: {label} must not be empty"
        )));
    }
    if value.trim() != value {
        return Err(LogPoseError::Message(format!(
            "invalid LOGPOSE_CONFIG: {label} must not include leading or trailing whitespace"
        )));
    }
    Ok(())
}

fn validate_optional_token(label: &str, value: &Option<String>) -> Result<()> {
    let Some(value) = value.as_deref() else {
        return Ok(());
    };
    if value.trim().is_empty() {
        return Err(LogPoseError::Message(format!(
            "invalid LOGPOSE_CONFIG: {label} must not be empty"
        )));
    }
    if value.trim() != value {
        return Err(LogPoseError::Message(format!(
            "invalid LOGPOSE_CONFIG: {label} must not include leading or trailing whitespace"
        )));
    }
    Ok(())
}

fn listener_host_requires_advertise_host(host: &str) -> bool {
    let trimmed = host.trim();
    if trimmed.eq_ignore_ascii_case("localhost") || trimmed == "[::1]" {
        return true;
    }
    trimmed
        .parse::<IpAddr>()
        .map(|address| address.is_unspecified() || address.is_loopback())
        .unwrap_or(false)
}

const fn default_replica_transfer_timeout_ms() -> u64 {
    5_000
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
        assert_eq!(config.advertised_rest_host(), "0.0.0.0");
        assert_eq!(config.advertised_grpc_host(), "0.0.0.0");
    }

    #[test]
    fn from_toml_str_reads_etcd_metadata_backend() {
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
rest_advertise_host = "127.0.0.1"
grpc_host = "0.0.0.0"
grpc_port = 15051
grpc_advertise_host = "127.0.0.1"
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
cluster_name = "prod-cluster"

[internal]
replica_token = "replica-secret"
allow_non_routable_rest_advertise_host = true"#,
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
    fn from_toml_str_reads_advertised_hosts() {
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_advertise_host = "node-a"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_advertise_host = "node-a"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data""#,
        )
        .expect("config should load");

        assert_eq!(config.advertised_rest_host(), "node-a");
        assert_eq!(config.advertised_grpc_host(), "node-a");
    }

    #[test]
    fn from_toml_str_rejects_blank_advertised_hosts() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_advertise_host = " "
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data""#,
        )
        .expect_err("blank advertised host should be rejected");

        assert!(
            error
                .to_string()
                .contains("rest_advertise_host must not be empty")
        );
    }

    #[test]
    fn from_toml_str_rejects_etcd_data_node_without_replica_token() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
node_role = "data"
rest_host = "127.0.0.1"
rest_port = 18080
grpc_host = "127.0.0.1"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://127.0.0.1:2379"]
cluster_name = "prod-cluster""#,
        )
        .expect_err("etcd data nodes should require an internal replica token");

        assert!(
            error
                .to_string()
                .contains("internal.replica_token must be configured"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn from_toml_str_rejects_etcd_data_node_with_wildcard_rest_host_and_missing_advertise_host() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
node_role = "data"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "127.0.0.1"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://127.0.0.1:2379"]
cluster_name = "prod-cluster"

[internal]
replica_token = "replica-secret""#,
        )
        .expect_err("wildcard etcd data nodes should require an advertised REST host");

        assert!(
            error
                .to_string()
                .contains("rest_advertise_host must be configured"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn from_toml_str_rejects_remote_etcd_data_node_with_loopback_rest_advertise_host() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
node_role = "data"
rest_host = "0.0.0.0"
rest_advertise_host = "127.0.0.1"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_advertise_host = "edge-a.internal"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://10.0.0.5:2379"]
cluster_name = "prod-cluster"

[internal]
replica_token = "replica-secret""#,
        )
        .expect_err("etcd data nodes should require a peer-routable REST advertise host unless explicitly overridden");

        assert!(
            error.to_string().contains(
                "advertised REST endpoints for etcd data-serving nodes must be peer-routable unless internal.allow_non_routable_rest_advertise_host = true"
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn from_toml_str_allows_loopback_rest_advertise_host_with_explicit_override() {
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
node_role = "data"
rest_host = "0.0.0.0"
rest_advertise_host = "127.0.0.1"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_advertise_host = "127.0.0.1"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://10.0.0.5:2379"]
cluster_name = "prod-cluster"

[internal]
replica_token = "replica-secret"
allow_non_routable_rest_advertise_host = true"#,
        )
        .expect("explicit loopback override should allow single-host development config");

        assert!(config.internal.allow_non_routable_rest_advertise_host);
        assert_eq!(config.advertised_rest_host(), "127.0.0.1");
    }

    #[test]
    fn from_toml_str_rejects_etcd_backend_with_empty_endpoints() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_advertise_host = "edge-a.internal"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_advertise_host = "edge-a.internal"
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
    fn from_toml_str_rejects_blank_etcd_cluster_name() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_advertise_host = "edge-a.internal"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_advertise_host = "edge-a.internal"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://127.0.0.1:2379"]
cluster_name = "   ""#,
        )
        .expect_err("blank cluster name should be rejected");

        assert!(error.to_string().contains("metadata.etcd.cluster_name"));
    }

    #[test]
    fn from_toml_str_rejects_zero_etcd_timeout_and_ttls() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_advertise_host = "edge-a.internal"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_advertise_host = "edge-a.internal"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://127.0.0.1:2379"]
timeout_ms = 0
membership_ttl_secs = 0
leadership_ttl_secs = 0"#,
        )
        .expect_err("zero timeout and ttls should be rejected");

        assert!(
            error.to_string().contains("timeout_ms")
                || error.to_string().contains("membership_ttl_secs")
                || error.to_string().contains("leadership_ttl_secs")
        );
    }

    #[test]
    fn from_toml_str_requires_explicit_etcd_endpoints_when_backend_is_selected() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[metadata]
backend = "etcd""#,
        )
        .expect_err("etcd backend without explicit endpoints should be rejected");

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

    #[test]
    fn from_toml_str_reads_bootstrap_auth_tokens() {
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[auth]

[[auth.bootstrap_tokens]]
token = "operator-secret"

[auth.bootstrap_tokens.principal]
name = "ops-admin"
kind = "user"
access_tier = "operator"

[[auth.bootstrap_tokens]]
token = "service-secret"

[auth.bootstrap_tokens.principal]
name = "ingest-service"
kind = "service"
access_tier = "service""#,
        )
        .expect("config should load");

        assert_eq!(config.auth.bootstrap_tokens.len(), 2);
        assert_eq!(config.auth.bootstrap_tokens[0].token, "operator-secret");
        assert_eq!(config.auth.bootstrap_tokens[0].principal.name, "ops-admin");
        assert_eq!(config.auth.bootstrap_tokens[1].token, "service-secret");
        assert_eq!(
            config.auth.bootstrap_tokens[1].principal.name,
            "ingest-service"
        );
    }

    #[test]
    fn from_toml_str_rejects_duplicate_bootstrap_auth_tokens() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[auth]

[[auth.bootstrap_tokens]]
token = "duplicate-secret"

[auth.bootstrap_tokens.principal]
name = "ops-admin"
kind = "user"
access_tier = "operator"

[[auth.bootstrap_tokens]]
token = "duplicate-secret"

[auth.bootstrap_tokens.principal]
name = "other-admin"
kind = "user"
access_tier = "operator""#,
        )
        .expect_err("duplicate bootstrap tokens should fail");

        assert!(error.to_string().contains("auth.bootstrap_tokens"));
        assert!(
            !error.to_string().contains("duplicate-secret"),
            "duplicate token validation must not echo the secret token"
        );
    }

    #[test]
    fn from_toml_str_rejects_duplicate_bootstrap_principal_names() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[auth]

[[auth.bootstrap_tokens]]
token = "one-secret"

[auth.bootstrap_tokens.principal]
name = "shared-principal"
kind = "user"
access_tier = "operator"

[[auth.bootstrap_tokens]]
token = "two-secret"

[auth.bootstrap_tokens.principal]
name = "shared-principal"
kind = "user"
access_tier = "observer""#,
        )
        .expect_err("duplicate bootstrap principal names should fail");

        assert!(
            error.to_string().contains("duplicate principal names"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn from_toml_str_rejects_bootstrap_tokens_with_surrounding_whitespace() {
        let error = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"

[auth]

[[auth.bootstrap_tokens]]
token = " secret "

[auth.bootstrap_tokens.principal]
name = "ops-admin"
kind = "user"
access_tier = "operator""#,
        )
        .expect_err("tokens with surrounding whitespace should fail");

        assert!(
            error.to_string().contains("leading or trailing whitespace"),
            "unexpected error: {error}"
        );
    }
}
