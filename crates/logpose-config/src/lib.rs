//! Configuration loading for LogPose services and tooling.

use logpose_auth::Principal;
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, LogPoseError, MetadataBackend, MetadataConfig, NodeRole, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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
    /// Authentication bootstrap configuration.
    #[serde(default)]
    pub auth: AuthConfig,
    /// Request size limits for the REST and gRPC listeners.
    #[serde(default)]
    pub limits: LimitsConfig,
    /// Snapshot tokens: how long a pinned state lives and how many a collection holds.
    #[serde(default)]
    pub snapshots: SnapshotConfig,
    /// Vector index construction parameters for segments written by flush and compaction.
    #[serde(default)]
    pub index: IndexConfig,
    /// How long a stopping server waits for the requests in flight, in milliseconds, before it
    /// closes their connections and closes the storage engine anyway. `0` does not wait.
    #[serde(default = "default_drain_timeout_ms")]
    pub drain_timeout_ms: u64,
}

/// Default for [`LogPoseConfig::drain_timeout_ms`]: 20 seconds, which leaves a
/// 30-second stop deadline (the Kubernetes default grace period) time to close the engine.
pub const DEFAULT_DRAIN_TIMEOUT_MS: u64 = 20_000;

const fn default_drain_timeout_ms() -> u64 {
    DEFAULT_DRAIN_TIMEOUT_MS
}

/// Default for [`IndexConfig::hnsw_m`].
pub const DEFAULT_HNSW_M: usize = 16;
/// Default for [`IndexConfig::hnsw_ef_construction`].
pub const DEFAULT_HNSW_EF_CONSTRUCTION: usize = 128;
/// Largest accepted [`IndexConfig::hnsw_m`].
pub const MAX_HNSW_M: usize = 1024;
/// Largest accepted [`IndexConfig::hnsw_ef_construction`].
pub const MAX_HNSW_EF_CONSTRUCTION: usize = 4096;

/// HNSW graph construction parameters, applied to every collection on the node.
///
/// They affect only graphs built after a change: existing segments keep their graphs until
/// compaction rewrites them.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexConfig {
    /// Links per node on upper graph layers; layer 0 allows twice as many.
    pub hnsw_m: usize,
    /// Beam width used to find neighbor candidates while building a graph.
    pub hnsw_ef_construction: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            hnsw_m: DEFAULT_HNSW_M,
            hnsw_ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
        }
    }
}

impl IndexConfig {
    fn validate(&self) -> Result<()> {
        if !(2..=MAX_HNSW_M).contains(&self.hnsw_m) {
            return Err(LogPoseError::invalid_config(format!(
                "invalid LOGPOSE_CONFIG: index.hnsw_m must be 2 to {MAX_HNSW_M}"
            )));
        }
        if !(1..=MAX_HNSW_EF_CONSTRUCTION).contains(&self.hnsw_ef_construction) {
            return Err(LogPoseError::invalid_config(format!(
                "invalid LOGPOSE_CONFIG: index.hnsw_ef_construction must be 1 to {MAX_HNSW_EF_CONSTRUCTION}"
            )));
        }
        Ok(())
    }
}

/// Default for [`SnapshotConfig::token_ttl_ms`]: 5 minutes.
pub const DEFAULT_SNAPSHOT_TOKEN_TTL_MS: u64 = 5 * 60 * 1000;
/// Default for [`SnapshotConfig::max_tokens_per_collection`].
pub const DEFAULT_MAX_SNAPSHOT_TOKENS_PER_COLLECTION: usize = 64;

/// Snapshot token settings (engine plan decision D7).
///
/// A snapshot token pins one state of a collection for repeatable reads: a pinned query or
/// count returns one, and every scroll cursor carries one. A token expires `token_ttl_ms` after
/// its last use (each use extends it), after which reads through it, and scroll pages after it,
/// fail with `SNAPSHOT_EXPIRED`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotConfig {
    /// How long a token lives after its last use, in milliseconds.
    pub token_ttl_ms: u64,
    /// Most pinned states per collection; a pin past it fails with `TOO_MANY_SNAPSHOTS`.
    pub max_tokens_per_collection: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            token_ttl_ms: DEFAULT_SNAPSHOT_TOKEN_TTL_MS,
            max_tokens_per_collection: DEFAULT_MAX_SNAPSHOT_TOKENS_PER_COLLECTION,
        }
    }
}

impl SnapshotConfig {
    fn validate(&self) -> Result<()> {
        if self.token_ttl_ms == 0 {
            return Err(LogPoseError::invalid_config(
                "invalid LOGPOSE_CONFIG: snapshots.token_ttl_ms must be greater than 0",
            ));
        }
        if self.max_tokens_per_collection == 0 {
            return Err(LogPoseError::invalid_config(
                "invalid LOGPOSE_CONFIG: snapshots.max_tokens_per_collection must be greater than 0",
            ));
        }
        Ok(())
    }
}

/// Default for [`LimitsConfig::max_rest_body_bytes`]: 16 MiB.
pub const DEFAULT_MAX_REST_BODY_BYTES: usize = 16 * 1024 * 1024;
/// Default for [`LimitsConfig::max_grpc_message_bytes`]: 16 MiB.
pub const DEFAULT_MAX_GRPC_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Request and response size limits for the API listeners.
///
/// A REST body above its limit is rejected with HTTP 413 and a gRPC message above its limit
/// with `RESOURCE_EXHAUSTED`; both carry a `TOO_LARGE` error. The same limits bound the
/// responses of record reads (query, scroll, get), which fail the same way.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    /// Largest REST request body accepted, and largest record-read response sent, in bytes.
    pub max_rest_body_bytes: usize,
    /// Largest decoded gRPC request message accepted, and largest reply sent, in bytes. Each message of a
    /// `BulkWriteCollection` stream is one batch and is checked on its own.
    pub max_grpc_message_bytes: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_rest_body_bytes: DEFAULT_MAX_REST_BODY_BYTES,
            max_grpc_message_bytes: DEFAULT_MAX_GRPC_MESSAGE_BYTES,
        }
    }
}

impl LimitsConfig {
    fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("max_rest_body_bytes", self.max_rest_body_bytes),
            ("max_grpc_message_bytes", self.max_grpc_message_bytes),
        ] {
            if value == 0 {
                return Err(LogPoseError::invalid_config(format!(
                    "invalid LOGPOSE_CONFIG: limits.{name} must be greater than 0"
                )));
            }
        }
        Ok(())
    }
}

/// Authentication bootstrap and runtime configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Static bearer tokens bound to explicit principals for bootstrapping.
    #[serde(default)]
    pub bootstrap_tokens: Vec<BootstrapTokenConfig>,
}

impl AuthConfig {
    fn validate(&self) -> Result<()> {
        let mut seen_tokens = BTreeSet::new();
        let mut seen_principals = BTreeSet::new();
        for (index, token) in self.bootstrap_tokens.iter().enumerate() {
            token.validate().map_err(|message| {
                LogPoseError::invalid_config(format!(
                    "invalid LOGPOSE_CONFIG: auth.bootstrap_tokens[{index}] {message}"
                ))
            })?;
            if !seen_tokens.insert(token.token.clone()) {
                return Err(LogPoseError::invalid_config(
                    "invalid LOGPOSE_CONFIG: auth.bootstrap_tokens must not contain duplicate token values",
                ));
            }
            if !seen_principals.insert(token.principal.name.clone()) {
                return Err(LogPoseError::invalid_config(
                    "invalid LOGPOSE_CONFIG: auth.bootstrap_tokens must not contain duplicate principal names",
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
            rest_port: 8080,
            grpc_host: "127.0.0.1".to_owned(),
            grpc_port: 50051,
            log_filter: "info,logpose=debug".to_owned(),
            storage_root: PathBuf::from(".logpose"),
            metadata: MetadataConfig::default(),
            auth: AuthConfig::default(),
            limits: LimitsConfig::default(),
            snapshots: SnapshotConfig::default(),
            index: IndexConfig::default(),
            drain_timeout_ms: DEFAULT_DRAIN_TIMEOUT_MS,
        }
    }
}

impl LogPoseConfig {
    /// Validate configuration invariants that must hold before runtime bootstrap.
    pub fn validate(&self) -> Result<()> {
        if self.node_name == ANONYMOUS_LOCAL_NODE_NAME {
            return Err(LogPoseError::invalid_config(format!(
                "invalid LOGPOSE_CONFIG: node_name '{}' is reserved for anonymous local placement metadata",
                ANONYMOUS_LOCAL_NODE_NAME
            )));
        }
        if self.metadata.backend == MetadataBackend::Etcd {
            self.metadata.etcd.validate().map_err(|error| {
                LogPoseError::invalid_config(format!("invalid LOGPOSE_CONFIG: {error}"))
            })?;
        }
        self.auth.validate()?;
        self.limits.validate()?;
        self.snapshots.validate()?;
        self.index.validate()?;
        Ok(())
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml_str(value: &str) -> Result<Self> {
        let config: Self = toml::from_str(value).map_err(|error| {
            LogPoseError::invalid_config(format!("invalid LOGPOSE_CONFIG: {error}"))
        })?;
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
    fn request_limits_default_to_sixteen_mebibytes_and_parse_from_toml() {
        assert_eq!(
            LogPoseConfig::default().limits,
            LimitsConfig {
                max_rest_body_bytes: 16 * 1024 * 1024,
                max_grpc_message_bytes: 16 * 1024 * 1024,
            }
        );
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "127.0.0.1"
rest_port = 8080
grpc_host = "127.0.0.1"
grpc_port = 50051
log_filter = "info"
storage_root = ".logpose"

[limits]
max_rest_body_bytes = 1024
"#,
        )
        .expect("limits should parse");
        assert_eq!(config.limits.max_rest_body_bytes, 1024);
        assert_eq!(
            config.limits.max_grpc_message_bytes,
            DEFAULT_MAX_GRPC_MESSAGE_BYTES
        );
    }

    #[test]
    fn snapshot_settings_default_to_five_minutes_and_parse_from_toml() {
        assert_eq!(
            LogPoseConfig::default().snapshots,
            SnapshotConfig {
                token_ttl_ms: 300_000,
                max_tokens_per_collection: 64,
            }
        );
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "127.0.0.1"
rest_port = 8080
grpc_host = "127.0.0.1"
grpc_port = 50051
log_filter = "info"
storage_root = ".logpose"

[snapshots]
token_ttl_ms = 1500
"#,
        )
        .expect("snapshot settings should parse");
        assert_eq!(config.snapshots.token_ttl_ms, 1500);
        assert_eq!(config.snapshots.max_tokens_per_collection, 64);
        let mut config = LogPoseConfig::default();
        config.snapshots.token_ttl_ms = 0;
        let error = config.validate().expect_err("a zero TTL is invalid");
        assert!(error.to_string().contains("snapshots.token_ttl_ms"));
    }

    #[test]
    fn index_settings_default_to_m16_and_parse_from_toml() {
        assert_eq!(
            LogPoseConfig::default().index,
            IndexConfig {
                hnsw_m: 16,
                hnsw_ef_construction: 128,
            }
        );
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "127.0.0.1"
rest_port = 8080
grpc_host = "127.0.0.1"
grpc_port = 50051
log_filter = "info"
storage_root = ".logpose"

[index]
hnsw_ef_construction = 200
"#,
        )
        .expect("index settings should parse");
        assert_eq!(config.index.hnsw_m, 16);
        assert_eq!(config.index.hnsw_ef_construction, 200);
    }

    #[test]
    fn rejects_out_of_range_index_settings() {
        let mut config = LogPoseConfig::default();
        config.index.hnsw_m = 1;
        let error = config.validate().expect_err("m below 2 is invalid");
        assert!(error.to_string().contains("index.hnsw_m"));
        let mut config = LogPoseConfig::default();
        config.index.hnsw_ef_construction = 0;
        let error = config.validate().expect_err("a zero beam width is invalid");
        assert!(error.to_string().contains("index.hnsw_ef_construction"));
    }

    #[test]
    fn rejects_zero_request_limits() {
        let mut config = LogPoseConfig::default();
        config.limits.max_grpc_message_bytes = 0;
        let error = config.validate().expect_err("a zero limit is invalid");
        assert!(matches!(error, LogPoseError::InvalidConfig { .. }));
        assert!(error.to_string().contains("limits.max_grpc_message_bytes"));
    }

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
        assert_eq!(config.drain_timeout_ms, DEFAULT_DRAIN_TIMEOUT_MS);
    }

    #[test]
    fn from_toml_str_reads_drain_timeout() {
        let config = LogPoseConfig::from_toml_str(
            r#"node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 18080
grpc_host = "0.0.0.0"
grpc_port = 15051
log_filter = "info"
storage_root = "tmp/logpose-data"
drain_timeout_ms = 0"#,
        )
        .expect("config should load");
        assert_eq!(config.drain_timeout_ms, 0);
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
    fn from_toml_str_rejects_blank_etcd_cluster_name() {
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
rest_port = 18080
grpc_host = "0.0.0.0"
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
