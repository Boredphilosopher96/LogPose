//! Shared domain types for LogPose.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Common result type for workspace crates.
pub type Result<T> = std::result::Result<T, LogPoseError>;

/// Top-level workspace error.
#[derive(Debug, Error)]
pub enum LogPoseError {
    /// Generic bootstrap and configuration errors.
    #[error("{0}")]
    Message(String),
}

/// Build metadata surfaced by service entrypoints.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildInfo {
    /// Semantic version for the distribution.
    pub version: String,
    /// Source control revision when available.
    pub git_sha: String,
    /// Build profile used for compilation.
    pub profile: String,
}

impl BuildInfo {
    /// Create build metadata from compile-time environment values.
    #[must_use]
    pub fn current() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            git_sha: option_env!("LOGPOSE_GIT_SHA")
                .unwrap_or("development")
                .to_owned(),
            profile: option_env!("PROFILE").unwrap_or("debug").to_owned(),
        }
    }
}

/// Identifier for a collection or namespace.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ResourceId(pub Uuid);

impl Default for ResourceId {
    fn default() -> Self {
        Self(Uuid::new_v4())
    }
}
