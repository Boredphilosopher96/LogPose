//! Shared service lifecycle types.

use logpose_config::LogPoseConfig;
use logpose_types::BuildInfo;
use serde::Serialize;

/// Top-level state shared by transport layers and tools.
#[derive(Clone, Debug, Serialize)]
pub struct AppState {
    /// Effective runtime configuration.
    pub config: LogPoseConfig,
    /// Build metadata exposed through APIs and diagnostics.
    pub build: BuildInfo,
}

impl AppState {
    /// Construct shared state from configuration.
    #[must_use]
    pub fn new(config: LogPoseConfig) -> Self {
        Self {
            config,
            build: BuildInfo::current(),
        }
    }
}
