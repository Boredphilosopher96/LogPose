//! Shared service lifecycle types.

use logpose_config::LogPoseConfig;
use logpose_service::LogPoseDataService;
use logpose_types::BuildInfo;
use serde::Serialize;
use std::sync::Arc;

/// Top-level state shared by transport layers and tools.
#[derive(Clone, Debug, Serialize)]
pub struct AppState {
    /// Effective runtime configuration.
    pub config: LogPoseConfig,
    /// Build metadata exposed through APIs and diagnostics.
    pub build: BuildInfo,
    /// Shared application data service used by transport layers.
    #[serde(skip_serializing)]
    pub service: Arc<LogPoseDataService>,
}

impl AppState {
    /// Construct shared state from configuration.
    #[must_use]
    pub fn new(config: LogPoseConfig) -> Self {
        Self {
            service: Arc::new(LogPoseDataService::local(&config.storage_root)),
            config,
            build: BuildInfo::current(),
        }
    }
}
