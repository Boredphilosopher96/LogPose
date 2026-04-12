//! LogPose server entrypoint.

use anyhow::Context;
use logpose_config::LogPoseConfig;
use logpose_core::AppState;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = LogPoseConfig::load().context("failed to load configuration")?;
    logpose_telemetry::init(&config.log_filter);

    let state = Arc::new(AppState::new(config));
    info!(node = %state.config.node_name, "starting LogPose server");

    let rest_state = Arc::clone(&state);
    let grpc_state = Arc::clone(&state);

    tokio::try_join!(
        async move {
            logpose_api_rest::serve(rest_state)
                .await
                .map_err(anyhow::Error::from)
        },
        async move {
            logpose_api_grpc::serve(grpc_state)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))
        }
    )?;

    Ok(())
}
