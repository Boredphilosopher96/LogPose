//! LogPose server entrypoint.

use anyhow::Context;
use logpose_config::LogPoseConfig;
use logpose_core::AppState;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

// Used by the process-level tests in `tests/`, not by the binary's own unit tests.
#[cfg(test)]
use tempfile as _;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = LogPoseConfig::load().context("failed to load configuration")?;
    logpose_telemetry::init(&config.log_filter);

    let state = Arc::new(AppState::try_new(config).context("failed to start LogPose server")?);
    info!(node = %state.config.node_name, "starting LogPose server");

    let (stop, stopping) = watch::channel(false);
    tokio::spawn(async move {
        let signal = shutdown_signal().await;
        info!(
            signal,
            "shutting down: refusing new requests and finishing the ones in flight"
        );
        let _ = stop.send(true);
        let signal = shutdown_signal().await;
        warn!(
            signal,
            "second shutdown signal: exiting without a clean shutdown"
        );
        std::process::exit(1);
    });

    let rest_state = Arc::clone(&state);
    let grpc_state = Arc::clone(&state);
    let rest_stopping = stopping.clone();
    let served = tokio::try_join!(
        async move {
            logpose_api_rest::serve_until(rest_state, stop_requested(rest_stopping))
                .await
                .map_err(anyhow::Error::from)
        },
        async move {
            logpose_api_grpc::serve_until(grpc_state, stop_requested(stopping))
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))
        }
    );

    // Dropping the last reference closes the storage engine: running maintenance stops, queued
    // jobs stay recorded for the next start, and the storage root lock is released. The close
    // blocks until the engine's tasks finish, so it runs off the async worker threads.
    tokio::task::spawn_blocking(move || drop(state))
        .await
        .context("failed to close the storage engine")?;
    served?;
    info!("LogPose server stopped");
    Ok(())
}

/// Completes once a shutdown signal has been received.
async fn stop_requested(mut stopping: watch::Receiver<bool>) {
    // An error means the signal task is gone, which only happens as the process exits.
    let _ = stopping.wait_for(|stop| *stop).await;
}

/// Waits for SIGINT (Ctrl-C) or, on Unix, SIGTERM, and names the signal received.
async fn shutdown_signal() -> &'static str {
    let interrupt = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
        "SIGINT"
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
        "SIGTERM"
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<&'static str>();
    tokio::select! {
        signal = interrupt => signal,
        signal = terminate => signal,
    }
}
