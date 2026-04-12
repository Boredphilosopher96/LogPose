//! REST API surface for LogPose.

use axum::{Json, Router, extract::State, response::IntoResponse, routing::get};
use logpose_core::AppState;
use serde::Serialize;
use std::{net::SocketAddr, sync::Arc};
use tower_http::trace::TraceLayer;

/// Create the versioned REST router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/metadata", get(metadata))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

/// Serve the REST API until shutdown.
pub async fn serve(state: Arc<AppState>) -> Result<(), std::io::Error> {
    let address = SocketAddr::from((
        state
            .config
            .rest_host
            .parse::<std::net::IpAddr>()
            .map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
            })?,
        state.config.rest_port,
    ));

    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, router(state)).await
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

async fn metadata(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(MetadataResponse {
        product: "LogPose",
        node_name: state.config.node_name.clone(),
        version: state.build.version.clone(),
        profile: state.build.profile.clone(),
    })
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct MetadataResponse {
    product: &'static str,
    node_name: String,
    version: String,
    profile: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use logpose_config::LogPoseConfig;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let state = Arc::new(AppState::new(LogPoseConfig::default()));
        let app = router(state);
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(axum::body::Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
