//! REST API surface for LogPose.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use logpose_core::AppState;
use logpose_query::{ExplainMode, MetadataFilter, Predicate, QueryRequest, ScalarMetadataValue};
use logpose_service::ServiceError;
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{DistanceMetric, Snapshot, WriteOperation};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{net::SocketAddr, sync::Arc};
use tower_http::trace::TraceLayer;

/// Create the versioned REST router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/metadata", get(metadata))
        .route("/v1/collections", post(create_collection))
        .route("/v1/collections/{name}", get(get_collection))
        .route("/v1/collections/{name}/writes", post(write_collection))
        .route("/v1/collections/{name}/query", post(query_collection))
        .route("/v1/collections/{name}/stats", get(get_collection_stats))
        .route("/v1/collections/{name}/flush", post(flush_collection))
        .route("/v1/collections/{name}/compact", post(compact_collection))
        .route("/v1/collections/{name}/inspect", get(inspect_collection))
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
    Json(state.metadata())
}

async fn create_collection(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateCollectionBody>,
) -> Result<(StatusCode, Json<logpose_catalog::CollectionDescriptor>), ApiError> {
    let descriptor = state
        .service
        .create_collection(CreateCollectionRequest {
            name: request.name,
            dimensions: request.dimensions,
            metric: request.metric,
        })
        .await?;
    Ok((StatusCode::CREATED, Json(descriptor)))
}

async fn get_collection(
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_catalog::CollectionDescriptor>, ApiError> {
    Ok(Json(state.service.get_collection(&name).await?))
}

async fn write_collection(
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(request): Json<WriteCollectionBody>,
) -> Result<Json<logpose_types::CommitAck>, ApiError> {
    Ok(Json(state.service.write(&name, request.operations).await?))
}

async fn query_collection(
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(request): Json<QueryCollectionBody>,
) -> Result<Json<logpose_query::QueryResponse>, ApiError> {
    let filters = request
        .filters
        .into_iter()
        .map(|(field, value)| {
            ScalarMetadataValue::from_json(&value)
                .map(|value| MetadataFilter { field, value })
                .ok_or_else(|| {
                    ApiError(ServiceError::InvalidArgument(
                        "query filters must contain only scalar JSON values".to_owned(),
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(
        state
            .service
            .query(QueryRequest {
                collection_name: name,
                vector: request.vector,
                top_k: request.top_k,
                snapshot: request.snapshot,
                filters,
                predicate: request.predicate,
                explain: request.explain,
            })
            .await?,
    ))
}

async fn get_collection_stats(
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_types::CollectionStats>, ApiError> {
    Ok(Json(state.service.stats(&name).await?))
}

async fn flush_collection(
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_types::Snapshot>, ApiError> {
    Ok(Json(state.service.flush(&name).await?))
}

async fn compact_collection(
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_types::Snapshot>, ApiError> {
    Ok(Json(state.service.compact(&name).await?))
}

async fn inspect_collection(
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<InspectCollectionParams>,
) -> Result<Json<logpose_storage::InspectReport>, ApiError> {
    let target = inspect_target_from_params(params)?;
    Ok(Json(state.service.inspect(&name, target).await?))
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct CreateCollectionBody {
    name: String,
    dimensions: usize,
    metric: DistanceMetric,
}

#[derive(Debug, Deserialize)]
struct WriteCollectionBody {
    operations: Vec<WriteOperation>,
}

#[derive(Debug, Deserialize)]
struct QueryCollectionBody {
    vector: Vec<f32>,
    top_k: usize,
    #[serde(default)]
    snapshot: Option<Snapshot>,
    #[serde(default)]
    filters: Map<String, Value>,
    #[serde(default)]
    predicate: Option<Predicate>,
    #[serde(default)]
    explain: ExplainMode,
}

#[derive(Debug, Deserialize)]
struct InspectCollectionParams {
    target: Option<String>,
    segment_id: Option<String>,
}

#[derive(Debug)]
struct ApiError(ServiceError);

impl From<ServiceError> for ApiError {
    fn from(error: ServiceError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self.0 {
            ServiceError::AlreadyExists(message) => (StatusCode::CONFLICT, message),
            ServiceError::NotFound(message) => (StatusCode::NOT_FOUND, message),
            ServiceError::InvalidArgument(message) => (StatusCode::BAD_REQUEST, message),
            ServiceError::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

fn inspect_target_from_params(params: InspectCollectionParams) -> Result<InspectTarget, ApiError> {
    match params.target.as_deref().unwrap_or("manifest") {
        "manifest" => Ok(InspectTarget::Manifest),
        "wal" => Ok(InspectTarget::Wal),
        "segment" => params
            .segment_id
            .filter(|segment_id| !segment_id.is_empty())
            .map(InspectTarget::Segment)
            .ok_or_else(|| {
                ApiError(ServiceError::InvalidArgument(
                    "inspect target 'segment' requires segment_id".to_owned(),
                ))
            }),
        "maintenance" => Ok(InspectTarget::Maintenance),
        other => Err(ApiError(ServiceError::InvalidArgument(format!(
            "unsupported inspect target '{other}'"
        )))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use http_body_util::BodyExt;
    use logpose_config::LogPoseConfig;
    use serde_json::{Value, json};
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
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

    #[tokio::test]
    async fn data_endpoints_run_the_collection_workflow() {
        let state = Arc::new(AppState::new(test_config("rest-workflow")));
        let app = router(state);

        let create = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 2,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create.status(), StatusCode::CREATED);

        let write = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/writes")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "operations": [
                                {
                                    "op": "put",
                                    "id": "alpha",
                                    "vector": [1.0, 0.0],
                                    "metadata": {"kind": "keep", "color": "red"}
                                },
                                {
                                    "op": "put",
                                    "id": "beta",
                                    "vector": [3.0, 0.0],
                                    "metadata": {"kind": "drop", "color": "blue"}
                                },
                                {
                                    "op": "put",
                                    "id": "gamma",
                                    "vector": [2.0, 0.0],
                                    "metadata": {"kind": "keep", "color": "red"}
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(write.status(), StatusCode::OK);

        let query = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "vector": [1.0, 0.0],
                            "top_k": 3,
                            "filters": {"kind": "keep"}
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(query.status(), StatusCode::OK);
        let query_body = json_body(query).await;
        assert_eq!(
            query_body["matches"]
                .as_array()
                .expect("matches should be an array")
                .iter()
                .map(|candidate| candidate["id"].as_str().expect("id should be a string"))
                .collect::<Vec<_>>(),
            vec!["gamma", "alpha"]
        );

        let stats = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/stats")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(stats.status(), StatusCode::OK);
        let stats_body = json_body(stats).await;
        assert_eq!(stats_body["live_record_count"], 3);
        assert_eq!(stats_body["deleted_record_count"], 0);
        assert_eq!(stats_body["mutable_op_count"], 3);
        assert_eq!(stats_body["segment_count"], 0);

        let wal = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/inspect?target=wal")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(wal.status(), StatusCode::OK);
        let wal_body = json_body(wal).await;
        assert_eq!(wal_body["target"], "wal");
        assert_eq!(
            wal_body["payload"]["records"]
                .as_array()
                .expect("wal records should be an array")
                .len(),
            3
        );

        let flush = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/flush")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(flush.status(), StatusCode::OK);

        let compact = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/compact")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(compact.status(), StatusCode::OK);

        let inspect = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/inspect?target=manifest")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(inspect.status(), StatusCode::OK);
        let inspect_body = json_body(inspect).await;
        assert_eq!(inspect_body["target"], "manifest");
        let segment_id = inspect_body["payload"]["segments"][0]["segment_id"]
            .as_str()
            .expect("segment id should be a string")
            .to_owned();

        let segment = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/v1/collections/documents/inspect?target=segment&segment_id={segment_id}"
                    ))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(segment.status(), StatusCode::OK);
        let segment_body = json_body(segment).await;
        assert_eq!(
            segment_body["target"]
                .as_str()
                .expect("segment target should be a string"),
            format!("segment:{segment_id}")
        );
        assert_eq!(
            segment_body["payload"]["records"]
                .as_array()
                .expect("segment records should be an array")
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn inspect_endpoints_support_wal_and_segment_targets_after_flush() {
        let state = Arc::new(AppState::new(test_config("rest-inspect-targets")));
        let app = router(state);

        let create = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 2,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create.status(), StatusCode::CREATED);

        app.clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/writes")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "operations": [
                                {
                                    "op": "put",
                                    "id": "alpha",
                                    "vector": [1.0, 0.0],
                                    "metadata": {"kind": "keep"}
                                },
                                {
                                    "op": "put",
                                    "id": "beta",
                                    "vector": [0.0, 1.0],
                                    "metadata": {"kind": "drop"}
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        app.clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/flush")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        let manifest = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/inspect?target=manifest")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        let manifest_body = json_body(manifest).await;
        let segment_id = manifest_body["payload"]["segments"][0]["segment_id"]
            .as_str()
            .expect("segment id should be a string")
            .to_owned();

        let wal = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/inspect?target=wal")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(json_body(wal).await["target"], "wal");

        let segment = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/v1/collections/documents/inspect?target=segment&segment_id={segment_id}"
                    ))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        let segment_body = json_body(segment).await;
        assert_eq!(
            segment_body["target"]
                .as_str()
                .expect("segment target should be a string"),
            format!("segment:{segment_id}")
        );
    }

    #[tokio::test]
    async fn metadata_endpoint_reports_build_identity_fields() {
        let state = Arc::new(AppState::new(test_config("rest-metadata")));
        let app = router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/metadata")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["product"], "LogPose");
        assert_eq!(body["node_name"], "rest-metadata");
        assert_eq!(body["profile"], "debug");
        assert!(
            body["version"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert!(
            body["git_sha"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
    }

    #[tokio::test]
    async fn missing_collection_returns_not_found() {
        let state = Arc::new(AppState::new(test_config("rest-missing")));
        let app = router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/missing")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn segment_inspect_rejects_empty_segment_id() {
        let state = Arc::new(AppState::new(test_config("rest-empty-segment")));
        let app = router(state.clone());

        let create = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 2,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/inspect?target=segment&segment_id=")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn query_filters_preserve_large_integer_precision() {
        let state = Arc::new(AppState::new(test_config("rest-large-integers")));
        let app = router(state);

        let create = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 2,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create.status(), StatusCode::CREATED);

        let write = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/writes")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "operations": [
                                {
                                    "op": "put",
                                    "id": "lower",
                                    "vector": [1.0, 0.0],
                                    "metadata": { "score": 9007199254740992u64 }
                                },
                                {
                                    "op": "put",
                                    "id": "higher",
                                    "vector": [2.0, 0.0],
                                    "metadata": { "score": 9007199254740993u64 }
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(write.status(), StatusCode::OK);

        let query = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "vector": [1.0, 0.0],
                            "top_k": 5,
                            "filters": { "score": 9007199254740993u64 }
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(query.status(), StatusCode::OK);
        let query_body = json_body(query).await;
        assert_eq!(
            query_body["matches"]
                .as_array()
                .expect("matches should be an array")
                .iter()
                .map(|candidate| candidate["id"].as_str().expect("id should be a string"))
                .collect::<Vec<_>>(),
            vec!["higher"]
        );
    }

    #[tokio::test]
    async fn query_accepts_predicate_and_profile_diagnostics() {
        let state = Arc::new(AppState::new(test_config("rest-predicate-profile")));
        let app = router(state);

        let create = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 2,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create.status(), StatusCode::CREATED);

        let write = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/writes")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "operations": [
                                {
                                    "op": "put",
                                    "id": "alpha",
                                    "vector": [1.0, 0.0],
                                    "metadata": {"kind": "keep", "version": 1}
                                },
                                {
                                    "op": "put",
                                    "id": "beta",
                                    "vector": [2.0, 0.0],
                                    "metadata": {"kind": "drop", "version": 2}
                                },
                                {
                                    "op": "put",
                                    "id": "gamma",
                                    "vector": [3.0, 0.0],
                                    "metadata": {"kind": "drop", "version": 3}
                                },
                                {
                                    "op": "put",
                                    "id": "delta",
                                    "vector": [4.0, 0.0],
                                    "metadata": {"kind": "drop", "version": 4}
                                },
                                {
                                    "op": "put",
                                    "id": "epsilon",
                                    "vector": [5.0, 0.0],
                                    "metadata": {"kind": "keep", "version": 5}
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(write.status(), StatusCode::OK);

        let query = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "vector": [1.0, 0.0],
                            "top_k": 1,
                            "predicate": {
                                "kind": "comparison",
                                "field": "kind",
                                "operator": "eq",
                                "value": "keep"
                            },
                            "explain": "profile"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(query.status(), StatusCode::OK);
        let query_body = json_body(query).await;
        assert_eq!(
            query_body["matches"]
                .as_array()
                .expect("matches should be an array")
                .iter()
                .map(|candidate| candidate["id"].as_str().expect("id should be a string"))
                .collect::<Vec<_>>(),
            vec!["epsilon"]
        );
        assert_eq!(
            query_body["diagnostics"]["chosen_plan"],
            "predicate_first_exact"
        );
        assert!(
            query_body["diagnostics"]["stage_timings"]["planning_micros"]
                .as_u64()
                .is_some(),
            "profile mode should include stage timings"
        );
    }

    #[tokio::test]
    async fn inspect_supports_maintenance_target() {
        let state = Arc::new(AppState::new(test_config("rest-maintenance")));
        let app = router(state);

        let create = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 2,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create.status(), StatusCode::CREATED);

        let inspect = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/inspect?target=maintenance")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(inspect.status(), StatusCode::OK);
        let inspect_body = json_body(inspect).await;
        assert_eq!(inspect_body["target"], "maintenance");
        assert!(inspect_body["payload"]["last_error"].is_null());
    }

    #[tokio::test]
    async fn query_rejects_malformed_predicates() {
        let state = Arc::new(AppState::new(test_config("rest-invalid-predicate")));
        let app = router(state);

        let create = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 2,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create.status(), StatusCode::CREATED);

        let query = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "vector": [1.0, 0.0],
                            "top_k": 1,
                            "predicate": {
                                "kind": "comparison",
                                "field": "kind",
                                "operator": "eq"
                            }
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(query.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn query_rejects_empty_logical_predicates() {
        let state = Arc::new(AppState::new(test_config("rest-empty-logical-predicate")));
        let app = router(state);

        let create = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 2,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create.status(), StatusCode::CREATED);

        let query = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "vector": [1.0, 0.0],
                            "top_k": 1,
                            "predicate": {
                                "kind": "and",
                                "children": []
                            }
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(query.status(), StatusCode::BAD_REQUEST);
    }

    async fn json_body(response: axum::response::Response) -> Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("body should be valid json")
    }

    fn test_config(label: &str) -> LogPoseConfig {
        LogPoseConfig {
            node_name: label.to_owned(),
            storage_root: unique_temp_dir(label),
            ..LogPoseConfig::default()
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("logpose-api-rest-{label}-{suffix}"));
        fs::create_dir_all(&path).expect("temp dir should be created");
        path
    }
}
