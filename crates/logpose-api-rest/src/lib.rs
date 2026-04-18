//! REST API surface for LogPose.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use logpose_auth::DatabaseAccessPolicy;
use logpose_catalog::DatabaseDescriptor;
use logpose_core::{AppState, RequestAuth};
use logpose_query::{ExplainMode, MetadataFilter, Predicate, QueryRequest, ScalarMetadataValue};
use logpose_service::ServiceError;
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{
    CollectionRef, DEFAULT_DATABASE_NAME, DistanceMetric, Snapshot, WriteOperation,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{net::SocketAddr, sync::Arc};
use tower_http::trace::TraceLayer;

/// Create the versioned REST router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/metadata", get(metadata))
        .route("/v1/runtime/status", get(runtime_status))
        .route("/v1/databases", get(list_databases))
        .route("/v1/databases/{name}", get(get_database).put(put_database))
        .route("/v1/collections", post(create_collection))
        .route(
            "/v1/databases/{name}/policy",
            get(get_database_policy).put(put_database_policy),
        )
        .route("/v1/collections/{name}", get(get_collection))
        .route(
            "/v1/collections/{name}/placement",
            get(get_collection_placement),
        )
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
    serve_with_listener(state, listener).await
}

/// Serve the REST API over an existing listener.
pub async fn serve_with_listener(
    state: Arc<AppState>,
    listener: tokio::net::TcpListener,
) -> Result<(), std::io::Error> {
    axum::serve(listener, router(state)).await
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

async fn metadata(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.metadata())
}

async fn runtime_status(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_types::NodeRuntimeStatus>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(state.runtime_status_with_auth(&auth).await?))
}

async fn put_database(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(descriptor): Json<DatabaseDescriptor>,
) -> Result<Json<DatabaseDescriptor>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    validate_database_scope(&descriptor, &name)?;
    Ok(Json(state.put_database_with_auth(&auth, descriptor).await?))
}

async fn get_database(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DatabaseDescriptor>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(state.database_with_auth(&auth, &name).await?))
}

async fn list_databases(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<DatabaseDescriptor>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(state.databases_with_auth(&auth).await?))
}

async fn create_collection(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateCollectionBody>,
) -> Result<(StatusCode, Json<logpose_catalog::CollectionDescriptor>), ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let descriptor = state
        .create_collection_with_auth(
            &auth,
            CreateCollectionRequest::in_database(
                default_database_if_blank(request.database_name),
                request.name,
                request.dimensions,
                request.metric,
            ),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(descriptor)))
}

async fn put_database_policy(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(policy): Json<DatabaseAccessPolicy>,
) -> Result<Json<DatabaseAccessPolicy>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    validate_policy_scope(&policy, &name)?;
    Ok(Json(
        state
            .set_database_access_policy_with_auth(&auth, policy)
            .await?,
    ))
}

async fn get_database_policy(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DatabaseAccessPolicy>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(
        state.database_access_policy_with_auth(&auth, &name).await?,
    ))
}

async fn get_collection(
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(namespace): Query<NamespaceQuery>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_catalog::CollectionDescriptor>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let collection = namespace.collection(name);
    Ok(Json(
        state
            .get_collection_with_auth(&auth, &collection_lookup_key(&collection))
            .await?,
    ))
}

async fn get_collection_placement(
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(namespace): Query<NamespaceQuery>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_types::CollectionPlacement>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let collection = namespace.collection(name);
    Ok(Json(
        state
            .collection_placement_with_auth(&auth, &collection_lookup_key(&collection))
            .await?,
    ))
}

async fn write_collection(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(request): Json<WriteCollectionBody>,
) -> Result<Json<CollectionScopedResponse<logpose_types::CommitAck>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    for operation in &request.operations {
        if operation.id().as_str().is_empty() {
            return Err(ApiError(ServiceError::InvalidArgument(
                "write operation record id must not be empty".to_owned(),
            )));
        }
    }
    let collection = request.collection(name);
    let ack = state
        .write_with_auth(
            &auth,
            &collection_lookup_key(&collection),
            request.operations,
        )
        .await?;
    Ok(Json(CollectionScopedResponse::new(collection, ack)))
}

async fn query_collection(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(request): Json<QueryCollectionBody>,
) -> Result<Json<CollectionScopedResponse<logpose_query::QueryResponse>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    if request.top_k == 0 {
        return Err(ApiError(ServiceError::InvalidArgument(
            "top_k must be greater than 0".to_owned(),
        )));
    }

    let collection = request.collection(name);
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

    let response = state
        .query_with_auth(
            &auth,
            QueryRequest {
                collection_name: collection_lookup_key(&collection),
                vector: request.vector,
                top_k: request.top_k,
                snapshot: request.snapshot,
                filters,
                predicate: request.predicate,
                explain: request.explain,
            },
        )
        .await?;

    Ok(Json(CollectionScopedResponse::new(collection, response)))
}

async fn get_collection_stats(
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(namespace): Query<NamespaceQuery>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_types::CollectionStats>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let collection = namespace.collection(name);
    Ok(Json(
        state
            .stats_with_auth(&auth, &collection_lookup_key(&collection))
            .await?,
    ))
}

async fn flush_collection(
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(namespace): Query<NamespaceQuery>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CollectionScopedResponse<logpose_types::Snapshot>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let collection = namespace.collection(name);
    let snapshot = state
        .flush_with_auth(&auth, &collection_lookup_key(&collection))
        .await?;
    Ok(Json(CollectionScopedResponse::new(collection, snapshot)))
}

async fn compact_collection(
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(namespace): Query<NamespaceQuery>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CollectionScopedResponse<logpose_types::Snapshot>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let collection = namespace.collection(name);
    let snapshot = state
        .compact_with_auth(&auth, &collection_lookup_key(&collection))
        .await?;
    Ok(Json(CollectionScopedResponse::new(collection, snapshot)))
}

async fn inspect_collection(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<InspectCollectionParams>,
) -> Result<Json<CollectionScopedResponse<logpose_storage::InspectReport>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let (target, namespace) = inspect_target_from_params(params)?;
    let collection = namespace.collection(name);
    let report = state
        .inspect_with_auth(&auth, &collection_lookup_key(&collection), target)
        .await?;
    Ok(Json(CollectionScopedResponse::new(collection, report)))
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct CreateCollectionBody {
    #[serde(default = "default_database_name")]
    database_name: String,
    name: String,
    dimensions: usize,
    metric: DistanceMetric,
}

#[derive(Debug, Deserialize)]
struct NamespaceQuery {
    #[serde(
        default = "default_database_name",
        alias = "database_name",
        rename = "database"
    )]
    database: String,
}

impl NamespaceQuery {
    fn collection(&self, name: impl Into<String>) -> CollectionRef {
        CollectionRef::new(
            default_database_if_blank(self.database.clone()),
            name.into(),
        )
    }
}

fn validate_policy_scope(
    policy: &DatabaseAccessPolicy,
    database_name: &str,
) -> Result<(), ApiError> {
    if policy.database_name != database_name {
        return Err(ApiError(ServiceError::InvalidArgument(format!(
            "database policy database_name '{}' does not match request database '{}'",
            policy.database_name, database_name
        ))));
    }
    Ok(())
}

fn validate_database_scope(
    descriptor: &DatabaseDescriptor,
    database_name: &str,
) -> Result<(), ApiError> {
    if descriptor.name != database_name {
        return Err(ApiError(ServiceError::InvalidArgument(format!(
            "database descriptor name '{}' does not match request database '{}'",
            descriptor.name, database_name
        ))));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct WriteCollectionBody {
    #[serde(default = "default_database_name")]
    database_name: String,
    operations: Vec<WriteOperation>,
}

impl WriteCollectionBody {
    fn collection(&self, name: impl Into<String>) -> CollectionRef {
        CollectionRef::new(
            default_database_if_blank(self.database_name.clone()),
            name.into(),
        )
    }
}

#[derive(Debug, Deserialize)]
struct QueryCollectionBody {
    #[serde(default = "default_database_name")]
    database_name: String,
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

impl QueryCollectionBody {
    fn collection(&self, name: impl Into<String>) -> CollectionRef {
        CollectionRef::new(
            default_database_if_blank(self.database_name.clone()),
            name.into(),
        )
    }
}

#[derive(Debug, Deserialize)]
struct InspectCollectionParams {
    #[serde(
        default = "default_database_name",
        alias = "database_name",
        rename = "database"
    )]
    database: String,
    target: Option<String>,
    segment_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct CollectionScopedResponse<T> {
    database_name: String,
    collection_name: String,
    #[serde(flatten)]
    response: T,
}

impl<T> CollectionScopedResponse<T> {
    fn new(collection: CollectionRef, response: T) -> Self {
        Self {
            database_name: collection.database_name,
            collection_name: collection.collection_name,
            response,
        }
    }
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
            ServiceError::Unauthenticated(message) => (StatusCode::UNAUTHORIZED, message),
            ServiceError::PermissionDenied(message) => (StatusCode::FORBIDDEN, message),
            ServiceError::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

fn request_auth_from_headers(headers: &HeaderMap) -> Result<RequestAuth, ApiError> {
    let value = match headers.get(AUTHORIZATION) {
        Some(value) => value,
        None => return Ok(RequestAuth::default()),
    };
    let value = value.to_str().map_err(|_| {
        ApiError(ServiceError::Unauthenticated(
            "authorization header must be valid ASCII".to_owned(),
        ))
    })?;
    let (scheme, token) = value.split_once(' ').ok_or_else(|| {
        ApiError(ServiceError::Unauthenticated(
            "authorization header must use the Bearer scheme".to_owned(),
        ))
    })?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.trim().is_empty() {
        return Err(ApiError(ServiceError::Unauthenticated(
            "authorization header must use the Bearer scheme".to_owned(),
        )));
    }
    Ok(RequestAuth::bearer_token(token.trim()))
}

fn inspect_target_from_params(
    params: InspectCollectionParams,
) -> Result<(InspectTarget, NamespaceQuery), ApiError> {
    let namespace = NamespaceQuery {
        database: default_database_if_blank(params.database),
    };
    let target = match params.target.as_deref().unwrap_or("manifest") {
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
    }?;
    Ok((target, namespace))
}

fn default_database_name() -> String {
    DEFAULT_DATABASE_NAME.to_owned()
}

fn default_database_if_blank(value: String) -> String {
    if value.trim().is_empty() {
        DEFAULT_DATABASE_NAME.to_owned()
    } else {
        value
    }
}

fn collection_lookup_key(collection: &CollectionRef) -> String {
    collection.lookup_name()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use http_body_util::BodyExt;
    use logpose_auth::{
        AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
        Principal, PrincipalKind,
    };
    use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
    use logpose_query::{QueryDiagnostics, QueryPlanKind, QueryResponse, QueryStageTimings};
    use logpose_types::RecordId;
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tower::util::ServiceExt;

    #[test]
    fn query_response_serializes_ann_diagnostics_fields() {
        let payload = serde_json::to_value(QueryResponse {
            metric: DistanceMetric::Dot,
            top_k: 2,
            returned: 1,
            snapshot: Snapshot {
                manifest_generation: 7,
                visible_seq_no: 11,
            },
            matches: vec![logpose_query::QueryMatch {
                id: RecordId::new("alpha"),
                value: 42.0,
                metadata: json!({"kind":"keep"}),
            }],
            diagnostics: Some(QueryDiagnostics {
                chosen_plan: QueryPlanKind::CooperativeFilteredAnn,
                planner_reason:
                    "filtered ann traversal is cheaper than exact scan for this selectivity"
                        .to_owned(),
                estimated_selectivity: 0.25,
                units_considered: 2,
                units_pruned: 1,
                units_scanned: 1,
                candidates_before_filter: 17,
                candidates_after_filter: 13,
                candidates_reranked: 7,
                candidates_merged: 5,
                rerank_count: 1,
                fallback_reason: Some("fallback".to_owned()),
                unit_scan_mix: BTreeMap::from([
                    ("immutable_ann".to_owned(), 1),
                    ("mutable_exact".to_owned(), 2),
                ]),
                stage_timings: Some(QueryStageTimings {
                    planning_micros: 11,
                    prefilter_micros: 22,
                    candidate_generation_micros: 33,
                    postfilter_micros: 44,
                    rerank_micros: 55,
                    merge_micros: 66,
                }),
            }),
        })
        .expect("query response should serialize");

        assert_eq!(
            payload["diagnostics"]["chosen_plan"],
            "cooperative_filtered_ann"
        );
        assert_eq!(
            payload["diagnostics"]["planner_reason"],
            "filtered ann traversal is cheaper than exact scan for this selectivity"
        );
        assert_eq!(
            payload["diagnostics"]["estimated_selectivity"],
            Value::from(0.25)
        );
        assert_eq!(payload["diagnostics"]["units_considered"], 2);
        assert_eq!(payload["diagnostics"]["units_pruned"], 1);
        assert_eq!(payload["diagnostics"]["units_scanned"], 1);
        assert_eq!(payload["diagnostics"]["candidates_before_filter"], 17);
        assert_eq!(payload["diagnostics"]["candidates_after_filter"], 13);
        assert_eq!(payload["diagnostics"]["candidates_reranked"], 7);
        assert_eq!(payload["diagnostics"]["candidates_merged"], 5);
        assert_eq!(payload["diagnostics"]["rerank_count"], 1);
        assert_eq!(payload["diagnostics"]["fallback_reason"], "fallback");
        assert_eq!(payload["diagnostics"]["unit_scan_mix"]["immutable_ann"], 1);
        assert_eq!(payload["diagnostics"]["unit_scan_mix"]["mutable_exact"], 2);
        assert_eq!(
            payload["diagnostics"]["stage_timings"]["planning_micros"],
            11
        );
        assert_eq!(
            payload["diagnostics"]["stage_timings"]["prefilter_micros"],
            22
        );
        assert_eq!(
            payload["diagnostics"]["stage_timings"]["candidate_generation_micros"],
            33
        );
        assert_eq!(
            payload["diagnostics"]["stage_timings"]["postfilter_micros"],
            44
        );
        assert_eq!(payload["diagnostics"]["stage_timings"]["rerank_micros"], 55);
        assert_eq!(payload["diagnostics"]["stage_timings"]["merge_micros"], 66);
    }

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
    async fn runtime_status_requires_bearer_token_when_auth_is_configured() {
        let state = Arc::new(AppState::new(auth_test_config("rest-auth-runtime")));
        let app = router(state);

        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/runtime/status")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/runtime/status")
                    .header("authorization", "Bearer operator-secret")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn database_endpoints_round_trip_with_operator_auth() {
        let state = Arc::new(AppState::new(auth_test_config("rest-namespace-auth")));
        let app = router(state);

        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/databases")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let put_database = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
                    .uri("/v1/databases/analytics")
                    .header("authorization", "Bearer operator-secret")
                    .header("content-type", "application/json")
                    .body(Body::from(database_body("analytics").to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(put_database.status(), StatusCode::OK);
        let put_database_body = json_body(put_database).await;
        assert_eq!(put_database_body["name"], "analytics");

        let get_database = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/databases/analytics")
                    .header("authorization", "Bearer operator-secret")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(get_database.status(), StatusCode::OK);
        let get_database_body = json_body(get_database).await;
        assert_eq!(get_database_body["name"], "analytics");

        let list_databases = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/databases")
                    .header("authorization", "Bearer operator-secret")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(list_databases.status(), StatusCode::OK);
        let databases_body = json_body(list_databases).await;
        let databases = databases_body
            .as_array()
            .expect("databases should be an array");
        assert_eq!(databases.len(), 2);
        assert!(
            databases
                .iter()
                .any(|database| database["name"] == "default"),
            "default database should be lazily bootstrapped"
        );
        assert!(
            databases
                .iter()
                .any(|database| database["name"] == "analytics"),
            "created database should be listed"
        );
    }

    #[tokio::test]
    async fn read_only_principals_can_read_but_not_write_when_auth_is_configured() {
        let state = Arc::new(AppState::new(auth_test_config("rest-auth-readonly")));
        state
            .control
            .set_database_access_policy(read_only_policy("default", "reader"))
            .await
            .expect("database policy should persist");
        state
            .control
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        let app = router(state);

        let stats = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/stats")
                    .header("authorization", "Bearer reader-secret")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(stats.status(), StatusCode::OK);

        let write = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/writes")
                    .header("authorization", "Bearer reader-secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "operations": [
                                {
                                    "op": "put",
                                    "id": "alpha",
                                    "vector": [1.0, 0.0],
                                    "metadata": {}
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(write.status(), StatusCode::FORBIDDEN);
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
        let create_body = json_body(create).await;
        assert_eq!(create_body["database_name"], "default");

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
        let write_body = json_body(write).await;
        assert_eq!(write_body["database_name"], "default");
        assert_eq!(write_body["collection_name"], "documents");

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
        assert_eq!(query_body["database_name"], "default");
        assert_eq!(query_body["collection_name"], "documents");
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
        assert_eq!(stats_body["database_name"], "default");
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
        assert_eq!(wal_body["database_name"], "default");
        assert_eq!(wal_body["collection_name"], "documents");
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
        let flush_body = json_body(flush).await;
        assert_eq!(flush_body["database_name"], "default");
        assert_eq!(flush_body["collection_name"], "documents");

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
        let compact_body = json_body(compact).await;
        assert_eq!(compact_body["database_name"], "default");
        assert_eq!(compact_body["collection_name"], "documents");

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
        assert_eq!(inspect_body["database_name"], "default");
        assert_eq!(inspect_body["collection_name"], "documents");
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
    async fn runtime_status_endpoint_reports_control_plane_summary() {
        let state = Arc::new(AppState::new(test_config("rest-runtime-status")));
        state
            .control
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        let app = router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/runtime/status")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["role"], "combined");
        assert_eq!(body["storage_engine"], "local");
        assert_eq!(body["collection_count"], 1);
        assert_eq!(body["collections"][0]["collection_name"], "documents");
        assert_eq!(body["collections"][0]["database_name"], "default");
        assert_eq!(body["collections"][0]["assigned_role"], "data");
        assert_eq!(body["collections"][0]["route_kind"], "local");
        assert!(body["coordination"].is_null());
        assert!(
            body["collections"][0]["route_reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("single-node"))
        );
    }

    #[test]
    fn runtime_status_serializes_coordination_fields_when_present() {
        let payload = serde_json::to_value(logpose_types::NodeRuntimeStatus {
            metadata: logpose_types::NodeMetadata {
                product: "LogPose".to_owned(),
                node_name: "rest-node".to_owned(),
                version: "test".to_owned(),
                git_sha: "sha".to_owned(),
                profile: "debug".to_owned(),
            },
            role: logpose_types::NodeRole::Combined,
            rest_endpoint: "http://127.0.0.1:8080".to_owned(),
            grpc_endpoint: "http://127.0.0.1:50051".to_owned(),
            storage_engine: "local+etcd-metadata".to_owned(),
            control_plane_ready: true,
            data_plane_ready: true,
            collection_count: 0,
            collections: Vec::new(),
            coordination: Some(logpose_types::CoordinationStatus {
                cluster_name: "prod-cluster".to_owned(),
                membership_registered: true,
                membership_lease_id: Some(17),
                registered_members: vec!["rest-node".to_owned(), "rest-peer".to_owned()],
                leader_node: Some("rest-node".to_owned()),
                is_local_leader: true,
                leadership_lease_id: Some(23),
                last_error: Some("warn".to_owned()),
            }),
            maintenance: logpose_types::MaintenanceBacklog::default(),
        })
        .expect("runtime status should serialize");

        assert_eq!(payload["coordination"]["cluster_name"], "prod-cluster");
        assert_eq!(payload["coordination"]["membership_lease_id"], 17);
        assert_eq!(payload["coordination"]["leadership_lease_id"], 23);
        assert_eq!(payload["coordination"]["leader_node"], "rest-node");
        assert_eq!(
            payload["coordination"]["registered_members"],
            serde_json::json!(["rest-node", "rest-peer"])
        );
        assert_eq!(payload["coordination"]["last_error"], "warn");
    }

    #[tokio::test]
    async fn placement_endpoint_reports_local_assignment() {
        let state = Arc::new(AppState::new(test_config("rest-placement")));
        state
            .control
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        let app = router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/placement")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["database_name"], "default");
        assert_eq!(body["collection_name"], "documents");
        assert_eq!(body["assigned_node"], "rest-placement");
        assert_eq!(body["assigned_role"], "data");
        assert_eq!(body["route_kind"], "local");
    }

    #[tokio::test]
    async fn rest_round_trips_explicit_database_requests() {
        let state = Arc::new(AppState::new(test_config("rest-namespace-reject")));
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
                            "database_name": "analytics",
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
        let create_body = json_body(create).await;
        assert_eq!(create_body["database_name"], "analytics");

        let get = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents?database=analytics")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(get.status(), StatusCode::OK);
        let get_body = json_body(get).await;
        assert_eq!(get_body["database_name"], "analytics");
        assert_eq!(get_body["name"], "documents");
    }

    #[tokio::test]
    async fn rest_treats_blank_namespace_fields_as_default_namespace() {
        let state = Arc::new(AppState::new(test_config("rest-blank-namespace")));
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
                            "database_name": "   ",
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
        let create_body = json_body(create).await;
        assert_eq!(create_body["database_name"], "default");

        let get = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents?database=")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(get.status(), StatusCode::OK);
        let get_body = json_body(get).await;
        assert_eq!(get_body["database_name"], "default");
        assert_eq!(get_body["name"], "documents");

        let inspect = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/documents/inspect?database=")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(inspect.status(), StatusCode::OK);
        let inspect_body = json_body(inspect).await;
        assert_eq!(inspect_body["database_name"], "default");
        assert_eq!(inspect_body["collection_name"], "documents");
    }

    #[tokio::test]
    async fn data_only_nodes_reject_control_plane_collection_creation() {
        let app = router(Arc::new(AppState::new(test_config_with_role(
            "rest-data-only",
            logpose_types::NodeRole::Data,
        ))));

        let response = app
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

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        assert!(body["error"].as_str().is_some_and(|message| {
            message.contains(
                "data-only nodes cannot accept control-plane collection lifecycle mutations",
            )
        }));
    }

    #[tokio::test]
    async fn control_only_nodes_reject_control_plane_collection_creation() {
        let app = router(Arc::new(AppState::new(test_config_with_role(
            "rest-control-create",
            logpose_types::NodeRole::Control,
        ))));

        let response = app
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

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|message| message.contains("without a local data plane"))
        );
    }

    #[tokio::test]
    async fn control_only_nodes_reject_data_plane_rest_operations() {
        let root = unique_temp_dir("rest-control-only");
        let initial = Arc::new(AppState::new(test_config_with_root(
            "rest-control-only",
            logpose_types::NodeRole::Combined,
            root.clone(),
        )));
        initial
            .control
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        drop(initial);

        let state = Arc::new(AppState::new(test_config_with_root(
            "rest-control-only",
            logpose_types::NodeRole::Control,
            root,
        )));
        let app = router(state);

        let responses = vec![
            (
                "write",
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
                                        }
                                    ]
                                })
                                .to_string(),
                            ))
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "query",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .method("POST")
                            .uri("/v1/collections/documents/query")
                            .header("content-type", "application/json")
                            .body(Body::from(
                                json!({
                                    "vector": [1.0, 0.0],
                                    "top_k": 1
                                })
                                .to_string(),
                            ))
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "stats",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .uri("/v1/collections/documents/stats")
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "flush",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .method("POST")
                            .uri("/v1/collections/documents/flush")
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "compact",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .method("POST")
                            .uri("/v1/collections/documents/compact")
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "inspect",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .uri("/v1/collections/documents/inspect?target=manifest")
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
        ];

        for (operation, response) in responses {
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{operation} should be rejected on control-only nodes"
            );
            let body = json_body(response).await;
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|message| message.contains("data-plane operations")),
                "{operation} should explain the role mismatch"
            );
        }
    }

    #[tokio::test]
    async fn recorded_remote_assignments_reject_data_plane_rest_operations() {
        let root = unique_temp_dir("rest-recorded-route");
        let initial = Arc::new(AppState::new(test_config_with_root(
            "rest-recorded-node-a",
            logpose_types::NodeRole::Combined,
            root.clone(),
        )));
        initial
            .control
            .create_collection(CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .await
            .expect("collection should be created");
        drop(initial);

        let state = Arc::new(AppState::new(test_config_with_root(
            "rest-recorded-node-b",
            logpose_types::NodeRole::Combined,
            root,
        )));
        let app = router(state);

        let responses = vec![
            (
                "write",
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
                                        }
                                    ]
                                })
                                .to_string(),
                            ))
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "query",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .method("POST")
                            .uri("/v1/collections/documents/query")
                            .header("content-type", "application/json")
                            .body(Body::from(
                                json!({
                                    "vector": [1.0, 0.0],
                                    "top_k": 1
                                })
                                .to_string(),
                            ))
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "stats",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .uri("/v1/collections/documents/stats")
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "flush",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .method("POST")
                            .uri("/v1/collections/documents/flush")
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "compact",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .method("POST")
                            .uri("/v1/collections/documents/compact")
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
            (
                "inspect",
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .uri("/v1/collections/documents/inspect?target=manifest")
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("router should respond"),
            ),
        ];

        for (operation, response) in responses {
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{operation} should be rejected for recorded remote assignments"
            );
            let body = json_body(response).await;
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|message| message.contains("not locally served")),
                "{operation} should explain the recorded placement mismatch"
            );
        }
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
    async fn missing_collection_placement_returns_not_found() {
        let state = Arc::new(AppState::new(test_config("rest-missing-placement")));
        let app = router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/collections/missing/placement")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn create_collection_rejects_zero_dimensions() {
        let app = router(Arc::new(AppState::new(test_config("rest-zero-dimensions"))));

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "documents",
                            "dimensions": 0,
                            "metric": "dot"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|message| message.contains("dimensions must be greater than 0"))
        );
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
            query_body["diagnostics"]["fallback_reason"]
                .as_str()
                .is_some_and(|reason| !reason.is_empty())
        );
        assert!(
            query_body["diagnostics"]["candidates_reranked"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        assert!(
            query_body["diagnostics"]["candidates_merged"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        assert!(
            query_body["diagnostics"]["unit_scan_mix"]["mutable_exact"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        assert!(
            query_body["diagnostics"]["stage_timings"]["planning_micros"]
                .as_u64()
                .is_some(),
            "profile mode should include stage timings"
        );
        assert!(
            query_body["diagnostics"]["stage_timings"]["prefilter_micros"]
                .as_u64()
                .is_some()
        );
        assert!(
            query_body["diagnostics"]["stage_timings"]["candidate_generation_micros"]
                .as_u64()
                .is_some()
        );
        assert!(
            query_body["diagnostics"]["stage_timings"]["postfilter_micros"]
                .as_u64()
                .is_some()
        );
        assert!(
            query_body["diagnostics"]["stage_timings"]["rerank_micros"]
                .as_u64()
                .is_some()
        );
        assert!(
            query_body["diagnostics"]["stage_timings"]["merge_micros"]
                .as_u64()
                .is_some()
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

    #[tokio::test]
    async fn query_rejects_zero_top_k() {
        let state = Arc::new(AppState::new(test_config("rest-zero-top-k")));
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
                            "top_k": 0
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(query.status(), StatusCode::BAD_REQUEST);
        let body = json_body(query).await;
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|message| message.contains("top_k must be greater than 0"))
        );
    }

    #[tokio::test]
    async fn write_rejects_empty_put_record_id() {
        let state = Arc::new(AppState::new(test_config("rest-empty-put-id")));
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
                                    "id": "",
                                    "vector": [1.0, 0.0],
                                    "metadata": {}
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(write.status(), StatusCode::BAD_REQUEST);
        let body = json_body(write).await;
        assert!(body["error"].as_str().is_some_and(|message| {
            message.contains("write operation record id must not be empty")
        }));
    }

    #[tokio::test]
    async fn write_rejects_empty_delete_record_id() {
        let state = Arc::new(AppState::new(test_config("rest-empty-delete-id")));
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
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/collections/documents/writes")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "operations": [
                                {
                                    "op": "delete",
                                    "id": ""
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(write.status(), StatusCode::BAD_REQUEST);
        let body = json_body(write).await;
        assert!(body["error"].as_str().is_some_and(|message| {
            message.contains("write operation record id must not be empty")
        }));
    }

    #[tokio::test]
    async fn rest_database_policy_endpoints_round_trip_json_and_role_errors() {
        let combined = router(Arc::new(AppState::new(test_config("rest-policy-combined"))));
        let put = combined
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
                    .uri("/v1/databases/default/policy")
                    .header("content-type", "application/json")
                    .body(Body::from(policy_body("default").to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(put.status(), StatusCode::OK);
        let put_body = json_body(put).await;
        assert_eq!(put_body["database_name"], "default");
        assert_eq!(put_body["authentication_mode"], "external_token");
        assert_eq!(
            put_body["role_bindings"]
                .as_array()
                .expect("bindings should be an array")
                .len(),
            2
        );

        let get = combined
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/databases/default/policy")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(get.status(), StatusCode::OK);
        let get_body = json_body(get).await;
        assert_eq!(get_body, policy_body("default"));

        let data_only = router(Arc::new(AppState::new(test_config_with_role(
            "rest-policy-data-only",
            logpose_types::NodeRole::Data,
        ))));
        let data_error = data_only
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
                    .uri("/v1/databases/default/policy")
                    .header("content-type", "application/json")
                    .body(Body::from(policy_body("default").to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(data_error.status(), StatusCode::BAD_REQUEST);
        let data_error_body = json_body(data_error).await;
        assert!(data_error_body["error"].as_str().is_some_and(|message| {
            message.contains("data-only nodes cannot accept control-plane database mutations")
        }));
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
        test_config_with_role(label, logpose_types::NodeRole::Combined)
    }

    fn test_config_with_role(label: &str, node_role: logpose_types::NodeRole) -> LogPoseConfig {
        test_config_with_root(label, node_role, unique_temp_dir(label))
    }

    fn test_config_with_root(
        label: &str,
        node_role: logpose_types::NodeRole,
        storage_root: PathBuf,
    ) -> LogPoseConfig {
        LogPoseConfig {
            node_name: label.to_owned(),
            node_role,
            storage_root,
            ..LogPoseConfig::default()
        }
    }

    fn auth_test_config(label: &str) -> LogPoseConfig {
        let mut config = test_config(label);
        config.auth.bootstrap_tokens = vec![
            BootstrapTokenConfig {
                token: "operator-secret".to_owned(),
                principal: Principal::new_with_access_tier(
                    "ops-admin",
                    PrincipalKind::User,
                    AccessTier::Operator,
                ),
            },
            BootstrapTokenConfig {
                token: "reader-secret".to_owned(),
                principal: Principal::new_with_access_tier(
                    "reader",
                    PrincipalKind::User,
                    AccessTier::Service,
                ),
            },
        ];
        config
    }

    fn database_body(database_name: &str) -> Value {
        let descriptor = DatabaseDescriptor::new(database_name);
        serde_json::to_value(descriptor).expect("database descriptor should serialize")
    }

    fn read_only_policy(database_name: &str, principal_name: &str) -> DatabaseAccessPolicy {
        DatabaseAccessPolicy {
            database_name: database_name.to_owned(),
            authentication_mode: AuthenticationMode::ExternalToken,
            role_bindings: vec![DatabaseRoleBinding {
                database_name: database_name.to_owned(),
                principal_name: principal_name.to_owned(),
                role: DatabaseRole::ReadOnly,
            }],
        }
    }

    fn policy_body(database_name: &str) -> Value {
        json!({
            "database_name": database_name,
            "authentication_mode": "external_token",
            "role_bindings": [
                {
                    "database_name": database_name,
                    "principal_name": "ops-admin",
                    "role": "owner"
                },
                {
                    "database_name": database_name,
                    "principal_name": "reader-service",
                    "role": "read_only"
                }
            ]
        })
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
