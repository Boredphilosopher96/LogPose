//! REST API surface for LogPose.
//!
//! Every database-scoped route names its database in the path, `/v2/databases/{database}`,
//! and every collection-scoped route names its collection below it,
//! `/v2/databases/{database}/collections/{collection}`. No body or query string selects a
//! database. Records are natural JSON documents typed by the collection's schema.

mod error;

#[cfg(test)]
use yaml_rust2 as _;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, StatusCode, Uri, header::AUTHORIZATION},
    response::IntoResponse,
    routing::{MethodRouter, get, post},
};
use error::{ApiError, ApiJson, ApiPath, ApiQuery};
pub use error::{ErrorBody, http_status};
use logpose_auth::{AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding};
use logpose_catalog::{CollectionDescriptor, DatabaseDescriptor};
use logpose_core::{AppState, RequestAuth};
use logpose_query::{ExplainMode, FilterExpr, MetadataFilter, QueryRequest, ScalarMetadataValue};
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{
    CollectionRef, CommitAck, LogPoseError, ResourceKind, Snapshot,
    record::{PartialUpdate, PrimaryKey, Record},
    schema::{CollectionSchema, CreateCollectionSpec, FieldType, SchemaChange},
    value::Value as TypedValue,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{net::SocketAddr, sync::Arc};
use tower_http::trace::TraceLayer;

/// Collection routes live below this path template.
const COLLECTION: &str = "/v2/databases/{database}/collections/{collection}";

/// Every REST route: its path template and the handlers per method.
///
/// The router is built from this table, and the API contract test checks it against
/// `openapi/logpose.v2.yaml` in both directions.
fn routes() -> Vec<(String, MethodRouter<Arc<AppState>>)> {
    let collection = |suffix: &str| format!("{COLLECTION}{suffix}");
    vec![
        ("/health".to_owned(), get(health)),
        ("/v2/metadata".to_owned(), get(metadata)),
        ("/v2/runtime/status".to_owned(), get(runtime_status)),
        ("/v2/databases".to_owned(), get(list_databases)),
        (
            "/v2/databases/{database}".to_owned(),
            get(get_database).put(put_database).delete(drop_database),
        ),
        (
            "/v2/databases/{database}/policy".to_owned(),
            get(get_database_policy).put(put_database_policy),
        ),
        (
            "/v2/databases/{database}/collections".to_owned(),
            get(list_collections).post(create_collection),
        ),
        (
            collection(""),
            get(get_collection)
                .patch(alter_collection)
                .delete(drop_collection),
        ),
        (collection("/placement"), get(get_collection_placement)),
        (collection("/records/upsert"), post(upsert_records)),
        (collection("/records/update"), post(update_records)),
        (collection("/records/delete"), post(delete_records)),
        (collection("/records/get"), post(get_records)),
        (collection("/query"), post(query_collection)),
        (collection("/stats"), get(get_collection_stats)),
        (collection("/flush"), post(flush_collection)),
        (collection("/compact"), post(compact_collection)),
        (collection("/inspect"), get(inspect_collection)),
    ]
}

/// Path templates of every REST route, as the router registers them.
#[doc(hidden)]
#[must_use]
pub fn route_paths() -> Vec<String> {
    routes().into_iter().map(|(path, _)| path).collect()
}

/// Create the versioned REST router.
///
/// Request bodies above `limits.max_rest_body_bytes` are rejected with HTTP 413 and a typed
/// `TOO_LARGE` error; unknown paths, and methods a path does not serve, get a typed 404.
pub fn router(state: Arc<AppState>) -> Router {
    let body_limit = state.config.limits.max_rest_body_bytes;
    routes()
        .into_iter()
        .fold(Router::new(), |router, (path, handlers)| {
            router.route(&path, handlers)
        })
        .fallback(route_not_found)
        .method_not_allowed_fallback(route_not_found)
        .with_state(state)
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(TraceLayer::new_for_http())
}

async fn route_not_found(method: Method, uri: Uri) -> ApiError {
    ApiError(LogPoseError::not_found(
        ResourceKind::Route,
        format!("{method} {}", uri.path()),
    ))
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

/// The collection a route names in its path.
#[derive(Debug, Deserialize)]
struct CollectionPath {
    database: String,
    collection: String,
}

impl CollectionPath {
    fn reference(&self) -> CollectionRef {
        CollectionRef::new(self.database.clone(), self.collection.clone())
    }

    /// The `database/collection` key the application layer resolves.
    fn key(&self) -> String {
        self.reference().lookup_name()
    }

    fn scoped<T>(self, response: T) -> CollectionScopedResponse<T> {
        CollectionScopedResponse {
            database_name: self.database,
            collection_name: self.collection,
            response,
        }
    }
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

async fn list_databases(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DatabaseList>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(DatabaseList {
        databases: state.databases_with_auth(&auth).await?,
    }))
}

async fn put_database(
    headers: HeaderMap,
    ApiPath(database): ApiPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DatabaseDescriptor>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(
        state
            .put_database_with_auth(&auth, DatabaseDescriptor::new(database))
            .await?,
    ))
}

async fn get_database(
    headers: HeaderMap,
    ApiPath(database): ApiPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DatabaseDescriptor>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(state.database_with_auth(&auth, &database).await?))
}

async fn drop_database(
    headers: HeaderMap,
    ApiPath(database): ApiPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DroppedDatabase>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    state.drop_database_with_auth(&auth, &database).await?;
    Ok(Json(DroppedDatabase {
        database_name: database,
    }))
}

async fn put_database_policy(
    headers: HeaderMap,
    ApiPath(database): ApiPath<String>,
    State(state): State<Arc<AppState>>,
    ApiJson(body): ApiJson<DatabasePolicyBody>,
) -> Result<Json<DatabaseAccessPolicy>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let policy = DatabaseAccessPolicy {
        role_bindings: body
            .role_bindings
            .into_iter()
            .map(|binding| DatabaseRoleBinding {
                database_name: database.clone(),
                principal_name: binding.principal_name,
                role: binding.role,
            })
            .collect(),
        database_name: database,
        authentication_mode: body.authentication_mode,
    };
    Ok(Json(
        state
            .set_database_access_policy_with_auth(&auth, policy)
            .await?,
    ))
}

async fn get_database_policy(
    headers: HeaderMap,
    ApiPath(database): ApiPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DatabaseAccessPolicy>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(
        state
            .database_access_policy_with_auth(&auth, &database)
            .await?,
    ))
}

async fn list_collections(
    headers: HeaderMap,
    ApiPath(database): ApiPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CollectionList>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(CollectionList {
        collections: state.list_collections_with_auth(&auth, &database).await?,
    }))
}

async fn create_collection(
    headers: HeaderMap,
    ApiPath(database): ApiPath<String>,
    State(state): State<Arc<AppState>>,
    ApiJson(spec): ApiJson<CreateCollectionSpec>,
) -> Result<(StatusCode, Json<CollectionDescriptor>), ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let descriptor = state
        .create_collection_with_auth(&auth, CreateCollectionRequest::from_spec(database, spec))
        .await?;
    Ok((StatusCode::CREATED, Json(descriptor)))
}

async fn get_collection(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CollectionDescriptor>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(
        state.get_collection_with_auth(&auth, &path.key()).await?,
    ))
}

async fn alter_collection(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
    ApiJson(change): ApiJson<SchemaChange>,
) -> Result<Json<CollectionDescriptor>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(
        state
            .alter_collection_with_auth(&auth, &path.key(), change)
            .await?,
    ))
}

async fn drop_collection(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CollectionScopedResponse<Map<String, Value>>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    state.drop_collection_with_auth(&auth, &path.key()).await?;
    Ok(Json(path.scoped(Map::new())))
}

async fn get_collection_placement(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_types::CollectionPlacement>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    Ok(Json(
        state
            .collection_placement_with_auth(&auth, &path.key())
            .await?,
    ))
}

async fn upsert_records(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
    ApiJson(body): ApiJson<RecordsBody>,
) -> Result<Json<CollectionScopedResponse<CommitAck>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let schema = state
        .collection_schema_with_auth(&auth, &path.key())
        .await?;
    let records = body
        .records
        .into_iter()
        .enumerate()
        .map(|(index, document)| {
            let key = document_key(&schema, &document);
            Record::from_json(&schema, document)
                .map_err(|error| error.to_error(&format!("records[{index}]"), key.as_ref()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ack = state
        .upsert_records_with_auth(&auth, &path.key(), records)
        .await?;
    Ok(Json(path.scoped(ack)))
}

async fn update_records(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
    ApiJson(body): ApiJson<RecordsBody>,
) -> Result<Json<CollectionScopedResponse<CommitAck>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let schema = state
        .collection_schema_with_auth(&auth, &path.key())
        .await?;
    let updates = body
        .records
        .into_iter()
        .enumerate()
        .map(|(index, document)| {
            let key = document_key(&schema, &document);
            PartialUpdate::from_json(&schema, document)
                .map_err(|error| error.to_error(&format!("records[{index}]"), key.as_ref()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ack = state
        .update_records_with_auth(&auth, &path.key(), updates)
        .await?;
    Ok(Json(path.scoped(ack)))
}

async fn delete_records(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
    ApiJson(body): ApiJson<KeysBody>,
) -> Result<Json<CollectionScopedResponse<CommitAck>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let keys = primary_keys_from_json(body.keys)?;
    let ack = state
        .delete_records_with_auth(&auth, &path.key(), keys)
        .await?;
    Ok(Json(path.scoped(ack)))
}

async fn get_records(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
    ApiJson(body): ApiJson<GetRecordsBody>,
) -> Result<Json<CollectionScopedResponse<RecordsResponse>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let keys = primary_keys_from_json(body.keys)?;
    let fetched = state
        .get_records_with_auth(&auth, &path.key(), keys.clone(), body.output_fields)
        .await?;
    let mut records = Vec::new();
    let mut missing_keys = Vec::new();
    for (key, record) in keys.into_iter().zip(fetched.records) {
        match record {
            Some(record) => records.push(record.to_json(&fetched.schema)),
            None => missing_keys.push(key.to_json()),
        }
    }
    Ok(Json(path.scoped(RecordsResponse {
        records,
        missing_keys,
        snapshot: fetched.snapshot,
    })))
}

async fn query_collection(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
    ApiJson(request): ApiJson<QueryCollectionBody>,
) -> Result<Json<CollectionScopedResponse<logpose_query::QueryResponse>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    if request.top_k == 0 {
        return Err(ApiError(LogPoseError::invalid_field(
            "top_k",
            "top_k must be greater than 0",
        )));
    }
    let filters = request
        .filters
        .into_iter()
        .map(|(field, value)| {
            let field_name = field.clone();
            ScalarMetadataValue::from_json(&value)
                .map(|value| MetadataFilter { field, value })
                .ok_or_else(|| {
                    ApiError(LogPoseError::invalid_field(
                        format!("filters.{field_name}"),
                        "query filters must contain only scalar JSON values",
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let response = state
        .query_with_auth(
            &auth,
            QueryRequest {
                collection_name: path.key(),
                vector: request.vector,
                top_k: request.top_k,
                snapshot: request.snapshot,
                read_barrier: request.read_barrier,
                filters,
                predicate: request.predicate,
                explain: request.explain,
                snapshot_token: request.snapshot_token,
                pin: request.pin,
            },
        )
        .await?;
    Ok(Json(path.scoped(response)))
}

async fn get_collection_stats(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    ApiQuery(params): ApiQuery<CollectionStatsQuery>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<logpose_types::CollectionStats>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let (snapshot, read_barrier) = read_constraints_from_query_pairs(
        params.snapshot_manifest_generation,
        params.snapshot_visible_seq_no,
        params.read_barrier_manifest_generation,
        params.read_barrier_visible_seq_no,
    )?;
    Ok(Json(
        state
            .stats_for_read_with_auth(&auth, &path.key(), snapshot, read_barrier)
            .await?,
    ))
}

async fn flush_collection(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CollectionScopedResponse<Snapshot>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let snapshot = state.flush_with_auth(&auth, &path.key()).await?;
    Ok(Json(path.scoped(snapshot)))
}

async fn compact_collection(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CollectionScopedResponse<Snapshot>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let snapshot = state.compact_with_auth(&auth, &path.key()).await?;
    Ok(Json(path.scoped(snapshot)))
}

async fn inspect_collection(
    headers: HeaderMap,
    ApiPath(path): ApiPath<CollectionPath>,
    State(state): State<Arc<AppState>>,
    ApiQuery(params): ApiQuery<InspectCollectionParams>,
) -> Result<Json<CollectionScopedResponse<logpose_storage::InspectReport>>, ApiError> {
    let auth = request_auth_from_headers(&headers)?;
    let target = inspect_target_from_params(params)?;
    let report = state.inspect_with_auth(&auth, &path.key(), target).await?;
    Ok(Json(path.scoped(report)))
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct DatabaseList {
    databases: Vec<DatabaseDescriptor>,
}

#[derive(Debug, Serialize)]
struct DroppedDatabase {
    database_name: String,
}

#[derive(Debug, Serialize)]
struct CollectionList {
    collections: Vec<CollectionDescriptor>,
}

/// A database access policy without its database, which the path names.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabasePolicyBody {
    authentication_mode: AuthenticationMode,
    #[serde(default)]
    role_bindings: Vec<RoleBindingBody>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleBindingBody {
    principal_name: String,
    role: DatabaseRole,
}

/// Natural JSON documents: whole records for an upsert, or a key plus the fields to change
/// for an update.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordsBody {
    records: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeysBody {
    keys: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GetRecordsBody {
    keys: Vec<Value>,
    #[serde(default)]
    output_fields: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RecordsResponse {
    /// The live records found, in request order, as natural JSON documents.
    records: Vec<Value>,
    /// The requested keys without a live record, in request order.
    missing_keys: Vec<Value>,
    /// The state the lookup read.
    snapshot: Snapshot,
}

/// The primary key a document names, when it names one of the schema's key type, so an error
/// about the document can say which record it is.
fn document_key(schema: &CollectionSchema, document: &Value) -> Option<PrimaryKey> {
    match document.get(&schema.primary_key().name)? {
        Value::String(value) => Some(PrimaryKey::String(value.clone())),
        Value::Number(number) => number.as_i64().map(PrimaryKey::Int64),
        _ => None,
    }
}

/// Primary keys from JSON: integers for `int64` keys and strings for `string` keys, checked
/// against the schema's key type by the engine. Each is named `keys[i]`.
fn primary_keys_from_json(keys: Vec<Value>) -> Result<Vec<PrimaryKey>, ApiError> {
    keys.into_iter()
        .enumerate()
        .map(|(index, key)| {
            let invalid = |message: String| {
                ApiError(LogPoseError::invalid_field(
                    format!("keys[{index}]"),
                    message,
                ))
            };
            match key {
                Value::String(value) => Ok(PrimaryKey::String(value)),
                number @ Value::Number(_) => {
                    match TypedValue::from_json(number, FieldType::Int64) {
                        Ok(TypedValue::Int64(value)) => Ok(PrimaryKey::Int64(value)),
                        Ok(_) => Err(invalid("a primary key must be an integer".to_owned())),
                        Err(error) => Err(invalid(error.to_string())),
                    }
                }
                other => Err(invalid(format!(
                    "a primary key must be an integer or a string, found {other}"
                ))),
            }
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct CollectionStatsQuery {
    snapshot_manifest_generation: Option<u64>,
    snapshot_visible_seq_no: Option<u64>,
    read_barrier_manifest_generation: Option<u64>,
    read_barrier_visible_seq_no: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct QueryCollectionBody {
    vector: Vec<f32>,
    top_k: usize,
    #[serde(default)]
    snapshot: Option<Snapshot>,
    #[serde(default)]
    read_barrier: Option<Snapshot>,
    #[serde(default)]
    filters: Map<String, Value>,
    #[serde(default)]
    predicate: Option<FilterExpr>,
    #[serde(default)]
    explain: ExplainMode,
    #[serde(default)]
    snapshot_token: Option<String>,
    #[serde(default)]
    pin: bool,
}

#[derive(Debug, Deserialize)]
struct InspectCollectionParams {
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

fn request_auth_from_headers(headers: &HeaderMap) -> Result<RequestAuth, ApiError> {
    let value = match headers.get(AUTHORIZATION) {
        Some(value) => value,
        None => return Ok(RequestAuth::default()),
    };
    let unauthenticated = |message: &str| {
        ApiError(LogPoseError::Unauthenticated {
            message: message.to_owned(),
        })
    };
    let value = value
        .to_str()
        .map_err(|_| unauthenticated("authorization header must be valid ASCII"))?;
    let (scheme, token) = value
        .split_once(' ')
        .ok_or_else(|| unauthenticated("authorization header must use the Bearer scheme"))?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.trim().is_empty() {
        return Err(unauthenticated(
            "authorization header must use the Bearer scheme",
        ));
    }
    Ok(RequestAuth::bearer_token(token.trim()))
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
                ApiError(LogPoseError::invalid_field(
                    "segment_id",
                    "inspect target 'segment' requires segment_id",
                ))
            }),
        "maintenance" => Ok(InspectTarget::Maintenance),
        other => Err(ApiError(LogPoseError::invalid_field(
            "target",
            format!("unsupported inspect target '{other}'"),
        ))),
    }
}

fn snapshot_from_query_pair(
    manifest_generation: Option<u64>,
    visible_seq_no: Option<u64>,
    manifest_field: &str,
    visible_seq_field: &str,
) -> Result<Option<Snapshot>, ApiError> {
    match (manifest_generation, visible_seq_no) {
        (Some(manifest_generation), Some(visible_seq_no)) => Ok(Some(Snapshot {
            manifest_generation,
            visible_seq_no,
        })),
        (None, None) => Ok(None),
        _ => Err(ApiError(LogPoseError::invalid_field(
            if manifest_generation.is_none() {
                manifest_field
            } else {
                visible_seq_field
            },
            format!("{manifest_field} and {visible_seq_field} must be provided together"),
        ))),
    }
}

fn read_constraints_from_query_pairs(
    snapshot_manifest_generation: Option<u64>,
    snapshot_visible_seq_no: Option<u64>,
    read_barrier_manifest_generation: Option<u64>,
    read_barrier_visible_seq_no: Option<u64>,
) -> Result<(Option<Snapshot>, Option<Snapshot>), ApiError> {
    let snapshot = snapshot_from_query_pair(
        snapshot_manifest_generation,
        snapshot_visible_seq_no,
        "snapshot_manifest_generation",
        "snapshot_visible_seq_no",
    )?;
    let read_barrier = snapshot_from_query_pair(
        read_barrier_manifest_generation,
        read_barrier_visible_seq_no,
        "read_barrier_manifest_generation",
        "read_barrier_visible_seq_no",
    )?;
    if snapshot.is_some() && read_barrier.is_some() {
        return Err(ApiError(LogPoseError::invalid_field(
            "read_barrier_manifest_generation",
            "snapshot and read_barrier cannot be provided together",
        )));
    }
    Ok((snapshot, read_barrier))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use logpose_auth::{
        AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
        Principal, PrincipalKind,
    };
    use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
    use logpose_query::{QueryDiagnostics, QueryPlanKind, QueryResponse, QueryStageTimings};
    use logpose_types::{DistanceMetric, RecordId};
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
            snapshot_token: None,
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

    #[test]
    fn snapshot_query_pair_requires_both_fields() {
        let snapshot = snapshot_from_query_pair(
            Some(7),
            Some(11),
            "snapshot_manifest_generation",
            "snapshot_visible_seq_no",
        )
        .expect("complete snapshot pair should parse");
        assert_eq!(
            snapshot,
            Some(Snapshot {
                manifest_generation: 7,
                visible_seq_no: 11,
            })
        );

        let error = snapshot_from_query_pair(
            Some(7),
            None,
            "snapshot_manifest_generation",
            "snapshot_visible_seq_no",
        )
        .expect_err("partial snapshot pair should fail");
        assert!(matches!(
            error.0,
            LogPoseError::InvalidArgument { field: Some(field), message }
                if field == "snapshot_visible_seq_no"
                    && message == "snapshot_manifest_generation and snapshot_visible_seq_no must be provided together"
        ));
    }

    #[test]
    fn read_constraints_reject_mixing_snapshot_and_read_barrier() {
        let error = read_constraints_from_query_pairs(Some(1), Some(2), Some(1), Some(2))
            .expect_err("exact snapshot and read barrier should conflict");

        assert!(matches!(
            error.0,
            LogPoseError::InvalidArgument { message, .. }
                if message == "snapshot and read_barrier cannot be provided together"
        ));
    }

    #[test]
    fn read_barrier_query_pair_requires_both_fields() {
        let error = read_constraints_from_query_pairs(None, None, Some(7), None)
            .expect_err("partial read barrier pair should fail");

        assert!(matches!(
            error.0,
            LogPoseError::InvalidArgument { field: Some(field), message }
                if field == "read_barrier_visible_seq_no"
                    && message == "read_barrier_manifest_generation and read_barrier_visible_seq_no must be provided together"
        ));
    }

    const DOCS: &str = "/v2/databases/default/collections/documents";

    /// Send one request and return its status, `retry-after` header, and JSON body (`null`
    /// when the body is empty or not JSON).
    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
        token: Option<&str>,
    ) -> (StatusCode, Option<String>, Value) {
        let mut request = axum::http::Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let request = match body {
            Some(body) => request
                .header("content-type", "application/json")
                .body(Body::from(body.to_string())),
            None => request.body(Body::empty()),
        }
        .expect("request should build");
        let response = app
            .clone()
            .oneshot(request)
            .await
            .expect("router should respond");
        let status = response.status();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes();
        (
            status,
            retry_after,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn call(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let (status, _, body) = send(app, method, uri, body, None).await;
        (status, body)
    }

    /// The single-vector create spec: string key `id`, vector `vector`, dynamic fields on.
    fn documents_spec(dimensions: u32, metric: &str) -> Value {
        json!({
            "name": "documents",
            "primary_key": {"name": "id", "type": "string"},
            "vectors": [{"name": "vector", "dimensions": dimensions, "metric": metric}]
        })
    }

    /// The engine plan's example collection (decision D4), with an int64 key.
    fn products_spec() -> Value {
        json!({
            "name": "products",
            "primary_key": {"name": "sku", "type": "int64"},
            "vectors": [{"name": "embedding", "dimensions": 3, "metric": "cosine"}],
            "fields": [
                {"name": "tenant", "type": "string", "nullable": false},
                {"name": "price", "type": "float64"},
                {"name": "tags", "type": "array<string>"},
                {"name": "updated_at", "type": "timestamp"},
                {"name": "attrs", "type": "json"}
            ],
            "dynamic_fields": true
        })
    }

    async fn create_documents(app: &Router) {
        let (status, body) = call(
            app,
            "POST",
            "/v2/databases/default/collections",
            Some(documents_spec(2, "dot")),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    async fn upsert(app: &Router, records: Value) -> (StatusCode, Value) {
        call(
            app,
            "POST",
            &format!("{DOCS}/records/upsert"),
            Some(json!({ "records": records })),
        )
        .await
    }

    fn violation(body: &Value) -> &Value {
        &body["details"]["field_violations"][0]["field"]
    }

    fn match_ids(body: &Value) -> Vec<&str> {
        body["matches"]
            .as_array()
            .expect("matches should be an array")
            .iter()
            .map(|candidate| candidate["id"].as_str().expect("id should be a string"))
            .collect()
    }

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let app = router(Arc::new(AppState::new(test_config("rest-health"))));
        let (status, body) = call(&app, "GET", "/health", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn request_bodies_above_the_limit_are_rejected_with_a_typed_413() {
        let mut config = test_config("rest-body-limit");
        config.limits.max_rest_body_bytes = 256;
        let app = router(Arc::new(AppState::new(config)));
        let mut spec = documents_spec(2, "dot");
        spec["fields"] = Value::Array(
            (0..8)
                .map(|index| json!({"name": format!("field_{index}_{}", "x".repeat(30)), "type": "string"}))
                .collect(),
        );
        let body = spec.to_string();
        let size = body.len();
        assert!(size > 256);

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v2/databases/default/collections")
                    .header("content-type", "application/json")
                    .header("content-length", size)
                    .body(Body::from(body))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = json_body(response).await;
        assert_eq!(body["code"], "RESOURCE_EXHAUSTED");
        assert_eq!(body["details"]["reason"], "TOO_LARGE");
        assert_eq!(body["details"]["metadata"]["limit_bytes"], "256");
        assert_eq!(body["details"]["metadata"]["size_bytes"], size.to_string());

        // A body under the limit is accepted.
        create_documents(&app).await;
    }

    #[tokio::test]
    async fn malformed_json_and_query_strings_are_typed_invalid_arguments() {
        let app = router(Arc::new(AppState::new(test_config("rest-malformed"))));
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v2/databases/default/collections")
                    .header("content-type", "application/json")
                    .body(Body::from("{not json"))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        assert_eq!(body["code"], "INVALID_ARGUMENT");
        assert_eq!(body["details"]["reason"], "INVALID_ARGUMENT");

        let (status, body) = call(
            &app,
            "GET",
            &format!("{DOCS}/stats?snapshot_visible_seq_no=abc"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "INVALID_ARGUMENT");

        // Unknown keys in a create request are rejected, so a typo never changes a schema.
        let mut spec = documents_spec(2, "dot");
        spec["dimension"] = json!(2);
        let (status, body) = call(
            &app,
            "POST",
            "/v2/databases/default/collections",
            Some(spec),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("dimension")),
            "{body}"
        );
    }

    #[tokio::test]
    async fn read_barriers_ahead_only_in_manifest_generation_name_the_generation() {
        let app = router(Arc::new(AppState::new(test_config(
            "rest-barrier-generation",
        ))));
        create_documents(&app).await;

        // Sequence 0 is visible; manifest generation 9 is not.
        let (status, retry_after, body) = send(
            &app,
            "GET",
            &format!(
                "{DOCS}/stats?read_barrier_manifest_generation=9&read_barrier_visible_seq_no=0"
            ),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(retry_after.is_none());
        assert_eq!(body["details"]["reason"], "READ_BARRIER_NOT_SATISFIED");
        let metadata = &body["details"]["metadata"];
        assert_eq!(metadata["required_manifest_generation"], "9");
        assert_eq!(metadata["visible_manifest_generation"], "0");
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("manifest generation 9")),
            "{body}"
        );
    }

    #[tokio::test]
    async fn unknown_routes_return_a_typed_not_found() {
        let app = router(Arc::new(AppState::new(test_config("rest-unknown-route"))));
        for path in ["/v2/nothing", "/v1/collections/documents"] {
            let (status, body) = call(&app, "GET", path, None).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert_eq!(body["code"], "NOT_FOUND");
            assert_eq!(body["details"]["metadata"]["resource_type"], "route");
            assert_eq!(
                body["details"]["metadata"]["resource_name"],
                format!("GET {path}")
            );
        }
    }

    #[tokio::test]
    async fn unserved_methods_on_known_paths_return_a_typed_not_found() {
        let app = router(Arc::new(AppState::new(test_config("rest-unserved-method"))));
        let (status, body) = call(&app, "POST", DOCS, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "NOT_FOUND");
        assert_eq!(body["details"]["metadata"]["resource_type"], "route");
        assert_eq!(
            body["details"]["metadata"]["resource_name"],
            format!("POST {DOCS}")
        );
    }

    #[tokio::test]
    async fn undecodable_path_parameters_are_typed_invalid_arguments() {
        let app = router(Arc::new(AppState::new(test_config("rest-bad-path"))));
        let (status, body) = call(&app, "GET", "/v2/databases/default/collections/%FF", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "INVALID_ARGUMENT");
        assert_eq!(body["details"]["reason"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn write_validation_errors_name_the_offending_field() {
        let app = router(Arc::new(AppState::new(test_config("rest-field-path"))));
        create_documents(&app).await;

        let (status, body) = upsert(
            &app,
            json!([
                {"id": "a", "vector": [1.0, 0.0]},
                {"id": "b", "vector": [1.0]}
            ]),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["details"]["reason"], "DIMENSION_MISMATCH");
        assert_eq!(violation(&body), "records[1].vector");
        assert_eq!(body["details"]["metadata"]["record_id"], "b");

        let (status, body) = upsert(&app, json!([{"id": "", "vector": [1.0, 0.0]}])).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "records[0].id");
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("must not be an empty string")),
            "{body}"
        );

        let (status, body) = call(
            &app,
            "POST",
            &format!("{DOCS}/records/delete"),
            Some(json!({"keys": ["a", ""]})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "keys[1]");

        let (status, body) = call(
            &app,
            "POST",
            &format!("{DOCS}/records/delete"),
            Some(json!({"keys": [1.5]})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "keys[0]");
    }

    #[tokio::test]
    async fn runtime_status_requires_bearer_token_when_auth_is_configured() {
        let app = router(Arc::new(AppState::new(auth_test_config(
            "rest-auth-runtime",
        ))));
        let (status, _) = call(&app, "GET", "/v2/runtime/status", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _, _) = send(
            &app,
            "GET",
            "/v2/runtime/status",
            None,
            Some("operator-secret"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn database_endpoints_round_trip_with_operator_auth() {
        let app = router(Arc::new(AppState::new(auth_test_config(
            "rest-namespace-auth",
        ))));
        let (status, _) = call(&app, "GET", "/v2/databases", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let operator = Some("operator-secret");
        let (status, _, body) = send(&app, "PUT", "/v2/databases/analytics", None, operator).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["name"], "analytics");
        assert_eq!(body["is_default"], false);

        let (status, _, body) = send(&app, "GET", "/v2/databases/analytics", None, operator).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["name"], "analytics");

        let (status, _, body) = send(&app, "GET", "/v2/databases", None, operator).await;
        assert_eq!(status, StatusCode::OK);
        let databases = body["databases"]
            .as_array()
            .expect("databases should be an array");
        assert_eq!(databases.len(), 2);
        assert!(
            databases
                .iter()
                .any(|database| database["name"] == "default")
        );
        assert!(
            databases
                .iter()
                .any(|database| database["name"] == "analytics")
        );

        let (status, _, _) = send(
            &app,
            "DELETE",
            "/v2/databases/analytics",
            None,
            Some("reader-secret"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "only operators drop databases"
        );
        let (status, _, body) =
            send(&app, "DELETE", "/v2/databases/analytics", None, operator).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["database_name"], "analytics");
        let (status, _, _) = send(&app, "GET", "/v2/databases/analytics", None, operator).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
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
        let reader = Some("reader-secret");

        let (status, _, _) = send(&app, "GET", &format!("{DOCS}/stats"), None, reader).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, _) = send(
            &app,
            "POST",
            &format!("{DOCS}/records/get"),
            Some(json!({"keys": ["alpha"]})),
            reader,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        for (path, body) in [
            (
                "records/upsert",
                json!({"records": [{"id": "alpha", "vector": [1.0, 0.0]}]}),
            ),
            ("records/delete", json!({"keys": ["alpha"]})),
        ] {
            let (status, _, _) =
                send(&app, "POST", &format!("{DOCS}/{path}"), Some(body), reader).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
        }
        let (status, _, _) = send(
            &app,
            "PATCH",
            DOCS,
            Some(json!({"drop_field": {"name": "vector"}})),
            reader,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _, _) = send(&app, "DELETE", DOCS, None, reader).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn data_endpoints_run_the_collection_workflow() {
        let app = router(Arc::new(AppState::new(test_config("rest-workflow"))));
        let (status, body) = call(
            &app,
            "POST",
            "/v2/databases/default/collections",
            Some(documents_spec(2, "dot")),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["database_name"], "default");
        assert_eq!(body["schema"]["vectors"][0]["dimensions"], 2);

        let (status, body) = upsert(
            &app,
            json!([
                {"id": "alpha", "vector": [1.0, 0.0], "kind": "keep", "color": "red"},
                {"id": "beta", "vector": [3.0, 0.0], "kind": "drop", "color": "blue"},
                {"id": "gamma", "vector": [2.0, 0.0], "kind": "keep", "color": "red"}
            ]),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["database_name"], "default");
        assert_eq!(body["collection_name"], "documents");
        assert_eq!(body["applied_ops"], 3);
        assert_eq!(body["snapshot"]["manifest_generation"], 0);
        assert_eq!(body["snapshot"]["visible_seq_no"], 3);

        let (status, body) = call(
            &app,
            "POST",
            &format!("{DOCS}/query"),
            Some(json!({"vector": [1.0, 0.0], "top_k": 3, "filters": {"kind": "keep"}})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["database_name"], "default");
        assert_eq!(body["collection_name"], "documents");
        assert_eq!(match_ids(&body), vec!["gamma", "alpha"]);

        let (status, body) = call(
            &app,
            "GET",
            &format!("{DOCS}/stats?snapshot_manifest_generation=0&snapshot_visible_seq_no=3"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["database_name"], "default");
        assert_eq!(body["live_record_count"], 3);
        assert_eq!(body["deleted_record_count"], 0);
        assert_eq!(body["mutable_op_count"], 3);
        assert_eq!(body["segment_count"], 0);

        let (status, body) = call(&app, "GET", &format!("{DOCS}/inspect?target=wal"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["database_name"], "default");
        assert_eq!(body["collection_name"], "documents");
        assert_eq!(body["target"], "wal");
        assert_eq!(
            body["payload"]["records"]
                .as_array()
                .expect("wal records should be an array")
                .len(),
            3
        );

        for action in ["flush", "compact"] {
            let (status, body) = call(&app, "POST", &format!("{DOCS}/{action}"), None).await;
            assert_eq!(status, StatusCode::OK, "{action}");
            assert_eq!(body["database_name"], "default");
            assert_eq!(body["collection_name"], "documents");
        }

        let (status, body) = call(
            &app,
            "GET",
            &format!("{DOCS}/inspect?target=manifest"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["target"], "manifest");
        let segment_id = body["payload"]["segments"][0]["segment_id"]
            .as_str()
            .expect("segment id should be a string")
            .to_owned();
        let (status, body) = call(
            &app,
            "GET",
            &format!("{DOCS}/inspect?target=segment&segment_id={segment_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["target"], format!("segment:{segment_id}"));
        assert_eq!(
            body["payload"]["records"]
                .as_array()
                .expect("segment records should be an array")
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn data_endpoints_support_read_barriers() {
        let app = router(Arc::new(AppState::new(test_config("rest-read-barrier"))));
        create_documents(&app).await;
        let (status, write) = upsert(
            &app,
            json!([{"id": "alpha", "vector": [1.0, 0.0], "kind": "keep"}]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, flush) = call(&app, "POST", &format!("{DOCS}/flush"), None).await;
        assert_eq!(status, StatusCode::OK);

        let (status, body) = call(
            &app,
            "POST",
            &format!("{DOCS}/query"),
            Some(json!({"vector": [1.0, 0.0], "top_k": 1, "read_barrier": write["snapshot"]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["snapshot"]["manifest_generation"],
            flush["manifest_generation"]
        );
        assert_eq!(body["snapshot"]["visible_seq_no"], flush["visible_seq_no"]);
        assert_eq!(body["matches"][0]["id"], "alpha");

        let (status, body) = call(
            &app,
            "GET",
            &format!(
                "{DOCS}/stats?read_barrier_manifest_generation=0&read_barrier_visible_seq_no=1"
            ),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["manifest_generation"], flush["manifest_generation"]);
        assert_eq!(body["visible_seq_no"], flush["visible_seq_no"]);

        let (status, retry_after, body) = send(
            &app,
            "POST",
            &format!("{DOCS}/query"),
            Some(json!({
                "vector": [1.0, 0.0],
                "top_k": 1,
                "read_barrier": {"manifest_generation": 0, "visible_seq_no": 2}
            })),
            None,
        )
        .await;
        // Waiting never satisfies a barrier on one node, so there is no retry hint.
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(retry_after.is_none());
        assert_eq!(body["code"], "FAILED_PRECONDITION");
        assert_eq!(body["details"]["reason"], "READ_BARRIER_NOT_SATISFIED");
        assert!(body["details"].get("retry_after_ms").is_none());
    }

    #[tokio::test]
    async fn inspect_supports_maintenance_target_and_rejects_empty_segment_ids() {
        let app = router(Arc::new(AppState::new(test_config("rest-maintenance"))));
        create_documents(&app).await;
        let (status, body) = call(
            &app,
            "GET",
            &format!("{DOCS}/inspect?target=maintenance"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["target"], "maintenance");
        let (status, body) = call(
            &app,
            "GET",
            &format!("{DOCS}/inspect?target=segment&segment_id="),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "segment_id");
    }

    #[tokio::test]
    async fn metadata_endpoint_reports_build_identity_fields() {
        let app = router(Arc::new(AppState::new(test_config("rest-metadata"))));
        let (status, body) = call(&app, "GET", "/v2/metadata", None).await;
        assert_eq!(status, StatusCode::OK);
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
        let (status, body) = call(&app, "GET", "/v2/runtime/status", None).await;
        assert_eq!(status, StatusCode::OK);
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
            json!(["rest-node", "rest-peer"])
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
        let (status, body) = call(&app, "GET", &format!("{DOCS}/placement"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["database_name"], "default");
        assert_eq!(body["collection_name"], "documents");
        assert_eq!(body["assigned_node"], "rest-placement");
        assert_eq!(body["assigned_role"], "data");
        assert_eq!(body["route_kind"], "local");
    }

    #[test]
    fn collection_placement_serializes_owner_fields_when_present() {
        let payload = serde_json::to_value(logpose_types::CollectionPlacement {
            collection_id: logpose_types::CollectionId::default(),
            database_name: "analytics".to_owned(),
            collection_name: "documents".to_owned(),
            assigned_node: "owner-a".to_owned(),
            assigned_role: logpose_types::NodeRole::Data,
            owner_node: Some("owner-b".to_owned()),
            ownership_epoch: Some(2),
            route_kind: "recorded".to_owned(),
            route_reason: "ownership epoch 2 is assigned to node 'owner-b'".to_owned(),
        })
        .expect("placement should serialize");

        assert_eq!(payload["owner_node"], "owner-b");
        assert_eq!(payload["ownership_epoch"], 2);
    }

    #[tokio::test]
    async fn routes_select_the_database_by_path() {
        let app = router(Arc::new(AppState::new(test_config("rest-database-path"))));
        let (status, body) = call(
            &app,
            "POST",
            "/v2/databases/analytics/collections",
            Some(documents_spec(2, "dot")),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["database_name"], "analytics");

        let (status, body) = call(
            &app,
            "GET",
            "/v2/databases/analytics/collections/documents",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["database_name"], "analytics");
        assert_eq!(body["name"], "documents");

        let (status, body) = call(&app, "GET", DOCS, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

        let (status, body) = call(
            &app,
            "POST",
            "/v2/databases/analytics/collections/documents/records/upsert",
            Some(json!({"records": [{"id": "a", "vector": [1.0, 0.0]}]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["database_name"], "analytics");

        // A body cannot select another database.
        let (status, _) = call(
            &app,
            "POST",
            "/v2/databases/analytics/collections/documents/records/upsert",
            Some(json!({"database_name": "default", "records": []})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn nodes_without_the_combined_role_reject_collection_lifecycle_changes() {
        for (label, role) in [
            ("rest-data-only", logpose_types::NodeRole::Data),
            ("rest-control-create", logpose_types::NodeRole::Control),
        ] {
            let app = router(Arc::new(AppState::new(test_config_with_role(label, role))));
            let (status, body) = call(
                &app,
                "POST",
                "/v2/databases/default/collections",
                Some(documents_spec(2, "dot")),
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(body["code"], "FAILED_PRECONDITION");
            assert_eq!(body["details"]["reason"], "WRONG_NODE_ROLE");
            assert_eq!(body["details"]["metadata"]["node_role"], role.to_string());
            assert!(body["message"].as_str().is_some_and(|message| {
                message.contains("cannot accept control-plane collection lifecycle mutations")
            }));
        }
    }

    /// Every collection data-plane request, for the routing tests.
    fn data_plane_requests() -> Vec<(&'static str, &'static str, String, Option<Value>)> {
        vec![
            (
                "upsert",
                "POST",
                format!("{DOCS}/records/upsert"),
                Some(json!({"records": [{"id": "alpha", "vector": [1.0, 0.0]}]})),
            ),
            (
                "update",
                "POST",
                format!("{DOCS}/records/update"),
                Some(json!({"records": [{"id": "alpha", "kind": "keep"}]})),
            ),
            (
                "delete",
                "POST",
                format!("{DOCS}/records/delete"),
                Some(json!({"keys": ["alpha"]})),
            ),
            (
                "get",
                "POST",
                format!("{DOCS}/records/get"),
                Some(json!({"keys": ["alpha"]})),
            ),
            (
                "alter",
                "PATCH",
                DOCS.to_owned(),
                Some(json!({"add_field": {"name": "kind", "type": "string"}})),
            ),
            (
                "query",
                "POST",
                format!("{DOCS}/query"),
                Some(json!({"vector": [1.0, 0.0], "top_k": 1})),
            ),
            ("stats", "GET", format!("{DOCS}/stats"), None),
            ("flush", "POST", format!("{DOCS}/flush"), None),
            ("compact", "POST", format!("{DOCS}/compact"), None),
            (
                "inspect",
                "GET",
                format!("{DOCS}/inspect?target=manifest"),
                None,
            ),
        ]
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
        let app = router(Arc::new(AppState::new(test_config_with_root(
            "rest-control-only",
            logpose_types::NodeRole::Control,
            root,
        ))));

        for (operation, method, uri, body) in data_plane_requests() {
            let (status, body) = call(&app, method, &uri, body).await;
            assert_eq!(status, StatusCode::CONFLICT, "{operation}: {body}");
            assert_eq!(body["details"]["reason"], "WRONG_NODE_ROLE", "{operation}");
            assert!(
                body["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("data-plane operations")),
                "{operation} should explain the role mismatch: {body}"
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
        let app = router(Arc::new(AppState::new(test_config_with_root(
            "rest-recorded-node-b",
            logpose_types::NodeRole::Combined,
            root,
        ))));

        for (operation, method, uri, body) in data_plane_requests() {
            let (status, retry_after, body) = send(&app, method, &uri, body, None).await;
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{operation} should be rejected for recorded remote assignments: {body}"
            );
            assert_eq!(retry_after.as_deref(), Some("1"), "{operation}");
            assert_eq!(body["details"]["reason"], "NOT_OWNER", "{operation}");
            assert!(
                body["details"]["metadata"]["owner_node"].is_string(),
                "{operation} should name the owner"
            );
            assert!(
                body["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("not locally served")),
                "{operation} should explain the recorded placement mismatch"
            );
        }
    }

    #[tokio::test]
    async fn missing_collections_and_databases_return_not_found() {
        let app = router(Arc::new(AppState::new(test_config("rest-missing"))));
        for (method, uri, body) in [
            (
                "GET",
                "/v2/databases/default/collections/missing".to_owned(),
                None,
            ),
            (
                "GET",
                "/v2/databases/default/collections/missing/placement".to_owned(),
                None,
            ),
            (
                "DELETE",
                "/v2/databases/default/collections/missing".to_owned(),
                None,
            ),
            (
                "POST",
                "/v2/databases/default/collections/missing/records/get".to_owned(),
                Some(json!({"keys": ["a"]})),
            ),
            ("GET", "/v2/databases/nowhere/collections".to_owned(), None),
            ("DELETE", "/v2/databases/nowhere".to_owned(), None),
        ] {
            let (status, body) = call(&app, method, &uri, body).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{method} {uri}: {body}");
            assert_eq!(body["code"], "NOT_FOUND");
        }
    }

    #[tokio::test]
    async fn create_collection_names_the_invalid_schema_field() {
        let app = router(Arc::new(AppState::new(test_config("rest-bad-schema"))));
        let cases = [
            ("vectors[0].dimensions", documents_spec(0, "dot")),
            ("vectors", {
                let mut spec = documents_spec(2, "dot");
                spec["vectors"] = json!([]);
                spec
            }),
            ("fields[1].name", {
                let mut spec = products_spec();
                spec["fields"][1]["name"] = json!("tenant");
                spec
            }),
            ("fields[0].index", {
                let mut spec = products_spec();
                spec["fields"][0] = json!({"name": "flag", "type": "bool", "index": "sorted"});
                spec
            }),
            ("primary_key.name", {
                let mut spec = documents_spec(2, "dot");
                spec["primary_key"]["name"] = json!("1st");
                spec
            }),
        ];
        for (field, spec) in cases {
            let (status, body) = call(
                &app,
                "POST",
                "/v2/databases/default/collections",
                Some(spec),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {body}");
            assert_eq!(violation(&body), field, "{body}");
        }
        let mut spec = products_spec();
        spec["fields"][2]["type"] = json!("array<array<string>>");
        let (status, body) = call(
            &app,
            "POST",
            "/v2/databases/default/collections",
            Some(spec),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("unknown field type")),
            "{body}"
        );
    }

    #[tokio::test]
    async fn typed_collections_are_created_described_listed_altered_and_dropped() {
        let app = router(Arc::new(AppState::new(test_config("rest-collections"))));
        let (status, _) = call(&app, "PUT", "/v2/databases/shop", None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, created) = call(
            &app,
            "POST",
            "/v2/databases/shop/collections",
            Some(products_spec()),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        let schema = &created["schema"];
        assert_eq!(schema["schema_version"], 1);
        assert_eq!(
            schema["primary_key"],
            json!({"id": 0, "name": "sku", "type": "int64"})
        );
        assert_eq!(
            schema["vectors"][0],
            json!({"id": 1, "name": "embedding", "dimensions": 3, "metric": "cosine"})
        );
        assert_eq!(
            schema["fields"][0],
            json!({"id": 2, "name": "tenant", "type": "string", "index": "inverted", "nullable": false})
        );
        assert_eq!(schema["fields"][1]["index"], "inverted_and_sorted");
        assert_eq!(schema["fields"][2]["type"], "array<string>");
        assert_eq!(schema["fields"][4]["index"], "none");
        assert_eq!(schema["dynamic_fields"], true);
        assert_eq!(schema["retired_names"], json!([]));

        let products = "/v2/databases/shop/collections/products";
        let (status, described) = call(&app, "GET", products, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(described, created);
        let (status, listed) = call(&app, "GET", "/v2/databases/shop/collections", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed, json!({"collections": [created]}));
        let (_, listed) = call(&app, "GET", "/v2/databases/default/collections", None).await;
        assert_eq!(listed, json!({"collections": []}));

        let (status, altered) = call(
            &app,
            "PATCH",
            products,
            Some(json!({"add_field": {"name": "stock", "type": "int64"}})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{altered}");
        assert_eq!(altered["schema"]["schema_version"], 2);
        assert_eq!(altered["schema"]["fields"][5]["name"], "stock");
        assert_eq!(altered["schema"]["fields"][5]["id"], 7);
        let (_, altered) = call(
            &app,
            "PATCH",
            products,
            Some(json!({"rename_field": {"from": "stock", "to": "inventory"}})),
        )
        .await;
        assert_eq!(altered["schema"]["fields"][5]["name"], "inventory");
        assert_eq!(altered["schema"]["retired_names"], json!(["stock"]));
        let (_, altered) = call(
            &app,
            "PATCH",
            products,
            Some(json!({"drop_field": {"name": "attrs"}})),
        )
        .await;
        assert_eq!(altered["schema"]["schema_version"], 4);
        assert_eq!(
            altered["schema"]["retired_names"],
            json!(["attrs", "stock"])
        );
        let (_, described) = call(&app, "GET", products, None).await;
        assert_eq!(described, altered, "describe returns the live schema");

        for (change, field) in [
            (
                json!({"add_field": {"name": "sku2", "type": "int64", "nullable": false}}),
                "add_field.nullable",
            ),
            (
                json!({"add_field": {"name": "tenant", "type": "string"}}),
                "add_field.name",
            ),
            (json!({"drop_field": {"name": "sku"}}), "drop_field.name"),
            (
                json!({"drop_field": {"name": "embedding"}}),
                "drop_field.name",
            ),
            (
                json!({"rename_field": {"from": "nope", "to": "other"}}),
                "rename_field.from",
            ),
            (
                json!({"rename_field": {"from": "price", "to": "$extra"}}),
                "rename_field.to",
            ),
        ] {
            let (status, body) = call(&app, "PATCH", products, Some(change)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {body}");
            assert_eq!(violation(&body), field, "{body}");
        }
        let (status, _) = call(
            &app,
            "PATCH",
            products,
            Some(json!({"drop_field": {"name": "price"}, "add_field": {"name": "x", "type": "bool"}})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "one change per request");

        let (status, body) = call(&app, "DELETE", "/v2/databases/shop", None).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "FAILED_PRECONDITION");
        let (status, body) = call(&app, "DELETE", products, None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({"database_name": "shop", "collection_name": "products"})
        );
        let (status, _) = call(&app, "GET", products, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = call(&app, "DELETE", "/v2/databases/shop", None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = call(&app, "DELETE", "/v2/databases/default", None).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
    }

    #[tokio::test]
    async fn typed_records_round_trip_as_natural_json() {
        let app = router(Arc::new(AppState::new(test_config("rest-typed"))));
        let (status, _) = call(
            &app,
            "POST",
            "/v2/databases/default/collections",
            Some(products_spec()),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let products = "/v2/databases/default/collections/products";

        let (status, ack) = call(
            &app,
            "POST",
            &format!("{products}/records/upsert"),
            Some(json!({"records": [
                {
                    "sku": 1,
                    "embedding": [3.0, 0.0, 4.0],
                    "tenant": "acme",
                    "price": 9.5,
                    "tags": ["a", "b"],
                    "updated_at": "2026-09-28T12:30:00.5+02:00",
                    "attrs": {"size": 3, "big": u64::MAX},
                    "color": "red"
                },
                {"sku": 2, "embedding": [0.0, 1.0, 0.0], "tenant": "acme", "updated_at": 0}
            ]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{ack}");
        assert_eq!(ack["applied_ops"], 2);

        let (status, body) = call(
            &app,
            "POST",
            &format!("{products}/records/get"),
            Some(json!({"keys": [1, 3, 2]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["database_name"], "default");
        assert_eq!(body["collection_name"], "products");
        assert_eq!(body["missing_keys"], json!([3]));
        assert_eq!(body["snapshot"], ack["snapshot"]);
        assert_eq!(
            body["records"][0],
            json!({
                "sku": 1,
                "embedding": [0.6, 0.0, 0.8],
                "tenant": "acme",
                "price": 9.5,
                "tags": ["a", "b"],
                "updated_at": "2026-09-28T10:30:00.500000Z",
                "attrs": {"size": 3, "big": u64::MAX},
                "color": "red"
            }),
            "cosine vectors are normalized, timestamps are RFC 3339 in UTC, and $extra keys \
             are flattened into the document"
        );
        assert_eq!(body["records"][1]["updated_at"], "1970-01-01T00:00:00Z");

        let (status, body) = call(
            &app,
            "POST",
            &format!("{products}/records/get"),
            Some(json!({"keys": [1], "output_fields": ["price", "color"]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["records"][0],
            json!({"sku": 1, "price": 9.5, "color": "red"})
        );

        let (status, body) = call(
            &app,
            "POST",
            &format!("{products}/records/update"),
            Some(json!({"records": [{"sku": 1, "price": null, "tags": ["c"], "color": null, "origin": "eu"}]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (_, body) = call(
            &app,
            "POST",
            &format!("{products}/records/get"),
            Some(json!({"keys": [1]})),
        )
        .await;
        let record = &body["records"][0];
        assert!(
            record.get("price").is_none(),
            "null clears a field: {record}"
        );
        assert!(record.get("color").is_none(), "null removes a dynamic key");
        assert_eq!(record["tags"], json!(["c"]));
        assert_eq!(record["origin"], "eu");
        assert_eq!(record["embedding"], json!([0.6, 0.0, 0.8]));

        let (status, body) = call(
            &app,
            "POST",
            &format!("{products}/records/update"),
            Some(json!({"records": [{"sku": 404, "price": 1.0}]})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["details"]["metadata"]["resource_type"], "record");
        assert_eq!(body["details"]["metadata"]["resource_name"], "404");

        let (status, body) = call(
            &app,
            "POST",
            &format!("{products}/records/delete"),
            Some(json!({"keys": [2, 99]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["applied_ops"], 2);
        let (_, body) = call(
            &app,
            "POST",
            &format!("{products}/records/get"),
            Some(json!({"keys": [2]})),
        )
        .await;
        assert_eq!(body["records"], json!([]));
        assert_eq!(body["missing_keys"], json!([2]));
    }

    #[tokio::test]
    async fn record_validation_errors_name_the_record_field() {
        let app = router(Arc::new(AppState::new(test_config("rest-record-errors"))));
        let (status, _) = call(
            &app,
            "POST",
            "/v2/databases/default/collections",
            Some(products_spec()),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let products = "/v2/databases/default/collections/products";
        let valid = json!({"sku": 1, "embedding": [1.0, 0.0, 0.0], "tenant": "acme"});
        let with = |key: &str, value: Value| {
            let mut record = valid.clone();
            record["sku"] = json!(2);
            record[key] = value;
            record
        };
        let cases = [
            ("records[1].price", with("price", json!("cheap"))),
            ("records[1].tags[1]", with("tags", json!(["a", 1]))),
            ("records[1].tags[0]", with("tags", json!([null]))),
            (
                "records[1].updated_at",
                with("updated_at", json!("yesterday")),
            ),
            ("records[1].tenant", with("tenant", json!(null))),
            ("records[1].sku", with("sku", json!("two"))),
            ("records[1].sku", with("sku", json!(1.5))),
            ("records[1].embedding", with("embedding", json!("up"))),
            ("records[1].$extra", with("$extra", json!({}))),
            ("records[1]", json!(["not", "an", "object"])),
            ("records[1]", valid.clone()),
        ];
        for (field, record) in cases {
            let (status, body) = call(
                &app,
                "POST",
                &format!("{products}/records/upsert"),
                Some(json!({"records": [valid.clone(), record]})),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {body}");
            assert_eq!(violation(&body), field, "{body}");
        }
        let (status, body) = call(
            &app,
            "POST",
            &format!("{products}/records/upsert"),
            Some(json!({"records": [{"sku": 3, "embedding": [0.0, 0.0, 0.0], "tenant": "x"}]})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "records[0].embedding");
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("cannot be normalized")),
            "a cosine vector of all zeros has no direction: {body}"
        );

        let (status, body) = call(
            &app,
            "POST",
            &format!("{products}/records/update"),
            Some(json!({"records": [{"sku": 1}]})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "records[0]");
        let (status, body) = call(
            &app,
            "POST",
            &format!("{products}/records/update"),
            Some(json!({"records": [{"price": 1.0}]})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "records[0].sku");

        for (keys, field) in [(json!(["a"]), "keys[0]"), (json!([1, true]), "keys[1]")] {
            let (status, body) = call(
                &app,
                "POST",
                &format!("{products}/records/get"),
                Some(json!({"keys": keys})),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(violation(&body), field);
        }
        let (status, body) = call(
            &app,
            "POST",
            "/v2/databases/default/collections",
            Some(json!({
                "name": "strict",
                "primary_key": {"name": "id", "type": "string"},
                "vectors": [{"name": "v", "dimensions": 1}],
                "dynamic_fields": false
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = call(
            &app,
            "POST",
            "/v2/databases/default/collections/strict/records/upsert",
            Some(json!({"records": [{"id": "a", "v": [1.0], "extra": 1}]})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "records[0].extra");
        let (status, body) = call(
            &app,
            "POST",
            "/v2/databases/default/collections/strict/records/get",
            Some(json!({"keys": ["a"], "output_fields": ["v", "extra"]})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "output_fields[1]");
    }

    #[tokio::test]
    async fn schema_changes_shadow_dynamic_keys_and_survive_a_restart() {
        let root = unique_temp_dir("rest-schema-restart");
        let config = test_config_with_root(
            "rest-schema-restart",
            logpose_types::NodeRole::Combined,
            root.clone(),
        );
        let app = router(Arc::new(AppState::new(config.clone())));
        create_documents(&app).await;
        let (status, _) = upsert(
            &app,
            json!([{"id": "alpha", "vector": [1.0, 0.0], "color": "red", "size": 1}]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, body) = call(
            &app,
            "PATCH",
            DOCS,
            Some(json!({"add_field": {"name": "color", "type": "string"}})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (_, body) = call(
            &app,
            "POST",
            &format!("{DOCS}/records/get"),
            Some(json!({"keys": ["alpha"]})),
        )
        .await;
        assert_eq!(
            body["records"][0],
            json!({"id": "alpha", "vector": [1.0, 0.0], "size": 1}),
            "the added field reads null on the old row, and the $extra key it names is hidden"
        );

        let (status, body) = upsert(
            &app,
            json!([{"id": "beta", "vector": [0.0, 1.0], "color": "blue"}]),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = upsert(
            &app,
            json!([{"id": "gamma", "vector": [0.0, 1.0], "color": 7}]),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "color is typed now: {body}"
        );
        assert_eq!(violation(&body), "records[0].color");

        let (status, _) = call(
            &app,
            "PATCH",
            DOCS,
            Some(json!({"rename_field": {"from": "color", "to": "colour"}})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = upsert(
            &app,
            json!([{"id": "gamma", "vector": [0.0, 1.0], "color": "green"}]),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(violation(&body), "records[0].color");
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("retired field name")),
            "{body}"
        );
        drop(app);

        let app = router(Arc::new(AppState::new(config)));
        let (status, body) = call(&app, "GET", DOCS, None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["schema"]["schema_version"], 3);
        assert_eq!(body["schema"]["fields"][0]["name"], "colour");
        assert_eq!(body["schema"]["retired_names"], json!(["color"]));
        let (_, body) = call(
            &app,
            "POST",
            &format!("{DOCS}/records/get"),
            Some(json!({"keys": ["alpha", "beta"]})),
        )
        .await;
        assert_eq!(
            body["records"],
            json!([
                {"id": "alpha", "vector": [1.0, 0.0], "size": 1},
                {"id": "beta", "vector": [0.0, 1.0], "colour": "blue"}
            ])
        );
        let (status, _) = call(&app, "POST", &format!("{DOCS}/flush"), None).await;
        assert_eq!(status, StatusCode::OK);
        let (_, after_flush) = call(
            &app,
            "POST",
            &format!("{DOCS}/records/get"),
            Some(json!({"keys": ["alpha", "beta"]})),
        )
        .await;
        assert_eq!(
            after_flush["records"], body["records"],
            "segments read the same as memtables"
        );
        drop(app);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn query_filters_preserve_large_integer_precision() {
        let app = router(Arc::new(AppState::new(test_config("rest-large-integers"))));
        create_documents(&app).await;
        let (status, _) = upsert(
            &app,
            json!([
                {"id": "lower", "vector": [1.0, 0.0], "score": 9_007_199_254_740_992_u64},
                {"id": "higher", "vector": [2.0, 0.0], "score": 9_007_199_254_740_993_u64}
            ]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = call(
            &app,
            "POST",
            &format!("{DOCS}/query"),
            Some(json!({
                "vector": [1.0, 0.0],
                "top_k": 5,
                "filters": {"score": 9_007_199_254_740_993_u64}
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(match_ids(&body), vec!["higher"]);
    }

    #[tokio::test]
    async fn query_accepts_predicate_and_profile_diagnostics() {
        let app = router(Arc::new(AppState::new(test_config(
            "rest-predicate-profile",
        ))));
        create_documents(&app).await;
        let (status, _) = upsert(
            &app,
            json!([
                {"id": "alpha", "vector": [1.0, 0.0], "kind": "keep", "version": 1},
                {"id": "beta", "vector": [2.0, 0.0], "kind": "drop", "version": 2},
                {"id": "gamma", "vector": [3.0, 0.0], "kind": "drop", "version": 3},
                {"id": "delta", "vector": [4.0, 0.0], "kind": "drop", "version": 4},
                {"id": "epsilon", "vector": [5.0, 0.0], "kind": "keep", "version": 5}
            ]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = call(
            &app,
            "POST",
            &format!("{DOCS}/query"),
            Some(json!({
                "vector": [1.0, 0.0],
                "top_k": 1,
                "predicate": {"kind": "comparison", "field": "kind", "operator": "eq", "value": "keep"},
                "explain": "profile"
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(match_ids(&body), vec!["epsilon"]);
        let diagnostics = &body["diagnostics"];
        assert_eq!(diagnostics["chosen_plan"], "predicate_first_exact");
        assert!(
            diagnostics["fallback_reason"]
                .as_str()
                .is_some_and(|reason| !reason.is_empty())
        );
        assert!(
            diagnostics["candidates_merged"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        assert!(
            diagnostics["unit_scan_mix"]["memtable_scan"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        for stage in [
            "planning_micros",
            "prefilter_micros",
            "candidate_generation_micros",
            "postfilter_micros",
            "rerank_micros",
            "merge_micros",
        ] {
            assert!(
                diagnostics["stage_timings"][stage].as_u64().is_some(),
                "profile mode should include {stage}"
            );
        }
    }

    #[tokio::test]
    async fn query_rejects_malformed_requests() {
        let app = router(Arc::new(AppState::new(test_config("rest-query-errors"))));
        create_documents(&app).await;
        let query = format!("{DOCS}/query");
        for request in [
            json!({"vector": [1.0, 0.0], "top_k": 1, "predicate": {"kind": "comparison", "field": "kind", "operator": "eq"}}),
            json!({"vector": [1.0, 0.0], "top_k": 1, "predicate": {"kind": "and", "children": []}}),
            json!({"vector": [1.0, 0.0], "top_k": 1, "filters": {"kind": {"nested": true}}}),
        ] {
            let (status, body) = call(&app, "POST", &query, Some(request)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        }
        let (status, body) = call(
            &app,
            "POST",
            &query,
            Some(json!({"vector": [1.0, 0.0], "top_k": 0})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(violation(&body), "top_k");
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("top_k must be greater than 0"))
        );
    }

    #[tokio::test]
    async fn rest_database_policy_endpoints_round_trip_json_and_role_errors() {
        let combined = router(Arc::new(AppState::new(test_config("rest-policy-combined"))));
        let (status, put) = call(
            &combined,
            "PUT",
            "/v2/databases/default/policy",
            Some(policy_body()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{put}");
        assert_eq!(put["database_name"], "default");
        assert_eq!(put["authentication_mode"], "external_token");
        assert_eq!(
            put["role_bindings"]
                .as_array()
                .expect("bindings should be an array")
                .len(),
            2
        );
        let (status, get) = call(&combined, "GET", "/v2/databases/default/policy", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(get, put);

        let mut with_database = policy_body();
        with_database["database_name"] = json!("other");
        let (status, _) = call(
            &combined,
            "PUT",
            "/v2/databases/default/policy",
            Some(with_database),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "the path names the database"
        );

        let data_only = router(Arc::new(AppState::new(test_config_with_role(
            "rest-policy-data-only",
            logpose_types::NodeRole::Data,
        ))));
        let (status, body) = call(
            &data_only,
            "PUT",
            "/v2/databases/default/policy",
            Some(policy_body()),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["details"]["reason"], "WRONG_NODE_ROLE");
        assert!(body["message"].as_str().is_some_and(|message| {
            message.contains("cannot accept control-plane database mutations")
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

    /// A policy body: the path names its database.
    fn policy_body() -> Value {
        json!({
            "authentication_mode": "external_token",
            "role_bindings": [
                {"principal_name": "ops-admin", "role": "owner"},
                {"principal_name": "reader-service", "role": "read_only"}
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
