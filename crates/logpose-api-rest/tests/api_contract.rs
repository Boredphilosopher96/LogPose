//! API contract checks: the OpenAPI document is a structurally valid OpenAPI 3.1 document, it
//! matches the REST router route for route and method for method, its error schema matches the
//! Rust error taxonomy, and every gRPC RPC has a REST operation and vice versa, apart from an
//! explicit allowlist.

use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use logpose_api_rest::{ErrorBody, http_status, route_paths, router};
use logpose_config::LogPoseConfig;
use logpose_core::AppState;
use logpose_types::{ErrorCode, error::fixtures::one_of_each_variant};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;
use yaml_rust2::{Yaml, YamlLoader};

use logpose_auth as _;
use logpose_catalog as _;
use logpose_query as _;
use logpose_storage as _;
use serde as _;
use tokio as _;
use tower_http as _;

const OPENAPI: &str = include_str!("../../../openapi/logpose.v1.yaml");
const PROTO: &str = include_str!("../../../proto/logpose/v1/logpose.proto");

/// gRPC RPCs without a REST operation, and why.
const GRPC_ONLY: &[(&str, &str)] = &[(
    "BulkWriteCollection",
    "client-streaming ingest has no REST form; REST clients send batches to writeCollection",
)];

/// REST operations without a gRPC RPC, and why.
const REST_ONLY: &[(&str, &str)] = &[(
    "health",
    "gRPC serves the standard grpc.health.v1.Health service instead",
)];

/// Marks YAML the conversion does not support; the structural test rejects it.
const UNSUPPORTED: &str = "<unsupported YAML>";

const HTTP_METHODS: [&str; 5] = ["get", "put", "post", "delete", "patch"];
const JSON_SCHEMA_TYPES: [&str; 7] = [
    "null", "boolean", "object", "array", "number", "string", "integer",
];

fn openapi() -> Value {
    let documents = YamlLoader::load_from_str(OPENAPI).expect("the OpenAPI document parses");
    assert_eq!(documents.len(), 1, "the OpenAPI file holds one document");
    yaml_to_json(&documents[0])
}

fn yaml_to_json(yaml: &Yaml) -> Value {
    match yaml {
        Yaml::Real(value) => value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map_or(Value::Null, Value::Number),
        Yaml::Integer(value) => Value::from(*value),
        Yaml::String(value) => Value::from(value.clone()),
        Yaml::Boolean(value) => Value::from(*value),
        Yaml::Array(items) => Value::Array(items.iter().map(yaml_to_json).collect()),
        Yaml::Hash(entries) => Value::Object(
            entries
                .iter()
                .map(|(key, value)| {
                    let key = match key {
                        Yaml::String(key) => key.clone(),
                        Yaml::Integer(key) => key.to_string(),
                        other => format!("{UNSUPPORTED} mapping key {other:?}"),
                    };
                    (key, yaml_to_json(value))
                })
                .collect::<Map<_, _>>(),
        ),
        Yaml::Null => Value::Null,
        Yaml::Alias(_) | Yaml::BadValue => Value::from(format!("{UNSUPPORTED} node {yaml:?}")),
    }
}

/// Every operation as `(path, method, operation)`.
fn operations(document: &Value) -> Vec<(String, String, Value)> {
    let paths = document["paths"].as_object().expect("paths is a mapping");
    let mut operations = Vec::new();
    for (path, item) in paths {
        let item = item.as_object().expect("a path item is a mapping");
        for (method, operation) in item {
            if HTTP_METHODS.contains(&method.as_str()) {
                operations.push((path.clone(), method.clone(), operation.clone()));
            }
        }
    }
    operations
}

fn resolve<'a>(document: &'a Value, reference: &str) -> Option<&'a Value> {
    let pointer = reference.strip_prefix('#')?;
    document.pointer(pointer)
}

/// Visit every mapping in `value` with its JSON pointer.
fn walk<'a>(
    value: &'a Value,
    pointer: String,
    visit: &mut impl FnMut(&str, &'a Map<String, Value>),
) {
    match value {
        Value::Object(map) => {
            visit(&pointer, map);
            for (key, child) in map {
                walk(child, format!("{pointer}/{key}"), visit);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                walk(child, format!("{pointer}/{index}"), visit);
            }
        }
        _ => {}
    }
}

fn enum_values(document: &Value, schema: &str) -> BTreeSet<String> {
    document["components"]["schemas"][schema]["enum"]
        .as_array()
        .expect("the schema has an enum")
        .iter()
        .map(|value| value.as_str().expect("enum values are strings").to_owned())
        .collect()
}

#[test]
fn openapi_document_is_a_structurally_valid_openapi_3_1_document() {
    let document = openapi();
    let version = document["openapi"].as_str().expect("openapi is a string");
    assert!(
        version.starts_with("3.1."),
        "expected OpenAPI 3.1, got {version}"
    );
    assert!(document["info"]["title"].is_string());
    assert!(document["info"]["version"].is_string());

    assert!(
        !OPENAPI.is_empty() && !document.to_string().contains(UNSUPPORTED),
        "the OpenAPI document uses YAML features (aliases, non-string keys) the check does not support"
    );
    let mut problems = Vec::new();
    walk(&document, String::new(), &mut |pointer, map| {
        if map.contains_key("nullable") {
            problems.push(format!(
                "{pointer}: `nullable` is not OpenAPI 3.1; use `type: [T, \"null\"]`"
            ));
        }
        if let Some(reference) = map.get("$ref") {
            let reference = reference.as_str().unwrap_or_default();
            if resolve(&document, reference).is_none() {
                problems.push(format!("{pointer}: unresolved $ref {reference}"));
            }
        }
        // Only schemas carry JSON Schema types; a property named `type` is not one.
        if pointer.contains("/schema")
            && !pointer.ends_with("/properties")
            && let Some(kind) = map.get("type")
        {
            let kinds = match kind {
                Value::String(kind) => vec![kind.as_str()],
                Value::Array(kinds) => kinds.iter().filter_map(Value::as_str).collect(),
                _ => Vec::new(),
            };
            if kinds.is_empty() || kinds.iter().any(|kind| !JSON_SCHEMA_TYPES.contains(kind)) {
                problems.push(format!("{pointer}: invalid schema type {kind}"));
            }
        }
    });

    let mut operation_ids = BTreeSet::new();
    for (path, _method, operation) in operations(&document) {
        let id = operation["operationId"]
            .as_str()
            .expect("every operation has an operationId");
        if !operation_ids.insert(id.to_owned()) {
            problems.push(format!("duplicate operationId {id}"));
        }
        let responses = operation["responses"]
            .as_object()
            .expect("every operation has responses");
        if !responses.keys().any(|code| code.starts_with('2')) {
            problems.push(format!("{id} documents no success response"));
        }
        for (code, response) in responses {
            let response = match response.get("$ref").and_then(Value::as_str) {
                Some(reference) => resolve(&document, reference).unwrap_or(response),
                None => response,
            };
            if !response["description"].is_string() {
                problems.push(format!("{id} {code}: response has no description"));
            }
            if !code.starts_with('2') {
                let schema = &response["content"]["application/json"]["schema"]["$ref"];
                if schema != "#/components/schemas/ErrorResponse" {
                    problems.push(format!("{id} {code}: error response is not ErrorResponse"));
                }
            }
        }
        for segment in path.split('/') {
            let Some(name) = segment
                .strip_prefix('{')
                .and_then(|segment| segment.strip_suffix('}'))
            else {
                continue;
            };
            let declared = operation["parameters"]
                .as_array()
                .is_some_and(|parameters| {
                    parameters.iter().any(|parameter| {
                        parameter["name"] == name
                            && parameter["in"] == "path"
                            && parameter["required"] == true
                    })
                });
            if !declared {
                problems.push(format!("{id}: path parameter {name} is not declared"));
            }
        }
        if operation.get("requestBody").is_some() && responses.get("413").is_none() {
            problems.push(format!("{id}: takes a body but does not document 413"));
        }
    }

    assert!(
        problems.is_empty(),
        "OpenAPI problems:\n{}",
        problems.join("\n")
    );
}

#[test]
fn openapi_error_schema_matches_the_error_taxonomy() {
    let document = openapi();
    let codes = ErrorCode::ALL
        .iter()
        .map(|code| code.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(enum_values(&document, "ErrorCode"), codes);

    let errors = one_of_each_variant();
    let reasons = errors
        .iter()
        .map(|error| error.reason().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(enum_values(&document, "ErrorReason"), reasons);

    let required = document["components"]["schemas"]["ErrorResponse"]["required"]
        .as_array()
        .expect("ErrorResponse lists required fields")
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let documented_statuses = document["components"]["responses"]
        .as_object()
        .expect("components.responses is a mapping")
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let status_components = BTreeMap::from([
        (400, "InvalidArgument"),
        (401, "Unauthenticated"),
        (403, "PermissionDenied"),
        (404, "NotFound"),
        (409, "Conflict"),
        (413, "PayloadTooLarge"),
        (500, "Internal"),
        (503, "Unavailable"),
    ]);
    for error in &errors {
        let body = serde_json::to_value(ErrorBody::from_error(error)).expect("body serializes");
        let keys = body
            .as_object()
            .expect("the body is an object")
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(keys, required, "{error:?}");
        assert!(
            body["details"]["metadata"]
                .as_object()
                .is_some_and(|metadata| metadata.values().all(Value::is_string)),
            "metadata values are strings: {error:?}"
        );
        let status = http_status(error).as_u16();
        let component = status_components.get(&status);
        assert!(
            component.is_some_and(|component| documented_statuses.contains(*component)),
            "status {status} for {error:?} is not documented"
        );
    }
}

fn lower_camel(name: &str) -> String {
    let mut chars = name.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_ascii_lowercase().to_string() + chars.as_str()
    })
}

fn rpc_names() -> BTreeSet<String> {
    PROTO
        .lines()
        .filter_map(|line| line.trim().strip_prefix("rpc "))
        .map(|rest| {
            rest.split('(')
                .next()
                .expect("an rpc has a name")
                .trim()
                .to_owned()
        })
        .collect()
}

#[test]
fn every_grpc_rpc_has_a_rest_operation_and_vice_versa() {
    let document = openapi();
    let operation_ids = operations(&document)
        .into_iter()
        .map(|(_, _, operation)| {
            operation["operationId"]
                .as_str()
                .expect("operations have ids")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    let rpcs = rpc_names();
    assert!(!rpcs.is_empty(), "the proto declares RPCs");

    for (rpc, reason) in GRPC_ONLY {
        assert!(
            rpcs.contains(*rpc),
            "stale gRPC-only exception {rpc}: {reason}"
        );
    }
    for (operation, reason) in REST_ONLY {
        assert!(
            operation_ids.contains(*operation),
            "stale REST-only exception {operation}: {reason}"
        );
    }

    let grpc_side = rpcs
        .iter()
        .filter(|rpc| !GRPC_ONLY.iter().any(|(name, _)| name == *rpc))
        .map(|rpc| lower_camel(rpc))
        .collect::<BTreeSet<_>>();
    let rest_side = operation_ids
        .iter()
        .filter(|operation| !REST_ONLY.iter().any(|(name, _)| name == *operation))
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        grpc_side.difference(&rest_side).collect::<Vec<_>>(),
        Vec::<&String>::new(),
        "gRPC RPCs without a REST operation (add one or allowlist it in GRPC_ONLY)"
    );
    assert_eq!(
        rest_side.difference(&grpc_side).collect::<Vec<_>>(),
        Vec::<&String>::new(),
        "REST operations without a gRPC RPC (add one or allowlist it in REST_ONLY)"
    );
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-api-contract-{label}-{suffix}"));
    std::fs::create_dir_all(&path).expect("temp dir should be created");
    path
}

#[tokio::test]
async fn rest_router_serves_exactly_the_documented_routes_and_methods() {
    let document = openapi();
    let documented_paths = document["paths"]
        .as_object()
        .expect("paths is a mapping")
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let routed_paths = route_paths()
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(routed_paths, documented_paths);

    let root = unique_temp_dir("routes");
    let app = router(Arc::new(AppState::new(LogPoseConfig {
        node_name: "contract".to_owned(),
        storage_root: root.clone(),
        ..LogPoseConfig::default()
    })));
    let mut problems = Vec::new();
    for path in &documented_paths {
        let concrete = path.replace("{name}", "contract");
        for method in HTTP_METHODS {
            let documented = document["paths"][path].get(method).is_some();
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.to_ascii_uppercase().as_str())
                        .uri(&concrete)
                        .body(Body::empty())
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");
            let status = response.status().as_u16();
            let body = response
                .into_body()
                .collect()
                .await
                .expect("body should read")
                .to_bytes();
            let body = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
            let route_missing =
                status == 405 || body["details"]["metadata"]["resource_type"] == "route";
            if documented && route_missing {
                problems.push(format!("{method} {path} is documented but not routed"));
            }
            if !documented && !route_missing {
                problems.push(format!(
                    "{method} {path} is routed (status {status}) but not documented"
                ));
            }
        }
    }
    let _ = std::fs::remove_dir_all(root);
    assert!(
        problems.is_empty(),
        "route problems:\n{}",
        problems.join("\n")
    );
}
