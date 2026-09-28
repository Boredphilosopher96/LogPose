//! Conversions between the protobuf messages and the domain types, shared by the server and the
//! Rust client.
//!
//! Conversions from proto validate what the proto types cannot express (a oneof with no kind,
//! a non-finite float, an unknown enum value) and report it as an `INVALID_ARGUMENT` that names
//! the request field. Record fields are named by field name below the record, such as
//! `records[2].price`, the same paths the REST API reports for its natural JSON documents.

use crate::proto;
use logpose_auth::{AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding};
use logpose_catalog::CollectionDescriptor;
use logpose_query::{OrderBy, SortDirection};
use logpose_types::{
    CollectionId, DistanceMetric, LogPoseError, RemoteBlobConfig, Snapshot,
    filter::{FilterExpr, RangeBounds},
    record::{PartialUpdate, PrimaryKey, Record, RecordPatch},
    schema::{
        CollectionSchema, CreateCollectionSpec, ElementType, FieldIndex, FieldRef, FieldType,
        PrimaryKeySpec, PrimaryKeyType, ScalarFieldSpec, SchemaChange, VectorFieldSpec,
    },
    value::{Timestamp, Value},
};
use serde_json::{Map, Number, Value as JsonValue, json};
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, LogPoseError>;

fn join(path: &str, field: &str) -> String {
    if path.is_empty() {
        field.to_owned()
    } else {
        format!("{path}.{field}")
    }
}

// ----- Names -----

/// A required database or collection name from a request: non-empty after trimming.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming `field` for an empty name.
pub fn required_name(field: &str, value: String) -> Result<String> {
    if value.trim().is_empty() {
        return Err(LogPoseError::invalid_field(
            field,
            format!("{field} must not be empty"),
        ));
    }
    Ok(value)
}

// ----- Enums -----

/// The proto form of a distance metric.
#[must_use]
pub fn metric_to_proto(metric: DistanceMetric) -> proto::DistanceMetric {
    match metric {
        DistanceMetric::Cosine => proto::DistanceMetric::Cosine,
        DistanceMetric::Dot => proto::DistanceMetric::Dot,
        DistanceMetric::L2 => proto::DistanceMetric::L2,
    }
}

/// A distance metric from proto; unspecified is the default, cosine.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming `path` for an unknown value.
pub fn metric_from_proto(metric: i32, path: &str) -> Result<DistanceMetric> {
    match proto::DistanceMetric::try_from(metric) {
        Ok(proto::DistanceMetric::Unspecified | proto::DistanceMetric::Cosine) => {
            Ok(DistanceMetric::Cosine)
        }
        Ok(proto::DistanceMetric::Dot) => Ok(DistanceMetric::Dot),
        Ok(proto::DistanceMetric::L2) => Ok(DistanceMetric::L2),
        Err(_) => Err(LogPoseError::invalid_field(
            path,
            format!("unknown distance metric {metric}"),
        )),
    }
}

fn primary_key_type_to_proto(key_type: PrimaryKeyType) -> proto::PrimaryKeyType {
    match key_type {
        PrimaryKeyType::String => proto::PrimaryKeyType::String,
        PrimaryKeyType::Int64 => proto::PrimaryKeyType::Int64,
    }
}

fn primary_key_type_from_proto(key_type: i32, path: &str) -> Result<PrimaryKeyType> {
    match proto::PrimaryKeyType::try_from(key_type) {
        Ok(proto::PrimaryKeyType::String) => Ok(PrimaryKeyType::String),
        Ok(proto::PrimaryKeyType::Int64) => Ok(PrimaryKeyType::Int64),
        Ok(proto::PrimaryKeyType::Unspecified) => Err(LogPoseError::invalid_field(
            path,
            "primary key type must be STRING or INT64",
        )),
        Err(_) => Err(LogPoseError::invalid_field(
            path,
            format!("unknown primary key type {key_type}"),
        )),
    }
}

fn field_type_to_proto(field_type: FieldType) -> proto::FieldType {
    match field_type {
        FieldType::Bool => proto::FieldType::Bool,
        FieldType::Int64 => proto::FieldType::Int64,
        FieldType::Float64 => proto::FieldType::Float64,
        FieldType::String => proto::FieldType::String,
        FieldType::Timestamp => proto::FieldType::Timestamp,
        FieldType::Json => proto::FieldType::Json,
        FieldType::Array(ElementType::Bool) => proto::FieldType::ArrayBool,
        FieldType::Array(ElementType::Int64) => proto::FieldType::ArrayInt64,
        FieldType::Array(ElementType::Float64) => proto::FieldType::ArrayFloat64,
        FieldType::Array(ElementType::String) => proto::FieldType::ArrayString,
        FieldType::Array(ElementType::Timestamp) => proto::FieldType::ArrayTimestamp,
    }
}

fn field_type_from_proto(field_type: i32, path: &str) -> Result<FieldType> {
    let parsed = proto::FieldType::try_from(field_type).map_err(|_| {
        LogPoseError::invalid_field(path, format!("unknown field type {field_type}"))
    })?;
    Ok(match parsed {
        proto::FieldType::Unspecified => {
            return Err(LogPoseError::invalid_field(path, "field type must be set"));
        }
        proto::FieldType::Bool => FieldType::Bool,
        proto::FieldType::Int64 => FieldType::Int64,
        proto::FieldType::Float64 => FieldType::Float64,
        proto::FieldType::String => FieldType::String,
        proto::FieldType::Timestamp => FieldType::Timestamp,
        proto::FieldType::Json => FieldType::Json,
        proto::FieldType::ArrayBool => FieldType::Array(ElementType::Bool),
        proto::FieldType::ArrayInt64 => FieldType::Array(ElementType::Int64),
        proto::FieldType::ArrayFloat64 => FieldType::Array(ElementType::Float64),
        proto::FieldType::ArrayString => FieldType::Array(ElementType::String),
        proto::FieldType::ArrayTimestamp => FieldType::Array(ElementType::Timestamp),
    })
}

fn field_index_to_proto(index: FieldIndex) -> proto::FieldIndex {
    match index {
        FieldIndex::Auto => proto::FieldIndex::Auto,
        FieldIndex::None => proto::FieldIndex::None,
        FieldIndex::Inverted => proto::FieldIndex::Inverted,
        FieldIndex::Sorted => proto::FieldIndex::Sorted,
        FieldIndex::InvertedAndSorted => proto::FieldIndex::InvertedAndSorted,
    }
}

fn field_index_from_proto(index: i32, path: &str) -> Result<FieldIndex> {
    match proto::FieldIndex::try_from(index) {
        Ok(proto::FieldIndex::Auto) => Ok(FieldIndex::Auto),
        Ok(proto::FieldIndex::None) => Ok(FieldIndex::None),
        Ok(proto::FieldIndex::Inverted) => Ok(FieldIndex::Inverted),
        Ok(proto::FieldIndex::Sorted) => Ok(FieldIndex::Sorted),
        Ok(proto::FieldIndex::InvertedAndSorted) => Ok(FieldIndex::InvertedAndSorted),
        Err(_) => Err(LogPoseError::invalid_field(
            path,
            format!("unknown field index {index}"),
        )),
    }
}

/// The proto form of an authentication mode.
#[must_use]
pub fn authentication_mode_to_proto(mode: AuthenticationMode) -> proto::AuthenticationMode {
    match mode {
        AuthenticationMode::Disabled => proto::AuthenticationMode::Disabled,
        AuthenticationMode::Password => proto::AuthenticationMode::Password,
        AuthenticationMode::MutualTls => proto::AuthenticationMode::MutualTls,
        AuthenticationMode::ExternalToken => proto::AuthenticationMode::ExternalToken,
    }
}

/// An authentication mode from proto.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming `authentication_mode` for an unset or unknown value.
pub fn authentication_mode_from_proto(mode: i32) -> Result<AuthenticationMode> {
    match proto::AuthenticationMode::try_from(mode) {
        Ok(proto::AuthenticationMode::Disabled) => Ok(AuthenticationMode::Disabled),
        Ok(proto::AuthenticationMode::Password) => Ok(AuthenticationMode::Password),
        Ok(proto::AuthenticationMode::MutualTls) => Ok(AuthenticationMode::MutualTls),
        Ok(proto::AuthenticationMode::ExternalToken) => Ok(AuthenticationMode::ExternalToken),
        Ok(proto::AuthenticationMode::Unspecified) => Err(LogPoseError::invalid_field(
            "authentication_mode",
            "authentication mode is required",
        )),
        Err(_) => Err(LogPoseError::invalid_field(
            "authentication_mode",
            format!("unsupported authentication mode '{mode}'"),
        )),
    }
}

fn database_role_to_proto(role: &DatabaseRole) -> proto::DatabaseRole {
    match role {
        DatabaseRole::Owner => proto::DatabaseRole::Owner,
        DatabaseRole::ReadWrite => proto::DatabaseRole::ReadWrite,
        DatabaseRole::ReadOnly => proto::DatabaseRole::ReadOnly,
    }
}

fn database_role_from_proto(role: i32, path: &str) -> Result<DatabaseRole> {
    match proto::DatabaseRole::try_from(role) {
        Ok(proto::DatabaseRole::Owner) => Ok(DatabaseRole::Owner),
        Ok(proto::DatabaseRole::ReadWrite) => Ok(DatabaseRole::ReadWrite),
        Ok(proto::DatabaseRole::ReadOnly) => Ok(DatabaseRole::ReadOnly),
        Ok(proto::DatabaseRole::Unspecified) => Err(LogPoseError::invalid_field(
            path,
            "database role is required",
        )),
        Err(_) => Err(LogPoseError::invalid_field(
            path,
            format!("unsupported database role '{role}'"),
        )),
    }
}

// ----- Database policies -----

/// The reply form of a database access policy.
#[must_use]
pub fn database_policy_to_proto(policy: DatabaseAccessPolicy) -> proto::DatabaseAccessPolicyReply {
    proto::DatabaseAccessPolicyReply {
        database_name: policy.database_name,
        authentication_mode: authentication_mode_to_proto(policy.authentication_mode) as i32,
        role_bindings: policy
            .role_bindings
            .iter()
            .map(|binding| proto::DatabaseRoleBinding {
                principal_name: binding.principal_name.clone(),
                role: database_role_to_proto(&binding.role) as i32,
            })
            .collect(),
    }
}

/// A database access policy of `database_name` from its mode and bindings.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the bad field.
pub fn database_policy_from_proto(
    database_name: String,
    authentication_mode: i32,
    role_bindings: Vec<proto::DatabaseRoleBinding>,
) -> Result<DatabaseAccessPolicy> {
    Ok(DatabaseAccessPolicy {
        authentication_mode: authentication_mode_from_proto(authentication_mode)?,
        role_bindings: role_bindings
            .into_iter()
            .enumerate()
            .map(|(index, binding)| {
                Ok(DatabaseRoleBinding {
                    database_name: database_name.clone(),
                    principal_name: binding.principal_name,
                    role: database_role_from_proto(
                        binding.role,
                        &format!("role_bindings[{index}].role"),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        database_name,
    })
}

// ----- Values -----

/// The proto form of a typed value.
#[must_use]
pub fn value_to_proto(value: Value) -> proto::Value {
    use proto::value::Kind;
    let kind = match value {
        Value::Null => Kind::NullValue(proto::NullValue::NullValue as i32),
        Value::Bool(value) => Kind::BoolValue(value),
        Value::Int64(value) => Kind::Int64Value(value),
        Value::Float64(value) => Kind::Float64Value(value),
        Value::String(value) => Kind::StringValue(value),
        Value::Timestamp(value) => Kind::TimestampMicros(value.as_micros()),
        Value::Array(values) => Kind::ArrayValue(proto::ValueArray {
            values: values.into_iter().map(value_to_proto).collect(),
        }),
        Value::Json(json) => Kind::JsonValue(json_to_proto(&json)),
    };
    proto::Value { kind: Some(kind) }
}

/// A typed value from proto. Whether it fits its field's type is checked against the schema
/// later.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming `path` for a value with no kind, a non-finite float, or
/// a timestamp outside years 0000 to 9999.
pub fn value_from_proto(value: proto::Value, path: &str) -> Result<Value> {
    use proto::value::Kind;
    let invalid = |message: String| LogPoseError::invalid_field(path, message);
    match value.kind {
        None => Err(invalid("a value must set exactly one kind".to_owned())),
        Some(Kind::NullValue(_)) => Ok(Value::Null),
        Some(Kind::BoolValue(value)) => Ok(Value::Bool(value)),
        Some(Kind::Int64Value(value)) => Ok(Value::Int64(value)),
        Some(Kind::Float64Value(value)) => {
            Value::float64(value).map_err(|error| invalid(error.to_string()))
        }
        Some(Kind::StringValue(value)) => Ok(Value::String(value)),
        Some(Kind::TimestampMicros(micros)) => Timestamp::from_micros(micros)
            .map(Value::Timestamp)
            .map_err(|error| invalid(error.to_string())),
        Some(Kind::ArrayValue(array)) => array
            .values
            .into_iter()
            .enumerate()
            .map(|(index, value)| value_from_proto(value, &format!("{path}[{index}]")))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        Some(Kind::JsonValue(json)) => json_from_proto(json, path).map(Value::Json),
    }
}

/// The proto form of a JSON document.
#[must_use]
pub fn json_to_proto(json: &JsonValue) -> proto::JsonValue {
    use proto::json_value::Kind;
    let kind = match json {
        JsonValue::Null => Kind::NullValue(proto::NullValue::NullValue as i32),
        JsonValue::Bool(value) => Kind::BoolValue(*value),
        JsonValue::Number(number) => number_to_proto(number),
        JsonValue::String(value) => Kind::StringValue(value.clone()),
        JsonValue::Array(values) => Kind::ArrayValue(proto::JsonArray {
            values: values.iter().map(json_to_proto).collect(),
        }),
        JsonValue::Object(object) => Kind::ObjectValue(json_object_to_proto(object)),
    };
    proto::JsonValue { kind: Some(kind) }
}

fn number_to_proto(number: &Number) -> proto::json_value::Kind {
    use proto::json_value::Kind;
    if let Some(value) = number.as_i64() {
        Kind::Int64Value(value)
    } else if let Some(value) = number.as_u64() {
        Kind::Uint64Value(value)
    } else {
        // serde_json numbers are finite, so this is never the fallback zero.
        Kind::Float64Value(number.as_f64().unwrap_or_default())
    }
}

/// A JSON document from proto.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming `path` for a node with no kind or a non-finite float.
pub fn json_from_proto(json: proto::JsonValue, path: &str) -> Result<JsonValue> {
    use proto::json_value::Kind;
    match json.kind {
        None => Err(LogPoseError::invalid_field(
            path,
            "a JSON value must set exactly one kind",
        )),
        Some(Kind::NullValue(_)) => Ok(JsonValue::Null),
        Some(Kind::BoolValue(value)) => Ok(JsonValue::Bool(value)),
        Some(Kind::Int64Value(value)) => Ok(JsonValue::from(value)),
        Some(Kind::Uint64Value(value)) => Ok(JsonValue::from(value)),
        Some(Kind::Float64Value(value)) => Number::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| LogPoseError::invalid_field(path, "JSON numbers must be finite")),
        Some(Kind::StringValue(value)) => Ok(JsonValue::String(value)),
        Some(Kind::ArrayValue(array)) => array
            .values
            .into_iter()
            .enumerate()
            .map(|(index, value)| json_from_proto(value, &format!("{path}[{index}]")))
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),
        Some(Kind::ObjectValue(object)) => {
            json_object_from_proto(object, path).map(JsonValue::Object)
        }
    }
}

/// The proto form of a JSON object.
#[must_use]
pub fn json_object_to_proto(object: &Map<String, JsonValue>) -> proto::JsonObject {
    proto::JsonObject {
        fields: object
            .iter()
            .map(|(key, value)| (key.clone(), json_to_proto(value)))
            .collect(),
    }
}

/// A JSON object from proto. Each member's path is `path.key`.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the offending member.
pub fn json_object_from_proto(
    object: proto::JsonObject,
    path: &str,
) -> Result<Map<String, JsonValue>> {
    object
        .fields
        .into_iter()
        .map(|(key, value)| {
            let value = json_from_proto(value, &join(path, &key))?;
            Ok((key, value))
        })
        .collect()
}

// ----- Keys and records -----

/// The proto form of a primary key.
#[must_use]
pub fn primary_key_to_proto(pk: PrimaryKey) -> proto::PrimaryKey {
    use proto::primary_key::Kind;
    proto::PrimaryKey {
        kind: Some(match pk {
            PrimaryKey::Int64(value) => Kind::Int64Value(value),
            PrimaryKey::String(value) => Kind::StringValue(value),
        }),
    }
}

/// A primary key from proto.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming `path` for a key with no kind.
pub fn primary_key_from_proto(pk: Option<proto::PrimaryKey>, path: &str) -> Result<PrimaryKey> {
    use proto::primary_key::Kind;
    match pk.and_then(|pk| pk.kind) {
        Some(Kind::Int64Value(value)) => Ok(PrimaryKey::Int64(value)),
        Some(Kind::StringValue(value)) => Ok(PrimaryKey::String(value)),
        None => Err(LogPoseError::invalid_field(
            path,
            "a primary key must be set to an int64 or a string",
        )),
    }
}

/// Primary keys from proto, each named `field[i]`.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the first key with no kind.
pub fn primary_keys_from_proto(
    keys: Vec<proto::PrimaryKey>,
    field: &str,
) -> Result<Vec<PrimaryKey>> {
    keys.into_iter()
        .enumerate()
        .map(|(index, key)| primary_key_from_proto(Some(key), &format!("{field}[{index}]")))
        .collect()
}

fn vectors_to_proto(vectors: BTreeMap<String, Vec<f32>>) -> BTreeMap<String, proto::Vector> {
    vectors
        .into_iter()
        .map(|(name, values)| (name, proto::Vector { values }))
        .collect()
}

fn fields_to_proto(fields: BTreeMap<String, Value>) -> BTreeMap<String, proto::Value> {
    fields
        .into_iter()
        .map(|(name, value)| (name, value_to_proto(value)))
        .collect()
}

type Parts = (
    BTreeMap<String, Vec<f32>>,
    BTreeMap<String, Value>,
    Map<String, JsonValue>,
);

fn parts_from_proto(
    vectors: impl IntoIterator<Item = (String, proto::Vector)>,
    fields: impl IntoIterator<Item = (String, proto::Value)>,
    extra: Option<proto::JsonObject>,
    path: &str,
) -> Result<Parts> {
    let vectors = vectors
        .into_iter()
        .map(|(name, vector)| (name, vector.values))
        .collect();
    let fields = fields
        .into_iter()
        .map(|(name, value)| {
            let value = value_from_proto(value, &join(path, &name))?;
            Ok((name, value))
        })
        .collect::<Result<_>>()?;
    let extra = match extra {
        Some(extra) => json_object_from_proto(extra, path)?,
        None => Map::new(),
    };
    Ok((vectors, fields, extra))
}

/// The proto form of a record.
#[must_use]
pub fn record_to_proto(record: Record) -> proto::Record {
    proto::Record {
        pk: Some(primary_key_to_proto(record.pk)),
        vectors: vectors_to_proto(record.vectors).into_iter().collect(),
        fields: fields_to_proto(record.fields).into_iter().collect(),
        extra: (!record.extra.is_empty()).then(|| json_object_to_proto(&record.extra)),
    }
}

/// A record from proto, before schema validation.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the bad member below `path`, such as
/// `records[2].price`.
pub fn record_from_proto(record: proto::Record, path: &str) -> Result<Record> {
    let pk = primary_key_from_proto(record.pk, &join(path, "pk"))?;
    let (vectors, fields, extra) =
        parts_from_proto(record.vectors, record.fields, record.extra, path)?;
    Ok(Record {
        pk,
        vectors,
        fields,
        extra,
    })
}

/// Records from proto, the `i`th named `field[i]`.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the first bad member.
pub fn records_from_proto(records: Vec<proto::Record>, field: &str) -> Result<Vec<Record>> {
    records
        .into_iter()
        .enumerate()
        .map(|(index, record)| record_from_proto(record, &format!("{field}[{index}]")))
        .collect()
}

/// The proto form of a partial update.
#[must_use]
pub fn update_to_proto(update: PartialUpdate) -> proto::RecordUpdate {
    proto::RecordUpdate {
        pk: Some(primary_key_to_proto(update.pk)),
        vectors: vectors_to_proto(update.vectors).into_iter().collect(),
        fields: fields_to_proto(update.fields).into_iter().collect(),
        extra: (!update.extra.is_empty()).then(|| json_object_to_proto(&update.extra)),
    }
}

/// Partial updates from proto, the `i`th named `field[i]`.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the first bad member.
pub fn updates_from_proto(
    updates: Vec<proto::RecordUpdate>,
    field: &str,
) -> Result<Vec<PartialUpdate>> {
    updates
        .into_iter()
        .enumerate()
        .map(|(index, update)| {
            let path = format!("{field}[{index}]");
            let pk = primary_key_from_proto(update.pk, &join(&path, "pk"))?;
            let (vectors, fields, extra) =
                parts_from_proto(update.vectors, update.fields, update.extra, &path)?;
            Ok(PartialUpdate {
                pk,
                vectors,
                fields,
                extra,
            })
        })
        .collect()
}

// ----- Filters, orders, and patches -----

/// The proto form of a filter.
#[must_use]
pub fn filter_to_proto(filter: FilterExpr) -> proto::Filter {
    use proto::filter::Node;
    let list = |children: Vec<FilterExpr>| proto::FilterList {
        filters: children.into_iter().map(filter_to_proto).collect(),
    };
    let field_value = |field: String, value: Value| proto::FieldValue {
        field,
        value: Some(value_to_proto(value)),
    };
    let field_values = |field: String, values: Vec<Value>| proto::FieldValues {
        field,
        values: values.into_iter().map(value_to_proto).collect(),
    };
    let node = match filter {
        FilterExpr::And(children) => Node::And(list(children)),
        FilterExpr::Or(children) => Node::Or(list(children)),
        FilterExpr::Not(child) => Node::Not(Box::new(filter_to_proto(*child))),
        FilterExpr::Eq { field, value } => Node::Eq(field_value(field, value)),
        FilterExpr::Ne { field, value } => Node::Ne(field_value(field, value)),
        FilterExpr::Contains { field, value } => Node::Contains(field_value(field, value)),
        FilterExpr::In { field, values } => Node::In(field_values(field, values)),
        FilterExpr::NotIn { field, values } => Node::NotIn(field_values(field, values)),
        FilterExpr::ContainsAny { field, values } => Node::ContainsAny(field_values(field, values)),
        FilterExpr::Range { field, bounds } => Node::Range(proto::FieldRange {
            field,
            gt: bounds.gt.map(value_to_proto),
            gte: bounds.gte.map(value_to_proto),
            lt: bounds.lt.map(value_to_proto),
            lte: bounds.lte.map(value_to_proto),
        }),
        FilterExpr::Exists { field } => Node::Exists(field),
        FilterExpr::IsNull { field } => Node::IsNull(field),
    };
    proto::Filter { node: Some(node) }
}

/// A filter from proto. Nodes are named by their path in the REST form of the filter, the paths
/// the query crate reports its own checks at: `filter.and[1].range.price.gte`.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the node for a filter with no node, a comparison without a
/// field path or an operand, or an operand that is not a valid value, and for a filter past
/// the limits of [`FilterExpr::check_limits`].
pub fn filter_from_proto(filter: proto::Filter, path: &str) -> Result<FilterExpr> {
    let filter = filter_node_from_proto(filter, path, 1)?;
    filter.check_limits(path)?;
    Ok(filter)
}

fn filter_node_from_proto(filter: proto::Filter, path: &str, depth: usize) -> Result<FilterExpr> {
    use proto::filter::Node;
    if depth > logpose_types::filter::MAX_FILTER_DEPTH {
        return Err(logpose_types::filter::too_deep(path));
    }
    let Some(node) = filter.node else {
        return Err(LogPoseError::invalid_field(
            path,
            "a filter must set exactly one node",
        ));
    };
    let operator = match &node {
        Node::And(_) => "and",
        Node::Or(_) => "or",
        Node::Not(_) => "not",
        Node::Eq(_) => "eq",
        Node::Ne(_) => "ne",
        Node::Range(_) => "range",
        Node::In(_) => "in",
        Node::NotIn(_) => "not_in",
        Node::Contains(_) => "contains",
        Node::ContainsAny(_) => "contains_any",
        Node::Exists(_) => "exists",
        Node::IsNull(_) => "is_null",
    };
    let node_path = format!("{path}.{operator}");
    let field_path = |field: &str| -> Result<String> {
        if field.is_empty() {
            return Err(LogPoseError::invalid_field(
                &node_path,
                format!("'{operator}' needs a field path"),
            ));
        }
        Ok(format!("{node_path}.{field}"))
    };
    let operand = |value: Option<proto::Value>, at: &str| -> Result<Value> {
        let value = value.ok_or_else(|| {
            LogPoseError::invalid_field(at, format!("'{operator}' needs an operand"))
        })?;
        value_from_proto(value, at)
    };
    let operands = |values: Vec<proto::Value>, at: &str| -> Result<Vec<Value>> {
        values
            .into_iter()
            .enumerate()
            .map(|(index, value)| value_from_proto(value, &format!("{at}[{index}]")))
            .collect()
    };
    let list = |list: proto::FilterList| -> Result<Vec<FilterExpr>> {
        list.filters
            .into_iter()
            .enumerate()
            .map(|(index, child)| {
                filter_node_from_proto(child, &format!("{node_path}[{index}]"), depth + 1)
            })
            .collect()
    };
    Ok(match node {
        Node::And(children) => FilterExpr::And(list(children)?),
        Node::Or(children) => FilterExpr::Or(list(children)?),
        Node::Not(child) => FilterExpr::Not(Box::new(filter_node_from_proto(
            *child,
            &node_path,
            depth + 1,
        )?)),
        Node::Eq(comparison) | Node::Ne(comparison) | Node::Contains(comparison) => {
            let at = field_path(&comparison.field)?;
            let value = operand(comparison.value, &at)?;
            let field = comparison.field;
            match operator {
                "eq" => FilterExpr::Eq { field, value },
                "ne" => FilterExpr::Ne { field, value },
                _ => FilterExpr::Contains { field, value },
            }
        }
        Node::In(comparison) | Node::NotIn(comparison) | Node::ContainsAny(comparison) => {
            let at = field_path(&comparison.field)?;
            let values = operands(comparison.values, &at)?;
            let field = comparison.field;
            match operator {
                "in" => FilterExpr::In { field, values },
                "not_in" => FilterExpr::NotIn { field, values },
                _ => FilterExpr::ContainsAny { field, values },
            }
        }
        Node::Range(range) => {
            let at = field_path(&range.field)?;
            let bound = |value: Option<proto::Value>, name: &str| {
                value
                    .map(|value| value_from_proto(value, &format!("{at}.{name}")))
                    .transpose()
            };
            FilterExpr::Range {
                bounds: RangeBounds {
                    gt: bound(range.gt, "gt")?,
                    gte: bound(range.gte, "gte")?,
                    lt: bound(range.lt, "lt")?,
                    lte: bound(range.lte, "lte")?,
                },
                field: range.field,
            }
        }
        Node::Exists(field) | Node::IsNull(field) => {
            if field.is_empty() {
                return Err(LogPoseError::invalid_field(
                    node_path,
                    format!("'{operator}' needs a field path"),
                ));
            }
            if operator == "exists" {
                FilterExpr::Exists { field }
            } else {
                FilterExpr::IsNull { field }
            }
        }
    })
}

/// The proto form of an order.
#[must_use]
pub fn order_by_to_proto(order: OrderBy) -> proto::OrderBy {
    proto::OrderBy {
        field: order.field,
        direction: match order.direction {
            SortDirection::Asc => proto::SortDirection::Asc,
            SortDirection::Desc => proto::SortDirection::Desc,
        } as i32,
    }
}

/// Orders from proto, the `i`th named `field[i]`.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` for an unknown direction.
pub fn order_by_from_proto(orders: Vec<proto::OrderBy>, field: &str) -> Result<Vec<OrderBy>> {
    orders
        .into_iter()
        .enumerate()
        .map(|(index, order)| {
            let direction = match proto::SortDirection::try_from(order.direction) {
                Ok(proto::SortDirection::Asc) => SortDirection::Asc,
                Ok(proto::SortDirection::Desc) => SortDirection::Desc,
                Err(_) => {
                    return Err(LogPoseError::invalid_field(
                        format!("{field}[{index}].direction"),
                        format!("unknown sort direction {}", order.direction),
                    ));
                }
            };
            Ok(OrderBy {
                field: order.field,
                direction,
            })
        })
        .collect()
}

/// The proto form of a patch.
#[must_use]
pub fn patch_to_proto(patch: RecordPatch) -> proto::RecordPatch {
    proto::RecordPatch {
        vectors: vectors_to_proto(patch.vectors).into_iter().collect(),
        fields: fields_to_proto(patch.fields).into_iter().collect(),
        extra: (!patch.extra.is_empty()).then(|| json_object_to_proto(&patch.extra)),
    }
}

/// A patch from proto, before schema validation; members are named below `path`.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the first bad member.
pub fn patch_from_proto(patch: proto::RecordPatch, path: &str) -> Result<RecordPatch> {
    let (vectors, fields, extra) =
        parts_from_proto(patch.vectors, patch.fields, patch.extra, path)?;
    Ok(RecordPatch {
        vectors,
        fields,
        extra,
    })
}

// ----- Snapshots -----

/// The proto form of a snapshot.
#[must_use]
pub fn snapshot_to_proto(snapshot: Snapshot) -> proto::Snapshot {
    proto::Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
    }
}

/// A snapshot from proto.
#[must_use]
pub fn snapshot_from_proto(snapshot: proto::Snapshot) -> Snapshot {
    Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no,
    }
}

// ----- Schemas -----

/// The proto form of a scalar field declaration.
#[must_use]
pub fn scalar_spec_to_proto(spec: ScalarFieldSpec) -> proto::ScalarFieldSpec {
    proto::ScalarFieldSpec {
        name: spec.name,
        r#type: field_type_to_proto(spec.field_type) as i32,
        index: field_index_to_proto(spec.index) as i32,
        nullable: Some(spec.nullable),
    }
}

/// A scalar field declaration from proto; `nullable` defaults to true.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming `path.type` or `path.index` for an unknown value.
pub fn scalar_spec_from_proto(spec: proto::ScalarFieldSpec, path: &str) -> Result<ScalarFieldSpec> {
    Ok(ScalarFieldSpec {
        field_type: field_type_from_proto(spec.r#type, &join(path, "type"))?,
        index: field_index_from_proto(spec.index, &join(path, "index"))?,
        nullable: spec.nullable.unwrap_or(true),
        name: spec.name,
    })
}

/// The create request for `spec` in `database_name`.
#[must_use]
pub fn create_request_to_proto(
    database_name: String,
    spec: CreateCollectionSpec,
) -> proto::CreateCollectionRequest {
    proto::CreateCollectionRequest {
        database_name,
        collection_name: spec.name,
        primary_key: Some(proto::PrimaryKeySpec {
            name: spec.primary_key.name,
            r#type: primary_key_type_to_proto(spec.primary_key.key_type) as i32,
        }),
        vectors: spec
            .vectors
            .into_iter()
            .map(|vector| proto::VectorFieldSpec {
                name: vector.name,
                dimensions: vector.dimensions,
                metric: metric_to_proto(vector.metric) as i32,
            })
            .collect(),
        fields: spec.fields.into_iter().map(scalar_spec_to_proto).collect(),
        dynamic_fields: Some(spec.dynamic_fields),
    }
}

/// The spec of a create request; the database name is the caller's to check.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the bad field, such as `fields[1].type`.
pub fn create_spec_from_proto(
    request: proto::CreateCollectionRequest,
) -> Result<CreateCollectionSpec> {
    let primary_key = request.primary_key.ok_or_else(|| {
        LogPoseError::invalid_field("primary_key", "a collection needs a primary key")
    })?;
    Ok(CreateCollectionSpec {
        name: required_name("collection_name", request.collection_name)?,
        primary_key: PrimaryKeySpec {
            key_type: primary_key_type_from_proto(primary_key.r#type, "primary_key.type")?,
            name: primary_key.name,
        },
        vectors: request
            .vectors
            .into_iter()
            .enumerate()
            .map(|(index, vector)| {
                Ok(VectorFieldSpec {
                    metric: metric_from_proto(vector.metric, &format!("vectors[{index}].metric"))?,
                    name: vector.name,
                    dimensions: vector.dimensions,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        fields: request
            .fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| scalar_spec_from_proto(field, &format!("fields[{index}]")))
            .collect::<Result<Vec<_>>>()?,
        dynamic_fields: request.dynamic_fields.unwrap_or(true),
    })
}

/// The proto form of a stored schema.
#[must_use]
pub fn schema_to_proto(schema: &CollectionSchema) -> proto::CollectionSchema {
    let primary_key = schema.primary_key();
    proto::CollectionSchema {
        schema_version: schema.schema_version(),
        next_field_id: schema.next_field_id().0,
        primary_key: Some(proto::PrimaryKeyField {
            id: primary_key.id.0,
            name: primary_key.name.clone(),
            r#type: primary_key_type_to_proto(primary_key.key_type) as i32,
        }),
        vectors: schema
            .vectors()
            .iter()
            .map(|vector| proto::VectorField {
                id: vector.id.0,
                name: vector.name.clone(),
                dimensions: vector.dimensions,
                metric: metric_to_proto(vector.metric) as i32,
            })
            .collect(),
        fields: schema
            .fields()
            .iter()
            .map(|field| proto::ScalarField {
                id: field.id.0,
                name: field.name.clone(),
                r#type: field_type_to_proto(field.field_type) as i32,
                index: field_index_to_proto(field.index) as i32,
                nullable: field.nullable,
            })
            .collect(),
        dynamic_fields: schema.dynamic_fields(),
        retired_names: schema.retired_names().iter().cloned().collect(),
    }
}

/// A stored schema from proto, checked against every schema invariant.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` when the message is not a valid stored schema.
pub fn schema_from_proto(schema: proto::CollectionSchema) -> Result<CollectionSchema> {
    let primary_key = schema
        .primary_key
        .ok_or_else(|| LogPoseError::invalid_field("schema.primary_key", "missing primary key"))?;
    let vectors = schema
        .vectors
        .into_iter()
        .enumerate()
        .map(|(index, vector)| {
            let metric = metric_from_proto(vector.metric, &format!("schema.vectors[{index}]"))?;
            Ok(json!({
                "id": vector.id,
                "name": vector.name,
                "dimensions": vector.dimensions,
                "metric": metric,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let fields = schema
        .fields
        .into_iter()
        .enumerate()
        .map(|(index, field)| {
            let path = format!("schema.fields[{index}]");
            Ok(json!({
                "id": field.id,
                "name": field.name,
                "type": field_type_from_proto(field.r#type, &join(&path, "type"))?,
                "index": field_index_from_proto(field.index, &join(&path, "index"))?,
                "nullable": field.nullable,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let stored = json!({
        "schema_version": schema.schema_version,
        "next_field_id": schema.next_field_id,
        "primary_key": {
            "id": primary_key.id,
            "name": primary_key.name,
            "type": primary_key_type_from_proto(primary_key.r#type, "schema.primary_key.type")?,
        },
        "vectors": vectors,
        "fields": fields,
        "dynamic_fields": schema.dynamic_fields,
        "retired_names": schema.retired_names,
    });
    serde_json::from_value(stored).map_err(|error| {
        LogPoseError::invalid_field("schema", format!("invalid collection schema: {error}"))
    })
}

/// The proto form of a schema change.
#[must_use]
pub fn schema_change_to_proto(change: SchemaChange) -> proto::alter_collection_request::Change {
    use proto::alter_collection_request::Change;
    match change {
        SchemaChange::AddField(spec) => Change::AddField(scalar_spec_to_proto(spec)),
        SchemaChange::DropField { name } => Change::DropField(proto::DropField { name }),
        SchemaChange::RenameField { from, to } => {
            Change::RenameField(proto::RenameField { from, to })
        }
    }
}

/// A schema change from proto.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` for a request with no change or a bad field declaration.
pub fn schema_change_from_proto(
    change: Option<proto::alter_collection_request::Change>,
) -> Result<SchemaChange> {
    use proto::alter_collection_request::Change;
    match change {
        Some(Change::AddField(spec)) => {
            scalar_spec_from_proto(spec, "add_field").map(SchemaChange::AddField)
        }
        Some(Change::DropField(drop)) => Ok(SchemaChange::DropField { name: drop.name }),
        Some(Change::RenameField(rename)) => Ok(SchemaChange::RenameField {
            from: rename.from,
            to: rename.to,
        }),
        None => Err(LogPoseError::invalid_argument(
            "an alter request must set add_field, drop_field, or rename_field",
        )),
    }
}

// ----- Collections -----

/// The reply form of a collection.
#[must_use]
pub fn collection_to_proto(descriptor: CollectionDescriptor) -> proto::CollectionReply {
    proto::CollectionReply {
        collection_id: descriptor.collection_id.to_string(),
        schema: Some(schema_to_proto(&descriptor.schema)),
        root_path: descriptor.root_path.display().to_string(),
        flush_threshold_ops: descriptor.flush_threshold_ops as u64,
        flush_threshold_bytes: descriptor.flush_threshold_bytes as u64,
        compaction_threshold_segments: descriptor.compaction_threshold_segments as u64,
        remote_blob: descriptor
            .remote_blob
            .map(|remote_blob| proto::RemoteBlobConfig {
                endpoint: remote_blob.endpoint,
                bucket: remote_blob.bucket,
                prefix: remote_blob.prefix,
            }),
        database_name: descriptor.database_name,
        name: descriptor.name,
    }
}

/// A collection from its reply.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` for a malformed id or schema.
pub fn collection_from_proto(reply: proto::CollectionReply) -> Result<CollectionDescriptor> {
    let collection_id = reply
        .collection_id
        .parse()
        .map(CollectionId)
        .map_err(|error| {
            LogPoseError::invalid_field("collection_id", format!("invalid collection id: {error}"))
        })?;
    let schema = reply
        .schema
        .ok_or_else(|| LogPoseError::invalid_field("schema", "a collection needs a schema"))
        .and_then(schema_from_proto)?;
    Ok(CollectionDescriptor {
        collection_id,
        database_name: reply.database_name,
        name: reply.name,
        schema,
        root_path: reply.root_path.into(),
        remote_blob: reply.remote_blob.map(|remote| RemoteBlobConfig {
            endpoint: remote.endpoint,
            bucket: remote.bucket,
            prefix: remote.prefix,
        }),
        flush_threshold_ops: reply.flush_threshold_ops as usize,
        flush_threshold_bytes: reply.flush_threshold_bytes as usize,
        compaction_threshold_segments: reply.compaction_threshold_segments as usize,
    })
}

/// Split looked-up records into the found ones and the missing keys, both in request order.
#[must_use]
pub fn split_lookups(
    keys: Vec<PrimaryKey>,
    records: Vec<Option<Record>>,
) -> (Vec<proto::Record>, Vec<proto::PrimaryKey>) {
    let mut found = Vec::new();
    let mut missing = Vec::new();
    for (key, record) in keys.into_iter().zip(records) {
        match record {
            Some(record) => found.push(record_to_proto(record)),
            None => missing.push(primary_key_to_proto(key)),
        }
    }
    (found, missing)
}

/// Whether `name` is a declared vector field of `schema`; used by clients that split a flat
/// document into vectors and scalar fields.
#[must_use]
pub fn is_vector_field(schema: &CollectionSchema, name: &str) -> bool {
    matches!(schema.field(name), Some(FieldRef::Vector(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_types::schema::ScalarFieldSpec;

    fn schema() -> CollectionSchema {
        CreateCollectionSpec {
            name: "products".to_owned(),
            primary_key: PrimaryKeySpec {
                name: "sku".to_owned(),
                key_type: PrimaryKeyType::Int64,
            },
            vectors: vec![VectorFieldSpec {
                name: "embedding".to_owned(),
                dimensions: 3,
                metric: DistanceMetric::Dot,
            }],
            fields: vec![
                ScalarFieldSpec::new("price", FieldType::Float64),
                ScalarFieldSpec::new("tags", FieldType::Array(ElementType::String)),
                ScalarFieldSpec {
                    nullable: false,
                    ..ScalarFieldSpec::new("tenant", FieldType::String)
                },
            ],
            dynamic_fields: false,
        }
        .to_schema()
        .expect("schema")
    }

    #[test]
    fn schemas_round_trip_through_proto_after_changes() {
        let mut schema = schema();
        schema
            .add_field(ScalarFieldSpec::new("updated_at", FieldType::Timestamp))
            .expect("add");
        schema.drop_field("price").expect("drop");
        schema.rename_field("tags", "labels").expect("rename");
        let decoded = schema_from_proto(schema_to_proto(&schema)).expect("decode");
        assert_eq!(decoded, schema);
        assert!(decoded.is_retired("price"));
        assert!(decoded.is_retired("tags"));
    }

    #[test]
    fn create_requests_round_trip_through_proto() {
        let spec = CreateCollectionSpec {
            name: "products".to_owned(),
            primary_key: PrimaryKeySpec {
                name: "sku".to_owned(),
                key_type: PrimaryKeyType::String,
            },
            vectors: vec![VectorFieldSpec {
                name: "embedding".to_owned(),
                dimensions: 768,
                metric: DistanceMetric::L2,
            }],
            fields: vec![ScalarFieldSpec {
                index: FieldIndex::Sorted,
                nullable: false,
                ..ScalarFieldSpec::new("price", FieldType::Float64)
            }],
            dynamic_fields: false,
        };
        let request = create_request_to_proto("shop".to_owned(), spec.clone());
        assert_eq!(request.database_name, "shop");
        assert_eq!(create_spec_from_proto(request).expect("decode"), spec);
    }

    #[test]
    fn create_requests_default_the_metric_nullability_and_dynamic_fields() {
        let spec = create_spec_from_proto(proto::CreateCollectionRequest {
            database_name: "default".to_owned(),
            collection_name: "docs".to_owned(),
            primary_key: Some(proto::PrimaryKeySpec {
                name: "id".to_owned(),
                r#type: proto::PrimaryKeyType::String as i32,
            }),
            vectors: vec![proto::VectorFieldSpec {
                name: "vector".to_owned(),
                dimensions: 2,
                metric: 0,
            }],
            fields: vec![proto::ScalarFieldSpec {
                name: "color".to_owned(),
                r#type: proto::FieldType::String as i32,
                index: 0,
                nullable: None,
            }],
            dynamic_fields: None,
        })
        .expect("decode");
        assert_eq!(spec.vectors[0].metric, DistanceMetric::Cosine);
        assert!(spec.fields[0].nullable);
        assert_eq!(spec.fields[0].index, FieldIndex::Auto);
        assert!(spec.dynamic_fields);
    }

    #[test]
    fn create_requests_name_the_bad_field() {
        let mut request = create_request_to_proto(
            "default".to_owned(),
            CreateCollectionSpec {
                name: "docs".to_owned(),
                primary_key: PrimaryKeySpec {
                    name: "id".to_owned(),
                    key_type: PrimaryKeyType::String,
                },
                vectors: Vec::new(),
                fields: vec![ScalarFieldSpec::new("a", FieldType::Bool)],
                dynamic_fields: true,
            },
        );
        request.fields[0].r#type = 0;
        let error = create_spec_from_proto(request.clone()).expect_err("unset type");
        assert_eq!(error.details().field_violations[0].field, "fields[0].type");

        request.fields[0].r#type = proto::FieldType::Bool as i32;
        request.primary_key = None;
        let error = create_spec_from_proto(request).expect_err("no primary key");
        assert_eq!(error.details().field_violations[0].field, "primary_key");
    }

    #[test]
    fn rest_filters_mean_the_same_through_proto() {
        let schema = CreateCollectionSpec {
            name: "products".to_owned(),
            primary_key: PrimaryKeySpec {
                name: "sku".to_owned(),
                key_type: PrimaryKeyType::Int64,
            },
            vectors: vec![VectorFieldSpec {
                name: "embedding".to_owned(),
                dimensions: 3,
                metric: DistanceMetric::Dot,
            }],
            fields: vec![
                ScalarFieldSpec::new("tenant", FieldType::String),
                ScalarFieldSpec::new("price", FieldType::Float64),
                ScalarFieldSpec::new("tags", FieldType::Array(ElementType::String)),
                ScalarFieldSpec::new("at", FieldType::Timestamp),
                ScalarFieldSpec::new("n", FieldType::Int64),
                ScalarFieldSpec::new("doc", FieldType::Json),
            ],
            dynamic_fields: true,
        }
        .to_schema()
        .expect("schema");
        let filters = [
            json!({ "and": [
                { "eq": { "tenant": "acme" } },
                { "range": { "price": { "gte": 10, "lt": 50.5 } } },
                { "contains": { "tags": "outdoor" } },
                { "not": { "exists": "$extra.archived" } }
            ] }),
            json!({ "or": [
                { "ne": { "sku": 7 } },
                { "in": { "sku": [1, 2, 3] } },
                { "not_in": { "tenant": ["a", "b"] } },
                { "contains_any": { "tags": ["x", "y"] } },
                { "is_null": "price" }
            ] }),
            json!({ "range": { "at": { "gt": "2026-01-01T00:00:00Z", "lte": 1_900_000_000_000_000_i64 } } }),
            json!({ "range": { "n": { "gt": 2.5, "lt": 9 } } }),
            json!({ "in": { "color": ["red", 3, true, 2.5] } }),
            json!({ "range": { "$extra.rank": { "gte": "b", "lt": 4 } } }),
            json!({ "eq": { "doc": "scalar" } }),
            json!({ "not": { "not": { "or": [{ "eq": { "n": 1 } }, { "exists": "doc" }] } } }),
        ];
        for json in filters {
            let filter = FilterExpr::from_json(&schema, json.clone(), "filter")
                .unwrap_or_else(|error| unreachable!("{json}: {error}"));
            let decoded =
                filter_from_proto(filter_to_proto(filter.clone()), "filter").expect("decode");
            assert_eq!(decoded, filter, "{json}");
        }
    }

    #[test]
    fn values_round_trip_through_proto() {
        let values = [
            Value::Null,
            Value::Bool(true),
            Value::Int64(i64::MIN),
            Value::Float64(2.5),
            Value::String("x".to_owned()),
            Value::Timestamp(Timestamp::from_micros(1_790_000_000_000_000).expect("ts")),
            Value::Array(vec![Value::Int64(1), Value::Int64(2)]),
            Value::Json(json!({"a": [1, u64::MAX, -2.5, null, "s", {"b": true}]})),
        ];
        for value in values {
            let decoded = value_from_proto(value_to_proto(value.clone()), "v").expect("decode");
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn invalid_values_name_their_path() {
        let error =
            value_from_proto(proto::Value { kind: None }, "records[0].price").expect_err("no kind");
        assert_eq!(
            error.details().field_violations[0].field,
            "records[0].price"
        );
        let error = value_from_proto(
            proto::Value {
                kind: Some(proto::value::Kind::ArrayValue(proto::ValueArray {
                    values: vec![
                        value_to_proto(Value::Float64(1.0)),
                        proto::Value {
                            kind: Some(proto::value::Kind::Float64Value(f64::NAN)),
                        },
                    ],
                })),
            },
            "records[0].scores",
        )
        .expect_err("NaN");
        assert_eq!(
            error.details().field_violations[0].field,
            "records[0].scores[1]"
        );
        let error = value_from_proto(
            proto::Value {
                kind: Some(proto::value::Kind::TimestampMicros(i64::MAX)),
            },
            "t",
        )
        .expect_err("out of range");
        assert!(error.to_string().contains("out of range"), "{error}");
    }

    #[test]
    fn records_round_trip_through_proto() {
        let mut record = Record::new(7)
            .with_vector("embedding", vec![1.0, 0.0, 0.0])
            .with_field("price", Value::Float64(9.5));
        record.extra.insert("color".to_owned(), json!("red"));
        let decoded =
            record_from_proto(record_to_proto(record.clone()), "records[0]").expect("decode");
        assert_eq!(decoded, record);
        assert!(is_vector_field(&schema(), "embedding"));
        assert!(!is_vector_field(&schema(), "price"));
    }

    #[test]
    fn records_without_a_key_name_it() {
        let error = records_from_proto(
            vec![proto::Record {
                pk: None,
                vectors: Default::default(),
                fields: Default::default(),
                extra: None,
            }],
            "records",
        )
        .expect_err("no key");
        assert_eq!(error.details().field_violations[0].field, "records[0].pk");
    }

    #[test]
    fn schema_changes_round_trip_through_proto() {
        for change in [
            SchemaChange::AddField(ScalarFieldSpec::new("color", FieldType::String)),
            SchemaChange::DropField {
                name: "color".to_owned(),
            },
            SchemaChange::RenameField {
                from: "a".to_owned(),
                to: "b".to_owned(),
            },
        ] {
            let decoded =
                schema_change_from_proto(Some(schema_change_to_proto(change.clone()))).expect("ok");
            assert_eq!(decoded, change);
        }
        assert!(schema_change_from_proto(None).is_err());
    }
}
