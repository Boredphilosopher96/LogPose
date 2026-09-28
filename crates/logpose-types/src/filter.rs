//! The filter AST shared by the APIs, the query crate, and the storage writer.
//!
//! [`FilterExpr`] lives here, not in `logpose-query`, so the storage writer can carry
//! delete-by-filter and update-by-filter requests without depending on the query crate.
//! Evaluation and planning stay in `logpose-query`, which also checks a filter against the
//! schema it reads (unknown fields, operand types) and reports each problem at its path.
//!
//! # Field paths
//!
//! A filter names a field by a path, resolved with [`resolve_field`] against the schema of the
//! state it reads:
//!
//! - a declared name: the primary key or a scalar field (vector fields cannot be filtered);
//! - `$extra.<key>`: the dynamic key `<key>` (everything after the first `.`), which must not be
//!   declared or retired, since such keys are hidden;
//! - with dynamic fields on, any other name: the dynamic key of that name.
//!
//! # JSON shape
//!
//! Requests carry filters as natural JSON (engine plan decision D11), typed by the schema like
//! records ([`FilterExpr::from_json`]):
//!
//! ```json
//! { "and": [
//!   { "eq": { "tenant": "acme" } },
//!   { "range": { "price": { "gte": 10, "lt": 50 } } },
//!   { "contains": { "tags": "outdoor" } },
//!   { "not": { "exists": "$extra.archived" } }
//! ] }
//! ```
//!
//! Every node is an object with one key. Errors name the node with its path, such as
//! `filter.and[1].range.price.gte`; the gRPC API reports the same paths.
//!
//! # Limits
//!
//! A filter nests at most [`MAX_FILTER_DEPTH`] levels (the root is level 1) and has at most
//! [`MAX_FILTER_TERMS`] terms: nodes plus the operands of `in`, `not_in`, and `contains_any`
//! lists. [`FilterExpr::check_limits`] enforces both; parsing and compiling a filter check them.

use crate::{
    LogPoseError,
    record::PrimaryKey,
    schema::{
        CollectionSchema, DYNAMIC_FIELD_NAME, FieldRef, FieldType, PrimaryKeyField, PrimaryKeyType,
        ScalarField,
    },
    value::Value,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

/// Most levels a filter nests: `{"not": {"eq": ...}}` has two.
pub const MAX_FILTER_DEPTH: usize = 32;

/// Most terms a filter has: its nodes plus the operands of its lists.
pub const MAX_FILTER_TERMS: usize = 10_000;

/// A boolean filter over record fields. Operands are typed [`Value`]s; a comparison with a
/// dynamic key or a `json` field uses the operand's JSON form.
///
/// Semantics, per row (see `logpose-query` for the evaluation):
///
/// - a declared field's *keys* are its value (one per array element), none for null;
/// - `eq`/`contains`: some key equals the operand; `in`/`contains_any`: some key is in the list;
/// - `ne`/`not_in`: the row has a key and none equals (is in) the operand(s), so nulls never
///   match;
/// - `range`: some key lies within every given bound;
/// - `exists`: the row has a value (a dynamic key: the key is present); `is_null`: it has none
///   (a dynamic key: present and JSON `null`);
/// - `not`: every live row the child does not match, nulls included.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterExpr {
    /// Every child matches. Must have at least one child.
    And(Vec<FilterExpr>),
    /// Some child matches. Must have at least one child.
    Or(Vec<FilterExpr>),
    /// The child does not match.
    Not(Box<FilterExpr>),
    /// The field equals the value (an array field: some element does).
    Eq {
        /// Field path.
        field: String,
        /// Operand; not null.
        value: Value,
    },
    /// The field has a value other than the operand (an array field: no element equals it).
    Ne {
        /// Field path.
        field: String,
        /// Operand; not null.
        value: Value,
    },
    /// The field lies within the bounds; at least one bound, and not both `gt` and `gte` (or
    /// `lt` and `lte`).
    Range {
        /// Field path.
        field: String,
        /// The bounds.
        bounds: RangeBounds,
    },
    /// The field equals one of the values (an array field: some element does).
    In {
        /// Field path.
        field: String,
        /// Operands; at least one, none null.
        values: Vec<Value>,
    },
    /// The field has a value and it equals none of the values.
    NotIn {
        /// Field path.
        field: String,
        /// Operands; at least one, none null.
        values: Vec<Value>,
    },
    /// An array field has an element equal to the value.
    Contains {
        /// Field path of an array field.
        field: String,
        /// Element operand.
        value: Value,
    },
    /// An array field has an element equal to one of the values.
    ContainsAny {
        /// Field path of an array field.
        field: String,
        /// Element operands; at least one.
        values: Vec<Value>,
    },
    /// The field has a value; a dynamic key is present.
    Exists {
        /// Field path.
        field: String,
    },
    /// The field is null; a dynamic key is present and JSON `null`.
    IsNull {
        /// Field path.
        field: String,
    },
}

/// Bounds of a [`FilterExpr::Range`]. Unset bounds do not constrain.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RangeBounds {
    /// Strictly greater than.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<Value>,
    /// Greater than or equal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<Value>,
    /// Strictly less than.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<Value>,
    /// Less than or equal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<Value>,
}

impl RangeBounds {
    /// The set bounds, by wire name, in the order `gt`, `gte`, `lt`, `lte`.
    #[must_use]
    pub fn named(&self) -> Vec<(&'static str, &Value)> {
        [
            ("gt", &self.gt),
            ("gte", &self.gte),
            ("lt", &self.lt),
            ("lte", &self.lte),
        ]
        .into_iter()
        .filter_map(|(name, bound)| bound.as_ref().map(|bound| (name, bound)))
        .collect()
    }
}

impl FilterExpr {
    /// `field == value`.
    #[must_use]
    pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.into(),
        }
    }

    /// `field != value`.
    #[must_use]
    pub fn ne(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Ne {
            field: field.into(),
            value: value.into(),
        }
    }

    /// `field > value`.
    #[must_use]
    pub fn gt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::range(
            field,
            RangeBounds {
                gt: Some(value.into()),
                ..RangeBounds::default()
            },
        )
    }

    /// `field >= value`.
    #[must_use]
    pub fn gte(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::range(
            field,
            RangeBounds {
                gte: Some(value.into()),
                ..RangeBounds::default()
            },
        )
    }

    /// `field < value`.
    #[must_use]
    pub fn lt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::range(
            field,
            RangeBounds {
                lt: Some(value.into()),
                ..RangeBounds::default()
            },
        )
    }

    /// `field <= value`.
    #[must_use]
    pub fn lte(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::range(
            field,
            RangeBounds {
                lte: Some(value.into()),
                ..RangeBounds::default()
            },
        )
    }

    /// `field` within `bounds`.
    #[must_use]
    pub fn range(field: impl Into<String>, bounds: RangeBounds) -> Self {
        Self::Range {
            field: field.into(),
            bounds,
        }
    }

    /// `field` in `values`.
    #[must_use]
    pub fn in_values(field: impl Into<String>, values: Vec<Value>) -> Self {
        Self::In {
            field: field.into(),
            values,
        }
    }

    /// `field` has a value not in `values`.
    #[must_use]
    pub fn not_in(field: impl Into<String>, values: Vec<Value>) -> Self {
        Self::NotIn {
            field: field.into(),
            values,
        }
    }

    /// The array `field` contains `value`.
    #[must_use]
    pub fn contains(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Contains {
            field: field.into(),
            value: value.into(),
        }
    }

    /// The array `field` contains one of `values`.
    #[must_use]
    pub fn contains_any(field: impl Into<String>, values: Vec<Value>) -> Self {
        Self::ContainsAny {
            field: field.into(),
            values,
        }
    }

    /// `field` has a value.
    #[must_use]
    pub fn exists(field: impl Into<String>) -> Self {
        Self::Exists {
            field: field.into(),
        }
    }

    /// `field` is null.
    #[must_use]
    pub fn is_null(field: impl Into<String>) -> Self {
        Self::IsNull {
            field: field.into(),
        }
    }

    /// Every child matches.
    #[must_use]
    pub fn and(children: Vec<FilterExpr>) -> Self {
        Self::And(children)
    }

    /// Some child matches.
    #[must_use]
    pub fn or(children: Vec<FilterExpr>) -> Self {
        Self::Or(children)
    }

    /// The child does not match.
    #[must_use]
    pub fn negate(child: FilterExpr) -> Self {
        Self::Not(Box::new(child))
    }

    /// The wire name of this node, such as `and` or `range`.
    #[must_use]
    pub fn operator(&self) -> &'static str {
        match self {
            Self::And(_) => "and",
            Self::Or(_) => "or",
            Self::Not(_) => "not",
            Self::Eq { .. } => "eq",
            Self::Ne { .. } => "ne",
            Self::Range { .. } => "range",
            Self::In { .. } => "in",
            Self::NotIn { .. } => "not_in",
            Self::Contains { .. } => "contains",
            Self::ContainsAny { .. } => "contains_any",
            Self::Exists { .. } => "exists",
            Self::IsNull { .. } => "is_null",
        }
    }

    /// Check the filter against [`MAX_FILTER_DEPTH`] and [`MAX_FILTER_TERMS`], without
    /// recursion, so any depth is safe to check. `path` names the filter in the request.
    ///
    /// # Errors
    ///
    /// [`LogPoseError::InvalidArgument`] at the first node past the depth limit, or at `path`
    /// for a filter with too many terms.
    pub fn check_limits(&self, path: &str) -> Result<(), LogPoseError> {
        let mut terms = 0_usize;
        let mut pending = vec![(self, 1_usize, path.to_owned())];
        while let Some((node, depth, node_path)) = pending.pop() {
            if depth > MAX_FILTER_DEPTH {
                return Err(too_deep(&node_path));
            }
            terms += 1;
            let node_path = format!("{node_path}.{}", node.operator());
            match node {
                Self::And(children) | Self::Or(children) => {
                    for (index, child) in children.iter().enumerate() {
                        pending.push((child, depth + 1, format!("{node_path}[{index}]")));
                    }
                }
                Self::Not(child) => pending.push((child, depth + 1, node_path)),
                Self::In { values, .. }
                | Self::NotIn { values, .. }
                | Self::ContainsAny { values, .. } => terms += values.len(),
                _ => {}
            }
            if terms > MAX_FILTER_TERMS {
                return Err(too_many_terms(path));
            }
        }
        Ok(())
    }

    /// Parse a filter from its natural JSON form, typing each operand by the field it compares:
    /// a declared field's operand converts like a record value of that field's type (an array
    /// field's operand like one element), the primary key's like a key, and a dynamic key's or
    /// `json` field's stays JSON. A non-integral bound of an `int64` or `timestamp` range stays
    /// a float, which the range rounds.
    ///
    /// `path` names the filter in the request, usually `filter`; errors name the offending node
    /// below it, such as `filter.or[0].in.tags[2]`.
    ///
    /// # Errors
    ///
    /// [`LogPoseError::InvalidArgument`] for a malformed node, a field path the schema does not
    /// resolve (see [`resolve_field`]), or an operand that does not convert.
    pub fn from_json(
        schema: &CollectionSchema,
        json: JsonValue,
        path: &str,
    ) -> Result<Self, LogPoseError> {
        let filter = parse_node(schema, json, path, 1)?;
        filter.check_limits(path)?;
        Ok(filter)
    }

    /// The natural JSON form: the inverse of [`FilterExpr::from_json`].
    #[must_use]
    pub fn to_json(&self) -> JsonValue {
        let field_value = |field: &str, value: JsonValue| {
            let mut entry = Map::new();
            entry.insert(field.to_owned(), value);
            JsonValue::Object(entry)
        };
        let values =
            |values: &[Value]| JsonValue::Array(values.iter().map(Value::to_json).collect());
        let body = match self {
            Self::And(children) | Self::Or(children) => {
                JsonValue::Array(children.iter().map(Self::to_json).collect())
            }
            Self::Not(child) => child.to_json(),
            Self::Eq { field, value }
            | Self::Ne { field, value }
            | Self::Contains { field, value } => field_value(field, value.to_json()),
            Self::In {
                field,
                values: list,
            }
            | Self::NotIn {
                field,
                values: list,
            }
            | Self::ContainsAny {
                field,
                values: list,
            } => field_value(field, values(list)),
            Self::Range { field, bounds } => field_value(
                field,
                JsonValue::Object(
                    bounds
                        .named()
                        .into_iter()
                        .map(|(name, bound)| (name.to_owned(), bound.to_json()))
                        .collect(),
                ),
            ),
            Self::Exists { field } | Self::IsNull { field } => JsonValue::String(field.clone()),
        };
        let mut node = Map::new();
        node.insert(self.operator().to_owned(), body);
        JsonValue::Object(node)
    }
}

/// What a filter field path names.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FilterTarget<'a> {
    /// The primary key.
    PrimaryKey(&'a PrimaryKeyField),
    /// A declared scalar field.
    Scalar(&'a ScalarField),
    /// A visible key of the dynamic `$extra` field.
    Dynamic(&'a str),
}

/// Resolve a filter field path against `schema` (see the [module documentation](self)).
///
/// # Errors
///
/// A message saying why the path names nothing filterable: a vector field, `$extra` without a
/// key, a dynamic key when dynamic fields are off, a dynamic key the schema hides (declared or
/// retired), or an undeclared name the schema retired or cannot hold.
pub fn resolve_field<'a>(
    schema: &'a CollectionSchema,
    path: &'a str,
) -> Result<FilterTarget<'a>, String> {
    if let Some(rest) = path.strip_prefix(DYNAMIC_FIELD_NAME) {
        let Some(key) = rest.strip_prefix('.').filter(|key| !key.is_empty()) else {
            return Err(format!(
                "'{path}' names no dynamic key; use '{DYNAMIC_FIELD_NAME}.<key>'"
            ));
        };
        if !schema.dynamic_fields() {
            return Err(format!(
                "'{path}' names a dynamic key, and the collection has no dynamic fields"
            ));
        }
        if schema.shadows_dynamic_key(key) {
            return Err(format!(
                "dynamic key '{key}' is hidden because the schema declares or retired the name; filter the declared field by its name"
            ));
        }
        return Ok(FilterTarget::Dynamic(key));
    }
    match schema.field(path) {
        Some(FieldRef::PrimaryKey(field)) => Ok(FilterTarget::PrimaryKey(field)),
        Some(FieldRef::Scalar(field)) => Ok(FilterTarget::Scalar(field)),
        Some(FieldRef::Vector(_)) => {
            Err(format!("'{path}' is a vector field and cannot be filtered"))
        }
        None if schema.is_retired(path) => Err(format!(
            "field '{path}' was dropped or renamed and cannot be filtered"
        )),
        None if schema.dynamic_fields() => Ok(FilterTarget::Dynamic(path)),
        None => Err(format!(
            "field '{path}' is not declared and the collection has no dynamic fields"
        )),
    }
}

/// How a JSON operand converts.
#[derive(Clone, Copy)]
enum Operand {
    /// One value compared for equality (or one array element).
    Exact,
    /// A range bound: a non-integral number stays a float for integer and timestamp fields.
    Bound,
}

/// The error for a node nested past [`MAX_FILTER_DEPTH`].
#[must_use]
pub fn too_deep(path: &str) -> LogPoseError {
    LogPoseError::invalid_field(
        path,
        format!("a filter nests at most {MAX_FILTER_DEPTH} levels"),
    )
}

fn too_many_terms(path: &str) -> LogPoseError {
    LogPoseError::invalid_field(
        path,
        format!(
            "a filter has at most {MAX_FILTER_TERMS} terms (nodes plus the values of its lists)"
        ),
    )
}

fn parse_node(
    schema: &CollectionSchema,
    json: JsonValue,
    path: &str,
    depth: usize,
) -> Result<FilterExpr, LogPoseError> {
    if depth > MAX_FILTER_DEPTH {
        return Err(too_deep(path));
    }
    let JsonValue::Object(object) = json else {
        return Err(LogPoseError::invalid_field(
            path,
            format!(
                "a filter must be a JSON object with one operator key, found {}",
                json_kind(&json)
            ),
        ));
    };
    let mut entries = object.into_iter();
    let (Some((operator, body)), None) = (entries.next(), entries.next()) else {
        return Err(LogPoseError::invalid_field(
            path,
            "a filter must have exactly one operator key",
        ));
    };
    let node_path = format!("{path}.{operator}");
    let invalid = |message: String| LogPoseError::invalid_field(&node_path, message);
    match operator.as_str() {
        "and" | "or" => {
            let JsonValue::Array(items) = body else {
                return Err(invalid(format!("'{operator}' takes an array of filters")));
            };
            let children = items
                .into_iter()
                .enumerate()
                .map(|(index, item)| {
                    parse_node(schema, item, &format!("{node_path}[{index}]"), depth + 1)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(if operator == "and" {
                FilterExpr::And(children)
            } else {
                FilterExpr::Or(children)
            })
        }
        "not" => Ok(FilterExpr::Not(Box::new(parse_node(
            schema,
            body,
            &node_path,
            depth + 1,
        )?))),
        "exists" | "is_null" => {
            let JsonValue::String(field) = body else {
                return Err(invalid(format!("'{operator}' takes a field path string")));
            };
            resolve_field(schema, &field).map_err(&invalid)?;
            Ok(if operator == "exists" {
                FilterExpr::Exists { field }
            } else {
                FilterExpr::IsNull { field }
            })
        }
        "eq" | "ne" | "contains" | "in" | "not_in" | "contains_any" | "range" => {
            let (field, operand_json) = single_entry(body, &node_path, &operator)?;
            let field_path = format!("{node_path}.{field}");
            let target = resolve_field(schema, &field)
                .map_err(|message| LogPoseError::invalid_field(&field_path, message))?;
            let list = |json: JsonValue| -> Result<Vec<Value>, LogPoseError> {
                let JsonValue::Array(items) = json else {
                    return Err(LogPoseError::invalid_field(
                        &field_path,
                        format!("'{operator}' takes an array of values"),
                    ));
                };
                items
                    .into_iter()
                    .enumerate()
                    .map(|(index, item)| {
                        operand(
                            target,
                            item,
                            Operand::Exact,
                            &format!("{field_path}[{index}]"),
                        )
                    })
                    .collect()
            };
            let exact = |json: JsonValue| operand(target, json, Operand::Exact, &field_path);
            Ok(match operator.as_str() {
                "eq" => FilterExpr::Eq {
                    value: exact(operand_json)?,
                    field,
                },
                "ne" => FilterExpr::Ne {
                    value: exact(operand_json)?,
                    field,
                },
                "contains" => FilterExpr::Contains {
                    value: exact(operand_json)?,
                    field,
                },
                "in" => FilterExpr::In {
                    values: list(operand_json)?,
                    field,
                },
                "not_in" => FilterExpr::NotIn {
                    values: list(operand_json)?,
                    field,
                },
                "contains_any" => FilterExpr::ContainsAny {
                    values: list(operand_json)?,
                    field,
                },
                _ => FilterExpr::Range {
                    bounds: parse_bounds(target, operand_json, &field_path)?,
                    field,
                },
            })
        }
        other => Err(LogPoseError::invalid_field(
            path,
            format!(
                "unknown filter operator '{other}'; expected and, or, not, eq, ne, range, in, not_in, contains, contains_any, exists, or is_null"
            ),
        )),
    }
}

/// The one `{field: operand}` entry of a comparison node.
fn single_entry(
    body: JsonValue,
    node_path: &str,
    operator: &str,
) -> Result<(String, JsonValue), LogPoseError> {
    let shape = || {
        LogPoseError::invalid_field(
            node_path,
            format!(
                "'{operator}' takes an object with exactly one field, such as {{\"price\": 10}}"
            ),
        )
    };
    let JsonValue::Object(object) = body else {
        return Err(shape());
    };
    let mut entries = object.into_iter();
    match (entries.next(), entries.next()) {
        (Some(entry), None) => Ok(entry),
        _ => Err(shape()),
    }
}

fn parse_bounds(
    target: FilterTarget<'_>,
    json: JsonValue,
    field_path: &str,
) -> Result<RangeBounds, LogPoseError> {
    let JsonValue::Object(object) = json else {
        return Err(LogPoseError::invalid_field(
            field_path,
            "'range' takes an object of bounds: gt, gte, lt, lte",
        ));
    };
    let mut bounds = RangeBounds::default();
    for (name, bound) in object {
        let bound_path = format!("{field_path}.{name}");
        let slot = match name.as_str() {
            "gt" => &mut bounds.gt,
            "gte" => &mut bounds.gte,
            "lt" => &mut bounds.lt,
            "lte" => &mut bounds.lte,
            other => {
                return Err(LogPoseError::invalid_field(
                    bound_path,
                    format!("unknown range bound '{other}'; expected gt, gte, lt, or lte"),
                ));
            }
        };
        *slot = Some(operand(target, bound, Operand::Bound, &bound_path)?);
    }
    Ok(bounds)
}

/// Type one JSON operand for `target`.
fn operand(
    target: FilterTarget<'_>,
    json: JsonValue,
    kind: Operand,
    path: &str,
) -> Result<Value, LogPoseError> {
    let invalid = |message: String| LogPoseError::invalid_field(path, message);
    if json.is_null() {
        return Err(invalid(
            "a filter operand cannot be null; use is_null or exists".to_owned(),
        ));
    }
    let field_type = match target {
        FilterTarget::Dynamic(_) => return Ok(Value::Json(json)),
        FilterTarget::PrimaryKey(field) => {
            return match (field.key_type, json) {
                (PrimaryKeyType::String, JsonValue::String(value)) => Ok(Value::String(value)),
                (PrimaryKeyType::Int64, number @ JsonValue::Number(_)) => {
                    Value::from_json(number, FieldType::Int64)
                        .map_err(|error| invalid(error.to_string()))
                }
                (key_type, other) => Err(invalid(format!(
                    "primary key '{}' is {key_type}, found {}",
                    field.name,
                    json_kind(&other)
                ))),
            };
        }
        FilterTarget::Scalar(field) => match field.field_type {
            FieldType::Array(element) => FieldType::from(element),
            FieldType::Json => return Ok(Value::Json(json)),
            scalar => scalar,
        },
    };
    if let (Operand::Bound, FieldType::Int64 | FieldType::Timestamp, JsonValue::Number(number)) =
        (kind, field_type, &json)
        && let Some(value) = number.as_f64().filter(|value| value.fract() != 0.0)
    {
        return Value::float64(value).map_err(|error| invalid(error.to_string()));
    }
    Value::from_json(json, field_type).map_err(|error| invalid(error.to_string()))
}

/// Kind name of a JSON value, used in error messages.
fn json_kind(json: &JsonValue) -> &'static str {
    match json {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<PrimaryKey> for Value {
    fn from(value: PrimaryKey) -> Self {
        match value {
            PrimaryKey::Int64(value) => Self::Int64(value),
            PrimaryKey::String(value) => Self::String(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FilterExpr, FilterTarget, MAX_FILTER_DEPTH, MAX_FILTER_TERMS, RangeBounds, resolve_field,
    };
    use crate::{
        ErrorCode, LogPoseError,
        schema::{
            CollectionSchema, CreateCollectionSpec, ElementType, FieldType, PrimaryKeySpec,
            PrimaryKeyType, ScalarFieldSpec, VectorFieldSpec,
        },
        value::{Timestamp, Value},
    };
    use serde_json::json;

    fn schema(dynamic: bool) -> CollectionSchema {
        let mut schema = CreateCollectionSpec {
            name: "products".to_owned(),
            primary_key: PrimaryKeySpec {
                name: "sku".to_owned(),
                key_type: PrimaryKeyType::Int64,
            },
            vectors: vec![VectorFieldSpec {
                name: "embedding".to_owned(),
                dimensions: 2,
                metric: crate::DistanceMetric::Cosine,
            }],
            fields: vec![
                ScalarFieldSpec::new("tenant", FieldType::String),
                ScalarFieldSpec::new("price", FieldType::Float64),
                ScalarFieldSpec::new("stock", FieldType::Int64),
                ScalarFieldSpec::new("tags", FieldType::Array(ElementType::String)),
                ScalarFieldSpec::new("updated_at", FieldType::Timestamp),
                ScalarFieldSpec::new("old", FieldType::Bool),
            ],
            dynamic_fields: dynamic,
        }
        .build_schema()
        .expect("schema should build");
        schema.drop_field("old").expect("drop should apply");
        schema
    }

    fn invalid_path(error: &LogPoseError) -> Option<&str> {
        match error {
            LogPoseError::InvalidArgument { field, .. } => field.as_deref(),
            _ => None,
        }
    }

    #[test]
    fn natural_json_filters_parse_to_typed_operands() {
        let schema = schema(true);
        let filter = FilterExpr::from_json(
            &schema,
            json!({ "and": [
                { "eq": { "tenant": "acme" } },
                { "range": { "price": { "gte": 10, "lt": 50.5 } } },
                { "contains": { "tags": "outdoor" } },
                { "in": { "sku": [1, 2] } },
                { "range": { "stock": { "gt": 2.5 } } },
                { "eq": { "updated_at": "2025-01-01T00:00:00Z" } },
                { "not": { "exists": "$extra.archived" } },
                { "eq": { "color": "red" } }
            ] }),
            "filter",
        )
        .expect("filter should parse");
        let timestamp = Timestamp::parse_rfc3339("2025-01-01T00:00:00Z").expect("timestamp");
        assert_eq!(
            filter,
            FilterExpr::and(vec![
                FilterExpr::eq("tenant", "acme"),
                FilterExpr::range(
                    "price",
                    RangeBounds {
                        gte: Some(Value::Float64(10.0)),
                        lt: Some(Value::Float64(50.5)),
                        ..RangeBounds::default()
                    }
                ),
                FilterExpr::contains("tags", "outdoor"),
                FilterExpr::in_values("sku", vec![Value::Int64(1), Value::Int64(2)]),
                FilterExpr::gt("stock", Value::Float64(2.5)),
                FilterExpr::eq("updated_at", Value::Timestamp(timestamp)),
                FilterExpr::negate(FilterExpr::exists("$extra.archived")),
                FilterExpr::eq("color", Value::Json(json!("red"))),
            ])
        );
        let round_trip =
            FilterExpr::from_json(&schema, filter.to_json(), "filter").expect("round trip");
        assert_eq!(round_trip, filter);
    }

    #[test]
    fn malformed_filters_name_the_offending_node() {
        let schema = schema(false);
        let cases = [
            (json!([]), "filter"),
            (
                json!({ "eq": { "tenant": "a" }, "ne": { "tenant": "b" } }),
                "filter",
            ),
            (json!({ "like": { "tenant": "a" } }), "filter"),
            (json!({ "and": { "eq": { "tenant": "a" } } }), "filter.and"),
            (
                json!({ "or": [{ "eq": { "tenant": 3 } }] }),
                "filter.or[0].eq.tenant",
            ),
            (json!({ "eq": { "tenant": "a", "price": 1 } }), "filter.eq"),
            (json!({ "eq": { "color": "red" } }), "filter.eq.color"),
            (json!({ "eq": { "embedding": 1 } }), "filter.eq.embedding"),
            (json!({ "eq": { "old": true } }), "filter.eq.old"),
            (json!({ "in": { "tags": ["a", 7] } }), "filter.in.tags[1]"),
            (json!({ "in": { "tags": "a" } }), "filter.in.tags"),
            (
                json!({ "range": { "price": { "above": 3 } } }),
                "filter.range.price.above",
            ),
            (
                json!({ "range": { "stock": { "gte": "x" } } }),
                "filter.range.stock.gte",
            ),
            (json!({ "eq": { "stock": 2.5 } }), "filter.eq.stock"),
            (json!({ "eq": { "sku": "7" } }), "filter.eq.sku"),
            (json!({ "eq": { "tenant": null } }), "filter.eq.tenant"),
            (json!({ "not": { "exists": 3 } }), "filter.not.exists"),
            (json!({ "exists": "$extra.color" }), "filter.exists"),
        ];
        for (json, path) in cases {
            let error = FilterExpr::from_json(&schema, json.clone(), "filter")
                .expect_err(&format!("{json} should be rejected"));
            assert_eq!(error.code(), ErrorCode::InvalidArgument, "{json}");
            assert_eq!(invalid_path(&error), Some(path), "{json}: {error}");
        }
    }

    #[test]
    fn filters_past_the_depth_and_term_limits_are_rejected() {
        let schema = schema(false);
        let mut json = json!({ "eq": { "tenant": "a" } });
        for _ in 1..MAX_FILTER_DEPTH {
            json = json!({ "not": json });
        }
        let filter = FilterExpr::from_json(&schema, json.clone(), "filter")
            .expect("a filter at the depth limit should parse");
        assert!(filter.check_limits("filter").is_ok());
        let deeper = json!({ "and": [json] });
        let error = FilterExpr::from_json(&schema, deeper, "filter").expect_err("too deep");
        let expected = format!("filter.and[0]{}", ".not".repeat(MAX_FILTER_DEPTH - 1));
        assert_eq!(invalid_path(&error), Some(expected.as_str()), "{error}");

        // Built directly, far past what any request parser would nest: checked without
        // recursion.
        let mut deep = FilterExpr::eq("tenant", "a");
        for _ in 0..100_000 {
            deep = FilterExpr::negate(deep);
        }
        let error = deep.check_limits("filter").expect_err("too deep");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        let mut deep = Some(deep);
        // Unnest before dropping: dropping 100k nested boxes would recurse.
        while let Some(FilterExpr::Not(child)) = deep.take() {
            deep = Some(*child);
        }

        let wide = FilterExpr::in_values(
            "sku",
            (0..i64::try_from(MAX_FILTER_TERMS).unwrap_or(i64::MAX))
                .map(Value::Int64)
                .collect(),
        );
        let error = wide.check_limits("filter").expect_err("too many terms");
        assert_eq!(invalid_path(&error), Some("filter"));
        let within = FilterExpr::in_values("sku", vec![Value::Int64(1); MAX_FILTER_TERMS - 1]);
        assert!(within.check_limits("filter").is_ok());
    }

    #[test]
    fn field_paths_resolve_dynamic_keys_and_hide_shadowed_ones() {
        let schema = schema(true);
        assert!(matches!(
            resolve_field(&schema, "$extra.color"),
            Ok(FilterTarget::Dynamic("color"))
        ));
        assert!(matches!(
            resolve_field(&schema, "$extra.a.b"),
            Ok(FilterTarget::Dynamic("a.b"))
        ));
        assert!(matches!(
            resolve_field(&schema, "color"),
            Ok(FilterTarget::Dynamic("color"))
        ));
        for hidden in [
            "$extra.tenant",
            "$extra.old",
            "old",
            "$extra",
            "$extra.",
            "embedding",
        ] {
            assert!(resolve_field(&schema, hidden).is_err(), "{hidden}");
        }
    }
}
