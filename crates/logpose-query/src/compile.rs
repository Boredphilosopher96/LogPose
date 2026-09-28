//! Filter compilation: a [`FilterExpr`] checked against a view's schema, evaluated per unit to
//! the bitmap of live rows it matches.
//!
//! # Checks
//!
//! [`CompiledFilter::compile`] resolves every field path with
//! [`resolve_field`] and checks every operand against the
//! field it compares, and reports the first problem as `InvalidArgument` at the node's path
//! below `filter` (the same paths [`FilterExpr::from_json`] uses, such as
//! `filter.and[1].range.price.gte`):
//!
//! - the filter is within the depth and term limits of [`FilterExpr::check_limits`];
//! - `and` and `or` need a child, `in`, `not_in`, and `contains_any` a value, and `range` a
//!   bound, but not both `gt` and `gte` (or `lt` and `lte`);
//! - `contains` and `contains_any` need an array field, and `range` an ordered one (not `bool`);
//! - an operand must be of the field's type (an array field's: of its element type). Numbers
//!   convert between `int64` and `float64` when exact; an `int64` or `timestamp` range bound may
//!   be any float, which rounds to the matching integer range (`x < 3.5` is `x <= 3`). A dynamic
//!   key or `json` field compares with a scalar JSON operand; its range bounds are strings or
//!   numbers.
//!
//! # Semantics
//!
//! A declared field's *keys* are its value's index keys: one per array element, one for a
//! scalar, none for null (or an empty array). `exists` holds when a row has a key and `is_null`
//! when it has none; `eq` and `contains` when some key equals the operand, `in` and
//! `contains_any` when some key is one of them, `range` when some key lies within every bound,
//! and `ne` and `not_in` when the row has a key and none is an operand (SQL-like: nulls never
//! match). Integers and timestamps compare as integers, floats as floats, strings bytewise. The
//! primary key has one key, never null.
//!
//! A dynamic key has JSON semantics: `exists` when the key is present, `is_null` when present
//! and `null`, `eq` and `in` by JSON scalar equality, `ne` and `not_in` when present with a
//! non-null scalar that differs, and the range bounds between two strings or two numbers. A
//! `json` field compares the same way, but is a declared field: `exists` when it has a value and
//! `is_null` when it has none.
//!
//! `not p` is `live AND NOT p`, so it includes rows where `p`'s field is null; `and` and `or`
//! are intersection and union.
//!
//! # Evaluation
//!
//! Per unit, a comparison on a declared field uses the unit's index when one serves it (inverted
//! or sorted for equality, `exists`, and `is_null`; sorted for ranges) and otherwise scans the
//! column over the rows still in play; `$extra` and the key scan rows. `and` evaluates its
//! index-served children first and narrows the rows later children scan.

use logpose_storage::{
    SectionNeed, UnitView,
    cache::PinSet,
    read::{ScalarKey, value_index_keys},
    segment_v2::DYNAMIC_BLOCK_ROWS,
};
use logpose_types::{
    LogPoseError, ScalarMetadataValue,
    filter::{FilterExpr, FilterTarget, RangeBounds, resolve_field},
    record::PrimaryKey,
    schema::{CollectionSchema, ElementType, FieldId, FieldRef, FieldType, PrimaryKeyType},
    value::Value,
};
use roaring::RoaringBitmap;
use serde_json::Value as JsonValue;
use std::{cmp::Ordering, ops::Bound, sync::Arc};

use crate::{QueryError, Result};

/// The request field filters are named under.
pub const FILTER_PATH: &str = "filter";

/// A filter compiled against one schema.
#[derive(Clone, Debug)]
pub struct CompiledFilter {
    root: Node,
    schema: Arc<CollectionSchema>,
}

#[derive(Clone, Debug)]
enum Node {
    And(Vec<Node>),
    Or(Vec<Node>),
    Not(Box<Node>),
    Typed { field: FieldId, cond: Cond },
    Json { target: JsonTarget, cond: JsonCond },
    Key(Cond),
}

#[derive(Clone, Debug)]
enum JsonTarget {
    Dynamic(String),
    Field(FieldId),
}

/// A condition over a row's index keys.
#[derive(Clone, Debug)]
enum Cond {
    IsNull,
    Exists,
    /// Sorted and deduplicated, for binary search.
    AnyOf(Vec<ScalarKey>),
    /// Sorted and deduplicated, for binary search.
    NoneOf(Vec<ScalarKey>),
    Range(Bound<ScalarKey>, Bound<ScalarKey>),
}

impl Cond {
    /// Whether a row with `keys` satisfies the condition.
    fn holds(&self, keys: &[ScalarKey]) -> bool {
        match self {
            Self::IsNull => keys.is_empty(),
            Self::Exists => !keys.is_empty(),
            Self::AnyOf(wanted) => keys.iter().any(|key| wanted.binary_search(key).is_ok()),
            Self::NoneOf(unwanted) => {
                !keys.is_empty() && keys.iter().all(|key| unwanted.binary_search(key).is_err())
            }
            Self::Range(low, high) => keys
                .iter()
                .any(|key| (low.as_ref(), high.as_ref()).contains_key(key)),
        }
    }

    /// The condition's name in `EXPLAIN`.
    fn name(&self) -> &'static str {
        match self {
            Self::IsNull => "is_null",
            Self::Exists => "exists",
            Self::AnyOf(keys) if keys.len() == 1 => "eq",
            Self::AnyOf(_) => "in",
            Self::NoneOf(keys) if keys.len() == 1 => "ne",
            Self::NoneOf(_) => "not_in",
            Self::Range(..) => "range",
        }
    }

    /// Whether an index can answer the condition: any index for everything but ranges,
    /// which need a sorted one.
    fn needs_sorted(&self) -> bool {
        matches!(self, Self::Range(..))
    }

    /// Whether the condition can match no row at all (an empty range).
    fn is_empty_range(&self) -> bool {
        let Self::Range(low, high) = self else {
            return false;
        };
        let (Some(low_key), Some(high_key)) = (bound_key(low), bound_key(high)) else {
            return false;
        };
        match low_key.cmp(high_key) {
            Ordering::Greater => true,
            Ordering::Equal => {
                matches!(low, Bound::Excluded(_)) || matches!(high, Bound::Excluded(_))
            }
            Ordering::Less => false,
        }
    }
}

fn bound_key(bound: &Bound<ScalarKey>) -> Option<&ScalarKey> {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => Some(key),
        Bound::Unbounded => None,
    }
}

/// A condition over a JSON value (a dynamic key or a `json` field).
#[derive(Clone, Debug)]
enum JsonCond {
    Exists,
    IsNull,
    AnyOf(Vec<ScalarMetadataValue>),
    NoneOf(Vec<ScalarMetadataValue>),
    /// `(operator, operand)` pairs that must all hold, the operator one of `gt`, `gte`, `lt`,
    /// `lte`.
    Range(Vec<(&'static str, ScalarMetadataValue)>),
}

impl JsonCond {
    /// The condition's name in `EXPLAIN`.
    fn name(&self) -> &'static str {
        match self {
            Self::Exists => "exists",
            Self::IsNull => "is_null",
            Self::AnyOf(values) if values.len() == 1 => "eq",
            Self::AnyOf(_) => "in",
            Self::NoneOf(values) if values.len() == 1 => "ne",
            Self::NoneOf(_) => "not_in",
            Self::Range(_) => "range",
        }
    }
}

trait BoundsExt {
    fn contains_key(&self, key: &ScalarKey) -> bool;
}

impl BoundsExt for (Bound<&ScalarKey>, Bound<&ScalarKey>) {
    fn contains_key(&self, key: &ScalarKey) -> bool {
        let above = match self.0 {
            Bound::Included(low) => key >= low,
            Bound::Excluded(low) => key > low,
            Bound::Unbounded => true,
        };
        let below = match self.1 {
            Bound::Included(high) => key <= high,
            Bound::Excluded(high) => key < high,
            Bound::Unbounded => true,
        };
        above && below && kind_matches(self.0, key) && kind_matches(self.1, key)
    }
}

/// Keys of different kinds never compare: a bound of another kind excludes the key.
fn kind_matches(bound: Bound<&ScalarKey>, key: &ScalarKey) -> bool {
    match bound {
        Bound::Included(other) | Bound::Excluded(other) => other.kind() == key.kind(),
        Bound::Unbounded => true,
    }
}

impl CompiledFilter {
    /// Check `expr` against `schema` and resolve its field paths. Errors name the offending
    /// node below `filter`.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` at the node's path; see the [module documentation](self).
    pub fn compile(schema: &Arc<CollectionSchema>, expr: &FilterExpr) -> Result<Self> {
        expr.check_limits(FILTER_PATH)?;
        Ok(Self {
            root: compile_node(schema, expr, FILTER_PATH)?,
            schema: Arc::clone(schema),
        })
    }

    /// The sections `unit` must fetch to evaluate the filter.
    #[must_use]
    pub fn needs(&self, unit: &UnitView<'_>) -> Vec<SectionNeed> {
        let mut needs = Vec::new();
        if unit.is_memtable() {
            return needs;
        }
        collect_needs(&self.root, unit, &mut needs);
        needs.dedup();
        needs
    }

    /// Whether the filter may match any row of `unit`, from its zone maps: `false` only when
    /// a conjunct on a declared field provably excludes every row.
    #[must_use]
    pub fn may_match(&self, unit: &UnitView<'_>) -> bool {
        may_match(&self.root, unit, &self.schema)
    }

    /// The live rows of `unit` matching the filter.
    ///
    /// # Errors
    ///
    /// `Internal` if a needed section was not fetched, or typed corruption.
    pub fn evaluate(
        &self,
        unit: &UnitView<'_>,
        pins: &PinSet,
    ) -> logpose_types::Result<RoaringBitmap> {
        let live = unit.live();
        Evaluator { unit, pins }.eval(&self.root, &live)
    }

    /// A one-line description of the filter as `unit` evaluates it: each comparison with the
    /// field and the access path (`index`, `sorted index`, `column`, `$extra`, or `key`), for
    /// `EXPLAIN`.
    #[must_use]
    pub fn describe(&self, unit: &UnitView<'_>) -> String {
        describe(&self.root, unit, &self.schema)
    }

    /// Whether one row satisfies the filter: the reference semantics the per-unit evaluation
    /// follows (tests compare against it).
    #[must_use]
    pub fn matches_record(&self, record: &logpose_types::record::Record) -> bool {
        row_matches(&self.root, &self.schema, record)
    }
}

fn invalid(path: &str, message: impl Into<String>) -> QueryError {
    QueryError::Storage(LogPoseError::invalid_field(path, message))
}

fn compile_node(schema: &CollectionSchema, expr: &FilterExpr, path: &str) -> Result<Node> {
    let node_path = format!("{path}.{}", expr.operator());
    let children = |children: &[FilterExpr]| -> Result<Vec<Node>> {
        if children.is_empty() {
            return Err(invalid(
                &node_path,
                format!("'{}' needs at least one filter", expr.operator()),
            ));
        }
        children
            .iter()
            .enumerate()
            .map(|(index, child)| compile_node(schema, child, &format!("{node_path}[{index}]")))
            .collect()
    };
    Ok(match expr {
        FilterExpr::And(list) => Node::And(children(list)?),
        FilterExpr::Or(list) => Node::Or(children(list)?),
        FilterExpr::Not(child) => Node::Not(Box::new(compile_node(schema, child, &node_path)?)),
        FilterExpr::Exists { field } | FilterExpr::IsNull { field } => {
            let exists = matches!(expr, FilterExpr::Exists { .. });
            let target =
                resolve_field(schema, field).map_err(|message| invalid(&node_path, message))?;
            leaf(
                target,
                if exists { Cond::Exists } else { Cond::IsNull },
                if exists {
                    JsonCond::Exists
                } else {
                    JsonCond::IsNull
                },
            )
        }
        FilterExpr::Eq { field, value }
        | FilterExpr::Ne { field, value }
        | FilterExpr::Contains { field, value } => {
            let field_path = format!("{node_path}.{field}");
            let target = target_for(schema, field, expr, &field_path)?;
            let negated = matches!(expr, FilterExpr::Ne { .. });
            comparison(
                target,
                std::slice::from_ref(value),
                negated,
                &field_path,
                false,
            )?
        }
        FilterExpr::In { field, values }
        | FilterExpr::NotIn { field, values }
        | FilterExpr::ContainsAny { field, values } => {
            let field_path = format!("{node_path}.{field}");
            let target = target_for(schema, field, expr, &field_path)?;
            if values.is_empty() {
                return Err(invalid(
                    &field_path,
                    format!("'{}' needs at least one value", expr.operator()),
                ));
            }
            let negated = matches!(expr, FilterExpr::NotIn { .. });
            comparison(target, values, negated, &field_path, true)?
        }
        FilterExpr::Range { field, bounds } => {
            let field_path = format!("{node_path}.{field}");
            let target = target_for(schema, field, expr, &field_path)?;
            range(target, bounds, &field_path)?
        }
    })
}

/// The node of a condition on `target`.
fn leaf(target: FilterTarget<'_>, cond: Cond, json: JsonCond) -> Node {
    match target {
        FilterTarget::PrimaryKey(_) => Node::Key(cond),
        FilterTarget::Scalar(field) if field.field_type == FieldType::Json => Node::Json {
            target: JsonTarget::Field(field.id),
            cond: json,
        },
        FilterTarget::Scalar(field) => Node::Typed {
            field: field.id,
            cond,
        },
        FilterTarget::Dynamic(key) => Node::Json {
            target: JsonTarget::Dynamic(key.to_owned()),
            cond: json,
        },
    }
}

/// Resolve the field of a comparison and check that the operator fits it.
fn target_for<'a>(
    schema: &'a CollectionSchema,
    field: &'a str,
    expr: &FilterExpr,
    field_path: &str,
) -> Result<FilterTarget<'a>> {
    let target = resolve_field(schema, field).map_err(|message| invalid(field_path, message))?;
    let field_type = match target {
        FilterTarget::Scalar(scalar) => Some(scalar.field_type),
        FilterTarget::PrimaryKey(_) | FilterTarget::Dynamic(_) => None,
    };
    match expr {
        FilterExpr::Contains { .. } | FilterExpr::ContainsAny { .. }
            if !matches!(field_type, Some(FieldType::Array(_))) =>
        {
            Err(invalid(
                field_path,
                format!(
                    "'{}' needs an array field; '{field}' is not one (use eq or in)",
                    expr.operator()
                ),
            ))
        }
        FilterExpr::Range { .. }
            if matches!(
                field_type,
                Some(FieldType::Bool | FieldType::Array(ElementType::Bool))
            ) =>
        {
            Err(invalid(
                field_path,
                format!("'range' needs an ordered field; '{field}' is bool"),
            ))
        }
        _ => Ok(target),
    }
}

/// The key type a target's operands convert to; `None` for JSON targets.
fn key_type(target: FilterTarget<'_>) -> Option<ElementType> {
    match target {
        FilterTarget::PrimaryKey(field) => Some(match field.key_type {
            PrimaryKeyType::Int64 => ElementType::Int64,
            PrimaryKeyType::String => ElementType::String,
        }),
        FilterTarget::Scalar(field) => element_type(field.field_type),
        FilterTarget::Dynamic(_) => None,
    }
}

/// An equality-style comparison (`eq`, `ne`, `in`, `not_in`, `contains`, `contains_any`).
fn comparison(
    target: FilterTarget<'_>,
    values: &[Value],
    negated: bool,
    field_path: &str,
    list: bool,
) -> Result<Node> {
    let at = |index: usize| {
        if list {
            format!("{field_path}[{index}]")
        } else {
            field_path.to_owned()
        }
    };
    match key_type(target) {
        Some(element) => {
            let mut keys = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    exact_key(element, value).map_err(|message| invalid(&at(index), message))
                })
                .collect::<Result<Vec<_>>>()?;
            keys.sort();
            keys.dedup();
            let cond = if negated {
                Cond::NoneOf(keys)
            } else {
                Cond::AnyOf(keys)
            };
            Ok(leaf(target, cond, JsonCond::Exists))
        }
        None => {
            let scalars = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    json_scalar(value).map_err(|message| invalid(&at(index), message))
                })
                .collect::<Result<Vec<_>>>()?;
            let cond = if negated {
                JsonCond::NoneOf(scalars)
            } else {
                JsonCond::AnyOf(scalars)
            };
            Ok(leaf(target, Cond::Exists, cond))
        }
    }
}

/// A `range` node.
fn range(target: FilterTarget<'_>, bounds: &RangeBounds, field_path: &str) -> Result<Node> {
    let named = bounds.named();
    if named.is_empty() {
        return Err(invalid(
            field_path,
            "'range' needs at least one of gt, gte, lt, lte",
        ));
    }
    if bounds.gt.is_some() && bounds.gte.is_some() {
        return Err(invalid(field_path, "'range' takes gt or gte, not both"));
    }
    if bounds.lt.is_some() && bounds.lte.is_some() {
        return Err(invalid(field_path, "'range' takes lt or lte, not both"));
    }
    let Some(element) = key_type(target) else {
        let pairs = named
            .into_iter()
            .map(|(name, value)| {
                let bound_path = format!("{field_path}.{name}");
                let scalar = json_scalar(value).map_err(|message| invalid(&bound_path, message))?;
                if !matches!(
                    scalar,
                    ScalarMetadataValue::String(_) | ScalarMetadataValue::Number(_)
                ) {
                    return Err(invalid(
                        &bound_path,
                        "a range bound on a JSON value must be a string or a number",
                    ));
                }
                Ok((name, scalar))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(leaf(target, Cond::Exists, JsonCond::Range(pairs)));
    };
    let mut low = Bound::Unbounded;
    let mut high = Bound::Unbounded;
    for (name, value) in named {
        let bound_path = format!("{field_path}.{name}");
        let bound =
            range_bound(element, name, value).map_err(|message| invalid(&bound_path, message))?;
        if matches!(name, "gt" | "gte") {
            low = bound;
        } else {
            high = bound;
        }
    }
    Ok(leaf(target, Cond::Range(low, high), JsonCond::Exists))
}

/// The element type a field's values index as; `None` for JSON.
fn element_type(field_type: FieldType) -> Option<ElementType> {
    Some(match field_type {
        FieldType::Bool => ElementType::Bool,
        FieldType::Int64 => ElementType::Int64,
        FieldType::Float64 => ElementType::Float64,
        FieldType::String => ElementType::String,
        FieldType::Timestamp => ElementType::Timestamp,
        FieldType::Array(element) => element,
        FieldType::Json => return None,
    })
}

fn mismatch(element: ElementType, value: &Value) -> String {
    format!(
        "expected an operand of type {element}, found {}",
        value.kind()
    )
}

/// The key equal to `value` in a column of `element` type.
fn exact_key(element: ElementType, value: &Value) -> std::result::Result<ScalarKey, String> {
    match (element, value) {
        (_, Value::Null) => {
            Err("a filter operand cannot be null; use is_null or exists".to_owned())
        }
        (ElementType::Bool, Value::Bool(value)) => Ok(ScalarKey::Bool(*value)),
        (ElementType::Int64, Value::Int64(value)) => Ok(ScalarKey::Int(*value)),
        (ElementType::Int64, Value::Float64(value)) => integral(*value)
            .map(ScalarKey::Int)
            .ok_or_else(|| format!("expected an int64 operand, found the non-integral {value}")),
        (ElementType::Timestamp, Value::Timestamp(value)) => {
            Ok(ScalarKey::timestamp_micros(value.as_micros()))
        }
        (ElementType::Timestamp, Value::Int64(micros)) => Ok(ScalarKey::timestamp_micros(*micros)),
        (ElementType::Timestamp, Value::Float64(value)) => integral(*value)
            .map(ScalarKey::timestamp_micros)
            .ok_or_else(|| {
                format!("expected timestamp microseconds, found the non-integral {value}")
            }),
        (ElementType::Float64, Value::Float64(value)) => float_key(*value),
        #[allow(clippy::cast_precision_loss)]
        (ElementType::Float64, Value::Int64(value)) => {
            let converted = *value as f64;
            #[allow(clippy::cast_possible_truncation)]
            let exact = converted as i64 == *value && converted < 9_223_372_036_854_775_808.0;
            if exact {
                float_key(converted)
            } else {
                Err(format!("int64 operand {value} has no exact float64 form"))
            }
        }
        (ElementType::String, Value::String(value)) => Ok(ScalarKey::string(value.as_str())),
        (element, other) => Err(mismatch(element, other)),
    }
}

fn float_key(value: f64) -> std::result::Result<ScalarKey, String> {
    ScalarKey::float(value).map_err(|_| "a float64 operand must be finite".to_owned())
}

/// The bound a range operator puts on keys of `element` type. A non-integral float bound of an
/// integer or timestamp field rounds to the matching integer bound: `> 3.5` is `>= 4`.
fn range_bound(
    element: ElementType,
    operator: &str,
    value: &Value,
) -> std::result::Result<Bound<ScalarKey>, String> {
    let lower = matches!(operator, "gt" | "gte");
    let inclusive = matches!(operator, "gte" | "lte");
    if let (ElementType::Int64 | ElementType::Timestamp, Value::Float64(float)) = (element, value)
        && integral(*float).is_none()
    {
        if !float.is_finite() {
            return Err("a float64 operand must be finite".to_owned());
        }
        // Past the integer range a bound excludes everything or nothing: no integer lies above
        // a lower bound past `i64::MAX` or below an upper bound under `i64::MIN`. The cast below
        // would saturate those to the extreme integer, which would then match.
        #[allow(clippy::cast_precision_loss)]
        let (min, max) = (i64::MIN as f64, i64::MAX as f64);
        if lower && *float >= max {
            return Ok(Bound::Excluded(ScalarKey::Int(i64::MAX)));
        }
        if !lower && *float < min {
            return Ok(Bound::Excluded(ScalarKey::Int(i64::MIN)));
        }
        #[allow(clippy::cast_possible_truncation)]
        let rounded = if lower { float.ceil() } else { float.floor() } as i64;
        return Ok(Bound::Included(ScalarKey::Int(rounded)));
    }
    let key = match (element, value) {
        (ElementType::Bool, _) => {
            return Err("'range' needs an ordered field; bool is not one".to_owned());
        }
        _ => exact_key(element, value)?,
    };
    Ok(if inclusive {
        Bound::Included(key)
    } else {
        Bound::Excluded(key)
    })
}

/// An integer exactly equal to `value`, if there is one.
fn integral(value: f64) -> Option<i64> {
    #[allow(clippy::cast_precision_loss)]
    let in_range = value >= i64::MIN as f64 && value < i64::MAX as f64;
    #[allow(clippy::cast_possible_truncation)]
    (value.fract() == 0.0 && in_range).then_some(value as i64)
}

/// A JSON target's operand: a scalar, never null.
fn json_scalar(value: &Value) -> std::result::Result<ScalarMetadataValue, String> {
    if value.is_null() {
        return Err("a filter operand cannot be null; use is_null or exists".to_owned());
    }
    let json = value.to_json();
    match ScalarMetadataValue::from_json(&json) {
        Some(ScalarMetadataValue::Null) | None => Err(format!(
            "a JSON value compares with a string, number, or bool operand, found {}",
            json_kind(&json)
        )),
        Some(scalar) => Ok(scalar),
    }
}

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

fn describe(node: &Node, unit: &UnitView<'_>, schema: &CollectionSchema) -> String {
    let list = |children: &[Node]| {
        children
            .iter()
            .map(|child| describe(child, unit, schema))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let field_name = |field: FieldId| {
        schema
            .field_by_id(field)
            .map_or_else(|| field.to_string(), |field| field.name().to_owned())
    };
    match node {
        Node::And(children) => format!("and({})", list(children)),
        Node::Or(children) => format!("or({})", list(children)),
        Node::Not(child) => format!("not({})", describe(child, unit, schema)),
        Node::Typed { field, cond } => {
            let path = if cond.is_empty_range() {
                "nothing"
            } else if unit.has_scalar_index(*field, true) {
                "sorted index"
            } else if !cond.needs_sorted() && unit.has_scalar_index(*field, false) {
                "index"
            } else {
                "column"
            };
            format!("{} {} via {path}", cond.name(), field_name(*field))
        }
        Node::Json { target, cond } => match target {
            JsonTarget::Dynamic(name) => format!("{} $extra.{name} via $extra", cond.name()),
            JsonTarget::Field(field) => {
                format!("{} {} via column", cond.name(), field_name(*field))
            }
        },
        Node::Key(cond) => format!("{} key via key column", cond.name()),
    }
}

fn collect_needs(node: &Node, unit: &UnitView<'_>, needs: &mut Vec<SectionNeed>) {
    match node {
        Node::And(children) | Node::Or(children) => {
            for child in children {
                collect_needs(child, unit, needs);
            }
        }
        Node::Not(child) => collect_needs(child, unit, needs),
        Node::Typed { field, cond } => {
            if cond.is_empty_range() {
                return;
            }
            let need = if unit.has_scalar_index(*field, cond.needs_sorted())
                || (!cond.needs_sorted() && unit.has_scalar_index(*field, true))
            {
                SectionNeed::ScalarIndex(*field)
            } else {
                SectionNeed::Column(*field)
            };
            if !needs.contains(&need) {
                needs.push(need);
            }
        }
        Node::Json { target, .. } => {
            let need = match target {
                JsonTarget::Dynamic(_) => {
                    if !unit.has_dynamic() {
                        return;
                    }
                    SectionNeed::DynamicBlocks(unit.live())
                }
                JsonTarget::Field(field) => SectionNeed::Column(*field),
            };
            if !needs.contains(&need) {
                needs.push(need);
            }
        }
        Node::Key(_) => {
            if !needs.contains(&SectionNeed::Pk) {
                needs.push(SectionNeed::Pk);
            }
        }
    }
}

fn may_match(node: &Node, unit: &UnitView<'_>, schema: &CollectionSchema) -> bool {
    match node {
        Node::And(children) => children.iter().all(|child| may_match(child, unit, schema)),
        Node::Or(children) => children.iter().any(|child| may_match(child, unit, schema)),
        Node::Not(_) | Node::Json { .. } | Node::Key(_) => true,
        Node::Typed { field, cond } => {
            if cond.is_empty_range() {
                return false;
            }
            let Some(zone) = unit.zone(*field) else {
                return true;
            };
            let (min, max, null_count) = (zone.min, zone.max, zone.null_count);
            let Some(FieldRef::Scalar(declared)) = schema.field_by_id(*field) else {
                return true;
            };
            if matches!(declared.field_type, FieldType::Array(_)) {
                return true;
            }
            let decode = |bytes: Option<&[u8]>| {
                let value =
                    logpose_types::value::codec::decode(bytes?, declared.field_type).ok()?;
                value_index_keys(&value).into_iter().next()
            };
            let (min, max) = (decode(min), decode(max));
            let rows = unit.row_count();
            let within = |key: &ScalarKey| match (&min, &max) {
                (Some(min), Some(max)) => min.kind() != key.kind() || (min <= key && key <= max),
                _ => null_count < rows,
            };
            match cond {
                Cond::IsNull => null_count > 0,
                Cond::Exists | Cond::NoneOf(_) => null_count < rows,
                Cond::AnyOf(keys) => keys.iter().any(within),
                Cond::Range(low, high) => match (min, max) {
                    (Some(min), Some(max)) => {
                        let low_ok = match high {
                            Bound::Included(high) => high.kind() != min.kind() || &min <= high,
                            Bound::Excluded(high) => high.kind() != min.kind() || &min < high,
                            Bound::Unbounded => true,
                        };
                        let high_ok = match low {
                            Bound::Included(low) => low.kind() != max.kind() || &max >= low,
                            Bound::Excluded(low) => low.kind() != max.kind() || &max > low,
                            Bound::Unbounded => true,
                        };
                        low_ok && high_ok
                    }
                    _ => null_count < rows,
                },
            }
        }
    }
}

struct Evaluator<'a, 'v> {
    unit: &'a UnitView<'v>,
    pins: &'a PinSet,
}

impl Evaluator<'_, '_> {
    /// Rows of `domain` matching `node`.
    fn eval(&self, node: &Node, domain: &RoaringBitmap) -> logpose_types::Result<RoaringBitmap> {
        if domain.is_empty() {
            return Ok(RoaringBitmap::new());
        }
        match node {
            Node::And(children) => {
                let mut order: Vec<&Node> = children.iter().collect();
                order.sort_by_key(|child| !self.index_served(child));
                let mut rows = domain.clone();
                for child in order {
                    rows = self.eval(child, &rows)?;
                    if rows.is_empty() {
                        break;
                    }
                }
                Ok(rows)
            }
            Node::Or(children) => {
                let mut rows = RoaringBitmap::new();
                for child in children {
                    let mut rest = domain.clone();
                    rest -= &rows;
                    rows |= self.eval(child, &rest)?;
                }
                Ok(rows)
            }
            Node::Not(child) => {
                let mut rows = domain.clone();
                rows -= self.eval(child, domain)?;
                Ok(rows)
            }
            Node::Typed { field, cond } => self.typed(*field, cond, domain),
            Node::Json { target, cond } => {
                let mut rows = RoaringBitmap::new();
                match target {
                    JsonTarget::Dynamic(name) => {
                        if !self.unit.has_dynamic() {
                            // No visible key anywhere: every comparison sees it absent.
                            if json_matches(None, cond) {
                                return Ok(domain.clone());
                            }
                            return Ok(rows);
                        }
                        // A row without the key never matches, so a block none of whose
                        // rows has it is skipped whole; otherwise only the key's member of
                        // each row is decoded.
                        let dynamic = self.unit.dynamic(self.pins)?;
                        let mut block = None;
                        let mut skip = false;
                        for row in domain {
                            let row_block = row / DYNAMIC_BLOCK_ROWS;
                            if block != Some(row_block) {
                                block = Some(row_block);
                                skip = !dynamic.block_may_have(row, name)?;
                            }
                            if skip {
                                continue;
                            }
                            let value = dynamic.member(row, name)?;
                            if json_matches(value.as_ref(), cond) {
                                rows.insert(row);
                            }
                        }
                    }
                    JsonTarget::Field(field) => {
                        let column = self.unit.column(*field, self.pins)?;
                        for row in domain {
                            let value = column
                                .value(row)?
                                .filter(|value| !value.is_null())
                                .map(Value::into_json);
                            if json_field_matches(value.as_ref(), cond) {
                                rows.insert(row);
                            }
                        }
                    }
                }
                Ok(rows)
            }
            Node::Key(cond) => {
                let pks = self.unit.pks(self.pins)?;
                let mut rows = RoaringBitmap::new();
                for row in domain {
                    let keys: Vec<ScalarKey> =
                        pks.pk_at(row).map(|pk| pk_key(&pk)).into_iter().collect();
                    if cond.holds(&keys) {
                        rows.insert(row);
                    }
                }
                Ok(rows)
            }
        }
    }

    fn index_served(&self, node: &Node) -> bool {
        match node {
            Node::Typed { field, cond } => {
                cond.is_empty_range()
                    || self.unit.has_scalar_index(*field, cond.needs_sorted())
                    || (!cond.needs_sorted() && self.unit.has_scalar_index(*field, true))
            }
            Node::And(children) | Node::Or(children) => {
                children.iter().all(|child| self.index_served(child))
            }
            Node::Not(child) => self.index_served(child),
            Node::Json { .. } | Node::Key(_) => false,
        }
    }

    fn typed(
        &self,
        field: FieldId,
        cond: &Cond,
        domain: &RoaringBitmap,
    ) -> logpose_types::Result<RoaringBitmap> {
        if cond.is_empty_range() {
            return Ok(RoaringBitmap::new());
        }
        let sorted = cond.needs_sorted();
        let index = match self.unit.scalar_index(field, sorted, self.pins)? {
            Some(index) => Some(index),
            None if !sorted => self.unit.scalar_index(field, true, self.pins)?,
            None => None,
        };
        if let Some(index) = index {
            let any_of = |keys: &[ScalarKey]| {
                let mut rows = RoaringBitmap::new();
                for key in keys {
                    rows |= index.equals(key);
                }
                rows
            };
            let mut rows = match cond {
                Cond::IsNull => index.nulls(),
                Cond::Exists => index.exists(),
                Cond::AnyOf(keys) => any_of(keys),
                Cond::NoneOf(keys) => {
                    let mut rows = index.exists();
                    rows -= any_of(keys);
                    rows
                }
                Cond::Range(low, high) => {
                    index.range(low.as_ref(), high.as_ref()).unwrap_or_default()
                }
            };
            rows &= domain;
            return Ok(rows);
        }
        let column = self.unit.column(field, self.pins)?;
        let mut rows = RoaringBitmap::new();
        for row in domain {
            let keys = column
                .value(row)?
                .map(|value| value_index_keys(&value))
                .unwrap_or_default();
            if cond.holds(&keys) {
                rows.insert(row);
            }
        }
        Ok(rows)
    }
}

/// The index key of a primary key.
fn pk_key(pk: &PrimaryKey) -> ScalarKey {
    match pk {
        PrimaryKey::Int64(value) => ScalarKey::Int(*value),
        PrimaryKey::String(value) => ScalarKey::string(value.as_str()),
    }
}

/// The semantics of one condition on a declared `json` field's value (`None` when null): a
/// declared field, so `exists` when it has a value and `is_null` when it has none; comparisons
/// as [`json_matches`].
fn json_field_matches(value: Option<&JsonValue>, cond: &JsonCond) -> bool {
    match cond {
        JsonCond::Exists => value.is_some(),
        JsonCond::IsNull => value.is_none(),
        _ => json_matches(value, cond),
    }
}

/// The JSON semantics of one condition on a possibly absent dynamic key.
fn json_matches(value: Option<&JsonValue>, cond: &JsonCond) -> bool {
    let scalar = || {
        value
            .and_then(ScalarMetadataValue::from_json)
            .filter(|scalar| !matches!(scalar, ScalarMetadataValue::Null))
    };
    match cond {
        JsonCond::Exists => value.is_some(),
        JsonCond::IsNull => matches!(value, Some(JsonValue::Null)),
        JsonCond::AnyOf(wanted) => scalar()
            .is_some_and(|actual| wanted.iter().any(|wanted| scalars_equal(&actual, wanted))),
        JsonCond::NoneOf(unwanted) => scalar().is_some_and(|actual| {
            unwanted
                .iter()
                .all(|unwanted| !scalars_equal(&actual, unwanted))
        }),
        JsonCond::Range(bounds) => scalar().is_some_and(|actual| {
            bounds.iter().all(|(operator, bound)| {
                compare_scalars(&actual, bound).is_some_and(|ordering| match *operator {
                    "gt" => ordering == Ordering::Greater,
                    "gte" => ordering != Ordering::Less,
                    "lt" => ordering == Ordering::Less,
                    _ => ordering != Ordering::Greater,
                })
            })
        }),
    }
}

fn scalars_equal(left: &ScalarMetadataValue, right: &ScalarMetadataValue) -> bool {
    match (left, right) {
        (ScalarMetadataValue::Number(left), ScalarMetadataValue::Number(right)) => {
            compare_numbers(left, right) == Ordering::Equal
        }
        _ => left == right,
    }
}

fn compare_scalars(left: &ScalarMetadataValue, right: &ScalarMetadataValue) -> Option<Ordering> {
    match (left, right) {
        (ScalarMetadataValue::String(left), ScalarMetadataValue::String(right)) => {
            Some(left.cmp(right))
        }
        (ScalarMetadataValue::Number(left), ScalarMetadataValue::Number(right)) => {
            Some(compare_numbers(left, right))
        }
        _ => None,
    }
}

fn compare_numbers(left: &serde_json::Number, right: &serde_json::Number) -> Ordering {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return left.cmp(&right);
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return left.cmp(&right);
    }
    left.as_f64()
        .unwrap_or_default()
        .partial_cmp(&right.as_f64().unwrap_or_default())
        .unwrap_or(Ordering::Equal)
}

/// The reference semantics over one row, read with the filter's schema.
fn row_matches(
    node: &Node,
    schema: &CollectionSchema,
    record: &logpose_types::record::Record,
) -> bool {
    match node {
        Node::And(children) => children
            .iter()
            .all(|child| row_matches(child, schema, record)),
        Node::Or(children) => children
            .iter()
            .any(|child| row_matches(child, schema, record)),
        Node::Not(child) => !row_matches(child, schema, record),
        Node::Typed { field, cond } => {
            let keys = schema
                .field_by_id(*field)
                .and_then(|field| record.fields.get(field.name()))
                .map(value_index_keys)
                .unwrap_or_default();
            cond.holds(&keys)
        }
        Node::Json { target, cond } => match target {
            JsonTarget::Dynamic(name) => json_matches(record.extra.get(name), cond),
            JsonTarget::Field(field) => {
                let value = schema
                    .field_by_id(*field)
                    .and_then(|field| record.fields.get(field.name()))
                    .filter(|value| !value.is_null())
                    .map(Value::to_json);
                json_field_matches(value.as_ref(), cond)
            }
        },
        Node::Key(cond) => cond.holds(&[pk_key(&record.pk)]),
    }
}

#[cfg(test)]
mod tests {
    use super::{CompiledFilter, FILTER_PATH};
    use logpose_types::{
        DistanceMetric, LogPoseError,
        filter::{FilterExpr, RangeBounds},
        record::Record,
        schema::{
            CollectionSchema, CreateCollectionSpec, ElementType, FieldType, PrimaryKeySpec,
            PrimaryKeyType, ScalarFieldSpec, VectorFieldSpec,
        },
        value::Value,
    };
    use serde_json::json;
    use std::sync::Arc;

    fn schema() -> Arc<CollectionSchema> {
        Arc::new(
            CreateCollectionSpec {
                name: "products".to_owned(),
                primary_key: PrimaryKeySpec {
                    name: "sku".to_owned(),
                    key_type: PrimaryKeyType::Int64,
                },
                vectors: vec![VectorFieldSpec {
                    name: "embedding".to_owned(),
                    dimensions: 2,
                    metric: DistanceMetric::Dot,
                }],
                fields: vec![
                    ScalarFieldSpec::new("tenant", FieldType::String),
                    ScalarFieldSpec::new("price", FieldType::Float64),
                    ScalarFieldSpec::new("stock", FieldType::Int64),
                    ScalarFieldSpec::new("tags", FieldType::Array(ElementType::String)),
                    ScalarFieldSpec::new("active", FieldType::Bool),
                    ScalarFieldSpec::new("meta", FieldType::Json),
                ],
                dynamic_fields: true,
            }
            .build_schema()
            .expect("schema should build"),
        )
    }

    fn error_path(filter: &FilterExpr) -> String {
        match CompiledFilter::compile(&schema(), filter) {
            Err(crate::QueryError::Storage(LogPoseError::InvalidArgument {
                field: Some(field),
                ..
            })) => field,
            other => unreachable!("expected an invalid argument, got {other:?}"),
        }
    }

    #[test]
    fn invalid_filters_name_the_node_path() {
        let cases = [
            (FilterExpr::and(vec![]), "filter.and"),
            (
                FilterExpr::or(vec![FilterExpr::eq("tenant", 3)]),
                "filter.or[0].eq.tenant",
            ),
            (FilterExpr::eq("embedding", 1), "filter.eq.embedding"),
            (
                FilterExpr::eq("$extra.tenant", "a"),
                "filter.eq.$extra.tenant",
            ),
            (
                FilterExpr::contains("tenant", "a"),
                "filter.contains.tenant",
            ),
            (FilterExpr::gt("active", true), "filter.range.active"),
            (
                FilterExpr::range("price", RangeBounds::default()),
                "filter.range.price",
            ),
            (
                FilterExpr::range(
                    "price",
                    RangeBounds {
                        gt: Some(Value::Float64(1.0)),
                        gte: Some(Value::Float64(1.0)),
                        ..RangeBounds::default()
                    },
                ),
                "filter.range.price",
            ),
            (FilterExpr::lt("price", "cheap"), "filter.range.price.lt"),
            (
                FilterExpr::in_values("tags", vec![Value::from("a"), Value::Int64(3)]),
                "filter.in.tags[1]",
            ),
            (FilterExpr::in_values("tags", vec![]), "filter.in.tags"),
            (
                FilterExpr::eq("stock", Value::Float64(2.5)),
                "filter.eq.stock",
            ),
            (FilterExpr::eq("sku", "7"), "filter.eq.sku"),
            (FilterExpr::eq("tenant", Value::Null), "filter.eq.tenant"),
            (
                FilterExpr::eq("color", Value::Json(json!([1]))),
                "filter.eq.color",
            ),
            (
                FilterExpr::negate(FilterExpr::exists("embedding")),
                "filter.not.exists",
            ),
        ];
        for (filter, path) in cases {
            assert_eq!(error_path(&filter), path, "{filter:?}");
        }
    }

    #[test]
    fn filters_follow_the_documented_semantics() {
        let schema = schema();
        let record = Record::new(7_i64)
            .with_field("tenant", Value::from("acme"))
            .with_field("price", Value::Float64(12.5))
            .with_field("stock", Value::Int64(3))
            .with_field(
                "tags",
                Value::Array(vec![Value::from("outdoor"), Value::from("sale")]),
            )
            .with_field("meta", Value::Json(json!(4)));
        let mut record = record;
        record.extra.insert("color".to_owned(), json!("red"));
        record.extra.insert("gone".to_owned(), json!(null));
        let matches = |filter: FilterExpr| {
            CompiledFilter::compile(&schema, &filter)
                .expect("filter should compile")
                .matches_record(&record)
        };
        assert!(matches(FilterExpr::eq("tenant", "acme")));
        assert!(!matches(FilterExpr::eq("price", Value::Int64(12))));
        assert!(matches(FilterExpr::gte("price", Value::Int64(12))));
        assert!(matches(FilterExpr::lt("stock", Value::Float64(3.5))));
        assert!(!matches(FilterExpr::lt("stock", Value::Float64(2.5))));
        assert!(matches(FilterExpr::contains("tags", "sale")));
        assert!(matches(FilterExpr::contains_any(
            "tags",
            vec![Value::from("x"), Value::from("outdoor")]
        )));
        assert!(!matches(FilterExpr::not_in(
            "tags",
            vec![Value::from("sale")]
        )));
        assert!(matches(FilterExpr::in_values(
            "sku",
            vec![Value::Int64(1), Value::Int64(7)]
        )));
        assert!(matches(FilterExpr::ne("color", Value::Json(json!("blue")))));
        assert!(matches(FilterExpr::eq("$extra.color", Value::from("red"))));
        assert!(matches(FilterExpr::is_null("gone")));
        assert!(!matches(FilterExpr::ne("gone", Value::from("x"))));
        assert!(!matches(FilterExpr::exists("active")));
        assert!(!matches(FilterExpr::ne("active", true)));
        assert!(matches(FilterExpr::negate(FilterExpr::eq("active", true))));
        assert!(matches(FilterExpr::gt("meta", Value::Int64(3))));
        // A declared json field is null when it has no value, like any declared field.
        assert!(matches(FilterExpr::exists("meta")));
        assert!(!matches(FilterExpr::is_null("meta")));
        let without_meta = Record::new(8_i64);
        let compiled = |filter: FilterExpr| {
            CompiledFilter::compile(&schema, &filter).expect("filter should compile")
        };
        assert!(compiled(FilterExpr::is_null("meta")).matches_record(&without_meta));
        assert!(!compiled(FilterExpr::exists("meta")).matches_record(&without_meta));
        assert!(!matches(FilterExpr::range(
            "stock",
            RangeBounds {
                gt: Some(Value::Int64(3)),
                lt: Some(Value::Int64(3)),
                ..RangeBounds::default()
            }
        )));
        assert_eq!(FILTER_PATH, "filter");
    }

    #[test]
    fn integer_range_bounds_past_the_int64_range_do_not_saturate() {
        let schema = schema();
        let matches = |stock: i64, filter: FilterExpr| {
            CompiledFilter::compile(&schema, &filter)
                .expect("filter should compile")
                .matches_record(&Record::new(1_i64).with_field("stock", Value::Int64(stock)))
        };
        // 2^63 is one past i64::MAX; 1e30 far past it.
        for bound in [9_223_372_036_854_775_808.0, 1e30] {
            assert!(!matches(
                i64::MAX,
                FilterExpr::gt("stock", Value::Float64(bound))
            ));
            assert!(!matches(
                i64::MAX,
                FilterExpr::gte("stock", Value::Float64(bound))
            ));
            assert!(matches(
                i64::MAX,
                FilterExpr::lt("stock", Value::Float64(bound))
            ));
            assert!(matches(
                i64::MAX,
                FilterExpr::lte("stock", Value::Float64(bound))
            ));
        }
        for bound in [-9_223_372_036_854_777_856.0, -1e30] {
            assert!(!matches(
                i64::MIN,
                FilterExpr::lt("stock", Value::Float64(bound))
            ));
            assert!(!matches(
                i64::MIN,
                FilterExpr::lte("stock", Value::Float64(bound))
            ));
            assert!(matches(
                i64::MIN,
                FilterExpr::gt("stock", Value::Float64(bound))
            ));
            assert!(matches(
                i64::MIN,
                FilterExpr::gte("stock", Value::Float64(bound))
            ));
        }
        // i64::MIN itself is exactly -2^63, an integral bound.
        assert!(matches(
            i64::MIN,
            FilterExpr::lte("stock", Value::Float64(-9_223_372_036_854_775_808.0))
        ));
    }
}
