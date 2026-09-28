//! Filter compilation: a [`FilterExpr`] against a view's schema, evaluated per unit to the
//! bitmap of live rows it matches.
//!
//! # Semantics
//!
//! A comparison names a field, resolved with the reading schema:
//!
//! - **Declared scalar field.** A row's *keys* are its value's index keys: one per array
//!   element, one for a scalar, none for null (or an empty array). `exists` holds when a row
//!   has a key and `is_null` when it has none; `eq v` when some key equals `v`; the ordered
//!   operators when some key lies in the range; and `ne v` when the row has a key and none
//!   equals `v` (SQL-like: nulls never match `ne`). The operand is converted to the field's
//!   type: integers and timestamps compare as integers (a timestamp also accepts an RFC 3339
//!   string, and a non-integral bound rounds to the matching integer range), floats as floats,
//!   strings bytewise. An operand of another type never equals a value, so `eq` matches
//!   nothing and `ne` matches every row with a value.
//! - **Undeclared name**, with dynamic fields on: the row's `$extra` key of that name, with the
//!   JSON semantics of the v1 API (`exists` when the key is present, `is_null` when present and
//!   `null`, `eq`/`ne` by JSON scalar equality, the ordered operators between two strings or
//!   two numbers). A key the schema declares or retires is shadowed and reads as absent.
//! - **The primary key**: its one key, never null.
//! - **A JSON field**: its document, with the `$extra` JSON semantics.
//!
//! `not p` is `live AND NOT p`, so it includes rows where `p`'s field is null; `and` and `or`
//! are intersection and union.
//!
//! # Evaluation
//!
//! Per unit, a comparison on a declared field uses the unit's index when one serves the
//! operator (inverted or sorted for `eq`, `ne`, `exists`, `is_null`; sorted for ranges) and
//! otherwise scans the column over the rows still in play; `$extra` and the key scan rows.
//! `and` evaluates its index-served children first and narrows the rows later children scan.

use logpose_storage::{
    SectionNeed, UnitView,
    cache::PinSet,
    read::{ScalarKey, value_index_keys},
};
use logpose_types::{
    ScalarMetadataValue,
    filter::{FilterComparison, FilterExpr, FilterOperator},
    record::PrimaryKey,
    schema::{CollectionSchema, ElementType, FieldId, FieldRef, FieldType},
    value::{Timestamp, Value},
};
use roaring::RoaringBitmap;
use serde_json::Value as JsonValue;
use std::{cmp::Ordering, ops::Bound, sync::Arc};

use crate::{QueryError, Result};

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
    Typed {
        field: FieldId,
        cond: Cond,
    },
    Json {
        target: JsonTarget,
        comparison: FilterComparison,
    },
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
    Nothing,
    IsNull,
    Exists,
    Eq(ScalarKey),
    Ne(Option<ScalarKey>),
    Range(Bound<ScalarKey>, Bound<ScalarKey>),
}

impl Cond {
    /// Whether a row with `keys` satisfies the condition.
    fn holds(&self, keys: &[ScalarKey]) -> bool {
        match self {
            Self::Nothing => false,
            Self::IsNull => keys.is_empty(),
            Self::Exists => !keys.is_empty(),
            Self::Eq(key) => keys.contains(key),
            Self::Ne(key) => !keys.is_empty() && key.as_ref().is_none_or(|key| !keys.contains(key)),
            Self::Range(low, high) => keys
                .iter()
                .any(|key| (low.as_ref(), high.as_ref()).contains_key(key)),
        }
    }

    /// Whether an index can answer the condition: any index for everything but ranges,
    /// which need a sorted one.
    fn needs_sorted(&self) -> bool {
        matches!(self, Self::Range(..))
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
        above && below && low_kind_matches(self.0, key) && low_kind_matches(self.1, key)
    }
}

/// Keys of different kinds never compare: a bound of another kind excludes the key.
fn low_kind_matches(bound: Bound<&ScalarKey>, key: &ScalarKey) -> bool {
    match bound {
        Bound::Included(other) | Bound::Excluded(other) => other.kind() == key.kind(),
        Bound::Unbounded => true,
    }
}

/// Check a filter's structure: non-empty `and`/`or`, value-less `exists` and `is_null`, a
/// value for the other operators, and a string or number for the ordered ones.
///
/// # Errors
///
/// [`QueryError::InvalidPredicate`].
pub fn validate(expr: &FilterExpr) -> Result<()> {
    match expr {
        FilterExpr::And { children } | FilterExpr::Or { children } => {
            if children.is_empty() {
                return Err(QueryError::InvalidPredicate(
                    "logical predicates must include at least one child".to_owned(),
                ));
            }
            children.iter().try_for_each(validate)
        }
        FilterExpr::Not { child } => validate(child),
        FilterExpr::Comparison(comparison) => match comparison.operator {
            FilterOperator::Exists | FilterOperator::IsNull => {
                if comparison.value.is_some() {
                    return Err(QueryError::InvalidPredicate(format!(
                        "predicate operator '{}' does not accept a value",
                        operator_name(comparison.operator)
                    )));
                }
                Ok(())
            }
            operator => {
                let Some(value) = comparison.value.as_ref() else {
                    return Err(QueryError::InvalidPredicate(format!(
                        "predicate operator '{}' requires a value",
                        operator_name(operator)
                    )));
                };
                let ordered = matches!(
                    operator,
                    FilterOperator::Lt
                        | FilterOperator::Lte
                        | FilterOperator::Gt
                        | FilterOperator::Gte
                );
                if ordered
                    && !matches!(
                        value,
                        ScalarMetadataValue::String(_) | ScalarMetadataValue::Number(_)
                    )
                {
                    return Err(QueryError::InvalidPredicate(format!(
                        "predicate operator '{}' requires a string or number value",
                        operator_name(operator)
                    )));
                }
                Ok(())
            }
        },
    }
}

/// The wire name of an operator.
#[must_use]
pub fn operator_name(operator: FilterOperator) -> &'static str {
    match operator {
        FilterOperator::Eq => "eq",
        FilterOperator::Ne => "ne",
        FilterOperator::Lt => "lt",
        FilterOperator::Lte => "lte",
        FilterOperator::Gt => "gt",
        FilterOperator::Gte => "gte",
        FilterOperator::Exists => "exists",
        FilterOperator::IsNull => "is_null",
    }
}

impl CompiledFilter {
    /// Validate `expr` and resolve its field names with `schema`.
    ///
    /// # Errors
    ///
    /// [`QueryError::InvalidPredicate`] for a malformed filter or one that compares a vector
    /// field.
    pub fn compile(schema: &Arc<CollectionSchema>, expr: &FilterExpr) -> Result<Self> {
        validate(expr)?;
        Ok(Self {
            root: compile_node(schema, expr)?,
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
        Evaluator {
            unit,
            pins,
            schema: &self.schema,
        }
        .eval(&self.root, &live)
    }

    /// Whether one row satisfies the filter: the reference semantics the per-unit evaluation
    /// follows (tests compare against it).
    #[must_use]
    pub fn matches_record(&self, record: &logpose_types::record::Record) -> bool {
        row_matches(&self.root, &self.schema, record)
    }
}

fn compile_node(schema: &CollectionSchema, expr: &FilterExpr) -> Result<Node> {
    Ok(match expr {
        FilterExpr::And { children } => Node::And(
            children
                .iter()
                .map(|child| compile_node(schema, child))
                .collect::<Result<_>>()?,
        ),
        FilterExpr::Or { children } => Node::Or(
            children
                .iter()
                .map(|child| compile_node(schema, child))
                .collect::<Result<_>>()?,
        ),
        FilterExpr::Not { child } => Node::Not(Box::new(compile_node(schema, child)?)),
        FilterExpr::Comparison(comparison) => {
            let name = comparison.field.as_str();
            if name == schema.primary_key().name {
                let kind = match schema.primary_key_type() {
                    logpose_types::schema::PrimaryKeyType::Int64 => ElementType::Int64,
                    logpose_types::schema::PrimaryKeyType::String => ElementType::String,
                };
                return Ok(Node::Key(condition(kind, comparison)));
            }
            match schema.field(name) {
                Some(FieldRef::PrimaryKey(_)) => {
                    return Err(QueryError::InvalidPredicate(format!(
                        "field '{name}' cannot be filtered"
                    )));
                }
                Some(FieldRef::Vector(_)) => {
                    return Err(QueryError::InvalidPredicate(format!(
                        "field '{name}' is a vector field and cannot be filtered"
                    )));
                }
                Some(FieldRef::Scalar(field)) => match element_type(field.field_type) {
                    Some(element) => Node::Typed {
                        field: field.id,
                        cond: condition(element, comparison),
                    },
                    None => Node::Json {
                        target: JsonTarget::Field(field.id),
                        comparison: comparison.clone(),
                    },
                },
                None => {
                    if schema.dynamic_fields() && !schema.shadows_dynamic_key(name) {
                        Node::Json {
                            target: JsonTarget::Dynamic(name.to_owned()),
                            comparison: comparison.clone(),
                        }
                    } else {
                        Node::Json {
                            target: JsonTarget::Dynamic(String::new()),
                            comparison: comparison.clone(),
                        }
                    }
                }
            }
        }
    })
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

/// The condition `comparison` puts on keys of `element` type.
fn condition(element: ElementType, comparison: &FilterComparison) -> Cond {
    let operand = comparison.value.as_ref();
    match comparison.operator {
        FilterOperator::Exists => Cond::Exists,
        FilterOperator::IsNull => Cond::IsNull,
        FilterOperator::Eq => match operand {
            Some(ScalarMetadataValue::Null) => Cond::IsNull,
            Some(value) => exact_key(element, value).map_or(Cond::Nothing, Cond::Eq),
            None => Cond::Nothing,
        },
        FilterOperator::Ne => match operand {
            Some(ScalarMetadataValue::Null) => Cond::Exists,
            Some(value) => Cond::Ne(exact_key(element, value)),
            None => Cond::Nothing,
        },
        operator => {
            let Some(value) = operand else {
                return Cond::Nothing;
            };
            range(element, operator, value).unwrap_or(Cond::Nothing)
        }
    }
}

/// The key equal to `value` in a column of `element` type, if one can be.
fn exact_key(element: ElementType, value: &ScalarMetadataValue) -> Option<ScalarKey> {
    match (element, value) {
        (ElementType::Bool, ScalarMetadataValue::Bool(value)) => Some(ScalarKey::Bool(*value)),
        (ElementType::Int64 | ElementType::Timestamp, ScalarMetadataValue::Number(number)) => {
            integral(number).map(ScalarKey::Int)
        }
        (ElementType::Timestamp, ScalarMetadataValue::String(text)) => {
            Timestamp::parse_rfc3339(text)
                .ok()
                .map(|timestamp| ScalarKey::timestamp_micros(timestamp.as_micros()))
        }
        (ElementType::Float64, ScalarMetadataValue::Number(number)) => number
            .as_f64()
            .and_then(|value| ScalarKey::float(value).ok()),
        (ElementType::String, ScalarMetadataValue::String(text)) => {
            Some(ScalarKey::string(text.as_str()))
        }
        _ => None,
    }
}

/// An integer exactly equal to `number`, if there is one.
fn integral(number: &serde_json::Number) -> Option<i64> {
    if let Some(value) = number.as_i64() {
        return Some(value);
    }
    let value = number.as_f64()?;
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    let exact = value.fract() == 0.0 && value >= i64::MIN as f64 && value < i64::MAX as f64;
    #[allow(clippy::cast_possible_truncation)]
    exact.then_some(value as i64)
}

/// The key range of an ordered comparison in a column of `element` type.
fn range(
    element: ElementType,
    operator: FilterOperator,
    value: &ScalarMetadataValue,
) -> Option<Cond> {
    let (low, high) = match element {
        ElementType::Int64 | ElementType::Timestamp => {
            let key = match value {
                ScalarMetadataValue::Number(number) => number.as_f64()?,
                ScalarMetadataValue::String(text) if element == ElementType::Timestamp => {
                    #[allow(clippy::cast_precision_loss)]
                    let micros = Timestamp::parse_rfc3339(text).ok()?.as_micros() as f64;
                    micros
                }
                _ => return None,
            };
            if let Some(exact) = match value {
                ScalarMetadataValue::Number(number) => integral(number),
                ScalarMetadataValue::String(text) => Timestamp::parse_rfc3339(text)
                    .ok()
                    .map(Timestamp::as_micros),
                _ => None,
            } {
                let key = ScalarKey::Int(exact);
                bounds(operator, key)
            } else {
                // A non-integral bound: x < 3.5 is x <= 3, and x > 3.5 is x >= 4.
                #[allow(clippy::cast_possible_truncation)]
                let floor = key.floor() as i64;
                #[allow(clippy::cast_possible_truncation)]
                let ceil = key.ceil() as i64;
                match operator {
                    FilterOperator::Lt | FilterOperator::Lte => {
                        (Bound::Unbounded, Bound::Included(ScalarKey::Int(floor)))
                    }
                    _ => (Bound::Included(ScalarKey::Int(ceil)), Bound::Unbounded),
                }
            }
        }
        ElementType::Float64 => {
            let ScalarMetadataValue::Number(number) = value else {
                return None;
            };
            bounds(operator, ScalarKey::float(number.as_f64()?).ok()?)
        }
        ElementType::String => {
            let ScalarMetadataValue::String(text) = value else {
                return None;
            };
            bounds(operator, ScalarKey::string(text.as_str()))
        }
        ElementType::Bool => return None,
    };
    Some(Cond::Range(low, high))
}

fn bounds(operator: FilterOperator, key: ScalarKey) -> (Bound<ScalarKey>, Bound<ScalarKey>) {
    match operator {
        FilterOperator::Lt => (Bound::Unbounded, Bound::Excluded(key)),
        FilterOperator::Lte => (Bound::Unbounded, Bound::Included(key)),
        FilterOperator::Gt => (Bound::Excluded(key), Bound::Unbounded),
        _ => (Bound::Included(key), Bound::Unbounded),
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
            if matches!(cond, Cond::Nothing) {
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
                JsonTarget::Dynamic(name) if name.is_empty() => return,
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
                let value = logpose_wal_value(bytes?, declared.field_type)?;
                value_index_keys(&value).into_iter().next()
            };
            let (min, max) = (decode(min), decode(max));
            let rows = unit.row_count();
            match cond {
                Cond::Nothing => false,
                Cond::IsNull => null_count > 0,
                Cond::Exists | Cond::Ne(_) => null_count < rows,
                Cond::Eq(key) => match (min, max) {
                    (Some(min), Some(max)) => {
                        min.kind() != key.kind() || (&min <= key && key <= &max)
                    }
                    _ => null_count < rows,
                },
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

/// Decode a zone bound stored in the binary value codec.
fn logpose_wal_value(bytes: &[u8], field_type: FieldType) -> Option<Value> {
    logpose_types::value::codec::decode(bytes, field_type).ok()
}

struct Evaluator<'a, 'v> {
    unit: &'a UnitView<'v>,
    pins: &'a PinSet,
    schema: &'a CollectionSchema,
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
            Node::Json { target, comparison } => {
                let mut rows = RoaringBitmap::new();
                match target {
                    JsonTarget::Dynamic(name) => {
                        if name.is_empty() || !self.unit.has_dynamic() {
                            // No visible key anywhere: every comparison sees it absent.
                            if json_matches(None, comparison) {
                                return Ok(domain.clone());
                            }
                            return Ok(rows);
                        }
                        let dynamic = self.unit.dynamic(self.pins)?;
                        for row in domain {
                            let object = dynamic.object(row)?;
                            let value = object.as_ref().and_then(|object| object.get(name));
                            if json_matches(value, comparison) {
                                rows.insert(row);
                            }
                        }
                    }
                    JsonTarget::Field(field) => {
                        let column = self.unit.column(*field, self.pins)?;
                        for row in domain {
                            let value = column.value(row)?.map(Value::into_json);
                            if json_matches(value.as_ref(), comparison) {
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
                matches!(cond, Cond::Nothing)
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
        if matches!(cond, Cond::Nothing) {
            return Ok(RoaringBitmap::new());
        }
        let sorted = cond.needs_sorted();
        let index = match self.unit.scalar_index(field, sorted, self.pins)? {
            Some(index) => Some(index),
            None if !sorted => self.unit.scalar_index(field, true, self.pins)?,
            None => None,
        };
        if let Some(index) = index {
            let mut rows = match cond {
                Cond::Nothing => RoaringBitmap::new(),
                Cond::IsNull => index.nulls(),
                Cond::Exists => index.exists(),
                Cond::Eq(key) => index.equals(key),
                Cond::Ne(key) => {
                    let mut rows = index.exists();
                    if let Some(key) = key {
                        rows -= index.equals(key);
                    }
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
        let _ = self.schema;
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

/// The v1 JSON semantics of one comparison on a possibly absent JSON value.
fn json_matches(value: Option<&JsonValue>, comparison: &FilterComparison) -> bool {
    let scalar = || value.and_then(ScalarMetadataValue::from_json);
    let ordered = |wanted: Ordering| {
        scalar()
            .zip(comparison.value.clone())
            .and_then(|(actual, expected)| compare_scalars(&actual, &expected))
            .is_some_and(|ordering| ordering == wanted)
    };
    match comparison.operator {
        FilterOperator::Exists => value.is_some(),
        FilterOperator::IsNull => matches!(value, Some(JsonValue::Null)),
        FilterOperator::Eq => scalar()
            .zip(comparison.value.clone())
            .is_some_and(|(actual, expected)| actual == expected),
        FilterOperator::Ne => scalar()
            .zip(comparison.value.clone())
            .is_some_and(|(actual, expected)| actual != expected),
        FilterOperator::Lt => ordered(Ordering::Less),
        FilterOperator::Lte => ordered(Ordering::Less) || ordered(Ordering::Equal),
        FilterOperator::Gt => ordered(Ordering::Greater),
        FilterOperator::Gte => ordered(Ordering::Greater) || ordered(Ordering::Equal),
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
        Node::Json { target, comparison } => match target {
            JsonTarget::Dynamic(name) if name.is_empty() => json_matches(None, comparison),
            JsonTarget::Dynamic(name) => json_matches(record.extra.get(name), comparison),
            JsonTarget::Field(field) => {
                let value = schema
                    .field_by_id(*field)
                    .and_then(|field| record.fields.get(field.name()))
                    .filter(|value| !value.is_null())
                    .map(Value::to_json);
                json_matches(value.as_ref(), comparison)
            }
        },
        Node::Key(cond) => cond.holds(&[pk_key(&record.pk)]),
    }
}
