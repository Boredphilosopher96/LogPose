//! Field definitions: types, index kinds, and the three field families.

use super::SchemaError;
use crate::DistanceMetric;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

/// Name of the dynamic field that stores undeclared keys as JSON.
///
/// It can never be used as a declared field name. The leading `$` already
/// fails the field-name pattern; the name is reported as reserved instead.
pub const DYNAMIC_FIELD_NAME: &str = "$extra";
/// Longest allowed field name, in bytes.
pub const MAX_FIELD_NAME_LEN: usize = 64;
/// Smallest allowed vector dimension count.
pub const MIN_VECTOR_DIMENSIONS: u32 = 1;
/// Largest allowed vector dimension count.
pub const MAX_VECTOR_DIMENSIONS: u32 = 65_536;

/// Stable identifier of a field inside one collection schema.
///
/// Ids are assigned in declaration order when a schema is created and by
/// [`add_field`](super::CollectionSchema::add_field) afterwards. They are
/// never reused, so storage can key columns by id and survive renames and
/// drop-then-re-add of the same name with a different type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldId(pub u32);

impl fmt::Display for FieldId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Type of the primary key field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimaryKeyType {
    /// UTF-8 string keys.
    String,
    /// Signed 64-bit integer keys.
    Int64,
}

impl fmt::Display for PrimaryKeyType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::String => "string",
            Self::Int64 => "int64",
        })
    }
}

/// Element type of an array field. Arrays cannot nest and cannot hold JSON.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ElementType {
    /// Boolean elements.
    Bool,
    /// Signed 64-bit integer elements.
    Int64,
    /// Finite 64-bit float elements.
    Float64,
    /// UTF-8 string elements.
    String,
    /// Timestamp elements, in microseconds since the Unix epoch.
    Timestamp,
}

impl ElementType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int64 => "int64",
            Self::Float64 => "float64",
            Self::String => "string",
            Self::Timestamp => "timestamp",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "bool" => Some(Self::Bool),
            "int64" => Some(Self::Int64),
            "float64" => Some(Self::Float64),
            "string" => Some(Self::String),
            "timestamp" => Some(Self::Timestamp),
            _ => None,
        }
    }
}

impl fmt::Display for ElementType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<ElementType> for FieldType {
    fn from(element: ElementType) -> Self {
        match element {
            ElementType::Bool => Self::Bool,
            ElementType::Int64 => Self::Int64,
            ElementType::Float64 => Self::Float64,
            ElementType::String => Self::String,
            ElementType::Timestamp => Self::Timestamp,
        }
    }
}

/// Type of a scalar (non-vector, non-key) field.
///
/// On the wire a type is a string: `bool`, `int64`, `float64`, `string`,
/// `timestamp`, `json`, or `array<element>` such as `array<string>`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum FieldType {
    /// A boolean.
    Bool,
    /// A signed 64-bit integer.
    Int64,
    /// A finite 64-bit float. NaN and infinities are rejected.
    Float64,
    /// A UTF-8 string.
    String,
    /// A point in time stored as `i64` microseconds since the Unix epoch
    /// (1970-01-01T00:00:00Z), limited to years 0000 through 9999 so every
    /// value has an RFC 3339 form.
    Timestamp,
    /// A flat array of one element type. Arrays do not nest.
    Array(ElementType),
    /// An arbitrary JSON document. JSON fields cannot be indexed.
    Json,
}

impl FieldType {
    /// Index chosen by [`FieldIndex::Auto`] for this type.
    #[must_use]
    pub fn default_index(self) -> FieldIndex {
        match self {
            Self::Bool | Self::String | Self::Array(_) => FieldIndex::Inverted,
            Self::Int64 | Self::Float64 | Self::Timestamp => FieldIndex::InvertedAndSorted,
            Self::Json => FieldIndex::None,
        }
    }

    /// Whether `index` is valid for this type. [`FieldIndex::Auto`] is
    /// always supported.
    #[must_use]
    pub fn supports_index(self, index: FieldIndex) -> bool {
        match (self, index) {
            (_, FieldIndex::Auto | FieldIndex::None) => true,
            (Self::Json, _) => false,
            (Self::Bool | Self::Array(_), FieldIndex::Inverted) => true,
            (Self::Bool | Self::Array(_), _) => false,
            (Self::Int64 | Self::Float64 | Self::String | Self::Timestamp, _) => true,
        }
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool => formatter.write_str("bool"),
            Self::Int64 => formatter.write_str("int64"),
            Self::Float64 => formatter.write_str("float64"),
            Self::String => formatter.write_str("string"),
            Self::Timestamp => formatter.write_str("timestamp"),
            Self::Json => formatter.write_str("json"),
            Self::Array(element) => write!(formatter, "array<{element}>"),
        }
    }
}

impl FromStr for FieldType {
    type Err = SchemaError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "json" {
            return Ok(Self::Json);
        }
        if let Some(element) = ElementType::parse(value) {
            return Ok(element.into());
        }
        value
            .strip_prefix("array<")
            .and_then(|rest| rest.strip_suffix('>'))
            .and_then(ElementType::parse)
            .map(Self::Array)
            .ok_or_else(|| SchemaError::InvalidFieldType {
                value: value.to_owned(),
            })
    }
}

impl TryFrom<String> for FieldType {
    type Error = SchemaError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<FieldType> for String {
    fn from(value: FieldType) -> Self {
        value.to_string()
    }
}

/// Scalar index requested for a field.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldIndex {
    /// Let the engine choose from the field type; see [`FieldIndex::resolve`].
    #[default]
    Auto,
    /// No index; filters on the field scan the column.
    None,
    /// Term-to-bitmap index for equality, `IN`, and array membership.
    Inverted,
    /// Sorted `(value, row)` index for ranges and `order_by`.
    Sorted,
    /// Both an inverted and a sorted index.
    InvertedAndSorted,
}

impl FieldIndex {
    /// Map this request to a concrete index for `field_type`.
    ///
    /// `Auto` becomes `Inverted` for `bool`, `string`, and arrays,
    /// `InvertedAndSorted` for `int64`, `float64`, and `timestamp`, and
    /// `None` for `json`. An explicit choice is returned unchanged when the
    /// type supports it.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnsupportedIndex`] for invalid combinations:
    /// a sorted index on `bool` or an array, or any index on `json`.
    pub fn resolve(self, field: &str, field_type: FieldType) -> Result<Self, SchemaError> {
        if self == Self::Auto {
            return Ok(field_type.default_index());
        }
        if field_type.supports_index(self) {
            Ok(self)
        } else {
            Err(SchemaError::UnsupportedIndex {
                field: field.to_owned(),
                index: self,
                field_type,
            })
        }
    }

    /// Whether this index includes an inverted index.
    #[must_use]
    pub fn has_inverted(self) -> bool {
        matches!(self, Self::Inverted | Self::InvertedAndSorted)
    }

    /// Whether this index includes a sorted index.
    #[must_use]
    pub fn has_sorted(self) -> bool {
        matches!(self, Self::Sorted | Self::InvertedAndSorted)
    }
}

impl fmt::Display for FieldIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Auto => "auto",
            Self::None => "none",
            Self::Inverted => "inverted",
            Self::Sorted => "sorted",
            Self::InvertedAndSorted => "inverted_and_sorted",
        })
    }
}

/// Check a declared field name against `[A-Za-z_][A-Za-z0-9_]{0,63}`.
///
/// # Errors
///
/// Returns [`SchemaError::EmptyFieldName`], [`SchemaError::ReservedFieldName`]
/// for `$extra`, or [`SchemaError::InvalidFieldName`].
pub fn validate_field_name(name: &str) -> Result<(), SchemaError> {
    if name.is_empty() {
        return Err(SchemaError::EmptyFieldName);
    }
    if name == DYNAMIC_FIELD_NAME {
        return Err(SchemaError::ReservedFieldName {
            name: name.to_owned(),
        });
    }
    let mut bytes = name.bytes();
    let first_ok = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    let rest_ok = bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if first_ok && rest_ok && name.len() <= MAX_FIELD_NAME_LEN {
        Ok(())
    } else {
        Err(SchemaError::InvalidFieldName {
            name: name.to_owned(),
        })
    }
}

fn default_metric() -> DistanceMetric {
    DistanceMetric::Cosine
}

fn default_nullable() -> bool {
    true
}

/// Primary key declaration in a create request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryKeySpec {
    /// Field name.
    pub name: String,
    /// Key type.
    #[serde(rename = "type")]
    pub key_type: PrimaryKeyType,
}

/// Vector field declaration in a create request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorFieldSpec {
    /// Field name.
    pub name: String,
    /// Number of `f32` components, from 1 to 65,536.
    pub dimensions: u32,
    /// Distance metric; defaults to `cosine`.
    #[serde(default = "default_metric")]
    pub metric: DistanceMetric,
}

/// Scalar field declaration in a create or alter request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarFieldSpec {
    /// Field name.
    pub name: String,
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Requested index; defaults to `auto`.
    #[serde(default)]
    pub index: FieldIndex,
    /// Whether records may omit the field or set it to null; defaults to
    /// `true`.
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

impl ScalarFieldSpec {
    /// A nullable field with an automatically chosen index.
    #[must_use]
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            index: FieldIndex::Auto,
            nullable: true,
        }
    }
}

/// The primary key field of a stored schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrimaryKeyField {
    /// Stable field id.
    pub id: FieldId,
    /// Field name.
    pub name: String,
    /// Key type.
    #[serde(rename = "type")]
    pub key_type: PrimaryKeyType,
}

/// A vector field of a stored schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorField {
    /// Stable field id.
    pub id: FieldId,
    /// Field name.
    pub name: String,
    /// Number of `f32` components.
    pub dimensions: u32,
    /// Distance metric.
    pub metric: DistanceMetric,
}

/// A scalar field of a stored schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalarField {
    /// Stable field id.
    pub id: FieldId,
    /// Field name.
    pub name: String,
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Concrete index. A validated schema never stores `auto`.
    pub index: FieldIndex,
    /// Whether records may omit the field or set it to null.
    pub nullable: bool,
}
