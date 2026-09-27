//! Typed errors for schema definition and evolution.

use super::{FieldIndex, FieldType};
use thiserror::Error;

/// Reasons a collection schema, or a change to one, is rejected.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum SchemaError {
    /// A field name was empty.
    #[error("field name must not be empty")]
    EmptyFieldName,
    /// A field name does not match `[A-Za-z_][A-Za-z0-9_]{0,63}`.
    #[error("field name '{name}' is invalid: names must match [A-Za-z_][A-Za-z0-9_]{{0,63}}")]
    InvalidFieldName {
        /// The rejected name.
        name: String,
    },
    /// A field name is reserved for internal use, such as `$extra`.
    #[error("field name '{name}' is reserved")]
    ReservedFieldName {
        /// The reserved name.
        name: String,
    },
    /// Two fields share a name. Names are unique across the primary key,
    /// vector fields, and scalar fields.
    #[error("field name '{name}' is declared more than once")]
    DuplicateFieldName {
        /// The duplicated name.
        name: String,
    },
    /// The schema declares no vector field.
    #[error("a collection schema needs at least one vector field")]
    NoVectorField,
    /// A vector field has a dimension outside the supported range.
    #[error("vector field '{field}' has {dimensions} dimensions; expected {min} to {max}")]
    InvalidDimensions {
        /// The vector field name.
        field: String,
        /// The rejected dimension count.
        dimensions: u32,
        /// Smallest supported dimension count.
        min: u32,
        /// Largest supported dimension count.
        max: u32,
    },
    /// A field type string could not be parsed.
    #[error(
        "unknown field type '{value}'; expected bool, int64, float64, string, timestamp, json, or array<element>"
    )]
    InvalidFieldType {
        /// The rejected type string.
        value: String,
    },
    /// An index kind is not valid for a field's type.
    #[error("index '{index}' is not supported on field '{field}' of type {field_type}")]
    UnsupportedIndex {
        /// The field name.
        field: String,
        /// The requested index.
        index: FieldIndex,
        /// The field's declared type.
        field_type: FieldType,
    },
    /// A named field does not exist.
    #[error("field '{name}' does not exist")]
    UnknownField {
        /// The missing field name.
        name: String,
    },
    /// The primary key field cannot be dropped.
    #[error("primary key field '{name}' cannot be dropped")]
    CannotDropPrimaryKey {
        /// The primary key field name.
        name: String,
    },
    /// The last remaining vector field cannot be dropped.
    #[error("vector field '{name}' is the last vector field and cannot be dropped")]
    CannotDropLastVectorField {
        /// The vector field name.
        name: String,
    },
    /// Fields added to an existing schema must be nullable, because existing
    /// rows have no value for them.
    #[error("field '{name}' must be nullable to be added to an existing schema")]
    AddedFieldNotNullable {
        /// The field name.
        name: String,
    },
    /// Two fields in a stored schema share a field id, or an id is not below
    /// the schema's `next_field_id`.
    #[error("field id {id} on field '{name}' is duplicated or not below next_field_id")]
    InvalidFieldId {
        /// The field name carrying the id.
        name: String,
        /// The offending id.
        id: u32,
    },
    /// The schema ran out of field ids or schema versions.
    #[error("schema {counter} counter is exhausted")]
    CounterExhausted {
        /// Which counter overflowed.
        counter: &'static str,
    },
}
