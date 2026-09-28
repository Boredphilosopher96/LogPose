//! Typed errors for record and partial-update validation.

use super::PrimaryKey;
use crate::{LogPoseError, schema::PrimaryKeyType, value::ValueError};
use thiserror::Error;

/// Reasons a record or partial update does not fit a collection schema.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum RecordError {
    /// The document is not a JSON object.
    #[error("a record must be a JSON object, found {found}")]
    NotAnObject {
        /// The kind of JSON that was supplied.
        found: &'static str,
    },
    /// The document has no primary key.
    #[error("primary key field '{field}' is missing")]
    MissingPrimaryKey {
        /// The primary key field name.
        field: String,
    },
    /// The primary key has the wrong type.
    #[error("primary key field '{field}' expects {expected}, found {found}")]
    PrimaryKeyType {
        /// The primary key field name.
        field: String,
        /// The declared key type.
        expected: PrimaryKeyType,
        /// The kind of value that was supplied.
        found: &'static str,
    },
    /// An integer primary key is out of range or not integral.
    #[error("primary key field '{field}': {source}")]
    InvalidPrimaryKey {
        /// The primary key field name.
        field: String,
        /// Why the number was rejected.
        source: ValueError,
    },
    /// A string primary key is empty.
    #[error("primary key field '{field}' must not be an empty string")]
    EmptyPrimaryKey {
        /// The primary key field name.
        field: String,
    },
    /// A string primary key is longer than the limit.
    #[error("primary key field '{field}' is {len} bytes; the limit is {max}")]
    PrimaryKeyTooLong {
        /// The primary key field name.
        field: String,
        /// Length of the supplied key in bytes.
        len: usize,
        /// Largest allowed length in bytes.
        max: usize,
    },
    /// Two primary keys that must match do not.
    #[error("primary key mismatch: update targets {update}, record has {record}")]
    PrimaryKeyMismatch {
        /// The update's key, as JSON.
        update: String,
        /// The record's key, as JSON.
        record: String,
    },
    /// A declared vector field is missing from a record.
    #[error("vector field '{field}' is missing")]
    MissingVector {
        /// The vector field name.
        field: String,
    },
    /// A vector was supplied under a name that is not a vector field.
    #[error("'{field}' is not a vector field")]
    UnknownVectorField {
        /// The supplied name.
        field: String,
    },
    /// A vector value is not a JSON array.
    #[error("vector field '{field}' expects an array of numbers, found {found}")]
    VectorNotArray {
        /// The vector field name.
        field: String,
        /// The kind of JSON that was supplied.
        found: &'static str,
    },
    /// A vector component is not a number.
    #[error("vector field '{field}' component {index} is {found}, not a number")]
    VectorElementNotNumber {
        /// The vector field name.
        field: String,
        /// Position of the bad component.
        index: usize,
        /// The kind of JSON that was supplied.
        found: &'static str,
    },
    /// A vector has the wrong number of components.
    #[error("vector field '{field}' expects {expected} dimensions, found {actual}")]
    VectorDimensionMismatch {
        /// The vector field name.
        field: String,
        /// Declared dimension count.
        expected: u32,
        /// Supplied component count.
        actual: usize,
    },
    /// A vector component is NaN or infinite (including an `f64` too large
    /// for `f32`).
    #[error("vector field '{field}' component {index} is not a finite f32")]
    NonFiniteVectorElement {
        /// The vector field name.
        field: String,
        /// Position of the bad component.
        index: usize,
    },
    /// A scalar field value does not conform to its type.
    #[error("field '{field}': {source}")]
    InvalidField {
        /// The scalar field name.
        field: String,
        /// Why the value was rejected.
        source: ValueError,
    },
    /// A non-nullable field is missing or null.
    #[error("field '{field}' is required and cannot be null")]
    RequiredField {
        /// The scalar field name.
        field: String,
    },
    /// A scalar value was supplied under the primary key or a vector name.
    #[error("'{field}' is not a scalar field")]
    NotAScalarField {
        /// The supplied name.
        field: String,
    },
    /// A key is not declared and the collection has no dynamic field.
    #[error("field '{field}' is not declared and dynamic fields are disabled")]
    UnknownField {
        /// The undeclared key.
        field: String,
    },
    /// A document used the reserved `$extra` key.
    #[error("'{key}' is reserved and cannot be used as a record key")]
    ReservedKey {
        /// The reserved key.
        key: String,
    },
    /// A dynamic key collides with a declared field or another dynamic key.
    #[error("dynamic key '{key}' collides with a declared field or another dynamic key")]
    ExtraKeyConflict {
        /// The colliding key.
        key: String,
    },
    /// A dynamic key uses a retired name: one that a field declared once and
    /// that was dropped or renamed away. Readers shadow `$extra` keys with
    /// retired names, so storing one would silently hide the value. Add a
    /// field with that name to use it again.
    #[error("dynamic key '{key}' is a retired field name; add a field with that name to use it")]
    RetiredKey {
        /// The retired key.
        key: String,
    },
    /// A partial update changes nothing.
    #[error("a partial update must set at least one field or vector")]
    EmptyUpdate,
}

impl RecordError {
    /// This error as the wire error of the record at request path `path`, such as
    /// `records[2]`, whose key is `pk` when known.
    ///
    /// A vector of the wrong length is [`LogPoseError::DimensionMismatch`]; everything else is
    /// [`LogPoseError::InvalidArgument`]. Either names the offending field below `path`, such
    /// as `records[2].price`, or `records[2].tags[3]` for a bad array element.
    #[must_use]
    pub fn to_error(&self, path: &str, pk: Option<&PrimaryKey>) -> LogPoseError {
        let field = match self.field_name() {
            Some(name) => {
                let element = match self {
                    Self::InvalidField {
                        source:
                            ValueError::ArrayElement { index, .. }
                            | ValueError::NullArrayElement { index },
                        ..
                    } => format!("[{index}]"),
                    _ => String::new(),
                };
                format!("{path}.{name}{element}")
            }
            None => path.to_owned(),
        };
        if let Self::VectorDimensionMismatch {
            expected, actual, ..
        } = self
        {
            return LogPoseError::DimensionMismatch {
                field,
                record_id: pk.map(PrimaryKey::label),
                expected: *expected as usize,
                actual: *actual,
            };
        }
        let message = match pk {
            Some(pk) => format!("record {pk} is invalid: {self}"),
            None => self.to_string(),
        };
        LogPoseError::invalid_field(field, message)
    }

    /// The record field or key the error is about, when it names one.
    #[must_use]
    pub fn field_name(&self) -> Option<&str> {
        match self {
            Self::MissingPrimaryKey { field }
            | Self::PrimaryKeyType { field, .. }
            | Self::InvalidPrimaryKey { field, .. }
            | Self::EmptyPrimaryKey { field }
            | Self::PrimaryKeyTooLong { field, .. }
            | Self::MissingVector { field }
            | Self::UnknownVectorField { field }
            | Self::VectorNotArray { field, .. }
            | Self::VectorElementNotNumber { field, .. }
            | Self::VectorDimensionMismatch { field, .. }
            | Self::NonFiniteVectorElement { field, .. }
            | Self::InvalidField { field, .. }
            | Self::RequiredField { field }
            | Self::NotAScalarField { field }
            | Self::UnknownField { field } => Some(field),
            Self::ReservedKey { key }
            | Self::ExtraKeyConflict { key }
            | Self::RetiredKey { key } => Some(key),
            Self::NotAnObject { .. } | Self::PrimaryKeyMismatch { .. } | Self::EmptyUpdate => None,
        }
    }
}
