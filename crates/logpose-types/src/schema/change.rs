//! Online schema changes: the alter request and how it applies to a schema.

use super::{CollectionSchema, ScalarFieldSpec, SchemaError};
use serde::{Deserialize, Serialize};

/// One online schema change, applied in order with the writes around it.
///
/// On the wire it is an object with exactly one key:
///
/// ```json
/// { "add_field": { "name": "color", "type": "string" } }
/// { "drop_field": { "name": "color" } }
/// { "rename_field": { "from": "color", "to": "colour" } }
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaChange {
    /// Add a nullable scalar field. Rows written before it read null.
    AddField(ScalarFieldSpec),
    /// Drop a scalar or vector field. Its name becomes retired.
    DropField {
        /// The field's current name.
        name: String,
    },
    /// Rename any field, including the primary key.
    RenameField {
        /// The current name.
        from: String,
        /// The new name.
        to: String,
    },
}

impl SchemaChange {
    /// Apply the change to `schema`, which is unchanged on error.
    ///
    /// # Errors
    ///
    /// Returns the [`SchemaError`] of the underlying
    /// [`add_field`](CollectionSchema::add_field),
    /// [`drop_field`](CollectionSchema::drop_field), or
    /// [`rename_field`](CollectionSchema::rename_field).
    pub fn apply_to(&self, schema: &mut CollectionSchema) -> Result<(), SchemaError> {
        match self {
            Self::AddField(spec) => schema.add_field(spec.clone()).map(|_| ()),
            Self::DropField { name } => schema.drop_field(name).map(|_| ()),
            Self::RenameField { from, to } => schema.rename_field(from, to).map(|_| ()),
        }
    }

    /// The request field an error from [`apply_to`](Self::apply_to) is
    /// about, such as `add_field.type` or `rename_field.to`.
    #[must_use]
    pub fn error_field(&self, error: &SchemaError) -> String {
        match self {
            Self::AddField(_) => match error {
                SchemaError::UnsupportedIndex { .. } => "add_field.index",
                SchemaError::AddedFieldNotNullable { .. } => "add_field.nullable",
                SchemaError::InvalidFieldType { .. } => "add_field.type",
                _ => "add_field.name",
            },
            Self::DropField { .. } => "drop_field.name",
            Self::RenameField { from, .. } => match error {
                SchemaError::UnknownField { name } if name == from => "rename_field.from",
                _ => "rename_field.to",
            },
        }
        .to_owned()
    }
}
