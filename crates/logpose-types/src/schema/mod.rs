//! Typed collection schemas for the v2 engine (engine plan decision D4).
//!
//! A schema has exactly one primary key (`string` or `int64`), one or more
//! named vector fields, typed scalar fields with an index choice, and an
//! optional dynamic field, `$extra`, that keeps undeclared keys as JSON.
//!
//! [`CreateCollectionSpec`] is the wire shape of a create request.
//! [`CollectionSchema`] is the validated, stored shape: it adds stable
//! [`FieldId`]s, resolves `auto` indexes, and carries a `schema_version`
//! that every online change bumps.

mod change;
mod collection;
mod error;
mod field;

pub use change::SchemaChange;
pub use collection::{CollectionSchema, CreateCollectionSpec, FieldRef};
pub use error::SchemaError;
pub use field::{
    DYNAMIC_FIELD_NAME, ElementType, FieldId, FieldIndex, FieldType, MAX_FIELD_NAME_LEN,
    MAX_VECTOR_DIMENSIONS, MIN_VECTOR_DIMENSIONS, PrimaryKeyField, PrimaryKeySpec, PrimaryKeyType,
    ScalarField, ScalarFieldSpec, VectorField, VectorFieldSpec, validate_field_name,
};

#[cfg(test)]
mod tests;
