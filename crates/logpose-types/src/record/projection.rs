//! Projections: which fields of a record a read returns.

use super::Record;
use crate::{
    LogPoseError,
    schema::{CollectionSchema, DYNAMIC_FIELD_NAME, FieldRef},
};
use std::collections::BTreeSet;

/// The fields a read returns, resolved against the schema of the state it reads.
///
/// The primary key is always returned. A projection built from an empty field list returns
/// every field: all vectors, every non-null scalar field, and every visible `$extra` key.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Projection {
    /// `None` returns every field.
    selected: Option<Selected>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Selected {
    /// Declared vector and scalar fields, by current name.
    declared: BTreeSet<String>,
    /// Whether every visible `$extra` key is returned.
    all_extra: bool,
    /// Individual `$extra` keys.
    extra: BTreeSet<String>,
}

impl Projection {
    /// A projection that returns every field.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Resolve `output_fields` against `schema`.
    ///
    /// Each name is a declared field (the primary key, a vector, or a scalar field), `$extra`
    /// for every visible dynamic key, or, when dynamic fields are enabled, one dynamic key.
    /// An empty list returns every field.
    ///
    /// # Errors
    ///
    /// Returns [`LogPoseError::InvalidArgument`] naming `output_fields[i]` for a name that is
    /// neither declared nor a possible dynamic key: an undeclared name when dynamic fields are
    /// off, or a retired name (one that was dropped or renamed away), whose dynamic values are
    /// hidden.
    pub fn resolve(schema: &CollectionSchema, output_fields: &[String]) -> crate::Result<Self> {
        if output_fields.is_empty() {
            return Ok(Self::all());
        }
        let mut selected = Selected::default();
        for (index, name) in output_fields.iter().enumerate() {
            let invalid = |message: String| {
                LogPoseError::invalid_field(format!("output_fields[{index}]"), message)
            };
            match schema.field(name) {
                Some(FieldRef::PrimaryKey(_)) => {}
                Some(FieldRef::Vector(_) | FieldRef::Scalar(_)) => {
                    selected.declared.insert(name.clone());
                }
                None if name == DYNAMIC_FIELD_NAME => {
                    if !schema.dynamic_fields() {
                        return Err(invalid(format!(
                            "'{DYNAMIC_FIELD_NAME}' cannot be projected: the collection has no dynamic fields"
                        )));
                    }
                    selected.all_extra = true;
                }
                None if schema.is_retired(name) => {
                    return Err(invalid(format!(
                        "field '{name}' was dropped or renamed and cannot be projected"
                    )));
                }
                None if schema.dynamic_fields() => {
                    selected.extra.insert(name.clone());
                }
                None => {
                    return Err(invalid(format!(
                        "field '{name}' is not declared and the collection has no dynamic fields"
                    )));
                }
            }
        }
        Ok(Self {
            selected: Some(selected),
        })
    }

    /// Whether this projection returns every field.
    #[must_use]
    pub fn is_all(&self) -> bool {
        self.selected.is_none()
    }

    /// Keep only the projected fields of `record`.
    #[must_use]
    pub fn apply(&self, mut record: Record) -> Record {
        let Some(selected) = &self.selected else {
            return record;
        };
        record
            .vectors
            .retain(|name, _| selected.declared.contains(name));
        record
            .fields
            .retain(|name, _| selected.declared.contains(name));
        if !selected.all_extra {
            record.extra.retain(|key, _| selected.extra.contains(key));
        }
        record
    }
}
