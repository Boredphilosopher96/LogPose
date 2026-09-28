//! Projections: which fields of a record a read returns.

use super::Record;
use crate::{
    LogPoseError,
    schema::{CollectionSchema, DYNAMIC_FIELD_NAME, FieldRef},
};
use std::collections::BTreeSet;

/// The fields a read returns, resolved against the schema of the state it reads.
///
/// The primary key is always returned. A projection built from an empty field list (the
/// default) returns every non-null scalar field and every visible `$extra` key, but no vector:
/// a vector is returned only when named, which keeps the default response small.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Projection {
    /// `None` returns every field, vectors included.
    selected: Option<Selected>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Selected {
    /// Whether every scalar field is returned.
    all_scalars: bool,
    /// Declared vector and scalar fields, by current name.
    declared: BTreeSet<String>,
    /// Whether every visible `$extra` key is returned.
    all_extra: bool,
    /// Individual `$extra` keys.
    extra: BTreeSet<String>,
}

impl Projection {
    /// A projection that returns every field, vectors included.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// The default projection: every scalar field and `$extra` key, and no vector.
    #[must_use]
    pub fn scalars() -> Self {
        Self {
            selected: Some(Selected {
                all_scalars: true,
                all_extra: true,
                ..Selected::default()
            }),
        }
    }

    /// Resolve `output_fields` against `schema`.
    ///
    /// Each name is a declared field (the primary key, a vector, or a scalar field), `$extra`
    /// for every visible dynamic key, or, when dynamic fields are enabled, one dynamic key.
    /// An empty list is [`Projection::scalars`]: every scalar field and `$extra` key, no
    /// vector.
    ///
    /// # Errors
    ///
    /// Returns [`LogPoseError::InvalidArgument`] naming `output_fields[i]` for a name that is
    /// neither declared nor a possible dynamic key: an undeclared name when dynamic fields are
    /// off, or a retired name (one that was dropped or renamed away), whose dynamic values are
    /// hidden.
    pub fn resolve(schema: &CollectionSchema, output_fields: &[String]) -> crate::Result<Self> {
        if output_fields.is_empty() {
            return Ok(Self::scalars());
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

    /// Whether this projection returns every field, vectors included.
    #[must_use]
    pub fn is_all(&self) -> bool {
        self.selected.is_none()
    }

    /// Whether this projection returns any vector field of `schema`, so a read must load
    /// vectors.
    #[must_use]
    pub fn selects_vectors(&self, schema: &CollectionSchema) -> bool {
        match &self.selected {
            None => !schema.vectors().is_empty(),
            Some(selected) => selected
                .declared
                .iter()
                .any(|name| matches!(schema.field(name), Some(FieldRef::Vector(_)))),
        }
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
        if !selected.all_scalars {
            record
                .fields
                .retain(|name, _| selected.declared.contains(name));
        }
        if !selected.all_extra {
            record.extra.retain(|key, _| selected.extra.contains(key));
        }
        record
    }
}

#[cfg(test)]
mod tests {
    use super::Projection;
    use crate::{
        DistanceMetric,
        record::Record,
        schema::{
            CollectionSchema, CreateCollectionSpec, FieldType, PrimaryKeySpec, PrimaryKeyType,
            ScalarFieldSpec, VectorFieldSpec,
        },
        value::Value,
    };
    use serde_json::json;

    fn schema() -> CollectionSchema {
        CreateCollectionSpec {
            name: "items".to_owned(),
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
            ],
            dynamic_fields: true,
        }
        .build_schema()
        .expect("schema should build")
    }

    fn record() -> Record {
        let mut record = Record::new(7_i64)
            .with_vector("embedding", vec![1.0, 0.0])
            .with_field("tenant", Value::from("acme"))
            .with_field("price", Value::Float64(2.5));
        record.extra.insert("color".to_owned(), json!("red"));
        record
    }

    #[test]
    fn the_default_projection_returns_scalars_and_extra_but_no_vectors() {
        let schema = schema();
        let projection = Projection::resolve(&schema, &[]).expect("resolve");
        assert!(!projection.selects_vectors(&schema));
        assert!(!projection.is_all());
        let projected = projection.apply(record());
        assert!(projected.vectors.is_empty());
        assert_eq!(projected.fields.len(), 2);
        assert_eq!(projected.extra.get("color"), Some(&json!("red")));
    }

    #[test]
    fn a_named_vector_is_returned() {
        let schema = schema();
        let names = ["embedding".to_owned(), "tenant".to_owned()];
        let projection = Projection::resolve(&schema, &names).expect("resolve");
        assert!(projection.selects_vectors(&schema));
        let projected = projection.apply(record());
        assert_eq!(projected.vectors.len(), 1);
        assert_eq!(projected.fields.len(), 1);
        assert!(projected.extra.is_empty());
        assert_eq!(Projection::all().apply(record()), record());
    }
}
