//! The stored collection schema, its create request, and online evolution.

use super::{
    FieldId, MAX_VECTOR_DIMENSIONS, MIN_VECTOR_DIMENSIONS, PrimaryKeyField, PrimaryKeySpec,
    PrimaryKeyType, ScalarField, ScalarFieldSpec, SchemaError, VectorField, VectorFieldSpec,
    validate_field_name,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

fn default_dynamic_fields() -> bool {
    true
}

/// Wire shape of a create-collection request.
///
/// This is the shape in the engine plan (decision D4):
///
/// ```json
/// {
///   "name": "products",
///   "primary_key": { "name": "sku", "type": "string" },
///   "vectors": [{ "name": "embedding", "dimensions": 768, "metric": "cosine" }],
///   "fields": [{ "name": "tags", "type": "array<string>" }],
///   "dynamic_fields": true
/// }
/// ```
///
/// Omitted values take defaults: `metric` is `cosine`, `index` is `auto`,
/// `nullable` is `true`, `fields` is empty, and `dynamic_fields` is `true`.
/// Unknown keys are rejected so typos do not silently change a schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateCollectionSpec {
    /// Collection name. It is validated by the catalog, not by the schema.
    pub name: String,
    /// The primary key field.
    pub primary_key: PrimaryKeySpec,
    /// Vector fields; at least one is required.
    #[serde(default)]
    pub vectors: Vec<VectorFieldSpec>,
    /// Scalar fields.
    #[serde(default)]
    pub fields: Vec<ScalarFieldSpec>,
    /// Whether undeclared keys are kept in the `$extra` dynamic field.
    #[serde(default = "default_dynamic_fields")]
    pub dynamic_fields: bool,
}

impl CreateCollectionSpec {
    /// Build and validate the stored schema for this request.
    ///
    /// # Errors
    ///
    /// Returns a [`SchemaError`] when the request violates a schema rule.
    pub fn to_schema(&self) -> Result<CollectionSchema, SchemaError> {
        CollectionSchema::new(
            self.primary_key.clone(),
            self.vectors.clone(),
            self.fields.clone(),
            self.dynamic_fields,
        )
    }
}

/// A borrowed reference to any declared field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldRef<'a> {
    /// The primary key field.
    PrimaryKey(&'a PrimaryKeyField),
    /// A vector field.
    Vector(&'a VectorField),
    /// A scalar field.
    Scalar(&'a ScalarField),
}

impl<'a> FieldRef<'a> {
    /// The field's stable id.
    #[must_use]
    pub fn id(&self) -> FieldId {
        match self {
            Self::PrimaryKey(field) => field.id,
            Self::Vector(field) => field.id,
            Self::Scalar(field) => field.id,
        }
    }

    /// The field's name.
    #[must_use]
    pub fn name(&self) -> &'a str {
        match *self {
            Self::PrimaryKey(field) => &field.name,
            Self::Vector(field) => &field.name,
            Self::Scalar(field) => &field.name,
        }
    }
}

/// A validated, versioned collection schema.
///
/// Invariants, checked on construction and on deserialization:
///
/// - exactly one primary key, at least one vector field
/// - every name matches `[A-Za-z_][A-Za-z0-9_]{0,63}` and is unique across
///   the primary key, vector, and scalar fields; `$extra` is reserved
/// - vector dimensions are within 1 to 65,536
/// - scalar indexes are concrete (never `auto`) and valid for their type
/// - field ids are unique and below `next_field_id`
/// - every retired name is a valid field name that is not declared
///
/// Every successful change through [`add_field`](Self::add_field),
/// [`drop_field`](Self::drop_field), or [`rename_field`](Self::rename_field)
/// increments `schema_version` by one. A new schema starts at version 1.
///
/// The schema also remembers *retired names*: names that were declared once
/// and then dropped or renamed away. Keys stored in the dynamic `$extra`
/// field under a declared or retired name are shadowed on read; see
/// [`shadows_dynamic_key`](Self::shadows_dynamic_key).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "StoredSchema")]
pub struct CollectionSchema {
    schema_version: u64,
    next_field_id: u32,
    primary_key: PrimaryKeyField,
    vectors: Vec<VectorField>,
    fields: Vec<ScalarField>,
    dynamic_fields: bool,
    retired_names: BTreeSet<String>,
}

/// Unvalidated stored form, used only to validate on deserialization.
#[derive(Deserialize)]
struct StoredSchema {
    schema_version: u64,
    next_field_id: u32,
    primary_key: PrimaryKeyField,
    vectors: Vec<VectorField>,
    fields: Vec<ScalarField>,
    dynamic_fields: bool,
    retired_names: BTreeSet<String>,
}

impl TryFrom<StoredSchema> for CollectionSchema {
    type Error = SchemaError;

    fn try_from(stored: StoredSchema) -> Result<Self, Self::Error> {
        let mut schema = Self {
            schema_version: stored.schema_version,
            next_field_id: stored.next_field_id,
            primary_key: stored.primary_key,
            vectors: stored.vectors,
            fields: stored.fields,
            dynamic_fields: stored.dynamic_fields,
            retired_names: stored.retired_names,
        };
        schema.normalize_and_validate()?;
        Ok(schema)
    }
}

impl CollectionSchema {
    /// Build a version-1 schema, assigning field ids in declaration order:
    /// the primary key, then vector fields, then scalar fields.
    ///
    /// # Errors
    ///
    /// Returns a [`SchemaError`] when the declaration violates a schema rule.
    pub fn new(
        primary_key: PrimaryKeySpec,
        vectors: Vec<VectorFieldSpec>,
        fields: Vec<ScalarFieldSpec>,
        dynamic_fields: bool,
    ) -> Result<Self, SchemaError> {
        let mut ids = IdAllocator(0);
        let primary_key = PrimaryKeyField {
            id: ids.next()?,
            name: primary_key.name,
            key_type: primary_key.key_type,
        };
        let vectors = vectors
            .into_iter()
            .map(|spec| {
                Ok(VectorField {
                    id: ids.next()?,
                    name: spec.name,
                    dimensions: spec.dimensions,
                    metric: spec.metric,
                })
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        let fields = fields
            .into_iter()
            .map(|spec| {
                Ok(ScalarField {
                    id: ids.next()?,
                    name: spec.name,
                    field_type: spec.field_type,
                    index: spec.index,
                    nullable: spec.nullable,
                })
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        let mut schema = Self {
            schema_version: 1,
            next_field_id: ids.0,
            primary_key,
            vectors,
            fields,
            dynamic_fields,
            retired_names: BTreeSet::new(),
        };
        schema.normalize_and_validate()?;
        Ok(schema)
    }

    /// Monotonically increasing version, bumped by every schema change.
    #[must_use]
    pub fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// The id the next added field will receive.
    #[must_use]
    pub fn next_field_id(&self) -> FieldId {
        FieldId(self.next_field_id)
    }

    /// The primary key field.
    #[must_use]
    pub fn primary_key(&self) -> &PrimaryKeyField {
        &self.primary_key
    }

    /// The primary key type.
    #[must_use]
    pub fn primary_key_type(&self) -> PrimaryKeyType {
        self.primary_key.key_type
    }

    /// Vector fields in declaration order.
    #[must_use]
    pub fn vectors(&self) -> &[VectorField] {
        &self.vectors
    }

    /// Scalar fields in declaration order.
    #[must_use]
    pub fn fields(&self) -> &[ScalarField] {
        &self.fields
    }

    /// Whether undeclared keys are kept in the `$extra` dynamic field.
    #[must_use]
    pub fn dynamic_fields(&self) -> bool {
        self.dynamic_fields
    }

    /// Names that were declared once and then dropped or renamed away, and
    /// have not been declared again.
    #[must_use]
    pub fn retired_names(&self) -> &BTreeSet<String> {
        &self.retired_names
    }

    /// Whether `name` is a retired name.
    #[must_use]
    pub fn is_retired(&self, name: &str) -> bool {
        self.retired_names.contains(name)
    }

    /// Whether a key stored in a row's dynamic `$extra` field is hidden
    /// from readers of this schema.
    ///
    /// This is the read rule of dynamic field shadowing: a `$extra` key is
    /// visible only if this schema neither declares nor retires that name.
    /// It is a pure function of the key and the reading schema, so results
    /// never depend on whether a compaction rewrote the row. An added field
    /// therefore reads null on rows that stored the name dynamically, and a
    /// dropped or renamed field's name does not resurface from `$extra`.
    #[must_use]
    pub fn shadows_dynamic_key(&self, key: &str) -> bool {
        self.is_retired(key) || self.field(key).is_some()
    }

    /// Remove every shadowed key from a dynamic `$extra` object, leaving
    /// what a reader of this schema may see. The writer uses the same rule
    /// when it merges a partial update into an old row, so a shadowed value
    /// is never promoted into a typed field.
    pub fn retain_visible_dynamic(&self, extra: &mut serde_json::Map<String, serde_json::Value>) {
        extra.retain(|key, _| !self.shadows_dynamic_key(key));
    }

    /// Look up a vector field by name.
    #[must_use]
    pub fn vector_field(&self, name: &str) -> Option<&VectorField> {
        self.vectors.iter().find(|field| field.name == name)
    }

    /// Look up a scalar field by name.
    #[must_use]
    pub fn scalar_field(&self, name: &str) -> Option<&ScalarField> {
        self.fields.iter().find(|field| field.name == name)
    }

    /// Look up any declared field by name.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<FieldRef<'_>> {
        self.all_fields().find(|field| field.name() == name)
    }

    /// Look up any declared field by id.
    #[must_use]
    pub fn field_by_id(&self, id: FieldId) -> Option<FieldRef<'_>> {
        self.all_fields().find(|field| field.id() == id)
    }

    /// Every declared field: the primary key, vectors, then scalars.
    pub fn all_fields(&self) -> impl Iterator<Item = FieldRef<'_>> {
        std::iter::once(FieldRef::PrimaryKey(&self.primary_key))
            .chain(self.vectors.iter().map(FieldRef::Vector))
            .chain(self.fields.iter().map(FieldRef::Scalar))
    }

    /// Add a scalar field and return its id.
    ///
    /// Added fields must be nullable. Adding a field is a metadata-only
    /// change: rows written before it simply read as null, so no default has
    /// to be materialized into old segments. Defaults are deliberately not
    /// supported; a writer that wants one can set it on upsert.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::AddedFieldNotNullable`], a name error, or
    /// [`SchemaError::UnsupportedIndex`]. The schema is unchanged on error.
    pub fn add_field(&mut self, spec: ScalarFieldSpec) -> Result<FieldId, SchemaError> {
        validate_field_name(&spec.name)?;
        if self.field(&spec.name).is_some() {
            return Err(SchemaError::DuplicateFieldName { name: spec.name });
        }
        if !spec.nullable {
            return Err(SchemaError::AddedFieldNotNullable { name: spec.name });
        }
        let index = spec.index.resolve(&spec.name, spec.field_type)?;
        let mut ids = IdAllocator(self.next_field_id);
        let id = ids.next()?;
        let version = self.bumped_version()?;
        self.retired_names.remove(&spec.name);
        self.fields.push(ScalarField {
            id,
            name: spec.name,
            field_type: spec.field_type,
            index,
            nullable: true,
        });
        self.next_field_id = ids.0;
        self.schema_version = version;
        Ok(id)
    }

    /// Drop a scalar or vector field and return its id. The id is never
    /// reused, so compaction can reclaim the column later. The name becomes
    /// retired until a field with that name is added again.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnknownField`],
    /// [`SchemaError::CannotDropPrimaryKey`], or
    /// [`SchemaError::CannotDropLastVectorField`]. The schema is unchanged on
    /// error.
    pub fn drop_field(&mut self, name: &str) -> Result<FieldId, SchemaError> {
        let id = match self.field(name) {
            None => {
                return Err(SchemaError::UnknownField {
                    name: name.to_owned(),
                });
            }
            Some(FieldRef::PrimaryKey(_)) => {
                return Err(SchemaError::CannotDropPrimaryKey {
                    name: name.to_owned(),
                });
            }
            Some(FieldRef::Vector(_)) if self.vectors.len() == 1 => {
                return Err(SchemaError::CannotDropLastVectorField {
                    name: name.to_owned(),
                });
            }
            Some(field) => field.id(),
        };
        let version = self.bumped_version()?;
        self.vectors.retain(|field| field.id != id);
        self.fields.retain(|field| field.id != id);
        self.schema_version = version;
        self.retired_names.insert(name.to_owned());
        Ok(id)
    }

    /// Rename any field, including the primary key, and return its id.
    /// Storage keys columns by [`FieldId`], so a rename touches no data.
    /// The old name becomes retired, and the new name stops being retired
    /// because it is declared again.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnknownField`], a name error for `to`, or
    /// [`SchemaError::DuplicateFieldName`]. The schema is unchanged on error.
    pub fn rename_field(&mut self, from: &str, to: &str) -> Result<FieldId, SchemaError> {
        let id =
            self.field(from)
                .map(|field| field.id())
                .ok_or_else(|| SchemaError::UnknownField {
                    name: from.to_owned(),
                })?;
        validate_field_name(to)?;
        if self.field(to).is_some() {
            return Err(SchemaError::DuplicateFieldName {
                name: to.to_owned(),
            });
        }
        let version = self.bumped_version()?;
        if self.primary_key.id == id {
            to.clone_into(&mut self.primary_key.name);
        }
        for field in self.vectors.iter_mut().filter(|field| field.id == id) {
            to.clone_into(&mut field.name);
        }
        for field in self.fields.iter_mut().filter(|field| field.id == id) {
            to.clone_into(&mut field.name);
        }
        self.schema_version = version;
        self.retired_names.remove(to);
        self.retired_names.insert(from.to_owned());
        Ok(id)
    }

    fn bumped_version(&self) -> Result<u64, SchemaError> {
        self.schema_version
            .checked_add(1)
            .ok_or(SchemaError::CounterExhausted {
                counter: "schema_version",
            })
    }

    /// Resolve `auto` indexes and check every invariant.
    fn normalize_and_validate(&mut self) -> Result<(), SchemaError> {
        if self.vectors.is_empty() {
            return Err(SchemaError::NoVectorField);
        }
        for vector in &self.vectors {
            if !(MIN_VECTOR_DIMENSIONS..=MAX_VECTOR_DIMENSIONS).contains(&vector.dimensions) {
                return Err(SchemaError::InvalidDimensions {
                    field: vector.name.clone(),
                    dimensions: vector.dimensions,
                    min: MIN_VECTOR_DIMENSIONS,
                    max: MAX_VECTOR_DIMENSIONS,
                });
            }
        }
        for field in &mut self.fields {
            field.index = field.index.resolve(&field.name, field.field_type)?;
        }
        let mut names = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for field in self.all_fields() {
            validate_field_name(field.name())?;
            if !names.insert(field.name()) {
                return Err(SchemaError::DuplicateFieldName {
                    name: field.name().to_owned(),
                });
            }
            let id = field.id();
            if id.0 >= self.next_field_id || !ids.insert(id) {
                return Err(SchemaError::InvalidFieldId {
                    name: field.name().to_owned(),
                    id: id.0,
                });
            }
        }
        for name in &self.retired_names {
            validate_field_name(name)?;
            if names.contains(name.as_str()) {
                return Err(SchemaError::RetiredNameDeclared { name: name.clone() });
            }
        }
        Ok(())
    }
}

/// Hands out sequential field ids without overflowing.
struct IdAllocator(u32);

impl IdAllocator {
    fn next(&mut self) -> Result<FieldId, SchemaError> {
        let id = FieldId(self.0);
        self.0 = self.0.checked_add(1).ok_or(SchemaError::CounterExhausted {
            counter: "field id",
        })?;
        Ok(id)
    }
}
