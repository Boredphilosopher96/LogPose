use super::*;
use crate::DistanceMetric;
use serde_json::json;

fn plan_example() -> serde_json::Value {
    json!({
        "name": "products",
        "primary_key": { "name": "sku", "type": "string" },
        "vectors": [
            { "name": "embedding", "dimensions": 768, "metric": "cosine" }
        ],
        "fields": [
            { "name": "tenant", "type": "string" },
            { "name": "price", "type": "float64" },
            { "name": "tags", "type": "array<string>" },
            { "name": "updated_at", "type": "timestamp" }
        ],
        "dynamic_fields": true
    })
}

fn schema_with(fields: Vec<ScalarFieldSpec>) -> Result<CollectionSchema, SchemaError> {
    CollectionSchema::new(
        PrimaryKeySpec {
            name: "id".to_owned(),
            key_type: PrimaryKeyType::Int64,
        },
        vec![vector("embedding", 4)],
        fields,
        false,
    )
}

fn vector(name: &str, dimensions: u32) -> VectorFieldSpec {
    VectorFieldSpec {
        name: name.to_owned(),
        dimensions,
        metric: DistanceMetric::L2,
    }
}

fn base_schema() -> CollectionSchema {
    schema_with(vec![ScalarFieldSpec::new("price", FieldType::Float64)])
        .expect("base schema should be valid")
}

#[test]
fn parses_plan_example_create_request() {
    let spec: CreateCollectionSpec =
        serde_json::from_value(plan_example()).expect("plan example should parse");
    assert_eq!(spec.name, "products");

    let schema = spec.to_schema().expect("plan example should validate");
    assert_eq!(schema.schema_version(), 1);
    assert!(schema.dynamic_fields());
    assert_eq!(schema.primary_key().name, "sku");
    assert_eq!(schema.primary_key_type(), PrimaryKeyType::String);
    assert_eq!(schema.vectors().len(), 1);
    assert_eq!(schema.vectors()[0].dimensions, 768);
    assert_eq!(schema.vectors()[0].metric, DistanceMetric::Cosine);

    let resolved: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name.as_str(),
                field.field_type,
                field.index,
                field.nullable,
            )
        })
        .collect();
    assert_eq!(
        resolved,
        vec![
            ("tenant", FieldType::String, FieldIndex::Inverted, true),
            (
                "price",
                FieldType::Float64,
                FieldIndex::InvertedAndSorted,
                true
            ),
            (
                "tags",
                FieldType::Array(ElementType::String),
                FieldIndex::Inverted,
                true
            ),
            (
                "updated_at",
                FieldType::Timestamp,
                FieldIndex::InvertedAndSorted,
                true
            ),
        ]
    );
    let ids: Vec<_> = schema.all_fields().map(|field| field.id().0).collect();
    assert_eq!(ids, vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(schema.next_field_id(), FieldId(6));
}

#[test]
fn create_request_applies_defaults() {
    let spec: CreateCollectionSpec = serde_json::from_value(json!({
        "name": "docs",
        "primary_key": { "name": "id", "type": "int64" },
        "vectors": [{ "name": "v", "dimensions": 3 }]
    }))
    .expect("minimal request should parse");
    assert!(spec.fields.is_empty());
    assert!(spec.dynamic_fields);
    assert_eq!(spec.vectors[0].metric, DistanceMetric::Cosine);
    spec.to_schema().expect("minimal request should validate");
}

#[test]
fn create_request_rejects_unknown_keys() {
    let mut request = plan_example();
    request["dynamic_field"] = json!(true);
    assert!(serde_json::from_value::<CreateCollectionSpec>(request).is_err());

    let mut request = plan_example();
    request["fields"][0]["indexed"] = json!(true);
    assert!(serde_json::from_value::<CreateCollectionSpec>(request).is_err());
}

#[test]
fn create_request_without_vectors_is_rejected() {
    let spec: CreateCollectionSpec = serde_json::from_value(json!({
        "name": "docs",
        "primary_key": { "name": "id", "type": "int64" }
    }))
    .expect("request should parse");
    assert_eq!(spec.to_schema(), Err(SchemaError::NoVectorField));
}

#[test]
fn stored_schema_round_trips_with_stable_shape() {
    let schema = base_schema();
    let stored = serde_json::to_value(&schema).expect("schema should serialize");
    assert_eq!(
        stored,
        json!({
            "schema_version": 1,
            "next_field_id": 3,
            "primary_key": { "id": 0, "name": "id", "type": "int64" },
            "vectors": [{ "id": 1, "name": "embedding", "dimensions": 4, "metric": "l2" }],
            "fields": [{
                "id": 2,
                "name": "price",
                "type": "float64",
                "index": "inverted_and_sorted",
                "nullable": true
            }],
            "dynamic_fields": false
        })
    );
    let decoded: CollectionSchema =
        serde_json::from_value(stored).expect("stored schema should decode");
    assert_eq!(decoded, schema);
}

#[test]
fn stored_schema_is_validated_on_decode() {
    let mut stored = serde_json::to_value(base_schema()).expect("schema should serialize");
    stored["fields"][0]["id"] = json!(1);
    let error = serde_json::from_value::<CollectionSchema>(stored)
        .expect_err("duplicate field ids should fail");
    assert!(error.to_string().contains("field id 1"));

    let mut stored = serde_json::to_value(base_schema()).expect("schema should serialize");
    stored["next_field_id"] = json!(2);
    assert!(serde_json::from_value::<CollectionSchema>(stored).is_err());

    let mut stored = serde_json::to_value(base_schema()).expect("schema should serialize");
    stored["vectors"] = json!([]);
    assert!(serde_json::from_value::<CollectionSchema>(stored).is_err());

    let mut stored = serde_json::to_value(base_schema()).expect("schema should serialize");
    stored["fields"][0]["index"] = json!("auto");
    let decoded: CollectionSchema =
        serde_json::from_value(stored).expect("auto index should resolve on decode");
    assert_eq!(decoded.fields()[0].index, FieldIndex::InvertedAndSorted);
}

#[test]
fn validates_field_names() {
    assert_eq!(validate_field_name(""), Err(SchemaError::EmptyFieldName));
    assert_eq!(
        validate_field_name("$extra"),
        Err(SchemaError::ReservedFieldName {
            name: "$extra".to_owned()
        })
    );
    for bad in ["1abc", "has-dash", "has space", "émoji", "$other", "a.b"] {
        assert!(
            matches!(
                validate_field_name(bad),
                Err(SchemaError::InvalidFieldName { .. })
            ),
            "{bad} should be invalid"
        );
    }
    for good in ["a", "_", "_private", "Price2", "snake_case_name"] {
        assert_eq!(validate_field_name(good), Ok(()), "{good} should be valid");
    }
    let longest = "a".repeat(MAX_FIELD_NAME_LEN);
    assert_eq!(validate_field_name(&longest), Ok(()));
    let too_long = "a".repeat(MAX_FIELD_NAME_LEN + 1);
    assert!(validate_field_name(&too_long).is_err());
}

#[test]
fn rejects_duplicate_names_across_field_kinds() {
    let error = schema_with(vec![ScalarFieldSpec::new("id", FieldType::String)])
        .expect_err("scalar named like the primary key should fail");
    assert_eq!(
        error,
        SchemaError::DuplicateFieldName {
            name: "id".to_owned()
        }
    );

    let error = schema_with(vec![ScalarFieldSpec::new("embedding", FieldType::Json)])
        .expect_err("scalar named like a vector should fail");
    assert!(matches!(error, SchemaError::DuplicateFieldName { .. }));

    let error = schema_with(vec![
        ScalarFieldSpec::new("a", FieldType::Bool),
        ScalarFieldSpec::new("a", FieldType::Int64),
    ])
    .expect_err("duplicate scalars should fail");
    assert!(matches!(error, SchemaError::DuplicateFieldName { .. }));

    let error = schema_with(vec![ScalarFieldSpec::new("$extra", FieldType::Json)])
        .expect_err("$extra should be reserved");
    assert!(matches!(error, SchemaError::ReservedFieldName { .. }));
}

#[test]
fn enforces_vector_dimension_range() {
    for dimensions in [0, MAX_VECTOR_DIMENSIONS + 1] {
        let error = CollectionSchema::new(
            PrimaryKeySpec {
                name: "id".to_owned(),
                key_type: PrimaryKeyType::String,
            },
            vec![vector("v", dimensions)],
            Vec::new(),
            true,
        )
        .expect_err("out-of-range dimensions should fail");
        assert!(matches!(error, SchemaError::InvalidDimensions { .. }));
    }
    for dimensions in [MIN_VECTOR_DIMENSIONS, MAX_VECTOR_DIMENSIONS] {
        CollectionSchema::new(
            PrimaryKeySpec {
                name: "id".to_owned(),
                key_type: PrimaryKeyType::String,
            },
            vec![vector("v", dimensions)],
            Vec::new(),
            true,
        )
        .expect("boundary dimensions should be valid");
    }
}

#[test]
fn field_types_round_trip_as_strings() {
    let cases = [
        (FieldType::Bool, "bool"),
        (FieldType::Int64, "int64"),
        (FieldType::Float64, "float64"),
        (FieldType::String, "string"),
        (FieldType::Timestamp, "timestamp"),
        (FieldType::Json, "json"),
        (FieldType::Array(ElementType::Bool), "array<bool>"),
        (FieldType::Array(ElementType::Int64), "array<int64>"),
        (FieldType::Array(ElementType::Float64), "array<float64>"),
        (FieldType::Array(ElementType::String), "array<string>"),
        (FieldType::Array(ElementType::Timestamp), "array<timestamp>"),
    ];
    for (field_type, text) in cases {
        assert_eq!(field_type.to_string(), text);
        assert_eq!(text.parse::<FieldType>(), Ok(field_type));
        assert_eq!(
            serde_json::to_value(field_type).expect("type should serialize"),
            json!(text)
        );
    }
    for bad in [
        "array<array<string>>",
        "array<json>",
        "array<>",
        "array",
        "Int64",
        "float",
        "",
    ] {
        assert!(
            matches!(
                bad.parse::<FieldType>(),
                Err(SchemaError::InvalidFieldType { .. })
            ),
            "{bad} should not parse"
        );
    }
}

#[test]
fn auto_index_resolves_by_type() {
    let cases = [
        (FieldType::Bool, FieldIndex::Inverted),
        (FieldType::String, FieldIndex::Inverted),
        (FieldType::Array(ElementType::Int64), FieldIndex::Inverted),
        (FieldType::Int64, FieldIndex::InvertedAndSorted),
        (FieldType::Float64, FieldIndex::InvertedAndSorted),
        (FieldType::Timestamp, FieldIndex::InvertedAndSorted),
        (FieldType::Json, FieldIndex::None),
    ];
    for (field_type, expected) in cases {
        assert_eq!(FieldIndex::Auto.resolve("f", field_type), Ok(expected));
    }
}

#[test]
fn rejects_invalid_index_combinations() {
    let invalid = [
        (FieldType::Bool, FieldIndex::Sorted),
        (FieldType::Bool, FieldIndex::InvertedAndSorted),
        (FieldType::Array(ElementType::String), FieldIndex::Sorted),
        (
            FieldType::Array(ElementType::Int64),
            FieldIndex::InvertedAndSorted,
        ),
        (FieldType::Json, FieldIndex::Inverted),
        (FieldType::Json, FieldIndex::Sorted),
        (FieldType::Json, FieldIndex::InvertedAndSorted),
    ];
    for (field_type, index) in invalid {
        assert!(
            matches!(
                index.resolve("f", field_type),
                Err(SchemaError::UnsupportedIndex { .. })
            ),
            "{index} on {field_type} should fail"
        );
    }
    let valid = [
        (FieldType::String, FieldIndex::Sorted),
        (FieldType::Int64, FieldIndex::Inverted),
        (FieldType::Json, FieldIndex::None),
        (FieldType::Bool, FieldIndex::None),
    ];
    for (field_type, index) in valid {
        assert_eq!(index.resolve("f", field_type), Ok(index));
    }

    let mut spec = ScalarFieldSpec::new("flag", FieldType::Bool);
    spec.index = FieldIndex::Sorted;
    let error = schema_with(vec![spec]).expect_err("sorted bool should fail");
    assert!(error.to_string().contains("flag"));
}

#[test]
fn add_field_requires_nullable_and_bumps_version() {
    let mut schema = base_schema();
    let id = schema
        .add_field(ScalarFieldSpec::new("color", FieldType::String))
        .expect("nullable field should be added");
    assert_eq!(id, FieldId(3));
    assert_eq!(schema.schema_version(), 2);
    assert_eq!(schema.next_field_id(), FieldId(4));
    let color = schema.scalar_field("color").expect("field should exist");
    assert_eq!(color.index, FieldIndex::Inverted);

    let before = schema.clone();
    let mut required = ScalarFieldSpec::new("size", FieldType::Int64);
    required.nullable = false;
    assert_eq!(
        schema.add_field(required),
        Err(SchemaError::AddedFieldNotNullable {
            name: "size".to_owned()
        })
    );
    assert!(matches!(
        schema.add_field(ScalarFieldSpec::new("color", FieldType::Bool)),
        Err(SchemaError::DuplicateFieldName { .. })
    ));
    assert!(matches!(
        schema.add_field(ScalarFieldSpec::new("bad-name", FieldType::Bool)),
        Err(SchemaError::InvalidFieldName { .. })
    ));
    let mut bad_index = ScalarFieldSpec::new("doc", FieldType::Json);
    bad_index.index = FieldIndex::Inverted;
    assert!(matches!(
        schema.add_field(bad_index),
        Err(SchemaError::UnsupportedIndex { .. })
    ));
    assert_eq!(schema, before, "failed changes must not modify the schema");
}

#[test]
fn drop_field_protects_primary_key_and_last_vector() {
    let mut schema = base_schema();
    assert_eq!(
        schema.drop_field("id"),
        Err(SchemaError::CannotDropPrimaryKey {
            name: "id".to_owned()
        })
    );
    assert_eq!(
        schema.drop_field("embedding"),
        Err(SchemaError::CannotDropLastVectorField {
            name: "embedding".to_owned()
        })
    );
    assert_eq!(
        schema.drop_field("missing"),
        Err(SchemaError::UnknownField {
            name: "missing".to_owned()
        })
    );
    assert_eq!(schema.schema_version(), 1);

    assert_eq!(schema.drop_field("price"), Ok(FieldId(2)));
    assert_eq!(schema.schema_version(), 2);
    assert!(schema.scalar_field("price").is_none());

    let readded = schema
        .add_field(ScalarFieldSpec::new("price", FieldType::String))
        .expect("dropped name should be reusable");
    assert_eq!(readded, FieldId(3), "field ids are never reused");
    assert_eq!(schema.schema_version(), 3);
}

#[test]
fn drop_field_allows_dropping_one_of_several_vectors() {
    let mut schema = CollectionSchema::new(
        PrimaryKeySpec {
            name: "id".to_owned(),
            key_type: PrimaryKeyType::String,
        },
        vec![vector("text", 8), vector("image", 16)],
        Vec::new(),
        true,
    )
    .expect("schema should be valid");
    assert_eq!(schema.drop_field("text"), Ok(FieldId(1)));
    assert_eq!(schema.vectors().len(), 1);
    assert!(matches!(
        schema.drop_field("image"),
        Err(SchemaError::CannotDropLastVectorField { .. })
    ));
}

#[test]
fn rename_field_keeps_id_and_bumps_version() {
    let mut schema = base_schema();
    assert_eq!(schema.rename_field("price", "cost"), Ok(FieldId(2)));
    assert_eq!(schema.schema_version(), 2);
    assert!(schema.scalar_field("cost").is_some());
    assert!(schema.field("price").is_none());

    assert_eq!(schema.rename_field("id", "sku"), Ok(FieldId(0)));
    assert_eq!(schema.primary_key().name, "sku");

    assert!(matches!(
        schema.rename_field("cost", "embedding"),
        Err(SchemaError::DuplicateFieldName { .. })
    ));
    assert!(matches!(
        schema.rename_field("missing", "other"),
        Err(SchemaError::UnknownField { .. })
    ));
    assert!(matches!(
        schema.rename_field("cost", "$extra"),
        Err(SchemaError::ReservedFieldName { .. })
    ));
    assert_eq!(schema.schema_version(), 3);
    assert!(matches!(
        schema.field_by_id(FieldId(2)),
        Some(FieldRef::Scalar(field)) if field.name == "cost"
    ));
}
