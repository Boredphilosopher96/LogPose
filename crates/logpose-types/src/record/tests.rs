use super::*;
use crate::{
    schema::{CreateCollectionSpec, ElementType, FieldType},
    value::{Timestamp, ValueError},
};
use serde_json::json;

fn products_schema(dynamic_fields: bool) -> CollectionSchema {
    let spec: CreateCollectionSpec = serde_json::from_value(json!({
        "name": "products",
        "primary_key": { "name": "sku", "type": "string" },
        "vectors": [{ "name": "embedding", "dimensions": 3, "metric": "cosine" }],
        "fields": [
            { "name": "tenant", "type": "string", "nullable": false },
            { "name": "price", "type": "float64" },
            { "name": "tags", "type": "array<string>" },
            { "name": "updated_at", "type": "timestamp" }
        ],
        "dynamic_fields": dynamic_fields
    }))
    .expect("spec should parse");
    spec.to_schema().expect("schema should validate")
}

fn int_key_schema() -> CollectionSchema {
    let spec: CreateCollectionSpec = serde_json::from_value(json!({
        "name": "events",
        "primary_key": { "name": "id", "type": "int64" },
        "vectors": [{ "name": "v", "dimensions": 2 }]
    }))
    .expect("spec should parse");
    spec.to_schema().expect("schema should validate")
}

fn document() -> serde_json::Value {
    json!({
        "sku": "A-1",
        "embedding": [0.1, 0.2, 0.3],
        "tenant": "acme",
        "price": 12.5,
        "tags": ["outdoor", "sale"],
        "updated_at": "2026-09-27T10:30:00Z",
        "color": "red",
        "dims": { "w": 2 }
    })
}

#[test]
fn parses_user_document_and_routes_unknown_keys_to_extra() {
    let schema = products_schema(true);
    let record = Record::from_json(&schema, document()).expect("document should parse");

    assert_eq!(record.pk, PrimaryKey::String("A-1".to_owned()));
    assert_eq!(record.vectors["embedding"], vec![0.1_f32, 0.2, 0.3]);
    assert_eq!(record.fields["tenant"], Value::String("acme".to_owned()));
    assert_eq!(record.fields["price"], Value::Float64(12.5));
    assert_eq!(
        record.fields["tags"],
        Value::Array(vec![
            Value::String("outdoor".to_owned()),
            Value::String("sale".to_owned())
        ])
    );
    assert_eq!(
        record.fields["updated_at"],
        Value::Timestamp(Timestamp::from_micros(1_790_505_000_000_000).expect("in range"))
    );
    assert_eq!(record.extra.len(), 2);
    assert_eq!(record.extra["color"], json!("red"));
    assert_eq!(record.extra["dims"], json!({ "w": 2 }));

    assert_eq!(record.to_json(&schema), document());
    assert_eq!(
        Record::from_json(&schema, record.to_json(&schema)),
        Ok(record)
    );
}

#[test]
fn rejects_unknown_keys_without_dynamic_fields() {
    let schema = products_schema(false);
    assert_eq!(
        Record::from_json(&schema, document()),
        Err(RecordError::UnknownField {
            field: "color".to_owned()
        })
    );

    let mut record = Record::new("A-1")
        .with_vector("embedding", vec![0.0; 3])
        .with_field("tenant", Value::String("acme".to_owned()));
    record.extra.insert("color".to_owned(), json!("red"));
    assert!(matches!(
        schema.validate_record(record),
        Err(RecordError::UnknownField { .. })
    ));
}

#[test]
fn rejects_reserved_dynamic_key() {
    let schema = products_schema(true);
    let mut input = document();
    input["$extra"] = json!({ "color": "red" });
    assert_eq!(
        Record::from_json(&schema, input),
        Err(RecordError::ReservedKey {
            key: "$extra".to_owned()
        })
    );
}

#[test]
fn validates_primary_keys() {
    let schema = products_schema(true);
    let without_key = {
        let mut input = document();
        input.as_object_mut().map(|object| object.remove("sku"));
        input
    };
    assert_eq!(
        Record::from_json(&schema, without_key),
        Err(RecordError::MissingPrimaryKey {
            field: "sku".to_owned()
        })
    );

    let mut numeric = document();
    numeric["sku"] = json!(7);
    assert!(matches!(
        Record::from_json(&schema, numeric),
        Err(RecordError::PrimaryKeyType {
            found: "number",
            ..
        })
    ));

    let mut empty = document();
    empty["sku"] = json!("");
    assert!(matches!(
        Record::from_json(&schema, empty),
        Err(RecordError::EmptyPrimaryKey { .. })
    ));

    let mut long = document();
    long["sku"] = json!("k".repeat(MAX_STRING_PRIMARY_KEY_BYTES + 1));
    assert!(matches!(
        Record::from_json(&schema, long),
        Err(RecordError::PrimaryKeyTooLong { .. })
    ));

    let schema = int_key_schema();
    let record = Record::from_json(&schema, json!({ "id": 5.0, "v": [1, 2] }))
        .expect("integral float key should convert");
    assert_eq!(record.pk, PrimaryKey::Int64(5));
    assert!(matches!(
        Record::from_json(&schema, json!({ "id": 5.5, "v": [1, 2] })),
        Err(RecordError::InvalidPrimaryKey {
            source: ValueError::NotIntegral { .. },
            ..
        })
    ));
    assert!(matches!(
        Record::from_json(&schema, json!({ "id": "5", "v": [1, 2] })),
        Err(RecordError::PrimaryKeyType {
            found: "string",
            ..
        })
    ));
    assert!(matches!(
        schema.validate_record(Record::new("5").with_vector("v", vec![1.0, 2.0])),
        Err(RecordError::PrimaryKeyType {
            found: "string",
            ..
        })
    ));
}

#[test]
fn validates_vectors() {
    let schema = products_schema(true);
    let with_embedding = |embedding: serde_json::Value| {
        let mut input = document();
        input["embedding"] = embedding;
        Record::from_json(&schema, input)
    };

    assert_eq!(
        with_embedding(json!([0.1, 0.2])),
        Err(RecordError::VectorDimensionMismatch {
            field: "embedding".to_owned(),
            expected: 3,
            actual: 2
        })
    );
    assert_eq!(
        with_embedding(json!([0.1, "x", 0.3])),
        Err(RecordError::VectorElementNotNumber {
            field: "embedding".to_owned(),
            index: 1,
            found: "string"
        })
    );
    assert!(matches!(
        with_embedding(json!(null)),
        Err(RecordError::VectorNotArray { found: "null", .. })
    ));
    assert_eq!(
        with_embedding(json!([0.1, 1e39, 0.3])),
        Err(RecordError::NonFiniteVectorElement {
            field: "embedding".to_owned(),
            index: 1
        })
    );

    let mut missing = document();
    missing
        .as_object_mut()
        .map(|object| object.remove("embedding"));
    assert_eq!(
        Record::from_json(&schema, missing),
        Err(RecordError::MissingVector {
            field: "embedding".to_owned()
        })
    );

    let nan = Record::new("A-1")
        .with_vector("embedding", vec![0.0, f32::NAN, 0.0])
        .with_field("tenant", Value::String("acme".to_owned()));
    assert!(matches!(
        schema.validate_record(nan),
        Err(RecordError::NonFiniteVectorElement { index: 1, .. })
    ));

    let unknown = Record::new("A-1")
        .with_vector("embedding", vec![0.0; 3])
        .with_vector("other", vec![0.0; 3])
        .with_field("tenant", Value::String("acme".to_owned()));
    assert!(matches!(
        schema.validate_record(unknown),
        Err(RecordError::UnknownVectorField { .. })
    ));
}

#[test]
fn validates_scalar_fields_and_nullability() {
    let schema = products_schema(true);

    let mut wrong_type = document();
    wrong_type["price"] = json!("12.5");
    assert!(matches!(
        Record::from_json(&schema, wrong_type),
        Err(RecordError::InvalidField { ref field, .. }) if field == "price"
    ));

    let mut missing_required = document();
    missing_required
        .as_object_mut()
        .map(|object| object.remove("tenant"));
    assert_eq!(
        Record::from_json(&schema, missing_required),
        Err(RecordError::RequiredField {
            field: "tenant".to_owned()
        })
    );

    let mut null_required = document();
    null_required["tenant"] = json!(null);
    assert_eq!(
        Record::from_json(&schema, null_required),
        Err(RecordError::RequiredField {
            field: "tenant".to_owned()
        })
    );

    let mut null_optional = document();
    null_optional["price"] = json!(null);
    let record = Record::from_json(&schema, null_optional).expect("nullable null is fine");
    assert!(
        !record.fields.contains_key("price"),
        "nulls are dropped from canonical records"
    );

    let typed = Record::new("A-1")
        .with_vector("embedding", vec![0.0; 3])
        .with_field("tenant", Value::String("acme".to_owned()))
        .with_field("price", Value::Int64(3));
    assert!(matches!(
        schema.validate_record(typed),
        Err(RecordError::InvalidField {
            source: ValueError::TypeMismatch { .. },
            ..
        })
    ));

    let misplaced = Record::new("A-1")
        .with_vector("embedding", vec![0.0; 3])
        .with_field("tenant", Value::String("acme".to_owned()))
        .with_field("embedding", Value::Int64(3));
    assert_eq!(
        schema.validate_record(misplaced),
        Err(RecordError::NotAScalarField {
            field: "embedding".to_owned()
        })
    );
}

#[test]
fn validate_record_routes_undeclared_fields_into_extra() {
    let schema = products_schema(true);
    let record = Record::new("A-1")
        .with_vector("embedding", vec![0.0; 3])
        .with_field("tenant", Value::String("acme".to_owned()))
        .with_field(
            "seen",
            Value::Timestamp(Timestamp::from_micros(0).expect("in range")),
        );
    let record = schema
        .validate_record(record)
        .expect("undeclared field should route to extra");
    assert!(!record.fields.contains_key("seen"));
    assert_eq!(record.extra["seen"], json!("1970-01-01T00:00:00Z"));

    let mut conflict = Record::new("A-1")
        .with_vector("embedding", vec![0.0; 3])
        .with_field("tenant", Value::String("acme".to_owned()))
        .with_field("seen", Value::Bool(true));
    conflict.extra.insert("seen".to_owned(), json!(false));
    assert_eq!(
        schema.validate_record(conflict),
        Err(RecordError::ExtraKeyConflict {
            key: "seen".to_owned()
        })
    );

    let mut shadowing = Record::new("A-1")
        .with_vector("embedding", vec![0.0; 3])
        .with_field("tenant", Value::String("acme".to_owned()));
    shadowing.extra.insert("price".to_owned(), json!(1));
    assert_eq!(
        schema.validate_record(shadowing),
        Err(RecordError::ExtraKeyConflict {
            key: "price".to_owned()
        })
    );
}

#[test]
fn rejects_non_object_documents() {
    let schema = products_schema(true);
    assert_eq!(
        Record::from_json(&schema, json!([1, 2])),
        Err(RecordError::NotAnObject { found: "array" })
    );
}

#[test]
fn record_serde_round_trips() {
    let schema = products_schema(true);
    let record = Record::from_json(&schema, document()).expect("document should parse");
    let stored = serde_json::to_value(&record).expect("record should serialize");
    assert_eq!(stored["pk"], json!("A-1"));
    assert_eq!(stored["fields"]["price"], json!({ "float64": 12.5 }));
    let decoded: Record = serde_json::from_value(stored).expect("record should decode");
    assert_eq!(decoded, record);

    let key: PrimaryKey = serde_json::from_value(json!(42)).expect("int key should decode");
    assert_eq!(key, PrimaryKey::Int64(42));
}

#[test]
fn partial_update_does_not_need_vectors() {
    let schema = products_schema(true);
    let update = PartialUpdate::from_json(
        &schema,
        json!({ "sku": "A-1", "price": 9.99, "tags": null, "color": "blue", "dims": null }),
    )
    .expect("partial update should validate");
    assert!(update.vectors.is_empty());
    assert_eq!(update.fields["price"], Value::Float64(9.99));
    assert_eq!(update.fields["tags"], Value::Null);
    assert_eq!(update.extra["color"], json!("blue"));

    let mut record = Record::from_json(&schema, document()).expect("document should parse");
    update.apply_to(&mut record).expect("keys match");
    assert_eq!(record.fields["price"], Value::Float64(9.99));
    assert!(!record.fields.contains_key("tags"));
    assert_eq!(record.extra["color"], json!("blue"));
    assert!(!record.extra.contains_key("dims"));
    assert_eq!(record.vectors["embedding"], vec![0.1_f32, 0.2, 0.3]);
    assert_eq!(schema.validate_record(record.clone()), Ok(record));
}

#[test]
fn partial_update_rules() {
    let schema = products_schema(true);
    assert_eq!(
        PartialUpdate::from_json(&schema, json!({ "sku": "A-1" })),
        Err(RecordError::EmptyUpdate)
    );
    assert_eq!(
        PartialUpdate::from_json(&schema, json!({ "price": 1.0 })),
        Err(RecordError::MissingPrimaryKey {
            field: "sku".to_owned()
        })
    );
    assert_eq!(
        PartialUpdate::from_json(&schema, json!({ "sku": "A-1", "tenant": null })),
        Err(RecordError::RequiredField {
            field: "tenant".to_owned()
        })
    );
    assert!(matches!(
        PartialUpdate::from_json(&schema, json!({ "sku": "A-1", "embedding": [1.0] })),
        Err(RecordError::VectorDimensionMismatch { .. })
    ));
    let vector_only =
        PartialUpdate::from_json(&schema, json!({ "sku": "A-1", "embedding": [1, 0, 0] }))
            .expect("vector-only update should validate");
    assert_eq!(vector_only.vectors["embedding"], vec![1.0_f32, 0.0, 0.0]);

    let strict = products_schema(false);
    assert_eq!(
        PartialUpdate::from_json(&strict, json!({ "sku": "A-1", "color": "red" })),
        Err(RecordError::UnknownField {
            field: "color".to_owned()
        })
    );

    let mut other = Record::from_json(&schema, document()).expect("document should parse");
    other.pk = PrimaryKey::from("B-2");
    let mut update = PartialUpdate::new("A-1");
    update
        .fields
        .insert("price".to_owned(), Value::Float64(1.0));
    let before = other.clone();
    assert!(matches!(
        update.apply_to(&mut other),
        Err(RecordError::PrimaryKeyMismatch { .. })
    ));
    assert_eq!(other, before);
}

#[test]
fn partial_update_accepts_array_fields() {
    let schema = products_schema(true);
    let update = PartialUpdate::from_json(&schema, json!({ "sku": "A-1", "tags": ["x"] }))
        .expect("array update should validate");
    assert_eq!(
        update.fields["tags"]
            .clone()
            .conform(FieldType::Array(ElementType::String)),
        Ok(Value::Array(vec![Value::String("x".to_owned())]))
    );
}

#[test]
fn typed_json_null_is_treated_as_null() {
    let spec: CreateCollectionSpec = serde_json::from_value(json!({
        "name": "docs",
        "primary_key": { "name": "id", "type": "int64" },
        "vectors": [{ "name": "v", "dimensions": 2 }],
        "fields": [
            { "name": "body", "type": "json", "nullable": false },
            { "name": "meta", "type": "json" }
        ],
        "dynamic_fields": false
    }))
    .expect("spec should parse");
    let schema = spec.to_schema().expect("schema should validate");

    let bypass = Record::new(1)
        .with_vector("v", vec![0.0; 2])
        .with_field("body", Value::Json(serde_json::Value::Null));
    assert_eq!(
        schema.validate_record(bypass),
        Err(RecordError::RequiredField {
            field: "body".to_owned()
        }),
        "a JSON null must not satisfy a non-nullable json field"
    );

    let record = Record::new(1)
        .with_vector("v", vec![0.0; 2])
        .with_field("body", Value::Json(json!({ "a": 1 })))
        .with_field("meta", Value::Json(serde_json::Value::Null));
    let record = schema
        .validate_record(record)
        .expect("record should validate");
    assert!(
        !record.fields.contains_key("meta"),
        "a JSON null is dropped from canonical records like any null"
    );

    let mut clear_required = PartialUpdate::new(1);
    clear_required
        .fields
        .insert("body".to_owned(), Value::Json(serde_json::Value::Null));
    assert_eq!(
        schema.validate_update(clear_required),
        Err(RecordError::RequiredField {
            field: "body".to_owned()
        })
    );
}
