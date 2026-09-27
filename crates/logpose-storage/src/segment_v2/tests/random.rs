//! Seeded generators of schemas and rows.

use logpose_types::{
    DistanceMetric,
    record::{PrimaryKey, Record},
    schema::{
        CollectionSchema, ElementType, FieldType, PrimaryKeySpec, PrimaryKeyType, ScalarField,
        ScalarFieldSpec, VectorFieldSpec,
    },
    value::{Timestamp, Value},
};
use logpose_wal::codec::RowImage;
use rand::{RngExt, rngs::StdRng};
use serde_json::{Map, Number, Value as JsonValue, json};
use std::collections::BTreeSet;

const SCALAR_TYPES: [FieldType; 11] = [
    FieldType::Bool,
    FieldType::Int64,
    FieldType::Float64,
    FieldType::String,
    FieldType::Timestamp,
    FieldType::Json,
    FieldType::Array(ElementType::Bool),
    FieldType::Array(ElementType::Int64),
    FieldType::Array(ElementType::Float64),
    FieldType::Array(ElementType::String),
    FieldType::Array(ElementType::Timestamp),
];

/// A random valid schema, sometimes evolved (a dropped field and an added
/// one) so field ids have gaps and names are retired.
pub(super) fn schema(rng: &mut StdRng) -> CollectionSchema {
    let key_type = if rng.random_bool(0.5) {
        PrimaryKeyType::Int64
    } else {
        PrimaryKeyType::String
    };
    let vectors = (0..rng.random_range(1..=3))
        .map(|index| VectorFieldSpec {
            name: format!("v{index}"),
            dimensions: rng.random_range(1..=24),
            metric: [
                DistanceMetric::Cosine,
                DistanceMetric::Dot,
                DistanceMetric::L2,
            ][rng.random_range(0..3)],
        })
        .collect();
    let fields = (0..rng.random_range(0..=9))
        .map(|index| {
            ScalarFieldSpec::new(
                format!("f{index}"),
                SCALAR_TYPES[rng.random_range(0..SCALAR_TYPES.len())],
            )
        })
        .collect::<Vec<_>>();
    let field_count = fields.len();
    let mut schema = CollectionSchema::new(
        PrimaryKeySpec {
            name: "pk".to_owned(),
            key_type,
        },
        vectors,
        fields,
        rng.random_bool(0.7),
    )
    .expect("random schema is valid");
    if field_count > 0 && rng.random_bool(0.3) {
        schema.drop_field("f0").expect("drop f0");
        let field_type = SCALAR_TYPES[rng.random_range(0..SCALAR_TYPES.len())];
        schema
            .add_field(ScalarFieldSpec::new("late", field_type))
            .expect("add late field");
    }
    schema
}

/// Unique primary keys of the schema's key type.
pub(super) fn keys(rng: &mut StdRng, key_type: PrimaryKeyType, count: usize) -> Vec<PrimaryKey> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::with_capacity(count);
    while keys.len() < count {
        let key = match key_type {
            PrimaryKeyType::Int64 => match rng.random_range(0..10) {
                0 => PrimaryKey::Int64(i64::MIN + rng.random_range(0..4)),
                1 => PrimaryKey::Int64(i64::MAX - rng.random_range(0..4)),
                _ => PrimaryKey::Int64(rng.random_range(-1_000_000..1_000_000)),
            },
            PrimaryKeyType::String => PrimaryKey::String(nonempty_string(rng)),
        };
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    keys
}

fn nonempty_string(rng: &mut StdRng) -> String {
    let mut value = string(rng, 12);
    if value.is_empty() {
        value.push('k');
    }
    value
}

fn string(rng: &mut StdRng, max: usize) -> String {
    const ALPHABET: [char; 10] = [
        'a',
        'b',
        'c',
        'Z',
        '0',
        '-',
        ' ',
        '\u{e9}',
        '\u{65e5}',
        '\u{1f600}',
    ];
    (0..rng.random_range(0..=max))
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())])
        .collect()
}

/// A random row for `schema` as a WAL row image. Some vectors are null.
pub(super) fn row(rng: &mut StdRng, schema: &CollectionSchema, pk: PrimaryKey) -> RowImage {
    let mut record = Record::new(pk);
    for field in schema.vectors() {
        let mut vector: Vec<f32> = (0..field.dimensions)
            .map(|_| rng.random_range(-1.0_f32..1.0))
            .collect();
        vector[0] += 2.0;
        record.vectors.insert(field.name.clone(), vector);
    }
    for field in schema.fields() {
        if rng.random_bool(0.75) {
            record.fields.insert(field.name.clone(), value(rng, field));
        }
    }
    if schema.dynamic_fields() && rng.random_bool(0.5) {
        for _ in 0..rng.random_range(0..4) {
            let key = format!("dyn_{}", rng.random_range(0..6));
            record.extra.insert(key, json_value(rng, 2));
        }
    }
    let mut image = RowImage::from_record(schema, record).expect("random row is valid");
    image.vectors.retain(|_| rng.random_bool(0.85));
    image
}

fn value(rng: &mut StdRng, field: &ScalarField) -> Value {
    match field.field_type {
        FieldType::Array(element) => Value::Array(
            (0..rng.random_range(0..5))
                .map(|_| scalar(rng, element.into()))
                .collect(),
        ),
        FieldType::Json => Value::Json(json_value(rng, 3)),
        other => scalar(rng, other),
    }
}

fn scalar(rng: &mut StdRng, field_type: FieldType) -> Value {
    match field_type {
        FieldType::Bool => Value::Bool(rng.random_bool(0.5)),
        FieldType::Int64 => Value::Int64(match rng.random_range(0..6) {
            0 => i64::MIN,
            1 => i64::MAX,
            2 => 0,
            _ => rng.random_range(-100..100),
        }),
        FieldType::Float64 => Value::Float64(match rng.random_range(0..6) {
            0 => 0.0,
            1 => f64::MAX,
            2 => -1e-300,
            _ => f64::from(rng.random_range(-100_i32..100)) / 8.0,
        }),
        FieldType::String => Value::String(if rng.random_bool(0.5) {
            ["red", "green", "blue", ""][rng.random_range(0..4)].to_owned()
        } else {
            string(rng, 10)
        }),
        FieldType::Timestamp => Value::Timestamp(
            Timestamp::from_micros(match rng.random_range(0..5) {
                0 => Timestamp::MIN.as_micros(),
                1 => Timestamp::MAX.as_micros(),
                _ => rng.random_range(-1_000_000_000_000..2_000_000_000_000_000),
            })
            .expect("in range"),
        ),
        FieldType::Json => Value::Json(json_value(rng, 2)),
        FieldType::Array(element) => scalar(rng, element.into()),
    }
}

fn json_value(rng: &mut StdRng, depth: u32) -> JsonValue {
    let leaf = depth == 0;
    match rng.random_range(0..if leaf { 7 } else { 9 }) {
        0 => JsonValue::Null,
        1 => JsonValue::Bool(rng.random_bool(0.5)),
        2 => json!(rng.random_range(i64::MIN..i64::MAX)),
        3 => json!(u64::MAX - rng.random_range(0..1000)),
        4 => Number::from_f64(f64::from(rng.random_range(-1000_i32..1000)) / 16.0 + 0.5)
            .map_or(JsonValue::Null, JsonValue::Number),
        5 | 6 => JsonValue::String(string(rng, 8)),
        7 => JsonValue::Array(
            (0..rng.random_range(0..4))
                .map(|_| json_value(rng, depth - 1))
                .collect(),
        ),
        _ => {
            let mut map = Map::new();
            for _ in 0..rng.random_range(0..4) {
                map.insert(string(rng, 5), json_value(rng, depth - 1));
            }
            JsonValue::Object(map)
        }
    }
}
