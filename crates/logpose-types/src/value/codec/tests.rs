use super::*;
use serde_json::json;

fn ts(micros: i64) -> Value {
    Value::Timestamp(Timestamp::from_micros(micros).expect("timestamp in range"))
}

fn encoded(value: &Value) -> Vec<u8> {
    encode(value).expect("value should encode")
}

/// Golden bytes pin the format. If one of these fails, the encoding changed:
/// every stored WAL frame, memtable cell, and segment column would be
/// misread. Change the format only on purpose, with a format version bump.
#[test]
fn golden_bytes_for_every_value_variant() {
    let cases: Vec<(Value, FieldType, Vec<u8>)> = vec![
        (Value::Null, FieldType::Int64, vec![0x00]),
        (Value::Bool(false), FieldType::Bool, vec![0x01]),
        (Value::Bool(true), FieldType::Bool, vec![0x02]),
        (Value::Int64(0), FieldType::Int64, vec![0x03, 0x00]),
        (Value::Int64(-1), FieldType::Int64, vec![0x03, 0x01]),
        (Value::Int64(1), FieldType::Int64, vec![0x03, 0x02]),
        (Value::Int64(300), FieldType::Int64, vec![0x03, 0xd8, 0x04]),
        (
            Value::Int64(i64::MIN),
            FieldType::Int64,
            vec![
                0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
            ],
        ),
        (
            Value::Int64(i64::MAX),
            FieldType::Int64,
            vec![
                0x03, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
            ],
        ),
        (
            Value::Float64(1.5),
            FieldType::Float64,
            vec![0x04, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f],
        ),
        (
            Value::String("hé".to_owned()),
            FieldType::String,
            vec![0x05, 0x03, 0x68, 0xc3, 0xa9],
        ),
        (
            ts(1_000_000),
            FieldType::Timestamp,
            vec![0x06, 0x80, 0x89, 0x7a],
        ),
        (ts(-1), FieldType::Timestamp, vec![0x06, 0x01]),
        (
            Value::Array(vec![
                Value::String("a".to_owned()),
                Value::String("b".to_owned()),
            ]),
            FieldType::Array(ElementType::String),
            vec![0x07, 0x02, 0x05, 0x01, 0x61, 0x05, 0x01, 0x62],
        ),
        (
            Value::Array(Vec::new()),
            FieldType::Array(ElementType::Int64),
            vec![0x07, 0x00],
        ),
        (
            Value::Json(json!({ "b": [1, -2.5, null], "a": true, "big": u64::MAX })),
            FieldType::Json,
            vec![
                0x08, 0x18, 0x03, // object, 3 keys
                0x01, 0x61, 0x12, // "a": true
                0x01, 0x62, 0x17, 0x03, // "b": array of 3
                0x13, 0x02, // 1
                0x15, 0, 0, 0, 0, 0, 0, 0x04, 0xc0, // -2.5
                0x10, // null
                0x03, 0x62, 0x69, 0x67, // "big"
                0x14, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
            ],
        ),
        (
            Value::Json(json!("x")),
            FieldType::Json,
            vec![0x08, 0x16, 0x01, 0x78],
        ),
    ];
    for (value, field_type, bytes) in cases {
        assert_eq!(encoded(&value), bytes, "encoding of {value:?}");
        assert_eq!(
            decode(&bytes, field_type),
            Ok(value.clone()),
            "decoding of {value:?}"
        );
    }
}

#[test]
fn golden_bytes_for_bare_json_nodes() {
    let cases = vec![
        (json!(null), vec![0x10]),
        (json!(false), vec![0x11]),
        (json!(true), vec![0x12]),
        (json!(-3), vec![0x13, 0x05]),
        (json!(0.25), vec![0x15, 0, 0, 0, 0, 0, 0, 0xd0, 0x3f]),
        (json!("x"), vec![0x16, 0x01, 0x78]),
        (json!([]), vec![0x17, 0x00]),
        (json!({}), vec![0x18, 0x00]),
        (
            json!({ "k": { "n": 1 } }),
            vec![0x18, 0x01, 0x01, 0x6b, 0x18, 0x01, 0x01, 0x6e, 0x13, 0x02],
        ),
    ];
    for (json, bytes) in cases {
        assert_eq!(encode_json(&json), Ok(bytes.clone()), "encoding of {json}");
        assert_eq!(decode_json(&bytes), Ok(json.clone()), "decoding of {json}");
    }
}

#[test]
fn canonical_forms_fold_equal_values() {
    assert_eq!(
        encoded(&Value::Float64(-0.0)),
        encoded(&Value::Float64(0.0))
    );
    assert_eq!(encoded(&Value::Json(json!(null))), encoded(&Value::Null));
    assert_eq!(encode_json(&json!(-0.0)), encode_json(&json!(0.0)));
    // An integral float stays a float; JSON distinguishes 1 from 1.0.
    assert_ne!(encode_json(&json!(1.0)), encode_json(&json!(1)));

    let mut forward = Map::new();
    forward.insert("a".to_owned(), json!(1));
    forward.insert("b".to_owned(), json!(2));
    let mut backward = Map::new();
    backward.insert("b".to_owned(), json!(2));
    backward.insert("a".to_owned(), json!(1));
    assert_eq!(
        encode_json(&JsonValue::Object(forward)),
        encode_json(&JsonValue::Object(backward))
    );
}

#[test]
fn encode_rejects_non_finite_floats_and_leaves_output_unchanged() {
    let mut out = vec![0xaa];
    assert_eq!(
        encode_into(
            &Value::Array(vec![Value::Float64(1.0), Value::Float64(f64::NAN)]),
            &mut out
        ),
        Err(CodecError::NonFiniteFloat)
    );
    assert_eq!(out, vec![0xaa]);
    assert_eq!(
        encode(&Value::Float64(f64::INFINITY)),
        Err(CodecError::NonFiniteFloat)
    );
}

#[test]
fn decode_checks_the_tag_against_the_field_type() {
    let bytes = encoded(&Value::Int64(5));
    assert_eq!(
        decode(&bytes, FieldType::Float64),
        Err(CodecError::TypeMismatch {
            tag: 0x03,
            offset: 0,
            expected: FieldType::Float64
        })
    );
    let strings = encoded(&Value::Array(vec![Value::String("a".to_owned())]));
    assert!(matches!(
        decode(&strings, FieldType::Array(ElementType::Int64)),
        Err(CodecError::TypeMismatch { tag: 0x05, .. })
    ));
    assert_eq!(
        decode(&strings, FieldType::String),
        Err(CodecError::TypeMismatch {
            tag: 0x07,
            offset: 0,
            expected: FieldType::String
        })
    );
    assert_eq!(
        decode(&[0x09], FieldType::Json),
        Err(CodecError::UnknownTag {
            tag: 0x09,
            offset: 0
        })
    );
    assert_eq!(
        decode(&[0x10], FieldType::Json),
        Err(CodecError::UnknownTag {
            tag: 0x10,
            offset: 0
        })
    );
}

#[test]
fn decode_rejects_malformed_and_non_canonical_bytes() {
    let cases: Vec<(&[u8], FieldType, CodecError)> = vec![
        (
            &[],
            FieldType::Int64,
            CodecError::UnexpectedEof { offset: 0 },
        ),
        (
            &[0x03, 0x80],
            FieldType::Int64,
            CodecError::UnexpectedEof { offset: 2 },
        ),
        (
            &[0x03, 0x80, 0x00],
            FieldType::Int64,
            CodecError::InvalidVarint { offset: 1 },
        ),
        (
            &[
                0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02,
            ],
            FieldType::Int64,
            CodecError::InvalidVarint { offset: 1 },
        ),
        (
            &[0x03, 0x02, 0x00],
            FieldType::Int64,
            CodecError::TrailingBytes { count: 1 },
        ),
        (
            &[0x04, 0, 0, 0, 0, 0, 0, 0, 0x80],
            FieldType::Float64,
            CodecError::NonCanonical { offset: 1 },
        ),
        (
            &[0x04, 0, 0, 0, 0, 0, 0, 0xf8, 0x7f],
            FieldType::Float64,
            CodecError::NonCanonical { offset: 1 },
        ),
        (
            &[0x05, 0x05, 0x61],
            FieldType::String,
            CodecError::LengthOutOfBounds { len: 5, offset: 1 },
        ),
        (
            &[0x05, 0x01, 0xff],
            FieldType::String,
            CodecError::InvalidUtf8 { offset: 2 },
        ),
        (
            &[
                0x06, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
            ],
            FieldType::Timestamp,
            CodecError::TimestampOutOfRange { offset: 0 },
        ),
        (
            &[0x07, 0x01, 0x00],
            FieldType::Array(ElementType::Bool),
            CodecError::NullArrayElement { offset: 2 },
        ),
        (
            &[0x07, 0xff, 0xff, 0xff, 0xff, 0x0f],
            FieldType::Array(ElementType::Bool),
            CodecError::LengthOutOfBounds {
                len: 0xffff_ffff,
                offset: 1,
            },
        ),
        (
            &[0x08, 0x10],
            FieldType::Json,
            CodecError::NonCanonical { offset: 0 },
        ),
        (
            &[0x08, 0x14, 0x05],
            FieldType::Json,
            CodecError::NonCanonical { offset: 1 },
        ),
        (
            &[0x08, 0x18, 0x02, 0x01, 0x62, 0x10, 0x01, 0x61, 0x10],
            FieldType::Json,
            CodecError::UnsortedKeys { offset: 6 },
        ),
        (
            &[0x08, 0x18, 0x02, 0x01, 0x61, 0x10, 0x01, 0x61, 0x10],
            FieldType::Json,
            CodecError::UnsortedKeys { offset: 6 },
        ),
    ];
    for (bytes, field_type, error) in cases {
        assert_eq!(
            decode(bytes, field_type),
            Err(error),
            "decoding {bytes:02x?}"
        );
    }
}

#[test]
fn nesting_is_limited_on_both_sides() {
    let mut deep = json!(1);
    for _ in 0..=MAX_NESTING_DEPTH {
        deep = json!([deep]);
    }
    assert_eq!(
        encode_json(&deep),
        Err(CodecError::TooDeep {
            max: MAX_NESTING_DEPTH
        })
    );

    let mut bytes = vec![0x17, 0x01];
    for _ in 0..MAX_NESTING_DEPTH {
        bytes.extend_from_slice(&[0x17, 0x01]);
    }
    bytes.push(0x10);
    assert_eq!(
        decode_json(&bytes),
        Err(CodecError::TooDeep {
            max: MAX_NESTING_DEPTH
        })
    );

    let mut limit = json!(1);
    for _ in 0..MAX_NESTING_DEPTH {
        limit = json!([limit]);
    }
    let bytes = encode_json(&limit).expect("the limit itself is allowed");
    assert_eq!(decode_json(&bytes), Ok(limit));
}

/// Small deterministic generator so the property tests need no extra
/// dependency and every failure reproduces from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn int(&mut self) -> i64 {
        match self.below(4) {
            0 => self.below(200) as i64 - 100,
            1 => i64::MIN + self.below(3) as i64,
            2 => i64::MAX - self.below(3) as i64,
            _ => self.next() as i64,
        }
    }

    fn float(&mut self) -> f64 {
        match self.below(4) {
            0 => 0.0,
            1 => -0.0,
            2 => (self.below(2000) as f64 - 1000.0) / 8.0,
            _ => {
                let value = f64::from_bits(self.next());
                if value.is_finite() { value } else { 1.0 }
            }
        }
    }

    fn string(&mut self) -> String {
        const ALPHABET: [&str; 6] = ["a", "b", "z", "é", "\u{1f600}", "\0"];
        (0..self.below(6))
            .map(|_| ALPHABET[self.below(ALPHABET.len() as u64) as usize])
            .collect()
    }

    fn timestamp(&mut self) -> Timestamp {
        let span = (Timestamp::MAX.as_micros() - Timestamp::MIN.as_micros()) as u64;
        Timestamp::from_micros(Timestamp::MIN.as_micros() + self.below(span) as i64)
            .expect("generated timestamp in range")
    }

    fn element_type(&mut self) -> ElementType {
        match self.below(5) {
            0 => ElementType::Bool,
            1 => ElementType::Int64,
            2 => ElementType::Float64,
            3 => ElementType::String,
            _ => ElementType::Timestamp,
        }
    }

    fn field_type(&mut self) -> FieldType {
        match self.below(4) {
            0 => FieldType::Array(self.element_type()),
            1 => FieldType::Json,
            _ => self.element_type().into(),
        }
    }

    fn value(&mut self, field_type: FieldType, allow_null: bool) -> Value {
        if allow_null && self.below(8) == 0 {
            return Value::Null;
        }
        match field_type {
            FieldType::Bool => Value::Bool(self.below(2) == 1),
            FieldType::Int64 => Value::Int64(self.int()),
            FieldType::Float64 => Value::Float64(self.float()),
            FieldType::String => Value::String(self.string()),
            FieldType::Timestamp => Value::Timestamp(self.timestamp()),
            FieldType::Array(element) => Value::Array(
                (0..self.below(5))
                    .map(|_| self.value(element.into(), false))
                    .collect(),
            ),
            FieldType::Json => Value::Json(self.json(3)),
        }
    }

    fn json(&mut self, depth: u32) -> JsonValue {
        let leaf = depth == 0 || self.below(3) != 0;
        match self.below(if leaf { 6 } else { 8 }) {
            0 => JsonValue::Null,
            1 => JsonValue::Bool(self.below(2) == 1),
            2 => JsonValue::from(self.int()),
            3 => JsonValue::from(self.next()),
            4 => Number::from_f64(self.float()).map_or(JsonValue::Null, JsonValue::Number),
            5 => JsonValue::String(self.string()),
            6 => JsonValue::Array((0..self.below(4)).map(|_| self.json(depth - 1)).collect()),
            _ => JsonValue::Object(
                (0..self.below(4))
                    .map(|_| (self.string(), self.json(depth - 1)))
                    .collect(),
            ),
        }
    }
}

/// The canonical form of a value: what decoding its encoding yields.
fn canonical(value: Value) -> Value {
    match value {
        // Folds -0.0, which compares equal to 0.0.
        Value::Float64(value) => Value::Float64(if value == 0.0 { 0.0 } else { value }),
        Value::Json(JsonValue::Null) => Value::Null,
        Value::Json(json) => Value::Json(canonical_json(json)),
        Value::Array(items) => Value::Array(items.into_iter().map(canonical).collect()),
        other => other,
    }
}

fn canonical_json(json: JsonValue) -> JsonValue {
    match json {
        JsonValue::Number(number) if number.as_f64() == Some(0.0) && number.is_f64() => {
            json!(0.0)
        }
        JsonValue::Array(items) => {
            JsonValue::Array(items.into_iter().map(canonical_json).collect())
        }
        JsonValue::Object(object) => JsonValue::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect(),
        ),
        other => other,
    }
}

#[test]
fn random_values_round_trip_through_canonical_form() {
    let mut rng = Rng(0x5eed_0001);
    for case in 0..5_000 {
        let field_type = rng.field_type();
        let value = rng.value(field_type, true);
        let bytes = encoded(&value);
        let expected = canonical(value);
        assert_eq!(
            decode(&bytes, field_type),
            Ok(expected.clone()),
            "case {case}"
        );
        assert_eq!(
            encoded(&expected),
            bytes,
            "case {case}: re-encoding differs"
        );
    }
}

#[test]
fn random_json_round_trips_through_canonical_form() {
    let mut rng = Rng(0x5eed_0002);
    for case in 0..5_000 {
        let json = rng.json(4);
        let bytes = encode_json(&json).expect("json should encode");
        let decoded = decode_json(&bytes).expect("json should decode");
        assert_eq!(decoded, canonical_json(json), "case {case}");
        assert_eq!(encode_json(&decoded), Ok(bytes), "case {case}");
    }
}

/// Every byte string that decodes is the canonical encoding of what it
/// decodes to, and no input panics. Mutating valid encodings explores the
/// interesting neighborhood of the format.
#[test]
fn mutated_bytes_never_panic_and_accepted_bytes_are_canonical() {
    let mut rng = Rng(0x5eed_0003);
    for _ in 0..20_000 {
        let field_type = rng.field_type();
        let mut bytes = encoded(&rng.value(field_type, true));
        for _ in 0..=rng.below(3) {
            let index = rng.below(bytes.len() as u64 + 1) as usize;
            match rng.below(3) {
                0 if index < bytes.len() => bytes[index] = rng.next() as u8,
                1 if index < bytes.len() => {
                    bytes.remove(index);
                }
                _ => bytes.insert(index, rng.next() as u8),
            }
        }
        if let Ok(value) = decode(&bytes, field_type) {
            assert_eq!(encoded(&value), bytes, "accepted non-canonical bytes");
        }
        if let Ok(json) = decode_json(&bytes) {
            assert_eq!(encode_json(&json), Ok(bytes), "accepted non-canonical json");
        }
    }
}

/// Looking up one member agrees with decoding the whole object, for present, absent, first,
/// and last keys, with nested values skipped on the way.
#[test]
fn member_lookup_agrees_with_full_decoding() {
    let mut rng = Rng(0x5eed_0004);
    for case in 0..2_000 {
        let JsonValue::Object(object) = canonical_json(json!({
            "a": rng.json(3),
            "m": rng.json(3),
            "nested": { "x": [1, {"y": "z"}], "w": 2.5 },
            "z": rng.json(2),
        })) else {
            unreachable!("an object literal")
        };
        let bytes = encode_json(&JsonValue::Object(object.clone())).expect("json should encode");
        for key in ["a", "m", "nested", "z", "", "b", "zz", "n"] {
            assert_eq!(
                decode_json_member(&bytes, key).expect("member lookup"),
                object.get(key).cloned(),
                "case {case} key {key}"
            );
        }
        assert_eq!(
            json_object_keys(&bytes).expect("keys"),
            ["a", "m", "nested", "z"],
            "case {case}"
        );
    }
    let array = encode_json(&json!([1])).expect("json should encode");
    assert!(decode_json_member(&array, "a").is_err());
}

/// Member lookup agrees with the full decoder on random objects whose keys are random strings
/// (multi-byte, NUL, prefixes of one another, empty) and whose values nest, for every key the
/// object has and for random absent ones; the key list agrees with the decoded object's keys.
#[test]
fn member_lookup_agrees_with_full_decoding_for_random_keys() {
    let mut rng = Rng(0x5eed_0005);
    for case in 0..3_000 {
        let object = (0..rng.below(8))
            .map(|_| (rng.string(), rng.json(3)))
            .collect::<serde_json::Map<_, _>>();
        let bytes = encode_json(&JsonValue::Object(object)).expect("json should encode");
        let JsonValue::Object(decoded) = decode_json(&bytes).expect("json should decode") else {
            unreachable!("an object encodes as an object")
        };
        for key in decoded.keys() {
            assert_eq!(
                decode_json_member(&bytes, key).expect("member lookup"),
                decoded.get(key).cloned(),
                "case {case} key {key:?}"
            );
        }
        for _ in 0..4 {
            let key = rng.string();
            assert_eq!(
                decode_json_member(&bytes, &key).expect("member lookup"),
                decoded.get(&key).cloned(),
                "case {case} key {key:?}"
            );
        }
        let mut keys = decoded.keys().map(String::as_str).collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(json_object_keys(&bytes).expect("keys"), keys, "case {case}");
    }
}
