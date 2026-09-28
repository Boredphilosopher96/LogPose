use super::*;
use logpose_index::scalar::ScalarKey;
use logpose_types::{
    DistanceMetric,
    record::Record,
    schema::{
        ElementType, FieldIndex, FieldType, PrimaryKeySpec, PrimaryKeyType, ScalarFieldSpec,
        VectorFieldSpec,
    },
};
use serde_json::json;

fn schema() -> CollectionSchema {
    let mut note = ScalarFieldSpec::new("note", FieldType::String);
    note.index = FieldIndex::None;
    CollectionSchema::new(
        PrimaryKeySpec {
            name: "id".to_owned(),
            key_type: PrimaryKeyType::String,
        },
        vec![VectorFieldSpec {
            name: "embedding".to_owned(),
            dimensions: 3,
            metric: DistanceMetric::Dot,
        }],
        vec![
            ScalarFieldSpec::new("price", FieldType::Int64),
            ScalarFieldSpec::new("tags", FieldType::Array(ElementType::String)),
            note,
        ],
        true,
    )
    .expect("schema")
}

fn image(schema: &CollectionSchema, id: &str, price: Option<i64>, vector: bool) -> RowImage {
    let mut record = Record::new(id).with_vector("embedding", vec![1.0, 2.0, 3.0]);
    if let Some(price) = price {
        record = record.with_field("price", Value::Int64(price)).with_field(
            "tags",
            Value::Array(vec![
                Value::String(format!("t{}", price % 3)),
                Value::String("all".to_owned()),
            ]),
        );
    }
    record.extra.insert("color".to_owned(), json!(id));
    let mut image = RowImage::from_record(schema, record).expect("image");
    if !vector {
        // Records must carry every vector; a null vector reaches a memtable only through a row
        // image (a batch replayed after its vector field was dropped and re-added cannot, but
        // the arena supports it).
        image.vectors.clear();
    }
    image
}

fn memtable(schema: &CollectionSchema) -> MemtableData {
    MemtableData::new(UnitId(4), Arc::new(schema.clone()), 1, Duration::ZERO)
}

#[test]
fn a_pushed_row_reads_back_as_the_same_image() {
    let schema = schema();
    let mut memtable = memtable(&schema);
    for (index, (price, vector)) in [(Some(5), true), (None, false), (Some(7), true)]
        .into_iter()
        .enumerate()
    {
        let row = image(&schema, &format!("k{index}"), price, vector);
        let slot = memtable.push(index as SeqNo + 1, &row).expect("push");
        assert_eq!(slot, index as RowId);
        assert_eq!(memtable.row_image(slot).expect("image"), row);
        assert_eq!(memtable.seq_no(slot), Some(index as SeqNo + 1));
    }
    assert_eq!(memtable.slot_count(), 3);
    assert_eq!(memtable.op_count(), 3);
    assert_eq!(memtable.find(&PrimaryKey::from("k1")), Some(1));
    assert_eq!(
        memtable.vector(FieldId(1), 1),
        None,
        "a null vector reads null"
    );
    assert!(memtable.bytes().total() > 3 * SLOT_OVERHEAD);
}

#[test]
fn a_clone_never_sees_later_writes() {
    let schema = schema();
    let mut memtable = memtable(&schema);
    let mut snapshots = Vec::new();
    for index in 0..(BLOCK_ROWS * 3 + 5) {
        snapshots.push(memtable.clone());
        let row = image(
            &schema,
            &format!("k{index}"),
            Some(index as i64),
            index % 4 != 0,
        );
        memtable.push(index as SeqNo + 1, &row).expect("push");
    }
    for (count, snapshot) in snapshots.iter().enumerate() {
        assert_eq!(snapshot.slot_count() as usize, count);
        assert_eq!(snapshot.find(&PrimaryKey::from(format!("k{count}"))), None);
        for slot in 0..count as RowId {
            assert_eq!(
                snapshot.row_image(slot).expect("image"),
                memtable.row_image(slot).expect("image"),
                "slot {slot} of the snapshot at {count} slots"
            );
        }
    }
}

#[test]
fn schema_changes_add_columns_from_the_next_slot_and_drop_removed_fields() {
    let mut schema = schema();
    let mut memtable = memtable(&schema);
    memtable
        .push(1, &image(&schema, "a", Some(1), true))
        .expect("push");
    let rating = schema
        .add_field(ScalarFieldSpec::new("rating", FieldType::Float64))
        .expect("add");
    schema.drop_field("price").expect("drop");
    memtable.apply_schema(Arc::new(schema.clone()));
    let mut record = Record::new("b").with_field("rating", Value::Float64(2.5));
    record = record.with_vector("embedding", vec![0.0, 1.0, 0.0]);
    let row = RowImage::from_record(&schema, record).expect("image");
    memtable.push(2, &row).expect("push");

    assert_eq!(
        memtable.value(rating, 0),
        None,
        "slots before the add read null"
    );
    assert_eq!(memtable.value(rating, 1), Some(Value::Float64(2.5)));
    let price = FieldId(2);
    assert_eq!(memtable.value(price, 0), None, "a dropped field is gone");
    assert!(memtable.index(price, IndexFlavor::Inverted).is_none());
    let index = memtable
        .index(rating, IndexFlavor::Sorted)
        .expect("float64 gets a sorted index");
    assert_eq!(index.nulls(), [0].into_iter().collect());
    assert_eq!(
        index.eq(&ScalarKey::float(2.5).expect("key")),
        [1].into_iter().collect()
    );
    assert!(
        memtable
            .row_image(0)
            .expect("image")
            .scalars
            .iter()
            .all(|(id, _)| *id != price),
        "the dropped field's value is no longer read"
    );
}

#[test]
fn postings_agree_with_the_columns() {
    let schema = schema();
    let mut memtable = memtable(&schema);
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for slot in 0..200_u32 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let price = (state >> 33) % 7;
        let row = image(
            &schema,
            &format!("k{slot}"),
            (price != 0).then_some(price as i64),
            true,
        );
        memtable.push(SeqNo::from(slot) + 1, &row).expect("push");
    }
    let price = FieldId(2);
    let tags = FieldId(3);
    for flavor in [IndexFlavor::Inverted, IndexFlavor::Sorted] {
        let index = memtable.index(price, flavor).expect("int64 has both");
        for key in 0..7 {
            let expected = (0..200)
                .filter(|slot| memtable.value(price, *slot) == Some(Value::Int64(key)))
                .collect::<RoaringBitmap>();
            assert_eq!(index.eq(&ScalarKey::Int(key)), expected, "{flavor:?} {key}");
        }
        let nulls = (0..200)
            .filter(|slot| memtable.value(price, *slot).is_none())
            .collect::<RoaringBitmap>();
        assert_eq!(index.nulls(), nulls);
    }
    let sorted = memtable.index(price, IndexFlavor::Sorted).expect("sorted");
    let range = sorted
        .range(
            Bound::Included(&ScalarKey::Int(2)),
            Bound::Excluded(&ScalarKey::Int(5)),
        )
        .expect("sorted serves ranges");
    let expected = (0..200)
        .filter(|slot| {
            matches!(memtable.value(price, *slot), Some(Value::Int64(value)) if (2..5).contains(&value))
        })
        .collect::<RoaringBitmap>();
    assert_eq!(range, expected);
    let tags = memtable.index(tags, IndexFlavor::Inverted).expect("arrays");
    assert!(tags.range(Bound::Unbounded, Bound::Unbounded).is_none());
    let all = tags.eq(&ScalarKey::string("all"));
    assert_eq!(all.len() + tags.nulls().len(), 200);
    assert!(
        memtable.index(FieldId(4), IndexFlavor::Inverted).is_none(),
        "index none has no postings"
    );
}

use roaring::RoaringBitmap;
use std::ops::Bound;
