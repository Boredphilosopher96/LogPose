use super::*;
use crate::{recovery::new_state, writer::PkIndex};
use logpose_types::{
    DistanceMetric, UnitId,
    legacy::legacy_schema,
    record::Record,
    schema::{FieldType, ScalarFieldSpec},
    value::Value,
};
use logpose_wal::codec::WirePk;
use std::time::Duration;

fn base() -> CollectionSchema {
    legacy_schema(2, DistanceMetric::Dot).expect("schema")
}

fn state(schema: CollectionSchema) -> LogicalState {
    new_state(
        Arc::new(schema),
        UnitId(0),
        1,
        Duration::ZERO,
        Arc::from(Vec::new()),
        DeletionMap::default(),
        PkIndex::default(),
        true,
    )
}

fn put(schema: &CollectionSchema, id: &str, price: Option<i64>) -> RowOp {
    let mut record = Record::new(id).with_vector("vector", vec![1.0, 0.0]);
    if let Some(price) = price {
        record = record.with_field("price", Value::Int64(price));
    }
    RowOp::Put(RowImage::from_record(schema, record).expect("image"))
}

fn delete(id: &str) -> RowOp {
    RowOp::Delete(WirePk::String(id.to_owned()))
}

fn addr(slot: u32) -> RowAddr {
    RowAddr {
        unit: UnitId(0),
        row: slot,
    }
}

#[test]
fn an_upsert_of_an_existing_key_appends_a_slot_and_deletes_the_old_one() {
    let schema = base();
    let mut state = state(schema.clone());
    apply(
        &mut state,
        1,
        Change::Batch {
            schema_version: 1,
            ops: vec![put(&schema, "a", None), put(&schema, "b", None)],
        },
    )
    .expect("batch");
    apply(
        &mut state,
        3,
        Change::Batch {
            schema_version: 1,
            ops: vec![put(&schema, "a", None)],
        },
    )
    .expect("upsert");
    assert_eq!(state.active.slot_count(), 3, "slots are never reused");
    assert!(state.deletes.is_deleted(addr(0)), "the old row is deleted");
    assert!(!state.deletes.is_deleted(addr(1)));
    assert!(!state.deletes.is_deleted(addr(2)));
    assert_eq!(state.pk.resolve(&PrimaryKey::from("a")), Ok(Some(addr(2))));
    assert_eq!(state.counters.total_rows, 3);
    assert_eq!(state.counters.deleted_rows, 1);
    assert_eq!(state.visible_seq_no(), 3);

    apply(
        &mut state,
        4,
        Change::Batch {
            schema_version: 1,
            ops: vec![delete("b"), delete("missing")],
        },
    )
    .expect("deletes");
    assert!(state.deletes.is_deleted(addr(1)));
    assert_eq!(state.pk.resolve(&PrimaryKey::from("b")), Ok(None));
    assert_eq!(state.counters.deleted_rows, 2);
    assert_eq!(
        state.visible_seq_no(),
        5,
        "a delete of a missing key still consumes its sequence number"
    );
    assert_eq!(state.active.op_count(), 5);
}

#[test]
fn a_restored_savepoint_takes_back_a_whole_group() {
    let schema = base();
    let mut state = state(schema.clone());
    apply(
        &mut state,
        1,
        Change::Batch {
            schema_version: 1,
            ops: vec![put(&schema, "a", None)],
        },
    )
    .expect("batch");
    let savepoint = state.savepoint();
    apply(
        &mut state,
        2,
        Change::Batch {
            schema_version: 1,
            ops: vec![
                put(&schema, "a", None),
                put(&schema, "b", None),
                delete("a"),
            ],
        },
    )
    .expect("group");
    state.restore(savepoint);
    assert_eq!(state.active.slot_count(), 1);
    assert!(!state.deletes.is_deleted(addr(0)));
    assert_eq!(state.pk.resolve(&PrimaryKey::from("a")), Ok(Some(addr(0))));
    assert_eq!(state.pk.resolve(&PrimaryKey::from("b")), Ok(None));
    assert_eq!(state.counters.total_rows, 1);
    assert_eq!(state.counters.deleted_rows, 0);
    assert_eq!(state.visible_seq_no(), 1);
}

#[test]
fn batches_from_an_older_schema_lose_fields_a_later_drop_hid() {
    let mut v2 = base();
    v2.add_field(ScalarFieldSpec::new("price", FieldType::Int64))
        .expect("add");
    let mut v3 = v2.clone();
    v3.drop_field("price").expect("drop");

    // Replay from a manifest whose schema already is v3: the add (seq 1) and the drop (seq 3)
    // are skipped, and the batch written under v2 loses its price.
    let mut state = state(v3.clone());
    apply(&mut state, 1, Change::Schema(v2.clone())).expect("skip v2");
    apply(
        &mut state,
        2,
        Change::Batch {
            schema_version: 2,
            ops: vec![put(&v2, "a", Some(5))],
        },
    )
    .expect("batch");
    apply(&mut state, 3, Change::Schema(v3.clone())).expect("skip v3");
    assert_eq!(state.schema.schema_version(), 3);
    assert_eq!(state.visible_seq_no(), 3);
    let image = state.active.row_image(0).expect("image");
    assert!(
        image.scalars.is_empty(),
        "the dropped field's value is gone"
    );
    assert_eq!(image.pk, WirePk::String("a".to_owned()));
}

#[test]
fn replay_from_an_older_schema_applies_changes_in_order() {
    let mut v2 = base();
    let price = v2
        .add_field(ScalarFieldSpec::new("price", FieldType::Int64))
        .expect("add");
    let mut state = state(base());
    apply(&mut state, 1, Change::Schema(v2.clone())).expect("v2");
    apply(
        &mut state,
        2,
        Change::Batch {
            schema_version: 2,
            ops: vec![put(&v2, "a", Some(5)), delete("b")],
        },
    )
    .expect("batch");
    assert_eq!(state.schema.as_ref(), &v2);
    assert_eq!(state.active.schema.as_ref(), &v2);
    assert_eq!(
        state.active.value(price, 0),
        Some(Value::Int64(5)),
        "the field is declared, so its value stays"
    );
    assert_eq!(state.visible_seq_no(), 3);
}

#[test]
fn frames_that_skip_schema_versions_are_rejected() {
    let mut v2 = base();
    v2.add_field(ScalarFieldSpec::new("price", FieldType::Int64))
        .expect("add");
    let mut v3 = v2.clone();
    v3.drop_field("price").expect("drop");
    let mut state = state(base());
    assert_eq!(
        apply(&mut state, 1, Change::Schema(v3)),
        Err(ApplyError::SchemaGap {
            change: 3,
            current: 1
        })
    );
    assert_eq!(
        apply(
            &mut state,
            1,
            Change::Batch {
                schema_version: 2,
                ops: vec![put(&v2, "a", None)],
            },
        ),
        Err(ApplyError::BatchFromTheFuture {
            batch: 2,
            current: 1
        })
    );
    assert!(!state.active.has_ops(), "a rejected frame changes nothing");
}
