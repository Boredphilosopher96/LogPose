//! Deletion-vector and primary-key-index tests at the writer: upserts, updates, and deletes of
//! keys wherever their live row is, deletions that land while a flush or a compaction builds,
//! the compaction's DV file, and schema changes that remove the vector field.

use super::*;
use crate::{
    CreateCollectionRequest, Engine, EngineConfig,
    dv::dv_path,
    legacy_view::{legacy_id, legacy_put},
    paths::segment_path,
};
use logpose_types::{
    CollectionRef, DistanceMetric, ResourceKind,
    record::{PartialUpdate, PrimaryKey, Record},
};
use logpose_vfs::{CrashPoint, FaultPlan, FaultVfs};
use logpose_wal::BootId;
use serde_json::json;
use std::collections::BTreeMap;

const ROOT: &str = "/storage";

fn open(fault: &Arc<FaultVfs>) -> Engine {
    Engine::open(
        fault.process(),
        ROOT,
        EngineConfig {
            boot_id: Some(BootId::new("boot")),
            ..EngineConfig::default()
        },
    )
    .expect("engine should open")
}

/// Create `name` with background maintenance off, so every job comes from the test.
fn create(engine: &Engine, name: &str) -> Arc<CollectionHandle> {
    let mut descriptor = engine
        .core()
        .plan_collection_descriptor(&CreateCollectionRequest::new(name, 2, DistanceMetric::Dot))
        .expect("descriptor should plan");
    descriptor.flush_threshold_ops = usize::MAX;
    descriptor.flush_threshold_bytes = usize::MAX;
    descriptor.compaction_threshold_segments = usize::MAX;
    engine
        .create_collection(descriptor, None)
        .expect("collection should be created")
}

fn reopen(engine: &Engine, name: &str) -> Arc<CollectionHandle> {
    engine
        .collection(&CollectionRef::new_default(name))
        .expect("collection should reopen")
}

fn upsert(id: &str, x: f32, extra: serde_json::Value) -> ClientOp {
    let mut record = Record::new(id).with_vector("vector", vec![x, 1.0]);
    if let serde_json::Value::Object(extra) = extra {
        record.extra = extra;
    }
    ClientOp::Upsert(record)
}

fn delete(id: &str) -> ClientOp {
    ClientOp::Delete(PrimaryKey::from(id))
}

fn update(id: &str, change: impl FnOnce(&mut PartialUpdate)) -> ClientOp {
    let mut update = PartialUpdate::new(id);
    change(&mut update);
    ClientOp::Update(update)
}

fn write(handle: &CollectionHandle, ops: Vec<ClientOp>) {
    handle.write_blocking(ops).expect("write should commit");
}

/// Every live row, as `id -> (vector, metadata)`, after checking the version's invariants
/// (I5 among them: one live row per key).
fn live(handle: &CollectionHandle) -> BTreeMap<String, (Vec<f32>, serde_json::Value)> {
    let version = handle.current();
    version.check_invariants().expect("invariants hold");
    version
        .live_images()
        .into_iter()
        .map(|(_, image)| {
            let put = legacy_put(&version.schema, &image).expect("row should read");
            (put.id.as_str().to_owned(), (put.vector, put.metadata))
        })
        .collect()
}

fn row(handle: &CollectionHandle, id: &str) -> Option<(Vec<f32>, serde_json::Value)> {
    live(handle).remove(id)
}

fn segment_units(handle: &CollectionHandle) -> Vec<UnitId> {
    handle.current().manifest.units().collect()
}

fn deleted_rows(handle: &CollectionHandle, unit: UnitId) -> Vec<u32> {
    handle
        .current()
        .deletes
        .get(unit)
        .map(|dv| dv.iter().collect())
        .unwrap_or_default()
}

fn exists(fault: &FaultVfs, path: &std::path::Path) -> bool {
    logpose_vfs::exists(fault.process().as_ref(), path).expect("exists")
}

/// An upsert of a key whose live row is in a segment appends a memtable slot and sets the
/// segment row's bit; the WAL holds the bit until the next flush writes the segment's DV file,
/// which recovery then loads.
#[test]
fn an_upsert_of_a_flushed_key_marks_its_segment_row_until_the_next_flush_persists_it() {
    let fault = FaultVfs::new(60);
    let engine = open(&fault);
    let handle = create(&engine, "upserts");
    let core = engine.core();
    write(
        &handle,
        vec![
            upsert("a", 1.0, json!({"v": 1})),
            upsert("b", 2.0, json!({"v": 1})),
        ],
    );
    core.flush_collection(&handle).expect("flush");
    let [segment] = segment_units(&handle)[..] else {
        unreachable!("one segment");
    };

    write(&handle, vec![upsert("a", 3.0, json!({"v": 2}))]);
    assert_eq!(
        deleted_rows(&handle, segment),
        [0],
        "a was the segment's first row"
    );
    let version = handle.current();
    assert_eq!(version.counters.deleted_rows, 1);
    assert_eq!(version.active.slot_count(), 1);
    assert!(
        version.manifest.segments[0].dv.is_none(),
        "no DV file before the next checkpoint"
    );
    drop(version);
    assert_eq!(row(&handle, "a"), Some((vec![3.0, 1.0], json!({"v": 2}))));

    core.flush_collection(&handle).expect("flush");
    let version = handle.current();
    let entry = version
        .manifest
        .segments
        .iter()
        .find(|entry| entry.unit == segment)
        .expect("the segment stays");
    let dv = entry.dv.expect("the flush wrote the segment's DV file");
    assert_eq!(dv.cardinality, 1);
    assert!(exists(
        &fault,
        &dv_path(&handle.meta().dir, segment, dv.generation)
    ));
    drop(version);

    let before = live(&handle);
    drop((handle, core));
    drop(engine);
    fault.crash();
    let engine = open(&fault);
    let handle = reopen(&engine, "upserts");
    assert_eq!(live(&handle), before);
    assert_eq!(
        deleted_rows(&handle, segment),
        [0],
        "loaded from the DV file"
    );
}

/// `Update` merges its patch into the key's live row, whether that row is in a segment, in
/// the memtable, or written earlier in the same batch, and logs the merged row, so replay
/// needs no read. An update of a key with no live row fails its request with `NotFound`.
#[test]
fn a_partial_update_merges_into_the_live_row_wherever_it_lives() {
    let fault = FaultVfs::new(61);
    let engine = open(&fault);
    let handle = create(&engine, "updates");
    let core = engine.core();
    write(
        &handle,
        vec![upsert("a", 1.0, json!({"color": "red", "size": 1}))],
    );
    core.flush_collection(&handle).expect("flush");
    let [segment] = segment_units(&handle)[..] else {
        unreachable!("one segment");
    };

    // The live row is in the segment.
    write(
        &handle,
        vec![update("a", |update| {
            update.extra.insert("size".to_owned(), json!(2));
        })],
    );
    assert_eq!(
        row(&handle, "a"),
        Some((vec![1.0, 1.0], json!({"color": "red", "size": 2})))
    );
    assert_eq!(deleted_rows(&handle, segment), [0]);

    // The live row is in the memtable: replace the vector and remove a key.
    write(
        &handle,
        vec![update("a", |update| {
            update.vectors.insert("vector".to_owned(), vec![0.0, 2.0]);
            update
                .extra
                .insert("color".to_owned(), serde_json::Value::Null);
        })],
    );
    assert_eq!(
        row(&handle, "a"),
        Some((vec![0.0, 2.0], json!({"size": 2})))
    );
    let version = handle.current();
    assert_eq!(version.active.slot_count(), 2);
    assert_eq!(deleted_rows(&handle, version.active.unit), [0]);
    drop(version);

    // The live row is in the frozen memtable of a flush that is still building.
    write(&handle, vec![upsert("b", 1.0, json!({"n": 1}))]);
    let (mut ticket, start) = handle.begin_job(JobKind::Flush).expect("flush begins");
    let JobWork::Flush(work) = start.work else {
        unreachable!("the memtable has operations to flush");
    };
    write(
        &handle,
        vec![update("b", |update| {
            update.extra.insert("m".to_owned(), json!(2));
        })],
    );
    let commit = core
        .build_flush(&handle, &start.version, start.unit, &work, &mut ticket)
        .expect("the flush builds");
    ticket.commit(commit).expect("the flush commits");
    drop((start.version, work));
    assert_eq!(
        row(&handle, "b"),
        Some((vec![1.0, 1.0], json!({"n": 1, "m": 2})))
    );

    // No live row: the request fails and consumes no sequence number.
    write(&handle, vec![delete("b")]);
    let visible = handle.current().visible_seq_no;
    for missing in ["b", "never"] {
        let error = handle
            .write_blocking(vec![update(missing, |update| {
                update.extra.insert("x".to_owned(), json!(1));
            })])
            .expect_err("an update needs a live row");
        assert!(
            matches!(
                &error,
                LogPoseError::NotFound {
                    resource: ResourceKind::Record,
                    ..
                }
            ),
            "{error:?}"
        );
    }
    assert_eq!(handle.current().visible_seq_no, visible);

    // Replay applies the logged rows.
    let before = live(&handle);
    drop((handle, core));
    drop(engine);
    fault.crash();
    let engine = open(&fault);
    let handle = reopen(&engine, "updates");
    assert_eq!(live(&handle), before);
}

/// Deletions that land on the frozen memtable while its flush builds are mapped onto the new
/// segment's rows at commit. They are above the flush's checkpoint, so recovery replays them
/// onto the segment, and the next flush writes them into its DV file.
#[test]
fn deletions_that_land_while_a_flush_builds_reach_the_new_segment() {
    let fault = FaultVfs::new(62);
    let engine = open(&fault);
    let handle = create(&engine, "late");
    let core = engine.core();
    write(
        &handle,
        vec![
            upsert("a", 1.0, json!({})),
            upsert("b", 2.0, json!({})),
            upsert("c", 3.0, json!({})),
        ],
    );
    let (mut ticket, start) = handle.begin_job(JobKind::Flush).expect("flush begins");
    let JobWork::Flush(work) = start.work else {
        unreachable!("the memtable has operations to flush");
    };
    write(&handle, vec![delete("b")]);
    write(&handle, vec![upsert("c", 30.0, json!({}))]);
    let commit = core
        .build_flush(&handle, &start.version, start.unit, &work, &mut ticket)
        .expect("the flush builds");
    ticket.commit(commit).expect("the flush commits");

    assert_eq!(segment_units(&handle), [start.unit]);
    assert_eq!(handle.current().segments[0].row_count(), 3);
    assert_eq!(deleted_rows(&handle, start.unit), [1, 2]);
    let expected = live(&handle);
    assert_eq!(
        expected.keys().map(String::as_str).collect::<Vec<_>>(),
        ["a", "c"]
    );
    assert_eq!(expected["c"].0, [30.0, 1.0]);

    drop((handle, core));
    drop(engine);
    fault.crash();
    let engine = open(&fault);
    let handle = reopen(&engine, "late");
    assert_eq!(live(&handle), expected);
    assert_eq!(
        deleted_rows(&handle, start.unit),
        [1, 2],
        "replayed from the WAL"
    );
    engine
        .core()
        .flush_collection(&handle)
        .expect("the next flush");
    let dv = handle
        .current()
        .manifest
        .segments
        .iter()
        .find(|entry| entry.unit == start.unit)
        .and_then(|entry| entry.dv)
        .expect("the next flush wrote the DV file");
    assert_eq!(dv.cardinality, 2);
}

/// Build two segments, `s1 = {a, b, c, d}` with `d` deleted and `s2 = {e, d}`, and return their
/// units.
fn two_segments(engine: &Engine, handle: &Arc<CollectionHandle>) -> (UnitId, UnitId) {
    let core = engine.core();
    write(
        handle,
        ["a", "b", "c", "d"]
            .iter()
            .zip(1..)
            .map(|(id, x)| upsert(id, x as f32, json!({})))
            .collect(),
    );
    core.flush_collection(handle).expect("flush");
    write(
        handle,
        vec![upsert("e", 5.0, json!({})), upsert("d", 40.0, json!({}))],
    );
    core.flush_collection(handle).expect("flush");
    let [s1, s2] = segment_units(handle)[..] else {
        unreachable!("two segments");
    };
    assert_eq!(deleted_rows(handle, s1), [3]);
    (s1, s2)
}

/// Compaction reconciliation over worked cases: a key upserted while the job builds, a key
/// upserted and then deleted, a delete of a row of the second input, and a row the job did not
/// copy because it was already deleted. Each deletion lands on the output's row exactly once,
/// the output's DV file is written before its manifest, the primary-key index forwards later
/// writes to the output, and a row compacted a second time keeps its key's single live row.
#[test]
fn deletions_that_land_while_a_compaction_builds_are_reconciled_onto_its_output() {
    let fault = FaultVfs::new(63);
    let engine = open(&fault);
    let handle = create(&engine, "compacted");
    let core = engine.core();
    let (s1, s2) = two_segments(&engine, &handle);

    let (mut ticket, start) = handle
        .begin_job(JobKind::Compact)
        .expect("compaction begins");
    let JobWork::Compact(work) = start.work else {
        unreachable!("two segments to compact");
    };
    assert_eq!(
        work.inputs
            .iter()
            .map(|(segment, _)| segment.unit)
            .collect::<Vec<_>>(),
        [s1, s2]
    );
    write(&handle, vec![upsert("a", 10.0, json!({}))]);
    write(&handle, vec![upsert("b", 20.0, json!({}))]);
    write(&handle, vec![delete("b")]);
    write(&handle, vec![delete("e")]);
    let expected = live(&handle);
    let commit = core
        .build_compaction(&handle, &start.version, start.unit, &work, &mut ticket)
        .expect("the compaction builds");
    ticket.commit(commit).expect("the compaction commits");
    let output = start.unit;
    drop((start.version, work));
    engine.wait_for_gc();

    assert_eq!(segment_units(&handle), [output]);
    // The output holds a, b, c from s1 (not d, deleted when the job began), then e, d from s2.
    assert_eq!(handle.current().segments[0].row_count(), 5);
    assert_eq!(deleted_rows(&handle, output), [0, 1, 3], "a, b and e");
    assert_eq!(live(&handle), expected);
    let dv = handle.current().manifest.segments[0]
        .dv
        .expect("the commit wrote the output's DV file");
    assert_eq!(dv.cardinality, 3);
    let dir = handle.meta().dir.clone();
    assert!(exists(&fault, &dv_path(&dir, output, dv.generation)));
    for input in [s1, s2] {
        assert!(
            !exists(&fault, &segment_path(&dir, input)),
            "{input} removed"
        );
    }
    let names = fault
        .process()
        .list(&dir.join("segments"))
        .expect("list")
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    assert_eq!(names.len(), 2, "the output and its DV file only: {names:?}");

    // The index forwards c from s1 to the output.
    write(&handle, vec![upsert("c", 30.0, json!({}))]);
    assert_eq!(deleted_rows(&handle, output), [0, 1, 2, 3]);

    // Compact again: d's row moves a second time, and the index follows it.
    core.flush_collection(&handle).expect("flush");
    core.compact_collection(&handle).expect("compact");
    let [second] = segment_units(&handle)[..] else {
        unreachable!("one segment");
    };
    assert!(deleted_rows(&handle, second).is_empty());
    write(&handle, vec![upsert("d", 41.0, json!({}))]);
    assert_eq!(deleted_rows(&handle, second).len(), 1);
    let expected = live(&handle);
    assert_eq!(
        expected.keys().map(String::as_str).collect::<Vec<_>>(),
        ["a", "c", "d"]
    );
    assert_eq!(expected["d"].0, [41.0, 1.0]);

    // Recovery rebuilds the index from the segments and the WAL to the same answers.
    drop((handle, core));
    drop(engine);
    fault.crash();
    let engine = open(&fault);
    let handle = reopen(&engine, "compacted");
    assert_eq!(live(&handle), expected);
    for id in ["a", "c", "d"] {
        write(&handle, vec![upsert(id, 0.5, json!({"again": true}))]);
    }
    let rewritten = live(&handle);
    assert_eq!(rewritten.len(), 3);
    assert!(
        rewritten
            .values()
            .all(|(vector, metadata)| *vector == [0.5, 1.0] && *metadata == json!({"again": true}))
    );
}

/// A crash right after the compaction's output DV file is synced, before its manifest, leaves
/// the inputs in force: recovery replays the deletion onto the input from the WAL and removes
/// the output and its DV file as orphans.
#[test]
fn a_crash_after_the_compaction_dv_sync_keeps_the_inputs() {
    let fault = FaultVfs::new(64);
    let engine = open(&fault);
    let handle = create(&engine, "crashed");
    let core = engine.core();
    let (s1, s2) = two_segments(&engine, &handle);
    let (mut ticket, start) = handle
        .begin_job(JobKind::Compact)
        .expect("compaction begins");
    let JobWork::Compact(work) = start.work else {
        unreachable!("two segments to compact");
    };
    write(&handle, vec![delete("a")]);
    let expected = live(&handle);
    let commit = core
        .build_compaction(&handle, &start.version, start.unit, &work, &mut ticket)
        .expect("the compaction builds");
    fault.set_plan(FaultPlan {
        crash_at: Some(CrashPoint::CompactionAfterDvSync),
        ..FaultPlan::default()
    });
    ticket
        .commit(commit)
        .expect_err("the crash stops the commit");
    assert!(fault.is_crashed());
    drop((handle, core));
    drop(engine);
    fault.crash();
    fault.set_plan(FaultPlan::default());

    let engine = open(&fault);
    let handle = reopen(&engine, "crashed");
    assert_eq!(segment_units(&handle), [s1, s2]);
    assert_eq!(live(&handle), expected);
    assert_eq!(
        deleted_rows(&handle, s1),
        [0, 3],
        "a from the WAL, d from s1's DV file"
    );
    let dir = handle.meta().dir.clone();
    let mut names = fault
        .process()
        .list(&dir.join("segments"))
        .expect("list")
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    names.sort();
    let dv = handle.current().manifest.segments[0]
        .dv
        .expect("s1's DV file");
    let mut expected_names = [
        segment_path(&dir, s1),
        segment_path(&dir, s2),
        dv_path(&dir, s1, dv.generation),
    ]
    .iter()
    .filter_map(|path| path.file_name())
    .map(|name| name.to_string_lossy().into_owned())
    .collect::<Vec<_>>();
    expected_names.sort();
    assert_eq!(
        names, expected_names,
        "the output and its DV file are orphans"
    );
}

/// The schema refuses to drop a collection's last vector field, so rows always carry one; a
/// rename of it is a schema change like any other, and flush, compaction, and recovery read
/// the renamed field's vectors.
#[test]
fn the_last_vector_field_cannot_be_dropped_and_a_renamed_one_keeps_flushing() {
    let fault = FaultVfs::new(65);
    let engine = open(&fault);
    let handle = create(&engine, "renamed");
    let core = engine.core();
    write(&handle, vec![upsert("a", 1.0, json!({"k": 1}))]);
    core.flush_collection(&handle).expect("flush");
    write(&handle, vec![upsert("b", 2.0, json!({"k": 2}))]);
    let error = handle
        .alter_schema_blocking(SchemaChange::DropField {
            name: "vector".to_owned(),
        })
        .expect_err("the last vector field stays");
    assert!(error.to_string().contains("last vector field"), "{error}");
    handle
        .alter_schema_blocking(SchemaChange::RenameField {
            from: "vector".to_owned(),
            to: "embedding".to_owned(),
        })
        .expect("the vector field renames");
    write(
        &handle,
        vec![ClientOp::Upsert(
            Record::new("c").with_vector("embedding", vec![3.0, 1.0]),
        )],
    );
    core.flush_collection(&handle).expect("flush");
    core.compact_collection(&handle).expect("compact");
    let vectors = |handle: &CollectionHandle| {
        let version = handle.current();
        version.check_invariants().expect("invariants hold");
        let mut rows = version
            .live_images()
            .into_iter()
            .map(|(_, image)| {
                let record = image.to_record(&version.schema).expect("row reads");
                (
                    legacy_id(&image.pk).as_str().to_owned(),
                    record.vectors.get("embedding").cloned(),
                )
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        rows
    };
    let expected = vec![
        ("a".to_owned(), Some(vec![1.0, 1.0])),
        ("b".to_owned(), Some(vec![2.0, 1.0])),
        ("c".to_owned(), Some(vec![3.0, 1.0])),
    ];
    assert_eq!(vectors(&handle), expected);

    drop((handle, core));
    drop(engine);
    fault.crash();
    let engine = open(&fault);
    let handle = reopen(&engine, "renamed");
    assert_eq!(vectors(&handle), expected);
}
