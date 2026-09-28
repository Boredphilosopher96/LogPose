//! Golden files: the byte-exact storage root a small deterministic workload leaves behind,
//! committed under `crates/logpose-storage/tests/golden/root/`. It holds WAL frames (a
//! checkpoint group, write batches, a schema change), a manifest and `CURRENT`, two segments
//! with scalar index sections, and a deletion-vector file, so a change to any of those formats
//! (or to what the engine writes into them) fails here and must be made deliberately.
//!
//! The committed files are also opened as they are, so the current reader keeps reading what
//! the committed writer wrote. Regenerate after a deliberate format change with
//! `LOGPOSE_UPDATE_GOLDEN=1 cargo test -p logpose-storage --test harness golden`.

use logpose_storage::{CreateCollectionRequest, Engine, EngineConfig, IndexPolicy, SchemaChange};
use logpose_types::{
    CollectionId, CollectionRef, DistanceMetric,
    record::{ClientOp, PartialUpdate, PrimaryKey, Record},
    schema::{FieldType, ScalarFieldSpec},
    value::Value,
};
use logpose_vfs::{FaultVfs, OpenMode, Vfs};
use logpose_wal::BootId;
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::IoSlice,
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

const ROOT: &str = "/golden";

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/root")
}

fn config() -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new("golden")),
        strict_invariants: true,
        index: IndexPolicy {
            graph_min_rows: u32::MAX,
            sq8_min_rows: u32::MAX,
            ..IndexPolicy::default()
        },
        ..EngineConfig::default()
    }
}

fn reference() -> CollectionRef {
    CollectionRef::new_default("golden")
}

/// Run the workload on a fresh `FaultVfs` and return every file it left, by path under the
/// root.
fn generate() -> BTreeMap<PathBuf, Vec<u8>> {
    let fault = FaultVfs::new(1);
    {
        let engine = Engine::open(fault.process(), ROOT, config()).expect("engine opens");
        let mut descriptor = engine
            .plan_collection_descriptor(&CreateCollectionRequest::new(
                "golden",
                3,
                DistanceMetric::Dot,
            ))
            .expect("descriptor plans");
        descriptor.collection_id = CollectionId(Uuid::from_u128(0x0123_4567_89ab_cdef));
        descriptor.flush_threshold_ops = usize::MAX;
        descriptor.flush_threshold_bytes = usize::MAX;
        descriptor.compaction_threshold_segments = usize::MAX;
        let handle = engine
            .create_collection_blocking(descriptor, None)
            .expect("collection created");
        for (name, field_type) in [("n", FieldType::Int64), ("s", FieldType::String)] {
            handle
                .alter_schema_blocking(SchemaChange::AddField(ScalarFieldSpec::new(
                    name, field_type,
                )))
                .expect("field added");
        }
        let labelled = |key: &str, x: f32, n: i64, field: &str, label: &str| {
            let mut record = Record::new(key)
                .with_vector("vector", vec![x, 1.0, -x])
                .with_field("n", Value::Int64(n))
                .with_field(field, Value::String(label.to_owned()));
            record
                .extra
                .insert("tag".to_owned(), json!(format!("t{n}")));
            ClientOp::Upsert(record)
        };
        let record = |key: &str, x: f32, n: i64, label: &str| labelled(key, x, n, "s", label);
        let write = |ops: Vec<ClientOp>| {
            handle.write_blocking(ops).expect("write commits");
        };
        write(vec![
            record("a", 1.0, 1, "alpha"),
            record("b", 2.0, 2, "beta"),
            record("c", 3.0, 3, "gamma"),
        ]);
        let mut update = PartialUpdate::new("b");
        update
            .fields
            .insert("s".to_owned(), Value::String("bravo".to_owned()));
        write(vec![ClientOp::Update(update)]);
        handle.flush_blocking().expect("first flush");
        write(vec![
            ClientOp::Delete(PrimaryKey::from("a")),
            record("d", 4.0, 4, "delta"),
        ]);
        handle
            .alter_schema_blocking(SchemaChange::RenameField {
                from: "s".to_owned(),
                to: "label".to_owned(),
            })
            .expect("rename");
        handle
            .flush_blocking()
            .expect("second flush writes a DV file");
        // What stays in the WAL above the checkpoint: a batch, a schema change, an update.
        write(vec![labelled("e", 5.0, 5, "label", "echo")]);
        handle
            .alter_schema_blocking(SchemaChange::AddField(ScalarFieldSpec::new(
                "rank",
                FieldType::Int64,
            )))
            .expect("field added");
        let mut update = PartialUpdate::new("c");
        update.fields.insert("rank".to_owned(), Value::Int64(9));
        write(vec![ClientOp::Update(update)]);
        engine.wait_for_gc();
    }
    let mut files = BTreeMap::new();
    collect(fault.as_ref(), Path::new(ROOT), &mut files);
    // The collection directory has a random name; recovery finds a collection by scanning
    // `collections/` and replaces the descriptor's `root_path` with the directory it found, so
    // the committed copy uses a fixed name.
    let dir = files
        .keys()
        .find_map(|path| {
            let relative = path
                .strip_prefix(Path::new(ROOT).join("collections"))
                .ok()?;
            Some(
                relative
                    .components()
                    .next()?
                    .as_os_str()
                    .to_string_lossy()
                    .into_owned(),
            )
        })
        .expect("the collection has a directory");
    files
        .into_iter()
        .filter_map(|(path, mut bytes)| {
            let relative = path.strip_prefix(ROOT).ok()?.to_path_buf();
            if relative == Path::new("LOCK") {
                return None;
            }
            let renamed = PathBuf::from(relative.to_string_lossy().replace(&dir, "golden"));
            if renamed.ends_with("descriptor.json") {
                bytes = String::from_utf8_lossy(&bytes)
                    .replace(&dir, "golden")
                    .into_bytes();
            }
            Some((renamed, bytes))
        })
        .collect()
}

fn collect(vfs: &dyn Vfs, dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
    for entry in vfs.list(dir).expect("list") {
        let path = dir.join(&entry.name);
        if entry.is_dir {
            collect(vfs, &path, files);
        } else {
            files.insert(
                path.clone(),
                logpose_vfs::read_file(vfs, &path).expect("read"),
            );
        }
    }
}

fn committed() -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let root = golden_dir();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("the golden root is committed") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let relative = path
                    .strip_prefix(&root)
                    .expect("under the root")
                    .to_path_buf();
                files.insert(relative, std::fs::read(&path).expect("read"));
            }
        }
    }
    files
}

#[test]
fn the_engine_writes_the_committed_golden_files() {
    let generated = generate();
    assert_eq!(
        generate(),
        generated,
        "the golden workload is deterministic"
    );
    let kinds = |suffix: &str| {
        generated
            .keys()
            .filter(|path| path.to_string_lossy().contains(suffix))
            .count()
    };
    assert!(kinds(".seg") == 2 && kinds(".dv.") == 1 && kinds(".mf") >= 1 && kinds("wal/") >= 1);
    if std::env::var_os("LOGPOSE_UPDATE_GOLDEN").is_some() {
        let root = golden_dir();
        let _ = std::fs::remove_dir_all(&root);
        for (path, bytes) in &generated {
            let target = root.join(path);
            std::fs::create_dir_all(target.parent().expect("a parent")).expect("mkdir");
            std::fs::write(target, bytes).expect("golden file written");
        }
    }
    let committed = committed();
    let names = |files: &BTreeMap<PathBuf, Vec<u8>>| files.keys().cloned().collect::<Vec<_>>();
    assert_eq!(
        names(&generated),
        names(&committed),
        "the engine writes other files than the committed golden root; a deliberate format \
         change regenerates it with LOGPOSE_UPDATE_GOLDEN=1"
    );
    for (path, bytes) in &generated {
        assert!(
            committed[path] == *bytes,
            "{} changed ({} bytes, committed {}): a format change must be deliberate; \
             regenerate with LOGPOSE_UPDATE_GOLDEN=1 and document it",
            path.display(),
            bytes.len(),
            committed[path].len()
        );
    }
}

#[test]
fn the_committed_golden_files_open_and_read_back() {
    let fault = FaultVfs::new(2);
    let vfs: Arc<dyn Vfs> = fault.process();
    for (path, bytes) in committed() {
        let target = Path::new(ROOT).join(&path);
        let parent = target.parent().expect("a parent");
        vfs.create_dir_all(parent).expect("mkdir");
        let file = vfs.open(&target, OpenMode::CreateNew).expect("create");
        file.append(&[IoSlice::new(&bytes)]).expect("append");
        file.sync_all().expect("sync");
    }
    let engine = Engine::open(fault.process(), ROOT, config()).expect("the golden root opens");
    let handle = engine
        .collection(&reference())
        .expect("the golden collection");
    handle
        .current()
        .check_invariants()
        .expect("invariants hold");
    let keys = ["a", "b", "c", "d", "e"].map(PrimaryKey::from);
    let view = engine
        .read_view_blocking(&reference(), &Default::default())
        .expect("view");
    let rows = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(view.get(&keys, logpose_storage::Projection::full()))
        .expect("get");
    let found = rows
        .into_iter()
        .map(|row| {
            row.map(|row| {
                (
                    row.record.pk.clone(),
                    row.record.vectors["vector"][0],
                    row.record.fields.get("label").cloned(),
                    row.record.extra.get("tag").cloned(),
                )
            })
        })
        .collect::<Vec<_>>();
    let row = |key: &str, x: f32, label: &str, n: i64| {
        Some((
            PrimaryKey::from(key),
            x,
            Some(Value::String(label.to_owned())),
            Some(json!(format!("t{n}"))),
        ))
    };
    assert_eq!(
        found,
        vec![
            None,
            row("b", 2.0, "bravo", 2),
            row("c", 3.0, "gamma", 3),
            row("d", 4.0, "delta", 4),
            row("e", 5.0, "echo", 5),
        ]
    );
    let c = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(view.get(
            &[PrimaryKey::from("c")],
            logpose_storage::Projection::full(),
        ))
        .expect("get");
    assert_eq!(
        c[0].as_ref()
            .and_then(|row| row.record.fields.get("rank").cloned()),
        Some(Value::Int64(9)),
        "the WAL's schema change and update replay"
    );
    assert_eq!(view.counters().segment_count, 2);
    // `a` deleted, and `c` superseded by its update in the memtable.
    assert_eq!(view.counters().deleted_rows, 2);
}
