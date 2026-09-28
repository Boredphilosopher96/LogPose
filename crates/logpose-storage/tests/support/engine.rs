//! Engine helpers for the storage integration tests: collections of the single-vector shape
//! [`CreateCollectionRequest::new`] creates, and whole-collection scans over the read path
//! (`ReadView` and `logpose_query::ops::scroll_view`).

#![allow(dead_code, reason = "each test crate uses a subset")]

use logpose_catalog::CollectionDescriptor;
use logpose_query::ops::{ScrollOrder, scroll_view};
use logpose_storage::{
    CollectionHandle, CollectionReader, CreateCollectionRequest, Engine, EngineConfig, Projection,
    ReadOptions, SnapshotToken,
};
use logpose_types::{
    CollectionRef, Result, SeqNo, Snapshot,
    record::{ClientOp, PrimaryKey, Record},
};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};

/// Open an engine with the default configuration on the real filesystem.
pub fn open(root: &Path) -> Engine {
    Engine::open_local(root, EngineConfig::default()).expect("engine should open")
}

/// Plan and create the collection `request` describes.
pub async fn create(
    engine: &Engine,
    request: CreateCollectionRequest,
) -> Result<Arc<CollectionHandle>> {
    let descriptor = engine.plan_collection_descriptor(&request)?;
    engine.create_collection(descriptor, None).await
}

/// The open collection `name` (`collection` or `database/collection`).
pub fn handle(engine: &Engine, name: &str) -> Arc<CollectionHandle> {
    engine
        .collection(&CollectionRef::parse(name).expect("collection name"))
        .expect("the collection should be open")
}

/// The descriptor, with its live schema, of the collection `name`.
pub fn describe(engine: &Engine, name: &str) -> Result<CollectionDescriptor> {
    engine
        .collection(&CollectionRef::parse(name)?)
        .map(|handle| handle.describe())
}

/// An upsert of `id` with `vector` and the `$extra` object `extra`.
pub fn put_with(id: &str, vector: Vec<f32>, extra: Value) -> ClientOp {
    let mut record = Record::new(id).with_vector("vector", vector);
    if let Value::Object(extra) = extra {
        record.extra = extra;
    }
    ClientOp::Upsert(record)
}

/// An upsert of `id` with `vector` and `$extra` `{"key": id}`.
pub fn put(id: &str, vector: Vec<f32>) -> ClientOp {
    put_with(id, vector, json!({"key": id}))
}

/// A delete of `id`.
pub fn delete(id: &str) -> ClientOp {
    ClientOp::Delete(PrimaryKey::from(id))
}

/// One live row as the tests compare it: its key, vector, `$extra` object, and the sequence
/// number of its write.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    /// The key, as a label.
    pub id: String,
    /// The `vector` field.
    pub vector: Vec<f32>,
    /// The `$extra` object.
    pub metadata: Value,
    /// The sequence number of the row's write.
    pub seq_no: SeqNo,
}

impl Row {
    fn from_record(seq_no: SeqNo, mut record: Record) -> Self {
        Self {
            id: record.pk.label(),
            vector: record.vectors.remove("vector").unwrap_or_default(),
            metadata: Value::Object(record.extra),
            seq_no,
        }
    }
}

/// Every live row of the state `options` select, ordered by key.
pub async fn scan_with(engine: &Engine, name: &str, options: ReadOptions) -> Result<Vec<Row>> {
    let view = engine
        .read_view(&CollectionRef::parse(name)?, options)
        .await?;
    let (rows, _) = scroll_view(
        &view,
        None,
        &ScrollOrder::Pk,
        u32::MAX,
        Projection::full(),
        None,
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Row::from_record(row.seq_no, row.record))
        .collect())
}

/// Every live row of the current state, or of exactly `snapshot`, ordered by key.
pub async fn scan(engine: &Engine, name: &str, snapshot: Option<Snapshot>) -> Result<Vec<Row>> {
    let options = ReadOptions {
        snapshot,
        ..ReadOptions::default()
    };
    scan_with(engine, name, options).await
}

/// Every live row of the state `token` pins, ordered by key, extending the token's expiry.
pub async fn scan_at_token(engine: &Engine, name: &str, token: SnapshotToken) -> Result<Vec<Row>> {
    let options = ReadOptions {
        token: Some(token),
        ..ReadOptions::default()
    };
    scan_with(engine, name, options).await
}

/// Pin the current state of `name`, returning the token and the snapshot it names.
pub fn pin(engine: &Engine, name: &str) -> Result<(SnapshotToken, Snapshot)> {
    let handle = handle(engine, name);
    let token = handle.pin_snapshot()?;
    let snapshot = handle.snapshot_version(&token)?.snapshot();
    Ok((token, snapshot))
}
