//! Shared fixtures for the read-path tests: typed collections on a local engine, clustered
//! vectors, and a deterministic generator.

#![allow(dead_code, reason = "each test crate uses a subset")]

use logpose_storage::{
    CollectionHandle, CreateCollectionRequest, Engine, EngineConfig, IndexPolicy, ReadOptions,
    ReadView, SchemaChange,
};
use logpose_types::{
    CollectionRef, DistanceMetric,
    record::{ClientOp, Record},
    schema::{FieldType, ScalarFieldSpec},
    value::Value,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::TempDir;

/// A deterministic generator (SplitMix64).
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1_u64 << 53) as f64
    }

    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound.max(1)
    }

    pub fn gaussian(&mut self) -> f32 {
        let u1 = self.unit().max(f64::MIN_POSITIVE);
        let u2 = self.unit();
        ((-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()) as f32
    }

    pub fn vector(&mut self, dims: usize) -> Vec<f32> {
        (0..dims).map(|_| self.gaussian()).collect()
    }
}

/// `count` random cluster centers of `dims` dimensions, spread with unit variance per axis.
pub fn centers(rng: &mut Rng, count: usize, dims: usize) -> Vec<Vec<f32>> {
    (0..count).map(|_| rng.vector(dims)).collect()
}

/// A point near `center`.
pub fn near(rng: &mut Rng, center: &[f32], spread: f32) -> Vec<f32> {
    center
        .iter()
        .map(|value| value + rng.gaussian() * spread)
        .collect()
}

/// A fresh directory named `logpose-{name}-…` under the system temp directory, removed when
/// the returned guard drops, also when the test panics.
pub fn unique_temp_dir(name: &str) -> TempDir {
    tempfile::Builder::new()
        .prefix(&format!("logpose-{name}-"))
        .tempdir()
        .expect("temp dir should be created")
}

/// A typed collection on its own local engine.
pub struct Fixture {
    pub engine: Engine,
    pub handle: Arc<CollectionHandle>,
    pub reference: CollectionRef,
    /// The engine's storage root, removed when the fixture drops. Declared last, so it drops
    /// after the engine.
    pub dir: TempDir,
}

impl Fixture {
    /// A collection `name` with string key `id`, one vector field `vector`, dynamic fields, and
    /// the typed scalar `fields`, on an engine that writes index sections per `policy`.
    pub async fn new(
        label: &str,
        dims: usize,
        metric: DistanceMetric,
        policy: IndexPolicy,
        fields: &[(&str, FieldType)],
    ) -> Self {
        let dir = unique_temp_dir(label);
        let engine = Engine::open_local(
            dir.path(),
            EngineConfig {
                index: policy,
                resolver: Some(logpose_query::resolver()),
                ..EngineConfig::default()
            },
        )
        .expect("engine should open");
        Self::create(engine, dir, dims, metric, fields).await
    }

    /// The engine's storage root.
    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// Drop the engine and hand back its storage root, which stays until the returned guard
    /// drops, for tests that change files on disk and reopen.
    pub fn close(self) -> TempDir {
        let Self {
            engine,
            handle,
            dir,
            ..
        } = self;
        drop(handle);
        drop(engine);
        dir
    }

    /// Reopen on the same root (the previous engine must be dropped).
    pub fn reopen(root: &PathBuf, policy: IndexPolicy) -> Engine {
        Engine::open_local(
            root,
            EngineConfig {
                index: policy,
                resolver: Some(logpose_query::resolver()),
                ..EngineConfig::default()
            },
        )
        .expect("engine should reopen")
    }

    async fn create(
        engine: Engine,
        dir: TempDir,
        dims: usize,
        metric: DistanceMetric,
        fields: &[(&str, FieldType)],
    ) -> Self {
        let request = CreateCollectionRequest::new("items", dims, metric);
        let reference = request.collection_ref();
        let descriptor = engine
            .plan_collection_descriptor(&request)
            .expect("collection should plan");
        let handle = engine
            .create_collection(descriptor, None)
            .await
            .expect("collection should be created");
        for (name, field_type) in fields {
            handle
                .alter_schema(SchemaChange::AddField(ScalarFieldSpec::new(
                    *name,
                    *field_type,
                )))
                .await
                .expect("field should be added");
        }
        Self {
            engine,
            handle,
            reference,
            dir,
        }
    }

    pub async fn view(&self) -> ReadView {
        self.view_with(ReadOptions::default()).await
    }

    pub async fn view_with(&self, options: ReadOptions) -> ReadView {
        use logpose_storage::CollectionReader;
        self.engine
            .read_view(&self.reference, options)
            .await
            .expect("view should open")
    }

    pub async fn upsert(&self, records: Vec<Record>) {
        self.handle
            .write(records.into_iter().map(ClientOp::Upsert).collect())
            .await
            .expect("write should succeed");
    }

    pub async fn flush(&self) {
        self.handle.flush().await.expect("flush should succeed");
    }

    pub async fn compact(&self) {
        self.handle
            .compact()
            .await
            .expect("compaction should succeed");
    }
}

/// A record with key `id`, vector, and typed fields.
pub fn record(id: &str, vector: Vec<f32>, fields: &[(&str, Value)]) -> Record {
    let mut record = Record::new(id).with_vector("vector", vector);
    for (name, value) in fields {
        record = record.with_field(*name, value.clone());
    }
    record
}
