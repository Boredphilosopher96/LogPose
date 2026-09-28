//! Shared fixtures for the read-path tests: typed collections on a local engine, clustered
//! vectors, and a deterministic generator.

#![allow(dead_code, reason = "each test crate uses a subset")]

use logpose_storage::{
    CollectionHandle, CreateCollectionRequest, EngineConfig, IndexPolicy, LocalStorageEngine,
    ReadOptions, ReadView, SchemaChange, StorageEngine,
};
use logpose_types::{
    CollectionRef, DistanceMetric,
    record::{ClientOp, Record},
    schema::{FieldType, ScalarFieldSpec},
    value::Value,
};
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

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

pub fn unique_temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-{name}-{unique}"));
    if path.exists() {
        fs::remove_dir_all(&path).expect("stale temp dir should be removable");
    }
    path
}

/// A typed collection on its own local engine.
pub struct Fixture {
    pub engine: LocalStorageEngine,
    pub handle: Arc<CollectionHandle>,
    pub reference: CollectionRef,
    pub root: PathBuf,
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
        let root = unique_temp_dir(label);
        let engine = LocalStorageEngine::with_config(
            &root,
            EngineConfig {
                index: policy,
                resolver: Some(logpose_query::resolver()),
                ..EngineConfig::default()
            },
        )
        .expect("engine should open");
        Self::create(engine, root, dims, metric, fields).await
    }

    /// Reopen on the same root (the previous engine must be dropped).
    pub fn reopen(root: &PathBuf, policy: IndexPolicy) -> LocalStorageEngine {
        LocalStorageEngine::with_config(
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
        engine: LocalStorageEngine,
        root: PathBuf,
        dims: usize,
        metric: DistanceMetric,
        fields: &[(&str, FieldType)],
    ) -> Self {
        let request = CreateCollectionRequest::new("items", dims, metric);
        let reference = request.collection_ref();
        engine
            .create_collection(request)
            .await
            .expect("collection should be created");
        let handle = engine
            .engine()
            .collection(&reference)
            .expect("collection handle");
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
            root,
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
        self.engine
            .flush(&self.reference.lookup_name())
            .await
            .expect("flush should succeed");
    }

    pub async fn compact(&self) {
        self.engine
            .compact(&self.reference.lookup_name())
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
