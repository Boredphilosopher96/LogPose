//! The engine under test.
//!
//! [`BenchTarget`] is deliberately small and synchronous so that very different
//! systems can implement it: the in-process engine today, a future engine
//! behind the same trait, a REST or gRPC client, or another database for
//! comparison. Targets own any async runtime they need.

use crate::dataset::Metric;
use anyhow::{Context, Result, anyhow, ensure};
use logpose_query::{ExplainMode, Predicate, QueryRequest, query_exact};
use logpose_storage::{CreateCollectionRequest, LocalStorageEngine, StorageEngine};
use logpose_types::{DistanceMetric, PutRecord, RecordId, WriteOperation};
use serde_json::{Map, Value};
use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Runtime;

/// Collection shape the harness asks a target to create.
#[derive(Clone, Debug)]
pub struct CollectionSpec {
    /// Collection name.
    pub name: String,
    /// Vector dimensionality.
    pub dims: usize,
    /// Search metric.
    pub metric: Metric,
    /// Integer scalar fields every row carries, used by filters.
    pub scalar_fields: Vec<String>,
}

/// One row to ingest.
#[derive(Clone, Debug)]
pub struct Row<'a> {
    /// Row id; results are reported with the same id.
    pub id: u64,
    /// Vector payload.
    pub vector: &'a [f32],
    /// Integer scalar attributes by field name.
    pub scalars: Vec<(&'a str, i64)>,
}

/// One search to run.
#[derive(Clone, Copy, Debug)]
pub struct SearchRequest<'a> {
    /// Query vector.
    pub vector: &'a [f32],
    /// Optional filter in the query crate's predicate AST.
    pub filter: Option<&'a Predicate>,
    /// Number of neighbors to return.
    pub k: usize,
}

/// Result of one search.
#[derive(Clone, Debug, Default)]
pub struct SearchResponse {
    /// Returned row ids, best first.
    pub ids: Vec<u64>,
    /// Engine-reported plan name, when the target exposes one.
    pub plan: Option<String>,
}

/// A system the harness can benchmark.
pub trait BenchTarget: Send + Sync {
    /// Short stable identifier recorded in the report.
    fn name(&self) -> &'static str;

    /// Create an empty collection.
    fn create(&mut self, spec: &CollectionSpec) -> Result<()>;

    /// Ingest one batch. Returning means the write is acknowledged.
    fn ingest(&self, rows: &[Row<'_>]) -> Result<()>;

    /// Make buffered writes durable in immutable form and build indexes, if the
    /// target has such a step. Returns whether anything ran.
    fn flush_if_supported(&self) -> Result<bool>;

    /// Run one top-k search.
    fn search(&self, request: &SearchRequest<'_>) -> Result<SearchResponse>;

    /// Target-specific statistics for the report.
    fn stats(&self) -> Result<Option<Value>> {
        Ok(None)
    }
}

/// How long to wait for background maintenance to settle.
const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(600);

/// The current in-process engine: `LocalStorageEngine` plus `query_exact`.
///
/// Queries run with [`ExplainMode::Plan`] so the report can show which plan
/// the planner chose; plan diagnostics are cheap to produce.
pub struct LocalEngineTarget {
    runtime: Runtime,
    engine: LocalStorageEngine,
    root: PathBuf,
    remove_on_drop: bool,
    collection: Option<CollectionSpec>,
}

impl LocalEngineTarget {
    /// Open an engine rooted at `root`, or at a fresh temporary directory.
    ///
    /// A temporary directory is deleted when the target is dropped.
    pub fn open(root: Option<PathBuf>) -> Result<Self> {
        let remove_on_drop = root.is_none();
        let root = match root {
            Some(root) => root,
            None => {
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_nanos())
                    .unwrap_or_default();
                std::env::temp_dir().join(format!("logpose-bench-{}-{nanos}", std::process::id()))
            }
        };
        std::fs::create_dir_all(&root)
            .with_context(|| format!("creating engine root {}", root.display()))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("building tokio runtime")?;
        let engine = LocalStorageEngine::new(&root)
            .with_context(|| format!("opening engine at {}", root.display()))?;
        Ok(Self {
            runtime,
            engine,
            root,
            remove_on_drop,
            collection: None,
        })
    }

    fn collection(&self) -> Result<&str> {
        self.spec().map(|spec| spec.name.as_str())
    }

    fn spec(&self) -> Result<&CollectionSpec> {
        self.collection
            .as_ref()
            .ok_or_else(|| anyhow!("collection has not been created"))
    }

    fn wait_for_maintenance(&self) -> Result<()> {
        let collection = self.collection()?;
        let started = Instant::now();
        // The engine clears `in_progress` before it enqueues follow-up work (a
        // flush that crosses the compaction threshold), so one idle reading can
        // fall in that gap. Require two idle readings with no run in between.
        let mut idle_after_runs = None;
        loop {
            let stats = self.runtime.block_on(self.engine.stats(collection))?;
            let maintenance = &stats.maintenance;
            if let Some(error) = maintenance.last_error.as_ref() {
                return Err(anyhow!("background maintenance failed: {error}"));
            }
            if maintenance.pending.is_empty() && maintenance.in_progress.is_none() {
                if idle_after_runs == Some(maintenance.completed_runs) {
                    return Ok(());
                }
                idle_after_runs = Some(maintenance.completed_runs);
            } else {
                idle_after_runs = None;
            }
            if started.elapsed() > MAINTENANCE_TIMEOUT {
                return Err(anyhow!(
                    "background maintenance did not settle within {MAINTENANCE_TIMEOUT:?}"
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for LocalEngineTarget {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

fn engine_metric(metric: Metric) -> DistanceMetric {
    match metric {
        Metric::L2 => DistanceMetric::L2,
        Metric::Cosine => DistanceMetric::Cosine,
        Metric::Dot => DistanceMetric::Dot,
    }
}

impl BenchTarget for LocalEngineTarget {
    fn name(&self) -> &'static str {
        "logpose-local-engine"
    }

    fn create(&mut self, spec: &CollectionSpec) -> Result<()> {
        self.runtime
            .block_on(self.engine.create_collection(CreateCollectionRequest::new(
                spec.name.clone(),
                spec.dims,
                engine_metric(spec.metric),
            )))
            .context("creating collection")?;
        self.collection = Some(spec.clone());
        Ok(())
    }

    fn ingest(&self, rows: &[Row<'_>]) -> Result<()> {
        let spec = self.spec()?;
        let operations = rows
            .iter()
            .map(|row| {
                // The engine is schemaless today; enforce the declared fields
                // anyway so the harness honors the contract a typed schema needs.
                let metadata = row
                    .scalars
                    .iter()
                    .map(|(field, value)| {
                        ensure!(
                            spec.scalar_fields.iter().any(|declared| declared == field),
                            "row {} has undeclared scalar field {field}",
                            row.id
                        );
                        Ok(((*field).to_owned(), Value::from(*value)))
                    })
                    .collect::<Result<Map<_, _>>>()?;
                Ok(WriteOperation::Put(PutRecord {
                    id: RecordId::new(row.id.to_string()),
                    vector: row.vector.to_vec(),
                    metadata: Value::Object(metadata),
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        self.runtime
            .block_on(self.engine.write(self.collection()?, operations))
            .context("writing batch")?;
        Ok(())
    }

    fn flush_if_supported(&self) -> Result<bool> {
        // Automatic flushes may be queued by ingest; let them finish first so
        // the explicit flush does not race them, then wait for any compaction.
        self.wait_for_maintenance()?;
        self.runtime
            .block_on(self.engine.flush(self.collection()?))
            .context("flushing collection")?;
        self.wait_for_maintenance()?;
        Ok(true)
    }

    fn search(&self, request: &SearchRequest<'_>) -> Result<SearchResponse> {
        let response = self
            .runtime
            .block_on(query_exact(
                &self.engine,
                QueryRequest {
                    collection_name: self.collection()?.to_owned(),
                    vector: request.vector.to_vec(),
                    top_k: request.k,
                    snapshot: None,
                    read_barrier: None,
                    filters: Vec::new(),
                    predicate: request.filter.cloned(),
                    explain: ExplainMode::Plan,
                },
            ))
            .context("running query")?;
        let ids = response
            .matches
            .iter()
            .map(|candidate| {
                candidate
                    .id
                    .as_str()
                    .parse::<u64>()
                    .with_context(|| format!("unexpected record id {}", candidate.id.as_str()))
            })
            .collect::<Result<Vec<_>>>()?;
        let plan = response.diagnostics.and_then(|diagnostics| {
            serde_json::to_value(diagnostics.chosen_plan)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
        });
        Ok(SearchResponse { ids, plan })
    }

    fn stats(&self) -> Result<Option<Value>> {
        let stats = self
            .runtime
            .block_on(self.engine.stats(self.collection()?))?;
        let disk_bytes = directory_size(&self.root);
        Ok(Some(serde_json::json!({
            "segment_count": stats.segment_count,
            "live_record_count": stats.live_record_count,
            "mutable_op_count": stats.mutable_op_count,
            "manifest_generation": stats.manifest_generation,
            "maintenance_completed_runs": stats.maintenance.completed_runs,
            "query_units": stats.query_units.len(),
            "on_disk_bytes": disk_bytes,
        })))
    }
}

fn directory_size(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(std::result::Result::ok)
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => directory_size(&entry.path()),
            Ok(_) => entry.metadata().map(|meta| meta.len()).unwrap_or(0),
            Err(_) => 0,
        })
        .sum()
}
