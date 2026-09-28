//! VectorDBBench-style workloads against a running server, for comparing
//! LogPose with other vector databases on the same data.
//!
//! `vdb-prepare` writes a dataset directory (vectors, a `rank` scalar column,
//! and exact ground truth per case) that every driver reads; see [`files`].
//! `vdb-run` loads it into a LogPose server over gRPC and runs the cases:
//! unfiltered search and `rank < t` range filters at the prepared
//! selectivities. For each case it sweeps `ef` until mean recall@k reaches the
//! target, then measures throughput and latency at that `ef` with 1, 4, and 8
//! concurrent clients. The Milvus driver in `scripts/bench/milvus_vdb.py` does
//! the same against Milvus and writes the same [`report::VdbReport`] shape.

pub mod files;
pub mod logpose;
pub mod report;

use crate::{
    dataset::{EmbeddingLikeSpec, Metric},
    report::MachineInfo,
};
use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use files::{PrepareRequest, Prepared};
use logpose::DriverConfig;
use report::{LoadReport, REPORT_SCHEMA, VdbReport};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Dataset shapes. Each mirrors the row count, dimensionality, query count, and
/// metric of a VectorDBBench dataset, with synthetic embedding-like vectors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Shape {
    /// 2,000 rows of 32 dimensions; for smoke tests.
    Tiny,
    /// VectorDBBench Cohere small: 100,000 rows of 768 dimensions, cosine.
    #[value(name = "cohere-100k")]
    Cohere100k,
    /// VectorDBBench OpenAI small: 50,000 rows of 1,536 dimensions, cosine.
    #[value(name = "openai-50k")]
    Openai50k,
    /// VectorDBBench Cohere medium: 1,000,000 rows of 768 dimensions, cosine.
    #[value(name = "cohere-1m")]
    Cohere1m,
    /// VectorDBBench OpenAI medium: 500,000 rows of 1,536 dimensions, cosine.
    #[value(name = "openai-500k")]
    Openai500k,
}

impl Shape {
    /// Directory and report name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Tiny => "tiny",
            Self::Cohere100k => "cohere-100k",
            Self::Openai50k => "openai-50k",
            Self::Cohere1m => "cohere-1m",
            Self::Openai500k => "openai-500k",
        }
    }

    /// The public dataset whose shape this mirrors.
    #[must_use]
    pub fn mirrors(self) -> Option<&'static str> {
        match self {
            Self::Tiny => None,
            Self::Cohere100k => Some("VectorDBBench Cohere 100K (768 dims, cosine)"),
            Self::Openai50k => Some("VectorDBBench OpenAI 50K (1536 dims, cosine)"),
            Self::Cohere1m => Some("VectorDBBench Cohere 1M (768 dims, cosine)"),
            Self::Openai500k => Some("VectorDBBench OpenAI 500K (1536 dims, cosine)"),
        }
    }

    /// Generator parameters for this shape.
    ///
    /// The latent structure (32 latent dimensions, 256 clusters) gives well-defined
    /// nearest neighbors, unlike an isotropic Gaussian. At 50K and 100K rows an
    /// HNSW index with `M = 16` and `ef_construction = 200` reaches recall@10 of
    /// 0.95 at an `ef` of 16 to 24, so the data may be easier than real embeddings.
    #[must_use]
    pub fn generator(self, seed: u64) -> EmbeddingLikeSpec {
        let (n, dims, queries) = match self {
            Self::Tiny => (2_000, 32, 50),
            Self::Cohere100k => (100_000, 768, 1_000),
            Self::Openai50k => (50_000, 1_536, 1_000),
            Self::Cohere1m => (1_000_000, 768, 1_000),
            Self::Openai500k => (500_000, 1_536, 1_000),
        };
        EmbeddingLikeSpec {
            n,
            queries,
            dims,
            latent_dims: if self == Self::Tiny { 8 } else { 32 },
            clusters: if self == Self::Tiny { 16 } else { 256 },
            spread: 1.0,
            noise: 0.1,
            seed,
        }
    }
}

/// `vdb-prepare`: write a dataset directory with ground truth.
#[derive(Debug, Args)]
pub struct PrepareArgs {
    /// Dataset shape.
    #[arg(long, value_enum)]
    pub shape: Shape,
    /// Parent directory of prepared datasets; the dataset goes in `<dir>/<shape>`.
    /// Keep it outside the repository.
    #[arg(long, env = "LOGPOSE_BENCH_DATA")]
    pub data_dir: PathBuf,
    /// Seed for vectors, queries, and the `rank` column.
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
    /// Neighbors per query; recall is recall@k.
    #[arg(long, default_value_t = 10)]
    pub k: usize,
    /// Filter selectivities as fractions of rows, comma separated.
    #[arg(long, value_delimiter = ',', default_values_t = vec![0.01, 0.99])]
    pub selectivities: Vec<f64>,
    /// Regenerate even when a matching directory exists.
    #[arg(long)]
    pub force: bool,
}

impl PrepareArgs {
    fn request(&self) -> PrepareRequest {
        PrepareRequest {
            name: self.shape.name().to_owned(),
            mirrors: self.shape.mirrors().map(str::to_owned),
            generator: self.shape.generator(self.seed),
            metric: Metric::Cosine,
            k: self.k,
            selectivities: self.selectivities.clone(),
        }
    }
}

/// `vdb-run`: load a prepared dataset into a LogPose server and run every case.
#[derive(Debug, Args)]
pub struct RunArgs {
    /// Prepared dataset directory (from `vdb-prepare`).
    #[arg(long)]
    pub dataset: PathBuf,
    /// LogPose gRPC endpoint.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    pub endpoint: String,
    /// Database for the benchmark collection.
    #[arg(long, default_value = "default")]
    pub database: String,
    /// Benchmark collection; an existing collection of this name is dropped.
    #[arg(long, default_value = "vdb_bench")]
    pub collection: String,
    /// Rows per insert request.
    #[arg(long, default_value_t = 1_000)]
    pub batch_size: usize,
    /// `ef` values to sweep, ascending; the sweep stops at the first that meets the target.
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = vec![16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024]
    )]
    pub ef: Vec<u32>,
    /// Mean recall@k the sweep looks for.
    #[arg(long, default_value_t = 0.95)]
    pub target_recall: f64,
    /// Concurrent client counts for the throughput runs, comma separated.
    #[arg(long, value_delimiter = ',', default_values_t = vec![1, 4, 8])]
    pub concurrency: Vec<usize>,
    /// Seconds per throughput run.
    #[arg(long, default_value_t = 20.0)]
    pub duration: f64,
    /// Untimed queries before each case.
    #[arg(long, default_value_t = 100)]
    pub warmup: usize,
    /// Query the existing collection instead of loading it again.
    #[arg(long)]
    pub skip_load: bool,
    /// HNSW `M` the server was configured with, recorded in the report.
    #[arg(long, default_value_t = 16)]
    pub hnsw_m: usize,
    /// HNSW `ef_construction` the server was configured with, recorded in the report.
    #[arg(long, default_value_t = 128)]
    pub hnsw_ef_construction: usize,
    /// Where to write the JSON report.
    #[arg(long)]
    pub output: PathBuf,
}

impl RunArgs {
    fn driver_config(&self) -> DriverConfig {
        DriverConfig {
            endpoint: self.endpoint.clone(),
            database: self.database.clone(),
            collection: self.collection.clone(),
            batch_size: self.batch_size,
            ef_sweep: self.ef.clone(),
            target_recall: self.target_recall,
            concurrency: self.concurrency.clone(),
            duration: Duration::from_secs_f64(self.duration.max(0.1)),
            warmup: self.warmup,
            skip_load: self.skip_load,
        }
    }
}

/// Run `vdb-prepare`.
pub fn prepare(args: &PrepareArgs) -> Result<()> {
    let prepared = files::prepare(&args.data_dir, &args.request(), args.force)?;
    println!("{}", prepared.dir.display());
    Ok(())
}

/// Run `vdb-run` and write the report.
pub fn run(args: &RunArgs) -> Result<VdbReport> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    let prepared = Arc::new(files::load(&args.dataset)?);
    let report = runtime.block_on(run_async(args, prepared))?;
    write_report(&args.output, &report)?;
    Ok(report)
}

async fn run_async(args: &RunArgs, prepared: Arc<Prepared>) -> Result<VdbReport> {
    let started = Instant::now();
    let started_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let config = args.driver_config();
    let system_version = logpose::server_version(&config).await.ok();
    let load = if config.skip_load {
        LoadReport::default()
    } else {
        logpose::load(&config, Arc::clone(&prepared)).await?
    };
    let server_stats = logpose::server_stats(&config).await.ok();
    let cases = logpose::run_cases(&config, Arc::clone(&prepared)).await?;
    let mut notes = vec![
        "recall is mean recall@k against exact ground truth computed by logpose-bench vdb-prepare".to_owned(),
        "latency is measured by the client around each gRPC call, so it includes serialization and loopback transport".to_owned(),
        "each concurrent client has its own gRPC connection and runs in the driver's tokio runtime on the same machine as the server".to_owned(),
        "LogPose searches with at least 4 * k candidates, so sweep values below that run (and are reported) as 4 * k; filtered walks that come up short widen ef further".to_owned(),
    ];
    if prepared.manifest.synthetic {
        notes.push(
            "the dataset is synthetic (embedding-like, see dataset.generator); it mirrors only the shape of the public dataset named in dataset.mirrors".to_owned(),
        );
    }
    if cfg!(debug_assertions) {
        notes.push("debug build: numbers are not representative, rerun with --release".to_owned());
    }
    Ok(VdbReport {
        schema: REPORT_SCHEMA,
        system: "logpose".to_owned(),
        system_version,
        endpoint: config.endpoint.clone(),
        driver: format!("logpose-bench {} vdb-run", env!("CARGO_PKG_VERSION")),
        started_at_unix,
        total_seconds: started.elapsed().as_secs_f64(),
        machine: MachineInfo::collect(),
        dataset: prepared.manifest.clone(),
        index: serde_json::json!({
            "type": "hnsw",
            "m": args.hnsw_m,
            "ef_construction": args.hnsw_ef_construction,
            "quantization": "sq8 traversal with f32 rerank",
            "scalar_index": "auto (inverted and sorted for int64)",
        }),
        target_recall: config.target_recall,
        load,
        cases,
        server_stats,
        notes,
    })
}

fn write_report(path: &std::path::Path, report: &VdbReport) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut json = serde_json::to_string_pretty(report)?;
    json.push('\n');
    std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{RunArgs, files, logpose};
    use crate::vdb::report::SweepPoint;
    use crate::{metrics::LatencySummary, vdb::files::scratch_dir};
    use logpose_config::LogPoseConfig;
    use logpose_core::AppState;
    use std::sync::Arc;

    fn point(ef: u32, recall: f64) -> SweepPoint {
        SweepPoint {
            ef,
            recall,
            min_recall: recall,
            queries: 1,
            qps: 1.0,
            latency: LatencySummary::default(),
        }
    }

    #[test]
    fn sweep_values_below_the_candidate_floor_run_at_the_floor() -> anyhow::Result<()> {
        assert_eq!(
            logpose::effective_sweep(&[16, 24, 32, 48, 64], 10)?,
            vec![40, 48, 64]
        );
        assert!(logpose::effective_sweep(&[], 10).is_err());
        Ok(())
    }

    #[test]
    fn choose_ef_takes_the_first_that_meets_the_target() -> anyhow::Result<()> {
        let sweep = vec![point(16, 0.8), point(32, 0.96), point(64, 0.99)];
        assert_eq!(logpose::choose_ef(&sweep, 0.95)?, (32, 0.96, true));
        let short = vec![point(16, 0.8), point(32, 0.9), point(64, 0.85)];
        assert_eq!(logpose::choose_ef(&short, 0.95)?, (32, 0.9, false));
        assert!(logpose::choose_ef(&[], 0.95).is_err());
        Ok(())
    }

    /// Start an in-process server, prepare a tiny dataset, and run the whole
    /// driver against it. Segments this small are searched exactly, so recall
    /// must be perfect.
    #[test]
    fn runs_every_case_against_an_in_process_server() -> anyhow::Result<()> {
        let root_dir = scratch_dir("run")?;
        let root = root_dir.path().to_path_buf();
        let prepared = files::prepare(&root.join("data"), &files::tests::request(), false)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let listener = runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
        let address = listener.local_addr()?;
        let config = LogPoseConfig {
            node_name: "vdb-test".to_owned(),
            grpc_port: address.port(),
            storage_root: root.join("server"),
            log_filter: "warn".to_owned(),
            ..LogPoseConfig::default()
        };
        let state = Arc::new(AppState::try_new(config)?);
        let server = runtime.spawn(logpose_api_grpc::serve_with_listener(state, listener));

        let args = RunArgs {
            dataset: prepared.dir.clone(),
            endpoint: format!("http://{address}"),
            database: "default".to_owned(),
            collection: "vdb_test".to_owned(),
            batch_size: 64,
            ef: vec![16, 64],
            target_recall: 0.95,
            concurrency: vec![1, 2],
            duration: 0.2,
            warmup: 2,
            skip_load: false,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            output: root.join("report.json"),
        };
        let report = super::run(&args)?;
        assert_eq!(report.load.rows, 300);
        assert_eq!(report.cases.len(), 3);
        for case in &report.cases {
            assert!(
                case.met_target,
                "{} recall {}",
                case.name, case.chosen_recall
            );
            // k is 5, so the sweep starts at the 4 * k candidate floor.
            assert_eq!(case.chosen_ef, 20);
            assert_eq!(case.sweep.len(), 1);
            assert_eq!(case.concurrency.len(), 2);
            for run in &case.concurrency {
                assert!(run.queries > 0);
                assert!(
                    (run.recall - 1.0).abs() < 1e-9,
                    "{} recall {}",
                    case.name,
                    run.recall
                );
            }
        }
        assert_eq!(report.cases[1].filter.as_deref(), Some("rank < 3"));
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&args.output)?)?;
        assert_eq!(json["schema"], "logpose-vdb-report/1");
        assert_eq!(json["dataset"]["synthetic"], true);

        // A second run can query the loaded collection without reloading it.
        let again = super::run(&RunArgs {
            skip_load: true,
            concurrency: vec![1],
            ..args
        })?;
        assert_eq!(again.load.rows, 0);
        assert!(again.cases.iter().all(|case| case.met_target));

        server.abort();
        drop(runtime);
        Ok(())
    }
}
