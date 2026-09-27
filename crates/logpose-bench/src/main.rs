//! `logpose-bench`: the benchmark harness and scoreboard for LogPose engines.
//!
//! It generates or loads a dataset, computes exact ground truth in the
//! harness, drives a [`target::BenchTarget`] through ingest, flush, filtered and
//! unfiltered search, and freshness probes, then writes a JSON report and
//! prints a summary table.

mod dataset;
mod filter;
mod metrics;
mod oracle;
mod report;
mod rng;
mod runner;
mod target;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use dataset::{FvecsSpec, Metric, SyntheticSpec, generate_synthetic, load_fvecs_dataset};
use filter::{FilterMode, FilterStyle};
use report::RunConfig;
use std::{path::PathBuf, time::Instant};
use target::{BenchTarget, LocalEngineTarget};

/// Scale presets. Explicit flags override preset values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Preset {
    /// Seconds: 2k rows, 32 dims, 20 queries.
    Smoke,
    /// Minutes on the current engine: 20k rows, 128 dims, 200 queries.
    Small,
    /// 100k rows, 128 dims, 500 queries.
    Medium,
}

struct PresetValues {
    n: usize,
    dims: usize,
    queries: usize,
    clusters: usize,
    batch_size: usize,
    warmup: usize,
    freshness_probes: usize,
}

impl Preset {
    fn values(self) -> PresetValues {
        match self {
            Self::Smoke => PresetValues {
                n: 2_000,
                dims: 32,
                queries: 20,
                clusters: 8,
                batch_size: 500,
                warmup: 2,
                freshness_probes: 5,
            },
            Self::Small => PresetValues {
                n: 20_000,
                dims: 128,
                queries: 200,
                clusters: 32,
                batch_size: 1_000,
                warmup: 5,
                freshness_probes: 20,
            },
            Self::Medium => PresetValues {
                n: 100_000,
                dims: 128,
                queries: 500,
                clusters: 64,
                batch_size: 1_000,
                warmup: 10,
                freshness_probes: 20,
            },
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Small => "small",
            Self::Medium => "medium",
        }
    }
}

/// Systems the harness can drive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TargetKind {
    /// The in-process `LocalStorageEngine` with the `query_exact` planner.
    LocalEngine,
}

/// Benchmark a LogPose engine on ingest, freshness, latency, recall, memory, and I/O.
#[derive(Debug, Parser)]
#[command(name = "logpose-bench", version)]
struct Cli {
    /// Scale preset; explicit flags override its values.
    #[arg(long, value_enum, default_value_t = Preset::Smoke)]
    preset: Preset,
    /// Engine to benchmark.
    #[arg(long, value_enum, default_value_t = TargetKind::LocalEngine)]
    target: TargetKind,
    /// Base rows (for fvecs input, a limit on rows read).
    #[arg(long)]
    n: Option<usize>,
    /// Vector dimensionality (synthetic data only).
    #[arg(long)]
    dims: Option<usize>,
    /// Number of queries.
    #[arg(long)]
    queries: Option<usize>,
    /// Gaussian clusters (synthetic data only).
    #[arg(long)]
    clusters: Option<usize>,
    /// Fraction of clusters queries are drawn from (synthetic data only).
    #[arg(long, default_value_t = 0.25)]
    query_cluster_fraction: f64,
    /// Point standard deviation around cluster centers, relative to the spread of
    /// the centers themselves (synthetic data only). At 1.0 clusters overlap
    /// moderately; below about 0.5 they separate into islands.
    #[arg(long, default_value_t = 1.0)]
    spread: f64,
    /// Search metric.
    #[arg(long, value_enum, default_value_t = Metric::L2)]
    metric: Metric,
    /// Neighbors per query; recall is recall@k.
    #[arg(long, default_value_t = 10)]
    k: usize,
    /// Filter selectivities as fractions of rows, comma separated.
    #[arg(long, value_delimiter = ',', default_values_t = vec![0.001, 0.01, 0.1, 0.5, 0.99])]
    selectivities: Vec<f64>,
    /// Filter modes, comma separated.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_values_t = vec![FilterMode::Uncorrelated, FilterMode::AntiCorrelated]
    )]
    filter_modes: Vec<FilterMode>,
    /// How filters reach the engine: an equality flag per filter, or a range over a rank field.
    #[arg(long, value_enum, default_value_t = FilterStyle::Equality)]
    filter_style: FilterStyle,
    /// Rows per ingest batch.
    #[arg(long)]
    batch_size: Option<usize>,
    /// Client threads for an extra concurrent QPS pass per case; 1 disables it.
    #[arg(long, default_value_t = 1)]
    threads: usize,
    /// Untimed warmup queries per case.
    #[arg(long)]
    warmup: Option<usize>,
    /// Write-to-searchable probes.
    #[arg(long)]
    freshness_probes: Option<usize>,
    /// Skip the flush after ingest, so searches hit only unflushed data
    /// (background maintenance may still flush on its own).
    #[arg(long)]
    no_flush: bool,
    /// Seed for vectors, queries, and attributes.
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// SIFT-format base vectors; replaces the synthetic generator.
    #[arg(long)]
    base_fvecs: Option<PathBuf>,
    /// SIFT-format query vectors; without it, queries are held out from the base file.
    #[arg(long, requires = "base_fvecs")]
    query_fvecs: Option<PathBuf>,
    /// SIFT-format reference ground truth, used to validate the oracle.
    #[arg(long, requires = "base_fvecs")]
    ground_truth_ivecs: Option<PathBuf>,
    /// Engine data directory; defaults to a temporary directory removed afterwards.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Where to write the JSON report.
    #[arg(long, default_value = "target/logpose-bench/report.json")]
    output: PathBuf,
    /// Free-form label stored in the report.
    #[arg(long)]
    label: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let preset = cli.preset.values();
    let n = cli.n.unwrap_or(preset.n);
    let queries = cli.queries.unwrap_or(preset.queries);

    let load_started = Instant::now();
    let dataset = match &cli.base_fvecs {
        Some(base) => load_fvecs_dataset(
            &FvecsSpec {
                base: base.clone(),
                queries: cli.query_fvecs.clone(),
                ground_truth: cli.ground_truth_ivecs.clone(),
                limit_n: cli.n,
                query_count: queries,
            },
            cli.metric,
        )?,
        None => generate_synthetic(
            &SyntheticSpec {
                n,
                queries,
                dims: cli.dims.unwrap_or(preset.dims),
                clusters: cli.clusters.unwrap_or(preset.clusters),
                query_cluster_fraction: cli.query_cluster_fraction,
                spread: cli.spread,
                seed: cli.seed,
            },
            cli.metric,
        )?,
    };
    let prep = runner::PrepTimings {
        load_seconds: load_started.elapsed().as_secs_f64(),
    };

    let config = RunConfig {
        preset: Some(cli.preset.name().to_owned()),
        k: cli.k,
        batch_size: cli.batch_size.unwrap_or(preset.batch_size),
        selectivities: cli.selectivities.clone(),
        filter_modes: cli.filter_modes.clone(),
        filter_style: cli.filter_style,
        threads: cli.threads.max(1),
        warmup: cli.warmup.unwrap_or(preset.warmup),
        freshness_probes: cli.freshness_probes.unwrap_or(preset.freshness_probes),
        flush: !cli.no_flush,
        seed: cli.seed,
    };

    let mut target: Box<dyn BenchTarget> = match cli.target {
        TargetKind::LocalEngine => Box::new(LocalEngineTarget::open(cli.data_dir.clone())?),
    };
    let report = runner::run(target.as_mut(), &dataset, config, cli.label.clone(), prep)?;

    if let Some(parent) = cli
        .output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut json = serde_json::to_string_pretty(&report)?;
    json.push('\n');
    std::fs::write(&cli.output, json)
        .with_context(|| format!("writing {}", cli.output.display()))?;

    print!("{}", report::render_table(&report));
    println!("report written to {}", cli.output.display());
    Ok(())
}
