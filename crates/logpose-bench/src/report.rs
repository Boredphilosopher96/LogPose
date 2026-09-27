//! Report types, machine information, and the stdout table.

use crate::{
    dataset::{DatasetSource, Metric},
    filter::{FilterMode, FilterStyle},
    metrics::{IoPerOp, LatencySummary, MemorySample},
};
use logpose_query::FilterExpr;
use serde::Serialize;
use serde_json::Value;
use std::{collections::BTreeMap, fmt::Write as _, process::Command};

/// Bumped whenever a field changes meaning, so later tooling can compare reports.
pub const REPORT_SCHEMA_VERSION: u32 = 1;

/// Full benchmark report written as JSON.
#[derive(Clone, Debug, Serialize)]
pub struct Report {
    /// Report format version.
    pub schema_version: u32,
    /// Harness build information.
    pub harness: HarnessInfo,
    /// Free-form label from the command line.
    pub label: Option<String>,
    /// Wall-clock start, seconds since the Unix epoch.
    pub started_at_unix: u64,
    /// Total run time in seconds.
    pub total_seconds: f64,
    /// Machine the run executed on.
    pub machine: MachineInfo,
    /// Engine under test.
    pub target: TargetInfo,
    /// Run parameters.
    pub config: RunConfig,
    /// Dataset description and oracle cost.
    pub dataset: DatasetReport,
    /// Bulk ingest results.
    pub ingest: IngestReport,
    /// Write-to-searchable probes.
    pub freshness: FreshnessReport,
    /// One entry per search case.
    pub cases: Vec<CaseReport>,
    /// Harness-process memory at phase boundaries.
    pub memory: MemoryReport,
    /// Caveats needed to read the numbers correctly.
    pub notes: Vec<String>,
}

/// Harness build information.
#[derive(Clone, Debug, Serialize)]
pub struct HarnessInfo {
    /// Package name.
    pub name: &'static str,
    /// Package version.
    pub version: &'static str,
    /// `release` or `debug`.
    pub build_profile: &'static str,
}

impl HarnessInfo {
    /// Information for this build.
    #[must_use]
    pub fn current() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            build_profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        }
    }
}

/// Parameters that shape a run.
#[derive(Clone, Debug, Serialize)]
pub struct RunConfig {
    /// Scale preset the parameters started from, if any.
    pub preset: Option<String>,
    /// Neighbors per query.
    pub k: usize,
    /// Rows per ingest batch.
    pub batch_size: usize,
    /// Filter selectivities, as fractions of rows.
    pub selectivities: Vec<f64>,
    /// Filter modes to run.
    pub filter_modes: Vec<FilterMode>,
    /// How filters are expressed to the target.
    pub filter_style: FilterStyle,
    /// Client threads for the concurrent QPS pass; 1 disables it.
    pub threads: usize,
    /// Untimed queries run before each case.
    pub warmup: usize,
    /// Number of write-to-searchable probes.
    pub freshness_probes: usize,
    /// Whether the run flushes after ingest.
    pub flush: bool,
    /// Seed for the dataset and attributes.
    pub seed: u64,
}

/// Machine description.
#[derive(Clone, Debug, Serialize)]
pub struct MachineInfo {
    /// Operating system family.
    pub os: &'static str,
    /// CPU architecture.
    pub arch: &'static str,
    /// Kernel release.
    pub kernel: Option<String>,
    /// CPU model name.
    pub cpu_model: Option<String>,
    /// Logical CPUs available to the process.
    pub logical_cpus: usize,
    /// Physical memory in bytes.
    pub total_memory_bytes: Option<u64>,
    /// Commit of the working tree the harness ran from.
    pub git_commit: Option<String>,
    /// Whether the working tree had uncommitted changes.
    pub git_dirty: Option<bool>,
}

impl MachineInfo {
    /// Collect information about the current machine.
    #[must_use]
    pub fn collect() -> Self {
        let read = |path: &str| std::fs::read_to_string(path).ok();
        let cpu_model = read("/proc/cpuinfo").and_then(|info| {
            info.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == "model name").then(|| value.trim().to_owned())
            })
        });
        let total_memory_bytes = read("/proc/meminfo").and_then(|info| {
            info.lines().find_map(|line| {
                let kib = line
                    .strip_prefix("MemTotal:")?
                    .trim()
                    .strip_suffix("kB")?
                    .trim()
                    .parse::<u64>()
                    .ok()?;
                Some(kib * 1024)
            })
        });
        let git = |args: &[&str]| {
            Command::new("git")
                .args(args)
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        };
        Self {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            kernel: read("/proc/sys/kernel/osrelease").map(|value| value.trim().to_owned()),
            cpu_model,
            logical_cpus: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            total_memory_bytes,
            git_commit: git(&["rev-parse", "HEAD"]),
            git_dirty: git(&["status", "--porcelain", "--untracked-files=no"])
                .map(|status| !status.is_empty()),
        }
    }
}

/// Engine under test.
#[derive(Clone, Debug, Serialize)]
pub struct TargetInfo {
    /// Target identifier.
    pub name: &'static str,
    /// Target statistics after ingest and flush.
    pub stats_after_ingest: Option<Value>,
    /// Target statistics at the end of the run.
    pub stats_final: Option<Value>,
}

/// Dataset description.
#[derive(Clone, Debug, Serialize)]
pub struct DatasetReport {
    /// Provenance and generator parameters.
    pub source: DatasetSource,
    /// Base rows.
    pub n: usize,
    /// Queries.
    pub queries: usize,
    /// Dimensionality.
    pub dims: usize,
    /// Metric.
    pub metric: Metric,
    /// Time to generate or load the vectors.
    pub load_seconds: f64,
    /// Time to build filter attributes.
    pub attribute_seconds: f64,
    /// Time to compute exact ground truth for every case.
    pub oracle_seconds: f64,
    /// Recall of the harness oracle against a reference `.ivecs` file, when given.
    pub oracle_vs_reference_recall: Option<f64>,
}

/// Bulk ingest results.
#[derive(Clone, Debug, Serialize)]
pub struct IngestReport {
    /// Rows written.
    pub rows: usize,
    /// Rows per batch.
    pub batch_size: usize,
    /// Batches written.
    pub batches: usize,
    /// Wall time of all batches.
    pub seconds: f64,
    /// Rows per second across all batches.
    pub rows_per_sec: f64,
    /// Per-batch acknowledgement latency.
    pub batch_latency: LatencySummary,
    /// Wall time of the flush step, including waiting for background maintenance.
    pub flush_seconds: Option<f64>,
}

/// Write-to-searchable probe results.
#[derive(Clone, Debug, Default, Serialize)]
pub struct FreshnessReport {
    /// Probes attempted.
    pub probes: usize,
    /// Probes whose row became searchable before the timeout.
    pub visible: usize,
    /// Latency of the single-row write acknowledgement.
    pub write_ack: LatencySummary,
    /// Time from acknowledgement until a search first returned the row.
    pub ack_to_visible: LatencySummary,
    /// Time from issuing the write until a search first returned the row.
    pub write_to_visible: LatencySummary,
    /// Mean number of searches issued until the row appeared.
    pub mean_searches_until_visible: f64,
}

/// Filter description for a case.
#[derive(Clone, Debug, Serialize)]
pub struct CaseFilter {
    /// Filter mode.
    pub mode: FilterMode,
    /// How the filter is expressed.
    pub style: FilterStyle,
    /// Metadata field the predicate reads.
    pub field: String,
    /// Requested selectivity.
    pub target_selectivity: f64,
    /// Fraction of rows that match.
    pub actual_selectivity: f64,
    /// Rows that match.
    pub matching_rows: usize,
    /// Filter sent to the target.
    pub predicate: FilterExpr,
}

/// Recall distribution across queries.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RecallSummary {
    /// Mean recall@k.
    pub mean: f64,
    /// Worst query.
    pub min: f64,
    /// 5th percentile (the bad tail).
    pub p5: f64,
    /// Queries that returned fewer ids than the oracle found.
    pub short_results: usize,
}

/// Concurrent QPS pass.
#[derive(Clone, Debug, Serialize)]
pub struct ConcurrentReport {
    /// Client threads.
    pub threads: usize,
    /// Queries issued.
    pub queries: usize,
    /// Throughput.
    pub qps: f64,
    /// Per-query latency seen by the clients.
    pub latency: LatencySummary,
}

/// One search case.
#[derive(Clone, Debug, Serialize)]
pub struct CaseReport {
    /// Case name, for example `unfiltered` or `anti-1%`.
    pub name: String,
    /// Filter, or `None` for unfiltered search.
    pub filter: Option<CaseFilter>,
    /// Neighbors per query.
    pub k: usize,
    /// Timed queries.
    pub queries: usize,
    /// Single-client throughput.
    pub qps: f64,
    /// Single-client latency.
    pub latency: LatencySummary,
    /// Concurrent pass, when enabled.
    pub concurrent: Option<ConcurrentReport>,
    /// Recall@k against the exact oracle.
    pub recall: RecallSummary,
    /// Bytes read per single-client query.
    pub io_per_query: IoPerOp,
    /// Plans the target reported, with counts.
    pub plans: BTreeMap<String, usize>,
    /// Harness-process memory after the case.
    pub memory_after: MemorySample,
}

/// Harness-process memory at phase boundaries.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MemoryReport {
    /// After the dataset, attributes, and oracle exist, before the target opens.
    pub before_target: MemorySample,
    /// After ingest.
    pub after_ingest: MemorySample,
    /// After flush.
    pub after_flush: MemorySample,
    /// At the end of the run; `vm_hwm_bytes` is the peak for the whole run.
    pub end: MemorySample,
}

fn mib(bytes: Option<u64>) -> String {
    bytes.map_or_else(
        || "n/a".to_owned(),
        |bytes| format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0)),
    )
}

fn human_bytes(bytes: Option<f64>) -> String {
    match bytes {
        None => "n/a".to_owned(),
        Some(bytes) if bytes >= 1024.0 * 1024.0 => format!("{:.1}M", bytes / (1024.0 * 1024.0)),
        Some(bytes) if bytes >= 1024.0 => format!("{:.1}K", bytes / 1024.0),
        Some(bytes) => format!("{bytes:.0}"),
    }
}

/// Render the human-readable summary printed to stdout.
#[must_use]
pub fn render_table(report: &Report) -> String {
    let mut out = String::new();
    let dataset = &report.dataset;
    let _ = writeln!(
        out,
        "target {}  |  n={} dims={} metric={:?} queries={} k={}  |  build {}",
        report.target.name,
        dataset.n,
        dataset.dims,
        dataset.metric,
        dataset.queries,
        report.config.k,
        report.harness.build_profile
    );
    let ingest = &report.ingest;
    let _ = writeln!(
        out,
        "ingest {:.0} rows/s ({} rows in {:.2}s, batch p50 {:.1} ms, p99 {:.1} ms){}",
        ingest.rows_per_sec,
        ingest.rows,
        ingest.seconds,
        ingest.batch_latency.p50_ms,
        ingest.batch_latency.p99_ms,
        ingest
            .flush_seconds
            .map(|seconds| format!(", flush {seconds:.2}s"))
            .unwrap_or_default()
    );
    let freshness = &report.freshness;
    let _ = writeln!(
        out,
        "write-to-searchable p50 {:.1} ms, p99 {:.1} ms ({}/{} probes visible)",
        freshness.write_to_visible.p50_ms,
        freshness.write_to_visible.p99_ms,
        freshness.visible,
        freshness.probes
    );
    let _ = writeln!(
        out,
        "memory rss {} peak {}",
        mib(report.memory.end.vm_rss_bytes),
        mib(report.memory.end.vm_hwm_bytes)
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "{:<14} {:>7} {:>8} {:>9} {:>9} {:>9} {:>8} {:>8} {:>9} {:>9}  plan",
        "case",
        "match%",
        "qps",
        "p50 ms",
        "p95 ms",
        "p99 ms",
        "recall",
        "min rec",
        "rchar/q",
        "disk/q"
    );
    for case in &report.cases {
        let matching = case
            .filter
            .as_ref()
            .map_or(100.0, |filter| filter.actual_selectivity * 100.0);
        let plans = case
            .plans
            .iter()
            .map(|(plan, count)| format!("{plan}:{count}"))
            .collect::<Vec<_>>()
            .join(",");
        let qps = match &case.concurrent {
            Some(concurrent) => format!("{:.1}/{:.1}", case.qps, concurrent.qps),
            None => format!("{:.1}", case.qps),
        };
        let _ = writeln!(
            out,
            "{:<14} {:>7.2} {:>8} {:>9.2} {:>9.2} {:>9.2} {:>8.3} {:>8.3} {:>9} {:>9}  {}",
            case.name,
            matching,
            qps,
            case.latency.p50_ms,
            case.latency.p95_ms,
            case.latency.p99_ms,
            case.recall.mean,
            case.recall.min,
            human_bytes(case.io_per_query.rchar),
            human_bytes(case.io_per_query.read_bytes),
            plans
        );
    }
    if report.config.threads > 1 {
        let _ = writeln!(
            out,
            "qps column shows single-client/{}-thread throughput",
            report.config.threads
        );
    }
    out
}
