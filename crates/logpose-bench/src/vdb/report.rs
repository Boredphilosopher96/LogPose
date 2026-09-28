//! The report every VectorDBBench-style driver writes.
//!
//! The Milvus driver (`scripts/bench/milvus_vdb.py`) writes the same shape, and
//! `scripts/bench/vdb_summary.py` merges one report per system into the
//! committed comparison. Change both drivers and the summary together.

use super::files::Manifest;
use crate::{metrics::LatencySummary, report::MachineInfo};
use serde::Serialize;
use serde_json::Value;

/// Identifies the report shape.
pub const REPORT_SCHEMA: &str = "logpose-vdb-report/1";

/// One system's results on one prepared dataset.
#[derive(Clone, Debug, Serialize)]
pub struct VdbReport {
    /// Always [`REPORT_SCHEMA`].
    pub schema: &'static str,
    /// System name, `logpose` here.
    pub system: String,
    /// Server version as the server reports it.
    pub system_version: Option<String>,
    /// Where the driver connected.
    pub endpoint: String,
    /// Driver that produced the report.
    pub driver: String,
    /// Wall-clock start, seconds since the Unix epoch.
    pub started_at_unix: u64,
    /// Total driver run time in seconds.
    pub total_seconds: f64,
    /// Machine the driver ran on.
    pub machine: MachineInfo,
    /// The prepared dataset.
    pub dataset: Manifest,
    /// Index configuration the system was run with.
    pub index: Value,
    /// Target recall the ef sweep looks for.
    pub target_recall: f64,
    /// Load results.
    pub load: LoadReport,
    /// One entry per case, in manifest order.
    pub cases: Vec<CaseResult>,
    /// Server-side statistics after load, when the system exposes them.
    pub server_stats: Option<Value>,
    /// Caveats needed to read the numbers correctly.
    pub notes: Vec<String>,
}

/// Load phase timings.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LoadReport {
    /// Rows inserted.
    pub rows: usize,
    /// Rows per insert request.
    pub batch_size: usize,
    /// Seconds from the first insert request to the last acknowledgement.
    pub insert_seconds: f64,
    /// Seconds to make the data fully indexed and searchable after the inserts
    /// (for LogPose: flush, compact, and wait for background maintenance).
    pub optimize_seconds: f64,
    /// `insert_seconds + optimize_seconds`.
    pub total_seconds: f64,
    /// Rows per second over `insert_seconds`.
    pub insert_rows_per_sec: f64,
}

/// Results of one case.
#[derive(Clone, Debug, Serialize)]
pub struct CaseResult {
    /// Case name from the manifest.
    pub name: String,
    /// Fraction of rows the filter matches; `None` when unfiltered.
    pub selectivity: Option<f64>,
    /// The filter as text, such as `rank < 1000`.
    pub filter: Option<String>,
    /// Rows the filter matches.
    pub matching_rows: usize,
    /// One serial pass over every query per `ef`, in sweep order.
    pub sweep: Vec<SweepPoint>,
    /// The smallest swept `ef` that met the target recall, or the best one.
    pub chosen_ef: u32,
    /// Mean recall at `chosen_ef`.
    pub chosen_recall: f64,
    /// Whether `chosen_ef` met the target recall.
    pub met_target: bool,
    /// Plan the server chose at `chosen_ef`, when it reports one.
    pub plan: Option<Value>,
    /// Throughput runs at `chosen_ef`, one per client count.
    pub concurrency: Vec<ConcurrencyResult>,
}

/// One serial pass over every query at one `ef`.
#[derive(Clone, Debug, Serialize)]
pub struct SweepPoint {
    /// Requested beam width.
    pub ef: u32,
    /// Mean recall@k.
    pub recall: f64,
    /// Lowest per-query recall.
    pub min_recall: f64,
    /// Queries run.
    pub queries: usize,
    /// Queries per second of the serial pass.
    pub qps: f64,
    /// Latency of the serial pass.
    pub latency: LatencySummary,
}

/// A fixed-duration run with several concurrent clients.
#[derive(Clone, Debug, Serialize)]
pub struct ConcurrencyResult {
    /// Concurrent clients, each with its own connection.
    pub clients: usize,
    /// Measured wall time in seconds.
    pub duration_seconds: f64,
    /// Queries completed.
    pub queries: usize,
    /// Queries per second.
    pub qps: f64,
    /// Per-query latency.
    pub latency: LatencySummary,
    /// Mean recall@k of the answers.
    pub recall: f64,
}
