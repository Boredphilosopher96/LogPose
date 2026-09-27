//! Latency summaries, recall, and process resource counters.

use serde::Serialize;
use std::{collections::HashSet, time::Duration};

/// Summary of a latency distribution, in milliseconds.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencySummary {
    /// Number of samples.
    pub count: usize,
    /// Arithmetic mean.
    pub mean_ms: f64,
    /// Minimum.
    pub min_ms: f64,
    /// Median.
    pub p50_ms: f64,
    /// 95th percentile.
    pub p95_ms: f64,
    /// 99th percentile.
    pub p99_ms: f64,
    /// Maximum.
    pub max_ms: f64,
}

impl LatencySummary {
    /// Summarize samples with nearest-rank percentiles.
    #[must_use]
    pub fn from_durations(samples: &[Duration]) -> Self {
        let mut millis = samples
            .iter()
            .map(|sample| sample.as_secs_f64() * 1_000.0)
            .collect::<Vec<_>>();
        millis.sort_by(f64::total_cmp);
        if millis.is_empty() {
            return Self::default();
        }
        Self {
            count: millis.len(),
            mean_ms: millis.iter().sum::<f64>() / millis.len() as f64,
            min_ms: millis[0],
            p50_ms: percentile(&millis, 50.0),
            p95_ms: percentile(&millis, 95.0),
            p99_ms: percentile(&millis, 99.0),
            max_ms: millis[millis.len() - 1],
        }
    }
}

/// Nearest-rank percentile of an ascending slice. Returns 0 for an empty slice.
#[must_use]
pub fn percentile(sorted: &[f64], percent: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((percent / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// Recall@k: the fraction of the true top-k that appears in the returned top-k.
///
/// When fewer than `k` rows match the filter, the truth is shorter and recall
/// is measured against what exists. Empty truth counts as perfect recall.
#[must_use]
pub fn recall_at_k(truth: &[u64], returned: &[u64], k: usize) -> f64 {
    let truth = &truth[..truth.len().min(k)];
    if truth.is_empty() {
        return 1.0;
    }
    let returned = returned.iter().take(k).copied().collect::<HashSet<_>>();
    let hits = truth.iter().filter(|id| returned.contains(id)).count();
    hits as f64 / truth.len() as f64
}

/// Resident memory counters from `/proc/self/status`.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct MemorySample {
    /// Current resident set size in bytes.
    pub vm_rss_bytes: Option<u64>,
    /// Peak resident set size in bytes.
    pub vm_hwm_bytes: Option<u64>,
}

impl MemorySample {
    /// Read the current process's memory counters. Fields are `None` off Linux.
    #[must_use]
    pub fn read() -> Self {
        let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
            return Self::default();
        };
        Self::parse(&status)
    }

    fn parse(status: &str) -> Self {
        let field = |name: &str| {
            status.lines().find_map(|line| {
                let rest = line.strip_prefix(name)?.strip_prefix(':')?;
                let kib = rest.trim().strip_suffix("kB")?.trim().parse::<u64>().ok()?;
                Some(kib * 1024)
            })
        };
        Self {
            vm_rss_bytes: field("VmRSS"),
            vm_hwm_bytes: field("VmHWM"),
        }
    }
}

/// Read counters from `/proc/self/io`.
///
/// `rchar` counts bytes returned by read syscalls, including page-cache hits.
/// `read_bytes` counts bytes fetched from the block device, so a warm page
/// cache drives it toward zero.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct IoSample {
    /// Bytes read through read-like syscalls.
    pub rchar: Option<u64>,
    /// Bytes read from storage.
    pub read_bytes: Option<u64>,
}

impl IoSample {
    /// Read the current process's I/O counters. Fields are `None` when unavailable.
    #[must_use]
    pub fn read() -> Self {
        let Ok(io) = std::fs::read_to_string("/proc/self/io") else {
            return Self::default();
        };
        Self::parse(&io)
    }

    fn parse(io: &str) -> Self {
        let field = |name: &str| {
            io.lines().find_map(|line| {
                line.strip_prefix(name)?
                    .strip_prefix(':')?
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
        };
        Self {
            rchar: field("rchar"),
            read_bytes: field("read_bytes"),
        }
    }

    /// Per-operation delta since an earlier sample.
    #[must_use]
    pub fn per_op_since(&self, earlier: &Self, ops: usize) -> IoPerOp {
        let per = |now: Option<u64>, before: Option<u64>| {
            now.zip(before)
                .map(|(now, before)| now.saturating_sub(before) as f64 / ops.max(1) as f64)
        };
        IoPerOp {
            rchar: per(self.rchar, earlier.rchar),
            read_bytes: per(self.read_bytes, earlier.read_bytes),
        }
    }
}

/// Average bytes read per operation.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct IoPerOp {
    /// Average `rchar` delta, including page-cache hits.
    pub rchar: Option<f64>,
    /// Average `read_bytes` delta, device reads only.
    pub read_bytes: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::{IoSample, LatencySummary, MemorySample, percentile, recall_at_k};
    use std::time::Duration;

    #[test]
    fn recall_counts_overlap_with_truth() {
        assert!((recall_at_k(&[1, 2, 3, 4], &[4, 3, 9, 8], 4) - 0.5).abs() < 1e-9);
        assert!((recall_at_k(&[1, 2], &[2, 1], 2) - 1.0).abs() < 1e-9);
        assert!((recall_at_k(&[1, 2, 3], &[], 3)).abs() < 1e-9);
    }

    #[test]
    fn recall_truncates_to_k_and_handles_short_truth() {
        // Only the first k returned ids count.
        assert!((recall_at_k(&[1, 2], &[5, 6, 1, 2], 2)).abs() < 1e-9);
        // Truth shorter than k (a filter matched few rows).
        assert!((recall_at_k(&[7], &[7, 8, 9], 3) - 1.0).abs() < 1e-9);
        assert!((recall_at_k(&[], &[1], 3) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn percentiles_use_nearest_rank() {
        let values = (1..=100).map(f64::from).collect::<Vec<_>>();
        assert!((percentile(&values, 50.0) - 50.0).abs() < 1e-9);
        assert!((percentile(&values, 99.0) - 99.0).abs() < 1e-9);
        assert!((percentile(&[3.0], 95.0) - 3.0).abs() < 1e-9);
        let summary =
            LatencySummary::from_durations(&[Duration::from_millis(2), Duration::from_millis(4)]);
        assert!((summary.mean_ms - 3.0).abs() < 1e-9);
        assert!((summary.p50_ms - 2.0).abs() < 1e-9);
        assert!((summary.max_ms - 4.0).abs() < 1e-9);
    }

    #[test]
    fn parses_proc_counters() {
        let memory = MemorySample::parse("Name:\tx\nVmHWM:\t  2048 kB\nVmRSS:\t  1024 kB\n");
        assert_eq!(memory.vm_rss_bytes, Some(1024 * 1024));
        assert_eq!(memory.vm_hwm_bytes, Some(2048 * 1024));
        let before = IoSample::parse("rchar: 100\nwchar: 5\nread_bytes: 0\n");
        let after = IoSample::parse("rchar: 1100\nwchar: 5\nread_bytes: 4096\n");
        let per_op = after.per_op_since(&before, 10);
        assert_eq!(per_op.rchar, Some(100.0));
        assert_eq!(per_op.read_bytes, Some(409.6));
    }
}
