//! Drives a [`BenchTarget`] through ingest, flush, search cases, and freshness probes.

use crate::{
    dataset::{Dataset, Metric},
    filter::{Attributes, FilterSpec, FilterStyle, format_percent},
    metrics::{IoSample, LatencySummary, MemorySample, percentile, recall_at_k},
    oracle::ground_truth,
    report::{
        CaseFilter, CaseReport, ConcurrentReport, DatasetReport, FreshnessReport, HarnessInfo,
        IngestReport, MachineInfo, MemoryReport, RecallSummary, Report, RunConfig, TargetInfo,
    },
    rng::SplitMix64,
    target::{BenchTarget, CollectionSpec, Row, SearchRequest},
};
use anyhow::{Context, Result, anyhow, ensure};
use std::{
    collections::BTreeMap,
    sync::Mutex,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Collection name used for every run.
pub const COLLECTION_NAME: &str = "bench";

/// Longest a freshness probe waits for its row to become searchable.
const FRESHNESS_TIMEOUT: Duration = Duration::from_secs(30);

/// Random stream for freshness probe vectors.
const STREAM_PROBES: u64 = 201;

/// Scale factor for probe rows under the dot metric, so the probe is its own top hit.
const DOT_PROBE_SCALE: f32 = 1_000.0;

/// One search case: a name and an optional filter.
#[derive(Clone, Debug)]
pub struct Case {
    /// Case name.
    pub name: String,
    /// Filter, or `None` for unfiltered search.
    pub filter: Option<FilterSpec>,
}

/// Unfiltered search plus every mode and selectivity combination.
#[must_use]
pub fn build_cases(config: &RunConfig, n: usize) -> Vec<Case> {
    let mut cases = vec![Case {
        name: "unfiltered".to_owned(),
        filter: None,
    }];
    for mode in &config.filter_modes {
        for selectivity in &config.selectivities {
            cases.push(Case {
                name: format!("{}-{}%", mode.label(), format_percent(*selectivity)),
                filter: Some(FilterSpec::new(*mode, config.filter_style, *selectivity, n)),
            });
        }
    }
    cases
}

fn log(message: impl AsRef<str>) {
    eprintln!("[logpose-bench] {}", message.as_ref());
}

/// Timing of dataset preparation, measured by the caller.
#[derive(Clone, Copy, Debug, Default)]
pub struct PrepTimings {
    /// Seconds to generate or load vectors.
    pub load_seconds: f64,
}

/// Run the whole benchmark and return the report.
pub fn run(
    target: &mut dyn BenchTarget,
    dataset: &Dataset,
    config: RunConfig,
    label: Option<String>,
    prep: PrepTimings,
) -> Result<Report> {
    ensure!(config.k > 0, "k must be positive");
    ensure!(config.batch_size > 0, "batch size must be positive");
    ensure!(!dataset.is_empty(), "dataset has no rows");
    ensure!(dataset.query_count() > 0, "dataset has no queries");
    for selectivity in &config.selectivities {
        ensure!(
            *selectivity > 0.0 && *selectivity <= 1.0,
            "selectivity {selectivity} must be in (0, 1]"
        );
    }

    let started = Instant::now();
    let started_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let n = dataset.len();

    log("building filter attributes");
    let attribute_started = Instant::now();
    let attributes = Attributes::build(dataset, config.seed);
    let attribute_seconds = attribute_started.elapsed().as_secs_f64();

    let cases = build_cases(&config, n);
    let stored = stored_fields(&cases)?;
    log(format!(
        "computing exact ground truth for {} cases",
        cases.len()
    ));
    let oracle_started = Instant::now();
    let truths = cases
        .iter()
        .map(|case| match &case.filter {
            None => ground_truth(dataset, config.k, |_| true),
            Some(filter) => {
                let column = attributes.column(filter.mode);
                ground_truth(dataset, config.k, |row| filter.matches(column[row]))
            }
        })
        .collect::<Vec<_>>();
    let oracle_seconds = oracle_started.elapsed().as_secs_f64();
    let oracle_vs_reference_recall = dataset.reference_ground_truth.as_ref().map(|reference| {
        let recalls = truths[0]
            .iter()
            .zip(reference)
            .map(|(ours, theirs)| {
                recall_at_k(&theirs[..theirs.len().min(config.k)], ours, config.k)
            })
            .collect::<Vec<_>>();
        recalls.iter().sum::<f64>() / recalls.len().max(1) as f64
    });

    let mut memory = MemoryReport {
        before_target: MemorySample::read(),
        ..MemoryReport::default()
    };

    target.create(&CollectionSpec {
        name: COLLECTION_NAME.to_owned(),
        dims: dataset.dims,
        metric: dataset.metric,
        scalar_fields: stored.iter().map(|filter| filter.field.clone()).collect(),
    })?;

    let ingest = ingest(target, dataset, &attributes, &stored, &config)?;
    memory.after_ingest = MemorySample::read();
    let flush_seconds = if config.flush {
        log("flushing");
        let flush_started = Instant::now();
        let flushed = target.flush_if_supported()?;
        flushed.then(|| flush_started.elapsed().as_secs_f64())
    } else {
        None
    };
    memory.after_flush = MemorySample::read();
    let ingest = IngestReport {
        flush_seconds,
        ..ingest
    };
    let stats_after_ingest = target.stats()?;

    let target: &dyn BenchTarget = target;
    let mut case_reports = Vec::with_capacity(cases.len());
    for (case, truth) in cases.iter().zip(&truths) {
        let report = run_case(target, dataset, &attributes, &config, case, truth)?;
        log(format!(
            "{}: {:.1} qps, p50 {:.1} ms, recall {:.3}",
            report.name, report.qps, report.latency.p50_ms, report.recall.mean
        ));
        case_reports.push(report);
    }

    let freshness = freshness(target, dataset, &config)?;
    let stats_final = target.stats()?;
    memory.end = MemorySample::read();

    let mut notes = vec![
        "memory is the harness process; for an in-process target it includes the dataset and oracle (see memory.before_target)".to_owned(),
        "io_per_query.rchar counts bytes returned by read syscalls, including page-cache hits; read_bytes counts only device reads and is near zero when the working set is cached".to_owned(),
        "recall is measured against a brute-force oracle computed in the harness; ties at the k-th distance may count as misses".to_owned(),
    ];
    if cfg!(debug_assertions) {
        notes.push("debug build: numbers are not representative, rerun with --release".to_owned());
    }

    Ok(Report {
        schema_version: crate::report::REPORT_SCHEMA_VERSION,
        harness: HarnessInfo::current(),
        label,
        started_at_unix,
        total_seconds: started.elapsed().as_secs_f64() + prep.load_seconds,
        machine: MachineInfo::collect(),
        target: TargetInfo {
            name: target.name(),
            stats_after_ingest,
            stats_final,
        },
        dataset: DatasetReport {
            source: dataset.source.clone(),
            n,
            queries: dataset.query_count(),
            dims: dataset.dims,
            metric: dataset.metric,
            load_seconds: prep.load_seconds,
            attribute_seconds,
            oracle_seconds,
            oracle_vs_reference_recall,
        },
        config,
        ingest,
        freshness,
        cases: case_reports,
        memory,
        notes,
    })
}

/// One filter per distinct stored field; range filters of a mode share a field.
///
/// Equality fields are named after the rounded selectivity, so two
/// selectivities that round to the same name but match different row counts
/// would silently share one set of stored flags. That is rejected.
fn stored_fields(cases: &[Case]) -> Result<Vec<&FilterSpec>> {
    let mut stored = Vec::<&FilterSpec>::new();
    for filter in cases.iter().filter_map(|case| case.filter.as_ref()) {
        match stored
            .iter()
            .find(|existing| existing.field == filter.field)
        {
            None => stored.push(filter),
            Some(existing) => ensure!(
                filter.style == FilterStyle::Range || existing.threshold == filter.threshold,
                "selectivities {} and {} both map to field {}; use values that differ in the first four decimal places of a percent",
                existing.target_selectivity,
                filter.target_selectivity,
                filter.field
            ),
        }
    }
    Ok(stored)
}

fn scalars<'a>(
    stored: &[&'a FilterSpec],
    attributes: &Attributes,
    row: usize,
) -> Vec<(&'a str, i64)> {
    stored
        .iter()
        .map(|filter| {
            (
                filter.field.as_str(),
                filter.stored_value(attributes.column(filter.mode)[row]),
            )
        })
        .collect()
}

fn ingest(
    target: &dyn BenchTarget,
    dataset: &Dataset,
    attributes: &Attributes,
    stored: &[&FilterSpec],
    config: &RunConfig,
) -> Result<IngestReport> {
    let n = dataset.len();
    let mut batch_latencies = Vec::with_capacity(n.div_ceil(config.batch_size));
    let mut total = Duration::ZERO;
    let mut last_logged = Instant::now();
    for start in (0..n).step_by(config.batch_size) {
        let end = (start + config.batch_size).min(n);
        let rows = (start..end)
            .map(|row| Row {
                id: row as u64,
                vector: dataset.row(row),
                scalars: scalars(stored, attributes, row),
            })
            .collect::<Vec<_>>();
        let batch_started = Instant::now();
        target
            .ingest(&rows)
            .with_context(|| format!("ingesting rows {start}..{end}"))?;
        let elapsed = batch_started.elapsed();
        total += elapsed;
        batch_latencies.push(elapsed);
        if last_logged.elapsed() > Duration::from_secs(10) || end == n {
            log(format!("ingested {end}/{n} rows"));
            last_logged = Instant::now();
        }
    }
    let seconds = total.as_secs_f64();
    Ok(IngestReport {
        rows: n,
        batch_size: config.batch_size,
        batches: batch_latencies.len(),
        seconds,
        rows_per_sec: n as f64 / seconds.max(f64::MIN_POSITIVE),
        batch_latency: LatencySummary::from_durations(&batch_latencies),
        flush_seconds: None,
    })
}

fn run_case(
    target: &dyn BenchTarget,
    dataset: &Dataset,
    attributes: &Attributes,
    config: &RunConfig,
    case: &Case,
    truth: &[Vec<u64>],
) -> Result<CaseReport> {
    let predicate = case.filter.as_ref().map(FilterSpec::predicate);
    let queries = dataset.query_count();
    let request = |query: usize| SearchRequest {
        vector: dataset.query(query),
        filter: predicate.as_ref(),
        k: config.k,
    };

    for warm in 0..config.warmup {
        target.search(&request(warm % queries))?;
    }

    let mut latencies = Vec::with_capacity(queries);
    let mut recalls = Vec::with_capacity(queries);
    let mut short_results = 0;
    let mut plans = BTreeMap::<String, usize>::new();
    let io_before = IoSample::read();
    let loop_started = Instant::now();
    for (query, expected) in truth.iter().enumerate() {
        let search_started = Instant::now();
        let response = target
            .search(&request(query))
            .with_context(|| format!("case {} query {query}", case.name))?;
        latencies.push(search_started.elapsed());
        recalls.push(recall_at_k(expected, &response.ids, config.k));
        if response.ids.len() < expected.len() {
            short_results += 1;
        }
        *plans
            .entry(response.plan.unwrap_or_else(|| "unknown".to_owned()))
            .or_default() += 1;
    }
    let wall = loop_started.elapsed();
    let io_per_query = IoSample::read().per_op_since(&io_before, queries);

    let concurrent = if config.threads > 1 {
        Some(run_concurrent(target, config.threads, queries, &request)?)
    } else {
        None
    };

    let mut sorted_recalls = recalls.clone();
    sorted_recalls.sort_by(f64::total_cmp);
    let filter = case.filter.as_ref().map(|filter| {
        let matching_rows = attributes
            .column(filter.mode)
            .iter()
            .filter(|value| filter.matches(**value))
            .count();
        CaseFilter {
            mode: filter.mode,
            style: filter.style,
            field: filter.field.clone(),
            target_selectivity: filter.target_selectivity,
            actual_selectivity: matching_rows as f64 / dataset.len() as f64,
            matching_rows,
            predicate: filter.predicate(),
        }
    });

    Ok(CaseReport {
        name: case.name.clone(),
        filter,
        k: config.k,
        queries,
        qps: queries as f64 / wall.as_secs_f64().max(f64::MIN_POSITIVE),
        latency: LatencySummary::from_durations(&latencies),
        concurrent,
        recall: RecallSummary {
            mean: recalls.iter().sum::<f64>() / recalls.len().max(1) as f64,
            min: sorted_recalls.first().copied().unwrap_or(1.0),
            p5: percentile(&sorted_recalls, 5.0),
            short_results,
        },
        io_per_query,
        plans,
        memory_after: MemorySample::read(),
    })
}

fn run_concurrent<'a, F>(
    target: &dyn BenchTarget,
    threads: usize,
    queries: usize,
    request: &F,
) -> Result<ConcurrentReport>
where
    F: Fn(usize) -> SearchRequest<'a> + Sync,
{
    let latencies = Mutex::new(Vec::with_capacity(queries));
    let started = Instant::now();
    let results = thread::scope(|scope| {
        let handles = (0..threads)
            .map(|worker| {
                let latencies = &latencies;
                scope.spawn(move || -> Result<()> {
                    let mut local = Vec::new();
                    for query in (worker..queries).step_by(threads) {
                        let search_started = Instant::now();
                        target.search(&request(query))?;
                        local.push(search_started.elapsed());
                    }
                    latencies
                        .lock()
                        .map_err(|_| anyhow!("latency lock poisoned"))?
                        .extend(local);
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow!("concurrent search thread panicked"))?
            })
            .collect::<Result<Vec<_>>>()
    });
    let wall = started.elapsed();
    results?;
    let latencies = latencies
        .into_inner()
        .map_err(|_| anyhow!("latency lock poisoned"))?;
    Ok(ConcurrentReport {
        threads,
        queries,
        qps: queries as f64 / wall.as_secs_f64().max(f64::MIN_POSITIVE),
        latency: LatencySummary::from_durations(&latencies),
    })
}

/// Write single rows, then search until each one is returned.
fn freshness(
    target: &dyn BenchTarget,
    dataset: &Dataset,
    config: &RunConfig,
) -> Result<FreshnessReport> {
    if config.freshness_probes == 0 {
        return Ok(FreshnessReport::default());
    }
    log(format!(
        "running {} freshness probes",
        config.freshness_probes
    ));
    let mut rng = SplitMix64::stream(config.seed, STREAM_PROBES);
    let mut write_ack = Vec::new();
    let mut ack_to_visible = Vec::new();
    let mut write_to_visible = Vec::new();
    let mut searches = Vec::new();
    for probe in 0..config.freshness_probes {
        let query = (0..dataset.dims)
            .map(|_| rng.gaussian() as f32)
            .collect::<Vec<_>>();
        let stored = match dataset.metric {
            Metric::Dot => query.iter().map(|value| value * DOT_PROBE_SCALE).collect(),
            Metric::L2 | Metric::Cosine => query.clone(),
        };
        let id = (dataset.len() + probe) as u64;
        let row = Row {
            id,
            vector: &stored,
            // Probes carry no scalar fields, so they never match a filter case.
            scalars: Vec::new(),
        };
        let write_started = Instant::now();
        target.ingest(std::slice::from_ref(&row))?;
        let acked = Instant::now();
        write_ack.push(acked - write_started);
        let mut attempts = 0_usize;
        loop {
            attempts += 1;
            let response = target.search(&SearchRequest {
                vector: &query,
                filter: None,
                k: config.k,
            })?;
            if response.ids.contains(&id) {
                let now = Instant::now();
                ack_to_visible.push(now - acked);
                write_to_visible.push(now - write_started);
                searches.push(attempts as f64);
                break;
            }
            if acked.elapsed() > FRESHNESS_TIMEOUT {
                log(format!(
                    "probe {probe} not visible after {FRESHNESS_TIMEOUT:?}"
                ));
                break;
            }
        }
    }
    Ok(FreshnessReport {
        probes: config.freshness_probes,
        visible: ack_to_visible.len(),
        write_ack: LatencySummary::from_durations(&write_ack),
        ack_to_visible: LatencySummary::from_durations(&ack_to_visible),
        write_to_visible: LatencySummary::from_durations(&write_to_visible),
        mean_searches_until_visible: searches.iter().sum::<f64>() / searches.len().max(1) as f64,
    })
}

#[cfg(test)]
mod tests {
    use super::{PrepTimings, build_cases, run, stored_fields};
    use crate::{
        dataset::{Metric, SyntheticSpec, generate_synthetic},
        filter::{FilterMode, FilterStyle},
        report::{RunConfig, render_table},
        target::LocalEngineTarget,
    };

    fn config() -> RunConfig {
        RunConfig {
            preset: None,
            k: 5,
            batch_size: 64,
            selectivities: vec![0.05, 0.5],
            filter_modes: vec![FilterMode::Uncorrelated, FilterMode::AntiCorrelated],
            filter_style: FilterStyle::Equality,
            threads: 2,
            warmup: 1,
            freshness_probes: 2,
            flush: true,
            seed: 1,
        }
    }

    #[test]
    fn rejects_selectivities_that_share_an_equality_field() {
        let config = RunConfig {
            selectivities: vec![0.000_001_1, 0.000_001_4],
            filter_modes: vec![FilterMode::Uncorrelated],
            ..config()
        };
        let cases = build_cases(&config, 10_000_000);
        assert!(stored_fields(&cases).is_err());

        let range = RunConfig {
            filter_style: FilterStyle::Range,
            ..config.clone()
        };
        let cases = build_cases(&range, 10_000_000);
        assert_eq!(
            stored_fields(&cases).map(|fields| fields.len()).ok(),
            Some(1)
        );
    }

    #[test]
    fn case_names_are_readable() {
        let names = build_cases(&config(), 100)
            .into_iter()
            .map(|case| case.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "unfiltered",
                "uncorr-5%",
                "uncorr-50%",
                "anti-5%",
                "anti-50%"
            ]
        );
    }

    fn tiny_dataset() -> anyhow::Result<crate::dataset::Dataset> {
        generate_synthetic(
            &SyntheticSpec {
                n: 160,
                queries: 4,
                dims: 4,
                clusters: 4,
                query_cluster_fraction: 0.5,
                spread: 0.3,
                seed: 1,
            },
            Metric::L2,
        )
    }

    /// Without a flush the engine answers every case with an exact plan, so
    /// the harness oracle must agree with it exactly, for both filter styles.
    #[test]
    fn oracle_agrees_with_engine_exact_path() -> anyhow::Result<()> {
        let dataset = tiny_dataset()?;
        for filter_style in [FilterStyle::Equality, FilterStyle::Range] {
            let mut target = LocalEngineTarget::open(None)?;
            let config = RunConfig {
                flush: false,
                threads: 1,
                freshness_probes: 0,
                filter_style,
                ..config()
            };
            let report = run(&mut target, &dataset, config, None, PrepTimings::default())?;
            for case in &report.cases {
                assert!(
                    (case.recall.mean - 1.0).abs() < 1e-9,
                    "{filter_style:?} {} recall {} plans {:?}",
                    case.name,
                    case.recall.mean,
                    case.plans
                );
            }
        }
        Ok(())
    }

    #[test]
    fn end_to_end_run_with_flush_produces_full_report() -> anyhow::Result<()> {
        let dataset = tiny_dataset()?;
        let mut target = LocalEngineTarget::open(None)?;
        let report = run(
            &mut target,
            &dataset,
            config(),
            Some("test".to_owned()),
            PrepTimings::default(),
        )?;
        assert_eq!(report.cases.len(), 5);
        assert_eq!(report.ingest.rows, 160);
        assert!(report.ingest.flush_seconds.is_some());
        assert_eq!(report.freshness.visible, 2);
        for case in &report.cases {
            assert_eq!(case.latency.count, 4);
            assert!((0.0..=1.0).contains(&case.recall.mean));
            assert_eq!(case.plans.values().sum::<usize>(), 4);
            assert!(case.concurrent.is_some());
        }
        let anti = report
            .cases
            .iter()
            .find(|case| case.name == "anti-5%")
            .and_then(|case| case.filter.as_ref())
            .map(|filter| filter.matching_rows);
        assert_eq!(anti, Some(8));
        let json = serde_json::to_value(&report)?;
        assert_eq!(json["schema_version"], 1);
        assert!(render_table(&report).contains("anti-5%"));
        Ok(())
    }
}
