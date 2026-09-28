//! Recall of the staged search on clustered data: 20,000 rows of 128 dimensions in one
//! segment with a graph and SQ8 codes, top 10, unfiltered and filtered at 1, 10, and 50
//! percent selectivity, with filters independent of the query and anti-correlated with it.
//! Each filtered case runs with the cost model's choice and with each walk forced (ACORN-1 and
//! admit-only), so the walks carry the recall themselves, including the `ef` escalation for
//! anti-correlated filters.
//!
//! `qps_100k_top10_ef64` (ignored; run it in release) reports throughput on 100,000 rows, and
//! `calibrate_cost_model` (ignored; release) measures the cost model's constants on this host
//! and prints them.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index::{
    kernels,
    sq8::{Sq8Metric, Sq8Params},
};
use logpose_query::{
    FilterExpr, Force, Operator, SearchRequest, SearchTuning, UnitStrategy, search,
};
use logpose_storage::{IndexPolicy, ReadView};
use logpose_types::{DistanceMetric, schema::FieldType, value::Value};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json as _;
use std::time::Instant;
use thiserror as _;

mod support;

use support::{Fixture, Rng, centers, near, record};

const DIMS: usize = 128;
const CLUSTERS: usize = 100;
const K: usize = 10;
const MIN_RECALL: f64 = 0.95;

/// One stored row: its vector, cluster, and uniform bucket in `0..100`.
struct Row {
    vector: Vec<f32>,
    cluster: i64,
    bucket: i64,
}

/// A filter with its oracle.
struct Case {
    name: &'static str,
    filter: Option<FilterExpr>,
    keep: fn(&Row) -> bool,
    /// Whether the matching rows lie away from the queries.
    anti: bool,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "unfiltered",
            filter: None,
            keep: |_| true,
            anti: false,
        },
        Case {
            name: "uniform 1%",
            filter: Some(FilterExpr::eq("bucket", 7_i64)),
            keep: |row| row.bucket == 7,
            anti: false,
        },
        Case {
            name: "uniform 10%",
            filter: Some(FilterExpr::lt("bucket", 10_i64)),
            keep: |row| row.bucket < 10,
            anti: false,
        },
        Case {
            name: "uniform 50%",
            filter: Some(FilterExpr::lt("bucket", 50_i64)),
            keep: |row| row.bucket < 50,
            anti: false,
        },
        Case {
            name: "anti-correlated 1%",
            filter: Some(FilterExpr::eq("cluster", 99_i64)),
            keep: |row| row.cluster == 99,
            anti: true,
        },
        Case {
            name: "anti-correlated 10%",
            filter: Some(FilterExpr::gte("cluster", 90_i64)),
            keep: |row| row.cluster >= 90,
            anti: true,
        },
        Case {
            name: "anti-correlated 50%",
            filter: Some(FilterExpr::gte("cluster", 50_i64)),
            keep: |row| row.cluster >= 50,
            anti: true,
        },
    ]
}

fn l2(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| (a - b) * (a - b)).sum()
}

/// Ids of the exact top `k` rows passing `keep`.
fn truth(rows: &[Row], query: &[f32], keep: fn(&Row) -> bool, k: usize) -> Vec<usize> {
    let mut scored = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| keep(row))
        .map(|(index, row)| (l2(query, &row.vector), index))
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    scored.truncate(k);
    scored.into_iter().map(|(_, index)| index).collect()
}

async fn build(rows: usize, seed: u64, policy: IndexPolicy) -> (Fixture, Vec<Row>, Vec<Vec<f32>>) {
    let fixture = Fixture::new(
        "recall",
        DIMS,
        DistanceMetric::L2,
        policy,
        &[("cluster", FieldType::Int64), ("bucket", FieldType::Int64)],
    )
    .await;
    let mut rng = Rng::new(seed);
    let centers = centers(&mut rng, CLUSTERS, DIMS);
    let mut stored = Vec::with_capacity(rows);
    for index in 0..rows {
        let cluster = index % CLUSTERS;
        stored.push(Row {
            vector: near(&mut rng, &centers[cluster], 0.35),
            cluster: cluster as i64,
            bucket: rng.below(100) as i64,
        });
    }
    for (chunk, batch) in stored.chunks(2_000).enumerate() {
        let records = batch
            .iter()
            .enumerate()
            .map(|(offset, row)| {
                record(
                    &format!("r{:06}", chunk * 2_000 + offset),
                    row.vector.clone(),
                    &[
                        ("cluster", Value::Int64(row.cluster)),
                        ("bucket", Value::Int64(row.bucket)),
                    ],
                )
            })
            .collect();
        fixture.upsert(records).await;
    }
    fixture.flush().await;
    fixture.compact().await;
    (fixture, stored, centers)
}

/// Queries near clusters 0 to 9, which the anti-correlated filters exclude.
fn queries(centers: &[Vec<f32>], count: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng::new(seed);
    (0..count)
        .map(|index| near(&mut rng, &centers[index % 10], 0.35))
        .collect()
}

struct Measured {
    recall: f64,
    strategies: Vec<UnitStrategy>,
    escalations: u32,
}

async fn measure(
    view: &ReadView,
    rows: &[Row],
    queries: &[Vec<f32>],
    case: &Case,
    tuning: SearchTuning,
    ef: Option<usize>,
) -> Measured {
    let mut found = 0;
    let mut wanted = 0;
    let mut strategies = Vec::new();
    let mut escalations = 0;
    for query in queries {
        let request = SearchRequest {
            filter: case.filter.clone(),
            tuning,
            ef,
            ..SearchRequest::new(query.clone(), K)
        };
        let outcome = search(view, &request).await.expect("search should succeed");
        let got = outcome
            .hits
            .iter()
            .map(|hit| match &hit.row.record.pk {
                logpose_types::record::PrimaryKey::String(id) => {
                    id[1..].parse::<usize>().expect("id")
                }
                logpose_types::record::PrimaryKey::Int64(_) => usize::MAX,
            })
            .collect::<Vec<_>>();
        let expected = truth(rows, query, case.keep, K);
        wanted += expected.len();
        found += expected.iter().filter(|index| got.contains(index)).count();
        for unit in &outcome.units {
            if unit.live > 0 {
                strategies.push(unit.strategy);
                escalations += unit.escalations;
            }
        }
    }
    Measured {
        recall: if wanted == 0 {
            1.0
        } else {
            found as f64 / wanted as f64
        },
        strategies,
        escalations,
    }
}

#[tokio::test]
async fn staged_search_keeps_recall_on_clustered_data_at_every_selectivity() {
    let policy = IndexPolicy {
        graph_min_rows: 5_000,
        ..IndexPolicy::default()
    };
    let (fixture, rows, centers) = build(20_000, 17, policy).await;
    let view = fixture.view().await;
    assert_eq!(
        view.units()
            .iter()
            .filter(|unit| !unit.is_memtable())
            .count(),
        1,
        "the compaction leaves one segment"
    );
    let queries = queries(&centers, 30, 91);
    let mut report = Vec::new();
    for case in cases() {
        let default = measure(&view, &rows, &queries, &case, SearchTuning::default(), None).await;
        report.push(format!("{:<22} default {:.3}", case.name, default.recall));
        assert!(
            default.recall >= MIN_RECALL,
            "{}: recall {} below {MIN_RECALL}",
            case.name,
            default.recall
        );
        if case.filter.is_none() {
            assert!(
                default
                    .strategies
                    .iter()
                    .all(|strategy| *strategy == UnitStrategy::GraphAdmit)
            );
            continue;
        }
        for (force, strategy) in [
            (Force::Acorn, UnitStrategy::GraphAcorn),
            (Force::Admit, UnitStrategy::GraphAdmit),
        ] {
            let forced = SearchTuning {
                force,
                ..SearchTuning::default()
            };
            let walked = measure(&view, &rows, &queries, &case, forced, None).await;
            let fixed = measure(
                &view,
                &rows,
                &queries,
                &case,
                SearchTuning {
                    ef_escalation: false,
                    ..forced
                },
                None,
            )
            .await;
            report.push(format!(
                "{:<22} {:<7} {:.3} escalations {:>3}; without escalation {:.3}",
                case.name,
                strategy.name(),
                walked.recall,
                walked.escalations,
                fixed.recall
            ));
            assert!(
                walked.recall >= fixed.recall,
                "{} {force:?}: escalation never loses recall",
                case.name
            );
            assert!(
                walked.recall >= MIN_RECALL,
                "{} {force:?}: recall {} below {MIN_RECALL}",
                case.name,
                walked.recall
            );
            assert!(
                walked.strategies.iter().all(|found| *found == strategy),
                "{}: forced walks are {strategy:?}, got {:?}",
                case.name,
                walked.strategies
            );
        }
    }
    // Escalation, isolated: a narrow beam (ef 10, one candidate per result) walking the
    // anti-correlated filters. Without escalation the first beam settles on matching rows
    // far from the query; the cursor widens it and recovers the recall.
    let narrow = SearchTuning {
        force: Force::Walk,
        rerank_factor: 1,
        ..SearchTuning::default()
    };
    for case in cases().iter().filter(|case| case.anti) {
        let widened = measure(&view, &rows, &queries, case, narrow, Some(10)).await;
        let fixed = measure(
            &view,
            &rows,
            &queries,
            case,
            SearchTuning {
                ef_escalation: false,
                ..narrow
            },
            Some(10),
        )
        .await;
        report.push(format!(
            "{:<22} ef 10   {:.3} escalations {:>3}; without escalation {:.3}",
            case.name, widened.recall, widened.escalations, fixed.recall
        ));
        assert!(
            widened.escalations > 0,
            "{}: the narrow walk widens",
            case.name
        );
        assert!(
            widened.recall >= fixed.recall,
            "{}: escalation never loses recall",
            case.name
        );
    }
    println!("{}", report.join("\n"));
}

#[tokio::test]
#[ignore = "throughput report; run with --release -- --ignored --nocapture"]
async fn qps_100k_top10_ef64() {
    let (fixture, rows, centers) = build(100_000, 5, IndexPolicy::default()).await;
    let view = fixture.view().await;
    let queries = queries(&centers, 1_000, 3);
    let all = cases();
    // Unfiltered, uniform 10%, uniform 1%, anti-correlated 10%.
    let measured = [
        (&all[0], 1_000),
        (&all[2], 500),
        (&all[1], 500),
        (&all[5], 500),
    ];
    for (case, count) in measured {
        let warm = measure(
            &view,
            &rows,
            &queries[..50],
            case,
            SearchTuning::default(),
            Some(64),
        )
        .await;
        let started = Instant::now();
        let mut stages = [0_u64; 4];
        for query in &queries[..count] {
            let request = SearchRequest {
                ef: Some(64),
                filter: case.filter.clone(),
                ..SearchRequest::new(query.clone(), K)
            };
            let outcome = search(&view, &request)
                .await
                .expect("search should succeed");
            let timings = outcome.timings;
            let micros = [
                timings.planning,
                timings.candidates,
                timings.rerank + timings.merge,
                timings.project,
            ];
            for (total, micros) in stages.iter_mut().zip(micros) {
                *total += micros;
            }
        }
        let seconds = started.elapsed().as_secs_f64();
        let per_query = stages.map(|total| total / count as u64);
        let recall = measure(
            &view,
            &rows,
            &queries[..200],
            case,
            SearchTuning::default(),
            Some(64),
        )
        .await;
        println!(
            "100k x 128, top-10, ef=64, {:<20}: {:>6.0} QPS single-client, recall@10 {:.3} \
             (warm-up {:.3}); mean stage micros: plan+fetch {}, compute {}, fetch+rerank {}, \
             project {}",
            case.name,
            count as f64 / seconds,
            recall.recall,
            warm.recall,
            per_query[0],
            per_query[1],
            per_query[2],
            per_query[3]
        );
    }
}

/// Mean nanoseconds per call of `f` over `calls` calls.
fn time_ns(calls: usize, mut f: impl FnMut(usize) -> f32) -> f64 {
    let started = Instant::now();
    let mut sink = 0.0_f32;
    for index in 0..calls {
        sink += f(index);
    }
    assert!(sink.is_finite() || sink.is_nan());
    started.elapsed().as_secs_f64() * 1e9 / calls as f64
}

/// Least squares `y = a + b x`.
fn fit_line(points: &[(f64, f64)]) -> (f64, f64) {
    let n = points.len() as f64;
    let (sx, sy) = points
        .iter()
        .fold((0.0, 0.0), |(sx, sy), (x, y)| (sx + x, sy + y));
    let (mx, my) = (sx / n, sy / n);
    let (sxy, sxx) = points.iter().fold((0.0, 0.0), |(sxy, sxx), (x, y)| {
        (sxy + (x - mx) * (y - my), sxx + (x - mx) * (x - mx))
    });
    let slope = if sxx == 0.0 { 0.0 } else { sxy / sxx };
    (my - slope * mx, slope)
}

/// Mean counters and time of one unit's scan or walk over a batch of queries.
#[derive(Debug, Default)]
struct Sample {
    matched: f64,
    distances: f64,
    hops: f64,
    two_hops: f64,
    ef: f64,
    micros: f64,
    recall: f64,
}

async fn sample(
    view: &ReadView,
    rows: &[Row],
    queries: &[Vec<f32>],
    case: &Case,
    tuning: SearchTuning,
    ef: usize,
) -> Sample {
    let mut total = Sample::default();
    let mut strategies = Vec::new();
    for query in queries {
        let request = SearchRequest {
            filter: case.filter.clone(),
            tuning,
            ef: Some(ef),
            ..SearchRequest::new(query.clone(), K)
        };
        let outcome = search(view, &request).await.expect("search should succeed");
        let unit = outcome
            .units
            .iter()
            .find(|unit| !unit.memtable)
            .expect("one segment");
        strategies.push(unit.strategy);
        total.matched += unit.matched as f64;
        total.distances += unit.distances as f64;
        total.hops += unit.hops as f64;
        total.two_hops += unit.two_hops as f64;
        total.ef += unit.ef as f64;
        total.micros += outcome
            .plan
            .walk()
            .iter()
            .filter(|node| matches!(node.operator, Operator::GraphScan | Operator::ExactScan))
            .map(|node| node.actual.micros)
            .sum::<f64>();
    }
    let count = queries.len() as f64;
    let recall = measure(view, rows, queries, case, tuning, Some(ef))
        .await
        .recall;
    Sample {
        matched: total.matched / count,
        distances: total.distances / count,
        hops: total.hops / count,
        two_hops: total.two_hops / count,
        ef: total.ef / count,
        micros: total.micros / count,
        recall,
    }
}

#[tokio::test]
#[ignore = "cost model calibration; run with --release -- --ignored --nocapture"]
async fn calibrate_cost_model() {
    // Kernels alone: f32 and SQ8 distances at several widths give the per-dimension slope
    // and the fixed part of one call.
    let mut rng = Rng::new(77);
    let mut f32_points = Vec::new();
    let mut sq8_points = Vec::new();
    // The first pass warms the kernels and the CPU; the second is measured.
    for dims in [32_usize, 32, 128, 512] {
        let count = 4_096;
        let data = (0..count * dims)
            .map(|_| (rng.unit() * 2.0 - 1.0) as f32)
            .collect::<Vec<_>>();
        let query = (0..dims).map(|_| rng.unit() as f32).collect::<Vec<_>>();
        let f32_ns = time_ns(400_000, |index| {
            let row = index % count;
            kernels::l2_squared(&query, &data[row * dims..(row + 1) * dims])
        });
        let params = Sq8Params::train(&data, dims).expect("sq8 trains");
        let mut codes = vec![0_u8; count * dims];
        for row in 0..count {
            params
                .encode_into(
                    &data[row * dims..(row + 1) * dims],
                    &mut codes[row * dims..(row + 1) * dims],
                )
                .expect("encodes");
        }
        let sq8_query = params.query(Sq8Metric::L2Squared, &query).expect("query");
        let sq8_ns = time_ns(400_000, |index| {
            let row = index % count;
            sq8_query.estimate(&codes[row * dims..(row + 1) * dims])
        });
        println!("kernel dims {dims}: f32 {f32_ns:.1} ns, sq8 {sq8_ns:.1} ns");
        f32_points.push((dims as f64, f32_ns));
        sq8_points.push((dims as f64, sq8_ns));
    }
    // Drop the warm-up pass.
    f32_points.remove(0);
    sq8_points.remove(0);
    let (f32_fixed, f32_per_dim) = fit_line(&f32_points);
    let (sq8_kernel_fixed, sq8_per_dim) = fit_line(&sq8_points);

    let (fixture, rows, centers) = build(100_000, 5, IndexPolicy::default()).await;
    let view = fixture.view().await;
    let queries = queries(&centers, 100, 11);
    let base = SearchTuning {
        ef_escalation: false,
        parallel: false,
        ..SearchTuning::default()
    };
    let unfiltered = &cases()[0];
    let filtered = |percent: i64| Case {
        name: "uniform",
        filter: Some(FilterExpr::lt("bucket", percent)),
        keep: match percent {
            1 => |row: &Row| row.bucket < 1,
            3 => |row: &Row| row.bucket < 3,
            5 => |row: &Row| row.bucket < 5,
            10 => |row: &Row| row.bucket < 10,
            20 => |row: &Row| row.bucket < 20,
            30 => |row: &Row| row.bucket < 30,
            50 => |row: &Row| row.bucket < 50,
            _ => |_: &Row| true,
        },
        anti: false,
    };

    // Exact scans: engine cost per distance (kernel, code lookup, heap offer).
    let mut scan_ns = Vec::new();
    for percent in [5_i64, 20, 50, 100] {
        let case = filtered(percent);
        let exact = sample(
            &view,
            &rows,
            &queries[..40],
            &case,
            SearchTuning {
                force: Force::Exact,
                ..base
            },
            64,
        )
        .await;
        println!("exact {percent:>3}%: {exact:?}");
        scan_ns.push(exact.micros * 1_000.0 / exact.distances);
    }
    let per_distance = scan_ns.iter().sum::<f64>() / scan_ns.len() as f64;
    let sq8_fixed = per_distance - sq8_per_dim * DIMS as f64;
    println!("engine sq8 distance {per_distance:.1} ns (kernel fixed {sq8_kernel_fixed:.1})");

    // Unfiltered walks: expansions per unit of ef, novelty, and the price of a hop.
    let mut expansion_points = Vec::new();
    let mut novelty = Vec::new();
    let mut hop_points = Vec::new();
    let links = 32.0;
    for ef in [16_usize, 32, 64, 128, 256] {
        let walk = sample(
            &view,
            &rows,
            &queries,
            unfiltered,
            SearchTuning {
                force: Force::Admit,
                ..base
            },
            ef,
        )
        .await;
        println!("admit unfiltered ef {ef:>3}: {walk:?}");
        expansion_points.push((ef as f64, walk.hops));
        novelty.push(walk.distances / (walk.hops * links));
        hop_points.push(walk);
    }
    let (expansions_base, expansions_per_ef) = fit_line(&expansion_points);
    let novelty = novelty.iter().sum::<f64>() / novelty.len() as f64;
    let hop_ns = hop_points
        .iter()
        .map(|walk| (walk.micros * 1_000.0 - walk.distances * per_distance) / walk.hops)
        .sum::<f64>()
        / hop_points.len() as f64;

    // Filtered walks: how ACORN's expansions and two-hop lists grow as the filter narrows,
    // and how an admit-only walk's grow as 1 / s.
    let mut acorn_ratio = Vec::new();
    for percent in [1_i64, 3, 10, 30, 50] {
        let case = filtered(percent);
        for force in [Force::Admit, Force::Acorn] {
            let walk = sample(
                &view,
                &rows,
                &queries[..40],
                &case,
                SearchTuning { force, ..base },
                64,
            )
            .await;
            let expected = expansions_base + expansions_per_ef * 64.0;
            if force == Force::Acorn {
                acorn_ratio.push((walk.hops - walk.two_hops) / expected);
            }
            println!(
                "{force:?} {percent:>3}%: {walk:?} expansions/unfiltered {:.2} recall {:.3}",
                (walk.hops - walk.two_hops) / expected,
                walk.recall
            );
        }
        let probe = sample(
            &view,
            &rows,
            &queries[..40],
            &case,
            SearchTuning {
                force: Force::Acorn,
                ef_escalation: true,
                ..base
            },
            64,
        )
        .await;
        println!("Acorn {percent:>3}% escalating: {probe:?}");
    }
    let acorn_expansions = acorn_ratio.iter().sum::<f64>() / acorn_ratio.len() as f64;
    println!(
        "fit: sq8_ns_per_dim {sq8_per_dim:.3}, sq8_ns_fixed {sq8_fixed:.1}, f32_ns_per_dim \
         {f32_per_dim:.3}, f32_ns_fixed {f32_fixed:.1}, hop_ns {hop_ns:.1}, expansions_per_ef \
         {expansions_per_ef:.3}, expansions_base {expansions_base:.1}, novelty {novelty:.3}, \
         acorn_expansions {acorn_expansions:.3}"
    );
}
