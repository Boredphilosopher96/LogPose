//! Recall of the staged search on clustered data: 20,000 rows of 128 dimensions in one
//! segment with a graph and SQ8 codes, top 10, unfiltered and filtered at 1, 10, and 50
//! percent selectivity, with filters independent of the query and anti-correlated with it.
//! Each filtered case runs twice: with the default strategy thresholds (small filters scan
//! exactly) and with the exact scan disabled, so the ACORN-1 and admit-only walks carry the
//! recall themselves, including the `ef` escalation for anti-correlated filters.
//!
//! `qps_100k_top10_ef64` (ignored; run it in release) reports throughput on 100,000 rows.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    FilterComparison, FilterExpr, FilterOperator, ScalarMetadataValue, SearchRequest, SearchTuning,
    UnitStrategy, search,
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

fn comparison(field: &str, operator: FilterOperator, value: i64) -> FilterExpr {
    FilterExpr::Comparison(FilterComparison {
        field: field.to_owned(),
        operator,
        value: Some(ScalarMetadataValue::Number(value.into())),
    })
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
            filter: Some(comparison("bucket", FilterOperator::Eq, 7)),
            keep: |row| row.bucket == 7,
            anti: false,
        },
        Case {
            name: "uniform 10%",
            filter: Some(comparison("bucket", FilterOperator::Lt, 10)),
            keep: |row| row.bucket < 10,
            anti: false,
        },
        Case {
            name: "uniform 50%",
            filter: Some(comparison("bucket", FilterOperator::Lt, 50)),
            keep: |row| row.bucket < 50,
            anti: false,
        },
        Case {
            name: "anti-correlated 1%",
            filter: Some(comparison("cluster", FilterOperator::Eq, 99)),
            keep: |row| row.cluster == 99,
            anti: true,
        },
        Case {
            name: "anti-correlated 10%",
            filter: Some(comparison("cluster", FilterOperator::Gte, 90)),
            keep: |row| row.cluster >= 90,
            anti: true,
        },
        Case {
            name: "anti-correlated 50%",
            filter: Some(comparison("cluster", FilterOperator::Gte, 50)),
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
    let forced = SearchTuning {
        exact_max_matches: 0,
        ..SearchTuning::default()
    };
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
            "{:<22} walk    {:.3} escalations {:>3}; without escalation {:.3}",
            case.name, walked.recall, walked.escalations, fixed.recall
        ));
        assert!(
            walked.recall >= fixed.recall,
            "{}: escalation never loses recall",
            case.name
        );
        assert!(
            walked.recall >= MIN_RECALL,
            "{} (walk): recall {} below {MIN_RECALL}",
            case.name,
            walked.recall
        );
        assert!(
            walked.strategies.iter().all(|strategy| strategy.is_graph()),
            "{}: forced walks use the graph, got {:?}",
            case.name,
            walked.strategies
        );
        if case.name.ends_with("50%") {
            assert!(walked.strategies.contains(&UnitStrategy::GraphAdmit));
        } else {
            assert!(walked.strategies.contains(&UnitStrategy::GraphAcorn));
        }
    }
    // Escalation, isolated: a narrow beam (ef 10, one candidate per result) walking the
    // anti-correlated filters. Without escalation the first beam settles on matching rows
    // far from the query; the cursor widens it and recovers the recall.
    let narrow = SearchTuning {
        exact_max_matches: 0,
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
    let case = &cases()[0];
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
    for query in &queries {
        let request = SearchRequest {
            ef: Some(64),
            ..SearchRequest::new(query.clone(), K)
        };
        let outcome = search(&view, &request)
            .await
            .expect("search should succeed");
        for (total, micros) in stages.iter_mut().zip(outcome.micros) {
            *total += micros;
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    let per_query = stages.map(|total| total / queries.len() as u64);
    println!(
        "mean stage micros: plan+fetch {}, walk {}, fetch+rerank {}, project {}",
        per_query[0], per_query[1], per_query[2], per_query[3]
    );
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
        "100k x 128, top-10, ef=64: {:.0} QPS single-client ({} queries in {seconds:.2}s), \
         recall@10 {:.3} (warm-up recall {:.3})",
        queries.len() as f64 / seconds,
        queries.len(),
        recall.recall,
        warm.recall
    );
    let filtered = &cases()[2];
    let started = Instant::now();
    for query in &queries[..500] {
        let request = SearchRequest {
            ef: Some(64),
            filter: filtered.filter.clone(),
            ..SearchRequest::new(query.clone(), K)
        };
        search(&view, &request)
            .await
            .expect("search should succeed");
    }
    let seconds = started.elapsed().as_secs_f64();
    let recall = measure(
        &view,
        &rows,
        &queries[..200],
        filtered,
        SearchTuning::default(),
        Some(64),
    )
    .await;
    println!(
        "100k x 128, top-10, ef=64, uniform 10% filter: {:.0} QPS, recall@10 {:.3}",
        500.0 / seconds,
        recall.recall
    );
}
