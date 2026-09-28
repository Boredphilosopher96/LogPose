//! `EXPLAIN`: the plan tree is stable (snapshot tests of its rendering), carries estimates and
//! actual counts per operator with the reason for each strategy, and parallel execution
//! returns exactly what sequential execution returns.

use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    ExplainMode, FilterExpr, Force, Operator, PlanNode, QueryRequest, SearchRequest, SearchTuning,
    VectorQuery, search,
};
use logpose_storage::IndexPolicy;
use logpose_types::{DistanceMetric, schema::FieldType, value::Value};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json as _;
use thiserror as _;

mod support;

use support::{Fixture, Rng, record};

/// Two segments with SQ8 codes but no graph, and a memtable: every unit is scanned exactly,
/// so the plan and its counts depend only on the data.
async fn exact_fixture() -> Fixture {
    let fixture = Fixture::new(
        "explain-exact",
        4,
        DistanceMetric::L2,
        IndexPolicy {
            graph_min_rows: u32::MAX,
            sq8_min_rows: 8,
            ..IndexPolicy::default()
        },
        &[("bucket", FieldType::Int64)],
    )
    .await;
    let row = |index: usize| {
        let x = index as f32;
        record(
            &format!("r{index:03}"),
            vec![x, x * 0.5, -x, 1.0],
            &[("bucket", Value::Int64((index % 4) as i64))],
        )
    };
    fixture.upsert((0..40).map(row).collect()).await;
    fixture.flush().await;
    fixture.upsert((40..70).map(row).collect()).await;
    fixture.flush().await;
    fixture.upsert((70..75).map(row).collect()).await;
    fixture
}

fn operators(node: &PlanNode) -> Vec<Operator> {
    let mut out = vec![node.operator];
    if let Some(child) = node.children.first() {
        out.extend(operators(child));
    }
    out
}

#[tokio::test]
async fn explain_renders_a_stable_tree_for_exact_plans() {
    let fixture = exact_fixture().await;
    let view = fixture.view().await;
    let request = SearchRequest {
        filter: Some(FilterExpr::eq("bucket", 1_i64)),
        ..SearchRequest::new(vec![3.0, 1.5, -3.0, 1.0], 3)
    };
    // The first search loads the sections; the second reads them resident.
    search(&view, &request).await.expect("search");
    let outcome = search(&view, &request).await.expect("search");
    let keys = outcome
        .hits
        .iter()
        .map(|hit| hit.row.record.pk.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        ["r001", "r005", "r009"].map(logpose_types::record::PrimaryKey::from)
    );
    assert_eq!(
        outcome.plan.render(false),
        "\
Project k=3 (est rows=3) (actual rows=3)
└─ Merge units=3 k=3 (est rows=3) (actual rows=3)
   ├─ Rerank f32 keep=3 (est rows=3 dist=10) (actual rows=3 dist=10)
   │  └─ TopK k=12 (est rows=10) (actual rows=10)
   │     └─ ExactScan unit=00000002 sq8 (est rows=10 dist=10 resident=40B) (actual rows=10 dist=10)
   │        │  reason: no graph
   │        └─ MaskDeletes deleted=0 (est rows=10) (actual rows=10)
   │           └─ BitmapProbe eq bucket via sorted index (est rows=10) (actual rows=10)
   │              │  reason: exact cardinality from the bitmap
   │              └─ SegmentSource unit=00000002 segment rows=40 (est rows=40) (actual rows=40)
   ├─ Rerank f32 keep=3 (est rows=3 dist=8) (actual rows=3 dist=8)
   │  └─ TopK k=12 (est rows=8) (actual rows=8)
   │     └─ ExactScan unit=00000004 sq8 (est rows=8 dist=8 resident=32B) (actual rows=8 dist=8)
   │        │  reason: no graph
   │        └─ MaskDeletes deleted=0 (est rows=8) (actual rows=8)
   │           └─ BitmapProbe eq bucket via sorted index (est rows=8) (actual rows=8)
   │              │  reason: exact cardinality from the bitmap
   │              └─ SegmentSource unit=00000004 segment rows=30 (est rows=30) (actual rows=30)
   └─ ExactScan unit=00000003 f32 keep=3 (est rows=1 dist=1) (actual rows=1 dist=1)
      │  reason: memtables are scanned exactly
      └─ MaskDeletes deleted=0 (est rows=1) (actual rows=1)
         └─ BitmapProbe eq bucket via sorted index (est rows=1) (actual rows=1)
            │  reason: exact cardinality from the bitmap
            └─ SegmentSource unit=00000003 memtable rows=5 (est rows=5) (actual rows=5)
"
    );
}

#[tokio::test]
async fn query_explain_returns_the_tree_and_its_text() {
    let fixture = exact_fixture().await;
    let request = QueryRequest {
        vector: Some(VectorQuery {
            field: None,
            values: vec![0.0, 0.0, 0.0, 1.0],
        }),
        top_k: 2,
        explain: ExplainMode::Plan,
        ..QueryRequest::default()
    };
    let response = logpose_query::query(&fixture.engine, &fixture.reference, request.clone())
        .await
        .expect("query")
        .value;
    let diagnostics = response.diagnostics.expect("diagnostics");
    let plan = diagnostics.plan.expect("plan");
    assert_eq!(diagnostics.plan_text, plan.render(false));
    assert!(
        plan.walk().iter().all(|node| node.actual.micros == 0.0),
        "plan mode reports no times"
    );
    assert_eq!(
        operators(&plan),
        [
            Operator::Project,
            Operator::Merge,
            Operator::Rerank,
            Operator::TopK,
            Operator::ExactScan,
            Operator::MaskDeletes,
            Operator::SegmentSource
        ]
    );
    let profiled = logpose_query::query(
        &fixture.engine,
        &fixture.reference,
        QueryRequest {
            explain: ExplainMode::Profile,
            ..request
        },
    )
    .await
    .expect("query")
    .value
    .diagnostics
    .expect("diagnostics");
    let timings = profiled.stage_timings.expect("profile timings");
    assert!(timings.candidate_generation_micros >= timings.prefilter_micros);
    let plan = profiled.plan.expect("plan");
    assert!(plan.walk().iter().any(|node| node.actual.micros > 0.0));
    assert!(profiled.plan_text.contains("us)"), "{}", profiled.plan_text);

    // A query without a vector: Project over Merge over one ordered scan per unit.
    let scan = logpose_query::query(
        &fixture.engine,
        &fixture.reference,
        QueryRequest {
            filter: Some(FilterExpr::eq("bucket", 2_i64)),
            top_k: 4,
            explain: ExplainMode::Plan,
            ..QueryRequest::default()
        },
    )
    .await
    .expect("scan")
    .value
    .diagnostics
    .expect("diagnostics");
    let plan = scan.plan.expect("plan");
    assert_eq!(
        operators(&plan),
        [
            Operator::Project,
            Operator::Merge,
            Operator::OrderedScan,
            Operator::MaskDeletes,
            Operator::BitmapProbe,
            Operator::SegmentSource
        ]
    );
    assert_eq!(plan.children[0].children.len(), 3, "{}", scan.plan_text);
}

#[tokio::test]
async fn graph_plans_show_the_walk_its_estimate_and_its_reason() {
    let fixture = Fixture::new(
        "explain-graph",
        8,
        DistanceMetric::L2,
        IndexPolicy {
            graph_min_rows: 500,
            ..IndexPolicy::default()
        },
        &[("bucket", FieldType::Int64)],
    )
    .await;
    let mut rng = Rng::new(3);
    let records = (0..3_000)
        .map(|index| {
            record(
                &format!("g{index:05}"),
                rng.vector(8),
                &[("bucket", Value::Int64((index % 10) as i64))],
            )
        })
        .collect();
    fixture.upsert(records).await;
    fixture.flush().await;
    // A flushed segment gets its graph from an index build, which the compaction runs.
    fixture.compact().await;
    let view = fixture.view().await;
    for (force, filter) in [
        (Force::Admit, None),
        (Force::Acorn, Some(FilterExpr::eq("bucket", 3_i64))),
    ] {
        let outcome = search(
            &view,
            &SearchRequest {
                filter: filter.clone(),
                tuning: SearchTuning {
                    force,
                    ..SearchTuning::default()
                },
                ..SearchRequest::new(rng.vector(8), 5)
            },
        )
        .await
        .expect("search");
        let plan = &outcome.plan;
        let mut expected = vec![
            Operator::Project,
            Operator::Merge,
            Operator::Rerank,
            Operator::TopK,
            Operator::GraphScan,
            Operator::MaskDeletes,
        ];
        if filter.is_some() {
            expected.push(Operator::BitmapProbe);
        }
        expected.push(Operator::SegmentSource);
        assert_eq!(operators(plan), expected, "{}", plan.render(true));
        let walk = plan
            .walk()
            .into_iter()
            .find(|node| node.operator == Operator::GraphScan)
            .expect("a walk");
        let mode = if filter.is_some() { "acorn" } else { "admit" };
        assert!(walk.detail.contains(mode), "{}", walk.detail);
        assert!(walk.estimated.distances > 0 && walk.estimated.hops > 0);
        assert!(walk.actual.distances > 0 && walk.actual.hops > 0);
        assert!(walk.estimated.micros > 0.0);
        assert!(
            walk.reason
                .as_deref()
                .is_some_and(|reason| reason.contains("forced")),
            "{:?}",
            walk.reason
        );
    }
    // Unforced, the reason names the alternatives and their prices.
    let outcome = search(&view, &SearchRequest::new(rng.vector(8), 5))
        .await
        .expect("search");
    let reason = &outcome.units[0].reason;
    assert!(
        reason.contains("cheapest") || reason.contains("fit"),
        "{reason}"
    );
}

/// Units in parallel and exact scans split into parallel morsels return exactly the hits of a
/// sequential run, for every strategy.
#[tokio::test]
async fn parallel_execution_equals_sequential_execution() {
    let fixture = Fixture::new(
        "explain-parallel",
        8,
        DistanceMetric::Dot,
        IndexPolicy {
            graph_min_rows: 2_000,
            sq8_min_rows: 64,
            ..IndexPolicy::default()
        },
        &[("bucket", FieldType::Int64)],
    )
    .await;
    let mut rng = Rng::new(11);
    let mut next = 0;
    // A large segment (parallel morsels when scanned exactly), two small ones, a memtable.
    for (rows, flush) in [(20_000, true), (3_000, true), (500, true), (300, false)] {
        let records = (0..rows)
            .map(|_| {
                next += 1;
                record(
                    &format!("p{next:06}"),
                    rng.vector(8),
                    &[("bucket", Value::Int64(rng.below(100) as i64))],
                )
            })
            .collect();
        fixture.upsert(records).await;
        if flush {
            fixture.flush().await;
        }
    }
    let view = fixture.view().await;
    let filters = [
        None,
        Some(FilterExpr::lt("bucket", 90_i64)),
        Some(FilterExpr::lt("bucket", 10_i64)),
        Some(FilterExpr::eq("bucket", 5_i64)),
    ];
    for force in [Force::Auto, Force::Exact, Force::Walk, Force::Acorn] {
        for filter in &filters {
            for _ in 0..3 {
                let query = rng.vector(8);
                let run = |parallel: bool| SearchRequest {
                    filter: filter.clone(),
                    tuning: SearchTuning {
                        force,
                        parallel,
                        ..SearchTuning::default()
                    },
                    ..SearchRequest::new(query.clone(), 10)
                };
                let parallel = search(&view, &run(true)).await.expect("search");
                let sequential = search(&view, &run(false)).await.expect("search");
                let hits = |outcome: &logpose_query::SearchOutcome| {
                    outcome
                        .hits
                        .iter()
                        .map(|hit| (hit.row.record.pk.clone(), hit.value.to_bits()))
                        .collect::<Vec<_>>()
                };
                assert_eq!(
                    hits(&parallel),
                    hits(&sequential),
                    "force {force:?} filter {filter:?}"
                );
                assert_eq!(parallel.units.len(), sequential.units.len());
                if force == Force::Exact && filter.is_none() {
                    assert!(
                        parallel.plan.render(false).contains("morsels="),
                        "{}",
                        parallel.plan.render(false)
                    );
                }
            }
        }
    }
}
