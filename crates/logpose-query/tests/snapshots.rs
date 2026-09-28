//! Snapshot behavior of the read path: an implicit-snapshot query never expires under steady
//! flushing, pinned tokens read exactly their state, filter writes resolve against the
//! writer's latest state, and damaged index sections are typed corruption.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    ExplainMode, FilterComparison, FilterExpr, FilterOperator, QueryRequest, QueryResponse,
    ScalarMetadataValue, ScrollOrder, ScrollRequest, SearchRequest, count, query, scroll, search,
};
use logpose_storage::{
    CollectionReader, EngineConfig, IndexPolicy, LocalStorageEngine, Projection, ReadOptions,
    SnapshotToken, StorageEngine,
    segment_v2::{FileSource, SectionKind, SegmentReader},
};
use logpose_types::{
    CorruptionKind, DistanceMetric, LogPoseError,
    record::{PartialUpdate, PrimaryKey},
    schema::FieldType,
    value::Value,
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json as _;
use std::{fs, sync::Arc};
use thiserror as _;

mod support;

use support::{Fixture, Rng, record};

fn eq(field: &str, value: i64) -> FilterExpr {
    FilterExpr::Comparison(FilterComparison {
        field: field.to_owned(),
        operator: FilterOperator::Eq,
        value: Some(ScalarMetadataValue::Number(value.into())),
    })
}

fn request(vector: Vec<f32>, token: Option<String>, pin: bool) -> QueryRequest {
    QueryRequest {
        collection_name: "items".to_owned(),
        vector,
        top_k: 5,
        snapshot: None,
        read_barrier: None,
        filters: Vec::new(),
        predicate: None,
        explain: ExplainMode::None,
        snapshot_token: token,
        pin,
    }
}

async fn rows(fixture: &Fixture, rng: &mut Rng, from: usize, count: usize) {
    let records = (from..from + count)
        .map(|index| {
            record(
                &format!("r{index:05}"),
                rng.vector(8),
                &[("group", Value::Int64((index % 5) as i64))],
            )
        })
        .collect();
    fixture.upsert(records).await;
}

/// A query that names no snapshot holds one view for all its stages, so a flush or
/// compaction that publishes while it runs never fails it (before the view, such a query
/// could fail with `SNAPSHOT_EXPIRED` after three retries under steady flushing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn implicit_snapshot_queries_never_expire_under_steady_flushing() {
    let fixture = Arc::new(
        Fixture::new(
            "steady-flush",
            8,
            DistanceMetric::L2,
            IndexPolicy {
                graph_min_rows: 64,
                sq8_min_rows: 32,
                ..IndexPolicy::default()
            },
            &[("group", FieldType::Int64)],
        )
        .await,
    );
    let mut rng = Rng::new(3);
    rows(&fixture, &mut rng, 0, 200).await;
    fixture.flush().await;

    let writer = {
        let fixture = Arc::clone(&fixture);
        tokio::spawn(async move {
            let mut rng = Rng::new(5);
            for round in 0..60 {
                rows(&fixture, &mut rng, 200 + round * 20, 20).await;
                fixture.flush().await;
                if round % 10 == 9 {
                    fixture.compact().await;
                }
            }
        })
    };
    let mut readers = Vec::new();
    for reader in 0..4_u64 {
        let fixture = Arc::clone(&fixture);
        readers.push(tokio::spawn(async move {
            let mut rng = Rng::new(100 + reader);
            let mut served = 0;
            while served < 150 {
                let mut unfiltered = request(rng.vector(8), None, false);
                if served % 3 == 0 {
                    unfiltered.predicate = Some(eq("group", (served % 5) as i64));
                }
                query(&fixture.engine, unfiltered)
                    .await
                    .expect("an implicit-snapshot query never fails");
                count(
                    &fixture.engine,
                    &fixture.reference,
                    Some(&eq("group", 1)),
                    ReadOptions::default(),
                )
                .await
                .expect("an implicit-snapshot count never fails");
                served += 1;
            }
        }));
    }
    writer.await.expect("writer");
    for reader in readers {
        reader.await.expect("reader");
    }
}

/// A pinned token reads exactly the state it pinned, through writes, deletes, flushes, a
/// compaction, and filter writes; once released it fails with `SnapshotExpired`.
#[tokio::test]
async fn pinned_tokens_read_exactly_the_pinned_state() {
    let fixture = Fixture::new(
        "pinned-tokens",
        8,
        DistanceMetric::L2,
        IndexPolicy {
            graph_min_rows: 64,
            sq8_min_rows: 32,
            ..IndexPolicy::default()
        },
        &[("group", FieldType::Int64)],
    )
    .await;
    let mut rng = Rng::new(9);
    rows(&fixture, &mut rng, 0, 150).await;
    fixture.flush().await;
    rows(&fixture, &mut rng, 150, 40).await;

    let probe = rng.vector(8);
    let pinned = query(&fixture.engine, request(probe.clone(), None, true))
        .await
        .expect("pinning query");
    let token = pinned
        .snapshot_token
        .clone()
        .expect("a pinned query returns its token");
    let through_token = |token: &str| request(probe.clone(), Some(token.to_owned()), false);
    let parsed: SnapshotToken = token.parse().expect("token parses");
    let options = || ReadOptions {
        token: Some(parsed.clone()),
        ..ReadOptions::default()
    };
    let pinned_count = count(
        &fixture.engine,
        &fixture.reference,
        Some(&eq("group", 2)),
        options(),
    )
    .await
    .expect("count through the token");
    let pinned_rows = scroll(
        &fixture.engine,
        &fixture.reference,
        ScrollRequest {
            filter: None,
            order: ScrollOrder::Pk,
            limit: u32::MAX,
            projection: Projection::scalars(),
            cursor: None,
        },
    )
    .await
    .expect("scroll")
    .rows;

    // Change everything the token must not see.
    rows(&fixture, &mut rng, 0, 60).await;
    fixture
        .handle
        .write(vec![logpose_types::record::ClientOp::Delete(
            PrimaryKey::from("r00007"),
        )])
        .await
        .expect("delete");
    fixture
        .handle
        .delete_by_filter(eq("group", 3))
        .await
        .expect("delete by filter");
    let mut patch = PartialUpdate::new("ignored");
    patch.fields.insert("group".to_owned(), Value::Int64(2));
    fixture
        .handle
        .update_by_filter(eq("group", 4), patch)
        .await
        .expect("update by filter");
    fixture.flush().await;
    fixture.compact().await;

    let again = query(&fixture.engine, through_token(&token))
        .await
        .expect("query through the token");
    assert_same(&pinned, &again);
    assert_eq!(again.snapshot, pinned.snapshot);
    assert_eq!(
        count(
            &fixture.engine,
            &fixture.reference,
            Some(&eq("group", 2)),
            options()
        )
        .await
        .expect("count through the token"),
        pinned_count
    );
    let view = fixture.view_with(options()).await;
    let (now_rows, _) = logpose_query::scroll_view(
        &view,
        None,
        &ScrollOrder::Pk,
        u32::MAX,
        Projection::scalars(),
        None,
    )
    .await
    .expect("scroll through the token");
    assert_eq!(now_rows.len(), pinned_rows.len());
    assert!(
        now_rows
            .iter()
            .zip(&pinned_rows)
            .all(|(now, then)| now.record == then.record)
    );
    let current = query(&fixture.engine, request(probe.clone(), None, false))
        .await
        .expect("current query");
    assert_ne!(current.snapshot, pinned.snapshot);

    assert!(fixture.handle.release_snapshot(&parsed));
    let expired = query(&fixture.engine, through_token(&token))
        .await
        .expect_err("a released token expires");
    assert!(
        matches!(
            LogPoseError::from(expired),
            LogPoseError::SnapshotExpired { .. }
        ),
        "released tokens fail with SnapshotExpired"
    );
    let invalid = query(&fixture.engine, through_token("not-a-token"))
        .await
        .expect_err("a malformed token is refused");
    assert!(matches!(
        LogPoseError::from(invalid),
        LogPoseError::InvalidArgument { .. }
    ));
}

fn assert_same(left: &QueryResponse, right: &QueryResponse) {
    assert_eq!(left.matches, right.matches);
    assert_eq!(left.snapshot, right.snapshot);
}

/// Delete-by-filter and update-by-filter resolve against the writer's latest state across
/// memtables and segments; an engine without a resolver refuses them.
#[tokio::test]
async fn filter_writes_resolve_against_the_latest_state() {
    let fixture = Fixture::new(
        "filter-writes",
        8,
        DistanceMetric::L2,
        IndexPolicy::default(),
        &[("group", FieldType::Int64)],
    )
    .await;
    let mut rng = Rng::new(21);
    rows(&fixture, &mut rng, 0, 50).await;
    fixture.flush().await;
    rows(&fixture, &mut rng, 50, 25).await;

    let deleted = fixture
        .handle
        .delete_by_filter(eq("group", 1))
        .await
        .expect("delete by filter");
    assert_eq!(deleted.applied_ops, 15);
    let none = fixture
        .handle
        .delete_by_filter(eq("group", 1))
        .await
        .expect("nothing left to delete");
    assert_eq!(none.applied_ops, 0);
    let mut patch = PartialUpdate::new("ignored");
    patch.fields.insert("group".to_owned(), Value::Int64(1));
    let updated = fixture
        .handle
        .update_by_filter(eq("group", 2), patch)
        .await
        .expect("update by filter");
    assert_eq!(updated.applied_ops, 15);
    let options = ReadOptions::default;
    assert_eq!(
        count(
            &fixture.engine,
            &fixture.reference,
            Some(&eq("group", 1)),
            options()
        )
        .await
        .expect("count"),
        15
    );
    assert_eq!(
        count(
            &fixture.engine,
            &fixture.reference,
            Some(&eq("group", 2)),
            options()
        )
        .await
        .expect("count"),
        0
    );
    assert_eq!(
        count(&fixture.engine, &fixture.reference, None, options())
            .await
            .expect("count"),
        60
    );

    // Without a resolver.
    let root = support::unique_temp_dir("no-resolver");
    let engine = LocalStorageEngine::with_config(&root, EngineConfig::default()).expect("engine");
    engine
        .create_collection(logpose_storage::CreateCollectionRequest::new(
            "items",
            8,
            DistanceMetric::L2,
        ))
        .await
        .expect("create");
    let handle = engine
        .engine()
        .collection(
            &logpose_storage::CreateCollectionRequest::new("items", 8, DistanceMetric::L2)
                .collection_ref(),
        )
        .expect("handle");
    let refused = handle
        .delete_by_filter(eq("group", 1))
        .await
        .expect_err("no resolver");
    assert!(matches!(refused, LogPoseError::FailedPrecondition { .. }));
}

/// Flipping a byte inside an index section (the graph, then a scalar index) makes the reads
/// that load it fail with `Corrupt { kind: Index }`; reads that do not need it still work.
#[tokio::test]
async fn corrupted_index_sections_are_typed_index_corruption() {
    let policy = IndexPolicy {
        graph_min_rows: 64,
        sq8_min_rows: 32,
        ..IndexPolicy::default()
    };
    for kind in [SectionKind::VectorGraph, SectionKind::ScalarInverted] {
        let fixture = Fixture::new(
            "index-corruption",
            8,
            DistanceMetric::L2,
            policy,
            &[("group", FieldType::Int64)],
        )
        .await;
        let mut rng = Rng::new(33);
        rows(&fixture, &mut rng, 0, 200).await;
        fixture.flush().await;
        let root = fixture.root.clone();
        drop(fixture);

        let segment = find_segment(&root);
        let (offset, length) = {
            let reader = SegmentReader::open(FileSource::open(&segment).expect("open"))
                .expect("segment opens");
            let section = reader
                .sections()
                .iter()
                .find(|section| section.section_kind() == Some(kind))
                .copied()
                .expect("the segment has the index section");
            (section.offset, section.length)
        };
        let mut bytes = fs::read(&segment).expect("read segment");
        let at = usize::try_from(offset + length / 2).expect("fits");
        bytes[at] ^= 0x5a;
        fs::write(&segment, &bytes).expect("write segment");

        let engine = Fixture::reopen(&root, policy);
        let reference =
            logpose_storage::CreateCollectionRequest::new("items", 8, DistanceMetric::L2)
                .collection_ref();
        let view = engine
            .read_view(&reference, ReadOptions::default())
            .await
            .expect("the collection opens: index sections are verified when loaded");
        let error = match kind {
            SectionKind::VectorGraph => search(
                &view,
                &SearchRequest {
                    tuning: logpose_query::SearchTuning {
                        exact_max_matches: 0,
                        ..logpose_query::SearchTuning::default()
                    },
                    ..SearchRequest::new(rng.vector(8), 5)
                },
            )
            .await
            .map(|_| ())
            .expect_err("the graph is damaged"),
            _ => logpose_query::count_view(&view, Some(&eq("group", 1)))
                .await
                .map(|_| ())
                .expect_err("the inverted index is damaged"),
        };
        match LogPoseError::from(error) {
            LogPoseError::Corrupt { kind: found, .. } => assert_eq!(found, CorruptionKind::Index),
            other => assert!(
                matches!(other, LogPoseError::Corrupt { .. }),
                "expected index corruption, got {other:?}"
            ),
        }
        // A read that does not load the damaged section still works.
        assert_eq!(
            logpose_query::count_view(&view, None)
                .await
                .expect("an unfiltered count needs no index"),
            200
        );
        drop(view);
        drop(engine);
    }
}

fn find_segment(root: &std::path::Path) -> std::path::PathBuf {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|extension| extension == "seg") {
                return path;
            }
        }
    }
    unreachable!("no segment under {}", root.display())
}
