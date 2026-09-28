//! Snapshot behavior of the read path: an implicit-snapshot query never expires under steady
//! flushing, pinned tokens read exactly their state, filter writes resolve against the
//! writer's latest state, and damaged index sections are typed corruption.

use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    FilterExpr, QueryRequest, QueryResponse, ReadConsistency, ScrollOrder, ScrollRequest,
    SearchRequest, VectorQuery, count, scroll, search,
};
use logpose_storage::{
    CollectionReader, Engine, EngineConfig, IndexPolicy, Projection, ReadOptions, SnapshotToken,
    segment_v2::{FileSource, SectionKind, SegmentReader},
};
use logpose_types::{
    CollectionRef, CorruptionKind, DistanceMetric, LogPoseError,
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
    FilterExpr::eq(field, value)
}

fn request(vector: Vec<f32>, token: Option<String>, pin: bool) -> QueryRequest {
    QueryRequest {
        vector: Some(VectorQuery {
            field: None,
            values: vector,
        }),
        top_k: 5,
        output_fields: vec!["group".to_owned()],
        read: ReadConsistency {
            snapshot_token: token,
            pin,
            ..ReadConsistency::default()
        },
        ..QueryRequest::default()
    }
}

async fn query(
    reader: &dyn CollectionReader,
    request: QueryRequest,
) -> logpose_query::Result<QueryResponse> {
    let collection = CollectionRef::parse("items").expect("name");
    logpose_query::query(reader, &collection, request)
        .await
        .map(|result| result.value)
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
                    unfiltered.filter = Some(eq("group", (served % 5) as i64));
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
            token: None,
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
    assert_eq!(left.hits, right.hits);
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
    let root_dir = support::unique_temp_dir("no-resolver");
    let root = root_dir.path().to_path_buf();
    let engine = Engine::open_local(&root, EngineConfig::default()).expect("engine");
    let descriptor = engine
        .plan_collection_descriptor(&logpose_storage::CreateCollectionRequest::new(
            "items",
            8,
            DistanceMetric::L2,
        ))
        .expect("plan");
    let handle = engine
        .create_collection(descriptor, None)
        .await
        .expect("create");
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
        // The graph lands in the segment's index sidecar, which the compaction builds.
        fixture.compact().await;
        let dir = fixture.close();
        let root = dir.path().to_path_buf();

        let (segment, offset, length) = find_section(&root, kind);
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
                        force: logpose_query::Force::Walk,
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

/// The segment or index sidecar file under `root` holding a section of `kind`, with the
/// section's offset and length.
fn find_section(root: &std::path::Path, kind: SectionKind) -> (std::path::PathBuf, u64, u64) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("entry").path();
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned());
            if path.is_dir() {
                stack.push(path);
            } else if name.is_some_and(|name| name.ends_with(".seg") || name.contains(".idx.")) {
                let reader = SegmentReader::open(FileSource::open(&path).expect("open"))
                    .expect("the file opens");
                if let Some(section) = reader
                    .sections()
                    .iter()
                    .find(|section| section.section_kind() == Some(kind))
                {
                    return (path, section.offset, section.length);
                }
            }
        }
    }
    unreachable!("no {kind:?} section under {}", root.display())
}

/// A scroll that fits in one page pins nothing: it returns no cursor, so nobody could use or
/// release a token. Pinning up front leaked one token per such scroll until its TTL, and the
/// 65th refused to start with `TooManySnapshots`. A scroll with more pages pins once.
#[tokio::test]
async fn a_scroll_that_fits_in_one_page_pins_nothing() {
    let fixture = Fixture::new(
        "one-page-scroll",
        8,
        DistanceMetric::L2,
        IndexPolicy::default(),
        &[("group", FieldType::Int64)],
    )
    .await;
    let mut rng = Rng::new(21);
    rows(&fixture, &mut rng, 0, 30).await;
    let page = |limit: u32, cursor| ScrollRequest {
        filter: None,
        order: ScrollOrder::Pk,
        limit,
        projection: Projection::scalars(),
        cursor,
        token: None,
    };
    for _ in 0..70 {
        let whole = scroll(&fixture.engine, &fixture.reference, page(100, None))
            .await
            .expect("a one-page scroll");
        assert_eq!(whole.rows.len(), 30);
        assert!(whole.next.is_none());
    }
    assert_eq!(fixture.handle.pinned_snapshots(), 0);

    let first = scroll(&fixture.engine, &fixture.reference, page(20, None))
        .await
        .expect("the first of two pages");
    assert_eq!(first.rows.len(), 20);
    assert_eq!(fixture.handle.pinned_snapshots(), 1);
    let second = scroll(&fixture.engine, &fixture.reference, page(20, first.next))
        .await
        .expect("the second page");
    assert_eq!(second.rows.len(), 10);
    assert!(second.next.is_none());
    assert_eq!(
        fixture.handle.pinned_snapshots(),
        0,
        "the last page releases the scroll's pin"
    );
}

/// A view keeps its state but not the engine: once the engine is dropped, a search that needs
/// segment sections fails with `Unavailable` (it never hangs or panics), and what the view
/// holds itself (its counters) still answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_view_that_outlives_its_engine_fails_fetches_without_hanging() {
    let fixture = Fixture::new(
        "outlives",
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
    let mut rng = Rng::new(17);
    rows(&fixture, &mut rng, 0, 100).await;
    fixture.flush().await;
    rows(&fixture, &mut rng, 100, 10).await;
    let view = fixture.view().await;
    let probe = rng.vector(8);
    let Fixture {
        engine,
        handle,
        dir: _dir,
        ..
    } = fixture;
    drop(handle);
    drop(engine);
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        search(&view, &SearchRequest::new(probe, 5)),
    )
    .await
    .expect("a search on a view whose engine is gone never hangs");
    assert!(
        matches!(
            outcome,
            Err(logpose_query::QueryError::Storage(
                LogPoseError::Unavailable { .. }
            ))
        ),
        "{outcome:?}"
    );
    assert_eq!(
        logpose_query::count_view(&view, None)
            .await
            .expect("counters"),
        110
    );
    drop(view);
}

/// A filter update whose key set is too large for one WAL frame fails with `TooLarge` before
/// anything is logged: no row changes, no sequence number is taken, and the collection stays
/// writable.
#[tokio::test]
async fn a_filter_write_too_large_for_one_frame_fails_and_changes_nothing() {
    // 2,048 dimensions: an update's row image carries its 8 KiB vector, so about 8,200
    // matching rows exceed a 64 MiB frame.
    let dims = 2048;
    let fixture = Fixture::new(
        "too-large",
        dims,
        DistanceMetric::L2,
        IndexPolicy::default(),
        &[("group", FieldType::Int64)],
    )
    .await;
    let mut rng = Rng::new(3);
    for batch in 0..9 {
        let records = (batch * 1000..(batch + 1) * 1000)
            .map(|index| {
                record(
                    &format!("r{index:05}"),
                    rng.vector(dims),
                    &[("group", Value::Int64(1))],
                )
            })
            .collect();
        fixture.upsert(records).await;
        if batch == 4 {
            fixture.flush().await;
        }
    }
    let before = fixture.handle.current().snapshot();
    let mut patch = PartialUpdate::new("ignored");
    patch.fields.insert("group".to_owned(), Value::Int64(2));
    let error = fixture
        .handle
        .update_by_filter(eq("group", 1), patch)
        .await
        .expect_err("9,000 updated rows exceed one frame");
    assert!(matches!(error, LogPoseError::TooLarge { .. }), "{error:?}");
    assert_eq!(fixture.handle.current().snapshot(), before);
    assert_eq!(
        count(
            &fixture.engine,
            &fixture.reference,
            Some(&eq("group", 1)),
            ReadOptions::default()
        )
        .await
        .expect("count"),
        9000
    );
    let ack = fixture
        .handle
        .delete_by_filter(eq("group", 1))
        .await
        .expect("a delete of the same rows fits");
    assert_eq!(ack.applied_ops, 9000);
    assert_eq!(ack.last_seq_no, before.visible_seq_no + 9000);
}

/// A read barrier holds for the state a request reads: a token that pins a state before the
/// barrier fails with `ReadBarrierNotSatisfied` instead of passing because the current state
/// satisfies it and then reading the older, pinned one.
#[tokio::test]
async fn a_read_barrier_holds_for_the_pinned_state_a_token_reads() {
    let fixture = Fixture::new(
        "token-barrier",
        8,
        DistanceMetric::L2,
        IndexPolicy::default(),
        &[("group", FieldType::Int64)],
    )
    .await;
    let mut rng = Rng::new(5);
    rows(&fixture, &mut rng, 0, 10).await;
    let probe = rng.vector(8);
    let pinned = query(&fixture.engine, request(probe.clone(), None, true))
        .await
        .expect("pinning query");
    let token = pinned.snapshot_token.clone().expect("token");
    rows(&fixture, &mut rng, 10, 10).await;
    let barrier = fixture.handle.current().snapshot();
    assert!(barrier.visible_seq_no > pinned.snapshot.visible_seq_no);

    let mut behind = request(probe.clone(), Some(token.clone()), false);
    behind.read.read_barrier = Some(barrier.clone());
    let error = query(&fixture.engine, behind)
        .await
        .expect_err("the pinned state is behind the barrier");
    assert!(
        matches!(
            error,
            logpose_query::QueryError::Storage(LogPoseError::ReadBarrierNotSatisfied { .. })
        ),
        "{error:?}"
    );

    let mut met = request(probe.clone(), Some(token), false);
    met.read.read_barrier = Some(pinned.snapshot.clone());
    let reply = query(&fixture.engine, met)
        .await
        .expect("the barrier is met");
    assert_eq!(reply.snapshot, pinned.snapshot);
    let mut current = request(probe, None, false);
    current.read.read_barrier = Some(barrier.clone());
    let reply = query(&fixture.engine, current)
        .await
        .expect("the current state meets it");
    assert_eq!(reply.snapshot, barrier);
}
