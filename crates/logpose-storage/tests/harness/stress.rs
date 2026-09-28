//! Stress with invariant readers: writer threads issue random batches (and filter deletes) on
//! their own keys while reader threads check the internal consistency of every view they take,
//! and background flushes and compactions run with tiny thresholds.
//!
//! - Every acknowledged write is read back at once by another thread, which must see it (I1).
//! - Every view: `count` equals the rows a full scroll returns, no key appears twice, every
//!   scrolled row reads the same through `get`, the version's invariants hold, and
//!   `visible_seq_no` never decreases per reader (I6).
//! - At the end, and after a reopen, each writer's keys hold exactly its model.
//!
//! `LOGPOSE_STRESS_SECS` sets how long the writers run (default 3; nightly runs raise it).

use crate::session::{Backend, Maintenance, Session, Setup, reference};
use logpose_query::{
    FilterComparison, FilterExpr, FilterOperator, ScalarMetadataValue, ScrollOrder, count_view,
    scroll_view,
};
use logpose_storage::{CollectionHandle, Engine, Projection, ReadOptions, SchemaChange};
use logpose_types::{
    DistanceMetric, SeqNo,
    record::{ClientOp, PartialUpdate, PrimaryKey, Record},
    schema::{FieldType, ScalarFieldSpec},
    value::Value,
};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

const WRITERS: u64 = 4;
const READERS: u64 = 2;
const KEYS_PER_WRITER: u64 = 24;

/// A writer's rows: key to `(vector, n)`.
type Rows = BTreeMap<PrimaryKey, (Vec<f32>, i64)>;

fn stress_key(writer: u64, index: u64) -> PrimaryKey {
    PrimaryKey::from(format!("w{writer}-{index:02}").as_str())
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime builds")
}

/// What the ack checker is asked: after an ack at `seq`, `keys` must read as `expected`.
struct AckCheck {
    seq: SeqNo,
    expected: Vec<(PrimaryKey, Option<(Vec<f32>, i64)>)>,
    reply: mpsc::SyncSender<Result<(), String>>,
}

/// The value a stored row reads back as.
fn row_of(record: &Record) -> Option<(Vec<f32>, i64)> {
    let vector = record.vectors.get("vector")?.clone();
    let n = match record.fields.get("n") {
        Some(Value::Int64(n)) => *n,
        _ => return None,
    };
    Some((vector, n))
}

/// Read back each acknowledged write from another thread (I1).
fn ack_checker(engine: Engine, checks: mpsc::Receiver<AckCheck>) {
    let runtime = runtime();
    let collection = reference();
    for check in checks {
        let outcome = (|| {
            let view = engine
                .read_view_blocking(&collection, &ReadOptions::default())
                .map_err(|error| format!("view: {error}"))?;
            if view.visible_seq_no() < check.seq {
                return Err(format!(
                    "I1: a view after the ack at {} sees only {}",
                    check.seq,
                    view.visible_seq_no()
                ));
            }
            let keys = check
                .expected
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let rows = runtime
                .block_on(view.get(&keys, Projection::full()))
                .map_err(|error| format!("get: {error}"))?;
            for ((key, expected), row) in check.expected.iter().zip(rows) {
                let found = row.as_ref().and_then(|row| row_of(&row.record));
                if found != *expected {
                    return Err(format!(
                        "I1: {key} reads {found:?} right after the ack at {}, the writer wrote \
                         {expected:?}",
                        check.seq
                    ));
                }
            }
            Ok(())
        })();
        let _ = check.reply.send(outcome);
    }
}

/// One writer: random batches over its own keys, each checked by the ack checker.
fn writer(
    id: u64,
    handle: Arc<CollectionHandle>,
    checks: mpsc::SyncSender<AckCheck>,
    stop: Arc<AtomicBool>,
    writes: Arc<AtomicU64>,
) -> Result<Rows, String> {
    let mut rng = StdRng::seed_from_u64(0x57e55 + id);
    let runtime = runtime();
    let mut rows = Rows::new();
    let mut counter = 0_i64;
    while !stop.load(Ordering::Relaxed) {
        let mut next = rows.clone();
        let mut touched = BTreeSet::new();
        let result = if rng.random_range(0..20) == 0 {
            // Delete this writer's rows with a small `n`.
            let below = rng.random_range(0..8);
            let filter = FilterExpr::And {
                children: vec![
                    FilterExpr::Comparison(FilterComparison {
                        field: "w".to_owned(),
                        operator: FilterOperator::Eq,
                        value: Some(ScalarMetadataValue::Number((id as i64).into())),
                    }),
                    FilterExpr::Comparison(FilterComparison {
                        field: "n".to_owned(),
                        operator: FilterOperator::Lt,
                        value: Some(ScalarMetadataValue::Number(below.into())),
                    }),
                ],
            };
            next.retain(|key, (_, n)| {
                let keep = *n >= below;
                if !keep {
                    touched.insert(key.clone());
                }
                keep
            });
            let deleted = rows.len() - next.len();
            runtime
                .block_on(handle.delete_by_filter(filter))
                .and_then(|ack| {
                    if ack.applied_ops == deleted {
                        Ok(ack)
                    } else {
                        Err(logpose_types::LogPoseError::internal(format!(
                            "delete by filter removed {} rows, the writer expected {deleted}",
                            ack.applied_ops
                        )))
                    }
                })
        } else {
            let mut ops = Vec::new();
            for _ in 0..rng.random_range(1..=6) {
                let index = rng.random_range(0..KEYS_PER_WRITER);
                let key = stress_key(id, index);
                if !touched.insert(key.clone()) {
                    continue;
                }
                counter += 1;
                let vector = vec![(counter % 7) as f32, id as f32, 1.0, 0.0];
                let n = rng.random_range(0..10);
                match rng.random_range(0..10) {
                    0..=5 => {
                        let record = Record::new(key.clone())
                            .with_vector("vector", vector.clone())
                            .with_field("n", Value::Int64(n))
                            .with_field("w", Value::Int64(id as i64));
                        next.insert(key, (vector, n));
                        ops.push(ClientOp::Upsert(record));
                    }
                    6 | 7 if next.contains_key(&key) => {
                        let mut update = PartialUpdate::new(key.clone());
                        update.fields.insert("n".to_owned(), Value::Int64(n));
                        if let Some(row) = next.get_mut(&key) {
                            row.1 = n;
                        }
                        ops.push(ClientOp::Update(update));
                    }
                    _ => {
                        next.remove(&key);
                        ops.push(ClientOp::Delete(key));
                    }
                }
            }
            handle.write_blocking(ops)
        };
        let ack = result.map_err(|error| format!("writer {id}: {error}"))?;
        writes.fetch_add(1, Ordering::Relaxed);
        rows = next;
        let (reply, verdict) = mpsc::sync_channel(1);
        let expected = touched
            .into_iter()
            .map(|key| {
                let row = rows.get(&key).cloned();
                (key, row)
            })
            .collect();
        checks
            .send(AckCheck {
                seq: ack.last_seq_no,
                expected,
                reply,
            })
            .map_err(|_| "the ack checker stopped".to_owned())?;
        verdict
            .recv()
            .map_err(|_| "the ack checker stopped".to_owned())??;
    }
    Ok(rows)
}

/// One reader: take views and check each one's internal consistency.
fn reader(
    engine: Engine,
    handle: Arc<CollectionHandle>,
    stop: Arc<AtomicBool>,
) -> Result<u64, String> {
    let runtime = runtime();
    let collection = reference();
    let mut last = 0;
    let mut views = 0;
    while !stop.load(Ordering::Relaxed) {
        handle
            .current()
            .check_invariants()
            .map_err(|error| format!("invariants: {error}"))?;
        let view = engine
            .read_view_blocking(&collection, &ReadOptions::default())
            .map_err(|error| format!("view: {error}"))?;
        let seq = view.visible_seq_no();
        if seq < last {
            return Err(format!("I6: visible_seq_no went back from {last} to {seq}"));
        }
        last = seq;
        let count = runtime
            .block_on(count_view(&view, None))
            .map_err(|error| format!("count: {error}"))?;
        let (rows, _) = runtime
            .block_on(scroll_view(
                &view,
                None,
                &ScrollOrder::Pk,
                u32::MAX,
                Projection::full(),
                None,
            ))
            .map_err(|error| format!("scroll: {error}"))?;
        if rows.len() as u64 != count {
            return Err(format!(
                "I4: count is {count} but a full scroll returned {} rows at {seq}",
                rows.len()
            ));
        }
        let keys = rows
            .iter()
            .map(|row| row.record.pk.clone())
            .collect::<Vec<_>>();
        if keys.iter().collect::<BTreeSet<_>>().len() != keys.len() {
            return Err(format!("I5: a key appears twice in one scroll at {seq}"));
        }
        let got = runtime
            .block_on(view.get(&keys, Projection::full()))
            .map_err(|error| format!("get: {error}"))?;
        for (row, read) in rows.iter().zip(got) {
            if read.as_ref().map(|read| &read.record) != Some(&row.record) {
                return Err(format!(
                    "I4: {} scrolls as {:?} but reads as {:?} at {seq}",
                    row.record.pk,
                    row.record,
                    read.map(|read| read.record)
                ));
            }
        }
        // Filtered count against the scrolled rows.
        let small = rows
            .iter()
            .filter(|row| matches!(row.record.fields.get("n"), Some(Value::Int64(n)) if *n < 5))
            .count() as u64;
        let filter = FilterExpr::Comparison(FilterComparison {
            field: "n".to_owned(),
            operator: FilterOperator::Lt,
            value: Some(ScalarMetadataValue::Number(5.into())),
        });
        let counted = runtime
            .block_on(count_view(&view, Some(&filter)))
            .map_err(|error| format!("count: {error}"))?;
        if counted != small {
            return Err(format!(
                "I4: count(n < 5) is {counted} but the scroll holds {small} such rows at {seq}"
            ));
        }
        views += 1;
    }
    Ok(views)
}

/// Each writer's keys hold exactly its model.
fn check_final(session: &Session, models: &[Rows]) -> Result<(), String> {
    let view = session
        .view(&ReadOptions::default())
        .map_err(|error| format!("view: {error}"))?;
    let (rows, _) = session
        .block_on(scroll_view(
            &view,
            None,
            &ScrollOrder::Pk,
            u32::MAX,
            Projection::full(),
            None,
        ))
        .map_err(|error| format!("scroll: {error}"))?;
    let mut found = Rows::new();
    for row in rows {
        let value = row_of(&row.record).ok_or_else(|| format!("{} has no n", row.record.pk))?;
        found.insert(row.record.pk, value);
    }
    let expected = models
        .iter()
        .flatten()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Rows>();
    if found != expected {
        return Err(format!(
            "the final rows differ from the writers' models: {} vs {} rows",
            found.len(),
            expected.len()
        ));
    }
    Ok(())
}

#[test]
#[allow(clippy::panic, reason = "a failed stress run reports what broke")]
fn concurrent_writers_and_invariant_readers_beside_background_maintenance() {
    let secs = std::env::var("LOGPOSE_STRESS_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3);
    let setup = Setup {
        backend: Backend::Std,
        maintenance: Maintenance::Free,
        metric: DistanceMetric::Dot,
        indexed: true,
    };
    let mut session = Session::create(setup, 0).unwrap_or_else(|error| panic!("{error}"));
    session
        .handle()
        .alter_schema_blocking(SchemaChange::AddField(ScalarFieldSpec::new(
            "w",
            FieldType::Int64,
        )))
        .unwrap_or_else(|error| panic!("{error}"));
    let engine = session.engine().clone();
    let handle = session.handle().clone();
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let (checks, inbox) = mpsc::sync_channel::<AckCheck>(WRITERS as usize);
    let checker = {
        let engine = engine.clone();
        std::thread::spawn(move || ack_checker(engine, inbox))
    };
    let writers = (0..WRITERS)
        .map(|id| {
            let (handle, checks, stop, writes) = (
                handle.clone(),
                checks.clone(),
                Arc::clone(&stop),
                Arc::clone(&writes),
            );
            std::thread::spawn(move || writer(id, handle, checks, stop, writes))
        })
        .collect::<Vec<_>>();
    drop(checks);
    let readers = (0..READERS)
        .map(|_| {
            let (engine, handle, stop) = (engine.clone(), handle.clone(), Arc::clone(&stop));
            std::thread::spawn(move || reader(engine, handle, stop))
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(secs)
        && !writers.iter().any(|thread| thread.is_finished())
        && !readers.iter().any(|thread| thread.is_finished())
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);
    let models = writers
        .into_iter()
        .map(|thread| {
            thread
                .join()
                .unwrap_or_else(|_| panic!("a writer panicked"))
                .unwrap_or_else(|error| panic!("{error}"))
        })
        .collect::<Vec<_>>();
    let mut views = 0;
    for thread in readers {
        views += thread
            .join()
            .unwrap_or_else(|_| panic!("a reader panicked"))
            .unwrap_or_else(|error| panic!("{error}"));
    }
    let _ = checker.join();
    let written = handle.maintenance_written();
    drop((engine, handle));
    check_final(&session, &models).unwrap_or_else(|error| panic!("{error}"));
    session.close();
    session.open().unwrap_or_else(|error| panic!("{error}"));
    check_final(&session, &models).unwrap_or_else(|error| panic!("after a reopen: {error}"));
    let writes = writes.load(Ordering::Relaxed);
    println!("stress: {writes} writes, {views} checked views, {written:?} in {secs}s");
    assert!(writes > 0 && views > 0);
    assert!(written.flush_rows > 0, "{written:?}");
}
