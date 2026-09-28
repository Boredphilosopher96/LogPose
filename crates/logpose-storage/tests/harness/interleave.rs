//! Deterministic job interleaving: every position of each phase of a job (begin, build,
//! commit) relative to a short write sequence, with the phases of two jobs interleaved with
//! each other too. The writes move, update, and delete keys whose rows are in the job's inputs
//! (compaction inputs, the frozen memtable, the segment an index build indexes), so every case
//! of deletion-vector reconciliation, of a flush landing during a compaction, and of an index
//! build racing the compaction that takes its segment comes up. After every step the published state
//! equals the model and satisfies its invariants; at the end, a clean reopen and a crash both
//! recover it.

use crate::{
    actions::Checker,
    crash::{Ctx, rows_in_a_segment_and_the_memtable, three_segments},
};
use logpose_storage::JobKind;
use logpose_types::record::ClientOp;
use logpose_vfs::{FaultPlan, TearMode};

/// One step of an interleaving.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Event {
    /// The `n`-th write of the sequence.
    Write(usize),
    Begin(JobKind),
    Build(JobKind),
    Commit(JobKind),
}

fn phases(kind: JobKind) -> Vec<Event> {
    vec![Event::Begin(kind), Event::Build(kind), Event::Commit(kind)]
}

/// Every merge of `sequences` that keeps each one's order.
fn interleavings(sequences: &[Vec<Event>]) -> Vec<Vec<Event>> {
    fn merge(
        sequences: &[Vec<Event>],
        positions: &mut Vec<usize>,
        current: &mut Vec<Event>,
        out: &mut Vec<Vec<Event>>,
    ) {
        let mut done = true;
        for index in 0..sequences.len() {
            if positions[index] < sequences[index].len() {
                done = false;
                current.push(sequences[index][positions[index]]);
                positions[index] += 1;
                merge(sequences, positions, current, out);
                positions[index] -= 1;
                current.pop();
            }
        }
        if done {
            out.push(current.clone());
        }
    }
    let mut out = Vec::new();
    merge(
        sequences,
        &mut vec![0; sequences.len()],
        &mut Vec::new(),
        &mut out,
    );
    out
}

/// A scenario: a setup, the writes, and the jobs whose phases interleave with them.
struct Interleaving {
    name: &'static str,
    /// Whether segments get SQ8 codes and graphs.
    indexed: bool,
    setup: fn(&mut Ctx) -> Result<(), String>,
    writes: fn(&Ctx, usize) -> Vec<ClientOp>,
    write_count: usize,
    jobs: &'static [JobKind],
}

#[allow(clippy::panic, reason = "a failed interleaving reports its events")]
fn run(scenario: &Interleaving) -> usize {
    let mut sequences = vec![
        (0..scenario.write_count)
            .map(Event::Write)
            .collect::<Vec<_>>(),
    ];
    sequences.extend(scenario.jobs.iter().map(|kind| phases(*kind)));
    let all = interleavings(&sequences);
    for (index, events) in all.iter().enumerate() {
        let fail = |message: String| -> ! {
            panic!(
                "{}: interleaving {index} {events:?}: {message}",
                scenario.name
            )
        };
        let mut ctx =
            Ctx::with_indexes(index as u64, scenario.indexed).unwrap_or_else(|error| fail(error));
        (scenario.setup)(&mut ctx).unwrap_or_else(|error| fail(format!("setup: {error}")));
        for (step, event) in events.iter().enumerate() {
            let result = match event {
                Event::Write(n) => {
                    let ops = (scenario.writes)(&ctx, *n);
                    ctx.write(ops)
                }
                // An index build begun while a compaction holds every segment has nothing to
                // do; its later phases are then skipped.
                Event::Begin(JobKind::Index) => ctx.begin_if_due(JobKind::Index).map(drop),
                Event::Build(JobKind::Index) | Event::Commit(JobKind::Index)
                    if !ctx.has_job(JobKind::Index) =>
                {
                    Ok(())
                }
                Event::Begin(kind) => ctx.begin(*kind),
                Event::Build(kind) => ctx.build(*kind),
                Event::Commit(kind) => ctx.commit(*kind),
            };
            result.unwrap_or_else(|error| fail(format!("step {step} ({event:?}): {error}")));
            Checker::new(&ctx.session)
                .state_equals(ctx.last())
                .unwrap_or_else(|error| fail(format!("after step {step} ({event:?}): {error}")));
        }
        let model = ctx.last().clone();
        // A clean reopen, then a crash, recover the same state.
        ctx.close();
        ctx.session
            .open()
            .unwrap_or_else(|error| fail(format!("reopen: {error}")));
        Checker::new(&ctx.session)
            .state_equals(&model)
            .unwrap_or_else(|error| fail(format!("after a reopen: {error}")));
        ctx.close();
        let fault = ctx.fault();
        fault.set_plan(FaultPlan {
            tear: TearMode::TornGarbage,
            ..FaultPlan::default()
        });
        fault.crash();
        ctx.session
            .open()
            .unwrap_or_else(|error| fail(format!("recovery: {error}")));
        Checker::new(&ctx.session)
            .state_equals(&model)
            .unwrap_or_else(|error| fail(format!("after a crash: {error}")));
    }
    println!("{}: {} interleavings", scenario.name, all.len());
    all.len()
}

/// Writes that hit the rows of three segments of four rows (`k00` to `k11`): move a key out
/// of an input, delete an input row, update one, and delete the moved key again.
fn compaction_writes(ctx: &Ctx, n: usize) -> Vec<ClientOp> {
    match n {
        0 => vec![ctx.upsert(1, 10.0), ctx.delete(4)],
        1 => vec![ctx.update(9, 90.0), ctx.upsert(12, 12.0)],
        2 => vec![ctx.delete(1), ctx.update(2, 20.0)],
        _ => vec![ctx.upsert(4, 40.0), ctx.delete(12)],
    }
}

/// Writes over a segment (`k00` to `k05`, `k01` deleted) and memtable rows (`k06`, `k07`, and
/// the updated `k02`): delete and update rows of the memtable a flush freezes, and of the
/// segment whose deletion vector it writes.
fn flush_writes(ctx: &Ctx, n: usize) -> Vec<ClientOp> {
    match n {
        0 => vec![ctx.delete(6), ctx.update(3, 30.0)],
        1 => vec![ctx.update(7, 70.0), ctx.delete(0)],
        2 => vec![ctx.upsert(6, 60.0), ctx.delete(2)],
        _ => vec![ctx.delete(7), ctx.upsert(8, 8.0)],
    }
}

#[test]
fn every_position_of_a_compaction_among_writes_reconciles_deletions() {
    let count = run(&Interleaving {
        indexed: false,
        name: "compaction",
        setup: three_segments,
        writes: compaction_writes,
        write_count: 4,
        jobs: &[JobKind::Compact],
    });
    assert_eq!(count, 35);
}

#[test]
fn every_position_of_a_flush_among_writes_maps_late_deletions() {
    let count = run(&Interleaving {
        indexed: false,
        name: "flush",
        setup: rows_in_a_segment_and_the_memtable,
        writes: flush_writes,
        write_count: 4,
        jobs: &[JobKind::Flush],
    });
    assert_eq!(count, 35);
}

/// A flush and a compaction interleaved with each other and with two writes: every order of
/// their begins, builds, and commits.
#[test]
fn every_interleaving_of_a_flush_during_a_compaction_keeps_the_model() {
    let count = run(&Interleaving {
        indexed: false,
        name: "flush during compaction",
        setup: |ctx| {
            three_segments(ctx)?;
            let ops = vec![ctx.upsert(12, 12.0), ctx.upsert(13, 13.0), ctx.delete(5)];
            ctx.write(ops)
        },
        writes: |ctx, n| match n {
            0 => vec![ctx.upsert(1, 10.0), ctx.delete(12), ctx.update(6, 60.0)],
            _ => vec![ctx.delete(1), ctx.update(13, 130.0), ctx.upsert(12, 120.0)],
        },
        write_count: 2,
        jobs: &[JobKind::Compact, JobKind::Flush],
    });
    assert_eq!(count, 560);
}

/// Every position of an index build's begin, build, and commit among four writes that delete,
/// move, and update rows of the segment it indexes.
#[test]
fn every_position_of_an_index_build_among_writes_keeps_the_model() {
    let count = run(&Interleaving {
        indexed: true,
        name: "index build",
        setup: three_segments,
        writes: compaction_writes,
        write_count: 4,
        jobs: &[JobKind::Index],
    });
    assert_eq!(count, 35);
}

/// An index build and a compaction interleaved with each other and a write: a compaction
/// begun first leaves the build nothing to index, one begun during the build cancels it, and
/// one begun after its commit retires the segment with its sidecar.
#[test]
fn every_interleaving_of_an_index_build_and_a_compaction_keeps_the_model() {
    let count = run(&Interleaving {
        indexed: true,
        name: "index build and compaction",
        setup: three_segments,
        writes: compaction_writes,
        write_count: 1,
        jobs: &[JobKind::Index, JobKind::Compact],
    });
    assert_eq!(count, 140);
}
