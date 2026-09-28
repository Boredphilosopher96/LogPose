//! Randomized model checking: seeded runs of random actions from the full action table, each
//! checked against the model, in every maintenance mode and on both backends. A failing run
//! reports its seed and setup, and is replayed with fewer and fewer actions until it no longer
//! fails, so the report ends with a short trace.
//!
//! Environment:
//!
//! - `LOGPOSE_HARNESS_SEEDS`: seeds per test (default 12; CI and nightly runs raise it).
//! - `LOGPOSE_HARNESS_FIRST_SEED`: the first seed (default 0).
//! - `LOGPOSE_HARNESS_STEPS`: actions per seed (default 120).
//! - `LOGPOSE_HARNESS_SECS`: keep drawing seeds until this many seconds passed, after the fixed
//!   count (for time-bounded CI and nightly runs).
//! - `LOGPOSE_HARNESS_MINIMIZE=0`: report a failure without shrinking it.

use crate::{
    actions::{Action, Runner, Stats},
    generate::Generator,
    session::{Backend, Maintenance, Session, Setup},
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    time::{Duration, Instant},
};

/// Seeds that once found a bug, with their mode; they run first in every test of that mode.
/// See the "Implementation Notes (PR 13)" of the engine design for what each one found.
const REGRESSIONS: &[(Maintenance, u64)] = &[
    // Exact search broke ties at a candidate cut by row instead of by key.
    (Maintenance::Stepped, 1072),
];

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
}

pub fn steps() -> usize {
    env_u64("LOGPOSE_HARNESS_STEPS").map_or(120, |steps| steps as usize)
}

/// The seeds a test runs: pinned regressions, then `LOGPOSE_HARNESS_SEEDS` from
/// `LOGPOSE_HARNESS_FIRST_SEED`, then more until `LOGPOSE_HARNESS_SECS` passed.
pub struct Seeds {
    fixed: std::vec::IntoIter<u64>,
    next: u64,
    until: Option<Instant>,
}

impl Seeds {
    pub fn new(maintenance: Maintenance, default_count: u64) -> Self {
        let first = env_u64("LOGPOSE_HARNESS_FIRST_SEED").unwrap_or(0);
        let count = env_u64("LOGPOSE_HARNESS_SEEDS").unwrap_or(default_count);
        let mut fixed = REGRESSIONS
            .iter()
            .filter(|(mode, _)| *mode == maintenance)
            .map(|(_, seed)| *seed)
            .collect::<Vec<_>>();
        fixed.extend(first..first + count);
        Self {
            fixed: fixed.into_iter(),
            next: first + count,
            until: env_u64("LOGPOSE_HARNESS_SECS")
                .map(|secs| Instant::now() + Duration::from_secs(secs)),
        }
    }
}

impl Iterator for Seeds {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        if let Some(seed) = self.fixed.next() {
            return Some(seed);
        }
        match self.until {
            Some(until) if Instant::now() < until => {
                self.next += 1;
                Some(self.next - 1)
            }
            _ => None,
        }
    }
}

/// A failed run.
pub struct Failure {
    pub setup: Setup,
    pub seed: u64,
    pub message: String,
    pub actions: Vec<Action>,
}

/// Run `seed` for `steps` random actions.
pub fn run(setup: Setup, seed: u64, steps: usize) -> Result<Stats, Failure> {
    let mut actions = Vec::new();
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<Stats, String> {
        let session = Session::create(setup, seed)?;
        let mut runner = Runner::new(session);
        let mut generator = Generator::new(seed);
        for _ in 0..steps {
            let action = generator.next(&runner);
            actions.push(action.clone());
            runner.execute(&action)?;
        }
        Ok(runner.stats)
    }));
    let message = match outcome {
        Ok(Ok(stats)) => return Ok(stats),
        Ok(Err(message)) => message,
        Err(panic) => panic_message(&panic),
    };
    Err(Failure {
        setup,
        seed,
        message,
        actions,
    })
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
        .map_or_else(|| "panic".to_owned(), |text| format!("panic: {text}"))
}

/// Replay recorded actions from a fresh collection. Actions that refer to a token, scroll, or
/// job that no longer exists do nothing.
pub fn replay(setup: Setup, seed: u64, actions: &[Action]) -> Result<(), String> {
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        let session = Session::create(setup, seed)?;
        let mut runner = Runner::new(session);
        for (index, action) in actions.iter().enumerate() {
            runner
                .execute(action)
                .map_err(|error| format!("action {index}: {error}"))?;
        }
        Ok(())
    }));
    match outcome {
        Ok(result) => result,
        Err(panic) => Err(panic_message(&panic)),
    }
}

/// A failure message without numbers or the index of the failing action, so that two runs
/// failing the same check compare equal.
fn signature(message: &str) -> String {
    let message = message
        .strip_prefix("action ")
        .and_then(|rest| rest.split_once(": ").map(|(_, rest)| rest))
        .unwrap_or(message);
    message
        .chars()
        .filter(|c| !c.is_ascii_digit())
        .take(48)
        .collect()
}

/// Shrink a failing action list: drop chunks of actions, halving the chunk size, while the
/// replay still fails the same check. Bounded by a time budget, since background modes do not
/// always reproduce.
pub fn minimize(failure: &Failure) -> Option<(Vec<Action>, String)> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let wanted = signature(&failure.message);
    let same = |error: &String| signature(error) == wanted;
    let mut actions = failure.actions.clone();
    let mut message = replay(failure.setup, failure.seed, &actions)
        .err()
        .filter(same)?;
    let mut chunk = actions.len().div_ceil(2);
    while chunk >= 1 && Instant::now() < deadline {
        let mut start = 0;
        let mut shrunk = false;
        while start < actions.len() && Instant::now() < deadline {
            let end = (start + chunk).min(actions.len());
            let mut candidate = actions.clone();
            candidate.drain(start..end);
            match replay(failure.setup, failure.seed, &candidate) {
                Err(error) if same(&error) => {
                    actions = candidate;
                    message = error;
                    shrunk = true;
                }
                _ => start = end,
            }
        }
        if !shrunk {
            if chunk == 1 {
                break;
            }
            chunk = chunk.div_ceil(2);
        }
    }
    Some((actions, message))
}

/// Run every seed of `maintenance` on `backend`; on a failure, shrink it and panic with the
/// seed, the setup, and the trace. Returns the summed stats.
pub fn run_seeds(backend: Backend, maintenance: Maintenance, default_count: u64) -> Stats {
    let mut total = Stats::default();
    let mut seeds = 0;
    for seed in Seeds::new(maintenance, default_count) {
        let setup = Setup::for_seed(seed, backend, maintenance);
        match run(setup, seed, steps()) {
            Ok(stats) => total.add(&stats),
            Err(failure) => report(&failure),
        }
        seeds += 1;
    }
    println!("{backend:?} {maintenance:?}: {seeds} seeds, {total:?}");
    total
}

#[allow(
    clippy::panic,
    reason = "a failed run reports its seed, setup, and trace"
)]
fn report(failure: &Failure) -> ! {
    let trace = |actions: &[Action]| {
        actions
            .iter()
            .enumerate()
            .map(|(index, action)| format!("  {index:3} {action:?}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let shrunk = if std::env::var("LOGPOSE_HARNESS_MINIMIZE").as_deref() == Ok("0") {
        String::new()
    } else {
        match minimize(failure) {
            Some((actions, message)) => format!(
                "\nminimized to {} actions ({message}):\n{}",
                actions.len(),
                trace(&actions)
            ),
            None => {
                "\n(the failure did not reproduce on replay, so it was not minimized)".to_owned()
            }
        }
    };
    panic!(
        "seed {} failed ({:?}): {}\nreplay: LOGPOSE_HARNESS_FIRST_SEED={} LOGPOSE_HARNESS_SEEDS=1 \
         cargo test -p logpose-storage --test harness {:?}{shrunk}\nfull trace:\n{}",
        failure.seed,
        failure.setup,
        failure.message,
        failure.seed,
        failure.setup.maintenance,
        trace(&failure.actions)
    );
}

#[test]
fn random_actions_with_hand_stepped_jobs_match_the_model() {
    let stats = run_seeds(Backend::Fault, Maintenance::Stepped, 12);
    assert!(stats.jobs_committed > 0, "{stats:?}");
    assert!(stats.crashes > 0 && stats.token_reads > 0, "{stats:?}");
    assert!(
        stats.scrolls_finished > 0 && stats.filter_writes > 0,
        "{stats:?}"
    );
}

#[test]
fn random_actions_with_stepped_background_jobs_match_the_model() {
    let stats = run_seeds(Backend::Fault, Maintenance::Paused, 12);
    assert!(stats.steps_granted > 0 && stats.crashes > 0, "{stats:?}");
}

#[test]
fn random_actions_racing_free_background_jobs_match_the_model() {
    let stats = run_seeds(Backend::Fault, Maintenance::Free, 12);
    assert!(stats.crashes > 0, "{stats:?}");
}

#[test]
fn random_actions_on_the_real_filesystem_match_the_model() {
    let stats = run_seeds(Backend::Std, Maintenance::Stepped, 3);
    assert!(stats.jobs_committed > 0, "{stats:?}");
}
