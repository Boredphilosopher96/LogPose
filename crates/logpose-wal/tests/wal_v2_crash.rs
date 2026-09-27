//! Crash, fault and corruption tests for the WAL v2 frame layer on `FaultVfs`.
//!
//! A model tracks the groups the writer acknowledged and the one group whose outcome is unknown
//! when a crash hits. After every crash, recovery must return exactly the acknowledged groups
//! above the checkpoint, optionally followed by the whole in-flight group, and never a part of a
//! group.

use crc32c as _;
use crc32fast as _;
use logpose_types as _;
use postcard as _;
use serde as _;
use serde_json as _;
use thiserror as _;
use tracing as _;

use logpose_vfs::{FaultPlan, FaultVfs, TearMode, Vfs};
use logpose_wal::{
    codec::PayloadKind,
    v2::{
        BootId, GroupCommit, ReplayFrame, WalConfig, WalError, WalFrame, WalRecovery, WalWriter,
        WriteOutcome, frame_len,
    },
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const WAL_DIR: &str = "/col/wal";

/// SplitMix64, so workloads are reproducible from a seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// One frame as the model knows it.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Rec {
    kind: PayloadKind,
    first: u64,
    last: u64,
    payload: Vec<u8>,
}

impl Rec {
    fn from_replay(frame: &ReplayFrame) -> Self {
        Self {
            kind: frame.header.kind,
            first: frame.header.first_seq_no,
            last: frame.header.last_seq_no,
            payload: frame.payload.clone(),
        }
    }

    fn to_frame(&self) -> Result<WalFrame, WalError> {
        WalFrame::new(self.kind, self.first, self.last, self.payload.clone())
    }
}

#[derive(Default)]
struct Model {
    /// Groups known to be in the log, in order.
    groups: Vec<Vec<Rec>>,
    /// The group in flight when the process crashed: present or absent after recovery, but
    /// only as a whole.
    in_flight: Option<Vec<Rec>>,
    /// The durable checkpoint (the test's stand-in for a durable manifest).
    checkpoint: u64,
}

impl Model {
    fn visible(&self, groups: &[Vec<Rec>]) -> Vec<Rec> {
        groups
            .iter()
            .flatten()
            .filter(|rec| rec.last > self.checkpoint)
            .cloned()
            .collect()
    }

    /// Check recovered frames against the model and fold the in-flight group in if it survived.
    fn absorb(&mut self, recovered: &[Rec], context: &str) -> Result<(), String> {
        let without = self.visible(&self.groups);
        if recovered == without.as_slice() {
            self.in_flight = None;
            return Ok(());
        }
        if let Some(group) = self.in_flight.take() {
            let mut with_groups = self.groups.clone();
            with_groups.push(group.clone());
            if recovered == self.visible(&with_groups).as_slice() {
                self.groups.push(group);
                return Ok(());
            }
        }
        Err(format!(
            "{context}: recovered {} frames {:?} but the model allows {} frames {:?} (plus a whole in-flight group)",
            recovered.len(),
            summary(recovered),
            without.len(),
            summary(&without),
        ))
    }
}

fn summary(recs: &[Rec]) -> Vec<(u64, u64, usize)> {
    recs.iter()
        .map(|rec| (rec.first, rec.last, rec.payload.len()))
        .collect()
}

fn new_vfs(seed: u64) -> Result<Arc<FaultVfs>, std::io::Error> {
    let vfs = FaultVfs::new(seed);
    vfs.create_dir_all(Path::new("/col"))?;
    vfs.sync_dir(Path::new("/"))?;
    Ok(vfs)
}

fn config(vfs: &FaultVfs) -> WalConfig {
    WalConfig {
        file_bytes: 2048,
        epoch: 0,
        boot_id: BootId::new(format!("boot-{}", vfs.boot())),
    }
}

/// Recover fully. A crash injected into recovery itself is retried after the reboot, which
/// also checks that recovery is idempotent.
fn recover(vfs: &Arc<FaultVfs>, checkpoint: u64) -> Result<(Vec<Rec>, WalWriter), String> {
    for _attempt in 0..3 {
        match try_recover(vfs, checkpoint) {
            Ok(result) => return Ok(result),
            Err(error) if vfs.is_crashed() => {
                vfs.crash();
                let _ = error;
            }
            Err(error) => return Err(format!("recovery failed: {error}")),
        }
    }
    Err("recovery kept crashing".to_owned())
}

fn try_recover(vfs: &Arc<FaultVfs>, checkpoint: u64) -> Result<(Vec<Rec>, WalWriter), WalError> {
    let mut recovery = WalRecovery::open(vfs.process(), WAL_DIR, config(vfs), checkpoint)?;
    let mut frames = Vec::new();
    while let Some(frame) = recovery.next_frame()? {
        frames.push(Rec::from_replay(&frame));
    }
    let writer = recovery.into_writer(&WalFrame::checkpoint(checkpoint, Vec::new())?)?;
    Ok((frames, writer))
}

fn random_payload(rng: &mut Rng) -> Vec<u8> {
    let len = match rng.below(10) {
        0..=4 => rng.below(64),
        5..=8 => rng.below(700),
        _ => rng.below(6000),
    };
    (0..len).map(|_| rng.next() as u8).collect()
}

fn random_group(rng: &mut Rng, writer: &WalWriter, checkpoint: u64) -> Vec<Rec> {
    let mut seq = writer.next_seq_no();
    (0..=rng.below(4))
        .map(|_| {
            let payload = random_payload(rng);
            match rng.below(10) {
                0 => Rec {
                    kind: PayloadKind::Checkpoint,
                    first: checkpoint,
                    last: checkpoint,
                    payload,
                },
                1 => {
                    seq += 1;
                    Rec {
                        kind: PayloadKind::SchemaChange,
                        first: seq - 1,
                        last: seq - 1,
                        payload,
                    }
                }
                _ => {
                    let first = seq;
                    seq += 1 + rng.below(3);
                    Rec {
                        kind: PayloadKind::WriteBatch,
                        first,
                        last: seq - 1,
                        payload,
                    }
                }
            }
        })
        .collect()
}

/// Why a round of writing stopped.
enum Stop {
    /// The step budget ran out; the process is healthy.
    Done,
    /// A crash (planned) halted the process.
    Crashed,
    /// The writer failed without a crash (a failed sync during rotation).
    WriterFailed,
}

/// Run up to `steps` random writer actions, keeping the model in step.
fn write_round(
    rng: &mut Rng,
    vfs: &FaultVfs,
    writer: &mut WalWriter,
    model: &mut Model,
    steps: usize,
    commits: &mut Vec<(Vec<Rec>, GroupCommit)>,
) -> Result<Stop, String> {
    for _ in 0..steps {
        let action = rng.below(100);
        if action < 75 {
            let group = random_group(rng, writer, model.checkpoint);
            let frames = group
                .iter()
                .map(Rec::to_frame)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?;
            match writer.append_group(&frames) {
                Ok(commit) => {
                    model.groups.push(group.clone());
                    commits.push((group, commit));
                }
                Err(WalError::WriteFailed {
                    outcome: WriteOutcome::NotApplied,
                    ..
                }) if !vfs.is_crashed() => {
                    // Rolled back: absent after any crash; the writer stays usable.
                    if writer.failure().is_some() {
                        return Err("a clean rollback failed the writer".to_owned());
                    }
                }
                Err(error) => {
                    if !vfs.is_crashed() {
                        return Err(format!("append failed without a crash: {error}"));
                    }
                    model.in_flight = Some(group);
                    return Ok(Stop::Crashed);
                }
            }
        } else if action < 90 || writer.should_rotate() {
            let rec = Rec {
                kind: PayloadKind::Checkpoint,
                first: model.checkpoint,
                last: model.checkpoint,
                payload: random_payload(rng),
            };
            let frame = rec.to_frame().map_err(|error| error.to_string())?;
            match writer.rotate(&frame) {
                Ok(true) => model.groups.push(vec![rec]),
                Ok(false) => {}
                Err(_) if vfs.is_crashed() => {
                    model.in_flight = Some(vec![rec]);
                    return Ok(Stop::Crashed);
                }
                Err(WalError::WriteFailed {
                    outcome: WriteOutcome::NotApplied,
                    ..
                }) => return Ok(Stop::WriterFailed),
                Err(error) => return Err(format!("rotation failed without a crash: {error}")),
            }
        } else {
            // Checkpoint at the active file's boundary, then drop the files it covers.
            let active = writer
                .active_path()
                .file_name()
                .and_then(|name| logpose_wal::v2::parse_wal_file_name(&name.to_string_lossy()))
                .ok_or("active file has no WAL name")?;
            let checkpoint = active.saturating_sub(1);
            if checkpoint > model.checkpoint {
                model.checkpoint = checkpoint;
                if writer.remove_checkpointed(checkpoint).is_err() {
                    if vfs.is_crashed() {
                        return Ok(Stop::Crashed);
                    }
                    return Err("removing checkpointed files failed without a crash".to_owned());
                }
            }
        }
    }
    Ok(Stop::Done)
}

/// A multi-round scenario: write, crash (or fail a sync), recover and check, repeatedly.
fn run_random_scenario(seed: u64, tear: TearMode) -> Result<(), String> {
    let mut rng = Rng(seed ^ 0xA5A5_5A5A);
    let vfs = new_vfs(seed).map_err(|error| error.to_string())?;
    let mut model = Model::default();
    for round in 0..4 {
        // Sometimes crash inside recovery too.
        if round > 0 && rng.chance(30) {
            vfs.set_plan(FaultPlan {
                crash_after_ops: Some(rng.below(6)),
                tear,
                ..FaultPlan::default()
            });
        }
        let (recovered, mut writer) = recover(&vfs, model.checkpoint)?;
        vfs.set_plan(FaultPlan::default());
        model
            .absorb(&recovered, &format!("seed {seed} {tear:?} round {round}"))
            .map_err(|error| error.to_string())?;

        let mut plan = FaultPlan {
            tear,
            ..FaultPlan::default()
        };
        if rng.chance(80) {
            plan.crash_after_ops = Some(vfs.mutating_ops() + rng.below(120));
        }
        if rng.chance(40) {
            plan.fail_sync = Some(vfs.file_syncs() + rng.below(25));
        }
        vfs.set_plan(plan);
        let mut commits = Vec::new();
        let stop = write_round(&mut rng, &vfs, &mut writer, &mut model, 60, &mut commits)?;
        drop(writer);
        match stop {
            Stop::Crashed => {
                vfs.crash();
            }
            Stop::Done | Stop::WriterFailed => {
                vfs.set_plan(FaultPlan::default());
                if rng.chance(50) {
                    // Power loss with the process idle.
                    vfs.set_plan(FaultPlan {
                        tear,
                        ..FaultPlan::default()
                    });
                    vfs.crash();
                }
                // Otherwise an in-process reopen in the same boot.
            }
        }
    }
    let (recovered, _) = recover(&vfs, model.checkpoint)?;
    model.absorb(&recovered, &format!("seed {seed} {tear:?} final"))
}

#[test]
fn random_crashes_and_failed_syncs_never_lose_or_split_groups() -> TestResult {
    let mut failures = Vec::new();
    for seed in 0..150 {
        for tear in TearMode::ALL {
            if let Err(error) = run_random_scenario(seed, tear) {
                failures.push(error);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

/// A fixed workload with a crash after exactly `crash_after` mutating ops, then a check, more
/// writes on the repaired log, a second crash and a second check.
fn run_crash_at(seed: u64, tear: TearMode, crash_after: Option<u64>) -> Result<u64, String> {
    let mut rng = Rng(seed);
    let vfs = new_vfs(seed ^ 0x5EED).map_err(|error| error.to_string())?;
    let mut model = Model::default();
    let (_, mut writer) = recover(&vfs, 0)?;
    let start = vfs.mutating_ops();
    vfs.set_plan(FaultPlan {
        crash_after_ops: crash_after.map(|ops| start + ops),
        tear,
        ..FaultPlan::default()
    });
    let mut commits = Vec::new();
    write_round(&mut rng, &vfs, &mut writer, &mut model, 25, &mut commits)?;
    let used = vfs.mutating_ops() - start;
    drop(writer);
    vfs.crash();
    let context = format!("seed {seed} {tear:?} crash after {crash_after:?}");
    let (recovered, mut writer) = recover(&vfs, model.checkpoint)?;
    model.absorb(&recovered, &context)?;

    // The repaired log must accept appends that survive the next crash.
    vfs.set_plan(FaultPlan {
        tear,
        ..FaultPlan::default()
    });
    write_round(&mut rng, &vfs, &mut writer, &mut model, 5, &mut commits)?;
    drop(writer);
    vfs.crash();
    let (recovered, _) = recover(&vfs, model.checkpoint)?;
    model.absorb(&recovered, &format!("{context}, second crash"))?;
    Ok(used)
}

#[test]
fn exhaustive_crash_points_keep_every_acknowledged_group() -> TestResult {
    let mut failures = Vec::new();
    for seed in 0..4 {
        let total = run_crash_at(seed, TearMode::DropUnsynced, None)?;
        for tear in TearMode::ALL {
            for crash_after in 0..=total {
                if let Err(error) = run_crash_at(seed, tear, Some(crash_after)) {
                    failures.push(error);
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

/// Where each frame of a clean log lies on disk.
struct Layout {
    /// `(file, frame start, payload length, group index)`.
    frames: Vec<(PathBuf, u64, usize, usize)>,
    last_file: PathBuf,
    last_group: usize,
}

fn clean_log(seed: u64) -> Result<(Arc<FaultVfs>, Model, Layout), String> {
    let mut rng = Rng(seed);
    let vfs = new_vfs(seed).map_err(|error| error.to_string())?;
    let (_, mut writer) = recover(&vfs, 0)?;
    let mut model = Model::default();
    let mut frames = Vec::new();
    let mut group_index = 0;
    for _ in 0..30 {
        if rng.chance(15) {
            // A nonzero checkpoint marker keeps checkpoint frames visible to replay.
            let marker = writer.next_seq_no() - 1;
            let rec = Rec {
                kind: PayloadKind::Checkpoint,
                first: marker,
                last: marker,
                payload: random_payload(&mut rng),
            };
            let frame = rec.to_frame().map_err(|error| error.to_string())?;
            if writer.rotate(&frame).map_err(|error| error.to_string())? {
                frames.push((
                    writer.active_path().to_path_buf(),
                    0,
                    rec.payload.len(),
                    group_index,
                ));
                model.groups.push(vec![rec]);
                group_index += 1;
            }
            continue;
        }
        let mut group = random_group(&mut rng, &writer, 1);
        for rec in &mut group {
            if rec.kind == PayloadKind::Checkpoint {
                rec.first = 1;
                rec.last = 1;
            }
        }
        let wal_frames = group
            .iter()
            .map(Rec::to_frame)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        let commit = writer
            .append_group(&wal_frames)
            .map_err(|error| error.to_string())?;
        let mut offset = commit.start_offset;
        for rec in &group {
            frames.push((commit.file.clone(), offset, rec.payload.len(), group_index));
            offset += frame_len(rec.payload.len() as u32);
        }
        model.groups.push(group);
        group_index += 1;
    }
    let last_file = writer.active_path().to_path_buf();
    drop(writer);
    let last_group = group_index - 1;
    Ok((
        vfs,
        model,
        Layout {
            frames,
            last_file,
            last_group,
        },
    ))
}

#[test]
fn flipped_bytes_are_repaired_only_in_the_last_group_and_otherwise_rejected() -> TestResult {
    let mut failures = Vec::new();
    for seed in 0..60 {
        for trial in 0..8 {
            let (vfs, mut model, layout) = clean_log(seed)?;
            let mut rng = Rng(seed * 1000 + trial);
            let (file, start, payload_len, group) =
                layout.frames[rng.below(layout.frames.len() as u64) as usize].clone();
            let len = frame_len(payload_len as u32);
            let offset = start + rng.below(len);
            let mut byte = [0u8];
            let handle = vfs.open(&file, logpose_vfs::OpenMode::Read)?;
            handle.read_exact_at(&mut byte, offset)?;
            let flip = 1 + rng.below(255) as u8;
            vfs.corrupt(&file, offset, &[byte[0] ^ flip])?;

            let in_padding = offset >= start + 48 + payload_len as u64;
            let torn_tail = file == layout.last_file && group == layout.last_group;
            let context = format!(
                "seed {seed} trial {trial}: flipped offset {offset} of {} (frame at {start}, group {group})",
                file.display()
            );
            match recover(&vfs, 0) {
                Ok((recovered, _)) if in_padding => {
                    if let Err(error) = model.absorb(&recovered, &context) {
                        failures.push(error);
                    }
                }
                Ok((recovered, _)) if torn_tail => {
                    model.groups.pop();
                    if let Err(error) = model.absorb(&recovered, &context) {
                        failures.push(error);
                    }
                }
                Ok(_) => failures.push(format!("{context}: damage was silently accepted")),
                Err(error) if in_padding || torn_tail => {
                    failures.push(format!("{context}: unexpected error {error}"));
                }
                Err(error) if !error.contains("corrupt") => {
                    failures.push(format!(
                        "{context}: expected a corruption error, got {error}"
                    ));
                }
                Err(_) => {}
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

#[test]
fn in_process_reopen_after_a_rolled_back_group_agrees_with_the_next_boot() -> TestResult {
    for seed in 0..40 {
        for tear in TearMode::ALL {
            let vfs = new_vfs(seed)?;
            let mut rng = Rng(seed);
            let mut model = Model::default();
            let (_, mut writer) = recover(&vfs, 0)?;
            let mut commits = Vec::new();
            write_round(&mut rng, &vfs, &mut writer, &mut model, 5, &mut commits)?;
            vfs.set_plan(FaultPlan {
                fail_sync: Some(vfs.file_syncs()),
                tear,
                ..FaultPlan::default()
            });
            let group = random_group(&mut rng, &writer, model.checkpoint);
            let frames = group
                .iter()
                .map(Rec::to_frame)
                .collect::<Result<Vec<_>, _>>()?;
            let error = writer
                .append_group(&frames)
                .err()
                .ok_or("expected a failure")?;
            assert!(matches!(
                error,
                WalError::WriteFailed {
                    outcome: WriteOutcome::NotApplied,
                    ..
                }
            ));
            drop(writer);
            vfs.set_plan(FaultPlan {
                tear,
                ..FaultPlan::default()
            });
            // Reopen in the same boot, as the engine does after poisoning, then lose power.
            let (recovered, writer) = recover(&vfs, model.checkpoint)?;
            model.absorb(&recovered, &format!("seed {seed} {tear:?} in-process"))?;
            drop(writer);
            vfs.crash();
            let (recovered, _) = recover(&vfs, model.checkpoint)?;
            model.absorb(&recovered, &format!("seed {seed} {tear:?} after reboot"))?;
        }
    }
    Ok(())
}
