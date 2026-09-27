//! Property test for WAL v2 recovery over damaged multi-file logs.
//!
//! Each trial writes a clean log of several files, applies random damage (bit flips anywhere,
//! truncation, appended garbage, or a mix), and recovers. The exact outcome is predicted from
//! where the damage landed:
//!
//! - damage only in padding, in files at or below the checkpoint, or appended after the end of
//!   the last file: every frame comes back unchanged;
//! - damage confined to the last group, or a truncated last file: exactly the whole groups
//!   before the damage come back, and the repaired log accepts new groups;
//! - any other damage (a group that was followed by a later group, or any change to an older
//!   file's length): a typed corruption error.
//!
//! Recovery must never panic and never return a frame that differs from what was written.
//! `LOGPOSE_WAL_FUZZ_TRIALS` raises the number of trials.

use crc32c as _;
use crc32fast as _;
use logpose_types as _;
use postcard as _;
use serde as _;
use serde_json as _;
use thiserror as _;
use tracing as _;

use logpose_vfs::{FaultVfs, OpenMode, Vfs, read_file};
use logpose_wal::{
    codec::PayloadKind,
    v2::{BootId, FRAME_HEADER_LEN, WalConfig, WalError, WalFrame, WalRecovery, WalWriter},
};
use std::{
    io::IoSlice,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::Arc,
};

const WAL_DIR: &str = "/col/wal";

/// SplitMix64.
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

    fn bytes(&mut self, len: u64) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Rec {
    kind: PayloadKind,
    first: u64,
    last: u64,
    payload: Vec<u8>,
}

/// One frame's live bytes (header and payload, not padding).
#[derive(Clone, Debug)]
struct FrameSpan {
    start: u64,
    live_end: u64,
}

/// One fsync group as written.
#[derive(Clone, Debug)]
struct Group {
    file: usize,
    start: u64,
    end: u64,
    frames: Vec<FrameSpan>,
    recs: Vec<Rec>,
}

struct Log {
    vfs: Arc<FaultVfs>,
    files: Vec<PathBuf>,
    /// First sequence number of each file.
    file_first: Vec<u64>,
    groups: Vec<Group>,
    /// File contents of the clean log.
    clean: Vec<Vec<u8>>,
}

fn config() -> WalConfig {
    WalConfig {
        file_bytes: 1 << 30,
        epoch: 0,
        boot_id: BootId::new("fuzz"),
    }
}

fn recover(vfs: &Arc<FaultVfs>, checkpoint: u64) -> Result<(Vec<Rec>, WalWriter), WalError> {
    let mut recovery = WalRecovery::open(vfs.process(), WAL_DIR, config(), checkpoint)?;
    let mut recs = Vec::new();
    while let Some(frame) = recovery.next_frame()? {
        recs.push(Rec {
            kind: frame.header.kind,
            first: frame.header.first_seq_no,
            last: frame.header.last_seq_no,
            payload: frame.payload,
        });
    }
    let writer = recovery.into_writer(&WalFrame::checkpoint(checkpoint, b"restart".to_vec())?)?;
    Ok((recs, writer))
}

fn payload(rng: &mut Rng) -> Vec<u8> {
    let len = match rng.below(10) {
        0..=5 => rng.below(40),
        6..=8 => rng.below(400),
        _ => rng.below(5000),
    };
    rng.bytes(len)
}

fn span(start: u64, payload_len: usize) -> FrameSpan {
    FrameSpan {
        start,
        live_end: start + FRAME_HEADER_LEN as u64 + payload_len as u64,
    }
}

fn file_index(files: &[PathBuf], path: &Path) -> Result<usize, String> {
    files
        .iter()
        .position(|file| file == path)
        .ok_or_else(|| format!("unknown file {}", path.display()))
}

/// Write a clean log: the first file's checkpoint group, then random groups and rotations.
fn build_log(rng: &mut Rng) -> Result<Log, String> {
    let vfs = FaultVfs::new(rng.next());
    vfs.create_dir_all(Path::new("/col"))
        .and_then(|()| vfs.sync_dir(Path::new("/")))
        .map_err(|error| error.to_string())?;
    let (_, mut writer) = recover(&vfs, 0).map_err(|error| error.to_string())?;
    let mut files = vec![writer.active_path().to_path_buf()];
    let mut file_first = vec![1];
    let first_checkpoint =
        WalFrame::checkpoint(0, b"restart".to_vec()).map_err(|e| e.to_string())?;
    let mut groups = vec![Group {
        file: 0,
        start: 0,
        end: writer.synced_len(),
        frames: vec![span(0, first_checkpoint.payload().len())],
        recs: vec![Rec {
            kind: PayloadKind::Checkpoint,
            first: 0,
            last: 0,
            payload: first_checkpoint.payload().to_vec(),
        }],
    }];
    let group_count = 3 + rng.below(25);
    for _ in 0..group_count {
        if rng.chance(20) {
            let marker = writer.next_seq_no() - 1;
            let rec = Rec {
                kind: PayloadKind::Checkpoint,
                first: marker,
                last: marker,
                payload: payload(rng),
            };
            let frame =
                WalFrame::checkpoint(marker, rec.payload.clone()).map_err(|e| e.to_string())?;
            if writer.rotate(&frame).map_err(|e| e.to_string())? {
                files.push(writer.active_path().to_path_buf());
                file_first.push(writer.next_seq_no());
                groups.push(Group {
                    file: files.len() - 1,
                    start: 0,
                    end: writer.synced_len(),
                    frames: vec![span(0, rec.payload.len())],
                    recs: vec![rec],
                });
            }
            continue;
        }
        let mut seq = writer.next_seq_no();
        let mut recs = Vec::new();
        for _ in 0..=rng.below(4) {
            let rec = match rng.below(10) {
                0 => {
                    let marker = seq - 1;
                    Rec {
                        kind: PayloadKind::Checkpoint,
                        first: marker,
                        last: marker,
                        payload: payload(rng),
                    }
                }
                1 => {
                    seq += 1;
                    Rec {
                        kind: PayloadKind::SchemaChange,
                        first: seq - 1,
                        last: seq - 1,
                        payload: payload(rng),
                    }
                }
                _ => {
                    let first = seq;
                    seq += 1 + rng.below(3);
                    Rec {
                        kind: PayloadKind::WriteBatch,
                        first,
                        last: seq - 1,
                        payload: payload(rng),
                    }
                }
            };
            recs.push(rec);
        }
        let frames = recs
            .iter()
            .map(|rec| WalFrame::new(rec.kind, rec.first, rec.last, rec.payload.clone()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let commit = writer.append_group(&frames).map_err(|e| e.to_string())?;
        let mut offset = commit.start_offset;
        let mut spans = Vec::new();
        for frame in &frames {
            spans.push(span(offset, frame.payload().len()));
            offset += frame.encoded_len();
        }
        groups.push(Group {
            file: file_index(&files, &commit.file)?,
            start: commit.start_offset,
            end: commit.end_offset,
            frames: spans,
            recs,
        });
    }
    drop(writer);
    let clean = files
        .iter()
        .map(|file| read_file(vfs.as_ref(), file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    Ok(Log {
        vfs,
        files,
        file_first,
        groups,
        clean,
    })
}

/// What recovery must do for a damaged log.
#[derive(Clone, Copy, Debug)]
enum Expect {
    /// Exactly the first `n` groups, filtered to those above the checkpoint.
    Groups(usize),
    /// A typed corruption error.
    Corrupt,
}

fn damage(rng: &mut Rng, log: &Log) -> Result<String, String> {
    let vfs = log.vfs.as_ref();
    let last = log.files.len() - 1;
    let mut actions = Vec::new();
    let mut touched_last_group_only = rng.chance(30);
    for _ in 0..1 + rng.below(3) {
        let kind = rng.below(100);
        if kind < 55 {
            // Flip bits: anywhere, or (sometimes) only inside the last group.
            let (file, offset) = if touched_last_group_only {
                let group = log.groups.last().ok_or("no groups")?;
                (group.file, group.start + rng.below(group.end - group.start))
            } else {
                let file = rng.below(log.files.len() as u64) as usize;
                let len = log.clean[file].len() as u64;
                if len == 0 {
                    continue;
                }
                // Favor headers: they decide how everything after them is read.
                let offset = if rng.chance(50) {
                    let group = &log.groups[rng.below(log.groups.len() as u64) as usize];
                    let frame = &group.frames[rng.below(group.frames.len() as u64) as usize];
                    if group.file == file {
                        frame.start + rng.below(FRAME_HEADER_LEN as u64)
                    } else {
                        rng.below(len)
                    }
                } else {
                    rng.below(len)
                };
                (file, offset)
            };
            let path = &log.files[file];
            let current = read_file(vfs, path).map_err(|e| e.to_string())?;
            let Some(&byte) = usize::try_from(offset).ok().and_then(|at| current.get(at)) else {
                continue;
            };
            let flip = 1u8 << rng.below(8);
            log.vfs
                .corrupt(path, offset, &[byte ^ flip])
                .map_err(|e| e.to_string())?;
            actions.push(format!("flip {flip:#04x} at {}:{offset}", file));
        } else if kind < 75 {
            // Truncate a file (usually the last one).
            let file = if rng.chance(75) {
                last
            } else {
                rng.below(log.files.len() as u64) as usize
            };
            let path = &log.files[file];
            let handle = vfs
                .open(path, OpenMode::Append)
                .map_err(|e| e.to_string())?;
            let len = handle.len().map_err(|e| e.to_string())?;
            let cut = rng.below(len + 1);
            handle.set_len(cut).map_err(|e| e.to_string())?;
            handle.sync_all().map_err(|e| e.to_string())?;
            actions.push(format!("truncate {file} to {cut}"));
            touched_last_group_only = false;
        } else {
            // Append garbage (usually to the last file): random bytes, or zeros.
            let file = if rng.chance(75) {
                last
            } else {
                rng.below(log.files.len() as u64) as usize
            };
            let len = 1 + rng.below(300);
            let bytes = if rng.chance(50) {
                rng.bytes(len)
            } else {
                vec![0; len as usize]
            };
            let handle = vfs
                .open(&log.files[file], OpenMode::Append)
                .map_err(|e| e.to_string())?;
            handle
                .append(&[IoSlice::new(&bytes)])
                .and_then(|_| handle.sync_all())
                .map_err(|e| e.to_string())?;
            actions.push(format!("append {len} bytes to {file}"));
        }
    }
    Ok(actions.join(", "))
}

/// Predict the outcome from the actual bytes: which groups lost live bytes, and how file
/// lengths changed. Files below `first_read` are skipped by recovery.
fn predict(log: &Log, first_read: usize) -> Result<Expect, String> {
    let last = log.files.len() - 1;
    let mut first_damaged_group: Option<usize> = None;
    let mut note = |index: usize| {
        first_damaged_group = Some(first_damaged_group.map_or(index, |seen| seen.min(index)));
    };
    for (file, path) in log.files.iter().enumerate().skip(first_read) {
        let now = read_file(log.vfs.as_ref(), path).map_err(|e| e.to_string())?;
        let clean = &log.clean[file];
        if file != last && now.len() != clean.len() {
            return Ok(Expect::Corrupt);
        }
        for (index, group) in log.groups.iter().enumerate() {
            if group.file != file {
                continue;
            }
            let cut = (now.len() as u64) < group.end;
            let flipped = group.frames.iter().any(|frame| {
                (frame.start..frame.live_end).any(|at| {
                    let at = at as usize;
                    at < now.len() && now[at] != clean[at]
                })
            });
            if cut || flipped {
                note(index);
            }
        }
    }
    let Some(damaged) = first_damaged_group else {
        return Ok(Expect::Groups(log.groups.len()));
    };
    let group = &log.groups[damaged];
    if group.file != last {
        return Ok(Expect::Corrupt);
    }
    // Every group after the damaged one must be gone (cut off or damaged) for this to look
    // like a torn tail; a later intact group, or an intact frame of one, proves corruption.
    let now = read_file(log.vfs.as_ref(), &log.files[last]).map_err(|e| e.to_string())?;
    let clean = &log.clean[last];
    for later in &log.groups[damaged + 1..] {
        for frame in &later.frames {
            // The frame with its padding: a frame running past the end of the file is torn.
            let end = frame.start + (frame.live_end - frame.start).next_multiple_of(8);
            let intact = (end as usize) <= now.len()
                && (frame.start..frame.live_end).all(|at| now[at as usize] == clean[at as usize]);
            if intact {
                return Ok(Expect::Corrupt);
            }
        }
    }
    // Frames of the damaged group after the damage may survive; that is a torn group.
    Ok(Expect::Groups(damaged))
}

fn visible(log: &Log, groups: usize, checkpoint: u64) -> Vec<Rec> {
    log.groups[..groups]
        .iter()
        .flat_map(|group| group.recs.iter())
        .filter(|rec| rec.last > checkpoint)
        .cloned()
        .collect()
}

/// How often each outcome was predicted, so the test fails if a category is never exercised.
#[derive(Debug, Default)]
struct Tally {
    everything: u64,
    repaired: u64,
    corrupt: u64,
}

fn run_trial(seed: u64, tally: &mut Tally) -> Result<(), String> {
    let mut rng = Rng(seed);
    let log = build_log(&mut rng)?;
    // Usually replay everything; sometimes start at a file boundary so older files are skipped.
    let first_read = if log.files.len() > 1 && rng.chance(25) {
        rng.below(log.files.len() as u64) as usize
    } else {
        0
    };
    let checkpoint = log.file_first[first_read] - 1;
    let actions = damage(&mut rng, &log)?;
    let expect = predict(&log, first_read)?;
    match expect {
        Expect::Groups(groups) if groups == log.groups.len() => tally.everything += 1,
        Expect::Groups(_) => tally.repaired += 1,
        Expect::Corrupt => tally.corrupt += 1,
    }
    let context = format!("seed {seed}: {actions}; checkpoint {checkpoint}; expected {expect:?}");

    let outcome = catch_unwind(AssertUnwindSafe(|| recover(&log.vfs, checkpoint)))
        .map_err(|_| format!("{context}: recovery panicked"))?;
    match (expect, outcome) {
        (Expect::Groups(groups), Ok((recovered, mut writer))) => {
            let want = visible(&log, groups, checkpoint);
            if recovered != want {
                return Err(format!(
                    "{context}: recovered {} frames, expected {}",
                    recovered.len(),
                    want.len()
                ));
            }
            // The repaired log accepts a group that the next recovery returns.
            let seq = writer.next_seq_no();
            let frame = WalFrame::write_batch(seq, seq, b"after".to_vec())
                .map_err(|e| format!("{context}: {e}"))?;
            writer
                .append_group(&[frame])
                .map_err(|e| format!("{context}: append after repair failed: {e}"))?;
            drop(writer);
            let (again, _) = recover(&log.vfs, checkpoint)
                .map_err(|e| format!("{context}: second recovery failed: {e}"))?;
            let tail = again.last().map(|rec| (rec.first, rec.payload.clone()));
            if again.len() != want.len() + 1 || tail != Some((seq, b"after".to_vec())) {
                return Err(format!(
                    "{context}: second recovery returned the wrong frames"
                ));
            }
            Ok(())
        }
        (Expect::Groups(_), Err(error)) => Err(format!("{context}: unexpected error {error}")),
        (Expect::Corrupt, Ok((recovered, _))) => Err(format!(
            "{context}: damage accepted, recovered {} frames",
            recovered.len()
        )),
        (Expect::Corrupt, Err(error)) if error.is_corruption() => Ok(()),
        (Expect::Corrupt, Err(error)) => Err(format!(
            "{context}: expected a corruption error, got {error}"
        )),
    }
}

#[test]
fn damaged_logs_recover_a_prefix_of_whole_groups_or_fail_as_corrupt() {
    let trials = std::env::var("LOGPOSE_WAL_FUZZ_TRIALS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4000u64);
    let mut tally = Tally::default();
    let failures: Vec<String> = (0..trials)
        .filter_map(|seed| run_trial(seed, &mut tally).err())
        .collect();
    eprintln!("{trials} trials: {tally:?}");
    assert!(
        failures.is_empty(),
        "{} of {trials} trials failed:\n{}",
        failures.len(),
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        tally.everything > 0 && tally.repaired > 0 && tally.corrupt > 0,
        "a category was never exercised: {tally:?}"
    );
}
