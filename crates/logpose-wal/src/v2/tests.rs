//! Recovery and writer tests on [`FaultVfs`].

use super::*;
use crate::codec::PayloadKind;
use logpose_vfs::{
    CrashPoint, DirEntry, FaultPlan, FaultVfs, OpenMode, TearMode, Vfs, VfsFile, VfsLock, read_file,
};
use std::{
    io::{self, IoSlice},
    path::Path,
    sync::{Arc, Mutex},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const DIR: &str = "/c/wal";

fn dir() -> &'static Path {
    Path::new(DIR)
}

/// A filesystem holding a durable, empty collection directory `/c` (the engine's job).
fn new_vfs(seed: u64) -> Arc<FaultVfs> {
    let vfs = FaultVfs::new(seed);
    let created = vfs
        .create_dir_all(Path::new("/c"))
        .and_then(|()| vfs.sync_dir(Path::new("/")));
    assert!(created.is_ok(), "{created:?}");
    vfs
}

fn config(boot: &str) -> WalConfig {
    WalConfig {
        file_bytes: 1024,
        epoch: 0,
        boot_id: BootId::new(boot),
    }
}

/// Recover `vfs` fully: replayed frames, the writer, and the report.
fn recover(
    vfs: Arc<dyn Vfs>,
    boot: &str,
    checkpoint: SeqNo,
) -> Result<(Vec<ReplayFrame>, WalWriter, RecoveryReport), WalError> {
    let mut recovery = WalRecovery::open(vfs, dir(), config(boot), checkpoint)?;
    let mut frames = Vec::new();
    while let Some(frame) = recovery.next_frame()? {
        frames.push(frame);
    }
    let report = recovery.report().clone();
    let writer = recovery.into_writer(&WalFrame::checkpoint(checkpoint, Vec::new())?)?;
    Ok((frames, writer, report))
}

fn payload(seq: SeqNo, len: usize) -> Vec<u8> {
    (0..len).map(|index| (seq as usize + index) as u8).collect()
}

/// A group of `sizes.len()` single-operation write batches continuing the writer's log.
fn batch_group(writer: &WalWriter, sizes: &[usize]) -> Result<Vec<WalFrame>, WalError> {
    let mut seq = writer.next_seq_no();
    sizes
        .iter()
        .map(|&len| {
            let frame = WalFrame::write_batch(seq, seq, payload(seq, len));
            seq += 1;
            frame
        })
        .collect()
}

fn seqs(frames: &[ReplayFrame]) -> Vec<SeqNo> {
    frames
        .iter()
        .filter(|frame| frame.header.is_data())
        .map(|frame| frame.header.first_seq_no)
        .collect()
}

fn file_bytes(vfs: &dyn Vfs, path: &Path) -> Result<Vec<u8>, io::Error> {
    read_file(vfs, path)
}

/// Append raw bytes to a file, bypassing the writer.
fn append_raw(vfs: &dyn Vfs, path: &Path, bytes: &[u8]) -> TestResult {
    let file = vfs.open(path, OpenMode::Append)?;
    file.append(&[IoSlice::new(bytes)])?;
    file.sync_all()?;
    Ok(())
}

/// Write three groups (1 frame, 2 frames, 1 frame) and return the writer and the group commits.
fn three_groups(vfs: Arc<dyn Vfs>) -> Result<(WalWriter, Vec<GroupCommit>), WalError> {
    let (_, mut writer, _) = recover(vfs, "boot-a", 0)?;
    let mut commits = Vec::new();
    for sizes in [&[10][..], &[20, 30], &[40]] {
        let group = batch_group(&writer, sizes)?;
        commits.push(writer.append_group(&group)?);
    }
    Ok((writer, commits))
}

#[test]
fn fresh_directory_creates_the_first_file_durably() -> TestResult {
    let vfs = new_vfs(1);
    let (frames, writer, report) = recover(vfs.process(), "boot-a", 0)?;
    assert!(frames.is_empty());
    assert_eq!(report, RecoveryReport::default());
    assert_eq!(writer.next_seq_no(), 1);
    // The file starts with its own checkpoint group, group 0.
    assert_eq!(writer.next_group_no(), 1);
    assert_eq!(writer.synced_len(), frame_len(0));
    assert_eq!(writer.active_path(), dir().join(wal_file_name(1)));
    drop(writer);
    vfs.crash();
    let bytes = file_bytes(vfs.as_ref(), &dir().join(wal_file_name(1)))?;
    assert_eq!(
        bytes,
        WalFrame::checkpoint(0, Vec::new())?.encode(0, 0, true)
    );
    Ok(())
}

#[test]
fn groups_replay_in_order_with_group_numbers_and_group_end() -> TestResult {
    let vfs = new_vfs(2);
    let (writer, commits) = three_groups(vfs.process())?;
    assert_eq!(
        commits
            .iter()
            .map(|commit| commit.group_no)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(commits[0].start_offset, frame_len(0));
    assert_eq!(commits[1].seq_range, Some((2, 3)));
    assert_eq!(commits[1].start_offset, commits[0].end_offset);
    assert_eq!(writer.synced_len(), commits[2].end_offset);
    drop(writer);
    vfs.crash();

    let (frames, writer, report) = recover(vfs.process(), "boot-b", 0)?;
    assert_eq!(report.tail_repair, None);
    assert_eq!(seqs(&frames), vec![1, 2, 3, 4]);
    let groups: Vec<(u32, bool)> = frames
        .iter()
        .map(|frame| (frame.header.group_no, frame.header.group_end))
        .collect();
    assert_eq!(groups, vec![(1, true), (2, false), (2, true), (3, true)]);
    assert_eq!(frames[2].payload, payload(3, 30));
    assert_eq!(frames[2].offset, commits[1].start_offset + frame_len(20));
    assert_eq!(writer.next_seq_no(), 5);
    assert_eq!(writer.next_group_no(), 4);
    assert_eq!(writer.synced_len(), commits[2].end_offset);
    Ok(())
}

#[test]
fn a_group_is_one_append_and_one_sync() -> TestResult {
    let vfs = new_vfs(3);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    let before = vfs.mutating_ops();
    let points = vfs.crash_points_hit().len();
    let group = batch_group(&writer, &[100, 5000, 3])?;
    writer.append_group(&group)?;
    assert_eq!(vfs.mutating_ops() - before, 2);
    assert_eq!(
        vfs.crash_points_hit()[points..],
        [CrashPoint::WalAfterAppend, CrashPoint::WalAfterSync]
    );
    Ok(())
}

#[test]
fn rejects_groups_that_break_the_sequence_without_writing() -> TestResult {
    let vfs = new_vfs(4);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    let ops = vfs.mutating_ops();
    assert!(matches!(
        writer.append_group(&[]),
        Err(WalError::InvalidFrame { .. })
    ));
    let gap = [WalFrame::write_batch(2, 2, Vec::new())?];
    assert!(matches!(
        writer.append_group(&gap),
        Err(WalError::InvalidFrame { .. })
    ));
    let overlap = [
        WalFrame::write_batch(1, 3, Vec::new())?,
        WalFrame::write_batch(3, 3, Vec::new())?,
    ];
    assert!(matches!(
        writer.append_group(&overlap),
        Err(WalError::InvalidFrame { .. })
    ));
    assert_eq!(vfs.mutating_ops(), ops);
    // Checkpoint frames consume no sequence numbers and may appear anywhere in a group.
    let mixed = [
        WalFrame::write_batch(1, 3, Vec::new())?,
        WalFrame::checkpoint(0, b"marker".to_vec())?,
        WalFrame::schema_change(4, Vec::new())?,
    ];
    let commit = writer.append_group(&mixed)?;
    assert_eq!(commit.seq_range, Some((1, 4)));
    assert_eq!(writer.next_seq_no(), 5);
    Ok(())
}

#[test]
fn torn_partial_frame_at_the_tail_is_truncated_on_open() -> TestResult {
    let vfs = new_vfs(5);
    let (writer, commits) = three_groups(vfs.process())?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    let torn = WalFrame::write_batch(5, 5, payload(5, 100))?.encode(0, 4, true);
    append_raw(vfs.as_ref(), &path, &torn[..70])?;

    let (frames, writer, report) = recover(vfs.process(), "boot-a", 0)?;
    assert_eq!(seqs(&frames), vec![1, 2, 3, 4]);
    let repair = report.tail_repair.ok_or("expected a tail repair")?;
    assert_eq!(repair.original_len, commits[2].end_offset + 70);
    assert_eq!(repair.repaired_len, commits[2].end_offset);
    assert_eq!(repair.discarded_frames, 0);
    assert!(repair.damage.is_some());
    assert_eq!(writer.synced_len(), commits[2].end_offset);
    assert!(
        vfs.crash_points_hit()
            .contains(&CrashPoint::RecoveryAfterTailRepair)
    );
    // The truncation is durable.
    drop(writer);
    vfs.crash();
    assert_eq!(
        file_bytes(vfs.as_ref(), &path)?.len() as u64,
        commits[2].end_offset
    );
    Ok(())
}

#[test]
fn incomplete_last_group_is_discarded_as_a_unit() -> TestResult {
    let vfs = new_vfs(6);
    let (writer, commits) = three_groups(vfs.process())?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    // Two complete, checksummed frames of group 4 whose GROUP_END frame never made it.
    let mut raw = WalFrame::write_batch(5, 5, payload(5, 9))?.encode(0, 4, false);
    raw.extend(WalFrame::write_batch(6, 7, payload(6, 17))?.encode(0, 4, false));
    append_raw(vfs.as_ref(), &path, &raw)?;

    let (frames, writer, report) = recover(vfs.process(), "boot-a", 0)?;
    assert_eq!(seqs(&frames), vec![1, 2, 3, 4]);
    let repair = report.tail_repair.ok_or("expected a tail repair")?;
    assert_eq!(repair.repaired_len, commits[2].end_offset);
    assert_eq!(repair.discarded_frames, 2);
    assert_eq!(repair.discarded_seq, Some((5, 7)));
    assert_eq!(repair.damage, None);
    // The writer reuses the discarded group's number and sequence numbers.
    assert_eq!(writer.next_group_no(), 4);
    assert_eq!(writer.next_seq_no(), 5);
    Ok(())
}

#[test]
fn damaged_frame_followed_by_a_later_group_is_corruption_and_nothing_changes() -> TestResult {
    let vfs = new_vfs(7);
    let (writer, commits) = three_groups(vfs.process())?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    // Damage the payload of the first frame of group 1; group 2 follows it durably.
    vfs.corrupt(&path, commits[1].start_offset + 50, &[0xFF])?;
    let before = file_bytes(vfs.as_ref(), &path)?;

    let error = recover(vfs.process(), "boot-a", 0)
        .err()
        .ok_or("expected an error")?;
    assert!(
        matches!(&error, WalError::Corrupt { offset, .. } if *offset == commits[1].start_offset),
        "{error}"
    );
    assert!(error.is_corruption());
    assert_eq!(file_bytes(vfs.as_ref(), &path)?, before);
    assert!(
        !vfs.crash_points_hit()
            .contains(&CrashPoint::RecoveryAfterTailRepair)
    );
    Ok(())
}

#[test]
fn damaged_group_end_of_an_acked_group_followed_by_one_complete_group_is_corruption() -> TestResult
{
    // Group 1 = frames (2, 3); its GROUP_END frame 3 is damaged; group 2 (frame 4) is complete.
    // Looking only at GROUP_END flags, frame 4 would look like the end of the torn group and
    // repair would drop two acknowledged groups. The group numbers tell them apart.
    let vfs = new_vfs(8);
    let (writer, commits) = three_groups(vfs.process())?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    let group_end_frame = commits[1].start_offset + frame_len(20);
    vfs.corrupt(&path, group_end_frame + 4, &[0, 0, 0, 0])?;
    let error = recover(vfs.process(), "boot-a", 0)
        .err()
        .ok_or("expected an error")?;
    assert!(
        matches!(&error, WalError::Corrupt { offset, .. } if *offset == group_end_frame),
        "{error}"
    );
    Ok(())
}

#[test]
fn damage_inside_the_last_group_is_a_torn_tail() -> TestResult {
    let vfs = new_vfs(9);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    let first = writer.append_group(&batch_group(&writer, &[10])?)?;
    let last = writer.append_group(&batch_group(&writer, &[100, 9000, 20, 20])?)?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    // Out-of-order writeback: a page inside the second frame of the last group never reached
    // the disk, but the pages after it did.
    vfs.corrupt(&path, last.start_offset + 4096, &[0; 4096])?;

    let (frames, _, report) = recover(vfs.process(), "boot-a", 0)?;
    assert_eq!(seqs(&frames), vec![1]);
    let repair = report.tail_repair.ok_or("expected a tail repair")?;
    assert_eq!(repair.repaired_len, first.end_offset);
    // The frame before the damage and the checksummed frames found past it.
    assert_eq!(repair.discarded_frames, 3);
    assert_eq!(repair.discarded_seq, Some((2, 5)));
    assert_eq!(
        repair.damage,
        Some((last.start_offset + frame_len(100), "bad payload checksum"))
    );
    Ok(())
}

#[test]
fn damaged_first_frame_of_a_file_with_several_later_groups_is_corruption() -> TestResult {
    let vfs = new_vfs(10);
    let (writer, _) = three_groups(vfs.process())?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    vfs.corrupt(&path, 0, b"XXXX")?;
    let error = recover(vfs.process(), "boot-a", 0)
        .err()
        .ok_or("expected an error")?;
    assert!(
        matches!(error, WalError::Corrupt { offset: 0, .. }),
        "{error}"
    );
    Ok(())
}

#[test]
fn damaged_first_frame_of_a_single_group_file_is_a_torn_tail() -> TestResult {
    let vfs = new_vfs(11);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    let group = writer.append_group(&batch_group(&writer, &[10, 20, 30])?)?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    vfs.corrupt(&path, group.start_offset, b"XXXX")?;
    let (frames, writer, report) = recover(vfs.process(), "boot-a", 0)?;
    assert!(frames.is_empty());
    assert_eq!(
        report.tail_repair.map(|repair| repair.repaired_len),
        Some(group.start_offset)
    );
    assert_eq!(writer.next_seq_no(), 1);
    Ok(())
}

#[test]
fn damaged_checkpoint_frame_followed_by_one_acked_group_is_corruption() -> TestResult {
    // The file's first frame is its checkpoint group, synced before anything else is appended,
    // so damage to it followed by a checksummed frame is media damage to a durable group.
    // Without that rule, the single later group looks like the rest of a torn first group and
    // repair truncates the file to nothing, losing an acknowledged group.
    for remove_older in [false, true] {
        for damage_at in [20, 50] {
            let vfs = new_vfs(40);
            let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
            writer.append_group(&batch_group(&writer, &[10])?)?;
            assert!(writer.rotate(&WalFrame::checkpoint(0, vec![1; 16])?)?);
            if remove_older {
                writer.remove_checkpointed(1)?;
            }
            let acked = writer.append_group(&batch_group(&writer, &[10])?)?;
            assert_eq!(acked.seq_range, Some((2, 2)));
            let path = writer.active_path().to_path_buf();
            drop(writer);
            vfs.corrupt(&path, damage_at, &[0xFF])?;
            let before = file_bytes(vfs.as_ref(), &path)?;
            let checkpoint = u64::from(remove_older);
            let error = recover(vfs.process(), "boot-a", checkpoint)
                .err()
                .ok_or("expected an error, not a truncated log")?;
            assert!(
                matches!(error, WalError::Corrupt { offset: 0, .. }),
                "{error}"
            );
            assert_eq!(file_bytes(vfs.as_ref(), &path)?, before);
        }
    }
    Ok(())
}

#[test]
fn damaged_first_group_of_a_new_directory_followed_by_one_group_is_corruption() -> TestResult {
    let vfs = new_vfs(41);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    let first = writer.append_group(&batch_group(&writer, &[10])?)?;
    writer.append_group(&batch_group(&writer, &[10])?)?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    for offset in [0, first.start_offset] {
        let original = file_bytes(vfs.as_ref(), &path)?;
        vfs.corrupt(&path, offset + 20, &[0xFF])?;
        let error = recover(vfs.process(), "boot-a", 0)
            .err()
            .ok_or("expected an error, not a truncated log")?;
        assert!(error.is_corruption(), "{error}");
        vfs.corrupt(&path, 0, &original)?;
    }
    Ok(())
}

#[test]
fn a_file_that_does_not_start_with_a_checkpoint_group_is_corruption() -> TestResult {
    let vfs = new_vfs(42);
    let (_, writer, _) = recover(vfs.process(), "boot-a", 0)?;
    drop(writer);
    let path = dir().join(wal_file_name(1));
    vfs.process().open(&path, OpenMode::Append)?.set_len(0)?;
    append_raw(
        vfs.as_ref(),
        &path,
        &WalFrame::write_batch(1, 1, payload(1, 8))?.encode(0, 0, true),
    )?;
    let error = recover(vfs.process(), "boot-a", 0)
        .err()
        .ok_or("expected an error")?;
    assert!(
        matches!(&error, WalError::Corrupt { offset: 0, reason, .. } if reason.contains("checkpoint")),
        "{error}"
    );
    Ok(())
}

#[test]
fn a_file_repaired_to_nothing_gets_a_new_checkpoint_group() -> TestResult {
    let vfs = new_vfs(43);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    writer.append_group(&batch_group(&writer, &[10])?)?;
    assert!(writer.rotate(&WalFrame::checkpoint(1, vec![3; 40])?)?);
    let path = writer.active_path().to_path_buf();
    drop(writer);
    // The rotation's checkpoint frame is torn: the file is repaired to nothing.
    vfs.corrupt(&path, 60, &[0xFF])?;
    let mut recovery = WalRecovery::open(vfs.process(), dir(), config("boot-a"), 0)?;
    while recovery.next_frame()?.is_some() {}
    assert_eq!(
        recovery
            .report()
            .tail_repair
            .as_ref()
            .map(|repair| repair.repaired_len),
        Some(0)
    );
    // The writer needs a checkpoint frame to start the empty file.
    assert!(matches!(
        recovery.into_writer(&WalFrame::write_batch(2, 2, Vec::new())?),
        Err(WalError::InvalidFrame { .. })
    ));
    let (frames, mut writer, _) = recover(vfs.process(), "boot-a", 1)?;
    assert!(frames.is_empty());
    assert_eq!(writer.synced_len(), frame_len(0));
    let commit = writer.append_group(&batch_group(&writer, &[5])?)?;
    assert_eq!(commit.seq_range, Some((2, 2)));
    drop(writer);
    vfs.crash();
    let (frames, _, _) = recover(vfs.process(), "boot-b", 1)?;
    assert_eq!(seqs(&frames), vec![2]);
    Ok(())
}

#[test]
fn checksummed_frames_that_break_the_sequence_are_corruption() -> TestResult {
    let vfs = new_vfs(12);
    let (writer, commits) = three_groups(vfs.process())?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    // A complete, checksummed group with a sequence gap is never a torn write.
    let gap = WalFrame::write_batch(9, 9, Vec::new())?.encode(0, 3, true);
    append_raw(vfs.as_ref(), &path, &gap)?;
    let error = recover(vfs.process(), "boot-a", 0)
        .err()
        .ok_or("expected an error")?;
    assert!(
        matches!(&error, WalError::Corrupt { offset, .. } if *offset == commits[2].end_offset),
        "{error}"
    );

    let vfs = new_vfs(13);
    let (writer, _) = three_groups(vfs.process())?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    let wrong_group = WalFrame::write_batch(5, 5, Vec::new())?.encode(0, 7, true);
    append_raw(vfs.as_ref(), &path, &wrong_group)?;
    assert!(matches!(
        recover(vfs.process(), "boot-a", 0),
        Err(WalError::Corrupt { .. })
    ));
    Ok(())
}

#[test]
fn unsupported_format_version_is_a_typed_error() -> TestResult {
    let vfs = new_vfs(14);
    let (writer, _) = three_groups(vfs.process())?;
    let path = writer.active_path().to_path_buf();
    drop(writer);
    let mut header = WalFrame::write_batch(5, 5, Vec::new())?.encode(0, 3, true);
    header[14] = 9;
    let crc = frame::crc32c(&header[8..48]);
    header[4..8].copy_from_slice(&crc.to_le_bytes());
    append_raw(vfs.as_ref(), &path, &header)?;
    assert!(matches!(
        recover(vfs.process(), "boot-a", 0),
        Err(WalError::UnsupportedFormatVersion { version: 9, .. })
    ));
    Ok(())
}

#[test]
fn unexpected_wal_file_names_are_rejected() -> TestResult {
    let vfs = new_vfs(15);
    vfs.create_dir_all(dir())?;
    vfs.open(&dir().join("active.wal"), OpenMode::CreateNew)?;
    assert!(matches!(
        recover(vfs.process(), "boot-a", 0),
        Err(WalError::UnexpectedFile { .. })
    ));
    Ok(())
}

#[test]
fn failed_sync_rolls_back_the_group_and_the_next_group_takes_its_place() -> TestResult {
    for tear in TearMode::ALL {
        let vfs = new_vfs(16);
        let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
        let first = writer.append_group(&batch_group(&writer, &[10])?)?;
        vfs.set_plan(FaultPlan {
            fail_sync: Some(vfs.file_syncs()),
            tear,
            ..FaultPlan::default()
        });
        let failed = batch_group(&writer, &[3000, 3000])?;
        let error = writer
            .append_group(&failed)
            .err()
            .ok_or("expected an error")?;
        assert!(
            matches!(
                error,
                WalError::WriteFailed {
                    outcome: WriteOutcome::NotApplied,
                    ..
                }
            ),
            "{error}"
        );
        assert!(
            vfs.crash_points_hit()
                .contains(&CrashPoint::WalAfterRollback)
        );
        assert_eq!(writer.failure(), None);
        assert_eq!(writer.synced_len(), first.end_offset);
        assert_eq!(writer.next_seq_no(), 2);

        // The next group reuses the failed group's number and lands where it would have.
        let retry = writer.append_group(&batch_group(&writer, &[7])?)?;
        assert_eq!(retry.group_no, first.group_no + 1);
        assert_eq!(retry.start_offset, first.end_offset);
        assert_eq!(retry.seq_range, Some((2, 2)));
        drop(writer);
        vfs.crash();

        let (frames, _, _) = recover(vfs.process(), "boot-b", 0)?;
        assert_eq!(seqs(&frames), vec![1, 2], "{tear:?}");
        assert_eq!(frames[1].payload, payload(2, 7), "{tear:?}");
    }
    Ok(())
}

#[test]
fn failed_rollback_fences_the_wal_for_this_boot() -> TestResult {
    let fault = new_vfs(17);
    let flaky = FlakyVfs::wrap(fault.process());
    let vfs: Arc<dyn Vfs> = flaky.clone();
    let (_, mut writer, _) = recover(Arc::clone(&vfs), "boot-a", 0)?;
    writer.append_group(&batch_group(&writer, &[10])?)?;
    // Fail the group's sync_data and the rollback's sync_all.
    flaky.fail_next_syncs(2);
    let error = writer
        .append_group(&batch_group(&writer, &[20, 30])?)
        .err()
        .ok_or("expected an error")?;
    assert!(
        matches!(
            error,
            WalError::WriteFailed {
                outcome: WriteOutcome::Unknown { fenced: true },
                ..
            }
        ),
        "{error}"
    );
    assert_eq!(
        writer.failure(),
        Some(WriteOutcome::Unknown { fenced: true })
    );
    assert!(matches!(
        writer.append_group(&batch_group(&writer, &[1])?),
        Err(WalError::WriterFailed { .. })
    ));
    let marker = read_fence(vfs.as_ref(), dir())?.ok_or("expected a fence marker")?;
    assert_eq!(marker.boot_id, BootId::new("boot-a"));
    assert_eq!((marker.first_seq_no, marker.last_seq_no), (2, 3));
    drop(writer);

    // Same boot, in-process reopen: refused before anything is touched.
    let ops = fault.mutating_ops();
    assert!(matches!(
        recover(Arc::clone(&vfs), "boot-a", 0),
        Err(WalError::FsyncFailedSameBoot {
            first_seq_no: 2,
            last_seq_no: 3,
            ..
        })
    ));
    assert_eq!(fault.mutating_ops(), ops);

    // After a reboot the on-disk bytes are the truth; the marker is removed durably.
    fault.crash();
    let (frames, writer, report) = recover(fault.process(), "boot-b", 0)?;
    assert_eq!(
        report.stale_fence.map(|marker| marker.boot_id),
        Some(BootId::new("boot-a"))
    );
    // The failed group may or may not have reached the disk, but only as a whole.
    assert!(matches!(seqs(&frames).as_slice(), [1] | [1, 2, 3]));
    drop(writer);
    fault.crash();
    assert_eq!(read_fence(fault.as_ref(), dir())?, None);
    Ok(())
}

#[test]
fn failed_fence_reports_an_unfenced_unknown_outcome() -> TestResult {
    let fault = new_vfs(18);
    let flaky = FlakyVfs::wrap(fault.process());
    let (_, mut writer, _) = recover(flaky.clone(), "boot-a", 0)?;
    flaky.fail_next_syncs(2);
    flaky.fail_creates_named(FENCE_FILE_NAME);
    let error = writer
        .append_group(&batch_group(&writer, &[20])?)
        .err()
        .ok_or("expected an error")?;
    assert!(
        matches!(
            error,
            WalError::WriteFailed {
                outcome: WriteOutcome::Unknown { fenced: false },
                ..
            }
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn operator_can_clear_a_fence_from_this_boot() -> TestResult {
    let fault = new_vfs(19);
    let flaky = FlakyVfs::wrap(fault.process());
    let (_, mut writer, _) = recover(flaky.clone(), "boot-a", 0)?;
    flaky.fail_next_syncs(2);
    assert!(writer.append_group(&batch_group(&writer, &[20])?).is_err());
    drop(writer);
    assert!(recover(flaky.clone(), "boot-a", 0).is_err());
    assert!(clear_fence(flaky.as_ref(), dir())?);
    let (frames, _, report) = recover(flaky.clone(), "boot-a", 0)?;
    assert_eq!(report.stale_fence, None);
    // In this boot the page cache may still show the failed group; either way it is whole.
    assert!(matches!(seqs(&frames).as_slice(), [] | [1]));
    Ok(())
}

#[test]
fn unreadable_fence_marker_refuses_the_open() -> TestResult {
    let vfs = new_vfs(20);
    recover(vfs.process(), "boot-a", 0)?;
    let marker = dir().join(FENCE_FILE_NAME);
    vfs.open(&marker, OpenMode::CreateNew)?
        .append(&[IoSlice::new(b"logpose wal fence v1\nboot_")])?;
    assert!(matches!(
        recover(vfs.process(), "boot-b", 0),
        Err(WalError::FenceUnreadable { .. })
    ));
    Ok(())
}

#[test]
fn rotation_makes_the_new_file_durable_before_switching() -> TestResult {
    let vfs = new_vfs(21);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    writer.append_group(&batch_group(&writer, &[10, 10])?)?;
    let ops = vfs.mutating_ops();
    vfs.set_plan(FaultPlan {
        crash_at: Some(CrashPoint::WalAfterRotateCreate),
        ..FaultPlan::default()
    });
    let checkpoint = WalFrame::checkpoint(2, b"manifest 0".to_vec())?;
    assert!(writer.rotate(&checkpoint).is_err());
    // Create, append, sync, directory sync: in that order, before the crash point.
    assert_eq!(vfs.mutating_ops() - ops, 4);
    drop(writer);
    vfs.crash();

    let (frames, mut writer, report) = recover(vfs.process(), "boot-b", 0)?;
    assert_eq!(report.files.len(), 2);
    assert_eq!(writer.active_path(), dir().join(wal_file_name(3)));
    assert_eq!(seqs(&frames), vec![1, 2]);
    let marker = frames.last().ok_or("expected the checkpoint frame")?;
    assert_eq!(marker.header.kind, PayloadKind::Checkpoint);
    assert_eq!(marker.payload, b"manifest 0");
    assert_eq!((marker.header.group_no, marker.header.group_end), (2, true));
    let commit = writer.append_group(&batch_group(&writer, &[5])?)?;
    assert_eq!(commit.group_no, 3);
    assert_eq!(commit.seq_range, Some((3, 3)));
    Ok(())
}

#[test]
fn rotation_without_new_data_is_a_no_op() -> TestResult {
    let vfs = new_vfs(22);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    let checkpoint = WalFrame::checkpoint(0, Vec::new())?;
    assert!(!writer.rotate(&checkpoint)?);
    writer.append_group(&batch_group(&writer, &[1])?)?;
    assert!(writer.rotate(&checkpoint)?);
    assert!(!writer.rotate(&checkpoint)?);
    assert_eq!(writer.files().len(), 2);
    assert!(matches!(
        writer.rotate(&WalFrame::write_batch(2, 2, Vec::new())?),
        Err(WalError::InvalidFrame { .. })
    ));
    Ok(())
}

#[test]
fn should_rotate_once_the_file_reaches_its_size() -> TestResult {
    let vfs = new_vfs(23);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    assert!(!writer.should_rotate());
    writer.append_group(&batch_group(&writer, &[1000])?)?;
    assert!(writer.should_rotate());
    writer.rotate(&WalFrame::checkpoint(0, Vec::new())?)?;
    assert!(!writer.should_rotate());
    Ok(())
}

/// Write data into four files: 1..=2, 3..=4, 5..=6 and 7 (active), with checkpoint frames.
fn four_files(vfs: Arc<dyn Vfs>) -> Result<WalWriter, WalError> {
    let (_, mut writer, _) = recover(vfs, "boot-a", 0)?;
    for file in 0..4 {
        let group = batch_group(&writer, &[10, 20])?;
        if file < 3 {
            writer.append_group(&group)?;
            let checkpoint = writer.next_seq_no() - 1;
            writer.rotate(&WalFrame::checkpoint(checkpoint, Vec::new())?)?;
        } else {
            writer.append_group(&group[..1])?;
        }
    }
    Ok(writer)
}

#[test]
fn replay_spans_files_and_skips_checkpointed_ones() -> TestResult {
    let vfs = new_vfs(24);
    let writer = four_files(vfs.process())?;
    assert_eq!(writer.files().len(), 4);
    drop(writer);
    vfs.crash();

    let (frames, _, report) = recover(vfs.process(), "boot-b", 0)?;
    assert_eq!(seqs(&frames), (1..=7).collect::<Vec<_>>());
    assert!(report.skipped_files.is_empty());

    // Checkpoint 4: the first two files hold only operations at or below it.
    let (frames, mut writer, report) = recover(vfs.process(), "boot-b", 4)?;
    assert_eq!(seqs(&frames), vec![5, 6, 7]);
    assert_eq!(report.skipped_files.len(), 2);
    // Checkpoint frames at or below the checkpoint are skipped too; the one naming 6 is not.
    let markers: Vec<SeqNo> = frames
        .iter()
        .filter(|frame| !frame.header.is_data())
        .map(|frame| frame.header.first_seq_no)
        .collect();
    assert_eq!(markers, vec![6]);

    let removed = writer.remove_checkpointed(4)?;
    assert_eq!(
        removed,
        vec![dir().join(wal_file_name(1)), dir().join(wal_file_name(3))]
    );
    assert_eq!(writer.files().len(), 2);
    drop(writer);
    vfs.crash();
    let (frames, writer, report) = recover(vfs.process(), "boot-c", 4)?;
    assert_eq!(report.files.len(), 2);
    assert_eq!(seqs(&frames), vec![5, 6, 7]);
    assert_eq!(writer.next_seq_no(), 8);
    Ok(())
}

#[test]
fn checkpointed_files_are_not_read() -> TestResult {
    let vfs = new_vfs(25);
    drop(four_files(vfs.process())?);
    vfs.corrupt(&dir().join(wal_file_name(1)), 3, b"garbage")?;
    assert!(matches!(
        recover(vfs.process(), "boot-a", 0),
        Err(WalError::Corrupt { .. })
    ));
    let (frames, _, _) = recover(vfs.process(), "boot-a", 2)?;
    assert_eq!(seqs(&frames), vec![3, 4, 5, 6, 7]);
    Ok(())
}

#[test]
fn a_bad_frame_in_an_older_file_is_corruption_even_at_its_end() -> TestResult {
    let vfs = new_vfs(26);
    drop(four_files(vfs.process())?);
    // Garbage appended to a rolled file looks like a torn tail, but only the highest-named
    // file can have one.
    append_raw(vfs.as_ref(), &dir().join(wal_file_name(3)), &[1; 30])?;
    let error = recover(vfs.process(), "boot-a", 0)
        .err()
        .ok_or("expected an error")?;
    assert!(
        matches!(&error, WalError::Corrupt { file, .. } if file.ends_with(wal_file_name(3))),
        "{error}"
    );
    Ok(())
}

#[test]
fn a_missing_file_in_the_middle_is_corruption() -> TestResult {
    let vfs = new_vfs(27);
    drop(four_files(vfs.process())?);
    vfs.remove_file(&dir().join(wal_file_name(3)))?;
    assert!(matches!(
        recover(vfs.process(), "boot-a", 0),
        Err(WalError::Corrupt { .. })
    ));
    // A missing oldest needed file leaves a gap after the checkpoint.
    let vfs = new_vfs(28);
    drop(four_files(vfs.process())?);
    vfs.remove_file(&dir().join(wal_file_name(1)))?;
    assert!(matches!(
        recover(vfs.process(), "boot-a", 0),
        Err(WalError::Corrupt { .. })
    ));
    assert!(recover(vfs.process(), "boot-a", 2).is_ok());
    Ok(())
}

#[test]
fn a_frame_straddling_the_checkpoint_is_corruption() -> TestResult {
    let vfs = new_vfs(29);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    writer.append_group(&[WalFrame::write_batch(1, 5, Vec::new())?])?;
    drop(writer);
    assert!(matches!(
        recover(vfs.process(), "boot-a", 3),
        Err(WalError::Corrupt { .. })
    ));
    assert!(recover(vfs.process(), "boot-a", 5).is_ok());
    Ok(())
}

#[test]
fn a_checkpoint_beyond_the_end_of_the_log_is_corruption() -> TestResult {
    let vfs = new_vfs(30);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    writer.append_group(&batch_group(&writer, &[1, 1])?)?;
    drop(writer);
    assert!(matches!(
        recover(vfs.process(), "boot-a", 9),
        Err(WalError::Corrupt { .. })
    ));
    Ok(())
}

#[test]
fn failed_rotation_sync_fails_the_writer_and_leaves_a_recoverable_log() -> TestResult {
    let vfs = new_vfs(31);
    let (_, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    writer.append_group(&batch_group(&writer, &[10])?)?;
    vfs.set_plan(FaultPlan {
        fail_sync: Some(vfs.file_syncs()),
        tear: TearMode::KeepRandomPrefix,
        ..FaultPlan::default()
    });
    let error = writer
        .rotate(&WalFrame::checkpoint(1, Vec::new())?)
        .err()
        .ok_or("expected an error")?;
    assert!(matches!(
        error,
        WalError::WriteFailed {
            outcome: WriteOutcome::NotApplied,
            ..
        }
    ));
    assert!(matches!(
        writer.append_group(&batch_group(&writer, &[1])?),
        Err(WalError::WriterFailed { .. })
    ));
    drop(writer);

    // In-process reopen: the new file was rolled back to empty and becomes the active file.
    let (frames, mut writer, _) = recover(vfs.process(), "boot-a", 0)?;
    assert_eq!(seqs(&frames), vec![1]);
    assert_eq!(writer.active_path(), dir().join(wal_file_name(2)));
    writer.append_group(&batch_group(&writer, &[1])?)?;
    drop(writer);
    vfs.crash();
    let (frames, _, _) = recover(vfs.process(), "boot-b", 0)?;
    assert_eq!(seqs(&frames), vec![1, 2]);
    Ok(())
}

#[test]
fn failed_rotation_create_keeps_the_writer_usable() -> TestResult {
    let fault = new_vfs(32);
    let flaky = FlakyVfs::wrap(fault.process());
    let (_, mut writer, _) = recover(flaky.clone(), "boot-a", 0)?;
    writer.append_group(&batch_group(&writer, &[10])?)?;
    flaky.fail_creates_named(".wal");
    assert!(matches!(
        writer.rotate(&WalFrame::checkpoint(0, Vec::new())?),
        Err(WalError::Io { .. })
    ));
    assert_eq!(writer.failure(), None);
    writer.append_group(&batch_group(&writer, &[10])?)?;
    Ok(())
}

#[test]
fn failed_rotation_directory_sync_fails_the_writer() -> TestResult {
    let vfs = new_vfs(33);
    let flaky = FlakyVfs::wrap(vfs.process());
    let (_, mut writer, _) = recover(flaky.clone(), "boot-a", 0)?;
    writer.append_group(&batch_group(&writer, &[10])?)?;
    flaky.fail_next_dir_sync();
    assert!(matches!(
        writer.rotate(&WalFrame::checkpoint(0, Vec::new())?),
        Err(WalError::WriteFailed {
            outcome: WriteOutcome::NotApplied,
            ..
        })
    ));
    assert_eq!(writer.failure(), Some(WriteOutcome::NotApplied));
    drop(writer);
    vfs.crash();
    let (frames, _, _) = recover(vfs.process(), "boot-b", 0)?;
    assert_eq!(seqs(&frames), vec![1]);
    Ok(())
}

/// A [`Vfs`] wrapper that fails chosen file syncs and file creations, to reach failure
/// combinations one [`FaultPlan`] cannot express (a failed sync whose rollback also fails).
struct FlakyVfs {
    inner: Arc<dyn Vfs>,
    state: Arc<Mutex<Flaky>>,
}

#[derive(Default)]
struct Flaky {
    fail_syncs: u32,
    fail_dir_sync: bool,
    fail_create_suffix: Option<String>,
}

impl FlakyVfs {
    fn wrap(inner: Arc<dyn Vfs>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            state: Arc::default(),
        })
    }

    fn fail_next_syncs(&self, count: u32) {
        if let Ok(mut state) = self.state.lock() {
            state.fail_syncs = count;
        }
    }

    fn fail_next_dir_sync(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.fail_dir_sync = true;
        }
    }

    fn fail_creates_named(&self, suffix: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.fail_create_suffix = Some(suffix.to_owned());
        }
    }
}

impl Vfs for FlakyVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        if mode == OpenMode::CreateNew
            && let Ok(state) = self.state.lock()
            && let Some(suffix) = &state.fail_create_suffix
            && path.to_string_lossy().contains(suffix.as_str())
        {
            return Err(io::Error::from_raw_os_error(28));
        }
        let inner = self.inner.open(path, mode)?;
        Ok(Arc::new(FlakyFile {
            inner,
            state: Arc::clone(&self.state),
        }))
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        self.inner.list(dir)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let fail = self
            .state
            .lock()
            .is_ok_and(|mut state| std::mem::take(&mut state.fail_dir_sync));
        if fail {
            return Err(io::Error::from_raw_os_error(5));
        }
        self.inner.sync_dir(dir)
    }
    fn try_lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        self.inner.try_lock_exclusive(path)
    }
    fn crash_point(&self, point: CrashPoint) -> io::Result<()> {
        self.inner.crash_point(point)
    }
}

struct FlakyFile {
    inner: Arc<dyn VfsFile>,
    state: Arc<Mutex<Flaky>>,
}

impl FlakyFile {
    fn sync(&self, sync: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let fail = self.state.lock().is_ok_and(|mut state| {
            let fail = state.fail_syncs > 0;
            state.fail_syncs = state.fail_syncs.saturating_sub(1);
            fail
        });
        if fail {
            Err(io::Error::from_raw_os_error(5))
        } else {
            sync()
        }
    }
}

impl VfsFile for FlakyFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn append(&self, bufs: &[IoSlice<'_>]) -> io::Result<u64> {
        self.inner.append(bufs)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.sync(|| self.inner.sync_data())
    }
    fn sync_all(&self) -> io::Result<()> {
        self.sync(|| self.inner.sync_all())
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
}
