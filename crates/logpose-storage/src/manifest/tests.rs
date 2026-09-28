//! Manifest v2 codec and `CURRENT` protocol tests on `FaultVfs`.

use super::*;
use crate::test_support::ControlledVfs;
use crate::test_support::vector_schema;
use logpose_types::{CollectionId, DistanceMetric, schema::CollectionSchema};
use logpose_vfs::{FaultPlan, FaultVfs, TearMode};
use uuid::Uuid;

const DIR: &str = "/c";

fn schema() -> CollectionSchema {
    vector_schema(2, DistanceMetric::Dot)
}

fn collection_id() -> CollectionId {
    CollectionId(Uuid::from_u128(0x1234))
}

fn segment(unit: u32) -> ManifestSegment {
    ManifestSegment {
        unit: UnitId(unit),
        file_len: 100 + u64::from(unit),
        footer_crc: 0xdead_beef,
        row_count: 4,
        schema_version: 3,
        min_seq_no: 1,
        max_seq_no: 9,
        origin: SegmentOrigin::Compaction {
            inputs: vec![UnitId(0), UnitId(1)],
        },
        tier: 1,
        dv: Some(DvRef {
            generation: 5,
            cardinality: 1,
            covered_seq_no: 9,
        }),
        index: Some(crate::manifest::IndexRef {
            unit: UnitId(unit + 7),
            file_len: 512,
            footer_crc: 0x0bad_cafe,
        }),
        vectors: vec![VectorSummary {
            field_id: 1,
            has_graph: false,
            has_sq8: true,
            non_null: 4,
        }],
        zones: vec![FieldZone {
            field_id: 2,
            min: Some(vec![1, 2]),
            max: None,
            null_count: 1,
            distinct_estimate: 3,
        }],
    }
}

fn manifest(generation: u64) -> Manifest {
    Manifest {
        generation,
        checkpoint_seq_no: generation * 10,
        next_unit_id: 8,
        next_dv_gen: 6,
        segments: vec![segment(2), segment(5)],
        ..Manifest::empty(collection_id(), schema())
    }
    .with_totals()
}

/// A collection directory whose generation 0 is durably published.
fn published_dir(vfs: &dyn Vfs) {
    vfs.create_dir_all(&manifests_dir(Path::new(DIR)))
        .expect("create manifests/");
    vfs.sync_dir(Path::new("/")).expect("sync /");
    vfs.sync_dir(Path::new(DIR)).expect("sync dir");
    publish_manifest(vfs, Path::new(DIR), &manifest(0)).expect("publish generation 0");
}

#[test]
fn a_manifest_round_trips_through_its_file_format() {
    let manifest = manifest(7);
    assert_eq!(manifest.totals.rows, 8);
    assert_eq!(manifest.totals.deleted_rows, 2);
    assert_eq!(manifest.totals.segment_bytes, 207);
    let bytes = manifest.encode().expect("encode");
    assert_eq!(&bytes[..8], b"LPMANIF2");
    assert_eq!(le_u64(&bytes[8..16]), 7);
    assert_eq!(le_u64(&bytes[16..24]) as usize, bytes.len() - 32);
    let decoded = Manifest::decode(&bytes, Path::new("/m")).expect("decode");
    assert_eq!(decoded, manifest);
    let inspected = decoded.inspect_json();
    assert_eq!(inspected["segments"][1]["segment_id"], "00000005");
    assert_eq!(inspected["segments"][0]["deleted_rows"], 1);
    assert_eq!(inspected["totals"]["rows"], 8);
    assert_eq!(decoded.units().collect::<Vec<_>>(), [UnitId(2), UnitId(5)]);
}

#[test]
fn every_flipped_byte_of_a_manifest_file_is_detected_as_manifest_corruption() {
    let bytes = manifest(3).encode().expect("encode");
    for index in 0..bytes.len() {
        let mut damaged = bytes.clone();
        damaged[index] ^= 0x40;
        let error = Manifest::decode(&damaged, Path::new("/m"))
            .expect_err("a flipped byte must be detected");
        assert!(
            matches!(
                error,
                LogPoseError::Corrupt {
                    kind: CorruptionKind::Manifest,
                    ..
                }
            ),
            "byte {index}: {error}"
        );
    }
    for len in [0, 31, 32, bytes.len() - 1] {
        assert!(Manifest::decode(&bytes[..len], Path::new("/m")).is_err());
    }
    let mut longer = bytes.clone();
    longer.push(0);
    assert!(Manifest::decode(&longer, Path::new("/m")).is_err());
}

#[test]
fn a_manifest_whose_header_and_payload_disagree_is_corrupt() {
    let mut other = manifest(4);
    other.generation = 5;
    let mut bytes = other.encode().expect("encode");
    // Rewrite the header to claim generation 4, with valid checksums.
    bytes[8..16].copy_from_slice(&4_u64.to_le_bytes());
    let header_crc = crc32c::crc32c(&bytes[..28]);
    bytes[28..32].copy_from_slice(&header_crc.to_le_bytes());
    let error = Manifest::decode(&bytes, Path::new("/m")).expect_err("mismatch");
    assert!(error.to_string().contains("generation"), "{error}");

    let mut unordered = manifest(4);
    unordered.segments.reverse();
    let bytes = unordered.encode().expect("encode");
    assert!(Manifest::decode(&bytes, Path::new("/m")).is_err());
}

#[test]
fn current_must_be_a_21_byte_generation_pointer() {
    let vfs = FaultVfs::new(1);
    vfs.create_dir_all(Path::new(DIR)).expect("dir");
    let write = |contents: &[u8]| {
        let path = Path::new(DIR).join(CURRENT_FILE);
        let _ = vfs.remove_file(&path);
        let file = vfs.open(&path, OpenMode::CreateNew).expect("create");
        file.append(&[IoSlice::new(contents)]).expect("append");
    };
    write(b"00000000000000000042\n");
    assert_eq!(
        read_current(vfs.as_ref(), Path::new(DIR)).expect("parse"),
        42
    );
    for bad in [
        &b"42"[..],
        b"42\n",
        b"00000000000000000042",
        b"0000000000000000004x\n",
        b"000000000000000000042\n",
        b"",
    ] {
        write(bad);
        let error = read_current(vfs.as_ref(), Path::new(DIR)).expect_err("must be refused");
        assert!(
            matches!(
                error,
                LogPoseError::Corrupt {
                    kind: CorruptionKind::Manifest,
                    ..
                }
            ),
            "{bad:?}: {error}"
        );
    }
}

#[test]
fn a_publish_names_the_new_generation_durably() {
    let vfs = FaultVfs::new(2);
    published_dir(vfs.as_ref());
    publish_manifest(vfs.as_ref(), Path::new(DIR), &manifest(1)).expect("publish");
    vfs.crash();
    assert_eq!(
        read_current(vfs.as_ref(), Path::new(DIR)).expect("CURRENT"),
        1
    );
    assert_eq!(
        load_manifest(vfs.as_ref(), Path::new(DIR), 1).expect("load"),
        manifest(1)
    );
    let error = load_manifest(vfs.as_ref(), Path::new(DIR), 9).expect_err("missing");
    assert!(error.to_string().contains("does not exist"), "{error}");
}

/// Exhaustive: a crash at every mutating operation of a publish, under every tear mode,
/// leaves `CURRENT` naming either the old or the new generation, and whichever it names loads
/// and verifies. A publish that returned `Ok` is always the one named, and one that failed
/// before the rename is never named.
#[test]
fn a_crash_at_every_op_of_a_publish_leaves_a_complete_old_or_new_manifest() {
    let clean = FaultVfs::new(3);
    published_dir(clean.as_ref());
    let before = clean.mutating_ops();
    publish_manifest(clean.as_ref(), Path::new(DIR), &manifest(1)).expect("clean publish");
    let ops = clean.mutating_ops() - before;
    assert!(
        ops >= 7,
        "a publish is at least seven mutating ops, got {ops}"
    );

    for tear in TearMode::ALL {
        for seed in 0..4 {
            for k in 0..=ops {
                let vfs = FaultVfs::new(seed * 1000 + k);
                published_dir(vfs.as_ref());
                vfs.set_plan(FaultPlan {
                    crash_after_ops: Some(vfs.mutating_ops() + k),
                    tear,
                    ..FaultPlan::default()
                });
                let result = publish_manifest(vfs.as_ref(), Path::new(DIR), &manifest(1));
                vfs.crash();
                let context = format!("tear {tear:?}, seed {seed}, crash after {k} ops");
                let current = read_current(vfs.as_ref(), Path::new(DIR))
                    .map_err(|error| format!("{context}: {error}"))
                    .expect("CURRENT is readable");
                let loaded = load_manifest(vfs.as_ref(), Path::new(DIR), current)
                    .map_err(|error| format!("{context}: {error}"))
                    .expect("the named manifest loads");
                assert_eq!(loaded, manifest(current), "{context}");
                match result {
                    Ok(()) => assert_eq!(current, 1, "{context}: a durable publish is named"),
                    Err(failure) if !failure.current_unknown => {
                        assert_eq!(
                            current, 0,
                            "{context}: a publish that failed before the rename"
                        )
                    }
                    Err(_) => assert!(current <= 1, "{context}"),
                }
            }
        }
    }
}

/// The named crash points of the publish protocol, against the flush crash table: before its
/// step 5, `CURRENT` is old or new (old when nothing unsynced survives); after it, new.
#[test]
fn named_crash_points_of_a_publish_leave_the_generation_the_crash_table_names() {
    let cases = [
        (CrashPoint::ManifestAfterFileSync, Some(0)),
        (CrashPoint::ManifestAfterDirSync, Some(0)),
        (CrashPoint::CurrentAfterTempSync, Some(0)),
        (CrashPoint::CurrentAfterRename, None),
        (CrashPoint::CurrentAfterDirSync, Some(1)),
    ];
    for (point, expected) in cases {
        for tear in TearMode::ALL {
            let vfs = FaultVfs::new(11);
            published_dir(vfs.as_ref());
            vfs.set_plan(FaultPlan {
                crash_at: Some(point),
                tear,
                ..FaultPlan::default()
            });
            publish_manifest(vfs.as_ref(), Path::new(DIR), &manifest(1))
                .expect_err("the crash interrupts the publish");
            assert!(vfs.crash_points_hit().contains(&point));
            vfs.crash();
            let current = read_current(vfs.as_ref(), Path::new(DIR)).expect("CURRENT");
            match expected {
                Some(generation) => assert_eq!(current, generation, "{point:?} {tear:?}"),
                None if tear == TearMode::DropUnsynced => {
                    assert_eq!(current, 0, "{point:?}: the rename was never synced")
                }
                None => assert!(current <= 1, "{point:?} {tear:?}"),
            }
            load_manifest(vfs.as_ref(), Path::new(DIR), current).expect("named manifest loads");
        }
    }
}

/// A failure in steps 1 to 3 leaves `CURRENT` unchanged; a failure of the rename or the
/// directory sync after it leaves it unknown.
#[test]
fn a_failed_step_reports_whether_current_may_have_changed() {
    type Inject = fn(&ControlledVfs);
    let cases: [(&str, Inject, bool); 5] = [
        (
            "manifest file sync",
            |vfs| vfs.fail_file_syncs_containing(".mf", 1),
            false,
        ),
        (
            "manifests/ sync",
            |vfs| vfs.fail_dir_syncs(&manifests_dir(Path::new(DIR)), 1),
            false,
        ),
        (
            "CURRENT.tmp sync",
            |vfs| vfs.fail_file_syncs_containing(CURRENT_TEMP_FILE, 1),
            false,
        ),
        (
            "CURRENT rename",
            |vfs| vfs.fail_renames_to(CURRENT_FILE, 1),
            true,
        ),
        (
            "collection directory sync",
            |vfs| vfs.fail_dir_syncs(Path::new(DIR), 1),
            true,
        ),
    ];
    for (step, inject, unknown) in cases {
        let fault = FaultVfs::new(5);
        let vfs = ControlledVfs::wrap(fault.process());
        published_dir(vfs.as_ref());
        inject(&vfs);
        let failure = publish_manifest(vfs.as_ref(), Path::new(DIR), &manifest(1))
            .expect_err("the injected failure fails the publish");
        assert_eq!(
            failure.current_unknown, unknown,
            "{step}: {}",
            failure.error
        );
        if !unknown {
            assert_eq!(
                read_current(vfs.as_ref(), Path::new(DIR)).expect("CURRENT"),
                0,
                "{step}"
            );
        }
        // Burned: generation 1 is never written again; the next attempt uses 2.
        let mut retry = manifest(2);
        retry.generation = 2;
        publish_manifest(vfs.as_ref(), Path::new(DIR), &retry).expect("retry publishes");
        assert_eq!(
            read_current(vfs.as_ref(), Path::new(DIR)).expect("CURRENT"),
            2
        );
    }
}

#[test]
fn a_generation_is_never_written_twice() {
    let vfs = FaultVfs::new(6);
    published_dir(vfs.as_ref());
    let failure = publish_manifest(vfs.as_ref(), Path::new(DIR), &manifest(0))
        .expect_err("generation 0 exists");
    assert!(!failure.current_unknown);
    assert_eq!(
        read_current(vfs.as_ref(), Path::new(DIR)).expect("CURRENT"),
        0
    );
    assert_eq!(
        load_manifest(vfs.as_ref(), Path::new(DIR), 0).expect("load"),
        manifest(0)
    );
}

#[test]
fn manifest_file_names_parse_only_their_own_format() {
    assert_eq!(manifest_file_name(12), "00000000000000000012.mf");
    assert_eq!(
        parse_manifest_file_name("00000000000000000012.mf"),
        Some(12)
    );
    for foreign in [
        "12.mf",
        "00000000000000000012.json",
        "0000000000000000001x.mf",
    ] {
        assert_eq!(parse_manifest_file_name(foreign), None, "{foreign}");
    }
}
