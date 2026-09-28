//! Snapshot token tests: the token codec, the registry's expiry and limits, and engine-level
//! reads through tokens with a manual clock.

use super::*;
use crate::{
    CreateCollectionRequest, Engine, EngineConfig, LocalStorageEngine, ManualClock, StorageEngine,
    handle::CollectionMeta,
    manifest::Manifest,
    test_support::put,
    version::{DeltaLog, DeltaOp, DeltaRecord},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    DeleteRecord, DistanceMetric, RecordId, VisibleRecord, WriteOperation, legacy::legacy_schema,
};
use logpose_vfs::FaultVfs;
use logpose_wal::{BootId, codec::WirePk};
use std::path::PathBuf;

const TTL: Duration = Duration::from_secs(10);

fn config(max_per_collection: usize) -> TokenConfig {
    TokenConfig {
        ttl: TTL,
        max_per_collection,
        memory_limit: u64::MAX,
        reaper_interval: Duration::from_secs(3600),
    }
}

fn test_meta(name: &str) -> Arc<CollectionMeta> {
    let descriptor = CollectionDescriptor::new_in_database(
        "default",
        name,
        2,
        DistanceMetric::Dot,
        PathBuf::from("/c"),
    );
    Arc::new(CollectionMeta::new(descriptor, None))
}

/// Batches of one delete each for `seq_nos`.
fn delta(seq_nos: impl IntoIterator<Item = SeqNo>) -> DeltaLog {
    let mut log = DeltaLog::default();
    for seq_no in seq_nos {
        log.append(vec![DeltaRecord {
            seq_no,
            op: DeltaOp::Delete(WirePk::String(format!("key-{seq_no}"))),
        }]);
    }
    log
}

fn version(
    meta: &Arc<CollectionMeta>,
    id: u64,
    generation: u64,
    checkpoint: SeqNo,
    delta: DeltaLog,
) -> Arc<Version> {
    let schema = legacy_schema(2, DistanceMetric::Dot).expect("schema");
    let manifest = Manifest {
        generation,
        checkpoint_seq_no: checkpoint,
        ..Manifest::empty(meta.id.clone(), schema.clone())
    };
    Arc::new(Version::build(
        VersionId(id),
        Arc::clone(meta),
        Arc::new(schema),
        Arc::new(manifest),
        delta,
        Arc::from(Vec::new()),
    ))
}

fn new_registry(meta: &Arc<CollectionMeta>) -> TokenRegistry {
    TokenRegistry::new(meta.id.clone(), meta.descriptor.lookup_name())
}

fn is_expired(result: Result<Arc<Version>>) -> bool {
    matches!(result, Err(LogPoseError::SnapshotExpired { .. }))
}

#[test]
fn a_token_round_trips_through_its_text_and_rejects_tampering() {
    let token = SnapshotToken {
        collection_id: CollectionId(Uuid::from_u128(0xfeed_beef)),
        version_id: VersionId(42),
        nonce: 0x0123_4567_89ab_cdef,
    };
    let text = token.to_string();
    assert_eq!(text.len(), 48, "36 bytes are 48 base64url characters");
    assert!(
        text.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    );
    assert_eq!(text.parse::<SnapshotToken>(), Ok(token.clone()));
    for index in 0..text.len() {
        let mut tampered = text.clone().into_bytes();
        tampered[index] = if tampered[index] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).expect("ascii");
        assert_eq!(
            tampered.parse::<SnapshotToken>(),
            Err(InvalidSnapshotToken),
            "character {index}"
        );
    }
    for foreign in ["", "abc", &text[..47], &format!("{text}AAAA"), "!!!!"] {
        assert!(foreign.parse::<SnapshotToken>().is_err(), "{foreign}");
    }
}

#[test]
fn base64url_matches_the_rfc_4648_vectors() {
    // RFC 4648 section 10: the prefixes of "foobar".
    let foobar = b"foobar";
    let encodings = ["", "Zg", "Zm8", "Zm9v", "Zm9vYg", "Zm9vYmE", "Zm9vYmFy"];
    for (len, encoded) in encodings.into_iter().enumerate() {
        let plain = &foobar[..len];
        assert_eq!(base64url_encode(plain), encoded);
        assert_eq!(base64url_decode(encoded).as_deref(), Some(plain));
    }
    // The URL-safe alphabet.
    assert_eq!(base64url_encode(&[0xfb, 0xff]), "-_8");
    assert_eq!(base64url_decode("-_8").as_deref(), Some(&[0xfb, 0xff][..]));
    assert_eq!(base64url_decode("Zh"), None, "non-canonical trailing bits");
    assert_eq!(base64url_decode("Z"), None);
}

#[test]
fn expiry_slides_with_every_use_and_an_unused_token_expires() {
    let meta = test_meta("slide");
    let registry = new_registry(&meta);
    let pinned = version(&meta, 3, 1, 0, delta(1..=2));
    let token = registry
        .pin(Arc::clone(&pinned), Duration::ZERO, &config(4), false)
        .expect("pin");
    let mut now = Duration::ZERO;
    for _ in 0..5 {
        now += TTL - Duration::from_secs(1);
        let resolved = registry.resolve(&token, now, TTL).expect("still pinned");
        assert!(Arc::ptr_eq(&resolved, &pinned));
    }
    now += TTL;
    assert!(is_expired(registry.resolve(&token, now, TTL)));
    assert_eq!(registry.len(), 0, "the expired pin is dropped on use");
}

#[test]
fn released_unknown_and_foreign_tokens_are_expired() {
    let meta = test_meta("release");
    let registry = new_registry(&meta);
    let token = registry
        .pin(
            version(&meta, 1, 0, 0, delta([])),
            Duration::ZERO,
            &config(4),
            false,
        )
        .expect("pin");
    assert!(registry.release(&token));
    assert!(!registry.release(&token));
    assert!(is_expired(registry.resolve(&token, Duration::ZERO, TTL)));

    let other = test_meta("other");
    let foreign = new_registry(&other)
        .pin(
            version(&other, 1, 0, 0, delta([])),
            Duration::ZERO,
            &config(4),
            false,
        )
        .expect("pin");
    let error = registry
        .resolve(&foreign, Duration::ZERO, TTL)
        .expect_err("a token of another collection");
    assert!(error.to_string().contains("another collection"), "{error}");
}

#[test]
fn the_per_collection_limit_refuses_new_pins_until_one_expires() {
    let meta = test_meta("limit");
    let registry = new_registry(&meta);
    let config = config(2);
    for id in 0..2 {
        registry
            .pin(
                version(&meta, id, 0, 0, delta([])),
                Duration::ZERO,
                &config,
                false,
            )
            .expect("under the limit");
    }
    let error = registry
        .pin(
            version(&meta, 5, 0, 0, delta([])),
            Duration::ZERO,
            &config,
            false,
        )
        .expect_err("over the limit");
    assert!(
        matches!(error, LogPoseError::TooManySnapshots { .. }),
        "{error}"
    );
    // Expired pins never count against the limit.
    registry
        .pin(version(&meta, 6, 0, 0, delta([])), TTL, &config, false)
        .expect("the two expired pins are dropped first");
    assert_eq!(registry.len(), 1);
    let error = registry
        .pin(version(&meta, 7, 0, 0, delta([])), TTL, &config, true)
        .expect_err("over the memory limit");
    assert!(error.to_string().contains("pinned-memory limit"), "{error}");
}

#[test]
fn reaping_drops_exactly_the_expired_pins() {
    let meta = test_meta("reap");
    let registry = new_registry(&meta);
    let config = config(8);
    let old = registry
        .pin(
            version(&meta, 1, 0, 0, delta([])),
            Duration::ZERO,
            &config,
            false,
        )
        .expect("pin");
    let young = registry
        .pin(
            version(&meta, 2, 0, 0, delta([])),
            Duration::from_secs(5),
            &config,
            false,
        )
        .expect("pin");
    assert!(registry.reap(TTL - Duration::from_secs(1)).is_empty());
    let reaped = registry.reap(TTL);
    assert_eq!(reaped.len(), 1);
    assert_eq!(reaped[0].id, VersionId(1));
    assert!(is_expired(registry.resolve(&old, TTL, TTL)));
    registry
        .resolve(&young, TTL, TTL)
        .expect("young is still pinned");
}

#[test]
fn an_exact_snapshot_finds_the_pinned_version_that_covers_it() {
    let meta = test_meta("find");
    let registry = new_registry(&meta);
    let config = config(8);
    registry
        .pin(
            version(&meta, 1, 2, 4, delta(5..=9)),
            Duration::ZERO,
            &config,
            false,
        )
        .expect("pin");
    let snapshot = |manifest_generation, visible_seq_no| Snapshot {
        manifest_generation,
        visible_seq_no,
    };
    let found = registry
        .find(&snapshot(2, 7), Duration::ZERO, TTL)
        .expect("covered");
    assert_eq!(found.id, VersionId(1));
    for missing in [
        snapshot(2, 3),
        snapshot(2, 10),
        snapshot(1, 7),
        snapshot(3, 7),
    ] {
        assert!(
            registry.find(&missing, Duration::ZERO, TTL).is_none(),
            "{missing:?}"
        );
    }
    assert!(
        registry.find(&snapshot(2, 7), TTL, TTL).is_none(),
        "expired"
    );
}

/// Retired bytes count each batch at or below the current checkpoint once, however many pins
/// share it, and never count what the current version still holds.
#[test]
fn retired_bytes_count_each_shared_batch_once() {
    let meta = test_meta("retired");
    let full = delta(1..=20);
    let bytes = |after, through| full.bytes_in(after, through);
    // Two pins over checkpoint 0 (prefixes of one another) and one over checkpoint 8.
    let a = version(&meta, 1, 0, 0, full.after(0).clone());
    let prefix = {
        let mut log = DeltaLog::default();
        for batch in full.batches().take(6) {
            log.append(batch.to_vec());
        }
        log
    };
    let b = version(&meta, 2, 0, 0, prefix);
    let c = version(&meta, 3, 1, 8, full.after(8));
    assert_eq!(retired_bytes(&[], 20), 0);
    assert_eq!(
        retired_bytes(&[Arc::clone(&b)], 0),
        0,
        "nothing is retired yet"
    );
    assert_eq!(retired_bytes(&[Arc::clone(&b)], 12), bytes(0, 6));
    assert_eq!(
        retired_bytes(&[Arc::clone(&a), Arc::clone(&b), Arc::clone(&c)], 12),
        bytes(0, 12),
        "the union of (0, 12] and (8, 12]"
    );
    assert_eq!(retired_bytes(&[Arc::clone(&c)], 12), bytes(8, 12));
    assert_eq!(retired_bytes(&[a, b, c], 20), bytes(0, 20));
}

// Engine-level behavior.

const ROOT: &str = "/storage";
const NAME: &str = "docs";

fn engine_config(clock: &Arc<ManualClock>, tokens: TokenConfig) -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new("boot")),
        tokens,
        clock: Some(Arc::clone(clock) as Arc<dyn crate::Clock>),
        ..EngineConfig::default()
    }
}

async fn open(fault: &Arc<FaultVfs>, config: EngineConfig) -> LocalStorageEngine {
    let engine = LocalStorageEngine::from_engine(
        Engine::open(fault.process(), ROOT, config).expect("engine should open"),
    );
    engine
        .create_collection(CreateCollectionRequest::new(NAME, 2, DistanceMetric::Dot))
        .await
        .expect("collection should be created");
    engine
}

async fn write(engine: &LocalStorageEngine, operations: Vec<WriteOperation>) {
    engine.write(NAME, operations).await.expect("write");
}

fn delete(id: &str) -> WriteOperation {
    WriteOperation::Delete(DeleteRecord {
        id: RecordId::new(id),
    })
}

async fn scan(
    engine: &LocalStorageEngine,
    snapshot: Option<Snapshot>,
) -> Result<Vec<VisibleRecord>> {
    engine.scan_exact(NAME, snapshot).await
}

/// A token reads exactly the state it pinned, through writes, deletes, flushes and a
/// compaction (I12), and so does the exact snapshot it names; releasing it ends both.
#[tokio::test]
async fn reads_through_a_token_return_exactly_the_pinned_state() {
    let clock = Arc::new(ManualClock::new());
    let fault = FaultVfs::new(21);
    let engine = open(&fault, engine_config(&clock, config(4))).await;
    write(
        &engine,
        vec![put("a", vec![1.0, 0.0]), put("b", vec![0.0, 1.0])],
    )
    .await;
    engine.flush(NAME).await.expect("flush");
    write(&engine, vec![put("c", vec![1.0, 1.0])]).await;

    let (token, snapshot) = engine.pin_snapshot(NAME).expect("pin");
    let pinned = scan(&engine, None).await.expect("scan");
    let pinned_stats = engine.stats(NAME).await.expect("stats");
    assert_eq!(pinned.len(), 3);

    write(&engine, vec![delete("a"), put("b", vec![5.0, 5.0])]).await;
    engine.flush(NAME).await.expect("flush");
    write(&engine, vec![put("d", vec![1.0, 0.0])]).await;
    engine.flush(NAME).await.expect("flush");
    engine.compact(NAME).await.expect("compact");
    clock.advance(TTL - Duration::from_secs(1));

    assert_eq!(
        engine
            .scan_exact_at_token(NAME, token.clone())
            .await
            .expect("token scan"),
        pinned
    );
    assert_eq!(
        scan(&engine, Some(snapshot.clone()))
            .await
            .expect("snapshot scan"),
        pinned
    );
    let stats = engine
        .stats_at_token(NAME, token.clone())
        .await
        .expect("token stats");
    assert_eq!(stats.manifest_generation, pinned_stats.manifest_generation);
    assert_eq!(stats.visible_seq_no, pinned_stats.visible_seq_no);
    assert_eq!(stats.live_record_count, 3);
    let current = scan(&engine, None).await.expect("current");
    assert_ne!(current, pinned, "the current state moved on");

    assert!(engine.release_snapshot(NAME, &token).expect("release"));
    let error = engine
        .scan_exact_at_token(NAME, token)
        .await
        .expect_err("released");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
    let error = scan(&engine, Some(snapshot)).await.expect_err("unpinned");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
}

/// An exact snapshot of the current generation reads the current version up to its sequence
/// number; after a flush it is gone unless pinned.
#[tokio::test]
async fn an_unpinned_snapshot_expires_once_its_generation_is_superseded() {
    let clock = Arc::new(ManualClock::new());
    let fault = FaultVfs::new(22);
    let engine = open(&fault, engine_config(&clock, config(4))).await;
    write(&engine, vec![put("a", vec![1.0, 0.0])]).await;
    let snapshot = engine.snapshot(NAME).await.expect("snapshot");
    write(&engine, vec![put("b", vec![1.0, 0.0])]).await;
    assert_eq!(
        scan(&engine, Some(snapshot.clone()))
            .await
            .expect("same generation")
            .len(),
        1
    );
    engine.flush(NAME).await.expect("flush");
    let error = scan(&engine, Some(snapshot)).await.expect_err("superseded");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
}

/// Tokens expire after `ttl` without a use, measured on the injected clock; the reaper drops
/// expired pins.
#[tokio::test]
async fn tokens_expire_on_the_injected_clock_and_the_reaper_drops_them() {
    let clock = Arc::new(ManualClock::new());
    let fault = FaultVfs::new(23);
    let engine = open(&fault, engine_config(&clock, config(4))).await;
    write(&engine, vec![put("a", vec![1.0, 0.0])]).await;
    let (kept, _) = engine.pin_snapshot(NAME).expect("pin");
    let (idle, _) = engine.pin_snapshot(NAME).expect("pin");
    let handle = engine
        .engine()
        .collection(&logpose_types::CollectionRef::new_default(NAME))
        .expect("handle");
    for _ in 0..3 {
        clock.advance(TTL / 2);
        engine
            .scan_exact_at_token(NAME, kept.clone())
            .await
            .expect("used tokens stay pinned");
    }
    assert_eq!(handle.pinned_snapshots(), 2, "not reaped yet");
    assert_eq!(engine.engine().reap_snapshots(), 1);
    assert_eq!(handle.pinned_snapshots(), 1);
    let error = engine
        .scan_exact_at_token(NAME, idle)
        .await
        .expect_err("idle token expired");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
    clock.advance(TTL);
    assert_eq!(engine.engine().reap_snapshots(), 1);
    assert_eq!(handle.pinned_snapshots(), 0);
}

/// Pins beyond `max_per_collection` fail with `TooManySnapshots` (RESOURCE_EXHAUSTED).
#[tokio::test]
async fn pins_beyond_the_per_collection_limit_are_refused() {
    let clock = Arc::new(ManualClock::new());
    let fault = FaultVfs::new(24);
    let engine = open(&fault, engine_config(&clock, config(2))).await;
    let (first, _) = engine.pin_snapshot(NAME).expect("pin");
    engine.pin_snapshot(NAME).expect("pin");
    let error = engine.pin_snapshot(NAME).expect_err("over the limit");
    assert!(
        matches!(error, LogPoseError::TooManySnapshots { .. }),
        "{error}"
    );
    engine.release_snapshot(NAME, &first).expect("release");
    engine.pin_snapshot(NAME).expect("room again");
}

/// While pinned snapshots hold more retired memory than the limit, new pins are refused, and
/// the reaper expires the oldest pins until they no longer do.
#[tokio::test]
async fn the_pinned_memory_limit_refuses_new_pins_and_expires_the_oldest() {
    let clock = Arc::new(ManualClock::new());
    let fault = FaultVfs::new(25);
    let limit = TokenConfig {
        memory_limit: 64,
        ..config(8)
    };
    let engine = open(&fault, engine_config(&clock, limit)).await;
    write(&engine, vec![put("a", vec![1.0, 0.0])]).await;
    let (oldest, _) = engine.pin_snapshot(NAME).expect("nothing is retired yet");
    write(
        &engine,
        vec![put("b", vec![1.0, 0.0]), put("c", vec![0.0, 1.0])],
    )
    .await;
    let (newer, _) = engine.pin_snapshot(NAME).expect("nothing is retired yet");
    engine
        .flush(NAME)
        .await
        .expect("flush retires both pins' deltas");
    let handle = engine
        .engine()
        .collection(&logpose_types::CollectionRef::new_default(NAME))
        .expect("handle");
    assert!(handle.pinned_retired_bytes() > 64);

    let error = engine
        .pin_snapshot(NAME)
        .expect_err("over the memory limit");
    assert!(
        matches!(error, LogPoseError::TooManySnapshots { .. }),
        "{error}"
    );
    let reaped = engine.engine().reap_snapshots();
    assert!(reaped >= 1);
    assert!(handle.pinned_retired_bytes() <= 64);
    let error = engine
        .scan_exact_at_token(NAME, oldest)
        .await
        .expect_err("the oldest pin went first");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
    if reaped == 1 {
        engine
            .scan_exact_at_token(NAME, newer)
            .await
            .expect("the newer pin fits under the limit");
    }
    engine.pin_snapshot(NAME).expect("under the limit again");
}
