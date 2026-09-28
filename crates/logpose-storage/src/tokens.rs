//! Snapshot tokens: repeatable reads across requests (D7).
//!
//! A token pins one published [`Version`] in its collection's [`TokenRegistry`] until it
//! expires or is released. Expiry is sliding: every use extends it by the TTL, so a scroll
//! that keeps paging never loses its snapshot. A request resolves a token to an
//! `Arc<Version>` once and holds that `Arc` until it finishes, so expiry never pulls files out
//! from under a running read; the pin only keeps the `Version` alive between requests.
//!
//! Pins hold obsolete segment files on disk and retired memtables (memtables a flush turned
//! into a segment) in memory. The first is bounded by `max_per_collection`; the second by the
//! engine-wide `memory_limit`: new pins are refused with [`LogPoseError::TooManySnapshots`]
//! while pinned retired bytes exceed it, and the reaper expires pins oldest-first until they no
//! longer do. Pinned retired bytes also count against the engine-wide memtable budget.
//!
//! The registry mutex is held only for a map operation. Versions leave the map under the lock
//! but are dropped after it is released, because dropping one may enqueue file removals.

use crate::version::{Version, VersionId};
use logpose_types::{CollectionId, LogPoseError, Result, Snapshot, UnitId};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    str::FromStr,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

/// Snapshot token settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TokenConfig {
    /// How long a token lives after its last use. Default 5 minutes.
    pub ttl: Duration,
    /// Most pinned snapshots per collection. Default 64.
    pub max_per_collection: usize,
    /// Engine-wide bytes of retired memtables that only pinned snapshots still hold, above which
    /// new pins are refused and the reaper expires pins oldest-first. `None` (the default) is a
    /// quarter of the engine's memtable budget.
    pub memory_limit: Option<u64>,
    /// How often the reaper runs. Default 1 second.
    pub reaper_interval: Duration,
}

impl Default for TokenConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(5 * 60),
            max_per_collection: 64,
            memory_limit: None,
            reaper_interval: Duration::from_secs(1),
        }
    }
}

/// Bytes of an encoded token before base64: collection id, version id, nonce, CRC.
const TOKEN_BYTES: usize = 36;

/// An opaque handle on a pinned snapshot. Clients see it as the base64url (no padding) text of
/// its 36 bytes: the collection id (16), the version id (8), a random nonce that prevents
/// guessing (8), and a CRC-32C of those 32 bytes (4).
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SnapshotToken {
    collection_id: CollectionId,
    version_id: VersionId,
    nonce: u64,
}

impl SnapshotToken {
    /// The pinned version.
    #[must_use]
    pub fn version_id(&self) -> VersionId {
        self.version_id
    }

    /// The collection the token belongs to.
    #[must_use]
    pub fn collection_id(&self) -> &CollectionId {
        &self.collection_id
    }

    fn to_bytes(&self) -> [u8; TOKEN_BYTES] {
        let mut bytes = [0; TOKEN_BYTES];
        bytes[..16].copy_from_slice(self.collection_id.0.as_bytes());
        bytes[16..24].copy_from_slice(&self.version_id.0.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.nonce.to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..32]);
        bytes[32..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != TOKEN_BYTES {
            return None;
        }
        let word = |range: std::ops::Range<usize>| {
            let mut array = [0; 8];
            array.copy_from_slice(&bytes[range]);
            u64::from_le_bytes(array)
        };
        let mut crc = [0; 4];
        crc.copy_from_slice(&bytes[32..]);
        if crc32c::crc32c(&bytes[..32]) != u32::from_le_bytes(crc) {
            return None;
        }
        let collection = Uuid::from_slice(&bytes[..16]).ok()?;
        Some(Self {
            collection_id: CollectionId(collection),
            version_id: VersionId(word(16..24)),
            nonce: word(24..32),
        })
    }
}

impl fmt::Display for SnapshotToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&base64url_encode(&self.to_bytes()))
    }
}

/// Why a token string does not parse.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidSnapshotToken;

impl fmt::Display for InvalidSnapshotToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("not a snapshot token")
    }
}

impl std::error::Error for InvalidSnapshotToken {}

impl FromStr for SnapshotToken {
    type Err = InvalidSnapshotToken;

    fn from_str(text: &str) -> std::result::Result<Self, Self::Err> {
        base64url_decode(text)
            .and_then(|bytes| Self::from_bytes(&bytes))
            .ok_or(InvalidSnapshotToken)
    }
}

const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn base64url_encode(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let mut group = [0_u8; 3];
        group[..chunk.len()].copy_from_slice(chunk);
        let bits = u32::from(group[0]) << 16 | u32::from(group[1]) << 8 | u32::from(group[2]);
        for index in 0..=chunk.len() {
            let sextet = (bits >> (18 - 6 * index)) & 0x3f;
            text.push(char::from(BASE64URL[sextet as usize]));
        }
    }
    text
}

fn base64url_decode(text: &str) -> Option<Vec<u8>> {
    if text.len() % 4 == 1 {
        return None;
    }
    let mut bytes = Vec::with_capacity(text.len() * 3 / 4);
    for chunk in text.as_bytes().chunks(4) {
        let mut bits = 0_u32;
        for (index, symbol) in chunk.iter().enumerate() {
            let value = BASE64URL.iter().position(|candidate| candidate == symbol)?;
            bits |= (value as u32) << (18 - 6 * index);
        }
        let decoded = [(bits >> 16) as u8, (bits >> 8) as u8, bits as u8];
        let len = chunk.len() - 1;
        // Canonical encodings only: the unused low bits of a partial group are zero.
        if decoded[len..].iter().any(|byte| *byte != 0) {
            return None;
        }
        bytes.extend_from_slice(&decoded[..len]);
    }
    Some(bytes)
}

/// One pinned version.
struct Pin {
    version: Arc<Version>,
    expires_at: Duration,
    /// Creation order, for oldest-first expiry.
    order: u64,
}

type PinKey = (VersionId, u64);

/// The pinned snapshots of one collection.
pub(crate) struct TokenRegistry {
    collection_id: CollectionId,
    collection: String,
    pins: Mutex<HashMap<PinKey, Pin>>,
}

/// Creation order of pins across every collection, so the reaper can expire the oldest pin
/// engine-wide.
static NEXT_PIN_ORDER: AtomicU64 = AtomicU64::new(0);

impl TokenRegistry {
    pub(crate) fn new(collection_id: CollectionId, collection: String) -> Self {
        Self {
            collection_id,
            collection,
            pins: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PinKey, Pin>> {
        self.pins.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn expired(&self, reason: impl Into<String>) -> LogPoseError {
        LogPoseError::SnapshotExpired {
            collection: self.collection.clone(),
            reason: reason.into(),
        }
    }

    /// Pin `version` until `now + config.ttl`. Expired pins are dropped first, so they never
    /// count against the limit. `memory_exceeded` says whether the engine-wide pinned-memory
    /// limit is already exceeded.
    pub(crate) fn pin(
        &self,
        version: Arc<Version>,
        now: Duration,
        config: &TokenConfig,
        memory_exceeded: bool,
    ) -> Result<SnapshotToken> {
        let refused = |reason: String| LogPoseError::TooManySnapshots {
            collection: self.collection.clone(),
            reason,
        };
        if memory_exceeded {
            return Err(refused(format!(
                "pinned snapshots already hold more than the {}-byte pinned-memory limit",
                config.memory_limit.unwrap_or(0)
            )));
        }
        let token = SnapshotToken {
            collection_id: self.collection_id.clone(),
            version_id: version.id,
            nonce: Uuid::new_v4().as_u64_pair().0,
        };
        let expired = {
            let mut pins = self.lock();
            let expired = take_expired(&mut pins, now);
            if pins.len() >= config.max_per_collection {
                drop(pins);
                drop(expired);
                return Err(refused(format!(
                    "it already holds {} pinned snapshots, the per-collection limit",
                    config.max_per_collection
                )));
            }
            pins.insert(
                (token.version_id, token.nonce),
                Pin {
                    version,
                    expires_at: now.saturating_add(config.ttl),
                    order: NEXT_PIN_ORDER.fetch_add(1, Ordering::Relaxed),
                },
            );
            expired
        };
        drop(expired);
        Ok(token)
    }

    /// The version `token` pins, extending its expiry to `now + ttl`. Fails with
    /// [`LogPoseError::SnapshotExpired`] when the token expired, was released, was never issued,
    /// or belongs to another collection.
    pub(crate) fn resolve(
        &self,
        token: &SnapshotToken,
        now: Duration,
        ttl: Duration,
    ) -> Result<Arc<Version>> {
        if token.collection_id != self.collection_id {
            return Err(self.expired("the token belongs to another collection"));
        }
        let key = (token.version_id, token.nonce);
        let mut pins = self.lock();
        match pins.get_mut(&key) {
            Some(pin) if pin.expires_at > now => {
                pin.expires_at = now.saturating_add(ttl);
                Ok(Arc::clone(&pin.version))
            }
            Some(_) => {
                let pin = pins.remove(&key);
                drop(pins);
                drop(pin);
                Err(self.expired("the token expired"))
            }
            None => Err(self
                .expired("the token expired, was released, or was never issued; restart the read")),
        }
    }

    /// The pinned version an exact `snapshot` names: the same manifest generation and visible
    /// sequence number. Extends the pin's expiry.
    pub(crate) fn find(
        &self,
        snapshot: &Snapshot,
        now: Duration,
        ttl: Duration,
    ) -> Option<Arc<Version>> {
        let mut pins = self.lock();
        let pin = pins.values_mut().find(|pin| {
            pin.expires_at > now
                && pin.version.manifest_generation == snapshot.manifest_generation
                && pin.version.visible_seq_no == snapshot.visible_seq_no
        })?;
        pin.expires_at = now.saturating_add(ttl);
        Some(Arc::clone(&pin.version))
    }

    /// Unpin `token`. Returns whether it was pinned.
    pub(crate) fn release(&self, token: &SnapshotToken) -> bool {
        if token.collection_id != self.collection_id {
            return false;
        }
        let pin = self.lock().remove(&(token.version_id, token.nonce));
        pin.is_some()
    }

    /// Remove every pin that expired by `now`, returning their versions for the caller to drop
    /// outside any lock.
    pub(crate) fn reap(&self, now: Duration) -> Vec<Arc<Version>> {
        take_expired(&mut self.lock(), now)
    }

    /// Number of live pins, including ones that expired but were not reaped yet.
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// Bytes of retired memtables (memtables `current` no longer contains) that only pinned
    /// versions still hold. A memtable several pins share is counted once, at the largest size
    /// any of them holds.
    pub(crate) fn retired_bytes(&self, current: &Version) -> u64 {
        let versions = self
            .lock()
            .values()
            .map(|pin| Arc::clone(&pin.version))
            .collect::<Vec<_>>();
        retired_bytes(&versions, current)
    }

    /// The oldest pin that holds a retired memtable, with its creation order.
    pub(crate) fn oldest_retired(&self, current: &Version) -> Option<(u64, SnapshotToken)> {
        let live = live_memtables(current);
        self.lock()
            .iter()
            .filter(|(_, pin)| {
                pin.version
                    .memtables()
                    .any(|memtable| !live.contains(&memtable.unit))
            })
            .min_by_key(|(_, pin)| pin.order)
            .map(|((version_id, nonce), pin)| {
                (
                    pin.order,
                    SnapshotToken {
                        collection_id: self.collection_id.clone(),
                        version_id: *version_id,
                        nonce: *nonce,
                    },
                )
            })
    }

    /// Remove `token`'s pin and return its version for the caller to drop.
    pub(crate) fn evict(&self, token: &SnapshotToken) -> Option<Arc<Version>> {
        self.lock()
            .remove(&(token.version_id, token.nonce))
            .map(|pin| pin.version)
    }
}

fn take_expired(pins: &mut HashMap<PinKey, Pin>, now: Duration) -> Vec<Arc<Version>> {
    let keys = pins
        .iter()
        .filter(|(_, pin)| pin.expires_at <= now)
        .map(|(key, _)| *key)
        .collect::<Vec<_>>();
    keys.into_iter()
        .filter_map(|key| pins.remove(&key))
        .map(|pin| pin.version)
        .collect()
}

/// The memtables `current` contains.
fn live_memtables(current: &Version) -> HashSet<UnitId> {
    current.memtables().map(|memtable| memtable.unit).collect()
}

/// Bytes of the memtables `versions` hold that `current` does not, each counted once at the
/// largest size a version holds it at (versions of one memtable hold prefixes of it).
fn retired_bytes(versions: &[Arc<Version>], current: &Version) -> u64 {
    let live = live_memtables(current);
    let mut retired = HashMap::<UnitId, u64>::new();
    for version in versions {
        for memtable in version.memtables() {
            if live.contains(&memtable.unit) {
                continue;
            }
            let bytes = retired.entry(memtable.unit).or_default();
            *bytes = (*bytes).max(memtable.bytes().total());
        }
    }
    retired.values().sum()
}

#[cfg(test)]
mod tests;
