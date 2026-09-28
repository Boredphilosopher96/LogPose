//! What fetches report: per-access outcomes, [`FetchReport`] for `EXPLAIN`,
//! [`PinSet`] for the compute stage, and [`CacheStats`] for metrics.

use super::{AlignedBytes, ArtifactClass, CacheKey};
use std::{collections::HashMap, sync::Arc};

/// How one access was served.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Fetched {
    /// The unit was resident.
    Hit,
    /// Another caller was already loading the unit; this one waited for it.
    Waited,
    /// This caller missed and loaded the unit.
    Loaded {
        /// Bytes read.
        bytes: u64,
        /// Time the load took, in microseconds.
        micros: u64,
    },
}

/// Misses and bytes read for one class.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClassFetch {
    /// Units this caller loaded.
    pub misses: u32,
    /// Bytes this caller read.
    pub bytes: u64,
}

/// Cold-read accounting of one fetch stage, printed by `EXPLAIN`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FetchReport {
    /// Units that were resident.
    pub hits: u32,
    /// Units this stage loaded.
    pub misses: u32,
    /// Units another caller was loading, which this stage waited for.
    pub waits: u32,
    /// Bytes this stage read.
    pub bytes_read: u64,
    /// Time spent in this stage's loads, in microseconds.
    pub io_micros: u64,
    /// Misses and bytes per class, indexed by [`ArtifactClass::index`].
    pub by_class: [ClassFetch; ArtifactClass::COUNT],
}

impl FetchReport {
    /// Count one access of a unit of `class`.
    pub fn record(&mut self, class: ArtifactClass, fetched: Fetched) {
        match fetched {
            Fetched::Hit => self.hits += 1,
            Fetched::Waited => self.waits += 1,
            Fetched::Loaded { bytes, micros } => {
                self.misses += 1;
                self.bytes_read += bytes;
                self.io_micros += micros;
                let class = &mut self.by_class[class.index()];
                class.misses += 1;
                class.bytes += bytes;
            }
        }
    }

    /// Add another report's counts to this one.
    pub fn merge(&mut self, other: &Self) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.waits += other.waits;
        self.bytes_read += other.bytes_read;
        self.io_micros += other.io_micros;
        for (mine, theirs) in self.by_class.iter_mut().zip(other.by_class) {
            mine.misses += theirs.misses;
            mine.bytes += theirs.bytes;
        }
    }

    /// Whether anything had to be read or waited for.
    #[must_use]
    pub fn is_cold(&self) -> bool {
        self.misses > 0 || self.waits > 0
    }
}

/// The units a fetch stage loaded, pinned for the compute stage.
///
/// A pinned unit is never evicted, so a compute stage that reads only
/// through its `PinSet` never waits on I/O and never finds its data gone.
/// Dropping the set releases every pin.
#[derive(Clone, Debug, Default)]
pub struct PinSet {
    pins: HashMap<CacheKey, Arc<AlignedBytes>>,
}

impl PinSet {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin `bytes` under `key`, replacing any earlier pin of the key.
    pub fn insert(&mut self, key: CacheKey, bytes: Arc<AlignedBytes>) {
        self.pins.insert(key, bytes);
    }

    /// Add every pin of `other`.
    pub fn extend(&mut self, other: PinSet) {
        self.pins.extend(other.pins);
    }

    /// The pinned bytes of `key`.
    #[must_use]
    pub fn get(&self, key: &CacheKey) -> Option<&Arc<AlignedBytes>> {
        self.pins.get(key)
    }

    /// Whether `key` is pinned.
    #[must_use]
    pub fn contains(&self, key: &CacheKey) -> bool {
        self.pins.contains_key(key)
    }

    /// Release the pin of `key`.
    pub fn release(&mut self, key: &CacheKey) -> bool {
        self.pins.remove(key).is_some()
    }

    /// Number of pinned units.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pins.len()
    }

    /// Whether nothing is pinned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pins.is_empty()
    }

    /// Total bytes pinned.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.pins.values().map(|bytes| bytes.len() as u64).sum()
    }
}

/// A snapshot of cache counters and usage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    /// Current budget in bytes.
    pub budget: u64,
    /// Charged bytes per class, indexed by [`ArtifactClass::index`].
    pub used: [u64; ArtifactClass::COUNT],
    /// Resident entries.
    pub entries: u64,
    /// Accesses served from a resident entry.
    pub hits: u64,
    /// Loads started (cached or bypass).
    pub misses: u64,
    /// Accesses that waited for another caller's load.
    pub waits: u64,
    /// Loads that failed (I/O, CRC, structure, or aborted); none was cached.
    pub failed_loads: u64,
    /// Entries evicted under budget pressure.
    pub evictions: u64,
    /// Bytes evicted under budget pressure.
    pub evicted_bytes: u64,
    /// Entries removed because their file was invalidated.
    pub invalidated: u64,
    /// Times an eviction pass ended over budget because everything left was
    /// pinned.
    pub overcommits: u64,
    /// Bytes over budget at the end of the most recent eviction pass (0 when
    /// it ended within budget): `cache_overcommit_bytes`.
    pub overcommit_bytes: u64,
}

impl CacheStats {
    /// Total charged bytes.
    #[must_use]
    pub fn used_total(&self) -> u64 {
        self.used.iter().sum()
    }

    /// Charged bytes of `class`.
    #[must_use]
    pub fn used_by(&self, class: ArtifactClass) -> u64 {
        self.used[class.index()]
    }
}
