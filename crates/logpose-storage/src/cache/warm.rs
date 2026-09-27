//! Warm-up on open: load the hot classes in priority order, in the
//! background, without crowding out foreground misses.
//!
//! Items run class by class, highest priority first (the engine passes
//! `GraphAndCodes`, then `PkIndex`, then `ScalarIndex` sections, largest
//! segment first within a class). A class stops at the first item that would
//! take the cache past [`WARM_UP_FILL`] of its budget; the next class then
//! gets the chance to use what room is left. At most [`WARM_UP_IN_FLIGHT`]
//! loads run at once, so warm-up never occupies more than two I/O threads.
//! Resident items are skipped, and a failed item is counted and does not
//! stop the rest.

use super::{BufferCache, CacheKey, CacheMode, Fetch, Fetched, LoadExecutor, Loader, charge_for};
use crate::cache::ArtifactClass;
use std::{collections::VecDeque, fmt};

/// Warm-up stops a class when the cache would pass this share of its budget.
pub const WARM_UP_FILL: f64 = 0.9;

/// Warm-up loads in flight at once.
pub const WARM_UP_IN_FLIGHT: usize = 2;

/// One unit to warm.
pub struct WarmUpItem {
    /// The unit's key.
    pub key: CacheKey,
    /// Its class.
    pub class: ArtifactClass,
    /// Its size in bytes, known from the section table before loading.
    pub bytes: u64,
    /// Reads and verifies it.
    pub load: Loader,
}

impl fmt::Debug for WarmUpItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WarmUpItem")
            .field("key", &self.key)
            .field("class", &self.class)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

/// What a warm-up did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WarmUpReport {
    /// Units loaded.
    pub loaded: u32,
    /// Units that were already resident or were being loaded by someone
    /// else.
    pub resident: u32,
    /// Units not loaded because their class hit the fill limit.
    pub skipped: u32,
    /// Units whose load failed.
    pub failed: u32,
    /// Bytes read.
    pub bytes_read: u64,
}

pub(super) async fn warm_up(
    cache: &BufferCache,
    mut items: Vec<WarmUpItem>,
    executor: &dyn LoadExecutor,
) -> WarmUpReport {
    items.sort_by_key(|item| item.class);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let limit = (cache.budget() as f64 * WARM_UP_FILL) as u64;
    let mut report = WarmUpReport::default();
    let mut in_flight: VecDeque<(u64, Fetch)> = VecDeque::new();
    let mut stopped: Option<ArtifactClass> = None;
    for item in items {
        if stopped == Some(item.class) {
            report.skipped += 1;
            continue;
        }
        if cache.residency(&item.key) {
            report.resident += 1;
            continue;
        }
        while in_flight.len() >= WARM_UP_IN_FLIGHT {
            if let Some((_, fetch)) = in_flight.pop_front() {
                tally(&mut report, fetch.await);
            }
        }
        let pending: u64 = in_flight.iter().map(|(charge, _)| charge).sum();
        let charge = usize::try_from(item.bytes).map_or(u64::MAX, charge_for);
        if cache.used().saturating_add(pending).saturating_add(charge) > limit {
            stopped = Some(item.class);
            report.skipped += 1;
            continue;
        }
        let fetch = cache.get_or_load(item.key, item.class, CacheMode::Normal, executor, item.load);
        in_flight.push_back((charge, fetch));
    }
    for (_, fetch) in in_flight {
        tally(&mut report, fetch.await);
    }
    report
}

fn tally(
    report: &mut WarmUpReport,
    result: Result<(std::sync::Arc<super::AlignedBytes>, Fetched), crate::segment_v2::SegmentError>,
) {
    match result {
        Ok((_, Fetched::Loaded { bytes, .. })) => {
            report.loaded += 1;
            report.bytes_read += bytes;
        }
        Ok((_, Fetched::Hit | Fetched::Waited)) => report.resident += 1,
        Err(_) => report.failed += 1,
    }
}
