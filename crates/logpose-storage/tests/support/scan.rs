//! Whole-collection scans for storage tests, over the read path (`logpose_query::scan_records`).

use logpose_storage::{CollectionReader, ReadOptions, SnapshotToken};
use logpose_types::{CollectionRef, Result, Snapshot, VisibleRecord};

/// Scans of every live record, sorted by id, as the v1 read methods returned them.
#[allow(dead_code, reason = "each test crate uses a subset")]
pub trait ScanExt {
    /// The current state, or exactly `snapshot`.
    async fn scan_exact(
        &self,
        collection: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<VisibleRecord>>;

    /// The state `token` pins, extending its expiry.
    async fn scan_exact_at_token(
        &self,
        collection: &str,
        token: SnapshotToken,
    ) -> Result<Vec<VisibleRecord>>;
}

impl<T: CollectionReader + ?Sized> ScanExt for T {
    async fn scan_exact(
        &self,
        collection: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<VisibleRecord>> {
        let options = ReadOptions {
            snapshot,
            ..ReadOptions::default()
        };
        Ok(logpose_query::scan_records(self, &CollectionRef::parse(collection)?, options).await?)
    }

    async fn scan_exact_at_token(
        &self,
        collection: &str,
        token: SnapshotToken,
    ) -> Result<Vec<VisibleRecord>> {
        let options = ReadOptions {
            token: Some(token),
            ..ReadOptions::default()
        };
        Ok(logpose_query::scan_records(self, &CollectionRef::parse(collection)?, options).await?)
    }
}
