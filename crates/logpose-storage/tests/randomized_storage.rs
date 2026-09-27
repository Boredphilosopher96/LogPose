//! Seeded state-machine tests for `LocalStorageEngine`.

use arc_swap as _;
use async_trait as _;
use bytemuck as _;
use crc32c as _;
use crc32fast as _;
use logpose_auth as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query as _;
use logpose_wal as _;
use postcard as _;
use rayon as _;
use roaring as _;
use serde as _;
use thiserror as _;
use twox_hash as _;
use uuid as _;

#[path = "support/randomized.rs"]
mod support;

#[tokio::test]
async fn randomized_storage_scenarios_match_the_expected_model() {
    support::run_storage_scenarios(support::BackendKind::Std).await;
}

/// The same scenarios on `FaultVfs`, with crash-and-reopen actions: every acknowledged write
/// survives, an interrupted batch is wholly present or wholly absent, and every snapshot taken
/// before a crash still reads the same state after it.
#[tokio::test]
async fn randomized_storage_scenarios_survive_crashes() {
    support::run_storage_scenarios(support::BackendKind::Fault).await;
}

#[test]
fn current_exact_query_requests_use_default_snapshot_resolution() {
    let request = support::current_exact_query_request_for_test(vec![1.0, 0.0]);
    assert!(request.snapshot.is_none());
}
