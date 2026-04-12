//! Seeded state-machine tests for `LocalStorageEngine`.

use async_trait as _;
use crc32fast as _;
use logpose_catalog as _;
use logpose_wal as _;
use serde as _;
use uuid as _;

#[path = "support/randomized.rs"]
mod support;

#[tokio::test]
async fn randomized_storage_scenarios_match_the_expected_model() {
    support::run_storage_scenarios().await;
}
