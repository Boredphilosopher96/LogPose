//! Seeded randomized service and transport parity tests.

use logpose_auth as _;
use logpose_service as _;
use logpose_storage_etcd as _;
use serde as _;
use thiserror as _;

#[path = "support/randomized.rs"]
mod support;

#[tokio::test]
async fn randomized_service_scenarios_match_the_expected_model() {
    support::run_service_scenarios().await;
}

#[tokio::test]
async fn regression_seeds_match_the_expected_model() {
    support::run_regression_seeds().await;
}

#[tokio::test]
async fn background_maintenance_stays_off_past_the_default_thresholds() {
    support::run_background_maintenance_stays_off().await;
}
