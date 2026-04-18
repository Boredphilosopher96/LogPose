//! Seeded randomized service and transport parity tests.

use async_trait as _;
use logpose_auth as _;
use logpose_catalog as _;
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
