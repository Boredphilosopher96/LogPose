//! Deterministic control-plane/data-plane simulation scenarios.

use async_trait as _;
use logpose_auth as _;
use logpose_catalog as _;
use logpose_query as _;
use logpose_service as _;
use logpose_storage_etcd as _;
use rand as _;
use serde as _;
use thiserror as _;

#[path = "support/control_plane_simulation.rs"]
mod support;

#[tokio::test]
async fn simulated_control_plane_scenarios_preserve_runtime_placement_and_data_parity() {
    support::run_control_plane_scenarios().await;
}
