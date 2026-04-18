//! End-to-end etcd metadata integration coverage for `AppState`.

use etcd_client::{Client, DeleteOptions};
use logpose_catalog as _;
use logpose_config::LogPoseConfig;
use logpose_core::AppState;
use logpose_query as _;
use logpose_service::ServiceError;
use logpose_storage::CreateCollectionRequest;
use logpose_storage_etcd as _;
use logpose_types::{
    DistanceMetric, EtcdMetadataConfig, MetadataBackend, MetadataConfig, PutRecord, RecordId,
    WriteOperation,
};
use serde as _;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[tokio::test]
async fn etcd_metadata_backend_surfaces_remote_collections_across_nodes() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("remote-discovery");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let root_a = unique_temp_dir("etcd-node-a");
    let root_b = unique_temp_dir("etcd-node-b");
    let cluster_name = "core-etcd-metadata";

    let state_a = Arc::new(AppState::new(test_config(
        "node-a",
        root_a,
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    let descriptor = state_a
        .control
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created through authoritative metadata");

    let state_b = Arc::new(AppState::new(test_config(
        "node-b",
        root_b,
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    state_a
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("authoritative owner should serve local writes");
    let local_stats = state_a
        .stats("documents")
        .await
        .expect("authoritative owner should serve local stats");
    let local_runtime = state_a
        .control
        .runtime_status()
        .await
        .expect("healthy etcd-backed owner should report runtime status");
    let remote_descriptor = state_b
        .get_collection("documents")
        .await
        .expect("remote node should resolve the authoritative descriptor");
    let placement = state_b
        .control
        .collection_placement("documents")
        .await
        .expect("remote node should resolve recorded placement");
    let runtime = state_b
        .control
        .runtime_status()
        .await
        .expect("runtime status should list authoritative metadata");
    let stats_error = state_b
        .stats("documents")
        .await
        .expect_err("remote node must reject non-local data-plane operations");

    assert_eq!(remote_descriptor.collection_id, descriptor.collection_id);
    assert_eq!(remote_descriptor.lookup_name(), "default/default/documents");
    assert_eq!(local_stats.live_record_count, 1);
    assert!(local_runtime.control_plane_ready);
    assert!(local_runtime.data_plane_ready);
    assert_eq!(placement.collection_id, descriptor.collection_id);
    assert_eq!(placement.assigned_node, "node-a");
    assert_eq!(placement.route_kind, "recorded");
    assert!(runtime.control_plane_ready);
    assert!(runtime.data_plane_ready);
    assert_eq!(runtime.collections.len(), 1);
    assert_eq!(runtime.collections[0].collection_name, "documents");
    assert_eq!(runtime.collections[0].assigned_node, "node-a");
    assert_eq!(runtime.collections[0].route_kind, "recorded");
    assert!(matches!(
        stats_error,
        ServiceError::InvalidArgument(message) if message.contains("not locally served")
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

fn test_config(
    node_name: &str,
    storage_root: PathBuf,
    endpoints: &[String],
    key_prefix: &str,
    cluster_name: &str,
) -> LogPoseConfig {
    LogPoseConfig {
        node_name: node_name.to_owned(),
        storage_root,
        metadata: MetadataConfig {
            backend: MetadataBackend::Etcd,
            etcd: EtcdMetadataConfig {
                endpoints: endpoints.to_vec(),
                key_prefix: key_prefix.to_owned(),
                timeout_ms: 1_500,
                membership_ttl_secs: 15,
                leadership_ttl_secs: 10,
                cluster_name: cluster_name.to_owned(),
            },
        },
        ..LogPoseConfig::default()
    }
}

fn test_etcd_endpoints() -> Vec<String> {
    std::env::var("LOGPOSE_TEST_ETCD_ENDPOINTS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|endpoints| !endpoints.is_empty())
        .unwrap_or_else(|| vec!["http://127.0.0.1:2379".to_owned()])
}

async fn cleanup_prefix(endpoints: &[String], key_prefix: &str) {
    if let Ok(mut client) = Client::connect(endpoints.to_vec(), None).await {
        let _ = client
            .delete(key_prefix, Some(DeleteOptions::new().with_prefix()))
            .await;
    }
}

fn unique_etcd_prefix(label: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    format!("/logpose/tests/{label}/{suffix}")
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-core-{label}-{suffix}"));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}
