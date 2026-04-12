//! Integration tests for the gRPC-backed LogPose client.

use logpose_catalog as _;
use logpose_client::LogPoseClient;
use logpose_config::LogPoseConfig;
use logpose_core::AppState;
use logpose_query::QueryRequest;
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{DistanceMetric, PutRecord, RecordId, WriteOperation};
use serde as _;
use serde_json::json;
use std::{
    fs,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror as _;
use tonic as _;

#[tokio::test]
async fn grpc_client_runs_metadata_and_collection_workflows() {
    let temp_root = unique_temp_dir("client-grpc");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config(&temp_root, rest_addr, grpc_addr)));

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let endpoint = format!("http://{grpc_addr}");
    let client = LogPoseClient::connect(endpoint.clone())
        .await
        .expect("client should connect");

    let metadata = client.metadata().await.expect("metadata should load");
    assert_eq!(metadata.product, "LogPose");
    assert_eq!(metadata.node_name, "client-grpc");
    assert_eq!(metadata.profile, "debug");
    assert!(!metadata.version.is_empty(), "version should be non-empty");
    assert!(!metadata.git_sha.is_empty(), "git sha should be non-empty");

    let descriptor = client
        .create_collection(CreateCollectionRequest {
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");
    assert_eq!(descriptor.name, "documents");

    let read_back = client
        .get_collection("documents")
        .await
        .expect("collection should load");
    assert_eq!(read_back.collection_id, descriptor.collection_id);

    client
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep","color":"red"}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.5, 0.0],
                    metadata: json!({"kind":"drop","color":"blue"}),
                }),
            ],
        )
        .await
        .expect("write should succeed");

    let query = client
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            filters: vec![logpose_query::MetadataFilter {
                field: "kind".to_owned(),
                value: logpose_query::ScalarMetadataValue::String("keep".to_owned()),
            }],
        })
        .await
        .expect("query should succeed");
    assert_eq!(query.matches[0].id.as_str(), "alpha");

    let stats = client.stats("documents").await.expect("stats should load");
    assert_eq!(stats.live_record_count, 2);

    let flush = client
        .flush("documents")
        .await
        .expect("flush should succeed");
    assert!(flush.manifest_generation >= 1);

    let compact = client
        .compact("documents")
        .await
        .expect("compact should succeed");
    assert!(compact.manifest_generation >= flush.manifest_generation);

    let inspect = client
        .inspect("documents", InspectTarget::Manifest)
        .await
        .expect("inspect should succeed");
    assert_eq!(inspect.target, "manifest");

    server.abort();
    let _ = server.await;
}

fn test_config(root: &Path, rest_addr: SocketAddr, grpc_addr: SocketAddr) -> LogPoseConfig {
    LogPoseConfig {
        node_name: "client-grpc".to_owned(),
        rest_host: rest_addr.ip().to_string(),
        rest_port: rest_addr.port(),
        grpc_host: grpc_addr.ip().to_string(),
        grpc_port: grpc_addr.port(),
        log_filter: "info".to_owned(),
        storage_root: root.join("data"),
    }
}

fn reserve_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener should expose addr");
    drop(listener);
    address
}

async fn wait_for_port(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(address).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        TcpStream::connect(address).is_ok(),
        "timed out waiting for server at {address}"
    );
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}
