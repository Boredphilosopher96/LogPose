//! A real gRPC server on a loopback port, for tests that need the transport: size limits,
//! client streaming, and disconnects.

use crate::{proto, serve_with_listener};
use logpose_config::{LimitsConfig, LogPoseConfig};
use logpose_core::AppState;
use proto::log_pose_service_client::LogPoseServiceClient;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

pub(crate) struct TestServer {
    pub(crate) client: LogPoseServiceClient<Channel>,
    pub(crate) address: String,
    /// The server's state, for tests that drive a handler directly.
    pub(crate) state: Arc<AppState>,
    task: JoinHandle<()>,
    root: PathBuf,
}

impl TestServer {
    /// Start a combined node with `limits` and connect a client to it.
    pub(crate) async fn start(label: &str, limits: LimitsConfig) -> Self {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("logpose-api-grpc-{label}-{suffix}"));
        std::fs::create_dir_all(&root).expect("temp dir should be created");
        let state = Arc::new(AppState::new(LogPoseConfig {
            node_name: label.to_owned(),
            storage_root: root.clone(),
            limits,
            ..LogPoseConfig::default()
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let address = format!(
            "http://{}",
            listener.local_addr().expect("listener has an address")
        );
        let server_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            serve_with_listener(server_state, listener)
                .await
                .expect("gRPC server should run");
        });
        let mut attempts = 0;
        let client = loop {
            match LogPoseServiceClient::connect(address.clone()).await {
                Ok(client) => break client,
                Err(error) => {
                    attempts += 1;
                    assert!(attempts < 50, "gRPC server did not come up: {error}");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        Self {
            client,
            address,
            state,
            task,
            root,
        }
    }

    /// Create a two-dimensional dot-product collection in the default database.
    pub(crate) async fn create_collection(&mut self, name: &str) {
        self.client
            .create_collection(proto::CreateCollectionRequest {
                name: name.to_owned(),
                dimensions: 2,
                metric: proto::DistanceMetric::Dot as i32,
                database_name: String::new(),
            })
            .await
            .expect("collection should be created");
    }

    /// Live records in `collection`.
    pub(crate) async fn live_records(&mut self, collection: &str) -> u64 {
        self.client
            .get_collection_stats(proto::GetCollectionStatsRequest {
                collection_name: collection.to_owned(),
                ..Default::default()
            })
            .await
            .expect("stats should load")
            .into_inner()
            .live_record_count
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// A put of a two-dimensional vector.
pub(crate) fn put(id: &str) -> proto::WriteOperation {
    proto::WriteOperation {
        operation: Some(proto::write_operation::Operation::Put(proto::PutRecord {
            id: id.to_owned(),
            vector: vec![1.0, 0.5],
            metadata_json: "{}".to_owned(),
        })),
    }
}

/// Limits with a small gRPC message cap and the default REST cap.
pub(crate) fn small_grpc_limit(max_grpc_message_bytes: usize) -> LimitsConfig {
    LimitsConfig {
        max_grpc_message_bytes,
        ..LimitsConfig::default()
    }
}
