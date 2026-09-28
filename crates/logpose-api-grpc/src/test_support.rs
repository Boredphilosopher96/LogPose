//! A real gRPC server on a loopback port, for tests that need the transport: size limits,
//! client streaming, and disconnects.

use crate::{proto, serve_with_listener};
use logpose_config::{LimitsConfig, LogPoseConfig};
use logpose_core::AppState;
use proto::log_pose_service_client::LogPoseServiceClient;
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

pub(crate) struct TestServer {
    pub(crate) client: LogPoseServiceClient<Channel>,
    pub(crate) address: String,
    /// The server's state, for tests that drive a handler directly.
    pub(crate) state: Arc<AppState>,
    task: JoinHandle<()>,
    /// The node's storage root, removed when the server drops.
    _root: TempDir,
}

impl TestServer {
    /// Start a combined node with `limits` and connect a client to it.
    pub(crate) async fn start(label: &str, limits: LimitsConfig) -> Self {
        let root = tempfile::Builder::new()
            .prefix(&format!("logpose-api-grpc-{label}-"))
            .tempdir()
            .expect("temp dir should be created");
        let state = Arc::new(AppState::new(LogPoseConfig {
            node_name: label.to_owned(),
            storage_root: root.path().to_path_buf(),
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
            _root: root,
        }
    }

    /// Create a two-dimensional dot-product collection in the default database.
    pub(crate) async fn create_collection(&mut self, name: &str) {
        self.client
            .create_collection(proto::CreateCollectionRequest {
                database_name: "default".to_owned(),
                collection_name: name.to_owned(),
                primary_key: Some(proto::PrimaryKeySpec {
                    name: "id".to_owned(),
                    r#type: proto::PrimaryKeyType::String as i32,
                }),
                vectors: vec![proto::VectorFieldSpec {
                    name: "vector".to_owned(),
                    dimensions: 2,
                    metric: proto::DistanceMetric::Dot as i32,
                }],
                fields: Vec::new(),
                dynamic_fields: Some(true),
            })
            .await
            .expect("collection should be created");
    }

    /// Live records in `collection`.
    pub(crate) async fn live_records(&mut self, collection: &str) -> u64 {
        self.client
            .get_collection_stats(proto::GetCollectionStatsRequest {
                database_name: "default".to_owned(),
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
    }
}

/// A record with a two-dimensional vector, for a collection `create_collection` made.
pub(crate) fn put(id: &str) -> proto::Record {
    proto::Record {
        pk: Some(proto::PrimaryKey {
            kind: Some(proto::primary_key::Kind::StringValue(id.to_owned())),
        }),
        vectors: [(
            "vector".to_owned(),
            proto::Vector {
                values: vec![1.0, 0.5],
            },
        )]
        .into_iter()
        .collect(),
        fields: Default::default(),
        extra: None,
    }
}

/// A string field value.
pub(crate) fn text(value: String) -> proto::Value {
    proto::Value {
        kind: Some(proto::value::Kind::StringValue(value)),
    }
}

/// Limits with a small gRPC message cap and the default REST cap.
pub(crate) fn small_grpc_limit(max_grpc_message_bytes: usize) -> LimitsConfig {
    LimitsConfig {
        max_grpc_message_bytes,
        ..LimitsConfig::default()
    }
}
