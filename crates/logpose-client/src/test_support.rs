//! A fake LogPose gRPC server that answers from a script of error statuses.

use logpose_api_grpc::proto::{
    self,
    log_pose_service_server::{LogPoseService, LogPoseServiceServer},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, PoisonError},
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};

/// A running fake server.
///
/// Each call pops the next scripted status and fails with it; once the script is empty,
/// `GetMetadata`, `ListDatabases`, `WriteCollection`, and `FlushCollection` succeed and every
/// other RPC is `UNIMPLEMENTED`.
pub(crate) struct ScriptedServer {
    pub(crate) endpoint: String,
    calls: Arc<Mutex<Vec<&'static str>>>,
    task: JoinHandle<()>,
}

impl ScriptedServer {
    /// Start a server reporting `node_name` that fails with `script`, in order.
    pub(crate) async fn start(node_name: &str, script: Vec<Status>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake server should bind");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("listener has an address")
        );
        let calls = Arc::new(Mutex::new(Vec::new()));
        let service = ScriptedService {
            node_name: node_name.to_owned(),
            script: Arc::new(Mutex::new(script.into())),
            calls: Arc::clone(&calls),
        };
        let task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(LogPoseServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        Self {
            endpoint,
            calls,
            task,
        }
    }

    /// The RPCs served so far, in order.
    pub(crate) fn calls(&self) -> Vec<&'static str> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for ScriptedServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ScriptedService {
    node_name: String,
    script: Arc<Mutex<VecDeque<Status>>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl ScriptedService {
    /// Record `rpc` and fail with the next scripted status, if any.
    fn next(&self, rpc: &'static str) -> Result<(), Status> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(rpc);
        match self
            .script
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
        {
            Some(status) => Err(status),
            None => Ok(()),
        }
    }

    fn unimplemented<T>(&self, rpc: &'static str) -> Result<Response<T>, Status> {
        self.next(rpc)?;
        Err(Status::unimplemented(rpc))
    }
}

#[tonic::async_trait]
impl LogPoseService for ScriptedService {
    async fn get_metadata(
        &self,
        _request: Request<proto::GetMetadataRequest>,
    ) -> Result<Response<proto::GetMetadataReply>, Status> {
        self.next("get_metadata")?;
        Ok(Response::new(proto::GetMetadataReply {
            product: "LogPose".to_owned(),
            node_name: self.node_name.clone(),
            version: "test".to_owned(),
            git_sha: "test".to_owned(),
            profile: "test".to_owned(),
        }))
    }

    async fn get_runtime_status(
        &self,
        _request: Request<proto::GetRuntimeStatusRequest>,
    ) -> Result<Response<proto::GetRuntimeStatusReply>, Status> {
        self.unimplemented("get_runtime_status")
    }

    async fn put_database(
        &self,
        _request: Request<proto::PutDatabaseRequest>,
    ) -> Result<Response<proto::DatabaseDescriptorReply>, Status> {
        self.unimplemented("put_database")
    }

    async fn get_database(
        &self,
        _request: Request<proto::GetDatabaseRequest>,
    ) -> Result<Response<proto::DatabaseDescriptorReply>, Status> {
        self.unimplemented("get_database")
    }

    async fn list_databases(
        &self,
        _request: Request<proto::ListDatabasesRequest>,
    ) -> Result<Response<proto::ListDatabasesReply>, Status> {
        self.next("list_databases")?;
        Ok(Response::new(proto::ListDatabasesReply {
            databases: Vec::new(),
        }))
    }

    async fn create_collection(
        &self,
        _request: Request<proto::CreateCollectionRequest>,
    ) -> Result<Response<proto::CollectionDescriptorReply>, Status> {
        self.unimplemented("create_collection")
    }

    async fn get_collection(
        &self,
        _request: Request<proto::GetCollectionRequest>,
    ) -> Result<Response<proto::CollectionDescriptorReply>, Status> {
        self.unimplemented("get_collection")
    }

    async fn get_collection_placement(
        &self,
        _request: Request<proto::GetCollectionPlacementRequest>,
    ) -> Result<Response<proto::CollectionPlacementReply>, Status> {
        self.unimplemented("get_collection_placement")
    }

    async fn write_collection(
        &self,
        request: Request<proto::WriteCollectionRequest>,
    ) -> Result<Response<proto::CommitAckReply>, Status> {
        self.next("write_collection")?;
        let request = request.into_inner();
        let applied_ops = u64::try_from(request.operations.len()).unwrap_or(u64::MAX);
        Ok(Response::new(proto::CommitAckReply {
            last_seq_no: applied_ops,
            applied_ops,
            database_name: request.database_name,
            collection_name: request.collection_name,
            snapshot: Some(proto::Snapshot {
                manifest_generation: 1,
                visible_seq_no: applied_ops,
            }),
        }))
    }

    async fn bulk_write_collection(
        &self,
        _request: Request<Streaming<proto::BulkWriteCollectionRequest>>,
    ) -> Result<Response<proto::BulkWriteCollectionReply>, Status> {
        self.unimplemented("bulk_write_collection")
    }

    async fn query_collection(
        &self,
        _request: Request<proto::QueryCollectionRequest>,
    ) -> Result<Response<proto::QueryCollectionReply>, Status> {
        self.unimplemented("query_collection")
    }

    async fn get_collection_stats(
        &self,
        _request: Request<proto::GetCollectionStatsRequest>,
    ) -> Result<Response<proto::CollectionStatsReply>, Status> {
        self.unimplemented("get_collection_stats")
    }

    async fn flush_collection(
        &self,
        request: Request<proto::FlushCollectionRequest>,
    ) -> Result<Response<proto::SnapshotReply>, Status> {
        self.next("flush_collection")?;
        let request = request.into_inner();
        Ok(Response::new(proto::SnapshotReply {
            manifest_generation: 2,
            visible_seq_no: 0,
            database_name: request.database_name,
            collection_name: request.collection_name,
        }))
    }

    async fn compact_collection(
        &self,
        _request: Request<proto::CompactCollectionRequest>,
    ) -> Result<Response<proto::SnapshotReply>, Status> {
        self.unimplemented("compact_collection")
    }

    async fn inspect_collection(
        &self,
        _request: Request<proto::InspectCollectionRequest>,
    ) -> Result<Response<proto::InspectCollectionReply>, Status> {
        self.unimplemented("inspect_collection")
    }

    async fn put_database_policy(
        &self,
        _request: Request<proto::PutDatabasePolicyRequest>,
    ) -> Result<Response<proto::DatabaseAccessPolicyReply>, Status> {
        self.unimplemented("put_database_policy")
    }

    async fn get_database_policy(
        &self,
        _request: Request<proto::GetDatabasePolicyRequest>,
    ) -> Result<Response<proto::DatabaseAccessPolicyReply>, Status> {
        self.unimplemented("get_database_policy")
    }
}
