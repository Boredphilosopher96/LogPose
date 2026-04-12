//! gRPC API surface for LogPose.

use logpose_core::AppState;
use std::{net::SocketAddr, sync::Arc};
use tonic::{Request, Response, Status, transport::Server};
use tonic_health::server::health_reporter;
use tracing::info;

#[allow(missing_docs)]
/// Generated protobuf interfaces.
pub mod proto {
    tonic::include_proto!("logpose.v1");
}

use proto::log_pose_service_server::{LogPoseService, LogPoseServiceServer};
use proto::{GetMetadataReply, GetMetadataRequest};

/// Serve the gRPC API until shutdown.
pub async fn serve(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = SocketAddr::from((
        state.config.grpc_host.parse::<std::net::IpAddr>()?,
        state.config.grpc_port,
    ));

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<LogPoseServiceServer<GrpcLogPoseService>>()
        .await;

    info!(%address, "starting gRPC listener");

    Server::builder()
        .add_service(health_service)
        .add_service(LogPoseServiceServer::new(GrpcLogPoseService { state }))
        .serve(address)
        .await?;

    Ok(())
}

/// gRPC service implementation scaffold.
#[derive(Clone)]
pub struct GrpcLogPoseService {
    state: Arc<AppState>,
}

#[tonic::async_trait]
impl LogPoseService for GrpcLogPoseService {
    async fn get_metadata(
        &self,
        _request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataReply>, Status> {
        Ok(Response::new(GetMetadataReply {
            product: "LogPose".to_owned(),
            node_name: self.state.config.node_name.clone(),
            version: self.state.build.version.clone(),
        }))
    }
}
