# Configuration

LogPose currently loads default bootstrap settings or a TOML string from `LOGPOSE_CONFIG`.

Default endpoints:

- REST: `127.0.0.1:8080`
- gRPC: `127.0.0.1:50051`

Example:

```bash
export LOGPOSE_CONFIG='node_name = "edge-a"
rest_host = "0.0.0.0"
rest_port = 8080
grpc_host = "0.0.0.0"
grpc_port = 50051
log_filter = "info,logpose=debug"'
```

