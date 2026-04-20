# Configuration

LogPose currently loads default bootstrap settings or a TOML string from `LOGPOSE_CONFIG`.

Default endpoints:

- REST: `127.0.0.1:8080`
- gRPC: `127.0.0.1:50051`

Example:

```bash
export LOGPOSE_CONFIG='node_name = "edge-a"
node_role = "combined"
rest_host = "0.0.0.0"
rest_advertise_host = "edge-a.internal"
rest_port = 8080
grpc_host = "0.0.0.0"
grpc_advertise_host = "edge-a.internal"
grpc_port = 50051
log_filter = "info,logpose=debug"
storage_root = ".logpose-edge-a"

[internal]
replica_token = "cluster-internal-secret"
replica_transfer_timeout_ms = 30000
```

When `node_role` is omitted it defaults to `combined`. When `LOGPOSE_CONFIG` is provided, the remaining fields should still be present in the TOML payload.

`node_name` must not be `local`. That token is reserved for anonymous local placement metadata created by raw storage-engine workflows.

When a node binds on `0.0.0.0` or `::`, set `rest_advertise_host` and `grpc_advertise_host`
to the peer-reachable hostname or address that other nodes and operators should use.

For etcd-backed `combined` and `data` nodes, `internal.replica_token` is required because
background replica repair uses authenticated node-to-node REST. If `rest_host` or `grpc_host`
is wildcard or loopback-only in that topology, set `rest_advertise_host` and
`grpc_advertise_host` explicitly to the peer-reachable hostname or address that
other nodes and operators should use. Loopback or otherwise non-routable
advertised endpoints are rejected by default for etcd-backed data-serving
nodes; the only supported escape hatch is
`internal.allow_non_routable_rest_advertise_host = true` for deliberate
single-host development or test setups.
