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
rest_port = 8080
grpc_host = "0.0.0.0"
grpc_port = 50051
log_filter = "info,logpose=debug"
storage_root = ".logpose-edge-a"'
```

When `node_role` is omitted it defaults to `combined`. When `LOGPOSE_CONFIG` is provided, the remaining fields should still be present in the TOML payload.

`storage_root` belongs to one server process at a time. On startup the server takes an exclusive lock on `storage_root/LOCK` and holds it until exit; a second server pointed at the same directory exits with an error naming the directory and the pid holding it. The lock is released automatically when the owning process exits, so a stale `LOCK` file needs no cleanup. The lock is an advisory `flock`, so `storage_root` must live on a filesystem that supports it: startup fails with an error naming `LOCK` on filesystems that reject file locks, and network mounts that only lock locally (for example NFS mounted with `nolock`) cannot keep servers on different hosts apart.

`node_name` must not be `local`. That token is reserved for anonymous local placement metadata created by raw storage-engine workflows.
