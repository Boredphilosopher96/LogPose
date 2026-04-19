# Podman Chaos

PR4's local chaos workflow is the operator-facing way to exercise LogPose's
etcd-backed coordination path with real `logpose-server` processes under
Podman. Treat it as a local fault-injection lab, not as a production
orchestration surface.

This page documents the checked-in harness contract, the helper surface it
depends on, and the invariants each scenario is meant to exercise.

## Current Contract

The current repo truth for the local chaos flow is:

- `deploy/Dockerfile` builds the container image used for the local lab
- `scripts/podman-chaos.sh` is the checked-in wrapper for bootstrap, status,
  failure injection, and teardown
- `scripts/check-chaos.sh` is the repo-owned verification entrypoint for the
  shell harness contract
- `crates/logpose-storage-etcd/examples/etcd_coordination_admin.rs` is the
  operator-facing helper for membership, leader, and shard-owner inspection or
  promotion
- `crates/logpose-core/tests/etcd_metadata.rs` remains the authoritative
  in-repo safety coverage for lease loss, leadership handoff, ownership
  fencing, and fail-closed read barriers after promotion

There is still no public REST, gRPC, or CLI API for shard promotion. Local
failover drills that move ownership must use the etcd helper instead of a
server endpoint.

`scripts/check-chaos.sh` fixes the wrapper contract around these commands:

- `help`
- `list-nodes`
- `list-scenarios`
- `render-config <node> [--cluster <name>]`
- `bootstrap [--cluster <name>]`
- `teardown [--cluster <name>]`
- `reset [--cluster <name>]`
- `status [--cluster <name>] [collection]`
- `scenario <name> [--cluster <name>]`
- `self-test`

The checked node set is `node-a`, `node-b`, and `node-c`.

The checked scenario set is:

- `smoke`
- `new-node-registration`
- `concurrent-writers`
- `owner-failover`
- `leader-failover`
- `lagging-node-rejoin`
- `etcd-outage`
- `partition-heal`

## Topology

The harness brings up one Podman network, three etcd members, and three
LogPose nodes:

| Component | Count | Published ports | Purpose |
| --------- | ----- | --------------- | ------- |
| `etcd-1` | 1 | `12379 -> 2379` | cluster metadata quorum member |
| `etcd-2` | 1 | `22379 -> 2379` | cluster metadata quorum member |
| `etcd-3` | 1 | `32379 -> 2379` | cluster metadata quorum member |
| `node-a` | 1 | REST `18080`, gRPC `15051` | combined node, normal initial owner and leader |
| `node-b` | 1 | REST `18081`, gRPC `15052` | combined node, normal promotion target |
| `node-c` | 1 | REST `18082`, gRPC `15053` | data node, registration and partition target |

All LogPose nodes share the same `metadata.etcd.cluster_name`,
`metadata.etcd.key_prefix`, and endpoint set, but each node gets its own local
`storage_root`.

The first published etcd client port is `12379` instead of `2379` so the Podman
lab does not collide with a developer's local standalone etcd.

`node-a` and `node-b` are combined nodes because the current promotion drills
expect the promoted owner to be able to serve both placement inspection and
data-plane traffic. `node-c` is intentionally data-only so the local lab covers
role-asymmetric membership behavior too.

The wrapper defaults to:

- cluster name `podman-chaos`
- state root `.logpose/podman-chaos/<cluster>`
- image tag `localhost/logpose-chaos:dev`
- etcd key prefix `/logpose/chaos`
- machine name `podman-machine-default`

Use `render-config` instead of hand-writing node configs. The current rendered
shape for `node-a` is:

```toml
node_name = "node-a"
node_role = "combined"
rest_host = "0.0.0.0"
rest_port = 8080
grpc_host = "0.0.0.0"
grpc_port = 50051
log_filter = "info,logpose=debug"
storage_root = "/var/lib/logpose"

[metadata]
backend = "etcd"

[metadata.etcd]
endpoints = ["http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"]
key_prefix = "/logpose/chaos"
timeout_ms = 1500
membership_ttl_secs = 4
leadership_ttl_secs = 3
cluster_name = "podman-chaos"
```

Change only the cluster name, node name, and host-side mount roots through the
wrapper. Keep the etcd metadata section shared across the cluster.

## Prerequisites

- Podman installed on the host
- a working Podman machine connection if you are on macOS
- Rust and Cargo, because the harness builds the local CLI and the etcd admin
  helper before it starts the lab
- enough local ports for three etcd clients plus the three REST and three gRPC
  listeners

The wrapper builds the server image from the workspace and manages the etcd
containers itself. It does not require a pre-existing external etcd cluster.

## State Directory

By default the wrapper writes cluster state under
`.logpose/podman-chaos/<sanitized-cluster>/`:

```text
.logpose/podman-chaos/<cluster>/
  cli/
    node-a/
    node-b/
    node-c/
  config/
    node-a.toml
    node-b.toml
    node-c.toml
  data/
    node-a/
    node-b/
    node-c/
  etcd/
    etcd-1/
    etcd-2/
    etcd-3/
  tmp/
```

Operator rules:

- the wrapper binds `config/<node>.toml` into each container and mounts
  `data/<node>/` as `/var/lib/logpose`
- never share a `storage_root` across two nodes
- `bootstrap` deletes and recreates the selected cluster state directory before
  starting a fresh lab for that cluster name
- authoritative coordination keys live under
  `<key_prefix>/clusters/<cluster_name>/...`
- enabling etcd-backed metadata does not remove local storage; WAL, manifests,
  segments, indexes, and database descriptors still live under each node's
  `storage_root`
- if you expect a promoted owner to serve local reads or writes immediately,
  mirror the collection's local state into that node's `storage_root` before
  the ownership move

Optional overrides:

- `LOGPOSE_PODMAN_CHAOS_CLUSTER`
- `LOGPOSE_PODMAN_CHAOS_STATE_DIR`
- `LOGPOSE_PODMAN_CHAOS_IMAGE`
- `LOGPOSE_PODMAN_CHAOS_REBUILD_IMAGE=1`
- `LOGPOSE_PODMAN_CHAOS_ETCD_IMAGE`
- `LOGPOSE_PODMAN_CHAOS_KEY_PREFIX`
- `LOGPOSE_PODMAN_MACHINE_NAME`

The harness reuses an existing `LOGPOSE_PODMAN_CHAOS_IMAGE` by default. Set
`LOGPOSE_PODMAN_CHAOS_REBUILD_IMAGE=1` when you need to force a fresh image
build after local code changes.

## Major Commands

Render the exact node config the harness will run:

```bash
./scripts/podman-chaos.sh render-config node-a --cluster pr4-chaos
```

Bootstrap a fresh local cluster:

```bash
./scripts/podman-chaos.sh bootstrap --cluster pr4-chaos
```

`bootstrap` recreates `.logpose/podman-chaos/<cluster>/` from scratch for the
selected cluster name before it starts containers. Use `teardown` when you want
to stop the lab without immediately deleting the on-disk state, and use
`reset` when you want an explicit stop-and-delete command.

Inspect runtime status, optionally with collection placement:

```bash
./scripts/podman-chaos.sh status --cluster pr4-chaos
./scripts/podman-chaos.sh status documents --cluster pr4-chaos
```

Run the checked harness contract:

```bash
./scripts/check-chaos.sh
./scripts/check-chaos.sh --integration --cluster pr4-chaos
```

Run named scenarios directly:

```bash
./scripts/podman-chaos.sh scenario smoke --cluster pr4-chaos
./scripts/podman-chaos.sh scenario owner-failover --cluster pr4-chaos
./scripts/podman-chaos.sh scenario partition-heal --cluster pr4-chaos
```

Tear down or fully reset the lab:

```bash
./scripts/podman-chaos.sh teardown --cluster pr4-chaos
./scripts/podman-chaos.sh reset --cluster pr4-chaos
```

Because ownership promotion is not exposed as a public server API, use the
checked-in etcd helper for ownership inspection or promotion drills:

```bash
LOGPOSE_ETCD_ENDPOINTS=http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 \
LOGPOSE_ETCD_CLUSTER=pr4-chaos \
LOGPOSE_ETCD_KEY_PREFIX=/logpose/chaos \
cargo run -p logpose-storage-etcd --example etcd_coordination_admin -- \
  list-membership

LOGPOSE_ETCD_ENDPOINTS=http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 \
LOGPOSE_ETCD_CLUSTER=pr4-chaos \
LOGPOSE_ETCD_KEY_PREFIX=/logpose/chaos \
cargo run -p logpose-storage-etcd --example etcd_coordination_admin -- \
  promote-shard-owner documents node-b
```

## Scenarios And Invariants

Each scenario exists to verify safety properties, not just that containers
start.

### `smoke`

Use `smoke` to validate the happy-path cluster shape before injecting faults.

Required invariants:

- etcd becomes reachable and all three LogPose nodes register membership under
  one cluster name
- collection creation succeeds through the current owner
- writes return a snapshot that can be reused as a read barrier on the same
  node
- the live record count matches the inserted batch

### `new-node-registration`

Use `new-node-registration` to verify that a late node join becomes visible to
the existing cluster without destabilizing leadership.

Required invariants:

- the cluster starts with two nodes and later admits `node-c`
- existing members observe the new node in `registered_members`
- the active leader remains leader throughout the registration event

### `concurrent-writers`

Use `concurrent-writers` to race two writers against the same collection.

Required invariants:

- at least one writer succeeds
- the final record count matches the number of successful writers times the
  batch size
- placement still resolves to a single owner node

### `owner-failover`

Use `owner-failover` to validate explicit ownership promotion after the current
owner disappears.

Required invariants:

- the old owner can be stopped while a write is in flight
- shard ownership is promoted through etcd compare-and-swap, not blind update
- post-promotion read barriers fail closed until replica freshness metadata
  exists

### `leader-failover`

Use `leader-failover` for a combined-node control-plane handoff with an
explicit owner promotion.

Required invariants:

- the stopped node disappears from visible membership
- a surviving combined node becomes the promoted owner
- new writes succeed on the promoted owner after local state is mirrored

### `lagging-node-rejoin`

Use `lagging-node-rejoin` to validate that a previously removed node
re-registers cleanly.

Required invariants:

- the removed node disappears from the visible membership set
- the restarted node reappears without forcing a full cluster reset

### `etcd-outage`

Use `etcd-outage` to validate fail-closed behavior after etcd quorum loss.

Required invariants:

- stopping two etcd members removes quorum for the metadata backend
- the node eventually reports fail-closed runtime status with coordination
  errors rather than silently serving stale metadata
- control-plane mutation attempts fail with an etcd metadata error while quorum
  is unavailable

### `partition-heal`

Use `partition-heal` to validate temporary network isolation of a data node.

Required invariants:

- disconnecting the node removes it from visible membership
- reconnecting the node restores it to visible membership

## Ownership Promotion Drill

The helper-driven ownership promotion drill remains separate from the normal
REST, gRPC, and CLI operator path.

Required invariants:

- there is still no public server API for shard promotion
- promotion is compare-and-swap based, so stale promotions must conflict
- after promotion, the old owner must reject reads and writes as not locally
  served
- placement inspection must show the new `owner_node` and incremented
  `ownership_epoch`
- read barriers remain fail-closed on the promoted owner until replica
  freshness metadata exists
