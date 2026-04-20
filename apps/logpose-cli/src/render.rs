use crate::action::{Action, format_command, metric_name};
use crate::cli::OutputMode;
use anyhow::Context;
use logpose_auth::DatabaseAccessPolicy;
use logpose_catalog::{CollectionDescriptor, DatabaseDescriptor};
use logpose_client::ScopedCollectionResponse;
use logpose_config::LogPoseConfig;
use logpose_query::QueryResponse;
use logpose_storage::InspectReport;
use logpose_types::{
    CollectionPlacement, CollectionStats, CommitAck, DEFAULT_DATABASE_NAME, NodeMembershipStatus,
    NodeRuntimeStatus, Snapshot,
};
use serde::Serialize;

pub enum ActionOutput {
    Status(NodeRuntimeStatus),
    Config(LogPoseConfig),
    NodeMembership(NodeMembershipStatus),
    NodeDrained(NodeMembershipStatus),
    NodeUndrained(NodeMembershipStatus),
    DatabaseShown(DatabaseDescriptor),
    DatabaseUpdated(DatabaseDescriptor),
    DatabasesListed(Vec<DatabaseDescriptor>),
    DatabasePolicyShown(DatabaseAccessPolicy),
    DatabasePolicyUpdated(DatabaseAccessPolicy),
    CollectionCreated(CollectionDescriptor),
    CollectionShown(CollectionDescriptor),
    CollectionStats(CollectionStats),
    CollectionPlacement(CollectionPlacement),
    CollectionPromoted(CollectionPlacement),
    CollectionRebalanced(CollectionPlacement),
    CollectionFlushed(ScopedCollectionResponse<Snapshot>),
    CollectionCompacted(ScopedCollectionResponse<Snapshot>),
    RecordsWritten(ScopedCollectionResponse<CommitAck>),
    RecordDeleted(ScopedCollectionResponse<CommitAck>),
    Query(ScopedCollectionResponse<QueryResponse>),
    Inspect(ScopedCollectionResponse<InspectReport>),
}

impl ActionOutput {
    pub fn render_direct(&self, mode: OutputMode) -> anyhow::Result<()> {
        match mode {
            OutputMode::Human => {
                println!("{}", self.human_text()?);
                Ok(())
            }
            OutputMode::Json => self.write_json(),
        }
    }

    pub fn human_text(&self) -> anyhow::Result<String> {
        Ok(match self {
            ActionOutput::Status(status) => render_status(status),
            ActionOutput::Config(config) => render_config(config),
            ActionOutput::NodeMembership(status) => {
                render_node_membership("Node Membership", status)
            }
            ActionOutput::NodeDrained(status) => render_node_membership("Node Drained", status),
            ActionOutput::NodeUndrained(status) => render_node_membership("Node Ready", status),
            ActionOutput::DatabaseShown(descriptor) => render_database("Database", descriptor),
            ActionOutput::DatabaseUpdated(descriptor) => {
                render_database("Database updated", descriptor)
            }
            ActionOutput::DatabasesListed(descriptors) => render_databases(descriptors),
            ActionOutput::DatabasePolicyShown(policy) => {
                render_database_policy("Database Policy", policy)
            }
            ActionOutput::DatabasePolicyUpdated(policy) => {
                render_database_policy("Database policy updated", policy)
            }
            ActionOutput::CollectionCreated(descriptor) => format!(
                "Collection created\nCollection: {}\nDimensions: {}\nMetric: {}\nReplication factor: {}",
                collection_identity(&descriptor.database_name, &descriptor.name),
                descriptor.dimensions,
                metric_name(descriptor.metric),
                descriptor.replication_factor
            ),
            ActionOutput::CollectionShown(descriptor) => format!(
                "Collection\nCollection: {}\nDimensions: {}\nMetric: {}\nReplication factor: {}",
                collection_identity(&descriptor.database_name, &descriptor.name),
                descriptor.dimensions,
                metric_name(descriptor.metric),
                descriptor.replication_factor
            ),
            ActionOutput::CollectionStats(stats) => render_stats(stats),
            ActionOutput::CollectionPlacement(placement) => render_placement(placement),
            ActionOutput::CollectionPromoted(placement) => render_placement(placement),
            ActionOutput::CollectionRebalanced(placement) => render_placement(placement),
            ActionOutput::CollectionFlushed(snapshot) => format!(
                "Collection flushed\nCollection: {}\nManifest generation: {}\nVisible sequence number: {}",
                collection_identity(&snapshot.database_name, &snapshot.collection_name),
                snapshot.manifest_generation,
                snapshot.visible_seq_no
            ),
            ActionOutput::CollectionCompacted(snapshot) => format!(
                "Collection compacted\nCollection: {}\nManifest generation: {}\nVisible sequence number: {}",
                collection_identity(&snapshot.database_name, &snapshot.collection_name),
                snapshot.manifest_generation,
                snapshot.visible_seq_no
            ),
            ActionOutput::RecordsWritten(ack) => format!(
                "Write completed\nCollection: {}\nApplied operations: {}\nLast sequence number: {}\nWrite snapshot: generation {}, seq {}",
                collection_identity(&ack.database_name, &ack.collection_name),
                ack.applied_ops,
                ack.last_seq_no,
                ack.snapshot.manifest_generation,
                ack.snapshot.visible_seq_no,
            ),
            ActionOutput::RecordDeleted(ack) => format!(
                "Delete completed\nCollection: {}\nApplied operations: {}\nLast sequence number: {}\nWrite snapshot: generation {}, seq {}",
                collection_identity(&ack.database_name, &ack.collection_name),
                ack.applied_ops,
                ack.last_seq_no,
                ack.snapshot.manifest_generation,
                ack.snapshot.visible_seq_no,
            ),
            ActionOutput::Query(response) => render_query(response)?,
            ActionOutput::Inspect(report) => format!(
                "Inspection: {}\nCollection: {}\n{}",
                report.target,
                collection_identity(&report.database_name, &report.collection_name),
                serde_json::to_string_pretty(&report.payload)
                    .context("failed to serialize inspect payload")?
            ),
        })
    }

    pub fn json_text(&self) -> anyhow::Result<String> {
        match self {
            ActionOutput::Status(status) => pretty_json(status),
            ActionOutput::Config(config) => pretty_json(&redacted_config_json(config)?),
            ActionOutput::NodeMembership(status)
            | ActionOutput::NodeDrained(status)
            | ActionOutput::NodeUndrained(status) => pretty_json(status),
            ActionOutput::DatabaseShown(descriptor) | ActionOutput::DatabaseUpdated(descriptor) => {
                pretty_json(descriptor)
            }
            ActionOutput::DatabasesListed(descriptors) => pretty_json(descriptors),
            ActionOutput::DatabasePolicyShown(policy)
            | ActionOutput::DatabasePolicyUpdated(policy) => pretty_json(policy),
            ActionOutput::CollectionCreated(descriptor)
            | ActionOutput::CollectionShown(descriptor) => pretty_json(descriptor),
            ActionOutput::CollectionStats(stats) => pretty_json(stats),
            ActionOutput::CollectionPlacement(placement)
            | ActionOutput::CollectionPromoted(placement)
            | ActionOutput::CollectionRebalanced(placement) => pretty_json(placement),
            ActionOutput::CollectionFlushed(snapshot)
            | ActionOutput::CollectionCompacted(snapshot) => pretty_json(snapshot),
            ActionOutput::RecordsWritten(ack) | ActionOutput::RecordDeleted(ack) => {
                pretty_json(ack)
            }
            ActionOutput::Query(response) => pretty_json(response),
            ActionOutput::Inspect(report) => pretty_json(report),
        }
    }

    pub fn write_json(&self) -> anyhow::Result<()> {
        println!("{}", self.json_text()?);
        Ok(())
    }

    pub fn title(&self) -> &'static str {
        match self {
            ActionOutput::Status(_) => "Runtime Status",
            ActionOutput::Config(_) => "Configuration",
            ActionOutput::NodeMembership(_) => "Node Membership",
            ActionOutput::NodeDrained(_) => "Node Drained",
            ActionOutput::NodeUndrained(_) => "Node Ready",
            ActionOutput::DatabaseShown(_) => "Database",
            ActionOutput::DatabaseUpdated(_) => "Database Updated",
            ActionOutput::DatabasesListed(_) => "Databases",
            ActionOutput::DatabasePolicyShown(_) => "Database Policy",
            ActionOutput::DatabasePolicyUpdated(_) => "Database Policy Updated",
            ActionOutput::CollectionCreated(_) => "Collection Created",
            ActionOutput::CollectionShown(_) => "Collection",
            ActionOutput::CollectionStats(_) => "Collection Statistics",
            ActionOutput::CollectionPlacement(_) => "Collection Placement",
            ActionOutput::CollectionPromoted(_) => "Collection Promoted",
            ActionOutput::CollectionRebalanced(_) => "Collection Rebalanced",
            ActionOutput::CollectionFlushed(_) => "Collection Flushed",
            ActionOutput::CollectionCompacted(_) => "Collection Compacted",
            ActionOutput::RecordsWritten(_) => "Write Completed",
            ActionOutput::RecordDeleted(_) => "Delete Completed",
            ActionOutput::Query(_) => "Query Results",
            ActionOutput::Inspect(_) => "Inspection",
        }
    }
}

pub fn command_preview(action: &Action) -> String {
    format_command(action)
}

fn render_status(status: &NodeRuntimeStatus) -> String {
    let mut lines = vec![
        "Runtime Status".to_owned(),
        format!("Node: {}", status.metadata.node_name),
        format!("Role: {}", status.role.as_str()),
        format!("REST: {}", status.rest_endpoint),
        format!("gRPC: {}", status.grpc_endpoint),
        format!("Storage: {}", status.storage_engine),
        format!(
            "Control Plane Ready: {}",
            yes_no(status.control_plane_ready)
        ),
        format!("Data Plane Ready: {}", yes_no(status.data_plane_ready)),
        format!("Collections: {}", status.collection_count),
        format!(
            "Maintenance Pending: {}",
            status.maintenance.pending_operations
        ),
    ];

    if let Some(coordination) = &status.coordination {
        lines.push(format!("Cluster: {}", coordination.cluster_name));
        lines.push(format!(
            "Membership Registered: {}",
            yes_no(coordination.membership_registered)
        ));
        if let Some(state) = &coordination.membership_state {
            lines.push(format!("Membership State: {state}"));
        }
        if let Some(lease_id) = coordination.membership_lease_id {
            lines.push(format!("Membership Lease: {lease_id}"));
        }
        lines.push(format!(
            "Leader: {}",
            coordination
                .leader_node
                .as_deref()
                .map(|leader| {
                    if coordination.is_local_leader {
                        format!("{leader} (local leader)")
                    } else {
                        leader.to_owned()
                    }
                })
                .unwrap_or_else(|| "none".to_owned())
        ));
        if let Some(lease_id) = coordination.leadership_lease_id {
            lines.push(format!("Leadership Lease: {lease_id}"));
        }
        if let Some(revision) = coordination.metadata_revision {
            lines.push(format!("Metadata Revision: {revision}"));
        }
        if let Some(watch_lag) = coordination.watch_lag {
            lines.push(format!("Watch Lag: {watch_lag}"));
        }
        lines.push(format!(
            "Members: {}",
            if coordination.registered_members.is_empty() {
                "none".to_owned()
            } else {
                coordination.registered_members.join(", ")
            }
        ));
        if let Some(error) = &coordination.last_error {
            lines.push(format!("Coordination Error: {error}"));
        }
    }

    if !status.collections.is_empty() {
        lines.push("Placements:".to_owned());
        for placement in status.collections.iter().take(5) {
            let mut summary = format!(
                "  - {} -> {} ({})",
                collection_identity(&placement.database_name, &placement.collection_name),
                placement.assigned_node,
                placement.route_kind
            );
            if let Some(owner) = &placement.owner_node {
                summary.push_str(&format!(", owner={owner}"));
            }
            if let Some(epoch) = placement.ownership_epoch {
                summary.push_str(&format!(", epoch={epoch}"));
            }
            if !placement.replicas.is_empty() {
                summary.push_str(&format!(", replicas={}", placement.replicas.len()));
            }
            lines.push(summary);
        }
    }

    lines.join("\n")
}

fn render_config(config: &LogPoseConfig) -> String {
    format!(
        "Configuration\nNode name: {}\nRole: {}\nREST: {}\ngRPC: {}\nStorage root: {}\nLog filter: {}",
        config.node_name,
        config.node_role.as_str(),
        format_host_port(&config.rest_host, config.rest_port),
        format_host_port(&config.grpc_host, config.grpc_port),
        config.storage_root.display(),
        config.log_filter
    )
}

fn redacted_config_json(config: &LogPoseConfig) -> anyhow::Result<serde_json::Value> {
    let mut value = serde_json::to_value(config).context("failed to serialize configuration")?;
    let Some(tokens) = value
        .pointer_mut("/auth/bootstrap_tokens")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return Ok(value);
    };
    for token in tokens {
        if let Some(object) = token.as_object_mut() {
            object.insert(
                "token".to_owned(),
                serde_json::Value::String("<redacted>".to_owned()),
            );
        }
    }
    Ok(value)
}

fn render_database_policy(title: &str, policy: &DatabaseAccessPolicy) -> String {
    let mut lines = vec![
        title.to_owned(),
        format!("Database: {}", policy.database_name),
        format!("Authentication mode: {}", authentication_mode_name(policy)),
    ];
    if policy.role_bindings.is_empty() {
        lines.push("Role bindings: none".to_owned());
    } else {
        lines.push("Role bindings:".to_owned());
        for binding in &policy.role_bindings {
            lines.push(format!(
                "  - {} => {}",
                binding.principal_name,
                database_role_name(binding.role.clone())
            ));
        }
    }
    lines.join("\n")
}

fn render_database(title: &str, descriptor: &DatabaseDescriptor) -> String {
    format!(
        "{title}\nDatabase: {}\nDefault: {}",
        descriptor.name,
        yes_no(descriptor.is_default)
    )
}

fn render_node_membership(title: &str, status: &NodeMembershipStatus) -> String {
    format!(
        "{title}\nNode: {}\nRole: {}\nState: {}",
        status.node_id,
        status.node_role.as_str(),
        status.state
    )
}

fn render_databases(descriptors: &[DatabaseDescriptor]) -> String {
    let mut lines = vec![format!("Databases ({})", descriptors.len())];
    for descriptor in descriptors {
        lines.push(format!(
            "- {}{}",
            descriptor.name,
            if descriptor.is_default {
                " (default)"
            } else {
                ""
            }
        ));
    }
    lines.join("\n")
}

fn authentication_mode_name(policy: &DatabaseAccessPolicy) -> &'static str {
    match policy.authentication_mode {
        logpose_auth::AuthenticationMode::Disabled => "disabled",
        logpose_auth::AuthenticationMode::Password => "password",
        logpose_auth::AuthenticationMode::MutualTls => "mutual_tls",
        logpose_auth::AuthenticationMode::ExternalToken => "external_token",
    }
}

fn database_role_name(role: logpose_auth::DatabaseRole) -> &'static str {
    match role {
        logpose_auth::DatabaseRole::Owner => "owner",
        logpose_auth::DatabaseRole::ReadWrite => "read_write",
        logpose_auth::DatabaseRole::ReadOnly => "read_only",
    }
}

fn format_host_port(host: &str, port: u16) -> String {
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{host}]:{port}"),
        Ok(std::net::IpAddr::V4(_)) | Err(_) => format!("{host}:{port}"),
    }
}

fn render_stats(stats: &CollectionStats) -> String {
    format!(
        "Collection Statistics\nCollection: {}\nManifest generation: {}\nVisible sequence number: {}\nLive records: {}\nDeleted records: {}\nMutable operations: {}\nSegments: {}\nPending maintenance: {}",
        collection_identity(&stats.database_name, &stats.collection_name),
        stats.manifest_generation,
        stats.visible_seq_no,
        stats.live_record_count,
        stats.deleted_record_count,
        stats.mutable_op_count,
        stats.segment_count,
        stats.maintenance.pending.len()
    )
}

fn render_placement(placement: &CollectionPlacement) -> String {
    let mut lines = vec![
        "Collection Placement".to_owned(),
        format!(
            "Collection: {}",
            collection_identity(&placement.database_name, &placement.collection_name)
        ),
        format!("Assigned node: {}", placement.assigned_node),
        format!("Assigned role: {}", placement.assigned_role.as_str()),
    ];
    if let Some(owner) = &placement.owner_node {
        lines.push(format!("Owner node: {owner}"));
    }
    if let Some(epoch) = placement.ownership_epoch {
        lines.push(format!("Ownership epoch: {epoch}"));
    }
    if let Some(revision) = placement.metadata_revision {
        lines.push(format!("Metadata revision: {revision}"));
    }
    if let Some(reason) = &placement.failover_reason {
        lines.push(format!("Failover reason: {reason}"));
    }
    if placement.replicas.is_empty() {
        lines.push("Replicas: none".to_owned());
    } else {
        lines.push("Replicas:".to_owned());
        for replica in &placement.replicas {
            lines.push(format!(
                "  - {} ({}, {})",
                replica.node_id,
                replica.node_role.as_str(),
                replica.state
            ));
        }
    }
    lines.push(format!("Route kind: {}", placement.route_kind));
    lines.push(format!("Reason: {}", placement.route_reason));
    lines.join("\n")
}

fn render_query(response: &ScopedCollectionResponse<QueryResponse>) -> anyhow::Result<String> {
    let mut lines = vec![
        "Query Results".to_owned(),
        format!(
            "Collection: {}",
            collection_identity(&response.database_name, &response.collection_name)
        ),
        format!("Metric: {}", metric_name(response.metric)),
        format!("Returned: {}/{}", response.returned, response.top_k),
        format!(
            "Snapshot: generation {}, visible seq {}",
            response.snapshot.manifest_generation, response.snapshot.visible_seq_no
        ),
    ];
    if response.matches.is_empty() {
        lines.push("Matches: none".to_owned());
    } else {
        lines.push("Matches:".to_owned());
        for (index, item) in response.matches.iter().enumerate() {
            lines.push(format!(
                "  {}. {}  value={}  metadata={}",
                index + 1,
                item.id,
                item.value,
                compact_json(&item.metadata)?
            ));
        }
    }
    if let Some(diagnostics) = &response.diagnostics {
        lines.push("Diagnostics:".to_owned());
        lines.push(format!("  chosen plan: {:?}", diagnostics.chosen_plan));
        lines.push(format!("  units scanned: {}", diagnostics.units_scanned));
        lines.push(format!(
            "  candidates reranked: {}",
            diagnostics.candidates_reranked
        ));
    }
    Ok(lines.join("\n"))
}

fn compact_json<T: Serialize>(value: &T) -> anyhow::Result<String> {
    serde_json::to_string(value).context("failed to serialize compact json")
}

fn pretty_json<T: Serialize>(value: &T) -> anyhow::Result<String> {
    serde_json::to_string_pretty(value).context("failed to serialize json output")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn collection_identity(database_name: &str, collection_name: &str) -> String {
    if database_name == DEFAULT_DATABASE_NAME {
        collection_name.to_owned()
    } else {
        format!("{database_name}/{collection_name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_auth::{AccessTier, Principal, PrincipalKind};
    use logpose_config::{AuthConfig, BootstrapTokenConfig};
    use std::path::PathBuf;

    #[test]
    fn collection_descriptors_render_database_scoped_identity() {
        let output = ActionOutput::CollectionShown(CollectionDescriptor {
            collection_id: logpose_types::CollectionId::default(),
            database_name: "analytics".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: logpose_types::DistanceMetric::Dot,
            replication_factor: 1,
            root_path: PathBuf::from("/tmp/documents"),
            remote_blob: None,
            flush_threshold_ops: 10,
            flush_threshold_bytes: 20,
            compaction_threshold_segments: 3,
        })
        .human_text()
        .expect("descriptor should render");

        assert!(output.contains("analytics/documents"));
        assert!(!output.contains("acme/analytics/documents"));
    }

    #[test]
    fn scoped_write_results_render_database_scoped_identity() {
        let output = ActionOutput::RecordsWritten(ScopedCollectionResponse {
            database_name: "analytics".to_owned(),
            collection_name: "documents".to_owned(),
            response: CommitAck {
                last_seq_no: 7,
                applied_ops: 2,
                snapshot: Snapshot {
                    manifest_generation: 3,
                    visible_seq_no: 7,
                },
            },
        })
        .human_text()
        .expect("write result should render");

        assert!(output.contains("analytics/documents"));
        assert!(!output.contains("acme/analytics/documents"));
        assert!(output.contains("Applied operations: 2"));
        assert!(output.contains("Write snapshot: generation 3, seq 7"));
    }

    #[test]
    fn default_database_collections_render_without_a_namespace_prefix() {
        let output = ActionOutput::CollectionShown(CollectionDescriptor {
            collection_id: logpose_types::CollectionId::default(),
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: logpose_types::DistanceMetric::Dot,
            replication_factor: 1,
            root_path: PathBuf::from("/tmp/documents"),
            remote_blob: None,
            flush_threshold_ops: 10,
            flush_threshold_bytes: 20,
            compaction_threshold_segments: 3,
        })
        .human_text()
        .expect("descriptor should render");

        assert!(output.contains("Collection: documents"));
        assert!(!output.contains("default/default/documents"));
    }

    #[test]
    fn config_json_redacts_bootstrap_tokens() {
        let output = ActionOutput::Config(LogPoseConfig {
            auth: AuthConfig {
                bootstrap_tokens: vec![BootstrapTokenConfig {
                    token: "super-secret".to_owned(),
                    principal: Principal::new_with_access_tier(
                        "ops-admin",
                        PrincipalKind::User,
                        AccessTier::Operator,
                    ),
                }],
            },
            ..LogPoseConfig::default()
        })
        .json_text()
        .expect("config json should render");

        assert!(output.contains("\"token\": \"<redacted>\""));
        assert!(!output.contains("super-secret"));
    }

    #[test]
    fn status_render_includes_coordination_summary_when_present() {
        let output = ActionOutput::Status(NodeRuntimeStatus {
            metadata: logpose_types::NodeMetadata {
                product: "LogPose".to_owned(),
                node_name: "node-a".to_owned(),
                version: "test".to_owned(),
                git_sha: "sha".to_owned(),
                profile: "debug".to_owned(),
            },
            role: logpose_types::NodeRole::Combined,
            rest_endpoint: "http://127.0.0.1:8080".to_owned(),
            grpc_endpoint: "http://127.0.0.1:50051".to_owned(),
            storage_engine: "local+etcd-metadata".to_owned(),
            control_plane_ready: true,
            data_plane_ready: true,
            collection_count: 0,
            collections: Vec::new(),
            coordination: Some(logpose_types::CoordinationStatus {
                cluster_name: "prod-cluster".to_owned(),
                membership_registered: true,
                membership_state: Some("ready".to_owned()),
                membership_lease_id: Some(17),
                registered_members: vec!["node-a".to_owned(), "node-b".to_owned()],
                leader_node: Some("node-a".to_owned()),
                is_local_leader: true,
                leadership_lease_id: Some(23),
                metadata_revision: Some(42),
                watch_lag: Some(0),
                last_error: None,
            }),
            maintenance: logpose_types::MaintenanceBacklog::default(),
        })
        .human_text()
        .expect("status should render");

        assert!(output.contains("Cluster: prod-cluster"));
        assert!(output.contains("Membership Registered: yes"));
        assert!(output.contains("Membership Lease: 17"));
        assert!(output.contains("Leader: node-a (local leader)"));
        assert!(output.contains("Leadership Lease: 23"));
        assert!(output.contains("Metadata Revision: 42"));
        assert!(output.contains("Watch Lag: 0"));
        assert!(output.contains("Members: node-a, node-b"));
    }

    #[test]
    fn placement_render_includes_replicas_and_failover_reason() {
        let output = ActionOutput::CollectionPlacement(CollectionPlacement {
            collection_id: logpose_types::CollectionId::default(),
            database_name: "analytics".to_owned(),
            collection_name: "documents".to_owned(),
            assigned_node: "owner-a".to_owned(),
            assigned_role: logpose_types::NodeRole::Data,
            owner_node: Some("owner-b".to_owned()),
            ownership_epoch: Some(2),
            replicas: vec![logpose_types::ReplicaPlacement {
                node_id: "replica-a".to_owned(),
                node_role: logpose_types::NodeRole::Combined,
                state: "absent".to_owned(),
            }],
            failover_reason: Some("automatic self-promotion".to_owned()),
            metadata_revision: Some(42),
            route_kind: "recorded".to_owned(),
            route_reason: "ownership epoch 2 is assigned to node 'owner-b'".to_owned(),
        })
        .human_text()
        .expect("placement should render");

        assert!(output.contains("Owner node: owner-b"));
        assert!(output.contains("Ownership epoch: 2"));
        assert!(output.contains("Metadata revision: 42"));
        assert!(output.contains("Failover reason: automatic self-promotion"));
        assert!(output.contains("Replicas:"));
        assert!(output.contains("replica-a (combined, absent)"));
    }
}
