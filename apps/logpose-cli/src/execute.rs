use crate::{
    action::{
        Action, CLI_PUT_BATCH_BYTES, database_descriptor, query_request_from_action,
        read_database_policy_input, read_jsonl_put_batches, stats_read_barrier_from_action,
        stats_snapshot_from_action,
    },
    feedback::{ProgressHandle, Reporter},
    render::ActionOutput,
};
use anyhow::Context;
use logpose_client::{ClientConfig, LogPoseClient};
use logpose_config::LogPoseConfig;
use logpose_types::{DeleteRecord, RecordId, WriteOperation};

pub async fn execute_action<R: Reporter>(
    config: &LogPoseConfig,
    auth_token: Option<&str>,
    action: &Action,
    reporter: &R,
) -> anyhow::Result<ActionOutput> {
    match action {
        Action::Status => {
            let progress = ProgressHandle::start(reporter.clone(), "Fetching runtime status...");
            let client = connect_client(config, auth_token).await?;
            let status = client
                .runtime_status()
                .await
                .context("failed to fetch runtime status")?;
            progress.finish_success("Runtime status ready");
            Ok(ActionOutput::Status(status))
        }
        Action::ConfigShow => {
            reporter.emit(crate::feedback::ProgressEvent::Info(
                "Configuration ready".to_owned(),
            ));
            Ok(ActionOutput::Config(config.clone()))
        }
        Action::NodeMembership(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Fetching node membership...");
            let client = connect_client(config, auth_token).await?;
            let membership = client
                .node_membership(&action.node_name)
                .await
                .context("failed to fetch node membership")?;
            progress.finish_success("Node membership ready");
            Ok(ActionOutput::NodeMembership(membership))
        }
        Action::NodeDrain(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Draining node...");
            let client = connect_client(config, auth_token).await?;
            let membership = client
                .drain_node(&action.node_name)
                .await
                .context("failed to drain node")?;
            progress.finish_success("Node drain applied");
            Ok(ActionOutput::NodeDrained(membership))
        }
        Action::NodeUndrain(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Restoring node readiness...");
            let client = connect_client(config, auth_token).await?;
            let membership = client
                .undrain_node(&action.node_name)
                .await
                .context("failed to undrain node")?;
            progress.finish_success("Node restored");
            Ok(ActionOutput::NodeUndrained(membership))
        }
        Action::DatabaseList => {
            let progress = ProgressHandle::start(reporter.clone(), "Listing databases...");
            let client = connect_client(config, auth_token).await?;
            let databases = client
                .databases()
                .await
                .context("failed to list databases")?;
            progress.finish_success("Database list ready");
            Ok(ActionOutput::DatabasesListed(databases))
        }
        Action::DatabaseShow { database_name } => {
            let progress = ProgressHandle::start(reporter.clone(), "Fetching database...");
            let client = connect_client(config, auth_token).await?;
            let database = client
                .database(database_name)
                .await
                .context("failed to fetch database")?;
            progress.finish_success("Database ready");
            Ok(ActionOutput::DatabaseShown(database))
        }
        Action::DatabasePut(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Updating database...");
            let client = connect_client(config, auth_token).await?;
            let database = client
                .set_database(database_descriptor(&action.database_name))
                .await
                .context("failed to update database")?;
            progress.finish_success("Database updated");
            Ok(ActionOutput::DatabaseUpdated(database))
        }
        Action::DatabasePolicyShow { database_name } => {
            let progress = ProgressHandle::start(reporter.clone(), "Fetching database policy...");
            let client = connect_client(config, auth_token).await?;
            let policy = client
                .database_policy(database_name)
                .await
                .context("failed to fetch database policy")?;
            progress.finish_success("Database policy ready");
            Ok(ActionOutput::DatabasePolicyShown(policy))
        }
        Action::DatabasePolicySet(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Reading database policy...");
            let policy = read_database_policy_input(&action.input, &action.database_name)?;
            progress.set_message("Updating database policy...");
            let client = connect_client(config, auth_token).await?;
            let policy = client
                .set_database_policy(policy)
                .await
                .context("failed to update database policy")?;
            progress.finish_success("Database policy updated");
            Ok(ActionOutput::DatabasePolicyUpdated(policy))
        }
        Action::CollectionCreate(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Creating collection...");
            let client = connect_client(config, auth_token).await?;
            let descriptor = client
                .create_collection(action.request())
                .await
                .context("failed to create collection")?;
            progress.finish_success("Collection created");
            Ok(ActionOutput::CollectionCreated(descriptor))
        }
        Action::CollectionShow(collection) => {
            let progress =
                ProgressHandle::start(reporter.clone(), "Fetching collection metadata...");
            let client = connect_client(config, auth_token).await?;
            let descriptor = client
                .get_collection_in_database(&collection.database_name, &collection.collection_name)
                .await
                .context("failed to fetch collection")?;
            progress.finish_success("Collection metadata ready");
            Ok(ActionOutput::CollectionShown(descriptor))
        }
        Action::CollectionStats(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Fetching collection stats...");
            let client = connect_client(config, auth_token).await?;
            let stats = client
                .stats_in_database_for_read(
                    &action.collection.database_name,
                    &action.collection.collection_name,
                    stats_snapshot_from_action(action)?,
                    stats_read_barrier_from_action(action)?,
                )
                .await
                .context("failed to read collection stats")?;
            progress.finish_success("Collection stats ready");
            Ok(ActionOutput::CollectionStats(stats))
        }
        Action::CollectionPlacement(collection) => {
            let progress =
                ProgressHandle::start(reporter.clone(), "Fetching collection placement...");
            let client = connect_client(config, auth_token).await?;
            let placement = client
                .collection_placement_in_database(
                    &collection.database_name,
                    &collection.collection_name,
                )
                .await
                .context("failed to fetch collection placement")?;
            progress.finish_success("Collection placement ready");
            Ok(ActionOutput::CollectionPlacement(placement))
        }
        Action::CollectionPromote(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Promoting collection owner...");
            let client = connect_client(config, auth_token).await?;
            let placement = client
                .promote_collection_owner_in_database(
                    &action.collection.database_name,
                    &action.collection.collection_name,
                    &action.node_name,
                )
                .await
                .context("failed to promote collection owner")?;
            progress.finish_success("Collection owner promoted");
            Ok(ActionOutput::CollectionPromoted(placement))
        }
        Action::CollectionRebalance(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Rebalancing collection...");
            let client = connect_client(config, auth_token).await?;
            let placement = client
                .rebalance_collection_in_database(
                    &action.collection.database_name,
                    &action.collection.collection_name,
                    action.target_node_name.as_deref(),
                )
                .await
                .context("failed to rebalance collection")?;
            progress.finish_success("Collection rebalanced");
            Ok(ActionOutput::CollectionRebalanced(placement))
        }
        Action::CollectionFlush(collection) => {
            let progress = ProgressHandle::start(reporter.clone(), "Flushing collection...");
            let client = connect_client(config, auth_token).await?;
            let snapshot = client
                .flush_in_database(&collection.database_name, &collection.collection_name)
                .await
                .context("failed to flush collection")?;
            progress.finish_success("Flush completed");
            Ok(ActionOutput::CollectionFlushed(snapshot))
        }
        Action::CollectionCompact(collection) => {
            let progress = ProgressHandle::start(reporter.clone(), "Compacting collection...");
            let client = connect_client(config, auth_token).await?;
            let snapshot = client
                .compact_in_database(&collection.database_name, &collection.collection_name)
                .await
                .context("failed to compact collection")?;
            progress.finish_success("Compaction completed");
            Ok(ActionOutput::CollectionCompacted(snapshot))
        }
        Action::RecordPut(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Reading JSONL input...");
            let batches = read_jsonl_put_batches(&action.input, CLI_PUT_BATCH_BYTES)?;
            progress.set_message("Writing records...");
            let client = connect_client(config, auth_token).await?;
            let mut last_seq_no = 0;
            let mut applied_ops = 0;
            let mut acknowledged_snapshot = None;
            for operations in batches {
                let ack = match client
                    .write_in_database(
                        &action.collection.database_name,
                        &action.collection.collection_name,
                        operations,
                    )
                    .await
                {
                    Ok(ack) => ack,
                    Err(error) if applied_ops > 0 => {
                        return Err(error).context(format!(
                            "failed to write records after {applied_ops} fully acknowledged operations; the failing batch may have partially committed, so verify collection state before retrying"
                        ));
                    }
                    Err(error) => {
                        return Err(error).context(
                            "failed to write records; the failing batch may have partially committed, so verify collection state before retrying",
                        )
                    }
                };
                last_seq_no = ack.last_seq_no;
                applied_ops += ack.applied_ops;
                acknowledged_snapshot = Some(ack.snapshot.clone());
            }
            progress.finish_success("Write completed");
            Ok(ActionOutput::RecordsWritten(
                logpose_client::ScopedCollectionResponse {
                    database_name: action.collection.database_name.clone(),
                    collection_name: action.collection.collection_name.clone(),
                    response: logpose_types::CommitAck {
                        last_seq_no,
                        applied_ops,
                        snapshot: acknowledged_snapshot
                            .expect("at least one acknowledged batch should produce a snapshot"),
                    },
                },
            ))
        }
        Action::RecordDelete(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Deleting record...");
            let client = connect_client(config, auth_token).await?;
            let ack = client
                .write_in_database(
                    &action.collection.database_name,
                    &action.collection.collection_name,
                    vec![WriteOperation::Delete(DeleteRecord {
                        id: RecordId::new(action.id.clone()),
                    })],
                )
                .await
                .context(
                    "failed to delete record; the delete may have been durably recorded before the error was returned, so verify collection state before retrying",
                )?;
            progress.finish_success("Delete completed");
            Ok(ActionOutput::RecordDeleted(ack))
        }
        Action::Query(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Running query...");
            let request = query_request_from_action(action)?;
            let client = connect_client(config, auth_token).await?;
            let response = client
                .query_in_database(&action.collection.database_name, request)
                .await
                .context("failed to query collection")?;
            progress.finish_success("Query completed");
            Ok(ActionOutput::Query(response))
        }
        Action::Inspect { collection, target } => {
            let progress =
                ProgressHandle::start(reporter.clone(), "Inspecting collection storage...");
            let client = connect_client(config, auth_token).await?;
            let report = client
                .inspect_in_database(
                    &collection.database_name,
                    &collection.collection_name,
                    target.clone(),
                )
                .await
                .context("failed to inspect collection")?;
            progress.finish_success("Inspection ready");
            Ok(ActionOutput::Inspect(report))
        }
    }
}

pub async fn connect_client(
    config: &LogPoseConfig,
    auth_token: Option<&str>,
) -> anyhow::Result<LogPoseClient> {
    let endpoint = grpc_dial_endpoint(config);
    LogPoseClient::from_config(&ClientConfig {
        grpc_endpoint: endpoint.clone(),
        auth_token: auth_token.map(str::to_owned),
    })
    .await
    .with_context(|| format!("failed to connect to {endpoint}"))
}

#[cfg(test)]
pub fn rest_endpoint(config: &LogPoseConfig) -> String {
    endpoint_url(&config.rest_host, config.rest_port)
}

#[cfg(test)]
pub fn rest_dial_endpoint(config: &LogPoseConfig) -> String {
    dial_endpoint_url(&config.rest_host, config.rest_port)
}

#[cfg(test)]
pub fn grpc_endpoint(config: &LogPoseConfig) -> String {
    endpoint_url(&config.grpc_host, config.grpc_port)
}

pub fn grpc_dial_endpoint(config: &LogPoseConfig) -> String {
    dial_endpoint_url(&config.grpc_host, config.grpc_port)
}

pub fn endpoint_url(host: &str, port: u16) -> String {
    let authority = match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{host}]"),
        Ok(std::net::IpAddr::V4(_)) | Err(_) => host.to_owned(),
    };

    format!("http://{authority}:{port}")
}

pub fn dial_endpoint_url(host: &str, port: u16) -> String {
    let dial_host = match host {
        "0.0.0.0" => "127.0.0.1",
        "::" => "::1",
        _ => host,
    };
    endpoint_url(dial_host, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_helpers_bracket_ipv6_hosts() {
        let config = LogPoseConfig {
            rest_host: "::1".to_owned(),
            rest_port: 18080,
            grpc_host: "::1".to_owned(),
            grpc_port: 15051,
            auth: Default::default(),
            ..LogPoseConfig::default()
        };

        assert_eq!(rest_endpoint(&config), "http://[::1]:18080");
        assert_eq!(grpc_endpoint(&config), "http://[::1]:15051");
    }

    #[test]
    fn dial_endpoint_helpers_rewrite_wildcard_bind_addresses() {
        let config = LogPoseConfig {
            rest_host: "0.0.0.0".to_owned(),
            rest_port: 18080,
            grpc_host: "::".to_owned(),
            grpc_port: 15051,
            auth: Default::default(),
            ..LogPoseConfig::default()
        };

        assert_eq!(rest_dial_endpoint(&config), "http://127.0.0.1:18080");
        assert_eq!(grpc_dial_endpoint(&config), "http://[::1]:15051");
    }
}
