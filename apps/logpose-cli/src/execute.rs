use crate::{
    action::{
        Action, CLI_PUT_BATCH_BYTES, count_request_from_action, primary_key_from_text,
        query_request_from_action, read_collection_spec, read_database_policy_input,
        read_jsonl_put_batches, records_from_documents, scroll_request_from_action,
        stats_read_barrier_from_action, stats_snapshot_from_action,
    },
    feedback::{ProgressHandle, Reporter},
    render::ActionOutput,
};
use anyhow::{Context, bail};
use logpose_client::{ClientConfig, CreateCollectionRequest, LogPoseClient};
use logpose_config::LogPoseConfig;

/// What `record put` tells the operator about the batch whose write failed. Each batch is one
/// `UpsertRecords` call and commits atomically, so it never lands in part.
const FAILED_BATCH_ADVICE: &str = "each batch commits atomically, so the failing batch was \
     applied in full or not at all; verify collection state before retrying it";

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
                .put_database(&action.database_name)
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
        Action::DatabaseDrop { database_name } => {
            let progress = ProgressHandle::start(reporter.clone(), "Dropping database...");
            let client = connect_client(config, auth_token).await?;
            client
                .drop_database(database_name)
                .await
                .context("failed to drop database")?;
            progress.finish_success("Database dropped");
            Ok(ActionOutput::DatabaseDropped(database_name.clone()))
        }
        Action::CollectionCreateFromSchema(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Reading schema...");
            let spec = read_collection_spec(&action.schema, &action.collection.collection_name)?;
            progress.set_message("Creating collection...");
            let client = connect_client(config, auth_token).await?;
            let descriptor = client
                .create_collection(CreateCollectionRequest::from_spec(
                    action.collection.database_name.clone(),
                    spec,
                ))
                .await
                .context("failed to create collection")?;
            progress.finish_success("Collection created");
            Ok(ActionOutput::CollectionCreated(descriptor))
        }
        Action::CollectionList { database_name } => {
            let progress = ProgressHandle::start(reporter.clone(), "Listing collections...");
            let client = connect_client(config, auth_token).await?;
            let collections = client
                .collections(database_name)
                .await
                .context("failed to list collections")?;
            progress.finish_success("Collection list ready");
            Ok(ActionOutput::CollectionsListed(collections))
        }
        Action::CollectionAlter(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Changing schema...");
            let client = connect_client(config, auth_token).await?;
            let descriptor = client
                .alter_collection(&action.collection, action.change.clone())
                .await
                .context("failed to change the collection schema")?;
            progress.finish_success("Schema changed");
            Ok(ActionOutput::CollectionAltered(descriptor))
        }
        Action::CollectionDrop(collection) => {
            let progress = ProgressHandle::start(reporter.clone(), "Dropping collection...");
            let client = connect_client(config, auth_token).await?;
            client
                .drop_collection(collection)
                .await
                .context("failed to drop collection")?;
            progress.finish_success("Collection dropped");
            Ok(ActionOutput::CollectionDropped(collection.clone()))
        }
        Action::CollectionShow(collection) => {
            let progress =
                ProgressHandle::start(reporter.clone(), "Fetching collection metadata...");
            let client = connect_client(config, auth_token).await?;
            let descriptor = client
                .collection(collection)
                .await
                .context("failed to fetch collection")?;
            progress.finish_success("Collection metadata ready");
            Ok(ActionOutput::CollectionShown(descriptor))
        }
        Action::CollectionStats(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Fetching collection stats...");
            let client = connect_client(config, auth_token).await?;
            let stats = client
                .stats(
                    &action.collection,
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
                .collection_placement(collection)
                .await
                .context("failed to fetch collection placement")?;
            progress.finish_success("Collection placement ready");
            Ok(ActionOutput::CollectionPlacement(placement))
        }
        Action::CollectionFlush(collection) => {
            let progress = ProgressHandle::start(reporter.clone(), "Flushing collection...");
            let client = connect_client(config, auth_token).await?;
            let snapshot = client
                .flush(collection)
                .await
                .context("failed to flush collection")?;
            progress.finish_success("Flush completed");
            Ok(ActionOutput::CollectionFlushed(snapshot))
        }
        Action::CollectionCompact(collection) => {
            let progress = ProgressHandle::start(reporter.clone(), "Compacting collection...");
            let client = connect_client(config, auth_token).await?;
            let snapshot = client
                .compact(collection)
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
            let schema = client
                .collection(&action.collection)
                .await
                .context("failed to fetch the collection schema")?
                .schema;
            let mut last_seq_no = 0;
            let mut applied_ops = 0;
            let mut acknowledged_snapshot = None;
            for documents in batches {
                let records = records_from_documents(&schema, documents)?;
                let ack = match client.upsert(&action.collection, records).await {
                    Ok(ack) => ack,
                    Err(error) if applied_ops > 0 => {
                        return Err(error).context(format!(
                            "failed to write records after {applied_ops} acknowledged operations; {FAILED_BATCH_ADVICE}"
                        ));
                    }
                    Err(error) => {
                        return Err(error)
                            .context(format!("failed to write records; {FAILED_BATCH_ADVICE}"));
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
            let progress = ProgressHandle::start(reporter.clone(), "Deleting records...");
            // Local filter input errors surface before a connection is made.
            action.filter.document()?;
            match (&action.id, action.filter.is_empty()) {
                (Some(_), false) => bail!("delete either a record id or a filter, not both"),
                (None, true) => bail!("delete needs a record id or a filter"),
                _ => {}
            }
            let client = connect_client(config, auth_token).await?;
            let schema = client
                .collection(&action.collection)
                .await
                .context("failed to fetch the collection schema")?
                .schema;
            let ack = match (&action.id, action.filter.resolve(&schema)?) {
                (Some(id), None) => {
                    let key = primary_key_from_text(&schema, id)?;
                    client.delete(&action.collection, vec![key]).await.context(
                        "failed to delete record; the delete may have been durably recorded before the error was returned, so verify collection state before retrying",
                    )?
                }
                (None, Some(filter)) => client
                    .delete_by_filter(&action.collection, filter)
                    .await
                    .context(
                        "failed to delete by filter; the delete may have been durably recorded before the error was returned, so verify collection state before retrying",
                    )?,
                (Some(_), Some(_)) => bail!("delete either a record id or a filter, not both"),
                (None, None) => bail!("delete needs a record id or a filter"),
            };
            progress.finish_success("Delete completed");
            Ok(ActionOutput::RecordDeleted(ack))
        }
        Action::Count(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Counting records...");
            // Local filter input errors surface before a connection is made.
            action.filter.document()?;
            let client = connect_client(config, auth_token).await?;
            let schema = client
                .collection(&action.collection)
                .await
                .context("failed to fetch the collection schema")?
                .schema;
            let request = count_request_from_action(action, &schema)?;
            let response = client
                .count(&action.collection, request)
                .await
                .context("failed to count records")?;
            progress.finish_success("Count ready");
            Ok(ActionOutput::Count(response))
        }
        Action::Scroll(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Reading a page...");
            // Local filter input errors surface before a connection is made.
            action.filter.document()?;
            let client = connect_client(config, auth_token).await?;
            let schema = client
                .collection(&action.collection)
                .await
                .context("failed to fetch the collection schema")?
                .schema;
            let request = scroll_request_from_action(action, &schema)?;
            let response = client
                .scroll(&action.collection, request)
                .await
                .context("failed to scroll records")?;
            progress.finish_success("Page ready");
            Ok(ActionOutput::Scroll { schema, response })
        }
        Action::RecordGet(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Reading records...");
            let client = connect_client(config, auth_token).await?;
            let schema = client
                .collection(&action.collection)
                .await
                .context("failed to fetch the collection schema")?
                .schema;
            let keys = action
                .keys
                .iter()
                .map(|key| primary_key_from_text(&schema, key))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let response = client
                .get(&action.collection, keys, action.output_fields.clone())
                .await
                .context("failed to read records")?;
            progress.finish_success("Records ready");
            Ok(ActionOutput::RecordsFetched { schema, response })
        }
        Action::Query(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Running query...");
            // Local filter input errors surface before a connection is made.
            action.filter.document()?;
            let client = connect_client(config, auth_token).await?;
            let schema = client
                .collection(&action.collection)
                .await
                .context("failed to fetch the collection schema")?
                .schema;
            let request = query_request_from_action(action, &schema)?;
            let response = client
                .query(&action.collection, request)
                .await
                .context("failed to query collection")?;
            progress.finish_success("Query completed");
            Ok(ActionOutput::Query { schema, response })
        }
        Action::Inspect { collection, target } => {
            let progress =
                ProgressHandle::start(reporter.clone(), "Inspecting collection storage...");
            let client = connect_client(config, auth_token).await?;
            let report = client
                .inspect(collection, target.clone())
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
