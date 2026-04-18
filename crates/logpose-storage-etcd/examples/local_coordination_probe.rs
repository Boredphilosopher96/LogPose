//! Manual coordination smoke probe for a local etcd-backed LogPose cluster.

use async_trait as _;
use etcd_client::Client;
use logpose_catalog as _;
use logpose_storage as _;
use logpose_storage_etcd::{EtcdCoordinationClient, PromotionResult, ShardOwnership};
use logpose_types::{CollectionRef, EtcdMetadataConfig};
use serde as _;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoints = std::env::var("LOGPOSE_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned())
        .split(',')
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let cluster_name =
        std::env::var("LOGPOSE_ETCD_CLUSTER").unwrap_or_else(|_| "chaos-local".to_owned());
    let config = EtcdMetadataConfig {
        endpoints: endpoints.clone(),
        cluster_name: cluster_name.clone(),
        ..EtcdMetadataConfig::default()
    };
    let client = EtcdCoordinationClient::new(config.clone())?;
    let mut raw = Client::connect(endpoints, None).await?;
    let cluster_prefix = format!("{}/clusters/{}", config.key_prefix, config.cluster_name);

    raw.delete(
        cluster_prefix.clone(),
        Some(etcd_client::DeleteOptions::new().with_prefix()),
    )
    .await?;

    let node_a = client.register_membership("node-a").await?;
    let node_b = client.register_membership("node-b").await?;
    client.keep_alive(node_a.lease_id).await?;
    client.keep_alive(node_b.lease_id).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let members = client.list_membership().await?;
    println!("members: {members:#?}");

    let leader = client
        .try_acquire_leadership("node-a")
        .await?
        .expect("node-a should acquire leadership");
    let second_leader = client.try_acquire_leadership("node-b").await?;
    println!("leader: {leader:#?}");
    println!("node-b leadership attempt: {second_leader:#?}");

    let collection = CollectionRef::new_default("chaos");
    let shard_key = format!(
        "{cluster_prefix}/collections/{}/shards/0/owner",
        collection.lookup_name()
    );
    let seed = ShardOwnership {
        collection: collection.clone(),
        shard_id: "0".to_owned(),
        owner_node_id: "node-a".to_owned(),
        epoch: 1,
        mod_revision: 0,
    };
    raw.put(shard_key, serde_json::to_string(&seed)?, None)
        .await?;

    let current = client
        .shard_owner(&collection, "0")
        .await?
        .expect("seeded shard owner should exist");
    let contender_a = client.clone();
    let contender_b = client.clone();
    let current_for_a = current.clone();
    let current_for_b = current.clone();

    let (result_a, result_b) = tokio::join!(
        async move {
            contender_a
                .promote_shard_owner(&current_for_a, "node-b")
                .await
        },
        async move {
            contender_b
                .promote_shard_owner(&current_for_b, "node-c")
                .await
        }
    );

    println!("promotion race: {result_a:#?} / {result_b:#?}");

    let final_owner = client
        .shard_owner(&collection, "0")
        .await?
        .expect("final shard owner should exist");
    println!("final owner: {final_owner:#?}");

    match (result_a?, result_b?) {
        (PromotionResult::Applied(_), PromotionResult::Conflict)
        | (PromotionResult::Conflict, PromotionResult::Applied(_)) => {}
        other => {
            return Err(format!("unexpected promotion outcome: {other:#?}").into());
        }
    }

    assert_eq!(final_owner.epoch, 2);
    Ok(())
}
