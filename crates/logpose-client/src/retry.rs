//! Opt-in retries and owner/leader redirects.
//!
//! A [`LogPoseClient`](crate::LogPoseClient) sends each request once by default. Two policies
//! change that:
//!
//! - [`RetryPolicy`] retries errors the server marks retryable
//!   ([`ServerError::is_retryable`]) on the same node, waiting at least the server's retry
//!   hint. Reads are retried; writes only when [`RetryPolicy::retry_writes`] is set.
//! - [`RedirectPolicy`] follows the `owner_node` of a `NOT_OWNER` error and the
//!   `leader_node` of a `NOT_LEADER` error to that node, when a [`NodeResolver`] knows its
//!   endpoint.
//!
//! # Idempotency
//!
//! A lost reply (a transport failure) carries no hint, so it is never retried. Most hinted
//! errors refuse the request before it changes anything: `NOT_OWNER`, `NOT_LEADER`, and a
//! collection write without ownership metadata. A metadata store (etcd) failure is the
//! exception: it carries a hint too, and during `set_database`, `set_database_policy` or
//! `create_collection` the change may have committed before the reply was lost. Write retries
//! are opt-in for that reason, so a caller that cannot tolerate a repeat never gets one
//! silently. The client's writes are safe to repeat:
//!
//! - `write`: puts and deletes address records by id, so a repeated batch leaves the same
//!   records (with a newer sequence number).
//! - `set_database` and `set_database_policy` replace the whole descriptor or policy.
//! - `flush` and `compact` are maintenance: a repeat has nothing left to do or does it again.
//! - `create_collection` is not idempotent: a repeat of an applied create reports
//!   `RESOURCE_ALREADY_EXISTS`.
//!
//! Bulk streams (`BulkWriteCollection`) are never retried automatically: resume from the
//! failed batch the error reports.
//!
//! Redirects follow routing errors for reads and writes alike: `NOT_OWNER` and `NOT_LEADER`
//! refuse the request before applying anything.

use crate::error::ServerError;
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
    time::Duration,
};

/// Whether a call reads or changes state; decides whether a [`RetryPolicy`] applies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Operation {
    /// Reads nothing but state; always safe to repeat.
    Read,
    /// Changes state; repeated only when the caller opts in.
    Write,
}

/// When and how often to retry a request on the same node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Attempts per request, including the first. `1` disables retries.
    pub max_attempts: u32,
    /// Backoff before the first retry.
    pub initial_backoff: Duration,
    /// Cap on the client's own backoff, and the longest server hint the client will wait:
    /// an error whose hint is longer is returned instead of retried.
    pub max_backoff: Duration,
    /// Factor the backoff grows by after each retry.
    pub backoff_multiplier: u32,
    /// Also retry writes. See the [module documentation](self) for what is safe to repeat.
    pub retry_writes: bool,
}

impl RetryPolicy {
    /// Send every request exactly once.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            backoff_multiplier: 1,
            retry_writes: false,
        }
    }

    /// Retry reads up to `max_attempts` attempts in total with the default backoff.
    #[must_use]
    pub fn reads(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            ..Self::default()
        }
    }

    /// The same policy, also retrying writes.
    #[must_use]
    pub fn with_writes(mut self) -> Self {
        self.retry_writes = true;
        self
    }

    /// Whether this policy may retry `operation` at all.
    pub(crate) fn applies_to(&self, operation: Operation) -> bool {
        self.max_attempts > 1 && (operation == Operation::Read || self.retry_writes)
    }

    /// The client's own backoff before retry number `retry` (1 for the first retry).
    #[must_use]
    pub fn backoff(&self, retry: u32) -> Duration {
        let factor = self
            .backoff_multiplier
            .max(1)
            .saturating_pow(retry.saturating_sub(1));
        self.initial_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }

    /// How long to wait before retrying `operation` after attempt number `attempt` (1 for the
    /// first) failed with `error`, or `None` to return the error.
    ///
    /// The wait is the larger of the server's retry hint and the client's backoff.
    pub(crate) fn delay_before_retry(
        &self,
        operation: Operation,
        attempt: u32,
        error: &ServerError,
    ) -> Option<Duration> {
        if !self.applies_to(operation) || attempt >= self.max_attempts || !error.is_retryable() {
            return None;
        }
        let hint = error.retry_after().unwrap_or_default();
        if hint > self.max_backoff {
            return None;
        }
        Some(hint.max(self.backoff(attempt)))
    }
}

/// Three attempts for reads, backing off from 100 ms up to 5 s; writes are not retried.
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 2,
            retry_writes: false,
        }
    }
}

/// Maps a node id, as named by `owner_node` or `leader_node`, to its gRPC endpoint.
///
/// The node id comes from the server; the endpoint comes only from the resolver. The client
/// sends its bearer token to every endpoint the resolver returns and keeps one connection per
/// distinct endpoint for its lifetime, so return only endpoints of known nodes. A resolver that
/// builds an endpoint from any id (such as `format!("http://{node}:50051")`) lets the server
/// choose where the token goes and how many connections the client keeps.
pub trait NodeResolver: Send + Sync + 'static {
    /// The gRPC endpoint URL of `node`, or `None` when it is unknown.
    fn endpoint(&self, node: &str) -> Option<String>;
}

impl NodeResolver for BTreeMap<String, String> {
    fn endpoint(&self, node: &str) -> Option<String> {
        self.get(node).cloned()
    }
}

impl NodeResolver for HashMap<String, String> {
    fn endpoint(&self, node: &str) -> Option<String> {
        self.get(node).cloned()
    }
}

impl<F> NodeResolver for F
where
    F: Fn(&str) -> Option<String> + Send + Sync + 'static,
{
    fn endpoint(&self, node: &str) -> Option<String> {
        self(node)
    }
}

/// Follow `NOT_OWNER` and `NOT_LEADER` errors to the node they name.
#[derive(Clone)]
pub struct RedirectPolicy {
    resolver: Arc<dyn NodeResolver>,
    max_redirects: u32,
}

impl RedirectPolicy {
    /// Redirects per request unless changed with [`max_redirects`](Self::max_redirects).
    pub const DEFAULT_MAX_REDIRECTS: u32 = 2;

    /// Resolve node ids with `resolver`.
    pub fn new(resolver: impl NodeResolver) -> Self {
        Self {
            resolver: Arc::new(resolver),
            max_redirects: Self::DEFAULT_MAX_REDIRECTS,
        }
    }

    /// Follow at most `max_redirects` redirects per request; the next routing error is
    /// returned.
    #[must_use]
    pub fn max_redirects(mut self, max_redirects: u32) -> Self {
        self.max_redirects = max_redirects;
        self
    }

    /// The endpoint to redirect to after `redirects` redirects failed with `error`, or `None`
    /// to handle `error` otherwise.
    pub(crate) fn target(&self, redirects: u32, error: &ServerError) -> Option<String> {
        if redirects >= self.max_redirects {
            return None;
        }
        self.resolver.endpoint(error.redirect_node()?)
    }
}

impl fmt::Debug for RedirectPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedirectPolicy")
            .field("max_redirects", &self.max_redirects)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_api_grpc::status_from_error;
    use logpose_types::{LogPoseError, WriteOutcome};

    fn unavailable(retry_after: Option<Duration>) -> ServerError {
        ServerError::from_status(status_from_error(&LogPoseError::Unavailable {
            message: "busy".to_owned(),
            retry_after,
        }))
    }

    fn not_owner(owner: Option<&str>) -> ServerError {
        ServerError::from_status(status_from_error(&LogPoseError::NotOwner {
            collection: "default/docs".to_owned(),
            node: "node-a".to_owned(),
            owner_node: owner.map(str::to_owned),
        }))
    }

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            backoff_multiplier: 2,
            retry_writes: false,
        }
    }

    #[test]
    fn backoff_grows_by_the_multiplier_up_to_the_cap() {
        let policy = policy();
        assert_eq!(policy.backoff(1), Duration::from_millis(10));
        assert_eq!(policy.backoff(2), Duration::from_millis(20));
        assert_eq!(policy.backoff(3), Duration::from_millis(40));
        assert_eq!(policy.backoff(4), Duration::from_millis(50));
        assert_eq!(policy.backoff(u32::MAX), Duration::from_millis(50));
    }

    #[test]
    fn retries_wait_the_larger_of_the_hint_and_the_backoff() {
        let policy = policy();
        let hinted = unavailable(Some(Duration::from_millis(30)));
        assert_eq!(
            policy.delay_before_retry(Operation::Read, 1, &hinted),
            Some(Duration::from_millis(30))
        );
        assert_eq!(
            policy.delay_before_retry(Operation::Read, 3, &hinted),
            Some(Duration::from_millis(40))
        );
    }

    #[test]
    fn retries_stop_after_the_last_attempt() {
        let policy = policy();
        let hinted = unavailable(Some(Duration::from_millis(1)));
        assert!(
            policy
                .delay_before_retry(Operation::Read, 3, &hinted)
                .is_some()
        );
        assert_eq!(policy.delay_before_retry(Operation::Read, 4, &hinted), None);
        assert_eq!(
            RetryPolicy::disabled().delay_before_retry(Operation::Read, 1, &hinted),
            None
        );
    }

    #[test]
    fn errors_without_a_hint_or_with_a_hint_above_the_cap_are_returned() {
        let policy = policy();
        assert_eq!(
            policy.delay_before_retry(Operation::Read, 1, &unavailable(None)),
            None
        );
        // NOT_OWNER asks for one second, longer than this policy waits.
        assert_eq!(
            policy.delay_before_retry(Operation::Read, 1, &not_owner(None)),
            None
        );
    }

    #[test]
    fn writes_are_retried_only_when_the_caller_opts_in() {
        let hinted = unavailable(Some(Duration::from_millis(1)));
        assert_eq!(
            policy().delay_before_retry(Operation::Write, 1, &hinted),
            None
        );
        assert!(
            policy()
                .with_writes()
                .delay_before_retry(Operation::Write, 1, &hinted)
                .is_some()
        );
    }

    #[test]
    fn non_retryable_codes_are_never_retried() {
        let policy = policy().with_writes();
        for error in [
            LogPoseError::invalid_argument("bad"),
            LogPoseError::failed_precondition("dropping"),
            LogPoseError::CollectionPoisoned {
                collection: "default/docs".to_owned(),
                reason: "fsync".to_owned(),
            },
            LogPoseError::internal("boom"),
            // A WAL failure is never retried: `not_applied` carries no hint, and `unknown_*`
            // (INTERNAL) may still be replayed, so a repeat could apply the batch twice.
            LogPoseError::WalWriteFailed {
                collection: "default/docs".to_owned(),
                outcome: WriteOutcome::NotApplied,
                reason: "fsync".to_owned(),
            },
            LogPoseError::WalWriteFailed {
                collection: "default/docs".to_owned(),
                outcome: WriteOutcome::Unknown { fenced: true },
                reason: "fsync".to_owned(),
            },
            LogPoseError::WalWriteFailed {
                collection: "default/docs".to_owned(),
                outcome: WriteOutcome::Unknown { fenced: false },
                reason: "fsync".to_owned(),
            },
        ] {
            let decoded = ServerError::from_status(status_from_error(&error));
            for operation in [Operation::Read, Operation::Write] {
                assert_eq!(
                    policy.delay_before_retry(operation, 1, &decoded),
                    None,
                    "{error}"
                );
            }
        }
    }

    #[test]
    fn redirects_resolve_the_named_node_until_the_limit() {
        let resolver = BTreeMap::from([("node-b".to_owned(), "http://node-b:50051".to_owned())]);
        let redirects = RedirectPolicy::new(resolver).max_redirects(1);
        let error = not_owner(Some("node-b"));
        assert_eq!(
            redirects.target(0, &error).as_deref(),
            Some("http://node-b:50051")
        );
        assert_eq!(redirects.target(1, &error), None);
        assert_eq!(redirects.target(0, &not_owner(Some("node-z"))), None);
        assert_eq!(redirects.target(0, &not_owner(None)), None);
        assert_eq!(
            redirects.target(0, &unavailable(Some(Duration::ZERO))),
            None
        );
    }

    #[test]
    fn closures_and_hash_maps_resolve_nodes() {
        let closure = |node: &str| (node == "node-b").then(|| "http://b".to_owned());
        assert_eq!(closure.endpoint("node-b").as_deref(), Some("http://b"));
        assert_eq!(closure.endpoint("node-c"), None);
        let map = HashMap::from([("node-c".to_owned(), "http://c".to_owned())]);
        assert_eq!(map.endpoint("node-c").as_deref(), Some("http://c"));
    }
}

#[cfg(test)]
mod client_tests {
    use super::*;
    use crate::{ClientError, LogPoseClient, test_support::ScriptedServer};
    use logpose_api_grpc::status_from_error;
    use logpose_types::{
        CollectionRef, ErrorReason, LogPoseError, ResourceKind,
        record::{PrimaryKey, Record},
    };
    use std::time::Instant;
    use tonic::Status;

    fn unavailable(retry_after_ms: u64) -> Status {
        status_from_error(&LogPoseError::Unavailable {
            message: "metadata store is catching up".to_owned(),
            retry_after: Some(Duration::from_millis(retry_after_ms)),
        })
    }

    fn not_owner(owner: &str) -> Status {
        status_from_error(&LogPoseError::NotOwner {
            collection: "default/docs".to_owned(),
            node: "node-a".to_owned(),
            owner_node: Some(owner.to_owned()),
        })
    }

    fn quick_retries(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_secs(2),
            backoff_multiplier: 2,
            retry_writes: false,
        }
    }

    fn put(id: &str) -> Vec<Record> {
        vec![Record {
            pk: PrimaryKey::String(id.to_owned()),
            vectors: [("vector".to_owned(), vec![1.0, 0.0])].into(),
            fields: Default::default(),
            extra: Default::default(),
        }]
    }

    fn docs() -> CollectionRef {
        CollectionRef::new("default", "docs")
    }

    async fn client(server: &ScriptedServer) -> LogPoseClient {
        LogPoseClient::connect(server.endpoint.clone())
            .await
            .expect("client should connect to the fake server")
    }

    #[tokio::test]
    async fn without_a_policy_the_first_error_is_returned_typed() {
        let server = ScriptedServer::start("node-a", vec![unavailable(1)]).await;
        let error = client(&server)
            .await
            .metadata()
            .await
            .expect_err("the scripted error should surface");
        assert_eq!(error.reason(), Some(ErrorReason::Unavailable));
        assert_eq!(
            error.server_error().and_then(ServerError::retry_after),
            Some(Duration::from_millis(1))
        );
        assert_eq!(server.calls(), ["get_metadata"]);
    }

    #[tokio::test]
    async fn reads_retry_retryable_errors_until_they_succeed() {
        let server = ScriptedServer::start("node-a", vec![unavailable(1), unavailable(1)]).await;
        let metadata = client(&server)
            .await
            .with_retry_policy(quick_retries(3))
            .metadata()
            .await
            .expect("the third attempt should succeed");
        assert_eq!(metadata.node_name, "node-a");
        assert_eq!(server.calls().len(), 3);
    }

    #[tokio::test]
    async fn retries_wait_at_least_the_server_hint() {
        let server = ScriptedServer::start("node-a", vec![unavailable(150)]).await;
        let started = Instant::now();
        client(&server)
            .await
            .with_retry_policy(quick_retries(2))
            .databases()
            .await
            .expect("the retry should succeed");
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert_eq!(server.calls(), ["list_databases", "list_databases"]);
    }

    #[tokio::test]
    async fn retries_stop_at_max_attempts_and_return_the_last_error() {
        let server = ScriptedServer::start(
            "node-a",
            vec![unavailable(1), unavailable(1), unavailable(1)],
        )
        .await;
        let error = client(&server)
            .await
            .with_retry_policy(quick_retries(2))
            .metadata()
            .await
            .expect_err("two attempts should both fail");
        assert_eq!(error.reason(), Some(ErrorReason::Unavailable));
        assert_eq!(server.calls().len(), 2);
    }

    #[tokio::test]
    async fn failed_preconditions_and_invalid_arguments_are_never_retried() {
        for status in [
            status_from_error(&LogPoseError::failed_precondition("collection is dropping")),
            status_from_error(&LogPoseError::invalid_field("vector", "must not be empty")),
            status_from_error(&LogPoseError::not_found(ResourceKind::Database, "nope")),
            Status::unavailable("connection reset without a hint"),
        ] {
            let server = ScriptedServer::start("node-a", vec![status.clone()]).await;
            let error = client(&server)
                .await
                .with_retry_policy(quick_retries(5).with_writes())
                .metadata()
                .await
                .expect_err("a non-retryable error should surface");
            assert_eq!(
                error.status().map(Status::code),
                Some(status.code()),
                "{error}"
            );
            assert_eq!(server.calls().len(), 1, "{error}");
        }
    }

    #[tokio::test]
    async fn writes_are_retried_only_when_the_caller_opts_in() {
        let server = ScriptedServer::start("node-a", vec![unavailable(1)]).await;
        let error = client(&server)
            .await
            .with_retry_policy(quick_retries(3))
            .upsert(&docs(), put("a"))
            .await
            .expect_err("reads-only retries leave writes alone");
        assert_eq!(error.reason(), Some(ErrorReason::Unavailable));
        assert_eq!(server.calls(), ["upsert_records"]);

        let server = ScriptedServer::start("node-a", vec![unavailable(1)]).await;
        let ack = client(&server)
            .await
            .with_retry_policy(quick_retries(3).with_writes())
            .upsert(&docs(), put("a"))
            .await
            .expect("an opted-in write retry should succeed");
        assert_eq!(ack.applied_ops, 1);
        assert_eq!(server.calls(), ["upsert_records", "upsert_records"]);
    }

    #[tokio::test]
    async fn not_owner_redirects_to_the_owner_the_resolver_knows() {
        let owner = ScriptedServer::start("node-b", Vec::new()).await;
        let refuser = ScriptedServer::start("node-a", vec![not_owner("node-b")]).await;
        let resolver = BTreeMap::from([("node-b".to_owned(), owner.endpoint.clone())]);
        let client = client(&refuser)
            .await
            .with_redirects(RedirectPolicy::new(resolver));

        let ack = client
            .upsert(&docs(), put("a"))
            .await
            .expect("the owner should accept the redirected write");
        assert_eq!(ack.applied_ops, 1);
        assert_eq!(refuser.calls(), ["upsert_records"]);
        assert_eq!(owner.calls(), ["upsert_records"]);

        // Each request starts at the configured endpoint again.
        let metadata = client.metadata().await.expect("metadata should load");
        assert_eq!(metadata.node_name, "node-a");
    }

    #[tokio::test]
    async fn not_leader_redirects_to_the_leader_the_resolver_knows() {
        let leader = ScriptedServer::start("node-c", Vec::new()).await;
        let follower = ScriptedServer::start(
            "node-a",
            vec![status_from_error(&LogPoseError::NotLeader {
                node: "node-a".to_owned(),
                leader_node: Some("node-c".to_owned()),
            })],
        )
        .await;
        let leader_endpoint = leader.endpoint.clone();
        let resolver = move |node: &str| (node == "node-c").then(|| leader_endpoint.clone());
        let snapshot = client(&follower)
            .await
            .with_redirects(RedirectPolicy::new(resolver))
            .flush(&docs())
            .await
            .expect("the leader should flush");
        assert_eq!(snapshot.manifest_generation, 2);
        assert_eq!(leader.calls(), ["flush_collection"]);
    }

    #[tokio::test]
    async fn redirects_stop_at_the_limit_and_return_the_routing_error() {
        let node_b = ScriptedServer::start("node-b", vec![not_owner("node-a")]).await;
        let node_a = ScriptedServer::start("node-a", vec![not_owner("node-b")]).await;
        let resolver = BTreeMap::from([
            ("node-a".to_owned(), node_a.endpoint.clone()),
            ("node-b".to_owned(), node_b.endpoint.clone()),
        ]);
        let error = client(&node_a)
            .await
            .with_redirects(RedirectPolicy::new(resolver).max_redirects(1))
            .metadata()
            .await
            .expect_err("the second routing error exceeds the limit");
        let server_error = error.server_error().expect("a typed server error");
        assert_eq!(server_error.reason(), Some(ErrorReason::NotOwner));
        assert_eq!(server_error.redirect_node(), Some("node-a"));
        assert_eq!(node_a.calls().len(), 1);
        assert_eq!(node_b.calls().len(), 1);
    }

    #[tokio::test]
    async fn routing_errors_surface_when_no_resolver_knows_the_node() {
        let server = ScriptedServer::start("node-a", vec![not_owner("node-z")]).await;
        let error = client(&server)
            .await
            .with_redirects(RedirectPolicy::new(BTreeMap::new()))
            .metadata()
            .await
            .expect_err("an unknown owner cannot be followed");
        assert_eq!(error.reason(), Some(ErrorReason::NotOwner));
        assert_eq!(
            error.server_error().and_then(ServerError::redirect_node),
            Some("node-z")
        );

        let server = ScriptedServer::start("node-a", vec![not_owner("node-b")]).await;
        let error = client(&server)
            .await
            .metadata()
            .await
            .expect_err("without a resolver the routing error surfaces");
        assert!(matches!(&error, ClientError::Server(_)), "{error:?}");
        assert_eq!(error.reason(), Some(ErrorReason::NotOwner));
        assert_eq!(server.calls().len(), 1);
    }
}
