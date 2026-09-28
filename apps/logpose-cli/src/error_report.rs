//! Operator-facing rendering of typed server errors.

use logpose_client::{ClientError, ServerError, grpc_code_name};
use std::error::Error;

/// The typed server error `cause` is or wraps directly, if any.
pub fn server_error<'a>(cause: &'a (dyn Error + 'static)) -> Option<&'a ServerError> {
    cause.downcast_ref::<ServerError>().or_else(|| {
        cause
            .downcast_ref::<ClientError>()
            .and_then(ClientError::server_error)
    })
}

/// Detail lines for a typed server error, shown under its message: the code, each field
/// violation, the metadata, and where and when to retry when the server says so.
pub fn server_error_details(error: &ServerError) -> Vec<String> {
    let mut lines = vec![format!("code: {}", grpc_code_name(error.code()))];
    lines.extend(
        error
            .field_violations()
            .iter()
            .map(|violation| format!("field {}: {}", violation.field, violation.description)),
    );
    if !error.metadata().is_empty() {
        let metadata = error
            .metadata()
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("metadata: {metadata}"));
    }
    let retry_after = error
        .retry_after()
        .filter(|_| error.is_retryable())
        .map(|delay| format!("after {} ms", delay.as_millis()));
    match (error.redirect_node(), retry_after) {
        (Some(node), Some(after)) => {
            lines.push(format!("retry: on node '{node}', or here {after}"));
        }
        (Some(node), None) => lines.push(format!("retry: on node '{node}'")),
        (None, Some(after)) => lines.push(format!("retry: {after}")),
        (None, None) => {}
    }
    lines
}

/// The whole error on one line, for places with room for one line: every message in the chain
/// joined by `: `, then the field paths of a typed server error's violations, with their
/// descriptions when the message does not already say it.
pub fn error_summary(error: &anyhow::Error) -> String {
    let mut summary = format!("{error:#}");
    for server in error.chain().filter_map(server_error) {
        for violation in server.field_violations() {
            if server.message().contains(&violation.description) {
                summary.push_str(&format!(" [{}]", violation.field));
            } else {
                summary.push_str(&format!(
                    " [{}: {}]",
                    violation.field, violation.description
                ));
            }
        }
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_api_grpc::status_from_error;
    use logpose_types::{LogPoseError, ResourceKind};

    fn client_error(error: &LogPoseError) -> ClientError {
        ClientError::from(status_from_error(error))
    }

    #[test]
    fn field_violations_and_metadata_are_listed_under_the_error() {
        let error = client_error(&LogPoseError::DimensionMismatch {
            field: "records[1].vector".to_owned(),
            record_id: Some("a".to_owned()),
            expected: 3,
            actual: 2,
        });
        assert_eq!(
            error.to_string(),
            "DIMENSION_MISMATCH: record 'a' expected 3 dimensions but found 2"
        );
        let details = server_error(&error).map(server_error_details);
        assert_eq!(
            details,
            Some(vec![
                "code: INVALID_ARGUMENT".to_owned(),
                "field records[1].vector: record 'a' expected 3 dimensions but found 2".to_owned(),
                "metadata: actual_dimensions=2, expected_dimensions=3, record_id=a".to_owned(),
            ])
        );
    }

    #[test]
    fn routing_errors_say_where_and_when_to_retry() {
        let error = client_error(&LogPoseError::NotOwner {
            collection: "default/docs".to_owned(),
            node: "node-a".to_owned(),
            owner_node: Some("node-b".to_owned()),
        });
        let details = server_error(&error)
            .map(server_error_details)
            .unwrap_or_default();
        assert_eq!(
            details.last().map(String::as_str),
            Some("retry: on node 'node-b', or here after 1000 ms")
        );

        let error = client_error(&LogPoseError::Unavailable {
            message: "etcd is unreachable".to_owned(),
            retry_after: Some(std::time::Duration::from_millis(250)),
        });
        let details = server_error(&error)
            .map(server_error_details)
            .unwrap_or_default();
        assert_eq!(details, ["code: UNAVAILABLE", "retry: after 250 ms"]);
    }

    #[test]
    fn errors_that_must_not_be_retried_show_no_retry_line() {
        let error = client_error(&LogPoseError::not_found(
            ResourceKind::Collection,
            "default/docs",
        ));
        let details = server_error(&error)
            .map(server_error_details)
            .unwrap_or_default();
        assert_eq!(
            details,
            [
                "code: NOT_FOUND",
                "metadata: resource_name=default/docs, resource_type=collection",
            ]
        );
    }

    #[test]
    fn summaries_join_the_chain_and_append_field_violations() {
        let error = anyhow::Error::new(client_error(&LogPoseError::invalid_field(
            "operations[0].id",
            "record id must not be empty",
        )))
        .context("failed to write records");
        assert_eq!(
            error_summary(&error),
            "failed to write records: INVALID_ARGUMENT: record id must not be empty \
             [operations[0].id]"
        );

        let plain = anyhow::anyhow!("no server involved").context("failed to read input");
        assert_eq!(
            error_summary(&plain),
            "failed to read input: no server involved"
        );
    }
}
