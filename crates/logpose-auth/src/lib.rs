//! Authentication and authorization foundations.

use subtle::ConstantTimeEq;

/// Access model scaffold used to grow operator and service permissions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessTier {
    /// Full cluster administration privileges.
    Operator,
    /// Standard application workload privileges.
    Service,
    /// Read-only observability and metadata access.
    Observer,
}

/// Validate a bearer token extracted from an `Authorization` header value.
///
/// Returns `Ok(())` when the header is present with a `Bearer` scheme and the
/// token matches `expected_token`.  Returns a human-readable static error
/// description when authentication cannot be established.
pub fn validate_bearer_token(
    authorization_header: Option<&str>,
    expected_token: &str,
) -> Result<(), &'static str> {
    let header_value = authorization_header.ok_or("missing Authorization header")?;
    let token = header_value
        .strip_prefix("Bearer ")
        .ok_or("Authorization header must use Bearer scheme")?;
    if token.len() == expected_token.len()
        && bool::from(token.as_bytes().ct_eq(expected_token.as_bytes()))
    {
        Ok(())
    } else {
        Err("invalid bearer token")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_bearer_token() {
        assert!(validate_bearer_token(Some("Bearer my-secret"), "my-secret").is_ok());
    }

    #[test]
    fn rejects_missing_authorization_header() {
        let error = validate_bearer_token(None, "my-secret").expect_err("should reject");
        assert!(error.contains("missing"));
    }

    #[test]
    fn rejects_non_bearer_scheme() {
        let error = validate_bearer_token(Some("Basic abc"), "abc").expect_err("should reject");
        assert!(error.contains("Bearer"));
    }

    #[test]
    fn rejects_incorrect_token() {
        let error =
            validate_bearer_token(Some("Bearer wrong"), "my-secret").expect_err("should reject");
        assert!(error.contains("invalid"));
    }
}
