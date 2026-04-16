# Security

LogPose is scaffolded with supply-chain and repository hygiene in mind.

The repository includes configuration for:

- dependency policy enforcement
- advisory scanning
- typo detection
- markdown and workflow validation

Sensitive findings should be reported privately according to `SECURITY.md`.

## API Authentication

LogPose supports optional bearer-token authentication for both REST and gRPC
APIs. When the `auth_token` configuration field is set, every request (except
health checks) must include an `Authorization: Bearer <token>` header matching
the configured value.

- **REST**: The `/health` endpoint is always unauthenticated. All `/v1/*`
  endpoints require a valid token. Unauthorized requests receive a `401` status
  with a `WWW-Authenticate: Bearer` header.
- **gRPC**: The health service (`grpc.health.v1.Health`) is always
  unauthenticated. All `LogPoseService` RPCs require a valid token.
  Unauthorized requests receive an `UNAUTHENTICATED` gRPC status.

When `auth_token` is omitted, APIs operate in unauthenticated mode. An empty
string is rejected at startup as a misconfiguration.

Token comparison uses constant-time equality (`subtle::ConstantTimeEq`) to
prevent timing side-channel attacks.

## Request Size Limits

The REST API enforces a 16 MiB maximum request body size to prevent
memory-exhaustion denial-of-service attacks.

## Collection Name Validation

Collection names are validated to prevent path-traversal and related attacks:

- Must not be empty
- Must not exceed 256 bytes
- Must not contain path separators (`/`, `\`)
- Must not contain traversal sequences (`..`)
- Must not contain control characters
