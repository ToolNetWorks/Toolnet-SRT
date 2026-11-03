# ToolNet-SRT v0.3.0 — draft release notes

Status: **not released**. Production rollout and npm publication require separate approval after staging and release checks pass.

## Changes

- Durable job persistence in PostgreSQL and source/result storage in R2.
- Asynchronous URL ingestion with encrypted URLs, idempotency, DNS pinning and lease fencing.
- Durable workers, cancellation, progress tracking, checkpointed audio chunks and recovery.
- HTTP and MCP job operations, an idempotency-aware client and TypeScript declarations.
- Docker runtime dependency installation and sanitized ASR errors.

## Upgrade considerations

- Durable mode is disabled by default. Applying migrations 002–005, provisioning stable independent encryption and idempotency keys, and enabling the feature flag require an explicit rollout plan.
- URL requests in durable mode require a stable `Idempotency-Key`; the MCP tool accepts `idempotency_key`.
- The configured API key represents one tenant and grants access to all its jobs; this is not multi-tenant isolation.
- Fetch disk reservations and API creation limits are per process. Overlap deduplication is conservative and may retain near-duplicates.
- Chunk extraction currently decodes from the beginning of the local source for each chunk. Benchmark actual long media and the selected hosting plan.
- Development tests and the durable server require Node.js 20.19+ or 22.12+; the public package retains its existing version until release approval.

## Rollback

Stop or disable the durable schedulers and return the service to the previously verified image and configuration. Preserve PostgreSQL migrations, encryption keys, idempotency key and R2 objects; do not drop or reverse migrations during an application rollback. Durable jobs need a compatible worker to resume. Validate the previous deployment identifier and recovery procedure in the target Render account before release.
