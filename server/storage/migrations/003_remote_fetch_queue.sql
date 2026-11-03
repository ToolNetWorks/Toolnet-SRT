-- ToolNet-SRT v0.3: remote URL durable fetch queue.
-- Run in the schema containing toolnet_srt_jobs. Never store plaintext signed URLs.
CREATE TABLE IF NOT EXISTS toolnet_srt_remote_fetch_jobs (
    job_id UUID PRIMARY KEY REFERENCES toolnet_srt_jobs(id) ON DELETE CASCADE,
    encrypted_url TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner VARCHAR(128),
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    last_error_code VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT remote_fetch_status_check CHECK (status IN ('pending', 'fetching', 'ready', 'failed', 'cancelled')),
    CONSTRAINT remote_fetch_attempt_check CHECK (attempt_count >= 0),
    CONSTRAINT remote_fetch_url_lifecycle_check CHECK (
        (status IN ('pending', 'fetching') AND encrypted_url IS NOT NULL AND OCTET_LENGTH(encrypted_url) BETWEEN 1 AND 16000)
        OR (status IN ('ready', 'failed', 'cancelled') AND encrypted_url IS NULL)
    ),
    CONSTRAINT remote_fetch_lease_check CHECK (
        (status = 'fetching' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'fetching' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS toolnet_srt_remote_fetch_pending_idx
ON toolnet_srt_remote_fetch_jobs (available_at, created_at) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS toolnet_srt_remote_fetch_expired_idx
ON toolnet_srt_remote_fetch_jobs (lease_expires_at) WHERE status = 'fetching';
