BEGIN;

ALTER TABLE toolnet_srt_jobs
  ADD COLUMN IF NOT EXISTS lease_owner TEXT,
  ADD COLUMN IF NOT EXISTS lease_token UUID,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS
  idx_toolnet_srt_jobs_claim
ON toolnet_srt_jobs (available_at, created_at, id)
WHERE status IN ('queued', 'processing')
  AND source_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS
  idx_toolnet_srt_jobs_lease_expiry
ON toolnet_srt_jobs (lease_expires_at)
WHERE status = 'processing';

COMMIT;
