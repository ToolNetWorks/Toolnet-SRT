-- Run only in the schema containing toolnet_srt_jobs.
CREATE TABLE IF NOT EXISTS toolnet_srt_remote_idempotency (
  key_digest BYTEA PRIMARY KEY CHECK (octet_length(key_digest) = 32),
  request_digest BYTEA NOT NULL CHECK (octet_length(request_digest) = 32),
  job_id UUID NOT NULL UNIQUE REFERENCES toolnet_srt_jobs(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
