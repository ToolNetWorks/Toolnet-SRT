-- Durable ASR checkpoints. Apply only to the intended schema.
CREATE TABLE IF NOT EXISTS toolnet_srt_job_chunks (
  job_id UUID NOT NULL REFERENCES toolnet_srt_jobs(id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
  start_offset_ms BIGINT NOT NULL CHECK (start_offset_ms >= 0),
  duration_ms BIGINT NOT NULL CHECK (duration_ms > 0),
  overlap_ms INTEGER NOT NULL CHECK (overlap_ms >= 0 AND overlap_ms < duration_ms),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  lease_token UUID,
  error_code TEXT,
  segments JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (job_id, chunk_index),
  CHECK ((status = 'processing') = (lease_token IS NOT NULL)),
  CHECK ((status = 'completed') = (segments IS NOT NULL)),
  CHECK (segments IS NULL OR jsonb_typeof(segments) = 'array')
);
