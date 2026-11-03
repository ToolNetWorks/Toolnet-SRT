-- Apply only to the selected durable schema after staging verification.
CREATE TABLE IF NOT EXISTS toolnet_srt_asr_control (
  provider TEXT PRIMARY KEY,
  configured_limit INTEGER NOT NULL CHECK (configured_limit BETWEEN 1 AND 2),
  adaptive BOOLEAN NOT NULL,
  quarantine_seconds INTEGER NOT NULL CHECK (quarantine_seconds BETWEEN 300 AND 86400),
  cooldown_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
  reduced_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS toolnet_srt_asr_slots (
  provider TEXT NOT NULL REFERENCES toolnet_srt_asr_control(provider) ON DELETE CASCADE,
  slot SMALLINT NOT NULL CHECK (slot BETWEEN 1 AND 2),
  token UUID NOT NULL UNIQUE,
  -- Keep the slot until release/expiry even if its job row is removed.
  job_id UUID NOT NULL,
  chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
  worker_id VARCHAR(128) NOT NULL,
  job_lease_token UUID NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  reclaim_after TIMESTAMPTZ NOT NULL,
  CHECK (reclaim_after >= expires_at),
  PRIMARY KEY (provider, slot)
);
