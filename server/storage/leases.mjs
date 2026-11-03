import { randomUUID } from "node:crypto";

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const OWNER_RE = /^[a-zA-Z0-9._:-]{1,128}$/;
const ERROR_RE = /^[A-Z0-9_]{1,64}$/;

function validateId(id) {
  if (!UUID_RE.test(id ?? "")) throw new Error("INVALID_JOB_ID");
}

function validateLease(workerId, leaseToken) {
  if (!OWNER_RE.test(workerId ?? "")) throw new Error("INVALID_WORKER_ID");
  if (leaseToken !== undefined && !UUID_RE.test(leaseToken)) {
    throw new Error("INVALID_LEASE_TOKEN");
  }
}

function validateRange(value, min, max, code) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(code);
}

export function createPostgresLeaseManager({ pool } = {}) {
  if (!pool || typeof pool.query !== "function") throw new Error("POSTGRES_POOL_REQUIRED");

  async function claimJob(workerId, { leaseSeconds = 120, maxAttempts = 3 } = {}) {
    validateLease(workerId);
    validateRange(leaseSeconds, 30, 3600, "INVALID_LEASE_SECONDS");
    validateRange(maxAttempts, 1, 20, "INVALID_MAX_ATTEMPTS");
    const token = randomUUID();
    const result = await pool.query(`
      WITH candidate AS (
        SELECT id
        FROM toolnet_srt_jobs
        WHERE source_key IS NOT NULL
          AND available_at <= NOW()
          AND attempt_count < $3
          AND (status = 'queued' OR (status = 'processing' AND lease_expires_at < NOW()))
        ORDER BY available_at, created_at, id
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE toolnet_srt_jobs AS jobs
      SET
        status = 'processing',
        stage = 'claimed',
        -- Each lease attempt starts from a clean progress state.
        progress = 0,
        completed_segments = 0,
        total_segments = NULL,
        error_code = NULL,
        result_key = NULL,
        lease_owner = $1,
        lease_token = $2::uuid,
        lease_expires_at = NOW() + ($4::int * INTERVAL '1 second'),
        attempt_count = jobs.attempt_count + 1,
        updated_at = NOW()
      FROM candidate
      WHERE jobs.id = candidate.id
      RETURNING jobs.id, jobs.status, jobs.attempt_count, jobs.source_key,
        jobs.extension, jobs.lease_token, jobs.lease_expires_at
    `, [workerId, token, maxAttempts, leaseSeconds]);
    return result.rows[0] ?? null;
  }

  async function heartbeatJob({ jobId, workerId, leaseToken, leaseSeconds = 120 }) {
    validateId(jobId);
    validateLease(workerId, leaseToken);
    validateRange(leaseSeconds, 30, 3600, "INVALID_LEASE_SECONDS");
    const result = await pool.query(`
      UPDATE toolnet_srt_jobs
      SET lease_expires_at = NOW() + ($4::int * INTERVAL '1 second'), updated_at = NOW()
      WHERE id = $1::uuid
        AND lease_owner = $2
        AND lease_token = $3::uuid
        AND status = 'processing'
        AND lease_expires_at > NOW()
      RETURNING id, lease_expires_at
    `, [jobId, workerId, leaseToken, leaseSeconds]);
    return result.rows[0] ?? null;
  }

  async function completeJob({ jobId, workerId, leaseToken, resultKey }) {
    validateId(jobId);
    validateLease(workerId, leaseToken);
    // The result must belong to this exact lease attempt.
    const expectedKey = `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt`;
    if (resultKey !== expectedKey) throw new Error("INVALID_RESULT_KEY");
    // Caller must upload and verify the SRT on R2 first.
    const result = await pool.query(`
      UPDATE toolnet_srt_jobs
      SET status = 'completed', stage = 'completed', progress = 100, result_key = $4,
        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
      WHERE id = $1::uuid
        AND lease_owner = $2
        AND lease_token = $3::uuid
        AND status = 'processing'
        AND lease_expires_at > NOW()
      RETURNING id, status, result_key
    `, [jobId, workerId, leaseToken, resultKey]);
    return result.rows[0] ?? null;
  }

  async function failJob({ jobId, workerId, leaseToken, errorCode, maxAttempts = 3, retryDelaySeconds = 30 }) {
    validateId(jobId);
    validateLease(workerId, leaseToken);
    if (!ERROR_RE.test(errorCode ?? "")) throw new Error("INVALID_ERROR_CODE");
    validateRange(maxAttempts, 1, 20, "INVALID_MAX_ATTEMPTS");
    validateRange(retryDelaySeconds, 1, 86400, "INVALID_RETRY_DELAY");
    const result = await pool.query(`
      UPDATE toolnet_srt_jobs
      SET
        status = CASE WHEN attempt_count < $5 THEN 'queued' ELSE 'failed' END,
        stage = CASE WHEN attempt_count < $5 THEN 'retry_wait' ELSE 'failed' END,
        error_code = $4,
        available_at = CASE WHEN attempt_count < $5
          THEN NOW() + ($6::int * INTERVAL '1 second') ELSE available_at END,
        lease_owner = NULL,
        lease_token = NULL,
        lease_expires_at = NULL,
        updated_at = NOW()
      WHERE id = $1::uuid
        AND lease_owner = $2
        AND lease_token = $3::uuid
        AND status = 'processing'
        AND lease_expires_at > NOW()
      RETURNING id, status, attempt_count, available_at
    `, [jobId, workerId, leaseToken, errorCode, maxAttempts, retryDelaySeconds]);
    return result.rows[0] ?? null;
  }

  async function reapExhaustedJobs(maxAttempts = 3) {
    validateRange(maxAttempts, 1, 20, "INVALID_MAX_ATTEMPTS");
    const result = await pool.query(`
      UPDATE toolnet_srt_jobs
      SET status = 'failed', stage = 'failed', error_code = 'LEASE_RETRY_EXHAUSTED',
        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
      WHERE status = 'processing'
        AND lease_expires_at < NOW()
        AND attempt_count >= $1
      RETURNING id
    `, [maxAttempts]);
    return result.rows.map(row => row.id);
  }

  return { claimJob, heartbeatJob, completeJob, failJob, reapExhaustedJobs };
}
