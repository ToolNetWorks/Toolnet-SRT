import { randomUUID } from 'node:crypto';
import { remoteSourceKey } from './source-keys.mjs';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const OWNER_RE = /^[a-zA-Z0-9_.:-]{1,128}$/;
const ERROR_RE = /^[A-Z][A-Z0-9_]{0,79}$/;

function positiveInteger(value, name, min, max) {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`INVALID_${name}`);
  }
  return value;
}

function checkLease({ jobId, workerId, token }) {
  if (typeof jobId !== 'string' || !UUID_RE.test(jobId)) {
    throw new Error('INVALID_JOB_ID');
  }
  if (typeof workerId !== 'string' || !OWNER_RE.test(workerId)) {
    throw new Error('INVALID_WORKER_ID');
  }
  if (typeof token !== 'string' || !UUID_RE.test(token)) {
    throw new Error('INVALID_LEASE_TOKEN');
  }
}

function isCurrentLease(row, workerId, token) {
  return row && row.status === 'fetching' &&
    row.job_status === 'queued' && row.source_key === null &&
    row.lease_owner === workerId && row.lease_token === token &&
    row.lease_active === true;
}

export function createPostgresRemoteLeases({
  pool,
  maxAttempts = 3,
  leaseSeconds = 120,
  retryDelaySeconds = 30,
} = {}) {
  if (typeof pool?.connect !== 'function') {
    throw new Error('REMOTE_LEASE_POOL_REQUIRED');
  }
  positiveInteger(maxAttempts, 'MAX_ATTEMPTS', 1, 10);
  positiveInteger(leaseSeconds, 'LEASE_SECONDS', 30, 3600);
  positiveInteger(retryDelaySeconds, 'RETRY_DELAY_SECONDS', 1, 3600);

  async function transaction(operation) {
    const client = await pool.connect();
    let open = false;
    try {
      await client.query('BEGIN');
      open = true;
      const result = await operation(client);
      await client.query('COMMIT');
      open = false;
      return result;
    } catch (error) {
      if (open) await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async function lockedLease(client, jobId) {
    const { rows } = await client.query(`
      SELECT q.job_id, q.status, q.lease_owner,
             q.lease_token, q.attempt_count,
             q.lease_expires_at > clock_timestamp() AS lease_active,
             j.status AS job_status, j.source_key, j.extension
      FROM toolnet_srt_remote_fetch_jobs q
      JOIN toolnet_srt_jobs j ON j.id = q.job_id
      WHERE q.job_id = $1::uuid
      FOR UPDATE OF q, j
    `, [jobId]);
    return rows[0] ?? null;
  }

  async function claimNext(workerId) {
    if (typeof workerId !== 'string' || !OWNER_RE.test(workerId)) {
      throw new Error('INVALID_WORKER_ID');
    }

    return transaction(async client => {
      const { rows } = await client.query(`
        SELECT q.job_id, q.encrypted_url, q.attempt_count, j.extension
        FROM toolnet_srt_remote_fetch_jobs q
        JOIN toolnet_srt_jobs j ON j.id = q.job_id
        WHERE j.status = 'queued'
          AND j.source_key IS NULL
          AND q.attempt_count < $1
          AND (
            (q.status = 'pending' AND q.available_at <= clock_timestamp())
            OR
            (q.status = 'fetching' AND q.lease_expires_at <= clock_timestamp())
          )
        ORDER BY q.available_at, q.created_at, q.job_id
        LIMIT 1
        FOR UPDATE OF q, j SKIP LOCKED
      `, [maxAttempts]);
      const selected = rows[0];
      if (!selected) return null;

      const token = randomUUID();
      const updated = await client.query(`
        UPDATE toolnet_srt_remote_fetch_jobs
        SET status = 'fetching',
            attempt_count = attempt_count + 1,
            lease_owner = $2,
            lease_token = $3::uuid,
            lease_expires_at = clock_timestamp() + ($4 * INTERVAL '1 second'),
            updated_at = NOW()
        WHERE job_id = $1::uuid
        RETURNING attempt_count, lease_expires_at
      `, [selected.job_id, workerId, token, leaseSeconds]);

      await client.query(`
        UPDATE toolnet_srt_jobs
        SET stage = 'fetching', updated_at = NOW()
        WHERE id = $1::uuid AND status = 'queued' AND source_key IS NULL
      `, [selected.job_id]);

      return {
        jobId: selected.job_id,
        encryptedUrl: selected.encrypted_url,
        extension: selected.extension,
        workerId,
        token,
        attemptCount: updated.rows[0].attempt_count,
        leaseExpiresAt: updated.rows[0].lease_expires_at,
      };
    });
  }

  async function heartbeat({ jobId, workerId, token }) {
    checkLease({ jobId, workerId, token });
    return transaction(async client => {
      const row = await lockedLease(client, jobId);
      if (!isCurrentLease(row, workerId, token)) return false;
      await client.query(`
        UPDATE toolnet_srt_remote_fetch_jobs
        SET lease_expires_at = clock_timestamp() + ($2 * INTERVAL '1 second'),
            updated_at = NOW()
        WHERE job_id = $1::uuid
      `, [jobId, leaseSeconds]);
      return true;
    });
  }

  async function completeFetch({ jobId, workerId, token, sourceKey }) {
    checkLease({ jobId, workerId, token });
    return transaction(async client => {
      const row = await lockedLease(client, jobId);
      if (!isCurrentLease(row, workerId, token)) return false;

      let expectedKey;
      try { expectedKey = remoteSourceKey(jobId, token, `source${row.extension}`); }
      catch { throw new Error('INVALID_SOURCE_KEY'); }
      if (sourceKey !== expectedKey) throw new Error('INVALID_SOURCE_KEY');

      await client.query(`
        UPDATE toolnet_srt_jobs
        SET source_key = $2, stage = 'queued', updated_at = NOW()
        WHERE id = $1::uuid
      `, [jobId, sourceKey]);

      await client.query(`
        UPDATE toolnet_srt_remote_fetch_jobs
        SET status = 'ready', encrypted_url = NULL,
            lease_owner = NULL, lease_token = NULL,
            lease_expires_at = NULL, last_error_code = NULL,
            updated_at = NOW(), finished_at = NOW()
        WHERE job_id = $1::uuid
      `, [jobId]);
      return true;
    });
  }

  async function failFetch({ jobId, workerId, token,
    errorCode = 'SOURCE_DOWNLOAD_FAILED', retryable = true }) {
    checkLease({ jobId, workerId, token });
    if (typeof errorCode !== 'string' || !ERROR_RE.test(errorCode)) {
      throw new Error('INVALID_FETCH_ERROR_CODE');
    }
    if (typeof retryable !== 'boolean') {
      throw new Error('INVALID_RETRYABLE');
    }

    return transaction(async client => {
      const row = await lockedLease(client, jobId);
      if (!isCurrentLease(row, workerId, token)) {
        return { updated: false };
      }

      const willRetry = retryable && row.attempt_count < maxAttempts;
      if (willRetry) {
        const cap = Math.min(3600,
          retryDelaySeconds * 2 ** Math.min(row.attempt_count - 1, 6));
        const seconds = Math.max(1, Math.ceil(cap * (0.5 + Math.random() * 0.5)));
        await client.query(`
          UPDATE toolnet_srt_remote_fetch_jobs
          SET status = 'pending', lease_owner = NULL,
              lease_token = NULL, lease_expires_at = NULL,
              last_error_code = $2,
              available_at = clock_timestamp() + ($3 * INTERVAL '1 second'),
              updated_at = NOW()
          WHERE job_id = $1::uuid
        `, [jobId, errorCode, seconds]);
        await client.query(`
          UPDATE toolnet_srt_jobs
          SET stage = 'fetch_retry', updated_at = NOW()
          WHERE id = $1::uuid
        `, [jobId]);
        return { updated: true, status: 'pending', retryDelaySeconds: seconds };
      }

      await client.query(`
        UPDATE toolnet_srt_remote_fetch_jobs
        SET status = 'failed', encrypted_url = NULL,
            lease_owner = NULL, lease_token = NULL,
            lease_expires_at = NULL, last_error_code = $2,
            updated_at = NOW(), finished_at = NOW()
        WHERE job_id = $1::uuid
      `, [jobId, errorCode]);
      await client.query(`
        UPDATE toolnet_srt_jobs
        SET status = 'failed', stage = 'failed', error_code = $2,
            updated_at = NOW()
        WHERE id = $1::uuid
      `, [jobId, errorCode]);
      return { updated: true, status: 'failed' };
    });
  }

  async function reapExhausted(limit = 100) {
    positiveInteger(limit, 'REAP_LIMIT', 1, 100);
    return transaction(async client => {
      const { rows } = await client.query(`
        SELECT q.job_id
        FROM toolnet_srt_remote_fetch_jobs q
        JOIN toolnet_srt_jobs j ON j.id = q.job_id
        WHERE q.status = 'fetching'
          AND q.lease_expires_at <= clock_timestamp()
          AND q.attempt_count >= $1
          AND j.status = 'queued' AND j.source_key IS NULL
        ORDER BY q.lease_expires_at, q.job_id
        LIMIT $2
        FOR UPDATE OF q, j SKIP LOCKED
      `, [maxAttempts, limit]);
      const ids = rows.map(row => row.job_id);
      if (!ids.length) return 0;

      await client.query(`
        UPDATE toolnet_srt_remote_fetch_jobs
        SET status = 'failed', encrypted_url = NULL,
            lease_owner = NULL, lease_token = NULL,
            lease_expires_at = NULL,
            last_error_code = 'SOURCE_RETRY_EXHAUSTED',
            updated_at = NOW(), finished_at = NOW()
        WHERE job_id = ANY($1::uuid[])
      `, [ids]);
      await client.query(`
        UPDATE toolnet_srt_jobs
        SET status = 'failed', stage = 'failed',
            error_code = 'SOURCE_RETRY_EXHAUSTED', updated_at = NOW()
        WHERE id = ANY($1::uuid[])
      `, [ids]);
      return ids.length;
    });
  }

  return { claimNext, heartbeat, completeFetch, failFetch, reapExhausted };
}
