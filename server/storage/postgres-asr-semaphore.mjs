import { randomUUID } from 'node:crypto';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const WORKER = /^[a-zA-Z0-9._:-]{1,128}$/;

export function createPostgresAsrSemaphore({ pool, limit = 1, leaseSeconds = 120, adaptive = true, quarantineSeconds = 900 } = {}) {
  if (typeof pool?.connect !== 'function') throw new Error('ASR_SEMAPHORE_POOL_REQUIRED');
  if (![1, 2].includes(limit)) throw new Error('INVALID_ASR_GLOBAL_CONCURRENCY');
  if (!Number.isSafeInteger(leaseSeconds) || leaseSeconds < 30 || leaseSeconds > 3600) throw new Error('INVALID_ASR_SLOT_LEASE');
  if (typeof adaptive !== 'boolean') throw new Error('INVALID_ASR_ADAPTIVE_CONCURRENCY');
  if (!Number.isSafeInteger(quarantineSeconds) || quarantineSeconds < 300 || quarantineSeconds > 86400) throw new Error('INVALID_ASR_QUARANTINE');
  const provider = 'jianying';
  function validate(input) {
    if (!UUID.test(input?.jobId ?? '') || !UUID.test(input?.jobLeaseToken ?? '') ||
        !WORKER.test(input?.workerId ?? '') || !Number.isSafeInteger(input?.chunkIndex) || input.chunkIndex < 0) {
      throw new Error('INVALID_ASR_SLOT_OWNER');
    }
  }
  function validateToken(input) {
    validate(input);
    if (!UUID.test(input?.token ?? '')) throw new Error('INVALID_ASR_SLOT_TOKEN');
  }
  async function transaction(fn) {
    const client = await pool.connect(); let begun = false, committing = false, discard = false;
    try {
      await client.query('BEGIN'); begun = true;
      const value = await fn(client);
      committing = true; await client.query('COMMIT'); begun = false;
      return value;
    } catch (error) {
      if (begun && !committing) await client.query('ROLLBACK').catch(() => { discard = true; });
      throw error;
    } finally { client.release(discard || (begun && committing) ? true : undefined); }
  }
  async function lockControl(client) {
    await client.query(`INSERT INTO toolnet_srt_asr_control(provider, configured_limit, adaptive, quarantine_seconds)
      VALUES ($1,$2,$3,$4) ON CONFLICT (provider) DO NOTHING`, [provider, limit, adaptive, quarantineSeconds]);
    const result = await client.query(`SELECT configured_limit, adaptive, quarantine_seconds, cooldown_until > clock_timestamp() AS cooling,
      reduced_until > clock_timestamp() AS reduced FROM toolnet_srt_asr_control WHERE provider=$1 FOR UPDATE`, [provider]);
    const row = result.rows[0];
    // Every instance must agree; do not silently increase the global limit.
    if (!row || row.configured_limit !== limit || row.adaptive !== adaptive || row.quarantine_seconds !== quarantineSeconds) throw new Error('ASR_GLOBAL_LIMIT_MISMATCH');
    return row;
  }
  async function acquire(input) {
    validate(input);
    return transaction(async client => {
      const state = await lockControl(client);
      if (state.cooling) return null;
      const owner = await client.query(`SELECT 1 FROM toolnet_srt_jobs WHERE id=$1::uuid AND status='processing'
        AND lease_owner=$2 AND lease_token=$3::uuid AND lease_expires_at>clock_timestamp()`,
      [input.jobId, input.workerId, input.jobLeaseToken]);
      if (!owner.rowCount) throw new Error('ASR_JOB_LEASE_LOST');
      await client.query(`DELETE FROM toolnet_srt_asr_slots WHERE provider=$1 AND reclaim_after<=clock_timestamp()`, [provider]);
      const active = await client.query(`SELECT slot, job_id, worker_id, job_lease_token, expires_at<=clock_timestamp() AS uncertain
        FROM toolnet_srt_asr_slots WHERE provider=$1`, [provider]);
      // A previously dispatched request with an expired lease may still run
      // at the provider. Do not admit new work during its quarantine.
      if (active.rows.some(row => row.uncertain)) return null;
      // Keep a cohort bound to one parent lease. If its process cannot persist
      // a 429, other workers remain blocked by its reservation until quarantine.
      if (active.rows.some(row => row.job_id !== input.jobId.toLowerCase() || row.worker_id !== input.workerId ||
          row.job_lease_token !== input.jobLeaseToken.toLowerCase())) return null;
      const effectiveLimit = adaptive && state.reduced ? 1 : limit;
      // Do not grant a new slot while the existing population already meets the reduced limit.
      if (active.rows.length >= effectiveLimit) return null;
      const occupied = new Set(active.rows.map(row => Number(row.slot)));
      const slot = [1, 2].slice(0, limit).find(n => !occupied.has(n));
      if (!slot) return null;
      const token = randomUUID();
      await client.query(`INSERT INTO toolnet_srt_asr_slots
        (provider,slot,token,job_id,chunk_index,worker_id,job_lease_token,expires_at,reclaim_after)
        VALUES ($1,$2,$3::uuid,$4::uuid,$5,$6,$7::uuid,clock_timestamp()+$8*interval '1 second',
          clock_timestamp()+($8+$9)*interval '1 second')`,
      [provider,slot,token,input.jobId,input.chunkIndex,input.workerId,input.jobLeaseToken,leaseSeconds,quarantineSeconds]);
      return { ...input, token, slot };
    });
  }
  async function heartbeat(input) {
    validateToken(input);
    return transaction(async client => {
      const result = await client.query(`UPDATE toolnet_srt_asr_slots s SET expires_at=clock_timestamp()+$7*interval '1 second',
        reclaim_after=clock_timestamp()+($7+$8)*interval '1 second'
        WHERE provider=$1 AND token=$2::uuid AND job_id=$3::uuid AND chunk_index=$4 AND worker_id=$5
          AND job_lease_token=$6::uuid AND expires_at>clock_timestamp()
          AND EXISTS (SELECT 1 FROM toolnet_srt_jobs j WHERE j.id=s.job_id AND j.status='processing'
            AND j.lease_owner=s.worker_id AND j.lease_token=s.job_lease_token AND j.lease_expires_at>clock_timestamp())
        RETURNING slot`, [provider,input.token,input.jobId,input.chunkIndex,input.workerId,input.jobLeaseToken,leaseSeconds,quarantineSeconds]);
      return result.rowCount === 1;
    });
  }
  async function release(input) {
    validateToken(input);
    return transaction(async client => {
      const result = await client.query(`DELETE FROM toolnet_srt_asr_slots WHERE provider=$1 AND token=$2::uuid
        AND job_id=$3::uuid AND chunk_index=$4 AND worker_id=$5 AND job_lease_token=$6::uuid
        AND expires_at>clock_timestamp()`,
      [provider,input.token,input.jobId,input.chunkIndex,input.workerId,input.jobLeaseToken]);
      return result.rowCount === 1;
    });
  }
  async function rateLimited(input, retryAfterMs) {
    validateToken(input);
    if (!Number.isSafeInteger(retryAfterMs) || retryAfterMs < 0) throw new Error('INVALID_ASR_COOLDOWN');
    return transaction(async client => {
      await lockControl(client);
      // A stale token cannot alter global state or release the winner's slot.
      const owned = await client.query(`SELECT 1 FROM toolnet_srt_asr_slots WHERE provider=$1 AND token=$2::uuid
        AND job_id=$3::uuid AND chunk_index=$4 AND worker_id=$5 AND job_lease_token=$6::uuid
        AND expires_at>clock_timestamp()`, [provider,input.token,input.jobId,input.chunkIndex,input.workerId,input.jobLeaseToken]);
      if (!owned.rowCount) return false;
      await client.query(`UPDATE toolnet_srt_asr_control SET
        cooldown_until=GREATEST(cooldown_until,CASE WHEN $2::double precision>3153600000000
          THEN 'infinity'::timestamptz ELSE clock_timestamp()+$2::double precision*interval '1 millisecond' END),
        reduced_until=GREATEST(reduced_until,CASE WHEN $2::double precision>3153600000000
          THEN 'infinity'::timestamptz ELSE clock_timestamp()+($2::double precision+60000)*interval '1 millisecond' END),
        updated_at=clock_timestamp() WHERE provider=$1`, [provider,retryAfterMs]);
      // A 429 may occur while an upstream asynchronous task is still running.
      // Fence the token but retain the reservation through quarantine.
      await client.query(`UPDATE toolnet_srt_asr_slots SET expires_at=clock_timestamp()
        WHERE provider=$1 AND token=$2::uuid`, [provider,input.token]);
      return true;
    });
  }
  return { acquire, heartbeat, release, rateLimited, leaseSeconds, quarantineSeconds };
}
