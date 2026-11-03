const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const OWNER_RE = /^[a-zA-Z0-9._:-]{1,128}$/;
const ERROR_RE = /^[A-Z][A-Z0-9_]{0,79}$/;

export function createPostgresChunkCheckpoints({ pool } = {}) {
  if (typeof pool?.connect !== 'function') throw new Error('CHUNK_POOL_REQUIRED');
  function validateLease({ jobId, workerId, leaseToken }) {
    if (!UUID_RE.test(jobId ?? '') || !UUID_RE.test(leaseToken ?? '') || !OWNER_RE.test(workerId ?? '')) throw new Error('INVALID_CHUNK_LEASE');
  }
  async function transaction(lease, operation) {
    validateLease(lease);
    const client = await pool.connect(); let begun = false, committing = false, discard = false;
    try {
      await client.query('BEGIN'); begun = true;
      // The parent lock serializes claim, cancellation, reclaim and checkpoint writes.
      const owner = await client.query(`SELECT id FROM toolnet_srt_jobs
        WHERE id=$1::uuid AND status='processing' AND lease_owner=$2 AND lease_token=$3::uuid
          AND lease_expires_at > clock_timestamp() FOR UPDATE`, [lease.jobId, lease.workerId, lease.leaseToken]);
      if (!owner.rows.length) { await client.query('ROLLBACK'); begun = false; return null; }
      const result = await operation(client);
      const stillActive = await client.query(`SELECT 1 FROM toolnet_srt_jobs WHERE id=$1::uuid
        AND status='processing' AND lease_owner=$2 AND lease_token=$3::uuid
        AND lease_expires_at > clock_timestamp()`, [lease.jobId, lease.workerId, lease.leaseToken]);
      if (!stillActive.rows.length) { await client.query('ROLLBACK'); begun = false; return null; }
      committing = true; await client.query('COMMIT'); begun = false; return result;
    } catch (error) {
      if (begun && !committing) await client.query('ROLLBACK').catch(() => { discard = true; });
      throw error;
    } finally { client.release(discard || (begun && committing) ? true : undefined); }
  }
  async function initialize(lease, plan) {
    if (!Array.isArray(plan) || !plan.length || plan.length > 1000) throw new Error('INVALID_CHUNK_PLAN');
    for (const [index, item] of plan.entries()) {
      if (item.index !== index || !Number.isSafeInteger(item.startOffsetMs) || item.startOffsetMs < 0 ||
          !Number.isSafeInteger(item.durationMs) || item.durationMs < 1 || !Number.isSafeInteger(item.overlapMs) ||
          item.overlapMs < 0 || item.overlapMs >= item.durationMs) throw new Error('INVALID_CHUNK_PLAN');
    }
    return transaction(lease, async client => {
      for (const item of plan) await client.query(`INSERT INTO toolnet_srt_job_chunks
        (job_id,chunk_index,start_offset_ms,duration_ms,overlap_ms) VALUES ($1::uuid,$2,$3,$4,$5)
        ON CONFLICT (job_id,chunk_index) DO NOTHING`, [lease.jobId,item.index,item.startOffsetMs,item.durationMs,item.overlapMs]);
      const result = await client.query('SELECT * FROM toolnet_srt_job_chunks WHERE job_id=$1::uuid ORDER BY chunk_index', [lease.jobId]);
      if (result.rows.length !== plan.length || result.rows.some((row, i) => row.chunk_index !== i ||
          Number(row.start_offset_ms) !== plan[i].startOffsetMs || Number(row.duration_ms) !== plan[i].durationMs ||
          row.overlap_ms !== plan[i].overlapMs)) throw new Error('CHUNK_PLAN_MISMATCH');
      return result.rows;
    });
  }
  async function claim(lease, index, { maxAttempts = 3 } = {}) {
    if (!Number.isSafeInteger(index) || index < 0) throw new Error('INVALID_CHUNK_INDEX');
    if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 20) throw new Error('INVALID_CHUNK_MAX_ATTEMPTS');
    return transaction(lease, async client => {
      const result = await client.query(`UPDATE toolnet_srt_job_chunks SET status='processing',
        lease_token=$3::uuid, attempt_count=attempt_count+1, error_code=NULL, updated_at=NOW()
        WHERE job_id=$1::uuid AND chunk_index=$2 AND status IN ('pending','failed','processing')
          AND attempt_count < $4
          AND (status <> 'processing' OR lease_token <> $3::uuid)
        RETURNING *`, [lease.jobId,index,lease.leaseToken,maxAttempts]);
      return result.rows[0] ?? null;
    });
  }
  async function complete(lease, index, segments) {
    if (!Number.isSafeInteger(index) || index < 0 || !Array.isArray(segments)) throw new Error('INVALID_CHUNK_RESULT');
    const data = JSON.stringify(segments);
    if (Buffer.byteLength(data) > 1024 * 1024) throw new Error('CHUNK_RESULT_TOO_LARGE');
    return transaction(lease, async client => {
      const result = await client.query(`UPDATE toolnet_srt_job_chunks SET status='completed', segments=$4::jsonb,
        lease_token=NULL, error_code=NULL, updated_at=NOW() WHERE job_id=$1::uuid AND chunk_index=$2
        AND status='processing' AND lease_token=$3::uuid RETURNING *`, [lease.jobId,index,lease.leaseToken,data]);
      return result.rows[0] ?? null;
    });
  }
  async function fail(lease, index, errorCode = 'CHUNK_ASR_FAILED') {
    if (!Number.isSafeInteger(index) || index < 0 || !ERROR_RE.test(errorCode)) throw new Error('INVALID_CHUNK_ERROR');
    return transaction(lease, async client => {
      const result = await client.query(`UPDATE toolnet_srt_job_chunks SET status='failed', lease_token=NULL,
        error_code=$4, updated_at=NOW() WHERE job_id=$1::uuid AND chunk_index=$2 AND status='processing'
        AND lease_token=$3::uuid RETURNING *`, [lease.jobId,index,lease.leaseToken,errorCode]);
      return result.rows[0] ?? null;
    });
  }
  return { initialize, claim, complete, fail };
}
