const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function createPostgresCancellation({ pool } = {}) {
  if (!pool || typeof pool.query !== "function") throw new Error("POSTGRES_POOL_REQUIRED");

  async function cancelJob(jobId) {
    if (!UUID_RE.test(jobId ?? "")) throw new Error("INVALID_JOB_ID");
    // Atomically cancel active jobs and fence out any existing worker lease.
    const result = await pool.query(`
      UPDATE toolnet_srt_jobs
      SET
        status = 'cancelled',
        stage = 'cancelled',
        lease_owner = NULL,
        lease_token = NULL,
        lease_expires_at = NULL,
        updated_at = NOW()
      WHERE id = $1::uuid
        AND status IN ('queued', 'processing')
      RETURNING id, status, stage, progress, created_at, updated_at
    `, [jobId]);
    return result.rows[0] ?? null;
  }

  return { cancelJob };
}
