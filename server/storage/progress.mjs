const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const WORKER_RE = /^[a-zA-Z0-9._:-]{1,128}$/;
const STAGES = new Map([
  ["preparing", 0], ["downloading", 1], ["segmenting", 2],
  ["transcribing", 3], ["uploading_result", 4], ["finalizing", 5],
]);

function validateRange(value, min, max, code) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(code);
}

function validateRequest({ jobId, workerId, leaseToken, stage, progress, completedSegments, totalSegments }) {
  if (!UUID_RE.test(jobId ?? "")) throw new Error("INVALID_JOB_ID");
  if (!WORKER_RE.test(workerId ?? "")) throw new Error("INVALID_WORKER_ID");
  if (!UUID_RE.test(leaseToken ?? "")) throw new Error("INVALID_LEASE_TOKEN");
  if (!STAGES.has(stage)) throw new Error("INVALID_JOB_STAGE");
  validateRange(progress, 0, 99, "INVALID_PROGRESS");
  validateRange(completedSegments, 0, 1000, "INVALID_COMPLETED_SEGMENTS");
  if (totalSegments == null) {
    if (completedSegments !== 0) throw new Error("TOTAL_SEGMENTS_REQUIRED");
    return;
  }
  validateRange(totalSegments, 1, 1000, "INVALID_TOTAL_SEGMENTS");
  if (completedSegments > totalSegments) throw new Error("COMPLETED_EXCEEDS_TOTAL");
}

export function createPostgresProgressManager({ pool } = {}) {
  if (!pool || typeof pool.query !== "function") throw new Error("POSTGRES_POOL_REQUIRED");

  async function updateProgress({ jobId, workerId, leaseToken, stage, progress, completedSegments, totalSegments = null }) {
    validateRequest({ jobId, workerId, leaseToken, stage, progress, completedSegments, totalSegments });
    const result = await pool.query(`
      UPDATE toolnet_srt_jobs
      SET
        stage = $4,
        progress = $5,
        completed_segments = $6,
        total_segments = COALESCE(total_segments, $7::integer),
        updated_at = NOW()
      WHERE id = $1::uuid
        AND lease_owner = $2
        AND lease_token = $3::uuid
        AND status = 'processing'
        AND lease_expires_at > NOW()
        AND progress <= $5
        AND completed_segments <= $6
        AND (total_segments IS NULL OR $7::integer IS NULL OR total_segments = $7::integer)
        AND (total_segments IS NULL OR $6 <= total_segments)
        AND ($7::integer IS NULL OR $6 <= $7::integer)
        AND (total_segments IS NOT NULL OR $7::integer IS NOT NULL OR $6 = 0)
        AND (
          CASE stage
            WHEN 'claimed' THEN -1
            WHEN 'preparing' THEN 0
            WHEN 'downloading' THEN 1
            WHEN 'segmenting' THEN 2
            WHEN 'transcribing' THEN 3
            WHEN 'uploading_result' THEN 4
            WHEN 'finalizing' THEN 5
            ELSE 99
          END
        ) <= $8::integer
      RETURNING id, status, stage, progress, completed_segments, total_segments, updated_at
    `, [jobId, workerId, leaseToken, stage, progress, completedSegments, totalSegments, STAGES.get(stage)]);
    const row = result.rows[0];
    if (!row) return null;
    return { id: row.id, status: row.status, stage: row.stage, progress: row.progress,
      completedSegments: row.completed_segments, totalSegments: row.total_segments, updatedAt: row.updated_at };
  }

  return { updateProgress };
}
