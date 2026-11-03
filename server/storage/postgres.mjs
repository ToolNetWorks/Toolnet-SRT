import pg from "pg";
const { Pool } = pg;
const JOB_ID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const STATUSES = new Set(["queued", "processing", "completed", "failed", "cancelled"]);
const EXTENSION_RE = /^\.(mp3|mp4|wav|m4a|flac|ogg|mov|mkv|webm)$/;

export function isPostgresConfigured(env = process.env) {
  return Boolean(env.DATABASE_URL?.trim());
}

function validateJob(job) {
  if (!job || !JOB_ID_RE.test(job.id ?? "")) throw new Error("INVALID_JOB_ID");
  if (!STATUSES.has(job.status)) throw new Error("INVALID_JOB_STATUS");
  if (!EXTENSION_RE.test(job.extension ?? "")) throw new Error("INVALID_JOB_EXTENSION");
  const progress = job.progress ?? 0;
  const completed = job.completedSegments ?? 0;
  const total = job.totalSegments ?? null;
  if (!Number.isInteger(progress) || progress < 0 || progress > 100) {
    throw new Error("INVALID_PROGRESS");
  }
  if (!Number.isSafeInteger(completed) || completed < 0) {
    throw new Error("INVALID_COMPLETED_SEGMENTS");
  }
  if (total !== null && (!Number.isSafeInteger(total) || total < completed)) {
    throw new Error("INVALID_TOTAL_SEGMENTS");
  }
  for (const key of ["sourceKey", "resultKey"]) {
    const value = job[key];
    if (value == null) continue;
    if (typeof value !== "string" || !value.startsWith(`toolnet-srt/jobs/${job.id}/`)) {
      throw new Error("INVALID_STORAGE_KEY");
    }
  }
}

function mapRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    status: row.status,
    stage: row.stage,
    progress: row.progress,
    completedSegments: row.completed_segments,
    totalSegments: row.total_segments,
    extension: row.extension,
    errorCode: row.error_code,
    sourceKey: row.source_key,
    resultKey: row.result_key,
    createdAt: new Date(row.created_at).toISOString(),
    updatedAt: new Date(row.updated_at).toISOString(),
  };
}

export function createPostgresJobStore({ env = process.env, pool: injectedPool } = {}) {
  if (!injectedPool && !isPostgresConfigured(env)) {
    throw new Error("DATABASE_URL_MISSING");
  }
  const ownsPool = !injectedPool;
  const pool = injectedPool ?? new Pool({
    connectionString: env.DATABASE_URL,
    max: 3,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000,
    application_name: "toolnet-srt",
  });

  async function initialize() {
    // Explicit initialization only. No SQL runs at import.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS toolnet_srt_jobs (
        id UUID PRIMARY KEY,
        status TEXT NOT NULL
          CHECK (status IN ('queued', 'processing', 'completed', 'failed', 'cancelled')),
        stage TEXT NOT NULL DEFAULT 'queued',
        progress SMALLINT NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
        completed_segments INTEGER NOT NULL DEFAULT 0 CHECK (completed_segments >= 0),
        total_segments INTEGER,
        extension TEXT NOT NULL,
        error_code TEXT,
        source_key TEXT,
        result_key TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_toolnet_srt_jobs_status_created
      ON toolnet_srt_jobs (status, created_at)
    `);
    return true;
  }

  async function ping() {
    const result = await pool.query("SELECT 1 AS ok");
    return result.rows[0]?.ok === 1;
  }

  async function saveJob(job) {
    validateJob(job);
    // Creation only. State changes require lease-aware methods.
    if (job.status !== "queued") throw new Error("JOB_CREATION_REQUIRES_QUEUED_STATUS");
    const values = [
      job.id, job.status, job.stage ?? "queued", job.progress ?? 0,
      job.completedSegments ?? 0, job.totalSegments ?? null, job.extension,
      job.errorCode ?? null, job.sourceKey ?? null, job.resultKey ?? null,
      job.createdAt ?? new Date().toISOString(),
    ];
    const result = await pool.query(`
      INSERT INTO toolnet_srt_jobs (
        id, status, stage, progress, completed_segments, total_segments,
        extension, error_code, source_key, result_key, created_at, updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
      ON CONFLICT (id) DO NOTHING
      RETURNING *
    `, values);
    if (!result.rows[0]) throw new Error("JOB_ALREADY_EXISTS");
    return mapRow(result.rows[0]);
  }

  async function getJob(id) {
    if (!JOB_ID_RE.test(id ?? "")) throw new Error("INVALID_JOB_ID");
    const result = await pool.query(`SELECT * FROM toolnet_srt_jobs WHERE id = $1`, [id]);
    return mapRow(result.rows[0]);
  }

  async function listUnfinishedJobs(limit = 100) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error("INVALID_LIMIT");
    }
    const result = await pool.query(`
      SELECT *
      FROM toolnet_srt_jobs
      WHERE status IN ('queued', 'processing')
      ORDER BY created_at ASC
      LIMIT $1
    `, [limit]);
    return result.rows.map(mapRow);
  }

  async function close() {
    if (ownsPool) await pool.end();
  }

  return { initialize, ping, saveJob, getJob, listUnfinishedJobs, close };
}
