import pg from "pg";
import { S3Client, HeadBucketCommand } from "@aws-sdk/client-s3";
import { readDurableConfig } from "./durable-config.mjs";

const REQUIRED_COLUMNS = [
  "id", "status", "stage", "progress", "completed_segments", "total_segments",
  "source_key", "result_key", "error_code", "lease_owner", "lease_token",
  "lease_expires_at", "attempt_count", "available_at", "created_at", "updated_at",
];

function createDatabasePool(env) {
  return new pg.Pool({ connectionString: env.DATABASE_URL, max: 1, connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 1000, application_name: "toolnet-srt-preflight" });
}

function createR2Client(env) {
  if (!/^[a-f0-9]{32}$/i.test(env.R2_ACCOUNT_ID ?? "")) throw new Error("INVALID_R2_ACCOUNT_ID");
  return new S3Client({
    region: "auto",
    endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey: env.R2_SECRET_ACCESS_KEY },
    maxAttempts: 2,
  });
}

export function createDurablePreflight({ env = process.env, poolFactory = createDatabasePool, r2Factory = createR2Client } = {}) {
  async function check() {
    const config = readDurableConfig(env);
    if (!config.enabled) return { enabled: false, neon: "skipped", r2: "skipped", workerStarted: false };
    let pool;
    let r2;
    try {
      pool = poolFactory(env);
      if (typeof pool?.query !== "function" || typeof pool?.end !== "function") throw new Error("INVALID_DATABASE_POOL");
      try { await pool.query("SELECT 1 AS ok"); }
      catch { throw new Error("NEON_CONNECTION_FAILED"); }

      let table;
      try {
        const result = await pool.query(`SELECT to_regclass('public.toolnet_srt_jobs') AS table_ref`);
        table = result.rows[0]?.table_ref;
      } catch { throw new Error("NEON_SCHEMA_CHECK_FAILED"); }
      if (!table) throw new Error("NEON_JOBS_TABLE_MISSING");

      let columns;
      try {
        const result = await pool.query(`
          SELECT attname FROM pg_attribute
          WHERE attrelid = 'public.toolnet_srt_jobs'::regclass
            AND attnum > 0 AND NOT attisdropped
        `);
        columns = new Set(result.rows.map(row => row.attname));
      } catch { throw new Error("NEON_COLUMNS_CHECK_FAILED"); }
      const missing = REQUIRED_COLUMNS.filter(column => !columns.has(column));
      if (missing.length) throw new Error("NEON_MIGRATION_REQUIRED: " + missing.join(", "));

      r2 = r2Factory(env);
      if (typeof r2?.send !== "function" || typeof r2?.destroy !== "function") throw new Error("INVALID_R2_CLIENT");
      try { await r2.send(new HeadBucketCommand({ Bucket: env.R2_BUCKET })); }
      catch { throw new Error("R2_BUCKET_ACCESS_FAILED"); }
      return { enabled: true, neon: "ready", r2: "ready", schema: "verified", workerStarted: false };
    } finally {
      try { r2?.destroy(); } catch { /* Do not mask the original error. */ }
      if (pool) await pool.end().catch(() => {});
    }
  }
  return { check };
}
