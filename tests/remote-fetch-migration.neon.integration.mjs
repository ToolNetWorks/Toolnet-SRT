import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createRemoteUrlVault } from "../server/storage/remote-url-vault.mjs";

if (!process.env.DATABASE_URL) { console.error("DATABASE_URL_MISSING"); process.exit(1); }
const schema = `toolnet_srt_fetch_test_${randomBytes(6).toString("hex")}`;
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1,
  connectionTimeoutMillis: 10000, application_name: "toolnet-srt-fetch-migration-test" });
const jobId = randomUUID();
const signedUrl = "https://media.example/video.mp4?token=private123";
const vault = createRemoteUrlVault({ key: randomBytes(32).toString("base64url") });
let client, schemaCreated = false, passed = false, cleanupPassed = true;

async function transaction(operation) {
  await client.query("BEGIN");
  try {
    await client.query(`SET LOCAL search_path TO "${schema}"`);
    await client.query("SET LOCAL statement_timeout = '15000ms'");
    const result = await operation();
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

try {
  client = await pool.connect();
  const base = await client.query("SELECT current_schema() AS name");
  const baseSchema = base.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await client.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await client.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
  console.log("ISOLATED NEON SCHEMA: PASS");
  const sql = await readFile(new URL("../server/storage/migrations/003_remote_fetch_queue.sql", import.meta.url), "utf8");
  await transaction(() => client.query(sql));
  await transaction(() => client.query(sql));
  console.log("MIGRATION 003 IDEMPOTENT: PASS");
  const structure = await transaction(() => client.query(`SELECT column_name FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = 'toolnet_srt_remote_fetch_jobs'`, [schema]));
  const columns = new Set(structure.rows.map(row => row.column_name));
  for (const name of ["job_id", "encrypted_url", "status", "attempt_count", "available_at", "lease_owner",
    "lease_token", "lease_expires_at", "last_error_code", "created_at", "updated_at", "finished_at"]) assert.ok(columns.has(name), name);
  console.log("REMOTE FETCH COLUMNS: PASS");
  const indexes = await transaction(() => client.query(`SELECT indexname FROM pg_indexes WHERE schemaname = $1
    AND tablename = 'toolnet_srt_remote_fetch_jobs'`, [schema]));
  const names = new Set(indexes.rows.map(row => row.indexname));
  assert.ok(names.has("toolnet_srt_remote_fetch_pending_idx"));
  assert.ok(names.has("toolnet_srt_remote_fetch_expired_idx"));
  console.log("REMOTE FETCH INDEXES: PASS");
  const store = createPostgresJobStore({ pool: client });
  const leases = createPostgresLeaseManager({ pool: client });
  await transaction(() => store.saveJob({ id: jobId, status: "queued", stage: "queued", progress: 0,
    completedSegments: 0, totalSegments: null, extension: ".mp4", sourceKey: null, resultKey: null }));
  assert.equal(await transaction(() => leases.claimJob("fetch-migration-test")), null);
  console.log("ASR CLAIM BEFORE SOURCE: BLOCKED");
  const ciphertext = vault.encrypt(signedUrl, jobId);
  await transaction(() => client.query("INSERT INTO toolnet_srt_remote_fetch_jobs (job_id, encrypted_url) VALUES ($1::uuid, $2)", [jobId, ciphertext]));
  const inserted = await transaction(() => client.query("SELECT * FROM toolnet_srt_remote_fetch_jobs WHERE job_id = $1::uuid", [jobId]));
  const row = inserted.rows[0];
  assert.equal(row.status, "pending"); assert.equal(row.attempt_count, 0); assert.ok(row.encrypted_url);
  assert.equal(row.encrypted_url.includes("private123"), false); assert.equal(vault.decrypt(row.encrypted_url, jobId), signedUrl);
  console.log("ENCRYPTED URL STORED: PASS");
  await assert.rejects(transaction(() => client.query("INSERT INTO toolnet_srt_remote_fetch_jobs (job_id, encrypted_url) VALUES ($1::uuid, $2)", [randomUUID(), ciphertext])), { code: "23503" });
  console.log("FOREIGN KEY PROTECTION: PASS");
  await assert.rejects(transaction(() => client.query("UPDATE toolnet_srt_remote_fetch_jobs SET status = 'fetching' WHERE job_id = $1::uuid", [jobId])), { code: "23514" });
  console.log("LEASE CONSTRAINT: PASS");
  const leaseToken = randomUUID();
  await transaction(() => client.query(`UPDATE toolnet_srt_remote_fetch_jobs SET status = 'fetching',
    attempt_count = attempt_count + 1, lease_owner = 'fetch-test-worker', lease_token = $2::uuid,
    lease_expires_at = NOW() + INTERVAL '30 seconds' WHERE job_id = $1::uuid`, [jobId, leaseToken]));
  const fetching = await transaction(() => client.query("SELECT status, attempt_count, lease_token FROM toolnet_srt_remote_fetch_jobs WHERE job_id = $1::uuid", [jobId]));
  assert.equal(fetching.rows[0].status, "fetching"); assert.equal(fetching.rows[0].attempt_count, 1);
  assert.equal(fetching.rows[0].lease_token, leaseToken);
  console.log("FETCH LEASE STATE: PASS");
  await transaction(() => client.query(`UPDATE toolnet_srt_remote_fetch_jobs SET status = 'ready', encrypted_url = NULL,
    lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, finished_at = NOW(), updated_at = NOW()
    WHERE job_id = $1::uuid`, [jobId]));
  const ready = await transaction(() => client.query("SELECT status, encrypted_url, lease_token FROM toolnet_srt_remote_fetch_jobs WHERE job_id = $1::uuid", [jobId]));
  assert.equal(ready.rows[0].status, "ready"); assert.equal(ready.rows[0].encrypted_url, null); assert.equal(ready.rows[0].lease_token, null);
  console.log("URL ERASED AFTER READY: PASS");
  await transaction(() => client.query("DELETE FROM toolnet_srt_jobs WHERE id = $1::uuid", [jobId]));
  const orphan = await transaction(() => client.query("SELECT COUNT(*)::integer AS count FROM toolnet_srt_remote_fetch_jobs WHERE job_id = $1::uuid", [jobId]));
  assert.equal(orphan.rows[0].count, 0);
  console.log("CASCADE CLEANUP: PASS");
  passed = true;
} catch (error) {
  process.exitCode = 1;
  console.error("REMOTE FETCH MIGRATION TEST FAILED:", error.code || error.name || "UNKNOWN_ERROR");
} finally {
  if (schemaCreated && client) {
    try { await client.query(`DROP SCHEMA "${schema}" CASCADE`); console.log("TEST SCHEMA CLEANED: PASS"); }
    catch { cleanupPassed = false; console.error("TEST SCHEMA CLEANUP FAILED:", schema); }
  }
  client?.release();
  await pool.end().catch(() => { cleanupPassed = false; });
  if (!cleanupPassed) process.exitCode = 1;
}
if (passed && cleanupPassed) console.log("TOOLNET-SRT REMOTE FETCH MIGRATION PASSED");
