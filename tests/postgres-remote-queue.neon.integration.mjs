import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { createRemoteUrlVault } from "../server/storage/remote-url-vault.mjs";
import { createPostgresRemoteQueue } from "../server/storage/postgres-remote-queue.mjs";

if (!process.env.DATABASE_URL) { console.error("DATABASE_URL_MISSING"); process.exit(1); }
const schema = "srt_queue_test_" + randomBytes(6).toString("hex");
const url = "https://media.example/video.mp4?token=secret123";
const vault = createRemoteUrlVault({ key: randomBytes(32).toString("base64url") });
const control = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1, connectionTimeoutMillis: 10000 });
const workers = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 2, connectionTimeoutMillis: 10000 });
let schemaCreated = false, passed = false, cleanupPassed = true;

const isolatedPool = {
  async connect() {
    const client = await workers.connect();
    return {
      async query(sql, ...params) {
        if (sql === "BEGIN") {
          await client.query("BEGIN");
          await client.query(`SET LOCAL search_path TO "${schema}"`);
          const check = await client.query("SELECT current_schema() AS name");
          if (check.rows[0]?.name !== schema) throw new Error("ISOLATED_SCHEMA_NOT_ACTIVE");
          return;
        }
        return client.query(sql, ...params);
      },
      release() { client.release(); },
    };
  },
};
async function countRows(table) {
  if (!new Set(["toolnet_srt_jobs", "toolnet_srt_remote_fetch_jobs"]).has(table)) throw new Error("INVALID_TEST_TABLE");
  const result = await control.query(`SELECT COUNT(*)::integer AS count FROM "${schema}"."${table}"`);
  return result.rows[0].count;
}

try {
  const current = await control.query("SELECT current_schema() AS name");
  const baseSchema = current.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await control.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await control.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
  const migration = await readFile(new URL("../server/storage/migrations/003_remote_fetch_queue.sql", import.meta.url), "utf8");
  const setup = await control.connect();
  try {
    await setup.query("BEGIN");
    await setup.query(`SET LOCAL search_path TO "${schema}"`);
    await setup.query(migration);
    await setup.query("COMMIT");
  } catch (error) { await setup.query("ROLLBACK").catch(() => {}); throw error; }
  finally { setup.release(); }
  console.log("ISOLATED SCHEMA READY: PASS");

  const queue = createPostgresRemoteQueue({ pool: isolatedPool, vault, allowedHosts: ["media.example"] });
  const result = await queue.enqueue(url);
  assert.equal(result.status, "queued"); assert.equal(result.stage, "fetch_pending"); assert.equal(result.progress, 0);
  assert.equal(result.resultPath, null); assert.equal(JSON.stringify(result).includes("secret123"), false);
  assert.equal(await countRows("toolnet_srt_jobs"), 1); assert.equal(await countRows("toolnet_srt_remote_fetch_jobs"), 1);
  console.log("ATOMIC TWO-TABLE COMMIT: PASS");
  const query = await control.query(`SELECT j.id, j.status AS job_status, j.stage, j.source_key,
    q.status AS fetch_status, q.attempt_count, q.encrypted_url FROM "${schema}".toolnet_srt_jobs j
    JOIN "${schema}".toolnet_srt_remote_fetch_jobs q ON q.job_id = j.id WHERE j.id = $1::uuid`, [result.id]);
  assert.equal(query.rowCount, 1);
  const row = query.rows[0];
  assert.equal(row.job_status, "queued"); assert.equal(row.stage, "fetch_pending");
  assert.equal(row.fetch_status, "pending"); assert.equal(row.attempt_count, 0); assert.equal(row.source_key, null);
  console.log("ASR SOURCE GATE: PASS");
  assert.equal(row.encrypted_url.includes("secret123"), false);
  assert.equal(vault.decrypt(row.encrypted_url, result.id), url);
  console.log("SIGNED URL ENCRYPTION: PASS");

  const badQueue = createPostgresRemoteQueue({ pool: isolatedPool, vault: { encrypt() { return ""; } }, allowedHosts: ["media.example"] });
  await assert.rejects(badQueue.enqueue(url), { code: "REMOTE_QUEUE_WRITE_FAILED", status: 503 });
  assert.equal(await countRows("toolnet_srt_jobs"), 1); assert.equal(await countRows("toolnet_srt_remote_fetch_jobs"), 1);
  console.log("ATOMIC TRANSACTION ROLLBACK: PASS");
  await assert.rejects(queue.enqueue("https://unapproved.example/file.mp4"), { code: "SOURCE_HOST_NOT_ALLOWED", status: 403 });
  assert.equal(await countRows("toolnet_srt_jobs"), 1);
  console.log("ALLOWLIST BEFORE DB WRITE: PASS");
  passed = true;
} catch (error) {
  process.exitCode = 1;
  console.error("REMOTE QUEUE E2E FAILED:", error?.code || error?.name || "UNKNOWN");
} finally {
  await workers.end().catch(() => { cleanupPassed = false; });
  if (schemaCreated) {
    try { await control.query(`DROP SCHEMA "${schema}" CASCADE`); console.log("TEST SCHEMA CLEANED: PASS"); }
    catch { cleanupPassed = false; console.error("SCHEMA CLEANUP FAILED:", schema); }
  }
  await control.end().catch(() => { cleanupPassed = false; });
  if (!cleanupPassed) process.exitCode = 1;
}
if (passed && cleanupPassed) console.log("TOOLNET-SRT ATOMIC REMOTE QUEUE PASSED");
