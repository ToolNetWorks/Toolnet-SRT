import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import { S3Client, DeleteObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { createRemoteUrlVault } from "../server/storage/remote-url-vault.mjs";
import { createPostgresRemoteQueue } from "../server/storage/postgres-remote-queue.mjs";
import { createPostgresRemoteLeases } from "../server/storage/postgres-remote-leases.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createPostgresProgressManager } from "../server/storage/progress.mjs";
import { createR2Storage, isR2Configured } from "../server/storage/r2.mjs";
import { remoteSourceKey } from "../server/storage/source-keys.mjs";
import { createDurableWorker } from "../server/storage/durable-worker.mjs";
import { readFile as readSql } from "node:fs/promises";

if (!process.env.DATABASE_URL || !isR2Configured()) throw new Error("DATABASE_OR_R2_ENV_MISSING");
const env = process.env, schema = `srt_source_fence_${randomBytes(6).toString("hex")}`;
const control = new pg.Pool({ connectionString: env.DATABASE_URL, max: 1, connectionTimeoutMillis: 10000 });
const workers = new pg.Pool({ connectionString: env.DATABASE_URL, max: 3, connectionTimeoutMillis: 10000 });
const s3 = new S3Client({ region: "auto", endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  forcePathStyle: true, requestChecksumCalculation: "WHEN_REQUIRED",
  credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey: env.R2_SECRET_ACCESS_KEY } });
const storage = createR2Storage({ env, client: s3 });
const directory = await mkdtemp(join(tmpdir(), "source-fence-e2e-"));
const keys = new Set();
let created = false, cleanupOk = true, passed = false;
const isolatedPool = {
  async connect() {
    const raw = await workers.connect();
    return {
      async query(sql, params) {
        if (sql === "BEGIN") {
          try {
            await raw.query("BEGIN");
            await raw.query(`SET LOCAL search_path TO "${schema}"`);
            const check = await raw.query("SELECT current_schema() AS name");
            if (check.rows[0]?.name !== schema) throw new Error("ISOLATION_FAILED");
            return;
          } catch (error) { await raw.query("ROLLBACK").catch(() => {}); throw error; }
        }
        return raw.query(sql, params);
      },
      release() { raw.release(); },
    };
  },
};
const scopedDb = {
  async query(sql, params) {
    const client = await isolatedPool.connect();
    try { await client.query("BEGIN"); const result = await client.query(sql, params); await client.query("COMMIT"); return result; }
    catch (error) { await client.query("ROLLBACK").catch(() => {}); throw error; }
    finally { client.release(); }
  },
};

try {
  const base = (await control.query("SELECT current_schema() AS name")).rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(base ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await control.query(`CREATE SCHEMA "${schema}"`); created = true;
  await control.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
  const migration = await readSql(new URL("../server/storage/migrations/003_remote_fetch_queue.sql", import.meta.url), "utf8");
  await scopedDb.query(migration);
  const vault = createRemoteUrlVault({ key: randomBytes(32).toString("base64url") });
  const queue = createPostgresRemoteQueue({ pool: isolatedPool, vault, allowedHosts: ["media.example"] });
  const fetchLeases = createPostgresRemoteLeases({ pool: isolatedPool, leaseSeconds: 30 });
  const job = await queue.enqueue("https://media.example/source.mp4?token=test");
  const old = await fetchLeases.claimNext("source-old");
  assert.equal(old.jobId, job.id);
  const file = join(directory, "source.mp4");
  await writeFile(file, "old source");
  keys.add(remoteSourceKey(job.id, old.token, "source.mp4"));
  const oldObject = await storage.uploadRemoteSource({ jobId: job.id, fetchToken: old.token, filename: "source.mp4", localPath: file });
  await assert.rejects(
    storage.uploadRemoteSource({ jobId: job.id, fetchToken: old.token, filename: "source.mp4", localPath: file }),
    error => error.$metadata?.httpStatusCode === 412,
  );
  await control.query(`UPDATE "${schema}".toolnet_srt_remote_fetch_jobs SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE job_id = $1::uuid`, [job.id]);
  const current = await fetchLeases.claimNext("source-new");
  assert.notEqual(current.token, old.token);
  await writeFile(file, "new source");
  keys.add(remoteSourceKey(job.id, current.token, "source.mp4"));
  const newObject = await storage.uploadRemoteSource({ jobId: job.id, fetchToken: current.token, filename: "source.mp4", localPath: file });
  assert.equal(await fetchLeases.completeFetch({ ...old, sourceKey: oldObject.key }), false);
  await assert.rejects(fetchLeases.completeFetch({ ...current, sourceKey: oldObject.key }), /INVALID_SOURCE_KEY/);
  await assert.rejects(fetchLeases.completeFetch({ ...current, sourceKey: `x/${job.id}/${current.token}/source.mp4` }), /INVALID_SOURCE_KEY/);
  assert.equal(await fetchLeases.completeFetch({ ...current, sourceKey: newObject.key }), true);
  console.log("STALE FETCH FENCED AND WINNING SOURCE PUBLISHED: PASS");

  let actualSource;
  const worker = createDurableWorker({ leases: createPostgresLeaseManager({ pool: scopedDb }),
    progress: createPostgresProgressManager({ pool: scopedDb }), storage,
    results: { async putResult({ jobId, leaseToken }) { return { key: `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt` }; } },
    workerId: "source-e2e-worker", split: async input => { actualSource = await readFile(input, "utf8"); return [{ path: "mock", startMs: 0 }]; },
    transcribe: async () => [{ text: "test", startMs: 0, endMs: 100 }], toSrt: () => "subtitle",
  });
  assert.equal((await worker.runOnce()).status, "completed");
  assert.equal(actualSource, "new source");
  console.log("ASR WORKER READS WINNING R2 SOURCE: PASS");
  passed = true;
} catch (error) {
  console.error("SOURCE FENCING E2E FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  await workers.end().catch(() => { cleanupOk = false; });
  if (created) {
    try { await control.query(`DROP SCHEMA "${schema}" CASCADE`); }
    catch { cleanupOk = false; console.error("SCHEMA CLEANUP FAILED:", schema); }
  }
  await control.end().catch(() => { cleanupOk = false; });
  for (const key of keys) {
    try {
      await s3.send(new DeleteObjectCommand({ Bucket: env.R2_BUCKET, Key: key }));
      try { await s3.send(new HeadObjectCommand({ Bucket: env.R2_BUCKET, Key: key })); throw new Error("OBJECT_STILL_EXISTS"); }
      catch (error) { if (error.name !== "NotFound" && error.$metadata?.httpStatusCode !== 404) throw error; }
    } catch { cleanupOk = false; console.error("R2 CLEANUP FAILED:", key); }
  }
  s3.destroy();
  await rm(directory, { recursive: true, force: true }).catch(() => { cleanupOk = false; });
  if (!cleanupOk) process.exitCode = 1;
  else console.log("TEST RESOURCES CLEANED: PASS");
}
if (passed && cleanupOk) console.log("REMOTE SOURCE FENCING E2E PASSED");
