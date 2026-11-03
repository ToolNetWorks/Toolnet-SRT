import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";

import { createDurableUploadIngress } from "../server/storage/durable-ingress.mjs";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createR2Storage, isR2Configured } from "../server/storage/r2.mjs";

if (!process.env.DATABASE_URL || !isR2Configured()) {
  console.error("DATABASE_OR_R2_ENV_MISSING");
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 1,
  connectionTimeoutMillis: 10000,
});
const storage = createR2Storage();
const payload = Buffer.from("ID3\nToolNet-SRT durable ingress integration test\n", "utf8");
function sha256(buffer) {
  return createHash("sha256").update(buffer).digest("hex");
}

let client;
let transactionStarted = false;
let uploadedJobId = null;
let uploadedFilename = null;
let passed = false;
let cleanupPassed = true;
const directory = await mkdtemp(join(tmpdir(), "toolnet-srt-e2e-"));

try {
  client = await pool.connect();
  const schemaResult = await client.query("SELECT current_schema() AS schema");
  const schema = schemaResult.rows[0]?.schema;
  if (!/^[a-z_][a-z0-9_]*$/i.test(schema ?? "")) throw new Error("INVALID_DATABASE_SCHEMA");

  // The transaction pins a connection even through a transaction pooler.
  await client.query("BEGIN");
  transactionStarted = true;
  // The temporary table shadows the production table and disappears on rollback.
  await client.query(`
    CREATE TEMP TABLE toolnet_srt_jobs (
      LIKE "${schema}".toolnet_srt_jobs INCLUDING ALL
    ) ON COMMIT DROP
  `);
  console.log("NEON ISOLATED TABLE: PASS");

  const jobStore = createPostgresJobStore({ pool: client });
  const guardedStorage = {
    async uploadFile(args) {
      uploadedJobId = args.jobId;
      uploadedFilename = args.filename;
      return storage.uploadFile(args);
    },
  };
  const ingress = createDurableUploadIngress({ jobStore, storage: guardedStorage, maxBytes: 1024 * 1024 });
  const request = Readable.from([payload]);
  request.headers = { "content-type": "audio/mpeg", "content-length": String(payload.length) };
  const job = await ingress.createUpload(request, "audio/mpeg");
  assert.equal(job.status, "queued");
  assert.equal(job.id, uploadedJobId);
  console.log("DURABLE INGRESS: PASS");

  const storedJob = await jobStore.getJob(job.id);
  assert.ok(storedJob);
  assert.equal(storedJob.status, "queued");
  assert.equal(storedJob.sourceKey, job.sourceKey);
  console.log("NEON JOB PERSISTENCE: PASS");

  const metadata = await storage.getFileInfo({ jobId: job.id, filename: uploadedFilename });
  assert.ok(metadata);
  assert.equal(metadata.size, payload.length);
  console.log("R2 SOURCE METADATA: PASS");

  const downloadPath = join(directory, "source.mp3");
  await storage.downloadFile({ jobId: job.id, filename: uploadedFilename, localPath: downloadPath });
  const downloaded = await readFile(downloadPath);
  assert.equal(sha256(downloaded), sha256(payload));
  console.log("R2 SOURCE DOWNLOAD: PASS");
  console.log("SOURCE SHA256: PASS");
  passed = true;
} catch (error) {
  console.error("DURABLE E2E FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  if (transactionStarted) {
    try {
      await client.query("ROLLBACK");
      console.log("NEON TRANSACTION ROLLED BACK: PASS");
    } catch {
      cleanupPassed = false;
      console.error("NEON ROLLBACK FAILED");
    }
  }
  client?.release();
  if (uploadedJobId && uploadedFilename) {
    try {
      await storage.deleteFile({ jobId: uploadedJobId, filename: uploadedFilename });
      const remaining = await storage.getFileInfo({ jobId: uploadedJobId, filename: uploadedFilename });
      assert.equal(remaining, null);
      console.log("R2 TEST OBJECT CLEANED: PASS");
    } catch {
      cleanupPassed = false;
      console.error("R2 CLEANUP FAILED");
      console.error("TEST JOB ID:", uploadedJobId);
    }
  }
  await rm(directory, { recursive: true, force: true }).catch(() => { cleanupPassed = false; });
  await pool.end();
  if (!cleanupPassed) process.exitCode = 1;
}

if (passed && cleanupPassed) console.log("TOOLNET-SRT DURABLE INGRESS E2E PASSED");
