import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import { S3Client, DeleteObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createR2Storage, isR2Configured } from "../server/storage/r2.mjs";
import { createAttemptResultStorage, attemptResultKey } from "../server/storage/attempt-results.mjs";
import { createDurableWorker } from "../server/storage/durable-worker.mjs";

if (!process.env.DATABASE_URL || !isR2Configured()) {
  console.error("DATABASE_OR_R2_ENV_MISSING");
  process.exit(1);
}

const env = process.env;
const jobId = randomUUID();
const filename = "source.wav";
const sourceKey = `toolnet-srt/jobs/${jobId}/${filename}`;
const directory = await mkdtemp(join(tmpdir(), "toolnet-worker-e2e-"));
const audioPath = join(directory, filename);
const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  forcePathStyle: true,
  maxAttempts: 3,
  requestChecksumCalculation: "WHEN_REQUIRED",
  credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey: env.R2_SECRET_ACCESS_KEY },
});
const storage = createR2Storage({ env, client: s3 });
const results = createAttemptResultStorage({ env, client: s3 });
const client = new pg.Client({ connectionString: env.DATABASE_URL, connectionTimeoutMillis: 10000 });
let connected = false;
let transactionStarted = false;
let resultKey = null;
let completed = false;
let cleanupFailed = false;
let asrCalls = 0;

try {
  const ffmpeg = spawnSync("ffmpeg", [
    "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi",
    "-i", "sine=frequency=440:duration=2", "-ac", "1", "-ar", "16000",
    "-c:a", "pcm_s16le", audioPath,
  ], { timeout: 15000, stdio: "pipe" });
  if (ffmpeg.error || ffmpeg.status !== 0) throw new Error("FFMPEG_TEST_AUDIO_FAILED");
  assert.ok((await stat(audioPath)).size > 0);
  console.log("TEST AUDIO GENERATED: PASS");

  const uploaded = await storage.uploadFile({ jobId, filename, localPath: audioPath, contentType: "audio/wav" });
  assert.equal(uploaded.key, sourceKey);
  console.log("R2 SOURCE UPLOAD: PASS");
  await client.connect();
  connected = true;
  const schemaResult = await client.query("SELECT current_schema() AS name");
  const schema = schemaResult.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(schema ?? "")) throw new Error("INVALID_SCHEMA");

  // Pin a single transaction and shadow the production table with a temp table.
  await client.query("BEGIN");
  transactionStarted = true;
  await client.query(`CREATE TEMP TABLE toolnet_srt_jobs (LIKE "${schema}".toolnet_srt_jobs INCLUDING ALL) ON COMMIT DROP`);
  console.log("NEON TEMP TABLE: PASS");
  const store = createPostgresJobStore({ pool: client });
  const leases = createPostgresLeaseManager({ pool: client });
  await store.saveJob({ id: jobId, status: "queued", stage: "queued", progress: 0, completedSegments: 0,
    totalSegments: null, extension: ".wav", errorCode: null, sourceKey, resultKey: null });
  console.log("NEON QUEUED JOB: PASS");

  // Record the result key before upload so cleanup also handles ambiguous failures.
  const trackedResults = {
    async putResult(args) {
      resultKey = attemptResultKey(args.jobId, args.leaseToken);
      return results.putResult(args);
    },
  };
  const worker = createDurableWorker({
    leases, storage, results: trackedResults, workerId: "toolnet-e2e-worker", leaseSeconds: 120, heartbeatIntervalMs: 10000,
    // Only ASR is mocked; FFmpeg splitting and SRT conversion are real.
    transcribe: async (segmentPath, signal) => {
      if (signal.aborted) throw new Error("ASR_ABORTED");
      assert.ok((await stat(segmentPath)).size > 0);
      asrCalls++;
      return [{ text: "ToolNet SRT integration test", startMs: 200, endMs: 900 }];
    },
  });
  const outcome = await worker.runOnce();
  assert.equal(outcome.status, "completed");
  assert.equal(outcome.jobId, jobId);
  assert.equal(outcome.resultKey, resultKey);
  assert.ok(asrCalls > 0);
  console.log("REAL FFMPEG SEGMENTATION: PASS");
  console.log("MOCK ASR PIPELINE: PASS");
  const job = await store.getJob(jobId);
  assert.equal(job.status, "completed");
  assert.equal(job.resultKey, resultKey);
  assert.equal(job.progress, 100);
  console.log("NEON JOB COMPLETED: PASS");
  const srt = await results.getResult({ key: resultKey });
  const text = srt.toString("utf8");
  assert.match(text, /00:00:00,200/);
  assert.match(text, /ToolNet SRT integration test/);
  console.log("R2 SRT DOWNLOAD: PASS");
  console.log("SRT TIMESTAMPS: PASS");
  completed = true;
} catch (error) {
  console.error("WORKER E2E FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  if (transactionStarted) {
    try {
      await client.query("ROLLBACK");
      console.log("NEON ROLLBACK: PASS");
    } catch {
      console.error("NEON ROLLBACK: FAILED");
      cleanupFailed = true;
    }
  }
  if (connected) await client.end().catch(() => { cleanupFailed = true; });
  const keys = [sourceKey];
  if (resultKey) keys.push(resultKey);
  for (const key of keys) {
    try {
      await s3.send(new DeleteObjectCommand({ Bucket: env.R2_BUCKET, Key: key }));
      try {
        await s3.send(new HeadObjectCommand({ Bucket: env.R2_BUCKET, Key: key }));
        throw new Error("OBJECT_STILL_EXISTS");
      } catch (error) {
        if (error.name !== "NotFound" && error.$metadata?.httpStatusCode !== 404) throw error;
      }
    } catch {
      cleanupFailed = true;
      console.error("R2 CLEANUP FAILED:", key);
    }
  }
  await rm(directory, { recursive: true, force: true }).catch(() => { cleanupFailed = true; });
  s3.destroy();
  if (cleanupFailed) {
    console.error("TEST CLEANUP INCOMPLETE");
    console.error("TEST JOB ID:", jobId);
    process.exitCode = 1;
  } else {
    console.log("TEST RESOURCES CLEANED: PASS");
  }
}
if (completed && !cleanupFailed) console.log("TOOLNET-SRT DURABLE WORKER E2E PASSED");
