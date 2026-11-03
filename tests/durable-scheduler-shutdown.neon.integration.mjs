import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import pg from "pg";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createPostgresProgressManager } from "../server/storage/progress.mjs";
import { createDurableWorker } from "../server/storage/durable-worker.mjs";
import { createDurableScheduler } from "../server/storage/durable-scheduler.mjs";

if (!process.env.DATABASE_URL) { console.error("DATABASE_URL_MISSING"); process.exit(1); }
const schema = `toolnet_srt_shutdown_${randomBytes(6).toString("hex")}`;
const jobId = randomUUID();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 3,
  connectionTimeoutMillis: 15000, application_name: "toolnet-srt-shutdown-test" });
const scopedDb = {
  async query(sql, params = []) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`SET LOCAL search_path TO "${schema}"`);
      await client.query("SET LOCAL statement_timeout = '20000ms'");
      const result = await client.query(sql, params);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally { client.release(); }
  },
};
const store = createPostgresJobStore({ pool: scopedDb });
const leases = createPostgresLeaseManager({ pool: scopedDb });
const progress = createPostgresProgressManager({ pool: scopedDb });
let schemaCreated = false, cleanupPassed = true, testPassed = false;
let scheduler = null, restartedScheduler = null;
let transcriptionStarted = false, uploads = 0, transcriptionCalls = 0;
let releaseAsr;
const asrGate = new Promise(resolve => { releaseAsr = resolve; });

async function until(check, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error("WAIT_TIMEOUT");
}

function createWorker({ workerId }) {
  return createDurableWorker({
    leases, progress, workerId,
    storage: { async downloadFile({ localPath }) { await writeFile(localPath, "mock audio"); } },
    results: { async putResult({ jobId: resultJobId, leaseToken }) {
      uploads += 1;
      return { key: `toolnet-srt/jobs/${resultJobId}/attempts/${leaseToken}/result.srt` };
    } },
    split: async () => [{ path: "mock-segment.mp3", startMs: 0 }],
    transcribe: async () => { transcriptionCalls += 1; transcriptionStarted = true; return asrGate; },
    toSrt: () => "1\n00:00:00,000 --> 00:00:00,500\nTest\n",
  });
}
function makeScheduler() {
  return createDurableScheduler({ maxConcurrency: 1, pollIntervalMs: 100, errorBackoffMs: 100,
    workerIdPrefix: "shutdown-test", workerFactory: createWorker });
}

try {
  const baseResult = await pool.query("SELECT current_schema() AS name");
  const baseSchema = baseResult.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await pool.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
  console.log("ISOLATED NEON SCHEMA: PASS");
  await store.saveJob({ id: jobId, status: "queued", stage: "queued", progress: 0,
    completedSegments: 0, totalSegments: null, extension: ".mp3",
    sourceKey: `toolnet-srt/jobs/${jobId}/source.mp3`, resultKey: null });
  scheduler = makeScheduler();
  scheduler.start();
  await until(() => transcriptionStarted);
  console.log("WORKER ENTERED ASR: PASS");
  const processing = await store.getJob(jobId);
  assert.equal(processing.status, "processing");
  assert.equal(processing.progress, 10);
  console.log("NEON PROCESSING STATE: PASS");

  const timedOut = await scheduler.stop({ timeoutMs: 100 });
  assert.equal(timedOut.stopped, false);
  assert.equal(timedOut.state, "stopping");
  assert.equal(timedOut.activeWorkers, 1);
  console.log("SHUTDOWN TIMEOUT DETECTED: PASS");
  assert.throws(() => scheduler.start(), /SCHEDULER_ALREADY_RUNNING/);
  console.log("OVERLAPPING RESTART REJECTED: PASS");
  const pollsBefore = scheduler.getStatus().polls;
  await new Promise(resolve => setTimeout(resolve, 250));
  assert.equal(scheduler.getStatus().polls, pollsBefore);
  assert.equal(uploads, 0);
  assert.equal((await store.getJob(jobId)).status, "processing");
  console.log("NO NEW CLAIM DURING SHUTDOWN: PASS");

  releaseAsr([{ text: "Test", startMs: 0, endMs: 500 }]);
  const drained = await scheduler.stop({ timeoutMs: 10000 });
  assert.equal(drained.stopped, true);
  assert.equal(drained.activeWorkers, 0);
  assert.equal(drained.state, "stopped");
  console.log("GRACEFUL WORKER DRAIN: PASS");
  const finished = await store.getJob(jobId);
  assert.equal(finished.status, "completed");
  assert.equal(finished.progress, 100);
  const row = await scopedDb.query("SELECT attempt_count, result_key FROM toolnet_srt_jobs WHERE id = $1::uuid", [jobId]);
  assert.equal(row.rows.length, 1);
  assert.equal(Number(row.rows[0].attempt_count), 1);
  assert.ok(row.rows[0].result_key);
  assert.equal(uploads, 1);
  assert.equal(transcriptionCalls, 1);
  console.log("JOB COMPLETED AFTER SHUTDOWN: PASS");
  console.log("NO DUPLICATE ATTEMPT: PASS");

  restartedScheduler = makeScheduler();
  restartedScheduler.start();
  await until(() => restartedScheduler.getStatus().emptyPolls >= 1);
  const restartedStopped = await restartedScheduler.stop({ timeoutMs: 10000 });
  assert.equal(restartedStopped.stopped, true);
  assert.equal(uploads, 1);
  assert.equal(transcriptionCalls, 1);
  console.log("RESTART SKIPS COMPLETED JOB: PASS");
  testPassed = true;
} catch (error) {
  console.error("SCHEDULER SHUTDOWN TEST FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  releaseAsr([{ text: "Test", startMs: 0, endMs: 500 }]);
  for (const instance of [restartedScheduler, scheduler]) {
    if (!instance) continue;
    try { if (!(await instance.stop({ timeoutMs: 10000 })).stopped) cleanupPassed = false; }
    catch { cleanupPassed = false; }
  }
  if (schemaCreated && cleanupPassed) {
    try { await pool.query(`DROP SCHEMA "${schema}" CASCADE`); console.log("TEST SCHEMA CLEANED: PASS"); }
    catch { cleanupPassed = false; console.error("SCHEMA CLEANUP FAILED"); }
  }
  if (!cleanupPassed) { console.error("MANUAL CLEANUP MAY BE REQUIRED:", schema); process.exitCode = 1; }
  await pool.end().catch(() => { process.exitCode = 1; });
}
if (testPassed && cleanupPassed) console.log("TOOLNET-SRT NEON GRACEFUL SHUTDOWN PASSED");
