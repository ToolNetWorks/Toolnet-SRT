import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import pg from "pg";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createPostgresProgressManager } from "../server/storage/progress.mjs";
import { createPostgresCancellation } from "../server/storage/cancellation.mjs";
import { createDurableWorker } from "../server/storage/durable-worker.mjs";
import { createDurableScheduler } from "../server/storage/durable-scheduler.mjs";

if (!process.env.DATABASE_URL) { console.error("DATABASE_URL_MISSING"); process.exit(1); }
const schema = `toolnet_srt_scheduler_${randomBytes(6).toString("hex")}`;
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 5,
  connectionTimeoutMillis: 15000, application_name: "toolnet-srt-scheduler-test" });
let schemaCreated = false, cleanupPassed = true, testPassed = false;
const schedulers = [], seenUploads = new Map(), restartedProgress = [];
let abandonedJobId = null;

// Every query is scoped inside a transaction to support transaction poolers.
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
const cancellation = createPostgresCancellation({ pool: scopedDb });

function newJob(id) {
  return { id, status: "queued", stage: "queued", progress: 0, completedSegments: 0, totalSegments: null,
    extension: ".mp3", sourceKey: `toolnet-srt/jobs/${id}/source.mp3`, resultKey: null };
}
async function until(check, timeoutMs = 45000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error("WAIT_TIMEOUT");
}
function makeScheduler(concurrency) {
  const scheduler = createDurableScheduler({ maxConcurrency: concurrency, pollIntervalMs: 500,
    errorBackoffMs: 500, workerIdPrefix: "srt-neon-test", workerFactory({ workerId }) {
      const progressManager = {
        async updateProgress(input) {
          const row = await progress.updateProgress(input);
          if (row && input.jobId === abandonedJobId && input.stage === "preparing") restartedProgress.push(row.progress);
          return row;
        },
      };
      return createDurableWorker({ leases, progress: progressManager, workerId,
        storage: { async downloadFile({ localPath }) { await writeFile(localPath, "mock audio"); } },
        results: { async putResult({ jobId, leaseToken }) {
          const key = `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt`;
          seenUploads.set(jobId, (seenUploads.get(jobId) ?? 0) + 1);
          return { key };
        } },
        split: async () => [{ path: "mock-segment.mp3", startMs: 0 }],
        transcribe: async () => [{ text: "Test", startMs: 0, endMs: 500 }],
        toSrt: () => "1\n00:00:00,000 --> 00:00:00,500\nTest\n",
      });
    },
  });
  schedulers.push(scheduler);
  return scheduler;
}

try {
  const base = await pool.query("SELECT current_schema() AS name");
  const baseSchema = base.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await pool.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
  console.log("NEON ISOLATED SCHEMA: PASS");

  const firstId = randomUUID();
  await store.saveJob(newJob(firstId));
  const firstScheduler = makeScheduler(1);
  firstScheduler.start();
  await until(() => firstScheduler.getStatus().completed === 1);
  assert.equal((await firstScheduler.stop()).stopped, true);
  const firstJob = await store.getJob(firstId);
  assert.equal(firstJob.status, "completed");
  assert.equal(firstJob.progress, 100);
  console.log("SCHEDULER + DURABLE WORKER + NEON: PASS");

  abandonedJobId = randomUUID();
  await store.saveJob(newJob(abandonedJobId));
  const oldClaim = await leases.claimJob("crashed-worker");
  assert.equal(oldClaim?.id, abandonedJobId);
  await progress.updateProgress({ jobId: abandonedJobId, workerId: "crashed-worker", leaseToken: oldClaim.lease_token,
    stage: "preparing", progress: 1, completedSegments: 0, totalSegments: null });
  const oldProgress = await progress.updateProgress({ jobId: abandonedJobId, workerId: "crashed-worker", leaseToken: oldClaim.lease_token,
    stage: "transcribing", progress: 75, completedSegments: 1, totalSegments: 2 });
  assert.equal(oldProgress?.progress, 75);
  await scopedDb.query(`UPDATE toolnet_srt_jobs SET lease_expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1::uuid`, [abandonedJobId]);
  console.log("EXPIRED PROCESSING JOB: PASS");

  const queuedId = randomUUID(), cancelledId = randomUUID();
  await store.saveJob(newJob(queuedId));
  await store.saveJob(newJob(cancelledId));
  assert.equal((await cancellation.cancelJob(cancelledId))?.status, "cancelled");
  const restartedScheduler = makeScheduler(2);
  restartedScheduler.start();
  await until(() => restartedScheduler.getStatus().completed >= 2);
  assert.equal((await restartedScheduler.stop()).stopped, true);
  const recovered = await store.getJob(abandonedJobId);
  const queued = await store.getJob(queuedId);
  const cancelled = await store.getJob(cancelledId);
  // attempt_count is not included in the store's public mapping; inspect the isolated rows directly.
  const counts = await scopedDb.query("SELECT id, attempt_count FROM toolnet_srt_jobs WHERE id = ANY($1::uuid[])", [[abandonedJobId, queuedId]]);
  const attempts = new Map(counts.rows.map(row => [row.id, row.attempt_count]));
  assert.equal(recovered.status, "completed");
  assert.equal(recovered.progress, 100);
  assert.equal(attempts.get(abandonedJobId), 2);
  assert.ok(restartedProgress.includes(1));
  assert.equal(queued.status, "completed");
  assert.equal(attempts.get(queuedId), 1);
  assert.equal(cancelled.status, "cancelled");
  assert.equal(seenUploads.get(abandonedJobId), 1);
  assert.equal(seenUploads.get(queuedId), 1);
  assert.equal(seenUploads.has(cancelledId), false);
  console.log("RESTART + EXPIRED LEASE RECLAIM: PASS");
  console.log("RECLAIMED PROGRESS RESET: PASS");
  console.log("TWO WORKERS NO DUPLICATE COMPLETION: PASS");
  console.log("CANCELLED JOB NOT PROCESSED: PASS");
  const stale = await leases.heartbeatJob({ jobId: abandonedJobId, workerId: "crashed-worker", leaseToken: oldClaim.lease_token });
  assert.equal(stale, null);
  console.log("CRASHED WORKER TOKEN FENCED: PASS");
  testPassed = true;
} catch (error) {
  // Avoid logging connection strings or credentials from error messages.
  console.error("SCHEDULER NEON TEST FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  for (const scheduler of schedulers.reverse()) {
    try { if (!(await scheduler.stop({ timeoutMs: 30000 })).stopped) cleanupPassed = false; }
    catch { cleanupPassed = false; }
  }
  if (schemaCreated && cleanupPassed) {
    try { await pool.query(`DROP SCHEMA "${schema}" CASCADE`); console.log("TEST SCHEMA CLEANED: PASS"); }
    catch { cleanupPassed = false; console.error("TEST SCHEMA CLEANUP FAILED"); }
  }
  if (!cleanupPassed) { console.error("CHECK TEST SCHEMA MANUALLY:", schema); process.exitCode = 1; }
  await pool.end().catch(() => { process.exitCode = 1; });
}
if (testPassed && cleanupPassed) console.log("TOOLNET-SRT SCHEDULER NEON E2E PASSED");
