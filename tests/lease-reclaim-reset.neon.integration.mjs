import assert from "node:assert/strict";
import { randomUUID, randomBytes } from "node:crypto";
import pg from "pg";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createPostgresProgressManager } from "../server/storage/progress.mjs";

if (!process.env.DATABASE_URL) { console.error("DATABASE_URL_MISSING"); process.exit(1); }
const schema = `toolnet_srt_reclaim_${randomBytes(6).toString("hex")}`;
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 2,
  connectionTimeoutMillis: 10000, application_name: "toolnet-srt-reclaim-test" });
const workerA = "reclaim-worker-a", workerB = "reclaim-worker-b", jobId = randomUUID();
let clientA, clientB, schemaCreated = false, passed = false, cleanupPassed = true;

async function transaction(client, operation) {
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
  clientA = await pool.connect();
  clientB = await pool.connect();
  const base = await clientA.query("SELECT current_schema() AS name");
  const baseSchema = base.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await clientA.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await clientA.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
  console.log("ISOLATED TEST SCHEMA: PASS");
  const storeA = createPostgresJobStore({ pool: clientA });
  const storeB = createPostgresJobStore({ pool: clientB });
  const leasesA = createPostgresLeaseManager({ pool: clientA });
  const leasesB = createPostgresLeaseManager({ pool: clientB });
  const progressA = createPostgresProgressManager({ pool: clientA });
  const progressB = createPostgresProgressManager({ pool: clientB });

  await transaction(clientA, () => storeA.saveJob({ id: jobId, status: "queued", stage: "queued",
    progress: 0, completedSegments: 0, totalSegments: null, extension: ".mp3",
    sourceKey: `toolnet-srt/jobs/${jobId}/source.mp3`, resultKey: null }));
  console.log("TEST JOB CREATED: PASS");
  const first = await transaction(clientA, () => leasesA.claimJob(workerA));
  assert.equal(first?.id, jobId);
  assert.equal(first.attempt_count, 1);
  console.log("FIRST CLAIM: PASS");
  await transaction(clientA, () => progressA.updateProgress({ jobId, workerId: workerA,
    leaseToken: first.lease_token, stage: "segmenting", progress: 5, completedSegments: 0, totalSegments: 4 }));
  const beforeExpiry = await transaction(clientA, () => progressA.updateProgress({ jobId, workerId: workerA,
    leaseToken: first.lease_token, stage: "transcribing", progress: 75, completedSegments: 3, totalSegments: 4 }));
  assert.equal(beforeExpiry?.progress, 75);
  assert.equal(beforeExpiry?.completedSegments, 3);
  console.log("OLD ATTEMPT PROGRESS 75%: PASS");

  const oldResultKey = `toolnet-srt/jobs/${jobId}/attempts/${first.lease_token}/result.srt`;
  await transaction(clientA, () => clientA.query(`
    UPDATE toolnet_srt_jobs SET lease_expires_at = NOW() - INTERVAL '5 seconds',
      error_code = 'OLD_ATTEMPT_ERROR', result_key = $2 WHERE id = $1::uuid
  `, [jobId, oldResultKey]));
  console.log("EXPIRED LEASE SIMULATED: PASS");
  const second = await transaction(clientB, () => leasesB.claimJob(workerB));
  assert.equal(second?.id, jobId);
  assert.equal(second.attempt_count, 2);
  assert.notEqual(second.lease_token, first.lease_token);
  console.log("SECOND WORKER RECLAIM: PASS");

  const reset = await transaction(clientB, () => storeB.getJob(jobId));
  assert.equal(reset.status, "processing");
  assert.equal(reset.stage, "claimed");
  assert.equal(reset.progress, 0);
  assert.equal(reset.completedSegments, 0);
  assert.equal(reset.totalSegments, null);
  assert.equal(reset.errorCode, null);
  assert.equal(reset.resultKey, null);
  console.log("PROGRESS RESET TO ZERO: PASS");
  console.log("SEGMENT COUNTERS RESET: PASS");
  console.log("OLD ERROR AND RESULT CLEARED: PASS");

  const oldHeartbeat = await transaction(clientA, () => leasesA.heartbeatJob({ jobId, workerId: workerA, leaseToken: first.lease_token }));
  assert.equal(oldHeartbeat, null);
  const oldProgress = await transaction(clientA, () => progressA.updateProgress({ jobId, workerId: workerA,
    leaseToken: first.lease_token, stage: "transcribing", progress: 90, completedSegments: 4, totalSegments: 4 }));
  assert.equal(oldProgress, null);
  const oldComplete = await transaction(clientA, () => leasesA.completeJob({ jobId, workerId: workerA,
    leaseToken: first.lease_token, resultKey: oldResultKey }));
  assert.equal(oldComplete, null);
  console.log("OLD WORKER FENCED: PASS");

  const freshProgress = await transaction(clientB, () => progressB.updateProgress({ jobId, workerId: workerB,
    leaseToken: second.lease_token, stage: "preparing", progress: 1, completedSegments: 0, totalSegments: null }));
  assert.equal(freshProgress?.progress, 1);
  console.log("NEW WORKER PROGRESS: PASS");
  // Database transition only; no R2 object is written.
  const newResultKey = `toolnet-srt/jobs/${jobId}/attempts/${second.lease_token}/result.srt`;
  const completed = await transaction(clientB, () => leasesB.completeJob({ jobId, workerId: workerB,
    leaseToken: second.lease_token, resultKey: newResultKey }));
  assert.equal(completed?.status, "completed");
  const finalJob = await transaction(clientB, () => storeB.getJob(jobId));
  assert.equal(finalJob.status, "completed");
  assert.equal(finalJob.progress, 100);
  assert.equal(finalJob.resultKey, newResultKey);
  console.log("NEW WORKER COMPLETION: PASS");
  passed = true;
} catch (error) {
  console.error("RECLAIM RESET TEST FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  if (schemaCreated && clientA) {
    try { await clientA.query(`DROP SCHEMA "${schema}" CASCADE`); console.log("TEST SCHEMA CLEANED: PASS"); }
    catch { cleanupPassed = false; console.error("SCHEMA CLEANUP FAILED:", schema); }
  }
  clientA?.release();
  clientB?.release();
  await pool.end().catch(() => { cleanupPassed = false; });
  if (!cleanupPassed) process.exitCode = 1;
}
if (passed && cleanupPassed) console.log("TOOLNET-SRT NEON RECLAIM RESET TEST PASSED");
