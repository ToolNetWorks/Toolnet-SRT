import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import pg from "pg";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createPostgresCancellation } from "../server/storage/cancellation.mjs";

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL_MISSING");
  process.exit(1);
}
const schema = `toolnet_srt_cancel_test_${randomBytes(6).toString("hex")}`;
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 2,
  connectionTimeoutMillis: 10000, application_name: "toolnet-srt-cancel-test" });
let clientA;
let clientB;
let schemaCreated = false;
let passed = false;
let cleanupPassed = true;

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

function newJob(id) {
  return { id, status: "queued", stage: "queued", progress: 0, completedSegments: 0,
    totalSegments: null, extension: ".mp3", errorCode: null,
    sourceKey: `toolnet-srt/jobs/${id}/source.mp3`, resultKey: null };
}

try {
  clientA = await pool.connect();
  clientB = await pool.connect();
  const baseResult = await clientA.query("SELECT current_schema() AS name");
  const baseSchema = baseResult.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await clientA.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await clientA.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
  console.log("ISOLATED NEON SCHEMA: PASS");

  const store = createPostgresJobStore({ pool: clientA });
  const leaseA = createPostgresLeaseManager({ pool: clientA });
  const leaseB = createPostgresLeaseManager({ pool: clientB });
  const cancelA = createPostgresCancellation({ pool: clientA });
  const cancelB = createPostgresCancellation({ pool: clientB });
  async function insertJob(id) { return transaction(clientA, () => store.saveJob(newJob(id))); }

  const queuedId = randomUUID();
  await insertJob(queuedId);
  const cancelledQueued = await transaction(clientB, () => cancelB.cancelJob(queuedId));
  assert.equal(cancelledQueued?.status, "cancelled");
  console.log("CANCEL QUEUED JOB: PASS");
  const noClaim = await transaction(clientA, () => leaseA.claimJob("worker-a"));
  assert.equal(noClaim, null);
  console.log("CANCELLED JOB NOT CLAIMABLE: PASS");

  const processingId = randomUUID();
  await insertJob(processingId);
  const active = await transaction(clientA, () => leaseA.claimJob("worker-a"));
  assert.equal(active?.id, processingId);
  const cancelledProcessing = await transaction(clientB, () => cancelB.cancelJob(processingId));
  assert.equal(cancelledProcessing?.status, "cancelled");
  console.log("CANCEL PROCESSING JOB: PASS");
  const heartbeat = await transaction(clientA, () => leaseA.heartbeatJob({
    jobId: processingId, workerId: "worker-a", leaseToken: active.lease_token,
  }));
  assert.equal(heartbeat, null);
  console.log("CANCELLED LEASE HEARTBEAT REJECTED: PASS");
  const staleComplete = await transaction(clientA, () => leaseA.completeJob({
    jobId: processingId, workerId: "worker-a", leaseToken: active.lease_token,
    resultKey: `toolnet-srt/jobs/${processingId}/attempts/${active.lease_token}/result.srt`,
  }));
  assert.equal(staleComplete, null);
  console.log("CANCELLED JOB COMPLETION REJECTED: PASS");

  const raceId = randomUUID();
  await insertJob(raceId);
  const claimed = await transaction(clientA, () => leaseA.claimJob("worker-a"));
  assert.equal(claimed?.id, raceId);
  const resultKey = `toolnet-srt/jobs/${raceId}/attempts/${claimed.lease_token}/result.srt`;
  const [cancelResult, completeResult] = await Promise.all([
    transaction(clientA, () => cancelA.cancelJob(raceId)),
    transaction(clientB, () => leaseB.completeJob({ jobId: raceId, workerId: "worker-a", leaseToken: claimed.lease_token, resultKey })),
  ]);
  const cancelled = cancelResult !== null;
  const completed = completeResult !== null;
  assert.notEqual(cancelled, completed);
  const finalJob = await transaction(clientA, () => store.getJob(raceId));
  assert.equal(finalJob.status, cancelled ? "cancelled" : "completed");
  console.log("CONCURRENT CANCEL VS COMPLETE: PASS");
  console.log("SINGLE TERMINAL STATE: PASS");
  const secondCancel = await transaction(clientB, () => cancelB.cancelJob(raceId));
  assert.equal(secondCancel, null);
  console.log("TERMINAL STATE PROTECTED: PASS");
  passed = true;
} catch (error) {
  console.error("CANCELLATION TEST FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  if (schemaCreated && clientA) {
    try {
      await clientA.query(`DROP SCHEMA "${schema}" CASCADE`);
      console.log("TEST SCHEMA CLEANED: PASS");
    } catch {
      cleanupPassed = false;
      console.error("TEST SCHEMA CLEANUP FAILED");
      console.error("SCHEMA:", schema);
    }
  }
  clientA?.release();
  clientB?.release();
  await pool.end().catch(() => { cleanupPassed = false; });
  if (!cleanupPassed) process.exitCode = 1;
}
if (passed && cleanupPassed) console.log("TOOLNET-SRT NEON CANCELLATION TEST PASSED");
