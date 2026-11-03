import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import pg from "pg";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createPostgresProgressManager } from "../server/storage/progress.mjs";
import { createPostgresCancellation } from "../server/storage/cancellation.mjs";

if (!process.env.DATABASE_URL) { console.error("DATABASE_URL_MISSING"); process.exit(1); }
const schema = `toolnet_srt_progress_test_${randomBytes(6).toString("hex")}`;
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 2,
  connectionTimeoutMillis: 10000, application_name: "toolnet-srt-progress-test" });
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

function newJob(id) {
  return { id, status: "queued", stage: "queued", progress: 0, completedSegments: 0,
    totalSegments: null, extension: ".mp3", sourceKey: `toolnet-srt/jobs/${id}/source.mp3`, resultKey: null };
}
function progressInput(jobId, token, overrides = {}) {
  return { jobId, workerId: "worker-a", leaseToken: token, stage: "preparing", progress: 1,
    completedSegments: 0, totalSegments: null, ...overrides };
}

try {
  clientA = await pool.connect();
  clientB = await pool.connect();
  const current = await clientA.query("SELECT current_schema() AS name");
  const baseSchema = current.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await clientA.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await clientA.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
  console.log("ISOLATED NEON SCHEMA: PASS");

  const store = createPostgresJobStore({ pool: clientA });
  const leaseA = createPostgresLeaseManager({ pool: clientA });
  const progressA = createPostgresProgressManager({ pool: clientA });
  const progressB = createPostgresProgressManager({ pool: clientB });
  const cancelB = createPostgresCancellation({ pool: clientB });
  const jobId = randomUUID();
  await transaction(clientA, () => store.saveJob(newJob(jobId)));
  const claim = await transaction(clientA, () => leaseA.claimJob("worker-a"));
  assert.equal(claim?.id, jobId);
  console.log("JOB CLAIM: PASS");
  const preparing = await transaction(clientA, () => progressA.updateProgress(progressInput(jobId, claim.lease_token)));
  assert.equal(preparing?.progress, 1);
  const segmenting = await transaction(clientA, () => progressA.updateProgress(progressInput(jobId, claim.lease_token, {
    stage: "segmenting", progress: 5, totalSegments: 3,
  })));
  assert.equal(segmenting?.totalSegments, 3);
  const transcribing = await transaction(clientA, () => progressA.updateProgress(progressInput(jobId, claim.lease_token, {
    stage: "transcribing", progress: 60, completedSegments: 2, totalSegments: 3,
  })));
  assert.equal(transcribing?.completedSegments, 2);
  console.log("PROGRESS + SEGMENTS: PASS");

  const wrongOwner = await transaction(clientB, () => progressB.updateProgress(progressInput(jobId, claim.lease_token, {
    workerId: "worker-b", stage: "transcribing", progress: 65, completedSegments: 2, totalSegments: 3,
  })));
  assert.equal(wrongOwner, null);
  const wrongToken = await transaction(clientB, () => progressB.updateProgress(progressInput(jobId, randomUUID(), {
    stage: "transcribing", progress: 65, completedSegments: 2, totalSegments: 3,
  })));
  assert.equal(wrongToken, null);
  console.log("WRONG OWNER/TOKEN: REJECTED");

  const invalidUpdates = [
    { stage: "transcribing", progress: 50, completedSegments: 2, totalSegments: 3 },
    { stage: "transcribing", progress: 65, completedSegments: 1, totalSegments: 3 },
    { stage: "segmenting", progress: 65, completedSegments: 2, totalSegments: 3 },
    { stage: "transcribing", progress: 65, completedSegments: 2, totalSegments: 4 },
  ];
  for (const update of invalidUpdates) {
    const result = await transaction(clientB, () => progressB.updateProgress(progressInput(jobId, claim.lease_token, update)));
    assert.equal(result, null);
  }
  console.log("REGRESSIVE UPDATES: REJECTED");

  await Promise.all([
    transaction(clientA, () => progressA.updateProgress(progressInput(jobId, claim.lease_token, {
      stage: "transcribing", progress: 70, completedSegments: 2, totalSegments: 3,
    }))),
    transaction(clientB, () => progressB.updateProgress(progressInput(jobId, claim.lease_token, {
      stage: "transcribing", progress: 80, completedSegments: 2, totalSegments: 3,
    }))),
  ]);
  const afterRace = await transaction(clientA, () => store.getJob(jobId));
  assert.equal(afterRace?.progress, 80);
  console.log("CONCURRENT PROGRESS: PASS");

  const cancelled = await transaction(clientB, () => cancelB.cancelJob(jobId));
  assert.equal(cancelled?.status, "cancelled");
  const afterCancel = await transaction(clientA, () => progressA.updateProgress(progressInput(jobId, claim.lease_token, {
    stage: "finalizing", progress: 99, completedSegments: 3, totalSegments: 3,
  })));
  assert.equal(afterCancel, null);
  console.log("CANCELLED JOB PROGRESS: REJECTED");

  const completedId = randomUUID();
  await transaction(clientA, () => store.saveJob(newJob(completedId)));
  const completedClaim = await transaction(clientA, () => leaseA.claimJob("worker-a"));
  assert.equal(completedClaim?.id, completedId);
  // This only tests the database transition; no R2 object is created.
  const resultKey = `toolnet-srt/jobs/${completedId}/attempts/${completedClaim.lease_token}/result.srt`;
  const completed = await transaction(clientA, () => leaseA.completeJob({
    jobId: completedId, workerId: "worker-a", leaseToken: completedClaim.lease_token, resultKey,
  }));
  assert.equal(completed?.status, "completed");
  const afterComplete = await transaction(clientB, () => progressB.updateProgress(progressInput(completedId, completedClaim.lease_token)));
  assert.equal(afterComplete, null);
  console.log("COMPLETED JOB PROGRESS: REJECTED");
  passed = true;
} catch (error) {
  console.error("PROGRESS TEST FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  if (schemaCreated && clientA) {
    try {
      await clientA.query(`DROP SCHEMA "${schema}" CASCADE`);
      console.log("TEST SCHEMA CLEANED: PASS");
    } catch {
      cleanupPassed = false;
      console.error("TEST SCHEMA CLEANUP FAILED:", schema);
    }
  }
  clientA?.release();
  clientB?.release();
  await pool.end().catch(() => { cleanupPassed = false; });
  if (!cleanupPassed) process.exitCode = 1;
}
if (passed && cleanupPassed) console.log("TOOLNET-SRT NEON PROGRESS TEST PASSED");
