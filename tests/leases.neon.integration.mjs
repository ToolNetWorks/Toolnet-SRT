import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import pg from "pg";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL_MISSING");
  process.exit(1);
}

const schema = `toolnet_srt_test_${randomBytes(6).toString("hex")}`;
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 3,
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 10000,
  application_name: "toolnet-srt-lease-integration-test",
});
let clientA;
let clientB;
let schemaCreated = false;
const workerA = "integration-worker-a";
const workerB = "integration-worker-b";

async function insertJob(client, id) {
  const sourceKey = `toolnet-srt/jobs/${id}/source.mp4`;
  await client.query(`
    INSERT INTO toolnet_srt_jobs (
      id, status, stage, progress, completed_segments, extension, source_key
    ) VALUES ($1, 'queued', 'queued', 0, 0, '.mp4', $2)
  `, [id, sourceKey]);
}

// Transaction-local search_path remains isolated with transaction poolers.
function isolatedClient(client) {
  return {
    async query(sql, params) {
      await client.query("BEGIN");
      try {
        await client.query(`SET LOCAL search_path TO "${schema}"`);
        const result = await client.query(sql, params);
        await client.query("COMMIT");
        return result;
      } catch (error) {
        await client.query("ROLLBACK").catch(() => {});
        throw error;
      }
    },
  };
}

try {
  const base = await pool.query("SELECT current_schema() AS name");
  const baseSchema = base.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");

  // Test schema only. Production table is not modified.
  await pool.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await pool.query(`
    CREATE TABLE "${schema}".toolnet_srt_jobs (
      LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL
    )
  `);
  console.log("ISOLATED TEST SCHEMA: PASS");

  clientA = await pool.connect();
  clientB = await pool.connect();
  const isolatedA = isolatedClient(clientA);
  const isolatedB = isolatedClient(clientB);
  const managerA = createPostgresLeaseManager({ pool: isolatedA });
  const managerB = createPostgresLeaseManager({ pool: isolatedB });
  const jobId = randomUUID();
  const resultKeyFor = token => `toolnet-srt/jobs/${jobId}/attempts/${token}/result.srt`;
  await insertJob(isolatedA, jobId);

  const [claimA, claimB] = await Promise.all([managerA.claimJob(workerA), managerB.claimJob(workerB)]);
  const claims = [claimA, claimB].filter(Boolean);
  assert.equal(claims.length, 1, "Two workers claimed the same job");
  assert.equal(claims[0].id, jobId);
  console.log("CONCURRENT CLAIM (ONLY ONE WINNER): PASS");

  const firstIsA = Boolean(claimA);
  const firstManager = firstIsA ? managerA : managerB;
  const secondManager = firstIsA ? managerB : managerA;
  const firstWorker = firstIsA ? workerA : workerB;
  const secondWorker = firstIsA ? workerB : workerA;
  const firstToken = claims[0].lease_token;
  const heartbeat = await firstManager.heartbeatJob({ jobId, workerId: firstWorker, leaseToken: firstToken });
  assert.equal(heartbeat?.id, jobId);
  console.log("OWNER HEARTBEAT: PASS");

  const rejectedHeartbeat = await secondManager.heartbeatJob({ jobId, workerId: secondWorker, leaseToken: randomUUID() });
  assert.equal(rejectedHeartbeat, null);
  console.log("INVALID HEARTBEAT REJECTED: PASS");

  const rejectedComplete = await secondManager.completeJob({ jobId, workerId: secondWorker, leaseToken: firstToken, resultKey: resultKeyFor(firstToken) });
  assert.equal(rejectedComplete, null);
  console.log("WRONG OWNER REJECTED: PASS");

  await isolatedA.query(`UPDATE toolnet_srt_jobs SET lease_expires_at = NOW() - INTERVAL '10 seconds' WHERE id = $1`, [jobId]);
  const oldHeartbeat = await firstManager.heartbeatJob({ jobId, workerId: firstWorker, leaseToken: firstToken });
  assert.equal(oldHeartbeat, null);
  console.log("EXPIRED HEARTBEAT REJECTED: PASS");

  const reclaimed = await secondManager.claimJob(secondWorker);
  assert.equal(reclaimed?.id, jobId);
  assert.notEqual(reclaimed.lease_token, firstToken);
  console.log("EXPIRED LEASE RECLAIM: PASS");

  const staleComplete = await firstManager.completeJob({ jobId, workerId: firstWorker, leaseToken: firstToken, resultKey: resultKeyFor(firstToken) });
  assert.equal(staleComplete, null);
  console.log("STALE TOKEN REJECTED: PASS");

  // This tests the database transition only. No actual SRT is uploaded to R2.
  const completed = await secondManager.completeJob({ jobId, workerId: secondWorker, leaseToken: reclaimed.lease_token, resultKey: resultKeyFor(reclaimed.lease_token) });
  assert.equal(completed?.status, "completed");
  console.log("NEW OWNER COMPLETION: PASS");

  const retryId = randomUUID();
  await insertJob(isolatedA, retryId);
  const retryClaim = await managerA.claimJob(workerA);
  assert.equal(retryClaim?.id, retryId);
  const failed = await managerA.failJob({ jobId: retryId, workerId: workerA, leaseToken: retryClaim.lease_token, errorCode: "INTEGRATION_TEST_ERROR", retryDelaySeconds: 30 });
  assert.equal(failed?.status, "queued");
  console.log("RETRY SCHEDULED: PASS");

  const prematureRetry = await managerB.claimJob(workerB);
  assert.equal(prematureRetry, null);
  console.log("RETRY DELAY ENFORCED: PASS");
  console.log("TOOLNET-SRT NEON CONCURRENCY TEST PASSED");
} catch (error) {
  console.error("INTEGRATION TEST FAILED:", error.code || error.name);
  process.exitCode = 1;
} finally {
  clientA?.release();
  clientB?.release();
  if (schemaCreated) {
    try {
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      console.log("ISOLATED TEST SCHEMA CLEANED");
    } catch (error) {
      console.error("TEST SCHEMA CLEANUP FAILED");
      console.error("SCHEMA NAME:", schema);
      process.exitCode = 1;
    }
  }
  await pool.end();
}
