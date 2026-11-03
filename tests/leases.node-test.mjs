import test from "node:test";
import assert from "node:assert/strict";

import { createPostgresLeaseManager } from "../server/storage/leases.mjs";

const JOB_ID = "123e4567-e89b-42d3-a456-426614174000";
const WORKER_A = "worker-a";
const WORKER_B = "worker-b";
const RESULT_KEY = `toolnet-srt/jobs/${JOB_ID}/attempts/${JOB_ID}/result.srt`;

function createMockPool(handler) {
  const calls = [];
  return {
    calls,
    async query(sql, params = []) {
      calls.push({ sql, params });
      return handler(sql, params);
    },
  };
}

test("Claim uses atomic PostgreSQL locking", async () => {
  const pool = createMockPool((sql, params) => {
    assert.match(sql, /FOR UPDATE SKIP LOCKED/i);
    assert.match(sql, /lease_expires_at < NOW\(\)/);
    assert.match(sql, /source_key IS NOT NULL/);
    assert.match(sql, /attempt_count < \$3/);
    assert.equal(params[0], WORKER_A);
    assert.equal(params[2], 3);
    assert.equal(params[3], 120);
    return { rows: [{ id: JOB_ID, lease_token: params[1], status: "processing", attempt_count: 1 }] };
  });
  const manager = createPostgresLeaseManager({ pool });
  const job = await manager.claimJob(WORKER_A);
  assert.equal(job.id, JOB_ID);
  assert.equal(job.status, "processing");
  assert.match(job.lease_token, /^[0-9a-f-]{36}$/i);
  console.log("ATOMIC CLAIM: PASS");
});

test("No available job returns null", async () => {
  const pool = createMockPool(() => ({ rows: [] }));
  const manager = createPostgresLeaseManager({ pool });
  assert.equal(await manager.claimJob(WORKER_B), null);
  console.log("EMPTY CLAIM: PASS");
});

test("Heartbeat checks worker, token and expiry", async () => {
  const pool = createMockPool((sql, params) => {
    assert.match(sql, /lease_owner = \$2/);
    assert.match(sql, /lease_token = \$3::uuid/);
    assert.match(sql, /lease_expires_at > NOW\(\)/);
    assert.equal(params[0], JOB_ID);
    assert.equal(params[1], WORKER_A);
    return { rows: [{ id: JOB_ID }] };
  });
  const manager = createPostgresLeaseManager({ pool });
  const result = await manager.heartbeatJob({ jobId: JOB_ID, workerId: WORKER_A, leaseToken: JOB_ID });
  assert.equal(result.id, JOB_ID);
  console.log("HEARTBEAT GUARDS: PASS");
});

test("Expired or stolen lease cannot complete", async () => {
  const pool = createMockPool((sql) => {
    assert.match(sql, /lease_owner = \$2/);
    assert.match(sql, /lease_token = \$3::uuid/);
    assert.match(sql, /lease_expires_at > NOW\(\)/);
    return { rows: [] };
  });
  const manager = createPostgresLeaseManager({ pool });
  const result = await manager.completeJob({ jobId: JOB_ID, workerId: WORKER_B, leaseToken: JOB_ID, resultKey: RESULT_KEY });
  assert.equal(result, null);
  console.log("STALE LEASE GUARD: PASS");
});

test("Complete only accepts the job's SRT key", async () => {
  const pool = createMockPool(() => ({ rows: [] }));
  const manager = createPostgresLeaseManager({ pool });
  await assert.rejects(manager.completeJob({ jobId: JOB_ID, workerId: WORKER_A, leaseToken: JOB_ID, resultKey: "another-job/result.srt" }), /INVALID_RESULT_KEY/);
  assert.equal(pool.calls.length, 0);
  console.log("RESULT KEY VALIDATION: PASS");
});

test("Complete rejects another lease's result key", async () => {
  const pool = createMockPool(() => ({ rows: [] }));
  const manager = createPostgresLeaseManager({ pool });
  const otherToken = "123e4567-e89b-42d3-a456-426614174001";
  await assert.rejects(manager.completeJob({
    jobId: JOB_ID,
    workerId: WORKER_A,
    leaseToken: JOB_ID,
    resultKey: `toolnet-srt/jobs/${JOB_ID}/attempts/${otherToken}/result.srt`,
  }), /INVALID_RESULT_KEY/);
  assert.equal(pool.calls.length, 0);
});

test("Retry uses attempt limit and delay", async () => {
  const pool = createMockPool((sql, params) => {
    assert.match(sql, /attempt_count < \$5/);
    assert.match(sql, /lease_token = \$3::uuid/);
    assert.equal(params[3], "ASR_TIMEOUT");
    assert.equal(params[4], 3);
    assert.equal(params[5], 30);
    return { rows: [{ id: JOB_ID, status: "queued", attempt_count: 1 }] };
  });
  const manager = createPostgresLeaseManager({ pool });
  const result = await manager.failJob({ jobId: JOB_ID, workerId: WORKER_A, leaseToken: JOB_ID, errorCode: "ASR_TIMEOUT" });
  assert.equal(result.status, "queued");
  console.log("RETRY POLICY: PASS");
});

test("Exhausted leases are marked failed", async () => {
  const pool = createMockPool((sql, params) => {
    assert.match(sql, /LEASE_RETRY_EXHAUSTED/);
    assert.match(sql, /lease_expires_at < NOW\(\)/);
    assert.equal(params[0], 3);
    return { rows: [{ id: JOB_ID }] };
  });
  const manager = createPostgresLeaseManager({ pool });
  assert.deepEqual(await manager.reapExhaustedJobs(3), [JOB_ID]);
  console.log("EXHAUSTED LEASE: PASS");
});

test("Invalid worker and lease configuration rejected", async () => {
  const pool = createMockPool(() => ({ rows: [] }));
  const manager = createPostgresLeaseManager({ pool });
  await assert.rejects(manager.claimJob("invalid worker name"), /INVALID_WORKER_ID/);
  await assert.rejects(manager.claimJob(WORKER_A, { leaseSeconds: 0 }), /INVALID_LEASE_SECONDS/);
  assert.equal(pool.calls.length, 0);
  console.log("INPUT VALIDATION: PASS");
});
