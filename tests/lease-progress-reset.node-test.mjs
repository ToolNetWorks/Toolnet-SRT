import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";

test("Claim resets progress atomically", async () => {
  const id = randomUUID();
  const calls = [];
  const pool = {
    async query(sql, params) {
      calls.push({ sql, params });
      assert.match(sql, /FOR UPDATE SKIP LOCKED/);
      assert.match(sql, /status = 'processing'/);
      assert.match(sql, /stage = 'claimed'/);
      assert.match(sql, /progress = 0/);
      assert.match(sql, /completed_segments = 0/);
      assert.match(sql, /total_segments = NULL/);
      assert.match(sql, /error_code = NULL/);
      assert.match(sql, /result_key = NULL/);
      assert.match(sql, /attempt_count = jobs\.attempt_count \+ 1/);
      assert.match(sql, /lease_token = \$2::uuid/);
      return { rows: [{ id, status: "processing", attempt_count: 2, progress: 0, lease_token: params[1] }] };
    },
  };
  const manager = createPostgresLeaseManager({ pool });
  const claimed = await manager.claimJob("worker-b");
  assert.equal(claimed.id, id);
  assert.equal(claimed.attempt_count, 2);
  assert.equal(claimed.status, "processing");
  assert.equal(calls.length, 1);
  console.log("ATOMIC PROGRESS RESET: PASS");
});

test("Every claim receives a new lease token", async () => {
  const tokens = [];
  const pool = { async query(_sql, params) { tokens.push(params[1]); return { rows: [{ id: randomUUID(), lease_token: params[1] }] }; } };
  const manager = createPostgresLeaseManager({ pool });
  await manager.claimJob("worker-a");
  await manager.claimJob("worker-b");
  assert.equal(tokens.length, 2);
  assert.notEqual(tokens[0], tokens[1]);
  console.log("NEW LEASE TOKEN: PASS");
});

test("Invalid claim cannot execute SQL", async () => {
  let queryCount = 0;
  const pool = { async query() { queryCount++; return { rows: [] }; } };
  const manager = createPostgresLeaseManager({ pool });
  await assert.rejects(manager.claimJob("invalid worker name"), /INVALID_WORKER_ID/);
  assert.equal(queryCount, 0);
  console.log("CLAIM VALIDATION: PASS");
});
