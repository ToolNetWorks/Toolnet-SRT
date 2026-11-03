import test from "node:test";
import assert from "node:assert/strict";
import { createPostgresCancellation } from "../server/storage/cancellation.mjs";

const JOB_ID = "123e4567-e89b-42d3-a456-426614174000";

test("Cancel queued or processing job atomically", async () => {
  let queryCount = 0;
  const pool = {
    async query(sql, params) {
      queryCount++;
      assert.match(sql, /status IN \('queued', 'processing'\)/);
      assert.match(sql, /lease_token = NULL/);
      assert.match(sql, /lease_owner = NULL/);
      assert.match(sql, /lease_expires_at = NULL/);
      assert.deepEqual(params, [JOB_ID]);
      return { rows: [{ id: JOB_ID, status: "cancelled", stage: "cancelled" }] };
    },
  };
  const manager = createPostgresCancellation({ pool });
  const result = await manager.cancelJob(JOB_ID);
  assert.equal(result.status, "cancelled");
  assert.equal(queryCount, 1);
});

test("Finished job cannot be cancelled", async () => {
  const pool = { async query() { return { rows: [] }; } };
  const manager = createPostgresCancellation({ pool });
  assert.equal(await manager.cancelJob(JOB_ID), null);
});

test("Invalid ID rejected before SQL", async () => {
  let queried = false;
  const pool = { async query() { queried = true; return { rows: [] }; } };
  const manager = createPostgresCancellation({ pool });
  await assert.rejects(manager.cancelJob("../invalid"), /INVALID_JOB_ID/);
  assert.equal(queried, false);
});

test("Postgres pool is required", () => {
  assert.throws(() => createPostgresCancellation(), /POSTGRES_POOL_REQUIRED/);
});
