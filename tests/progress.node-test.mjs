import test from "node:test";
import assert from "node:assert/strict";
import { createPostgresProgressManager } from "../server/storage/progress.mjs";

const JOB_ID = "123e4567-e89b-42d3-a456-426614174000";
const TOKEN = "8b67d132-e591-459e-9d3c-c47672b26abb";
function input(overrides = {}) {
  return { jobId: JOB_ID, workerId: "worker-a", leaseToken: TOKEN, stage: "transcribing",
    progress: 45, completedSegments: 2, totalSegments: 5, ...overrides };
}
function createMockPool(rows = []) {
  const calls = [];
  return { calls, async query(sql, params) { calls.push({ sql, params }); return { rows }; } };
}

test("Progress update uses lease guards", async () => {
  const pool = createMockPool([{ id: JOB_ID, status: "processing", stage: "transcribing", progress: 45,
    completed_segments: 2, total_segments: 5, updated_at: new Date() }]);
  const manager = createPostgresProgressManager({ pool });
  const result = await manager.updateProgress(input());
  assert.equal(result.progress, 45);
  assert.equal(result.completedSegments, 2);
  const { sql, params } = pool.calls[0];
  assert.match(sql, /lease_owner = \$2/);
  assert.match(sql, /lease_token = \$3::uuid/);
  assert.match(sql, /lease_expires_at > NOW\(\)/);
  assert.match(sql, /status = 'processing'/);
  assert.match(sql, /progress <= \$5/);
  assert.match(sql, /completed_segments <= \$6/);
  assert.match(sql, /total_segments = \$7::integer/);
  assert.match(sql, /ELSE 99/);
  assert.equal(params[0], JOB_ID);
  assert.equal(params[1], "worker-a");
  assert.equal(params[2], TOKEN);
  assert.equal(params[3], "transcribing");
  assert.equal(params[7], 3);
});

test("Stale lease returns null", async () => {
  const manager = createPostgresProgressManager({ pool: createMockPool() });
  assert.equal(await manager.updateProgress(input()), null);
});

for (const [name, overrides, expected] of [
  ["Reject progress reaching 100 before completion", { progress: 100 }, /INVALID_PROGRESS/],
  ["Reject completed segments exceeding total", { completedSegments: 6 }, /COMPLETED_EXCEEDS_TOTAL/],
  ["Reject unknown stage", { stage: "unknown" }, /INVALID_JOB_STAGE/],
  ["Reject completed segments without total", { totalSegments: null }, /TOTAL_SEGMENTS_REQUIRED/],
  ["Invalid lease token rejected before SQL", { leaseToken: "invalid" }, /INVALID_LEASE_TOKEN/],
]) {
  test(name, async () => {
    const pool = createMockPool();
    const manager = createPostgresProgressManager({ pool });
    await assert.rejects(manager.updateProgress(input(overrides)), expected);
    assert.equal(pool.calls.length, 0);
  });
}
