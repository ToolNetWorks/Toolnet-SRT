import test from "node:test";
import assert from "node:assert/strict";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";

const JOB_ID = "123e4567-e89b-42d3-a456-426614174000";
function makeJob(overrides = {}) {
  return {
    id: JOB_ID, status: "queued", stage: "queued", progress: 0,
    completedSegments: 0, totalSegments: null, extension: ".mp4",
    sourceKey: `toolnet-srt/jobs/${JOB_ID}/source.mp4`, resultKey: null,
    ...overrides,
  };
}

test("New queued job uses insert-only SQL", async () => {
  const pool = {
    async query(sql, values) {
      assert.match(sql, /ON CONFLICT \(id\) DO NOTHING/);
      assert.doesNotMatch(sql, /ON CONFLICT[\s\S]*DO UPDATE/);
      return { rows: [{
        id: values[0], status: values[1], stage: values[2], progress: values[3],
        completed_segments: values[4], total_segments: values[5], extension: values[6],
        error_code: values[7], source_key: values[8], result_key: values[9],
        created_at: new Date(), updated_at: new Date(),
      }] };
    },
  };
  const store = createPostgresJobStore({ pool });
  assert.equal((await store.saveJob(makeJob())).status, "queued");
});

test("Duplicate ID cannot overwrite existing job", async () => {
  let queryCount = 0;
  const pool = { async query(sql) { queryCount++; assert.match(sql, /DO NOTHING/); return { rows: [] }; } };
  const store = createPostgresJobStore({ pool });
  await assert.rejects(store.saveJob(makeJob()), /JOB_ALREADY_EXISTS/);
  assert.equal(queryCount, 1);
});

test("Processing state rejected before SQL", async () => {
  let called = false;
  const pool = { async query() { called = true; return { rows: [] }; } };
  const store = createPostgresJobStore({ pool });
  await assert.rejects(store.saveJob(makeJob({ status: "processing" })), /JOB_CREATION_REQUIRES_QUEUED_STATUS/);
  assert.equal(called, false);
});

test("Completed state rejected before SQL", async () => {
  const pool = { async query() { throw new Error("SQL_SHOULD_NOT_RUN"); } };
  const store = createPostgresJobStore({ pool });
  await assert.rejects(store.saveJob(makeJob({ status: "completed" })), /JOB_CREATION_REQUIRES_QUEUED_STATUS/);
});
