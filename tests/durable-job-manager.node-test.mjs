import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createDurableJobManager } from "../server/storage/durable-job-manager.mjs";

function createHarness(status = "queued") {
  const id = randomUUID(), token = randomUUID();
  const job = { id, status, stage: status, progress: status === "completed" ? 100 : 0,
    completedSegments: 0, totalSegments: null, errorCode: null,
    resultKey: `toolnet-srt/jobs/${id}/attempts/${token}/result.srt`,
    createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  const events = [];
  const manager = createDurableJobManager({
    jobStore: { async getJob(requestedId) { events.push("neon-read"); return requestedId === id ? job : null; } },
    resultStorage: { async getResult({ key }) { events.push("r2-read"); assert.equal(key, job.resultKey); return Buffer.from("1\n00:00:00,000 --> 00:00:01,000\nHello\n"); } },
    cancellation: { async cancelJob(requestedId) {
      events.push("neon-cancel");
      if (requestedId !== id || !["queued", "processing"].includes(job.status)) return null;
      job.status = "cancelled"; job.stage = "cancelled"; return { id, status: "cancelled" };
    } },
  });
  return { manager, job, id, events };
}

test("Get job preserves API v0.2 response", async () => {
  const { manager, id } = createHarness();
  const job = await manager.getJob(id);
  assert.equal(job.id, id); assert.equal(job.status, "queued"); assert.equal(job.resultPath, null);
  assert.equal(job.progress, 0); assert.ok(job.createdAt);
});

test("Completed job returns SRT from R2", async () => {
  const { manager, id, events } = createHarness("completed");
  assert.equal((await manager.getJob(id)).resultPath, `/v1/jobs/${id}/result`);
  const srt = await manager.getResult(id);
  assert.ok(Buffer.isBuffer(srt)); assert.match(srt.toString(), /Hello/); assert.ok(events.includes("r2-read"));
});

test("Unfinished job cannot download SRT", async () => {
  const { manager, id, events } = createHarness();
  await assert.rejects(manager.getResult(id), { code: "JOB_NOT_COMPLETED", status: 409 });
  assert.ok(!events.includes("r2-read"));
});

test("Invalid result key is rejected", async () => {
  const { manager, job, id, events } = createHarness("completed");
  job.resultKey = "other-job/result.srt";
  await assert.rejects(manager.getResult(id), { code: "RESULT_NOT_AVAILABLE", status: 503 });
  assert.ok(!events.includes("r2-read"));
});

test("Cancel returns public job structure", async () => {
  const { manager, id } = createHarness("processing");
  const result = await manager.cancelJob(id);
  assert.equal(result.status, "cancelled"); assert.equal(result.stage, "cancelled"); assert.equal(result.resultPath, null);
});

test("Finished job cannot be cancelled", async () => {
  const { manager, id } = createHarness("completed");
  await assert.rejects(manager.cancelJob(id), { code: "JOB_ALREADY_FINISHED", status: 409 });
});

test("Invalid or missing job returns 404", async () => {
  const { manager } = createHarness();
  await assert.rejects(manager.getJob("../invalid"), { code: "JOB_NOT_FOUND", status: 404 });
  await assert.rejects(manager.getJob(randomUUID()), { code: "JOB_NOT_FOUND", status: 404 });
});
