import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { createDurableWorker } from "../server/storage/durable-worker.mjs";

function createHarness({ rejectStage = null } = {}) {
  const jobId = randomUUID(), token = randomUUID(), updates = [], events = [];
  const leases = {
    async claimJob() { events.push("claim"); return { id: jobId, lease_token: token, extension: ".mp3", source_key: `toolnet-srt/jobs/${jobId}/source.mp3` }; },
    async heartbeatJob() { return { id: jobId }; },
    async completeJob(args) { events.push("complete"); assert.equal(args.leaseToken, token); return { status: "completed" }; },
    async failJob() { events.push("fail"); return null; },
  };
  const storage = { async downloadFile({ localPath }) { events.push("download"); await writeFile(localPath, "mock audio"); } };
  const results = { async putResult({ jobId, leaseToken }) { events.push("upload-result"); return { key: `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt` }; } };
  const progress = { async updateProgress(input) { updates.push({ ...input }); return input.stage === rejectStage ? null : { id: jobId }; } };
  const worker = createDurableWorker({ leases, storage, results, progress, workerId: "progress-test-worker",
    split: async () => [{ path: "segment-1.mp3", startMs: 0 }, { path: "segment-2.mp3", startMs: 1000 }],
    transcribe: async () => [{ text: "Hello", startMs: 0, endMs: 500 }],
    toSrt: async () => "1\n00:00:00,000 --> 00:00:00,500\nHello\n",
  });
  return { worker, updates, events, jobId, token };
}

test("Worker reports all processing stages", async () => {
  const { worker, updates, events, jobId, token } = createHarness();
  assert.equal((await worker.runOnce()).status, "completed");
  assert.deepEqual(updates.map(item => item.stage), ["preparing", "downloading", "segmenting", "transcribing", "transcribing", "transcribing", "uploading_result", "finalizing"]);
  assert.deepEqual(updates.map(item => item.progress), [1, 3, 5, 10, 52, 95, 96, 99]);
  assert.deepEqual(updates.map(item => item.completedSegments), [0, 0, 0, 0, 1, 2, 2, 2]);
  assert.deepEqual(updates.map(item => item.totalSegments), [null, null, null, 2, 2, 2, 2, 2]);
  for (const update of updates) {
    assert.equal(update.jobId, jobId);
    assert.equal(update.leaseToken, token);
    assert.equal(update.workerId, "progress-test-worker");
  }
  assert.ok(events.indexOf("upload-result") < events.indexOf("complete"));
  console.log("WORKER PROGRESS STAGES: PASS");
});

test("Lost progress lease blocks SRT upload", async () => {
  const { worker, updates, events } = createHarness({ rejectStage: "transcribing" });
  const result = await worker.runOnce();
  assert.equal(result.status, "lease_lost");
  assert.equal(result.errorCode, "LEASE_LOST");
  assert.ok(updates.some(item => item.stage === "transcribing"));
  assert.ok(!events.includes("upload-result"));
  assert.ok(!events.includes("complete"));
  console.log("STALE PROGRESS LEASE: REJECTED");
});

test("Invalid progress manager is rejected", () => {
  assert.throws(() => createDurableWorker({
    leases: { claimJob() {}, heartbeatJob() {}, completeJob() {}, failJob() {} },
    storage: { downloadFile() {} }, results: { putResult() {} }, progress: {},
  }), /INVALID_PROGRESS_MANAGER/);
  console.log("PROGRESS MANAGER VALIDATION: PASS");
});
