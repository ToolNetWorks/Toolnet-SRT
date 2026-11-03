import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { createDurableWorker } from "../server/storage/durable-worker.mjs";

function setup({ sourceFails = false, staleLease = false, asrSemaphore = null } = {}) {
  const jobId = randomUUID();
  const leaseToken = randomUUID();
  const calls = [];
  const job = { id: jobId, lease_token: leaseToken, extension: ".mp3", source_key: `toolnet-srt/jobs/${jobId}/source.mp3` };
  const leases = {
    async claimJob() { calls.push("claim"); return job; },
    async heartbeatJob() { calls.push("heartbeat"); return staleLease ? null : { id: jobId }; },
    async completeJob(args) { calls.push("complete"); assert.equal(args.leaseToken, leaseToken); return { status: "completed" }; },
    async failJob() { calls.push("fail"); return { status: "queued" }; },
  };
  const storage = {
    async downloadFile({ localPath }) {
      calls.push("download");
      if (sourceFails) throw new Error("MOCK_R2_ERROR");
      await writeFile(localPath, "mock audio");
    },
  };
  const results = {
    async putResult({ jobId, leaseToken, srt }) {
      calls.push("upload-result");
      assert.match(String(srt), /Hello/);
      return { key: `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt` };
    },
  };
  const worker = createDurableWorker({
    leases, storage, results, asrSemaphore,
    split: async () => { calls.push("split"); return [{ path: "mock-segment.mp3", startMs: 0, endMs: 1000 }]; },
    transcribe: async () => { calls.push("transcribe"); return [{ text: "Hello", startMs: 0, endMs: 1000 }]; },
    toSrt: async () => { calls.push("to-srt"); return "1\n00:00:00,000 --> 00:00:01,000\nHello\n"; },
  });
  return { worker, calls, jobId };
}

test("Worker completes R2 -> ASR -> R2 -> Neon", async () => {
  const { worker, calls, jobId } = setup();
  const result = await worker.runOnce();
  assert.equal(result.status, "completed");
  assert.equal(result.jobId, jobId);
  assert.ok(calls.indexOf("claim") < calls.indexOf("download"));
  assert.ok(calls.indexOf("download") < calls.indexOf("split"));
  assert.ok(calls.indexOf("split") < calls.indexOf("transcribe"));
  assert.ok(calls.indexOf("to-srt") < calls.indexOf("upload-result"));
  assert.ok(calls.indexOf("upload-result") < calls.indexOf("complete"));
});

test('Injected semaphore fences the legacy segmented ASR path', async () => {
  const calls = [];
  const semaphore = {
    async acquire(owner) { calls.push('acquire'); assert.equal(owner.chunkIndex, 0); return {...owner,token:randomUUID()}; },
    async heartbeat() { calls.push('heartbeat'); return true; },
    async release() { calls.push('release'); return true; },
    async rateLimited() { throw new Error('UNEXPECTED_RATE_LIMIT'); },
  };
  const {worker}=setup({asrSemaphore:semaphore});
  assert.equal((await worker.runOnce()).status,'completed');
  assert.deepEqual(calls,['acquire','heartbeat','heartbeat','release']);
});

test("R2 source failure does not complete job", async () => {
  const { worker, calls } = setup({ sourceFails: true });
  const result = await worker.runOnce();
  assert.equal(result.status, "queued");
  assert.ok(calls.includes("fail"));
  assert.ok(!calls.includes("complete"));
});

test("Lost lease prevents result upload", async () => {
  const { worker, calls } = setup({ staleLease: true });
  const result = await worker.runOnce();
  assert.notEqual(result.status, "completed");
  assert.ok(!calls.includes("upload-result"));
  assert.ok(!calls.includes("complete"));
});

test("Idle queue performs no processing", async () => {
  const worker = createDurableWorker({
    leases: { claimJob: async () => null, heartbeatJob: async () => null, completeJob: async () => null, failJob: async () => null },
    storage: { downloadFile: async () => {} },
    results: { putResult: async () => {} },
  });
  assert.deepEqual(await worker.runOnce(), { status: "idle" });
});

test("Reject invalid configuration", () => {
  assert.throws(() => createDurableWorker({ leases: {}, storage: {}, results: {} }), /LEASE_MANAGER_REQUIRED/);
});
