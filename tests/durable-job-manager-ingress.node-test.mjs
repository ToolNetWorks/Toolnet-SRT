import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Readable } from "node:stream";
import { readFile } from "node:fs/promises";
import { createDurableJobManager } from "../server/storage/durable-job-manager.mjs";
import { createDurableUploadIngress } from "../server/storage/durable-ingress.mjs";

function createManager(ingress) {
  return createDurableJobManager({
    jobStore: { async getJob() { return null; } },
    resultStorage: { async getResult() { return Buffer.from("test"); } },
    cancellation: { async cancelJob() { return null; } },
    ingress,
  });
}

function mockJob(overrides = {}) {
  const id = randomUUID(), now = new Date().toISOString();
  return { id, status: "queued", stage: "queued", progress: 0, completedSegments: 0,
    totalSegments: null, errorCode: null, sourceKey: `toolnet-srt/jobs/${id}/source.mp3`,
    resultKey: null, createdAt: now, updatedAt: now, ...overrides };
}

test("Manager connects R2 ingress to Neon", async () => {
  const calls = [];
  const ingress = createDurableUploadIngress({
    maxBytes: 1024,
    storage: { async uploadFile({ jobId, filename, localPath }) {
      calls.push("r2");
      assert.equal((await readFile(localPath)).toString(), "mock audio");
      return { key: `toolnet-srt/jobs/${jobId}/${filename}` };
    } },
    jobStore: { async saveJob(job) { calls.push("neon"); assert.equal(job.status, "queued"); return job; } },
  });
  const manager = createManager(ingress);
  const request = Readable.from([Buffer.from("mock audio")]);
  request.headers = { "content-length": "10" };
  const result = await manager.createUpload(request, "audio/mpeg");
  assert.equal(result.status, "queued"); assert.equal(result.stage, "queued"); assert.equal(result.progress, 0);
  assert.match(result.id, /^[0-9a-f-]{36}$/i); assert.deepEqual(calls, ["r2", "neon"]);
  assert.equal(Object.hasOwn(result, "sourceKey"), false);
  assert.equal(Object.hasOwn(result, "resultKey"), false);
  assert.equal(result.resultPath, null);
  console.log("DURABLE API UPLOAD: PASS");
});

test("Upload disabled when ingress is absent", async () => {
  const manager = createManager();
  await assert.rejects(manager.createUpload(null, "audio/mpeg"), { code: "DURABLE_UPLOAD_NOT_CONFIGURED", status: 503 });
  console.log("INGRESS DISABLED GUARD: PASS");
});

test("Ingress errors propagate correctly", async () => {
  const manager = createManager({ async createUpload() { throw new Error("R2_UPLOAD_FAILED"); } });
  await assert.rejects(manager.createUpload(null, "audio/mpeg"), /R2_UPLOAD_FAILED/);
  console.log("INGRESS FAILURE HANDLING: PASS");
});

test("Manager rejects invalid queued job", async () => {
  const manager = createManager({ async createUpload() { return mockJob({ status: "processing" }); } });
  await assert.rejects(manager.createUpload(null, "audio/mpeg"), { code: "INVALID_QUEUED_JOB", status: 502 });
  console.log("QUEUED JOB VALIDATION: PASS");
});

test("Invalid ingress dependency is rejected", () => {
  assert.throws(() => createManager({}), /INVALID_DURABLE_INGRESS/);
  console.log("INGRESS DEPENDENCY VALIDATION: PASS");
});
