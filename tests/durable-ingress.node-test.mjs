import test from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { createDurableUploadIngress } from "../server/storage/durable-ingress.mjs";

function request(content, mime = "audio/mpeg") {
  const stream = Readable.from([Buffer.from(content)]);
  stream.headers = { "content-type": mime };
  return stream;
}

function setup({ failR2 = false, failDb = false } = {}) {
  const calls = [];
  const storage = {
    async uploadFile({ jobId, filename }) {
      calls.push("r2");
      if (failR2) throw new Error("MOCK_R2_FAILURE");
      return { key: `toolnet-srt/jobs/${jobId}/${filename}` };
    },
  };
  const jobStore = {
    async saveJob(job) {
      calls.push("neon");
      if (failDb) throw new Error("MOCK_DB_FAILURE");
      return job;
    },
  };
  return { calls, ingress: createDurableUploadIngress({ storage, jobStore, maxBytes: 1024 }) };
}

test("Source reaches R2 before queued job in Neon", async () => {
  const { ingress, calls } = setup();
  const job = await ingress.createUpload(request("test audio"), "audio/mpeg");
  assert.equal(job.status, "queued");
  assert.equal(job.extension, ".mp3");
  assert.match(job.sourceKey, /source\.mp3$/);
  assert.deepEqual(calls, ["r2", "neon"]);
});

test("R2 failure does not create Neon job", async () => {
  const { ingress, calls } = setup({ failR2: true });
  await assert.rejects(ingress.createUpload(request("audio"), "audio/mpeg"), /MOCK_R2_FAILURE/);
  assert.deepEqual(calls, ["r2"]);
});

test("DB failure is propagated", async () => {
  const { ingress, calls } = setup({ failDb: true });
  const originalError = console.error;
  console.error = () => {};
  try {
    await assert.rejects(ingress.createUpload(request("audio"), "audio/mpeg"), /MOCK_DB_FAILURE/);
  } finally {
    console.error = originalError;
  }
  assert.deepEqual(calls, ["r2", "neon"]);
});

test("Unsupported MIME rejected", async () => {
  const { ingress, calls } = setup();
  await assert.rejects(ingress.createUpload(request("hello"), "text/plain"), { code: "UNSUPPORTED_MEDIA_TYPE" });
  assert.deepEqual(calls, []);
});

test("Oversized upload rejected before R2", async () => {
  const { ingress, calls } = setup();
  await assert.rejects(ingress.createUpload(request("x".repeat(2048)), "audio/mpeg"), { code: "UPLOAD_TOO_LARGE" });
  assert.deepEqual(calls, []);
});

test("Empty upload rejected", async () => {
  const { ingress, calls } = setup();
  await assert.rejects(ingress.createUpload(request(""), "audio/mpeg"), { code: "EMPTY_UPLOAD" });
  assert.deepEqual(calls, []);
});
