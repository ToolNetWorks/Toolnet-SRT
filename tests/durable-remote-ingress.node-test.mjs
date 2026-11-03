import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { writeFile, access } from "node:fs/promises";
import { createDurableRemoteIngress } from "../server/storage/durable-remote-ingress.mjs";

const SOURCE_URL = "https://media.example/video.mp4?token=private123";
function createHarness({ downloadFailure = false, payload = "mock media", maxBytes = 1024 } = {}) {
  const events = [], jobId = randomUUID();
  let tempPath = null;
  const ingress = { async createUpload(source, mimeType) {
    events.push("ingress");
    assert.equal(mimeType, "video/mp4");
    assert.equal(Number(source.headers["content-length"]), Buffer.byteLength(payload));
    const chunks = [];
    for await (const chunk of source) chunks.push(chunk);
    assert.equal(Buffer.concat(chunks).toString(), payload);
    return { id: jobId, status: "queued", stage: "queued", progress: 0,
      sourceKey: `toolnet-srt/jobs/${jobId}/source.mp4` };
  } };
  const remote = createDurableRemoteIngress({ ingress, allowedHosts: ["media.example"], maxBytes,
    async validate(url, hosts) {
      events.push("validate"); assert.equal(url, SOURCE_URL); assert.deepEqual(hosts, ["media.example"]); return new URL(url);
    },
    async download(url, destination, options) {
      events.push("download"); assert.equal(url, SOURCE_URL); assert.equal(options.maxBytes, maxBytes);
      assert.deepEqual(options.allowedHosts, ["media.example"]); tempPath = destination;
      if (downloadFailure) throw new Error("MOCK_DOWNLOAD_FAILURE");
      await writeFile(destination, payload);
    },
  });
  return { remote, events, getTempPath: () => tempPath };
}

test("Remote URL creates a durable queued job", async () => {
  const h = createHarness(), result = await h.remote.createRemote(SOURCE_URL);
  assert.equal(result.status, "queued"); assert.equal(result.progress, 0);
  assert.deepEqual(h.events, ["validate", "download", "ingress"]);
  assert.equal(JSON.stringify(result).includes("private123"), false);
  console.log("REMOTE DURABLE INGESTION: PASS");
});
test("Temporary media is cleaned after success", async () => {
  const h = createHarness(); await h.remote.createRemote(SOURCE_URL);
  await assert.rejects(access(h.getTempPath()), { code: "ENOENT" }); console.log("TEMP FILE SUCCESS CLEANUP: PASS");
});
test("Temporary media is cleaned after failure", async () => {
  const h = createHarness({ downloadFailure: true });
  await assert.rejects(h.remote.createRemote(SOURCE_URL), /MOCK_DOWNLOAD_FAILURE/);
  assert.ok(!h.events.includes("ingress"));
  await assert.rejects(access(h.getTempPath()), { code: "ENOENT" }); console.log("TEMP FILE FAILURE CLEANUP: PASS");
});
test("Oversized remote file is rejected", async () => {
  const h = createHarness({ payload: "x".repeat(100), maxBytes: 10 });
  await assert.rejects(h.remote.createRemote(SOURCE_URL), { code: "UPLOAD_TOO_LARGE", status: 413 });
  assert.ok(!h.events.includes("ingress")); console.log("REMOTE SIZE LIMIT: PASS");
});
test("Remote mode requires an allowlist", async () => {
  const remote = createDurableRemoteIngress({ ingress: { async createUpload() { throw new Error("UNEXPECTED_UPLOAD"); } } });
  await assert.rejects(remote.createRemote(SOURCE_URL), { code: "REMOTE_URL_DISABLED", status: 503 });
  console.log("REMOTE ALLOWLIST GUARD: PASS");
});
test("Unapproved hostname is rejected", async () => {
  const remote = createDurableRemoteIngress({ ingress: { async createUpload() { throw new Error("UNEXPECTED_UPLOAD"); } }, allowedHosts: ["media.example"] });
  await assert.rejects(remote.createRemote("https://unapproved.example/file.mp4"), { code: "SOURCE_HOST_NOT_ALLOWED", status: 403 });
  console.log("UNAPPROVED HOST REJECTED: PASS");
});
test("Invalid configuration is rejected", () => {
  assert.throws(() => createDurableRemoteIngress(), /DURABLE_INGRESS_REQUIRED/);
  assert.throws(() => createDurableRemoteIngress({ ingress: { createUpload() {} }, maxBytes: 0 }), /INVALID_MAX_BYTES/);
  assert.throws(() => createDurableRemoteIngress({ ingress: { createUpload() {} }, allowedHosts: ["media.example"], downloadTimeoutMs: 0 }), /INVALID_DOWNLOAD_TIMEOUT/);
  console.log("REMOTE CONFIG VALIDATION: PASS");
});
