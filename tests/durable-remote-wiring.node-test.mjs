import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createDurableJobManager } from "../server/storage/durable-job-manager.mjs";
import { createDurableAssembly } from "../server/storage/durable-assembly.mjs";

const SIGNED_URL = "https://media.example/video.mp4?token=private123";
function queuedJob() {
  const now = new Date().toISOString();
  return { id: randomUUID(), status: "queued", stage: "queued", progress: 0, completedSegments: 0,
    totalSegments: null, errorCode: null, sourceKey: "internal-r2-key", createdAt: now, updatedAt: now };
}
function makeManager(remoteIngress = null) {
  return createDurableJobManager({ jobStore: { async getJob() { return null; } },
    resultStorage: { async getResult() { return Buffer.from("SRT"); } },
    cancellation: { async cancelJob() { return null; } }, remoteIngress });
}

test("Remote URL creates public queued job", async () => {
  let called = 0;
  const manager = makeManager({ async createRemote(url) { called++; assert.equal(url, SIGNED_URL); return queuedJob(); } });
  const result = await manager.createRemote(SIGNED_URL);
  assert.equal(called, 1); assert.equal(result.status, "queued"); assert.equal(result.stage, "queued");
  assert.equal(result.progress, 0); assert.equal(result.resultPath, null);
  const output = JSON.stringify(result);
  assert.ok(!output.includes("private123")); assert.ok(!output.includes("internal-r2-key"));
  assert.equal(Object.hasOwn(result, "sourceKey"), false);
  console.log("REMOTE MANAGER PUBLIC RESPONSE: PASS");
});

test("Remote disabled without dependency", async () => {
  await assert.rejects(makeManager().createRemote(SIGNED_URL), { code: "REMOTE_URL_DISABLED", status: 503 });
  console.log("REMOTE DISABLED GUARD: PASS");
});

test("Invalid remote job is rejected", async () => {
  const manager = makeManager({ async createRemote() { return { ...queuedJob(), status: "processing" }; } });
  await assert.rejects(manager.createRemote(SIGNED_URL), { code: "INVALID_QUEUED_JOB", status: 502 });
  console.log("REMOTE JOB VALIDATION: PASS");
});

test("Remote ingress errors propagate", async () => {
  const manager = makeManager({ async createRemote() { throw new Error("SOURCE_DOWNLOAD_FAILED"); } });
  await assert.rejects(manager.createRemote(SIGNED_URL), /SOURCE_DOWNLOAD_FAILED/);
  console.log("REMOTE ERROR PROPAGATION: PASS");
});

test("Assembly injects remote ingress and allowlist", async () => {
  let remoteOptions = null, managerOptions = null, closed = 0;
  const fakeIngress = { async createUpload() { return queuedJob(); } };
  const fakeRemote = { async createRemote() { return queuedJob(); } };
  const assemble = createDurableAssembly({ factories: {
    poolFactory() { return { async query() { return { rows: [] }; }, async end() { closed++; } }; },
    clientFactory() { return { async send() { return {}; }, destroy() { closed++; } }; },
    jobStore() { return { async ping() {} }; },
    leases() { return {}; }, progress() { return {}; }, cancellation() { return {}; },
    storage() { return {}; }, results() { return {}; }, ingress() { return fakeIngress; },
    remoteIngress(options) { remoteOptions = options; return fakeRemote; },
    manager(options) { managerOptions = options; return { createUpload() {}, createRemote() {}, getJob() {}, getResult() {}, cancelJob() {} }; },
    scheduler() { return { start() {}, async stop() { return { stopped: true }; }, getStatus() { return { state: "stopped" }; } }; },
  } });
  const runtime = await assemble({
    env: { DATABASE_URL: "postgresql://fake/fake", R2_BUCKET: "fake", TOOLNET_SRT_ALLOWED_MEDIA_HOSTS: "Media.Example, media.example, cdn.example" },
    config: { enabled: true, concurrency: 1, pollIntervalMs: 3000, errorBackoffMs: 10000,
      leaseSeconds: 120, heartbeatIntervalMs: 20000, maxAttempts: 3 },
  });
  try {
    assert.equal(remoteOptions.allowedHosts, null);
    assert.equal(remoteOptions.ingress, fakeIngress);
    assert.equal(managerOptions.remoteIngress, fakeRemote);
    assert.equal(managerOptions.ingress, fakeIngress);
    console.log("ASSEMBLY REMOTE INJECTION: PASS");
  } finally { await runtime.close(); }
  assert.equal(closed, 2);
});

test("Invalid remote dependency is rejected", () => {
  assert.throws(() => makeManager({}), /INVALID_DURABLE_REMOTE_INGRESS/);
  console.log("REMOTE DEPENDENCY VALIDATION: PASS");
});
