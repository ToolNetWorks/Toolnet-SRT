import test from "node:test";
import assert from "node:assert/strict";
import { createDurableAssembly } from "../server/storage/durable-assembly.mjs";

const env = { DATABASE_URL: "postgresql://unit:password@db.invalid/test", R2_ACCOUNT_ID: "a".repeat(32),
  R2_ACCESS_KEY_ID: "unit-access", R2_SECRET_ACCESS_KEY: "unit-secret", R2_BUCKET: "unit-bucket" };
const config = { enabled: true, concurrency: 2, pollIntervalMs: 100, errorBackoffMs: 200,
  leaseSeconds: 120, heartbeatIntervalMs: 20000, maxAttempts: 3 };

function harness({ failAt = null } = {}) {
  const events = [];
  let workerOptions = null, schedulerOptions = null;
  const pool = { async query() { return { rows: [] }; }, async end() { events.push("neon-close"); } };
  const client = { async send() { return {}; }, destroy() { events.push("r2-close"); } };
  const manager = { createUpload() {}, createRemote() {}, getJob() {}, getResult() {}, cancelJob() {} };
  const factories = {
    poolFactory() { events.push("neon-create"); return pool; },
    clientFactory() { events.push("r2-create"); return client; },
    jobStore() { return { async ping() { events.push("neon-ping"); if (failAt === "ping") throw new Error("SECRET_DB_ERROR"); } }; },
    storage() { return { downloadFile() {} }; },
    results() { return { putResult() {}, getResult() {} }; },
    leases() { return {}; },
    progress() { return { updateProgress() {} }; },
    cancellation() { return { cancelJob() {} }; },
    ingress() { if (failAt === "ingress") throw new Error("SECRET_INGRESS_ERROR"); return { createUpload() {} }; },
    remoteIngress() { return { createRemote() {} }; },
    manager() { return manager; },
    worker(options) { workerOptions = options; return { async runOnce() {} }; },
    scheduler(options) {
      schedulerOptions = options;
      return { start() { events.push("scheduler-start"); }, async stop() { return { stopped: true }; }, getStatus() { return { state: "stopped" }; } };
    },
    loadAsr: async () => ({ transcribe: async ({ input }) => { events.push("asr-call"); return [{ text: input }]; },
      toSrt: segments => { events.push("srt-call"); return String(segments.length); } }),
  };
  return { assemble: createDurableAssembly({ factories }), events,
    getWorkerOptions: () => workerOptions, getSchedulerOptions: () => schedulerOptions };
}

test("Assembly creates components without starting", async () => {
  const h = harness(), runtime = await h.assemble({ env, config });
  assert.equal(typeof runtime.manager.getJob, "function"); assert.equal(typeof runtime.scheduler.start, "function");
  assert.deepEqual(h.events, ["neon-create", "neon-ping", "r2-create"]);
  assert.equal(h.getSchedulerOptions().maxConcurrency, 2);
  await runtime.close(); assert.deepEqual(h.events.slice(-2), ["r2-close", "neon-close"]);
  console.log("ASSEMBLY WITHOUT AUTOSTART: PASS");
});

test("Each worker receives real dependencies", async () => {
  const h = harness(), runtime = await h.assemble({ env, config });
  const worker = h.getSchedulerOptions().workerFactory({ workerId: "worker-a" });
  assert.equal(typeof worker.runOnce, "function");
  const options = h.getWorkerOptions();
  assert.equal(options.workerId, "worker-a"); assert.equal(typeof options.progress.updateProgress, "function");
  assert.equal(typeof options.split, "function"); assert.equal(options.leaseSeconds, 120);
  assert.equal(options.heartbeatIntervalMs, 20000); assert.equal(options.maxAttempts, 3);
  const recognized = await options.transcribe("test.mp3", undefined);
  assert.equal(recognized[0].text, "test.mp3"); assert.equal(await options.toSrt(recognized), "1");
  await runtime.close(); console.log("WORKER DEPENDENCY INJECTION: PASS");
});

test("Database failure closes PostgreSQL pool", async () => {
  const h = harness({ failAt: "ping" });
  await assert.rejects(h.assemble({ env, config }), /DURABLE_ASSEMBLY_FAILED/);
  assert.deepEqual(h.events, ["neon-create", "neon-ping", "neon-close"]);
  console.log("NEON FAILURE CLEANUP: PASS");
});

test("Partial failure closes R2 and Neon", async () => {
  const h = harness({ failAt: "ingress" });
  await assert.rejects(h.assemble({ env, config }), /DURABLE_ASSEMBLY_FAILED/);
  assert.deepEqual(h.events.slice(-2), ["r2-close", "neon-close"]);
  console.log("PARTIAL ASSEMBLY CLEANUP: PASS");
});

test("Resources are closed exactly once", async () => {
  const h = harness(), runtime = await h.assemble({ env, config });
  await Promise.all([runtime.close(), runtime.close(), runtime.close()]);
  assert.equal(h.events.filter(x => x === "neon-close").length, 1);
  assert.equal(h.events.filter(x => x === "r2-close").length, 1);
  console.log("IDEMPOTENT RESOURCE CLEANUP: PASS");
});

test("Disabled configuration cannot assemble", async () => {
  const h = harness();
  await assert.rejects(h.assemble({ env, config: { enabled: false } }), /DURABLE_MODE_NOT_ENABLED/);
  assert.equal(h.events.length, 0);
  console.log("DISABLED ASSEMBLY GUARD: PASS");
});
