import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { createDurableRuntime } from "../server/storage/durable-runtime.mjs";

const env = { DURABLE_JOBS_ENABLED: "true", DATABASE_URL: "postgresql://unit:password@db.invalid/unit",
  R2_ACCOUNT_ID: "a".repeat(32), R2_ACCESS_KEY_ID: "unit-access", R2_SECRET_ACCESS_KEY: "unit-secret",
  R2_BUCKET: "unit-bucket", TOOLNET_SRT_API_KEY: "unit-api" };
const ready = () => ({ async check() { return { enabled: true, neon: "ready", r2: "ready", workerStarted: false }; } });

function createHarness({ timeoutFirst = false, invalid = false } = {}) {
  const calls = [], processRef = new EventEmitter();
  processRef.exitCode = 0;
  let attempts = 0;
  const manager = { createUpload() {}, getJob() {}, getResult() {}, cancelJob() {} };
  const scheduler = {
    start() { calls.push("start"); },
    async stop() { attempts++; calls.push("stop"); return { stopped: !(timeoutFirst && attempts === 1) }; },
    getStatus() { return { state: "mock" }; },
  };
  const runtime = createDurableRuntime({ env, processRef, preflightFactory: ready,
    async assemble() { return { manager: invalid ? null : manager, scheduler, async close() { calls.push("close"); } }; },
  });
  return { runtime, calls, manager, processRef };
}

test("Disabled runtime does not connect", async () => {
  let attempts = 0;
  const runtime = createDurableRuntime({ env: {},
    preflightFactory() { attempts++; throw new Error("UNEXPECTED_PREFLIGHT"); },
    async assemble() { attempts++; throw new Error("UNEXPECTED_ASSEMBLY"); },
  });
  assert.equal((await runtime.start()).state, "disabled");
  assert.equal(attempts, 0);
  assert.equal((await runtime.shutdown()).stopped, true);
  assert.throws(() => runtime.getManager(), /DURABLE_RUNTIME_NOT_READY/);
  console.log("DISABLED MODE: PASS");
});

test("Runtime starts and closes exactly once", async () => {
  const { runtime, calls, manager } = createHarness();
  assert.throws(() => runtime.getManager(), /DURABLE_RUNTIME_NOT_READY/);
  assert.equal((await runtime.start()).state, "running");
  assert.equal(runtime.getManager(), manager);
  assert.deepEqual(calls, ["start"]);
  await assert.rejects(runtime.start(), /DURABLE_RUNTIME_ALREADY_STARTED/);
  assert.equal((await runtime.shutdown()).stopped, true);
  assert.deepEqual(calls, ["start", "stop", "close"]);
  await runtime.shutdown();
  assert.deepEqual(calls, ["start", "stop", "close"]);
  console.log("RUNTIME START AND CLOSE: PASS");
});

test("Preflight failure blocks initialization", async () => {
  let assembled = false;
  const runtime = createDurableRuntime({ env, preflightFactory: () => ({ async check() { throw new Error("NEON_CONNECTION_FAILED"); } }),
    async assemble() { assembled = true; } });
  await assert.rejects(runtime.start(), /NEON_CONNECTION_FAILED/);
  assert.equal(assembled, false);
  assert.equal(runtime.getStatus().state, "failed");
  console.log("PREFLIGHT FAILURE GUARD: PASS");
});

test("Incomplete readiness is rejected", async () => {
  const runtime = createDurableRuntime({ env,
    preflightFactory: () => ({ async check() { return { enabled: true, neon: "ready", r2: "failed", workerStarted: false }; } }),
    async assemble() { throw new Error("MUST_NOT_RUN"); } });
  await assert.rejects(runtime.start(), /DURABLE_PREFLIGHT_NOT_READY/);
  console.log("READINESS VALIDATION: PASS");
});

test("Invalid components are closed", async () => {
  const { runtime, calls } = createHarness({ invalid: true });
  await assert.rejects(runtime.start(), /INVALID_DURABLE_COMPONENTS/);
  assert.deepEqual(calls, ["close"]);
  console.log("INVALID COMPONENT CLEANUP: PASS");
});

test("Timeout preserves resources until drain", async () => {
  const { runtime, calls } = createHarness({ timeoutFirst: true });
  await runtime.start();
  assert.equal((await runtime.shutdown()).stopped, false);
  assert.equal(runtime.getStatus().state, "timed_out");
  assert.deepEqual(calls, ["start", "stop"]);
  assert.equal((await runtime.shutdown()).stopped, true);
  assert.deepEqual(calls, ["start", "stop", "stop", "close"]);
  console.log("SHUTDOWN TIMEOUT SAFETY: PASS");
});

test("SIGTERM drains and closes resources", async () => {
  const { runtime, processRef, calls } = createHarness();
  await runtime.start();
  processRef.emit("SIGTERM");
  for (let i = 0; i < 100 && runtime.getStatus().state !== "stopped"; i++) {
    await new Promise(resolve => setTimeout(resolve, 2));
  }
  assert.equal(runtime.getStatus().state, "stopped");
  assert.deepEqual(calls, ["start", "stop", "close"]);
  assert.equal(processRef.listenerCount("SIGTERM"), 0);
  console.log("SIGTERM RESOURCE CLEANUP: PASS");
});
