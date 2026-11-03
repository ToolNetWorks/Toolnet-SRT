import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { createDurableRuntime } from "../server/storage/durable-runtime.mjs";

const env = { DURABLE_JOBS_ENABLED: "true", DATABASE_URL: "postgresql://unit:password@db.invalid/unit",
  R2_ACCOUNT_ID: "a".repeat(32), R2_ACCESS_KEY_ID: "unit-access", R2_SECRET_ACCESS_KEY: "unit-secret",
  R2_BUCKET: "unit-bucket", TOOLNET_SRT_API_KEY: "unit-api", TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY: Buffer.alloc(32, 1).toString("base64url"), TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY: Buffer.alloc(32, 2).toString("base64url") };
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

test('Runtime starts and drains both schedulers before closing', async () => {
  const events = [], processRef = new EventEmitter();
  const make = name => ({ start() { events.push(`${name}-start`); }, async stop() { events.push(`${name}-stop`); return { stopped: true }; }, getStatus() { return { state: 'mock' }; } });
  const runtime = createDurableRuntime({ env, processRef, preflightFactory: ready, async assemble() { return {
    manager: { createUpload() {}, getJob() {}, getResult() {}, cancelJob() {} },
    scheduler: make('asr'), fetchScheduler: make('fetch'), async close() { events.push('close'); },
  }; } });
  await runtime.start(); assert.deepEqual(events, ['fetch-start', 'asr-start']);
  assert.equal((await runtime.shutdown()).stopped, true);
  assert.deepEqual(events, ['fetch-start', 'asr-start', 'fetch-stop', 'asr-stop', 'close']);
});

test('API-only runtime does not start either scheduler', async () => {
  const events = [];
  const make = name => ({ start() { events.push(`${name}-start`); },
    async stop() { events.push(`${name}-stop`); return { stopped: true }; },
    getStatus() { return { state: 'stopped' }; } });
  const runtime = createDurableRuntime({ env: { ...env, DURABLE_RUN_WORKERS: 'false' },
    processRef: new EventEmitter(), preflightFactory: ready,
    async assemble() { return { manager: { createUpload(){}, getJob(){}, getResult(){}, cancelJob(){} },
      scheduler: make('asr'), fetchScheduler: make('fetch'), async close() { events.push('close'); } }; } });
  assert.equal((await runtime.start()).state, 'running');
  assert.equal(typeof runtime.getManager().getJob, 'function');
  assert.deepEqual(events, []);
  assert.equal((await runtime.shutdown()).stopped, true);
  assert.deepEqual(events, ['close']);
});

test('Worker mode rejects an invalid value', async () => {
  const runtime = createDurableRuntime({ env: { ...env, DURABLE_RUN_WORKERS: 'yes' },
    preflightFactory: ready, async assemble() { throw new Error('MUST_NOT_ASSEMBLE'); } });
  await assert.rejects(runtime.start(), /INVALID_DURABLE_RUN_WORKERS/);
});

test('Failed second scheduler start drains first before resource cleanup', async () => {
  const events = [], processRef = new EventEmitter();
  const scheduler = { start() { throw Error('start failure'); }, async stop() { events.push('asr-stop'); return { stopped: true }; }, getStatus() { return {}; } };
  const fetchScheduler = { start() { events.push('fetch-start'); }, async stop() { events.push('fetch-stop'); return { stopped: true }; }, getStatus() { return {}; } };
  const runtime = createDurableRuntime({ env, processRef, preflightFactory: ready, async assemble() { return {
    manager: { createUpload() {}, getJob() {}, getResult() {}, cancelJob() {} }, scheduler, fetchScheduler,
    async close() { events.push('close'); },
  }; } });
  await assert.rejects(runtime.start(), /start failure/);
  assert.deepEqual(events, ['fetch-start', 'fetch-stop', 'asr-stop', 'close']);
  assert.equal(processRef.listenerCount('SIGTERM'), 0);
});

test('Runtime waits for HTTP drain before closing dependencies', async () => {
  const events = [], processRef = new EventEmitter(); let release;
  const gate = new Promise(resolve => { release = resolve; });
  const scheduler = { start() {}, async stop() { events.push('stop'); return { stopped: true }; }, getStatus() { return {}; } };
  const runtime = createDurableRuntime({ env, processRef, preflightFactory: ready, beforeClose: () => gate,
    async assemble() { return { manager: { createUpload(){}, getJob(){}, getResult(){}, cancelJob(){} }, scheduler,
      async close() { events.push('close'); } }; } });
  await runtime.start(); const pending = runtime.shutdown();
  await new Promise(resolve => setTimeout(resolve, 5)); assert.deepEqual(events, ['stop']);
  release(); assert.equal((await pending).stopped, true); assert.deepEqual(events, ['stop','close']);
});
