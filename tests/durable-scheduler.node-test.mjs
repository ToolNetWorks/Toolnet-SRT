import test from "node:test";
import assert from "node:assert/strict";
import { createDurableScheduler } from "../server/storage/durable-scheduler.mjs";

async function waitFor(check, timeoutMs = 1500) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  throw new Error("WAIT_TIMEOUT");
}

test("Concurrency limit is enforced", async () => {
  const resolvers = [], ids = new Set();
  const scheduler = createDurableScheduler({ maxConcurrency: 2, pollIntervalMs: 100,
    workerFactory({ workerId }) {
      ids.add(workerId);
      return { runOnce() { return new Promise(resolve => { resolvers.push(resolve); }); } };
    },
  });
  scheduler.start();
  assert.equal(scheduler.getStatus().activeWorkers, 2);
  assert.equal(resolvers.length, 2);
  assert.equal(ids.size, 2);
  const timeout = await scheduler.stop({ timeoutMs: 20 });
  assert.equal(timeout.stopped, false);
  assert.equal(timeout.state, "stopping");
  for (const resolve of resolvers) resolve({ status: "completed" });
  const stopped = await scheduler.stop({ timeoutMs: 1000 });
  assert.equal(stopped.stopped, true);
  assert.equal(stopped.activeWorkers, 0);
  console.log("CONCURRENCY AND GRACEFUL STOP: PASS");
});

test("A new scheduler receives pending jobs", async () => {
  const queue = ["job-a"], processed = [];
  function makeScheduler() {
    return createDurableScheduler({ pollIntervalMs: 10, workerFactory() {
      return { async runOnce() { const id = queue.shift(); if (!id) return null; processed.push(id); return { status: "completed" }; } };
    } });
  }
  const first = makeScheduler();
  first.start();
  await waitFor(() => processed.length === 1);
  assert.equal((await first.stop()).stopped, true);
  queue.push("job-b");
  const second = makeScheduler();
  second.start();
  await waitFor(() => processed.length === 2);
  assert.equal((await second.stop()).stopped, true);
  assert.deepEqual(processed, ["job-a", "job-b"]);
  console.log("RESTART AND PENDING JOBS: PASS");
});

test("Worker failure triggers retry", async () => {
  let attempts = 0, logged = 0;
  const scheduler = createDurableScheduler({ pollIntervalMs: 10, errorBackoffMs: 10,
    logger: { error() { logged += 1; } }, workerFactory() {
      return { async runOnce() {
        attempts += 1;
        if (attempts === 1) throw new Error("TEMPORARY_DATABASE_ERROR");
        if (attempts === 2) return { status: "completed" };
        return null;
      } };
    },
  });
  scheduler.start();
  await waitFor(() => scheduler.getStatus().completed === 1);
  const result = await scheduler.stop();
  assert.equal(result.stopped, true);
  assert.equal(result.errors, 1);
  assert.equal(result.completed, 1);
  assert.equal(logged, 1);
  console.log("WORKER ERROR RECOVERY: PASS");
});

test("Double start is rejected", async () => {
  const scheduler = createDurableScheduler({ workerFactory() { return { async runOnce() { return null; } }; } });
  scheduler.start();
  assert.throws(() => scheduler.start(), /SCHEDULER_ALREADY_RUNNING/);
  await scheduler.stop();
  console.log("DUPLICATE START GUARD: PASS");
});

test("Invalid configuration is rejected", () => {
  assert.throws(() => createDurableScheduler(), /WORKER_FACTORY_REQUIRED/);
  assert.throws(() => createDurableScheduler({ workerFactory: () => ({}), maxConcurrency: 0 }), /INVALID_MAX_CONCURRENCY/);
  const scheduler = createDurableScheduler({ workerFactory: () => ({}) });
  assert.throws(() => scheduler.start(), /INVALID_DURABLE_WORKER/);
  assert.equal(scheduler.getStatus().state, "stopped");
  console.log("SCHEDULER VALIDATION: PASS");
});
