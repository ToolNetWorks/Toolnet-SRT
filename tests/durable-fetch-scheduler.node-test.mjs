import test from 'node:test';
import assert from 'node:assert/strict';
import { createDurableFetchScheduler } from '../server/storage/durable-fetch-scheduler.mjs';
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
test('Scheduler limits concurrency, tracks results and aborts during shutdown', async () => {
  const ids = new Set(); let active = 0, peak = 0, calls = 0;
  const scheduler = createDurableFetchScheduler({ maxConcurrency: 2, pollIntervalMs: 20, workerFactory({ workerId }) {
    ids.add(workerId);
    return { async runOnce({ signal }) {
      active++; peak = Math.max(peak, active); calls++;
      if (calls <= 2) { await wait(10); active--; return { claimed: true, status: 'completed' }; }
      await new Promise(resolve => { if (signal.aborted) resolve(); else signal.addEventListener('abort', resolve, { once: true }); });
      active--; return { status: 'idle' };
    } };
  } });
  scheduler.start(); await wait(40);
  const stopped = await scheduler.stop({ timeoutMs: 1000 });
  assert.equal(stopped.stopped, true); assert.equal(peak, 2); assert.equal(ids.size, 2);
  assert.equal(stopped.claimed, 2); assert.equal(stopped.completed, 2); assert.equal(stopped.active, 0);
});
test('Idle loop polls with delay and restart is protected', async () => {
  let calls = 0;
  const scheduler = createDurableFetchScheduler({ pollIntervalMs: 40, workerFactory: () => ({ async runOnce() { calls++; return { status: 'idle' }; } }) });
  scheduler.start(); assert.throws(() => scheduler.start(), /SCHEDULER_ALREADY_RUNNING/);
  await wait(100); await scheduler.stop(); assert.ok(calls >= 2 && calls <= 4);
});
