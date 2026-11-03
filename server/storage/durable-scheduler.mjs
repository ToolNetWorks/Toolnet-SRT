import { randomUUID } from "node:crypto";

function validateInteger(value, min, max, name) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`INVALID_${name}`);
}

function delay(ms, signal) {
  if (signal.aborted) return Promise.resolve();
  return new Promise(resolve => {
    let timer;
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
    if (signal.aborted) finish();
  });
}

export function createDurableScheduler({ workerFactory, maxConcurrency = 1, pollIntervalMs = 3000,
  errorBackoffMs = 10000, workerIdPrefix = "toolnet-srt", logger = console } = {}) {
  if (typeof workerFactory !== "function") throw new Error("WORKER_FACTORY_REQUIRED");
  validateInteger(maxConcurrency, 1, 32, "MAX_CONCURRENCY");
  validateInteger(pollIntervalMs, 10, 60000, "POLL_INTERVAL");
  validateInteger(errorBackoffMs, 10, 120000, "ERROR_BACKOFF");
  if (typeof workerIdPrefix !== "string" || !/^[a-zA-Z0-9._:-]{1,60}$/.test(workerIdPrefix)) {
    throw new Error("INVALID_WORKER_ID_PREFIX");
  }

  let state = "stopped", controller = null, runPromise = null, activeWorkers = 0;
  let counters = { polls: 0, completed: 0, leaseLost: 0, emptyPolls: 0, otherResults: 0, errors: 0 };
  function getStatus() { return { state, maxConcurrency, activeWorkers, ...counters }; }

  async function workerLoop(worker, workerId, signal) {
    while (!signal.aborted) {
      let result = null, failed = false;
      activeWorkers += 1;
      counters.polls += 1;
      try {
        result = await worker.runOnce();
        if (result?.status === "completed") counters.completed += 1;
        else if (result?.status === "lease_lost") counters.leaseLost += 1;
        else if (result == null || result.status === "idle" || result.status === "no_job") counters.emptyPolls += 1;
        else counters.otherResults += 1;
      } catch (error) {
        failed = true;
        counters.errors += 1;
        try { logger?.error?.(`[durable-scheduler] ${workerId}: ` + (error instanceof Error ? error.message : String(error))); }
        catch { /* Logging failure must not stop the scheduler. */ }
      } finally {
        activeWorkers -= 1;
      }
      if (signal.aborted) break;
      if (failed) { await delay(errorBackoffMs, signal); continue; }
      if (result?.status === "completed") { await delay(0, signal); continue; }
      await delay(pollIntervalMs, signal);
    }
  }

  function start() {
    if (state !== "stopped") throw new Error("SCHEDULER_ALREADY_RUNNING");
    // Construct all workers before changing state.
    const workers = [], instances = new Set();
    for (let index = 0; index < maxConcurrency; index++) {
      const workerId = `${workerIdPrefix}-${process.pid}-${randomUUID()}`;
      const worker = workerFactory({ index, workerId });
      if (!worker || typeof worker.runOnce !== "function") throw new Error("INVALID_DURABLE_WORKER");
      if (instances.has(worker)) throw new Error("DUPLICATE_WORKER_INSTANCE");
      instances.add(worker);
      workers.push({ worker, workerId });
    }
    counters = { polls: 0, completed: 0, leaseLost: 0, emptyPolls: 0, otherResults: 0, errors: 0 };
    controller = new AbortController();
    state = "running";
    const signal = controller.signal;
    runPromise = Promise.all(workers.map(({ worker, workerId }) => workerLoop(worker, workerId, signal)))
      .finally(() => { state = "stopped"; controller = null; runPromise = null; });
    return getStatus();
  }

  async function stop({ timeoutMs = 30000 } = {}) {
    validateInteger(timeoutMs, 0, 300000, "STOP_TIMEOUT");
    if (state === "stopped") return { stopped: true, ...getStatus() };
    state = "stopping";
    controller.abort();
    // Allow an in-flight runOnce to finish naturally.
    if (timeoutMs === 0) return { ...getStatus(), stopped: false };
    const currentRun = runPromise;
    let timer;
    const stopped = await Promise.race([
      currentRun.then(() => true),
      new Promise(resolve => { timer = setTimeout(() => resolve(false), timeoutMs); }),
    ]);
    clearTimeout(timer);
    return { ...getStatus(), stopped };
  }

  return { start, stop, getStatus };
}
