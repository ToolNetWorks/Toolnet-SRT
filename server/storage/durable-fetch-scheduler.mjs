import { randomUUID } from 'node:crypto';

export function createDurableFetchScheduler({ workerFactory, maxConcurrency = 1, pollIntervalMs = 3000, errorBackoffMs = 10000,
  orphanCleanup = null, cleanupIntervalMs = 3600000 } = {}) {
  if (typeof workerFactory !== 'function') throw new Error('WORKER_FACTORY_REQUIRED');
  for (const [v, min, max, name] of [[maxConcurrency, 1, 32, 'MAX_CONCURRENCY'], [pollIntervalMs, 10, 60000, 'POLL_INTERVAL'], [errorBackoffMs, 10, 120000, 'ERROR_BACKOFF']]) {
    if (!Number.isSafeInteger(v) || v < min || v > max) throw new Error(`INVALID_${name}`);
  }
  if (orphanCleanup !== null && typeof orphanCleanup?.sweep !== 'function') throw new Error('INVALID_ORPHAN_CLEANUP');
  if (!Number.isSafeInteger(cleanupIntervalMs) || cleanupIntervalMs < 60000) throw new Error('INVALID_CLEANUP_INTERVAL');
  let state = 'stopped', controller, running, cleanupTimer, cleanupTask, continuationToken;
  let counters = { claimed: 0, completed: 0, retried: 0, failed: 0, active: 0, errors: 0 };
  const getStatus = () => ({ state, maxConcurrency, ...counters });
  function delay(ms, signal) {
    return new Promise(resolve => {
      if (signal.aborted) return resolve();
      const finish = () => { clearTimeout(timer); signal.removeEventListener('abort', finish); resolve(); };
      const timer = setTimeout(finish, ms);
      signal.addEventListener('abort', finish, { once: true });
      if (signal.aborted) finish();
    });
  }
  async function loop(worker, signal) {
    while (!signal.aborted) {
      let result, error = false;
      let claimed = false;
      try {
        result = await worker.runOnce({ signal, onClaim() { claimed = true; counters.claimed++; counters.active++; } });
        if (result?.claimed && !claimed) counters.claimed++;
        if (['completed', 'retried', 'failed'].includes(result?.status)) counters[result.status]++;
      } catch { counters.errors++; error = true; }
      finally { if (claimed) counters.active--; }
      if (!signal.aborted) await delay(error ? errorBackoffMs : result?.claimed ? 0 : pollIntervalMs, signal);
    }
  }
  function start() {
    if (state !== 'stopped') throw new Error('SCHEDULER_ALREADY_RUNNING');
    const instances = new Set();
    const workers = Array.from({ length: maxConcurrency }, (_, index) => {
      const worker = workerFactory({ index, workerId: `fetch-${process.pid}-${randomUUID()}` });
      if (!worker || typeof worker.runOnce !== 'function') throw new Error('INVALID_DURABLE_WORKER');
      if (instances.has(worker)) throw new Error('DUPLICATE_WORKER_INSTANCE');
      instances.add(worker); return worker;
    });
    counters = { claimed: 0, completed: 0, retried: 0, failed: 0, active: 0, errors: 0 };
    controller = new AbortController(); state = 'running';
    running = Promise.all(workers.map(worker => loop(worker, controller.signal)))
      .then(async () => { if (cleanupTask) await cleanupTask; })
      .finally(() => { state = 'stopped'; });
    if (orphanCleanup) cleanupTimer = setInterval(() => {
      if (cleanupTask) return;
      cleanupTask = orphanCleanup.sweep({ continuationToken }).then(result => { continuationToken = result.continuation ?? undefined; })
        .catch(() => { counters.errors++; }).finally(() => { cleanupTask = null; });
    }, cleanupIntervalMs);
    return getStatus();
  }
  async function stop({ timeoutMs = 30000 } = {}) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 300000) throw new Error('INVALID_STOP_TIMEOUT');
    if (state === 'stopped') return { stopped: true, ...getStatus() };
    state = 'stopping'; clearInterval(cleanupTimer); controller.abort();
    if (!timeoutMs) return { stopped: false, ...getStatus() };
    let timer;
    const stopped = await Promise.race([Promise.all([running, cleanupTask]).then(() => true), new Promise(resolve => { timer = setTimeout(() => resolve(false), timeoutMs); })]);
    clearTimeout(timer);
    return { stopped, ...getStatus() };
  }
  return { start, stop, getStatus };
}
