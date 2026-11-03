import { readDurableConfig } from "./durable-config.mjs";
import { createDurablePreflight } from "./durable-preflight.mjs";
import { createDurableLifecycle } from "./durable-lifecycle.mjs";

export function createDurableRuntime({
  env = process.env,
  assemble,
  preflightFactory = options => createDurablePreflight(options),
  lifecycleFactory = options => createDurableLifecycle(options),
  processRef = process,
  beforeClose = async () => {},
} = {}) {
  let state = "idle";
  let enabled = false;
  let components = null;
  let lifecycle = null;
  let closePromise = null;
  let shutdownPromise = null;
  let managedScheduler = null;
  let startupDrainPending = false;

  function getStatus() {
    return { state, enabled, managerReady: state === "running", scheduler: components?.scheduler?.getStatus() ?? null,
      fetchScheduler: components?.fetchScheduler?.getStatus() ?? null };
  }

  function closeOnce() {
    if (!components || typeof components.close !== "function") return Promise.resolve();
    if (!closePromise) {
      let closing = false;
      closePromise = Promise.resolve().then(() => beforeClose()).then(() => { closing = true; return components.close(); })
        .catch(error => { if (!closing) closePromise = null; throw error; });
    }
    return closePromise;
  }

  function getManager() {
    if (state !== "running" || !components) throw new Error("DURABLE_RUNTIME_NOT_READY");
    return components.manager;
  }

  async function start() {
    if (state !== "idle") throw new Error("DURABLE_RUNTIME_ALREADY_STARTED");
    state = "starting";
    try {
      const config = readDurableConfig(env);
      enabled = config.enabled;
      if (!config.enabled) { state = "disabled"; return getStatus(); }
      if (typeof assemble !== "function") throw new Error("DURABLE_ASSEMBLER_REQUIRED");
      const preflight = preflightFactory({ env });
      if (typeof preflight?.check !== "function") throw new Error("INVALID_DURABLE_PREFLIGHT");
      const readiness = await preflight.check();
      if (readiness?.enabled !== true || readiness.neon !== "ready" || readiness.r2 !== "ready" ||
          readiness.workerStarted !== false) throw new Error("DURABLE_PREFLIGHT_NOT_READY");

      components = await assemble({ env, config });
      if (!components || typeof components.manager?.createUpload !== "function" ||
          typeof components.manager?.getJob !== "function" || typeof components.manager?.getResult !== "function" ||
          typeof components.manager?.cancelJob !== "function" || typeof components.scheduler?.start !== "function" ||
          typeof components.scheduler?.stop !== "function" || typeof components.scheduler?.getStatus !== "function" ||
          typeof components.close !== "function") throw new Error("INVALID_DURABLE_COMPONENTS");

      const fetchScheduler = components.fetchScheduler;
      if (fetchScheduler && !['start', 'stop', 'getStatus'].every(name => typeof fetchScheduler[name] === 'function')) {
        throw new Error('INVALID_FETCH_SCHEDULER');
      }
      managedScheduler = !config.runWorkers ? {
        start() {},
        async stop() { return { stopped: true }; },
        getStatus() { return { state: 'stopped' }; },
      } : fetchScheduler ? {
        start() { fetchScheduler.start(); return components.scheduler.start(); },
        async stop(options) {
          const [fetch, asr] = await Promise.all([fetchScheduler.stop(options), components.scheduler.stop(options)]);
          return { stopped: fetch.stopped && asr.stopped, fetch, asr };
        },
        getStatus() { return { fetch: fetchScheduler.getStatus(), asr: components.scheduler.getStatus() }; },
      } : components.scheduler;
      lifecycle = lifecycleFactory({
        scheduler: managedScheduler,
        gracePeriodMs: config.shutdownGraceMs,
        processRef,
        onDrained: async () => { await closeOnce(); state = "stopped"; },
        onTimeout: async () => { state = "timed_out"; },
      });
      if (typeof lifecycle?.start !== "function" || typeof lifecycle?.shutdown !== "function") {
        throw new Error("INVALID_DURABLE_LIFECYCLE");
      }
      lifecycle.start();
      state = "running";
      return getStatus();
    } catch (error) {
      state = "failed";
      if (managedScheduler && components?.fetchScheduler) {
        const result = await managedScheduler.stop({ timeoutMs: configGrace(env) }).catch(() => ({ stopped: false }));
        if (!result.stopped) {
          startupDrainPending = true;
          throw new AggregateError([error], "DURABLE_START_DRAIN_INCOMPLETE");
        }
      }
      try { await closeOnce(); }
      catch (cleanupError) { throw new AggregateError([error, cleanupError], "DURABLE_START_AND_CLEANUP_FAILED"); }
      throw error;
    }
  }

  function shutdown() {
    if (state === "starting") return Promise.reject(new Error("DURABLE_START_IN_PROGRESS"));
    if (startupDrainPending) {
      if (shutdownPromise) return shutdownPromise;
      state = 'stopping';
      shutdownPromise = managedScheduler.stop({ timeoutMs: configGrace(env) }).then(async result => {
        if (!result.stopped) { state = 'timed_out'; return { stopped: false, state }; }
        await closeOnce(); startupDrainPending = false; state = 'stopped'; return { stopped: true, state };
      }).catch(error => { state = 'failed'; throw error; }).finally(() => { shutdownPromise = null; });
      return shutdownPromise;
    }
    if (state === "idle" || state === "disabled" || state === "stopped" || (state === "failed" && !lifecycle)) {
      return Promise.resolve({ stopped: true, state });
    }
    if (shutdownPromise) return shutdownPromise;
    if (!lifecycle) return Promise.reject(new Error("DURABLE_LIFECYCLE_NOT_READY"));
    state = "stopping";
    const task = (async () => {
      const result = await lifecycle.shutdown("manual");
      if (!result.stopped) { state = "timed_out"; return { stopped: false, state }; }
      await closeOnce();
      state = "stopped";
      return { stopped: true, state };
    })();
    shutdownPromise = task.catch(error => { state = "failed"; throw error; })
      .finally(() => { shutdownPromise = null; });
    return shutdownPromise;
  }

  return { start, shutdown, getManager, getStatus };
}

function configGrace(env) {
  const value = Number(env.DURABLE_SHUTDOWN_GRACE_MS ?? 30000);
  return Number.isSafeInteger(value) && value >= 1 && value <= 300000 ? value : 30000;
}
