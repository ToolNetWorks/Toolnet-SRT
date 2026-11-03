const ALLOWED_SIGNALS = new Set(["SIGTERM", "SIGINT"]);

export function createDurableLifecycle({ scheduler, gracePeriodMs = 30000, signals = ["SIGTERM", "SIGINT"],
  processRef = process, onDrained = async () => {}, onTimeout = async () => {}, logger = console } = {}) {
  if (typeof scheduler?.start !== "function" || typeof scheduler?.stop !== "function" ||
      typeof scheduler?.getStatus !== "function") throw new Error("SCHEDULER_REQUIRED");
  if (!Number.isSafeInteger(gracePeriodMs) || gracePeriodMs < 1 || gracePeriodMs > 300000) {
    throw new Error("INVALID_GRACE_PERIOD");
  }
  if (!Array.isArray(signals) || signals.length === 0 || new Set(signals).size !== signals.length ||
      signals.some(signal => !ALLOWED_SIGNALS.has(signal))) throw new Error("INVALID_SIGNALS");
  if (typeof processRef?.on !== "function" || typeof processRef?.removeListener !== "function") {
    throw new Error("INVALID_PROCESS_REF");
  }
  if (typeof onDrained !== "function" || typeof onTimeout !== "function") throw new Error("INVALID_LIFECYCLE_CALLBACK");

  let state = "idle", stopPromise = null, listenersAttached = false;
  const handlers = new Map();
  function detachSignals() {
    if (!listenersAttached) return;
    for (const [signal, handler] of handlers) processRef.removeListener(signal, handler);
    listenersAttached = false;
  }
  function getStatus() { return { state, scheduler: scheduler.getStatus() }; }
  function shutdown(reason = "manual") {
    if (state === "idle" || state === "stopped") return Promise.resolve({ stopped: true, reason, state });
    if (stopPromise) return stopPromise;
    state = "stopping";
    const task = (async () => {
      try {
        const result = await scheduler.stop({ timeoutMs: gracePeriodMs });
        if (!result.stopped) {
          state = "timed_out";
          await onTimeout({ reason, scheduler: result });
          // Do not close resources or force exit while a worker may be active.
          return { stopped: false, reason, state };
        }
        state = "stopped";
        detachSignals();
        await onDrained({ reason, scheduler: result });
        return { stopped: true, reason, state };
      } catch (error) {
        state = "error";
        throw error;
      }
    })();
    stopPromise = task.finally(() => {
      if (state === "timed_out" || state === "error") stopPromise = null;
    });
    return stopPromise;
  }
  for (const signal of signals) {
    handlers.set(signal, () => {
      void shutdown(signal).catch(error => {
        try { logger?.error?.("[durable-lifecycle] shutdown failed:", error); }
        catch { /* Preserve the failure even if logging fails. */ }
        processRef.exitCode = 1;
      });
    });
  }
  function start() {
    if (state !== "idle") throw new Error("LIFECYCLE_ALREADY_STARTED");
    for (const [signal, handler] of handlers) processRef.on(signal, handler);
    listenersAttached = true;
    try {
      const result = scheduler.start();
      state = "running";
      return result;
    } catch (error) {
      detachSignals();
      throw error;
    }
  }
  return { start, shutdown, getStatus };
}
