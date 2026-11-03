// A slot is held only during one ASR invocation. No database transaction
// remains open while the provider is contacted.
export async function runWithAsrSlot({ semaphore, owner, signal, ensureOwnership, transcribe,
  heartbeatMs = 20000, waitMs = 500, requestTimeoutMs = 300000, dbOperationTimeoutMs = 10000, onWait = null, onRateLimit = null } = {}) {
  if (!['acquire','heartbeat','release','rateLimited'].every(name => typeof semaphore?.[name] === 'function')) throw new Error('ASR_SEMAPHORE_REQUIRED');
  if (typeof ensureOwnership !== 'function' || typeof transcribe !== 'function') throw new Error('INVALID_ASR_SLOT_DEPENDENCY');
  if (onRateLimit !== null && typeof onRateLimit !== 'function') throw new Error('INVALID_ASR_RATE_LIMIT_CALLBACK');
  if (!Number.isSafeInteger(heartbeatMs) || heartbeatMs < 1000 || heartbeatMs > 300000) throw new Error('INVALID_ASR_SLOT_HEARTBEAT');
  if (Number.isSafeInteger(semaphore.leaseSeconds) && heartbeatMs >= semaphore.leaseSeconds * 500) throw new Error('INVALID_ASR_SLOT_HEARTBEAT');
  if (!Number.isSafeInteger(waitMs) || waitMs < 10 || waitMs > 60000) throw new Error('INVALID_ASR_SLOT_WAIT');
  if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs < 1000 || requestTimeoutMs > 3600000) throw new Error('INVALID_ASR_REQUEST_TIMEOUT');
  if (!Number.isSafeInteger(dbOperationTimeoutMs) || dbOperationTimeoutMs < 10 || dbOperationTimeoutMs > 60000 ||
      (Number.isSafeInteger(semaphore.leaseSeconds) && heartbeatMs + dbOperationTimeoutMs >= semaphore.leaseSeconds * 1000)) throw new Error('INVALID_ASR_DB_TIMEOUT');
  // A stalled TCP connection must not keep an ASR invocation alive forever.
  // A late database operation may still commit, so unknown slots are retained.
  const dbCall = async operation => {
    let timeout;
    try { return await Promise.race([Promise.resolve().then(operation),new Promise((_,reject)=>{
      timeout=setTimeout(()=>reject(new Error('ASR_DATABASE_TIMEOUT')),dbOperationTimeoutMs);
    })]); } finally { clearTimeout(timeout); }
  };
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal?.addEventListener('abort', abort, { once: true });
  if (signal?.aborted) abort();
  let slot, timer, deadlineTimer, heartbeat = Promise.resolve(), leaseLost = false, timedOut = false, safeToRelease = true;
  try {
    const started = performance.now();
    while (!slot) {
      if (controller.signal.aborted) throw new Error('PROCESSING_ABORTED');
      await dbCall(ensureOwnership);
      slot = await dbCall(() => semaphore.acquire(owner));
      if (slot) break;
      await new Promise((resolve,reject) => {
        if (controller.signal.aborted) return reject(new Error('PROCESSING_ABORTED'));
        const done=()=>{clearTimeout(id);controller.signal.removeEventListener('abort',stop);resolve();};
        const stop=()=>{clearTimeout(id);controller.signal.removeEventListener('abort',stop);reject(new Error('PROCESSING_ABORTED'));};
        const id=setTimeout(done,waitMs+Math.floor(Math.random()*waitMs));
        controller.signal.addEventListener('abort',stop,{once:true});
        if (controller.signal.aborted) stop();
      });
    }
    try { const result=onWait?.(performance.now()-started); if(result?.then) Promise.resolve(result).catch(()=>{}); } catch {}
    await dbCall(ensureOwnership);
    if (!await dbCall(() => semaphore.heartbeat(slot))) throw new Error('ASR_SLOT_LOST');
    if (controller.signal.aborted) throw new Error('PROCESSING_ABORTED');
    timer = setInterval(() => {
      heartbeat = heartbeat.then(async () => {
        if (leaseLost || controller.signal.aborted) return;
        if (!await dbCall(() => semaphore.heartbeat(slot))) throw new Error('ASR_SLOT_LOST');
      }).catch(() => { leaseLost = true; controller.abort(); });
    }, heartbeatMs);
    timer.unref?.();
    let result;
    try {
      // Once a request is dispatched, an abort or broken connection does not
      // prove that the provider stopped. Retain the slot on uncertain outcome.
      safeToRelease = false;
      const aborted = new Promise((_,reject) => {
        const stop=()=>reject(new Error('PROCESSING_ABORTED'));
        controller.signal.addEventListener('abort',stop,{once:true});
        if(controller.signal.aborted) stop();
      });
      deadlineTimer=setTimeout(()=>{timedOut=true;controller.abort();},requestTimeoutMs);
      deadlineTimer.unref?.();
      result = await Promise.race([Promise.resolve().then(()=>transcribe(controller.signal)),aborted]);
      safeToRelease = true;
    } catch (error) {
      if (error?.status === 429) {
        // Fence sibling calls locally before attempting the durable cooldown.
        try { onRateLimit?.(); } catch {}
        // On an unknown database outcome, keep the slot until its TTL.
        const retry = Number.isSafeInteger(error.retryAfterMs) && error.retryAfterMs >= 0
          ? error.retryAfterMs : 30000;
        await dbCall(() => semaphore.rateLimited(slot,retry));
      }
      if (leaseLost) throw new Error('ASR_SLOT_LOST');
      if (timedOut) throw new Error('ASR_REQUEST_TIMEOUT');
      throw error;
    }
    if(deadlineTimer) { clearTimeout(deadlineTimer); deadlineTimer=null; }
    clearInterval(timer); timer = null;
    await heartbeat;
    if (leaseLost || controller.signal.aborted) throw new Error('ASR_SLOT_LOST');
    await dbCall(ensureOwnership);
    if (!await dbCall(() => semaphore.heartbeat(slot))) throw new Error('ASR_SLOT_LOST');
    if (controller.signal.aborted) throw new Error('PROCESSING_ABORTED');
    return result;
  } finally {
    if (timer) clearInterval(timer);
    if (deadlineTimer) clearTimeout(deadlineTimer);
    controller.abort();
    await heartbeat;
    if (slot && safeToRelease) await dbCall(() => semaphore.release(slot)).catch(() => {});
    signal?.removeEventListener('abort', abort);
  }
}
