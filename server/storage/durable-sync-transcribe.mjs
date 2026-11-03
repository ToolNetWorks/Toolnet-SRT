// Preserve the synchronous SRT response while the work itself passes through
// the durable manager and its distributed ASR gate.
export async function runDurableSynchronous({ manager, source, mimeType, signal, pollIntervalMs = 250 } = {}) {
  if (!['createUpload','getJob','getResult','cancelJob'].every(name => typeof manager?.[name] === 'function')) throw new Error('DURABLE_MANAGER_REQUIRED');
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs < 10 || pollIntervalMs > 60000) throw new Error('INVALID_POLL_INTERVAL');
  let jobId;
  const assertActive=()=>{if(signal?.aborted)throw new Error('PROCESSING_ABORTED');};
  const wait=()=>new Promise((resolve,reject)=>{
    if(signal?.aborted)return reject(new Error('PROCESSING_ABORTED'));
    const done=()=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);resolve();};
    const abort=()=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);reject(new Error('PROCESSING_ABORTED'));};
    const timer=setTimeout(done,pollIntervalMs);signal?.addEventListener('abort',abort,{once:true});
    if(signal?.aborted)abort();
  });
  try {
    assertActive();
    const created=await manager.createUpload(source,mimeType);
    jobId=created?.id;
    if(typeof jobId!=='string')throw new Error('INVALID_QUEUED_JOB');
    for (;;) {
      assertActive();
      const job=await manager.getJob(jobId);
      assertActive();
      if(job?.status==='completed') { const result=await manager.getResult(jobId);assertActive();return result; }
      if(job?.status==='failed'||job?.status==='cancelled')throw new Error('TRANSCRIPTION_FAILED');
      await wait();
    }
  } finally {
    if(signal?.aborted && jobId) await manager.cancelJob(jobId).catch(()=>{});
  }
}
