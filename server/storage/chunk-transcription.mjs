import { join } from 'node:path';
import { rm } from 'node:fs/promises';
import { planChunkMedia } from './chunk-media-plan.mjs';
import { extractTimelineChunk } from './chunk-extraction.mjs';
import { mergeChunkSegments } from './chunk-timestamps.mjs';

async function retryDelay(ms, signal) {
  while (ms > 2147483647) {
    await retryDelay(2147483647, signal);
    ms -= 2147483647;
  }
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new Error('PROCESSING_ABORTED'));
    const done = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); resolve(); };
    const abort = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); reject(new Error('PROCESSING_ABORTED')); };
    const timer = setTimeout(done, ms);
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
  });
}

// This service is injected into Durable Worker. Each persisted checkpoint is
// fenced by the parent job lease; completed chunks are never re-transcribed.
export async function transcribeCheckpointed({ input, directory, lease, checkpoints, transcribe, signal,
  chunkSeconds = 600, overlapSeconds = 0, chunkMaxAttempts = 3, chunkConcurrency = 1, seekPrerollMs = null, ensureOwnership, onProgress,
  plan = planChunkMedia, extract = extractTimelineChunk, retryWait = retryDelay, onTiming = null, reserveChunkSpace = null } = {}) {
  if (!checkpoints || !['initialize','claim','complete','fail'].every(name => typeof checkpoints[name] === 'function')) throw new Error('CHUNK_CHECKPOINTS_REQUIRED');
  if (typeof transcribe !== 'function' || typeof ensureOwnership !== 'function' || typeof onProgress !== 'function') throw new Error('INVALID_CHUNK_DEPENDENCY');
  if (typeof retryWait !== 'function') throw new Error('INVALID_CHUNK_RETRY_WAIT');
  if (onTiming !== null && typeof onTiming !== 'function') throw new Error('INVALID_TIMING_CALLBACK');
  if (reserveChunkSpace !== null && typeof reserveChunkSpace !== 'function') throw new Error('INVALID_CHUNK_DISK_RESERVATION');
  if (!Number.isSafeInteger(chunkMaxAttempts) || chunkMaxAttempts < 1 || chunkMaxAttempts > 20) throw new Error('INVALID_CHUNK_MAX_ATTEMPTS');
  if (![1,2].includes(chunkConcurrency)) throw new Error('INVALID_CHUNK_CONCURRENCY');
  const measure = async (phase, operation, chunkIndex = null) => {
    const started = performance.now();
    let outcome = 'ok', httpStatus = null, errorCode = null;
    try { return await operation(); }
    catch (error) { outcome = 'error'; if (Number.isInteger(error?.status) && error.status >= 100 && error.status <= 599) httpStatus = error.status;
      const safeMessages = new Set(['ASR_SLOT_LOST','ASR_REQUEST_TIMEOUT','ASR_JOB_LEASE_LOST','ASR_GLOBAL_LIMIT_MISMATCH','LEASE_LOST','PROCESSING_ABORTED','INVALID_ASR_RESULT']);
      if (safeMessages.has(error?.message)) errorCode = error.message;
      else if (typeof error?.code === 'string' && /^(?:[0-9]{2}[A-Z0-9]{3}|E[A-Z]{2,12})$/.test(error.code)) errorCode = error.code;
      throw error; }
    finally { try { const result = onTiming?.({ phase, chunkIndex, outcome, httpStatus, errorCode, durationMs: performance.now() - started });
      if (result?.then) Promise.resolve(result).catch(() => {}); } catch {} }
  };
  const timeline = await measure('probe_and_plan', () => plan(input, { chunkSeconds, overlapSeconds, signal }));
  const maximumChunkMs = Math.max(...timeline.chunks.map(chunk=>chunk.durationMs));
  const chunkBytes = chunkConcurrency * (Math.ceil(maximumChunkMs * 32) + 65536);
  if (!Number.isSafeInteger(chunkBytes) || chunkBytes < 1) throw new Error('INVALID_CHUNK_DISK_RESERVATION');
  const releaseChunkSpace = reserveChunkSpace ? await reserveChunkSpace(chunkBytes) : null;
  let cleanupFailed = false;
  try {
  await ensureOwnership();
  const rows = await measure('checkpoint_initialize', () => checkpoints.initialize(lease, timeline.chunks));
  if (!rows) throw new Error('LEASE_LOST');
  let completed = rows.filter(row => row.status === 'completed').length;
  await onProgress(completed, rows.length, 10);
  const chunks = new Array(rows.length);
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal?.addEventListener('abort', abort, { once:true });
  if (signal?.aborted) abort();
  const activeSignal = controller.signal;
  let next = 0, failure = null, progressTail = Promise.resolve();
  async function processRow(index) {
    const row=rows[index];
    if (activeSignal.aborted) throw new Error('PROCESSING_ABORTED');
    let segments = row.segments;
    if (row.status !== 'completed') {
      if (row.attempt_count >= chunkMaxAttempts) throw new Error('CHUNK_RETRY_EXHAUSTED');
      for (;;) {
        await ensureOwnership();
        const claimed = await measure('checkpoint_claim', () => checkpoints.claim(lease,index,{ maxAttempts: chunkMaxAttempts }), index);
        if (!claimed) throw new Error('LEASE_LOST');
        const path = join(directory, `chunk-${String(index).padStart(5,'0')}.wav`);
        let recognized;
        try {
          await measure('extract', () => extract(input,path,{ formatStartMs:timeline.formatStartMs, chunk:timeline.chunks[index], signal:activeSignal, seekPrerollMs }), index);
          recognized = await measure('asr', () => transcribe(path,activeSignal,{ chunkIndex:index }), index);
          if (!Array.isArray(recognized)) throw new Error('INVALID_ASR_RESULT');
        } catch (error) {
          if (activeSignal.aborted || error?.message === 'ASR_SLOT_LOST') throw error;
          const failed = await measure('checkpoint_fail', () => checkpoints.fail(lease,index,'CHUNK_ASR_FAILED'), index);
          if (!failed) throw new Error('LEASE_LOST');
          if (claimed.attempt_count >= chunkMaxAttempts) throw error;
          const base = Math.min(30000, 1000 * 2 ** Math.min(claimed.attempt_count - 1, 5));
          const retryAfter = error?.status === 429 && Number.isSafeInteger(error.retryAfterMs) ? Math.max(0, error.retryAfterMs) : 0;
          await measure('retry_wait', () => retryWait(Math.max(retryAfter, base + Math.floor(Math.random() * base)), activeSignal), index);
          continue;
        } finally { await rm(path,{force:true}).catch(() => { cleanupFailed=true; }); }
        if (activeSignal.aborted) throw new Error('PROCESSING_ABORTED');
        await ensureOwnership();
        if (activeSignal.aborted) throw new Error('PROCESSING_ABORTED');
        const saved = await measure('checkpoint_complete', () => checkpoints.complete(lease,index,recognized), index);
        if (!saved) throw new Error('LEASE_LOST');
        segments = saved.segments;
        const done=++completed;
        progressTail=progressTail.then(()=>onProgress(done,rows.length,10+Math.floor(85*done/rows.length)));
        await progressTail;
        break;
      }
    }
    chunks[index]={ ...timeline.chunks[index], segments };
  }
  async function run() {
    while (!activeSignal.aborted) {
      const index=next++;
      if(index>=rows.length)return;
      try { await processRow(index); }
      catch(error) { if(!failure) failure=error;controller.abort();return; }
    }
  }
  try {
    await Promise.all(Array.from({length:Math.min(chunkConcurrency,rows.length)},()=>run()));
    if(failure)throw failure;
    if(activeSignal.aborted)throw new Error('PROCESSING_ABORTED');
  } finally { signal?.removeEventListener('abort',abort); }
  // If all chunks were already complete, progress still advances to 95%.
  await onProgress(completed,rows.length,95);
  return measure('merge', () => mergeChunkSegments(chunks,timeline.durationMs));
  } finally { if (!cleanupFailed) releaseChunkSpace?.(); }
}
