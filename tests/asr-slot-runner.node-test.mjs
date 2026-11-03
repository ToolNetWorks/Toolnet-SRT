import test from 'node:test';
import assert from 'node:assert/strict';
import { runWithAsrSlot } from '../server/storage/asr-slot-runner.mjs';

const owner={jobId:'test',chunkIndex:0};
function harness(){const calls=[];return {calls,semaphore:{
  async acquire(){calls.push('acquire');return {...owner,token:'slot'};},
  async heartbeat(){calls.push('heartbeat');return true;},
  async release(){calls.push('release');return true;},
  async rateLimited(_slot,ms){calls.push(`cooldown:${ms}`);return true;},
}};}
test('Slot surrounds the ASR request and result is fenced',async()=>{
  const h=harness();const value=await runWithAsrSlot({...h,owner,ensureOwnership:async()=>h.calls.push('owner'),transcribe:async()=>{h.calls.push('asr');return 'done';}});
  assert.equal(value,'done');assert.deepEqual(h.calls,['owner','acquire','owner','heartbeat','asr','owner','heartbeat','release']);
});
test('429 persists cooldown and retains the uncertain slot',async()=>{
  const h=harness();await assert.rejects(runWithAsrSlot({...h,owner,onRateLimit:()=>h.calls.push('stop-siblings'),ensureOwnership:async()=>{},transcribe:async()=>{throw Object.assign(new Error('limited'),{status:429,retryAfterMs:5000});}}),/limited/);
  assert.ok(h.calls.indexOf('stop-siblings')<h.calls.indexOf('cooldown:5000'));assert.ok(!h.calls.includes('release'));
});
test('Unknown cooldown commit retains the slot for TTL recovery',async()=>{
  const h=harness();h.semaphore.rateLimited=async()=>{throw Error('db unavailable');};
  await assert.rejects(runWithAsrSlot({...h,owner,ensureOwnership:async()=>{},transcribe:async()=>{throw Object.assign(new Error('limited'),{status:429});}}),/db unavailable/);
  assert.ok(!h.calls.includes('release'));
});
test('Cancellation stops a waiting slot and never invokes ASR',async()=>{
  const h=harness();let called=false;h.semaphore.acquire=async()=>null;const controller=new AbortController();
  const work=runWithAsrSlot({...h,owner,signal:controller.signal,waitMs:10,ensureOwnership:async()=>{},transcribe:async()=>{called=true;}});
  controller.abort();await assert.rejects(work,/PROCESSING_ABORTED/);assert.equal(called,false);
});
test('A lost slot rejects a result before publication',async()=>{
  const h=harness();let heartbeats=0;h.semaphore.heartbeat=async()=>++heartbeats===1;
  await assert.rejects(runWithAsrSlot({...h,owner,ensureOwnership:async()=>{},transcribe:async()=>['result']}),/ASR_SLOT_LOST/);
  assert.ok(h.calls.includes('release'));
});
test('Heartbeat must be less than half the slot lease',async()=>{
  const h=harness();h.semaphore.leaseSeconds=30;
  await assert.rejects(runWithAsrSlot({...h,owner,heartbeatMs:15000,ensureOwnership:async()=>{},transcribe:async()=>{}}),/INVALID_ASR_SLOT_HEARTBEAT/);
});
test('Heartbeat failure aborts an in-flight ASR and rejects its result',async()=>{
  const h=harness();let beats=0;h.semaphore.heartbeat=async()=>++beats===1;
  let aborted=false;
  const request=runWithAsrSlot({...h,owner,heartbeatMs:1000,ensureOwnership:async()=>{},transcribe:signal=>new Promise((resolve,reject)=>{
    signal.addEventListener('abort',()=>{aborted=true;reject(Error('aborted'));},{once:true});
  })});
  const keepAlive=setTimeout(()=>{},2000);
  try { await assert.rejects(request,/ASR_SLOT_LOST/);assert.equal(aborted,true);assert.ok(!h.calls.includes('release')); }
  finally { clearTimeout(keepAlive); }
});
test('Timeout fences an ASR call even if it ignores cancellation',async()=>{
  const h=harness();const keepAlive=setTimeout(()=>{},2000);
  try { await assert.rejects(runWithAsrSlot({...h,owner,requestTimeoutMs:1000,ensureOwnership:async()=>{},
    transcribe:async()=>new Promise(()=>{})}),/ASR_REQUEST_TIMEOUT/);assert.ok(!h.calls.includes('release')); }
  finally { clearTimeout(keepAlive); }
});
test('A stalled Neon heartbeat aborts the provider request and retains its slot',async()=>{
  const h=harness();let beats=0,aborted=false;h.semaphore.heartbeat=async()=>++beats===1?true:new Promise(()=>{});
  const keepAlive=setTimeout(()=>{},2500);
  try {
    const request=runWithAsrSlot({...h,owner,heartbeatMs:1000,dbOperationTimeoutMs:25,ensureOwnership:async()=>{},
      transcribe:signal=>new Promise((_,reject)=>signal.addEventListener('abort',()=>{aborted=true;reject(Error('cancelled'));},{once:true}))});
    await assert.rejects(request,/ASR_SLOT_LOST/);assert.equal(aborted,true);assert.ok(!h.calls.includes('release'));
  } finally { clearTimeout(keepAlive); }
});
test('An unknown acquire outcome never starts ASR or releases an unknown slot',async()=>{
  const h=harness();let called=false;h.semaphore.acquire=async()=>new Promise(()=>{});
  await assert.rejects(runWithAsrSlot({...h,owner,dbOperationTimeoutMs:20,ensureOwnership:async()=>{},transcribe:async()=>{called=true;}}),/ASR_DATABASE_TIMEOUT/);
  assert.equal(called,false);assert.ok(!h.calls.includes('release'));
});
