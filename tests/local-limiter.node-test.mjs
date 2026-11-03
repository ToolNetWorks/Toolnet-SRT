import test from 'node:test';
import assert from 'node:assert/strict';
import { createLocalLimiter } from '../server/storage/local-limiter.mjs';

test('FFmpeg admission is shared and bounded; abort removes a queued task',async()=>{
  const gate=createLocalLimiter({limit:1});let release,started=0;
  const first=gate.run(()=>{started++;return new Promise(resolve=>{release=resolve;});});
  await new Promise(resolve=>setImmediate(resolve));
  const controller=new AbortController();
  const second=gate.run(()=>{started++;},controller.signal);controller.abort();
  await assert.rejects(second,/PROCESSING_ABORTED/);assert.equal(started,1);
  assert.deepEqual(gate.getStatus(),{active:1,waiting:0,limit:1});
  release();await first;await new Promise(resolve=>setImmediate(resolve));
  assert.equal(gate.getStatus().active,0);
});
