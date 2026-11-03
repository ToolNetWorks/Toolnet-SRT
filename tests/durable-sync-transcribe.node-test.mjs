import test from 'node:test';
import assert from 'node:assert/strict';
import { runDurableSynchronous } from '../server/storage/durable-sync-transcribe.mjs';

test('Synchronous response uses a durable queued job and returns its SRT',async()=>{
  const calls=[];let reads=0;
  const manager={async createUpload(source,mime){calls.push('upload');assert.equal(source,'stream');assert.equal(mime,'audio/mpeg');return {id:'job'};},
    async getJob(){calls.push('job');return {status:++reads===1?'processing':'completed'};},
    async getResult(){calls.push('result');return Buffer.from('subtitle');},async cancelJob(){calls.push('cancel');}};
  const result=await runDurableSynchronous({manager,source:'stream',mimeType:'audio/mpeg',pollIntervalMs:10});
  assert.equal(result.toString(),'subtitle');assert.deepEqual(calls,['upload','job','job','result']);
});
test('A disconnect while uploading cancels the job once it is created',async()=>{
  const controller=new AbortController();let cancelled=0;
  const manager={async createUpload(){controller.abort();return {id:'job'};},async getJob(){throw Error('UNEXPECTED');},
    async getResult(){throw Error('UNEXPECTED');},async cancelJob(){cancelled++;}};
  await assert.rejects(runDurableSynchronous({manager,signal:controller.signal}),/PROCESSING_ABORTED/);
  assert.equal(cancelled,1);
});
