import test from 'node:test';
import assert from 'node:assert/strict';
import { createFetchDiskQuota } from '../server/storage/fetch-disk-quota.mjs';
import { createDurableUploadIngress } from '../server/storage/durable-ingress.mjs';
import { createDurableWorker } from '../server/storage/durable-worker.mjs';
import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';

test('Concurrent workers cannot exceed the shared reservation', async () => {
  let releaseCheck;
  const gate = new Promise(resolve => { releaseCheck = resolve; });
  const quota = createFetchDiskQuota({ maxBytes: 100, freeSpace: async () => { await gate; return { bavail: 1000, bsize: 1 }; } });
  const first = quota.reserve(60, '/tmp');
  await assert.rejects(quota.reserve(60, '/tmp'), /FETCH_DISK_QUOTA_EXCEEDED/);
  releaseCheck(); const release = await first;
  assert.equal(quota.getStatus().reservedBytes, 60);
  release(); release(); assert.equal(quota.getStatus().reservedBytes, 0);
});

test('Ingress and ASR worker share reservations within one runtime',async()=>{
  const quota=createFetchDiskQuota({maxBytes:100,freeSpace:async()=>({bavail:10000,bsize:1})});
  let releaseUpload,started;const gate=new Promise(resolve=>{releaseUpload=resolve;});
  const entered=new Promise(resolve=>{started=resolve;});
  const ingress=createDurableUploadIngress({maxBytes:100,diskQuota:quota,
    storage:{async uploadFile({jobId,filename}){started();await gate;return {key:`toolnet-srt/jobs/${jobId}/${filename}`};}},
    jobStore:{async saveJob(job){return job;}},
  });
  const request=Readable.from([Buffer.from('audio')]);request.headers={'content-length':'5'};
  const pending=ingress.createUpload(request,'audio/mpeg');
  await entered;
  const id=randomUUID(),token=randomUUID();let downloaded=false;
  const worker=createDurableWorker({diskQuota:quota,maxSourceBytes:100,
    leases:{async claimJob(){return {id,lease_token:token,extension:'.mp3',source_key:`toolnet-srt/jobs/${id}/source.mp3`};},
      async heartbeatJob(){return {id};},async completeJob(){throw Error('UNEXPECTED');},async failJob(){return {status:'queued'};}},
    storage:{async downloadFile(){downloaded=true;}},results:{async putResult(){throw Error('UNEXPECTED');}},
  });
  try {assert.equal((await worker.runOnce()).status,'queued');assert.equal(downloaded,false);}
  finally {releaseUpload();await pending;}
  assert.equal(quota.getStatus().reservedBytes,0);
});
test('Insufficient disk releases the reservation', async () => {
  const quota = createFetchDiskQuota({ maxBytes: 100, freeSpace: async () => ({ bavail: 1, bsize: 1 }) });
  await assert.rejects(quota.reserve(10, '/tmp'), /INSUFFICIENT_DISK_SPACE/);
  assert.equal(quota.getStatus().reservedBytes, 0);
});
