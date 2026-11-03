import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createPostgresAsrSemaphore } from '../server/storage/postgres-asr-semaphore.mjs';

test('Heartbeat and release use transactions suitable for a pooled search_path',async()=>{
  const commands=[];
  const pool={async connect(){let transaction=false;return {
    async query(sql){commands.push(sql.trim().split(/\s+/)[0]);
      if(sql==='BEGIN'){transaction=true;return {};}
      if(sql==='COMMIT'||sql==='ROLLBACK'){transaction=false;return {};}
      assert.equal(transaction,true);return {rowCount:1,rows:[{slot:1}]};},
    release(){assert.equal(transaction,false);},
  };}};
  const gate=createPostgresAsrSemaphore({pool});
  const owner={jobId:randomUUID(),jobLeaseToken:randomUUID(),workerId:'worker',chunkIndex:0,token:randomUUID()};
  assert.equal(await gate.heartbeat(owner),true);assert.equal(await gate.release(owner),true);
  assert.deepEqual(commands,['BEGIN','UPDATE','COMMIT','BEGIN','DELETE','COMMIT']);
});
