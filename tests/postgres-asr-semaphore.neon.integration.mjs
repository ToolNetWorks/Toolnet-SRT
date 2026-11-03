import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fork } from 'node:child_process';
import pg from 'pg';
import { createPostgresAsrSemaphore } from '../server/storage/postgres-asr-semaphore.mjs';
import { runWithAsrSlot } from '../server/storage/asr-slot-runner.mjs';

if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL_MISSING');
const schema=`srt_asr_sem_${randomBytes(6).toString('hex')}`;
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:6,connectionTimeoutMillis:10000});
let created=false, cleanup=true;
const scoped={async connect(){
  const client=await pool.connect();let open=false;
  return {async query(sql,args){
    if(sql==='BEGIN'){await client.query('BEGIN');open=true;await client.query(`SET LOCAL search_path TO "${schema}"`);return;}
    if(!open){await client.query('BEGIN');try{await client.query(`SET LOCAL search_path TO "${schema}"`);const r=await client.query(sql,args);await client.query('COMMIT');return r;}catch(e){await client.query('ROLLBACK').catch(()=>{});throw e;}}
    const r=await client.query(sql,args);if(sql==='COMMIT'||sql==='ROLLBACK')open=false;return r;
  },release(error){client.release(error);}};
}};
try {
  await pool.query(`CREATE SCHEMA "${schema}"`);created=true;
  await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs(id uuid PRIMARY KEY,status text,lease_owner text,lease_token uuid,lease_expires_at timestamptz)`);
  const migration=await readFile(new URL('../server/storage/migrations/006_asr_semaphore.sql',import.meta.url),'utf8');
  const client=await scoped.connect();try{await client.query('BEGIN');await client.query(migration);await client.query(migration);await client.query('COMMIT');}finally{client.release();}
  const jobs=[];
  for(let i=0;i<10;i++){const jobId=randomUUID(),jobLeaseToken=randomUUID(),workerId=`test-worker-${i}`;
    await pool.query(`INSERT INTO "${schema}".toolnet_srt_jobs VALUES ($1,'processing',$2,$3,now()+interval '5 minutes')`,[jobId,workerId,jobLeaseToken]);
    jobs.push({jobId,jobLeaseToken,workerId,chunkIndex:i});}
  const first=createPostgresAsrSemaphore({pool:scoped,limit:2,leaseSeconds:30});
  const second=createPostgresAsrSemaphore({pool:scoped,limit:2,leaseSeconds:30});
  // Simulate a one-sided DB failure exactly when a 429 must be persisted.
  // The other worker must not fill the remaining slot while outcome is unknown.
  let uncertainSlot,stopped=false;
  const partial={...first,async acquire(owner){uncertainSlot=await first.acquire(owner);return uncertainSlot;},
    async rateLimited(){throw Error('SIMULATED_DB_PARTITION');}};
  await assert.rejects(runWithAsrSlot({semaphore:partial,owner:jobs[0],heartbeatMs:5000,ensureOwnership:async()=>{},
    onRateLimit:()=>{stopped=true;},transcribe:async()=>{throw Object.assign(Error('rate limited'),{status:429,retryAfterMs:5000});}}),/SIMULATED_DB_PARTITION/);
  assert.equal(stopped,true);assert.ok(uncertainSlot);assert.equal(await second.acquire(jobs[1]),null);
  // No provider was called in this simulation; releasing the slot is safe.
  assert.equal(await first.release(uncertainSlot),true);
  console.log('UNKNOWN 429 COMMIT BLOCKS OTHER JOBS: PASS');
  const anchor=await first.acquire(jobs[0]);assert.ok(anchor);
  const claims=await Promise.all([{...jobs[0],chunkIndex:1},...jobs.slice(1)].map((job,i)=>(i%2?first:second).acquire(job)));
  const slots=[anchor,...claims.filter(Boolean)];assert.equal(slots.length,2);assert.equal(new Set(slots.map(x=>x.slot)).size,2);
  assert.equal(await first.heartbeat(slots[0]),true);
  assert.equal(await first.rateLimited(slots[0],5000),true);
  assert.equal(await second.acquire(jobs.find(j=>!slots.some(s=>s.jobId===j.jobId))),null);
  const cooldownOwner=jobs.find(j=>!slots.some(s=>s.jobId===j.jobId));
  const restarted=fork(new URL('./fixtures/asr-slot-contender.mjs',import.meta.url),[],{
    env:{...process.env,TEST_ASR_SCHEMA:schema,TEST_ASR_OWNER:JSON.stringify(cooldownOwner)},stdio:['ignore','ignore','ignore','ipc'],
  });
  try {
    const observed=await new Promise((resolve,reject)=>{restarted.once('message',message=>resolve(message.slot));restarted.once('error',reject);restarted.once('exit',code=>{if(code!==0)reject(new Error('COOLDOWN_CHILD_FAILED'));});});
    assert.equal(observed,null);
  } finally {
    if(restarted.exitCode===null&&restarted.signalCode===null){const exited=new Promise(resolve=>restarted.once('exit',resolve));restarted.send('release');await exited;}
  }
  assert.equal(await first.heartbeat(slots[0]),false);
  assert.equal(await second.release({...slots[1],token:randomUUID()}),false);
  await new Promise(r=>setTimeout(r,5100));
  assert.equal(await first.acquire(jobs.find(j=>!slots.some(s=>s.jobId===j.jobId))),null);
  assert.equal(await second.release(slots[1]),true);
  // A 429 fences the token but retains its reservation until quarantine ends.
  assert.equal(await first.release(slots[0]),false);
  assert.equal(await first.acquire(jobs[5]),null);
  await pool.query(`UPDATE "${schema}".toolnet_srt_asr_slots SET expires_at=NOW()-interval '2 seconds', reclaim_after=NOW()-interval '1 second' WHERE token=$1::uuid`,[slots[0].token]);
  const reclaimed=await first.acquire(jobs[5]);assert.ok(reclaimed);
  await pool.query(`UPDATE "${schema}".toolnet_srt_asr_slots SET expires_at=NOW()-interval '1 second' WHERE token=$1::uuid`,[reclaimed.token]);
  assert.equal(await second.acquire(jobs[6]),null);
  await pool.query(`UPDATE "${schema}".toolnet_srt_asr_slots SET reclaim_after=NOW()-interval '500 milliseconds' WHERE token=$1::uuid`,[reclaimed.token]);
  const winner=await second.acquire(jobs[6]);assert.ok(winner);assert.notEqual(winner.token,reclaimed.token);
  assert.equal(await first.heartbeat(reclaimed),false);assert.equal(await first.release(reclaimed),false);
  await pool.query(`UPDATE "${schema}".toolnet_srt_jobs SET status='cancelled' WHERE id=$1`,[winner.jobId]);
  assert.equal(await second.heartbeat(winner),false);
  assert.equal(await second.release(winner),true);
  await assert.rejects(createPostgresAsrSemaphore({pool:scoped,limit:1}).acquire(jobs[8]),/ASR_GLOBAL_LIMIT_MISMATCH/);
  await assert.rejects(createPostgresAsrSemaphore({pool:scoped,limit:2,adaptive:false}).acquire(jobs[8]),/ASR_GLOBAL_LIMIT_MISMATCH/);
  await pool.query(`UPDATE "${schema}".toolnet_srt_asr_control SET reduced_until=NOW()-interval '1 second'`);
  const cohortAnchor=await first.acquire(jobs[0]);assert.ok(cohortAnchor);
  const children=[];
  try {
    const responses=[{...jobs[0],chunkIndex:1},...jobs.slice(1,4)].map(owner=>new Promise((resolve,reject)=>{
      const child=fork(new URL('./fixtures/asr-slot-contender.mjs',import.meta.url),[],{
        env:{...process.env,TEST_ASR_SCHEMA:schema,TEST_ASR_OWNER:JSON.stringify(owner)},stdio:['ignore','ignore','ignore','ipc'],
      });children.push(child);
      child.once('message',message=>resolve(message.slot));child.once('error',reject);
      child.once('exit',code=>{if(code!==0)reject(new Error('CONTENDER_FAILED'));});
    }));
    const claimed=await Promise.all(responses);
    assert.equal(claimed.filter(Boolean).length,1);
    assert.equal(claimed[0].slot!==cohortAnchor.slot,true);
    const victimIndex=claimed.findIndex(Boolean),old=claimed[victimIndex],victim=children[victimIndex];
    assert.equal(await first.release(cohortAnchor),true);
    const killed=new Promise(resolve=>victim.once('exit',resolve));victim.kill('SIGKILL');await killed;
    // Drain the healthy contenders before waiting for the killed owner's TTL.
    await Promise.all(children.filter((_,i)=>i!==victimIndex).map(child=>new Promise(resolve=>{
      if(child.exitCode!==null||child.signalCode!==null)return resolve();
      child.once('exit',resolve);child.send('release');
    })));
    const deadline=Date.now()+40000;
    while(Date.now()<deadline){const r=await pool.query(`SELECT expires_at<=NOW() AS expired FROM "${schema}".toolnet_srt_asr_slots WHERE token=$1`,[old.token]);if(r.rows[0]?.expired)break;await new Promise(resolve=>setTimeout(resolve,250));}
    assert.equal(await first.acquire(jobs[9]),null);
    // Avoid a 15-minute wait in this isolated test; expiration itself was real.
    await pool.query(`UPDATE "${schema}".toolnet_srt_asr_slots SET reclaim_after=NOW()-interval '1 second', expires_at=NOW()-interval '2 seconds' WHERE token=$1::uuid`,[old.token]);
    const newSlot=await first.acquire(jobs[9]);assert.ok(newSlot);assert.notEqual(newSlot.token,old.token);
    assert.equal(await first.heartbeat(old),false);assert.equal(await first.release(old),false);
    await first.release(newSlot);
  } finally {
    await Promise.all(children.map(child=>new Promise(resolve=>{
      if(child.exitCode!==null||child.signalCode!==null)return resolve();
      child.once('exit',resolve);child.send('release');
      const timer=setTimeout(()=>child.kill('SIGKILL'),5000);timer.unref();
    })));
  }
  console.log('INDEPENDENT PROCESSES GLOBAL SLOT LIMIT: PASS');
  console.log('ASR SEMAPHORE CONCURRENCY / COOLDOWN / FENCING: PASS');
} finally {
  if(created)try{await pool.query(`DROP SCHEMA "${schema}" CASCADE`);}catch{cleanup=false;console.error('SCHEMA CLEANUP FAILED:',schema);}
  await pool.end().catch(()=>{cleanup=false;});
  if(!cleanup)process.exitCode=1; else console.log('TEST SCHEMA CLEANED: PASS');
}
