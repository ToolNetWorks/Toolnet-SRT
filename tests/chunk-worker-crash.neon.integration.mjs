import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { createPostgresJobStore } from '../server/storage/postgres.mjs';
import { createPostgresLeaseManager } from '../server/storage/leases.mjs';
import { createPostgresChunkCheckpoints } from '../server/storage/postgres-chunk-checkpoints.mjs';
import { createDurableWorker } from '../server/storage/durable-worker.mjs';
import { createPostgresAsrSemaphore } from '../server/storage/postgres-asr-semaphore.mjs';
const schemaRe = /^srt_chunk_crash_[a-f0-9]{12}$/;
const sleep = ms=>new Promise(r=>setTimeout(r,ms));
async function until(check,ms=100000){const end=Date.now()+ms;while(Date.now()<end){if(await check())return;await sleep(100);}throw Error('WAIT_TIMEOUT');}
function scoped(pool,schema){
 if(!schemaRe.test(schema))throw Error('INVALID_SCHEMA');
 return {async connect(){const raw=await pool.connect();return {async query(sql,args){if(sql==='BEGIN'){try{await raw.query('BEGIN');await raw.query(`SET LOCAL search_path TO "${schema}"`);}catch(e){await raw.query('ROLLBACK').catch(()=>{});throw e;}return;}return raw.query(sql,args);},release(error){raw.release(error);}};},
 async query(sql,args){const c=await this.connect();try{await c.query('BEGIN');const r=await c.query(sql,args);await c.query('COMMIT');return r;}catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}finally{c.release();}}};
}
async function notify(type,extra={}){if(process.send&&process.connected)await new Promise(r=>process.send({type,...extra},r));}
async function child(){
 const role=process.argv[3],schema=process.env.TEST_SCHEMA;if(!schemaRe.test(schema??'')||!['A','B'].includes(role))throw Error('INVALID_CHILD');
 const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:3});const db=scoped(pool,schema);
 const worker=createDurableWorker({leases:createPostgresLeaseManager({pool:db}),chunkCheckpoints:createPostgresChunkCheckpoints({pool:db}),
   workerId:`chunk-worker-${role}`,leaseSeconds:30,heartbeatIntervalMs:2000,chunkSeconds:60,
   chunkSeekPrerollMs:process.env.TEST_FAST_SEEK==='1'?2000:null,
   asrSemaphore:process.env.TEST_ASR_SEMAPHORE==='1'?createPostgresAsrSemaphore({pool:db,limit:2,leaseSeconds:30}):null,
   asrSlotHeartbeatMs:5000,
   storage:{async downloadFile({localPath}){await writeFile(localPath,await readFile(process.env.TEST_AUDIO));}},
   results:{async putResult({jobId,leaseToken,srt}){await notify('srt',{srt:String(srt)});return {key:`toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt`};}},
   async transcribe(path){const index=Number(path.match(/chunk-(\d+)/)[1]);await notify('transcribe',{index});
     if(role==='A'&&index===1){await notify('blocked');return new Promise(()=>{});}return [{text:`word-${index}`,startMs:100,endMs:200}];},
   toSrt:async segments=>JSON.stringify(segments),
 });
 const outcome=await worker.runOnce();await notify('result',{status:outcome.status});await pool.end();process.disconnect?.();
}
function spawnWorker(role,schema,audio){
 const child=fork(fileURLToPath(import.meta.url),['--child',role],{env:{...process.env,TEST_SCHEMA:schema,TEST_AUDIO:audio},stdio:['ignore','ignore','pipe','ipc']});
 const p={child,messages:[],exit:null};child.on('message',m=>p.messages.push(m));child.on('exit',(code,signal)=>p.exit={code,signal});child.stderr.on('data',()=>{});return p;
}
async function parent(){
 if(!process.env.DATABASE_URL)throw Error('DATABASE_URL_MISSING');
 const schema=`srt_chunk_crash_${randomBytes(6).toString('hex')}`,dir=await mkdtemp(join(tmpdir(),'chunk-crash-'));
 const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:2}),processes=[];let created=false,cleanup=true,passed=false;
 try{
  const base=(await pool.query('SELECT current_schema() AS name')).rows[0]?.name;
  if(!/^[a-z_][a-z0-9_]*$/i.test(base??''))throw Error('INVALID_SCHEMA');
  await pool.query(`CREATE SCHEMA "${schema}"`);created=true;
  await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
  const db=scoped(pool,schema);await db.query(await readFile(new URL('../server/storage/migrations/005_chunk_checkpoints.sql',import.meta.url),'utf8'));
  if(process.env.TEST_ASR_SEMAPHORE==='1')await db.query(await readFile(new URL('../server/storage/migrations/006_asr_semaphore.sql',import.meta.url),'utf8'));
  const audio=join(dir,'source.wav'),rate=1000,seconds=125,samples=rate*seconds,header=Buffer.alloc(44);
  header.write('RIFF',0);header.writeUInt32LE(36+samples*2,4);header.write('WAVEfmt ',8);header.writeUInt32LE(16,16);header.writeUInt16LE(1,20);
  header.writeUInt16LE(1,22);header.writeUInt32LE(rate,24);header.writeUInt32LE(rate*2,28);header.writeUInt16LE(2,32);header.writeUInt16LE(16,34);
  header.write('data',36);header.writeUInt32LE(samples*2,40);await writeFile(audio,Buffer.concat([header,Buffer.alloc(samples*2)]));
  const store=createPostgresJobStore({pool:db}),id=randomUUID();
  await store.saveJob({id,status:'queued',stage:'queued',progress:0,completedSegments:0,totalSegments:null,extension:'.wav',sourceKey:`toolnet-srt/jobs/${id}/source.wav`,resultKey:null});
  const a=spawnWorker('A',schema,audio);processes.push(a);
  await until(()=>{if(a.exit)throw Error('A_EXITED');return a.messages.some(m=>m.type==='blocked');});
  const before=await pool.query(`SELECT chunk_index,status,attempt_count FROM "${schema}".toolnet_srt_job_chunks WHERE job_id=$1 ORDER BY chunk_index`,[id]);
  assert.equal(before.rows[0].status,'completed');assert.equal(before.rows[1].status,'processing');
  a.child.kill('SIGKILL');await until(()=>a.exit);assert.equal(a.exit.signal,'SIGKILL');
  await until(async()=>{const r=await pool.query(`SELECT lease_expires_at<=NOW() AS expired FROM "${schema}".toolnet_srt_jobs WHERE id=$1`,[id]);return r.rows[0].expired;});
  if(process.env.TEST_ASR_SEMAPHORE==='1') {
    // Test only: advance the additional quarantine after observing real job lease expiry.
    await pool.query(`UPDATE "${schema}".toolnet_srt_asr_slots SET expires_at=NOW()-interval '2 seconds',reclaim_after=NOW()-interval '1 second'`);
  }
  const b=spawnWorker('B',schema,audio);processes.push(b);
  await until(()=>{if(b.exit&&!b.messages.some(m=>m.type==='result'))throw Error('B_EXITED');return b.messages.some(m=>m.type==='result');});
  assert.equal(b.messages.find(m=>m.type==='result').status,'completed');
  assert.ok(!b.messages.some(m=>m.type==='transcribe'&&m.index===0));
  assert.ok(b.messages.some(m=>m.type==='transcribe'&&m.index===1));
  const after=await pool.query(`SELECT chunk_index,status,attempt_count FROM "${schema}".toolnet_srt_job_chunks WHERE job_id=$1 ORDER BY chunk_index`,[id]);
  assert.equal(after.rows[0].attempt_count,1);assert.equal(after.rows[1].attempt_count,2);assert.ok(after.rows.every(row=>row.status==='completed'));
  const srt=JSON.parse(b.messages.find(m=>m.type==='srt').srt);assert.equal(srt.length,3);
  assert.ok(srt[1].startMs>=60000);assert.ok(srt[2].startMs>=120000);
  console.log('SIGKILL + REAL LEASE EXPIRY + CHECKPOINT RECOVERY: PASS');
  console.log('COMPLETED CHUNK REUSED / FAILED CHUNK RETRIED: PASS');passed=true;
 }catch(e){process.exitCode=1;console.error('CHUNK CRASH TEST FAILED:',e.code||e.message);}
 finally{
  for(const p of processes)if(!p.exit)p.child.kill('SIGKILL');
  await Promise.all(processes.map(p=>until(()=>p.exit,5000).catch(()=>{cleanup=false;})));
  if(created&&cleanup)try{await pool.query(`DROP SCHEMA "${schema}" CASCADE`);}catch{cleanup=false;}
  await pool.end().catch(()=>{cleanup=false;});await rm(dir,{recursive:true,force:true}).catch(()=>{cleanup=false;});
  if(!cleanup){process.exitCode=1;console.error('CLEANUP FAILED:',schema);}else console.log('TEST RESOURCES CLEANED: PASS');
 }
 if(passed&&cleanup)console.log('TOOLNET-SRT CHUNK CRASH RECOVERY PASSED');
}
if(process.argv[2]==='--child')await child();else await parent();
