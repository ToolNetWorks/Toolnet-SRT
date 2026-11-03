import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { mkdtemp, open, readFile, rm, copyFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
import { createPostgresJobStore } from '../server/storage/postgres.mjs';
import { createPostgresLeaseManager } from '../server/storage/leases.mjs';
import { createPostgresProgressManager } from '../server/storage/progress.mjs';
import { createPostgresChunkCheckpoints } from '../server/storage/postgres-chunk-checkpoints.mjs';
import { createDurableWorker } from '../server/storage/durable-worker.mjs';
import { toSrt } from '../dist/index.js';
if (!process.env.DATABASE_URL) throw Error('DATABASE_URL_MISSING');
const schema=`srt_chunk_long_${randomBytes(6).toString('hex')}`;
const control=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1});
const workers=new pg.Pool({connectionString:process.env.DATABASE_URL,max:3});
const dir=await mkdtemp(join(tmpdir(),'chunk-worker-long-'));
let created=false,cleanup=true,passed=false;
const db={async connect(){const raw=await workers.connect();return {async query(sql,args){if(sql==='BEGIN'){try{await raw.query('BEGIN');await raw.query(`SET LOCAL search_path TO "${schema}"`);}catch(e){await raw.query('ROLLBACK').catch(()=>{});throw e;}return;}return raw.query(sql,args);},release(error){raw.release(error);}};},
 async query(sql,args){const c=await this.connect();try{await c.query('BEGIN');const r=await c.query(sql,args);await c.query('COMMIT');return r;}catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}finally{c.release();}}};
try {
 const base=(await control.query('SELECT current_schema() AS name')).rows[0]?.name;if(!/^[a-z_][a-z0-9_]*$/i.test(base??''))throw Error('INVALID_SCHEMA');
 await control.query(`CREATE SCHEMA "${schema}"`);created=true;
 await control.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
 await db.query(await readFile(new URL('../server/storage/migrations/005_chunk_checkpoints.sql',import.meta.url),'utf8'));
 const wav=join(dir,'source.wav'),samples=10802*16000,handle=await open(wav,'w');
 try {const h=Buffer.alloc(44);h.write('RIFF',0);h.writeUInt32LE(36+samples*2,4);h.write('WAVEfmt ',8);h.writeUInt32LE(16,16);h.writeUInt16LE(1,20);h.writeUInt16LE(1,22);h.writeUInt32LE(16000,24);h.writeUInt32LE(32000,28);h.writeUInt16LE(2,32);h.writeUInt16LE(16,34);h.write('data',36);h.writeUInt32LE(samples*2,40);await handle.write(h,0,h.length,0);await handle.truncate(44+samples*2);}finally{await handle.close();}
 const video=join(dir,'long.mkv'),short=join(dir,'short.mkv');
 const mux=spawnSync('ffmpeg',['-hide_banner','-loglevel','error','-y','-f','lavfi','-i','color=c=black:s=32x32:r=1/60','-i',wav,'-map','0:v:0','-map','1:a:0','-c:v','mpeg4','-c:a','copy','-t','10802',video]);assert.equal(mux.status,0);
 const clip=spawnSync('ffmpeg',['-hide_banner','-loglevel','error','-y','-i',video,'-t','1802','-c','copy',short]);assert.equal(clip.status,0);
 const store=createPostgresJobStore({pool:db}),leases=createPostgresLeaseManager({pool:db}),progress=createPostgresProgressManager({pool:db}),checkpoints=createPostgresChunkCheckpoints({pool:db});
 for(const [label,source,minCount] of [['30-MINUTE',short,3],['3-HOUR',video,18]]){
  const id=randomUUID();await store.saveJob({id,status:'queued',stage:'queued',progress:0,completedSegments:0,totalSegments:null,extension:'.mkv',sourceKey:`toolnet-srt/jobs/${id}/source.mkv`,resultKey:null});
  let failedOnce=false, output=''; const calls=new Map();
  const worker=createDurableWorker({leases,progress,chunkCheckpoints:checkpoints,workerId:'long-test-worker',chunkSeconds:600,chunkOverlapSeconds:5,
   storage:{async downloadFile({localPath}){await copyFile(source,localPath);}},
   results:{async putResult({jobId,leaseToken,srt}){output=String(srt);return {key:`toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt`};}},
   async transcribe(path){const index=Number(path.match(/chunk-(\d+)/)[1]);calls.set(index,(calls.get(index)??0)+1);
    if(index===1&&!failedOnce){failedOnce=true;throw Error('SIMULATED_TRANSIENT');}
    return [{text:`chunk-${index}`,startMs:100,endMs:200}];},toSrt,
  });
  const result=await worker.runOnce();assert.equal(result.status,'completed');assert.equal(result.jobId,id);
  const rows=(await control.query(`SELECT chunk_index,status,attempt_count,start_offset_ms FROM "${schema}".toolnet_srt_job_chunks WHERE job_id=$1 ORDER BY chunk_index`,[id])).rows;
  assert.ok(rows.length>=minCount);assert.ok(rows.every(r=>r.status==='completed'));assert.equal(rows[1].attempt_count,2);
  assert.ok(rows.filter(r=>r.chunk_index!==1).every(r=>r.attempt_count===1));
  assert.equal(calls.get(1),2);assert.match(output,/^1\n/);assert.match(output,/-->/);
  assert.equal((await store.getJob(id)).progress,100);
  console.log(`${label} DURABLE WORKER / SINGLE CHUNK RETRY: PASS (${rows.length} chunks)`);
 }
 passed=true;
}catch(e){process.exitCode=1;console.error('LONG CHUNK WORKER TEST FAILED:',e.code||e.message);}
finally{
 await workers.end().catch(()=>{cleanup=false;});
 if(created)try{await control.query(`DROP SCHEMA "${schema}" CASCADE`);}catch{cleanup=false;}
 await control.end().catch(()=>{cleanup=false;});await rm(dir,{recursive:true,force:true}).catch(()=>{cleanup=false;});
 if(!cleanup){process.exitCode=1;console.error('CLEANUP FAILED:',schema);}else console.log('TEST RESOURCES CLEANED: PASS');
}
if(passed&&cleanup)console.log('TOOLNET-SRT LONG CHUNK WORKER PASSED');
