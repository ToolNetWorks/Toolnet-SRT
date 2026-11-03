import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFile, readdir, stat } from 'node:fs/promises';
import { createServer } from 'node:net';
import { once } from 'node:events';
import pg from 'pg';
import { S3Client, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
const env=process.env;
for(const name of ['DATABASE_URL','R2_ACCOUNT_ID','R2_ACCESS_KEY_ID','R2_SECRET_ACCESS_KEY','R2_BUCKET','QA_AUDIO_30','QA_AUDIO_300']) if(!env[name]) throw Error('MISSING_'+name);
const longMode = env.QA_MODE === 'long' || env.QA_MODE === 'long-one';
if (longMode) for (const name of (env.QA_MODE === 'long-one' ? ['QA_AUDIO_3HOUR'] : ['QA_AUDIO_30MIN','QA_AUDIO_1HOUR','QA_AUDIO_3HOUR'])) if (!env[name]) throw Error('MISSING_'+name);
const schema=`srt_http_server_${randomBytes(6).toString('hex')}`;
const pool=new pg.Pool({connectionString:env.DATABASE_URL,max:1});
const s3=new S3Client({region:'auto',endpoint:`https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,forcePathStyle:true,credentials:{accessKeyId:env.R2_ACCESS_KEY_ID,secretAccessKey:env.R2_SECRET_ACCESS_KEY}});
const jobs=[],timings=[];let created=false,child,exit,worker,workerExit,workerReady=false,cleanup=true,passed=false,metricsTimer,mcpChecked=false;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let maxRss=0,maxCpu=0,maxDisk=0,maxActiveSlots=0;
async function diskSize(path){try {const info=await stat(path);if(info.isFile())return info.size;if(info.isDirectory()){let sum=0;for(const name of await readdir(path))sum+=await diskSize(path+'/'+name);return sum;}return 0;}catch{return 0;}}
const baseline=new Set((await readdir('/tmp')).filter(n=>/^toolnet-srt-(worker|ingress)-/.test(n)));
async function measure(){
 try {const text=execFileSync('ps',['-eo','pid=,ppid=,rss=,pcpu='],{encoding:'utf8'});const rows=text.trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number));
  const ids=new Set([child.pid,...(worker?[worker.pid]:[])]);for(let i=0;i<4;i++)for(const [pid,ppid] of rows)if(ids.has(ppid))ids.add(pid);
  const selected=rows.filter(([pid])=>ids.has(pid));maxRss=Math.max(maxRss,selected.reduce((n,r)=>n+r[2],0));maxCpu=Math.max(maxCpu,selected.reduce((n,r)=>n+r[3],0));
  const names=(await readdir('/tmp')).filter(n=>/^toolnet-srt-(worker|ingress)-/.test(n)&&!baseline.has(n));let bytes=0;for(const name of names)bytes+=await diskSize('/tmp/'+name);maxDisk=Math.max(maxDisk,bytes);
  if(env.DURABLE_EXPERIMENTAL_ASR_SEMAPHORE==='true'){const slots=await pool.query(`SELECT count(*)::int AS n FROM "${schema}".toolnet_srt_asr_slots WHERE expires_at>clock_timestamp()`);maxActiveSlots=Math.max(maxActiveSlots,slots.rows[0].n);}
 } catch {} }
try {
 const base=(await pool.query('SELECT current_schema() AS name')).rows[0]?.name;if(!/^[a-z_][a-z0-9_]*$/i.test(base??''))throw Error('INVALID_SCHEMA');
 await pool.query(`CREATE SCHEMA "${schema}"`);created=true;
 await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
 const c=await pool.connect();try{await c.query('BEGIN');await c.query(`SET LOCAL search_path TO "${schema}"`);
  for(const name of ['003_remote_fetch_queue.sql','004_remote_idempotency.sql','005_chunk_checkpoints.sql',...(env.DURABLE_EXPERIMENTAL_ASR_SEMAPHORE==='true'?['006_asr_semaphore.sql']:[])]) await c.query(await readFile(new URL(`../server/storage/migrations/${name}`,import.meta.url),'utf8'));
  await c.query('COMMIT');}catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}finally{c.release();}
 const socket=createServer();socket.listen(0,'127.0.0.1');await once(socket,'listening');const port=socket.address().port;await new Promise(r=>socket.close(r));
 const childEnv={...env,PORT:String(port),TEST_SERVER_SCHEMA:schema,DURABLE_JOBS_ENABLED:'true',TOOLNET_SRT_API_KEY:'qa-test-only',TOOLNET_SRT_ALLOWED_MEDIA_HOSTS:'media.example',TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY:randomBytes(32).toString('base64url'),TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY:randomBytes(32).toString('base64url'),DURABLE_CHUNK_SECONDS:longMode?'600':'60',DURABLE_WORKER_CONCURRENCY:env.QA_WORKER_CONCURRENCY||'1',DURABLE_POLL_INTERVAL_MS:'100',DURABLE_SHUTDOWN_GRACE_MS:'10000',TOOLNET_SRT_TIMING_DIAGNOSTICS:'true'};
 const collectTimings=stream=>{let pending='';stream.on('data',chunk=>{pending+=chunk.toString();const lines=pending.split('\n');pending=lines.pop();for(const line of lines){try{const event=JSON.parse(line);if(event.type==='durable_timing')timings.push(event);}catch{}}if(pending.length>16384)pending='';});};
 child=spawn(process.execPath,['--import','./tests/fixtures/isolated-server-db.mjs','server/index.mjs'],{env:{...childEnv,...(env.QA_SPLIT_WORKER==='true'?{DURABLE_RUN_WORKERS:'false'}:{})},stdio:['ignore','ignore','pipe']});
 child.on('exit',(code,signal)=>exit={code,signal});collectTimings(child.stderr);
 const baseUrl=`http://127.0.0.1:${port}`;
 for(let i=0;i<200;i++){if(exit)throw Error('SERVER_EXITED');try{if((await fetch(baseUrl+'/health')).ok)break;}catch{}await sleep(100);if(i===199)throw Error('SERVER_START_TIMEOUT');}
 if(env.QA_SPLIT_WORKER==='true'){
  worker=spawn(process.execPath,['--import','./tests/fixtures/isolated-server-db.mjs','server/durable-worker-service.mjs'],{env:childEnv,stdio:['ignore','pipe','pipe']});
  worker.on('exit',(code,signal)=>workerExit={code,signal});worker.stdout.on('data',chunk=>{if(chunk.toString().includes('DURABLE_WORKER_READY'))workerReady=true;});collectTimings(worker.stderr);
  for(let i=0;i<200&&!workerReady&&!workerExit;i++)await sleep(100);
  assert.equal(workerReady,true,'WORKER_NOT_READY');console.log('SEPARATE WORKER READY: PASS');
 }
 if(env.DURABLE_EXPERIMENTAL_ASR_SEMAPHORE==='true') {
  assert.equal((await fetch(baseUrl+'/v1/transcribe',{method:'POST'})).status,401);
  if(env.QA_VERIFY_SYNC==='true'){
   const response=await fetch(baseUrl+'/v1/transcribe',{method:'POST',headers:{'X-API-Key':'qa-test-only','Content-Type':'audio/mpeg'},body:await readFile(env.QA_AUDIO_30)});
   assert.equal(response.status,200);assert.match(await response.text(),/-->/);
   const created=await pool.query(`SELECT id FROM "${schema}".toolnet_srt_jobs`);for(const row of created.rows)if(!jobs.includes(row.id))jobs.push(row.id);
   console.log('DURABLE SYNCHRONOUS API: PASS');
  }
 }
 metricsTimer=setInterval(()=>void measure(),1000);
 async function submit(bytes,mime){const response=await fetch(baseUrl+'/v1/jobs',{method:'POST',headers:{'X-API-Key':'qa-test-only','Content-Type':mime},body:bytes});if(response.status===429)return null;if(response.status!==202)throw Error('UPLOAD_HTTP_'+response.status);const job=await response.json();jobs.push(job.id);return job.id;}
 async function waitJobs(ids,timeout=Number(env.QA_JOB_TIMEOUT_MS || 3600000)){const pending=new Set(ids),deadline=Date.now()+timeout;while(pending.size&&Date.now()<deadline){for(const id of [...pending]){const r=await fetch(baseUrl+'/v1/jobs/'+id,{headers:{'X-API-Key':'qa-test-only'}});const j=await r.json();if(j.status==='failed')throw Error('JOB_FAILED_'+j.errorCode);if(j.status==='completed')pending.delete(id);}if(pending.size)await sleep(1000);}if(pending.size)throw Error('JOB_WAIT_TIMEOUT');}
 async function verify(id){const response=await fetch(baseUrl+'/v1/jobs/'+id+'/result',{headers:{'X-API-Key':'qa-test-only'}});assert.equal(response.status,200);const srt=await response.text();assert.match(srt,/-->/);
  if(env.QA_VERIFY_MCP==='true'&&!mcpChecked){
   async function call(name){const r=await fetch(baseUrl+'/mcp',{method:'POST',headers:{'X-API-Key':'qa-test-only','Content-Type':'application/json',Accept:'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name,arguments:{job_id:id}}})});assert.equal(r.status,200);const body=await r.json();assert.equal(body.result.isError,false);return JSON.parse(body.result.content[0].text);}
   assert.equal((await call('get_transcription_job')).status,'completed');const mcp=await call('get_transcription_srt');assert.equal(mcp.srt,srt.slice(0,20000));mcpChecked=true;console.log('DURABLE MCP JOB + SRT: PASS');
  }
  if(env.QA_SRT_OUTPUT)await (await import('node:fs/promises')).writeFile(env.QA_SRT_OUTPUT,srt,{mode:0o600});const r=await pool.query(`SELECT count(*)::int AS n,coalesce(sum(attempt_count-1),0)::int AS retries FROM "${schema}".toolnet_srt_job_chunks WHERE job_id=$1 AND status='completed'`,[id]);assert.ok(r.rows[0].n>0);return {...r.rows[0],cues:(srt.match(/-->/g)??[]).length,srtBytes:Buffer.byteLength(srt)};}
 const cases=env.QA_MODE === 'long-one' ? [['3_HOURS',env.QA_AUDIO_3HOUR,1,'audio/mp4']] : longMode ? [['30_MINUTES',env.QA_AUDIO_30MIN,1,'audio/mp4'],['1_HOUR',env.QA_AUDIO_1HOUR,1,'audio/mp4'],['3_HOURS',env.QA_AUDIO_3HOUR,1,'audio/mp4']]
  : [['30_SECONDS',env.QA_AUDIO_30,1,'audio/mpeg'],['5_MINUTES',env.QA_AUDIO_300,1,'audio/mpeg'],['2_CONCURRENT',env.QA_AUDIO_30,2,'audio/mpeg'],['3_CONCURRENT',env.QA_AUDIO_30,3,'audio/mpeg'],['5_CONCURRENT',env.QA_AUDIO_30,5,'audio/mpeg']];
 for(const [label,path,count,mime] of cases){
  const bytes=await readFile(path);
  maxRss=0;maxCpu=0;maxDisk=0;maxActiveSlots=0;
  const start=Date.now(),responses=await Promise.all(Array.from({length:count},()=>submit(bytes,mime)));
  const ids=responses.filter(Boolean),rejected=count-ids.length;
  await waitJobs(ids);
  for(let i=0;i<rejected;i++){const id=await submit(bytes,mime);if(!id)throw Error('RATE_LIMIT_RETRY_FAILED');ids.push(id);}
  if(rejected)await waitJobs(ids);
  const data=[];for(const id of ids)data.push(await verify(id));
  await measure();const events=timings.filter(t=>ids.includes(t.jobId));const phase=name=>Math.round(events.filter(t=>t.phase===name).reduce((n,t)=>n+t.durationMs,0));
  assert.ok(maxActiveSlots<=Number(env.DURABLE_ASR_GLOBAL_CONCURRENCY||1));
  console.log(`${label}: PASS wall_ms=${Date.now()-start} chunks=${data.reduce((a,b)=>a+b.n,0)} cues=${data.reduce((a,b)=>a+b.cues,0)} srt_bytes=${data.reduce((a,b)=>a+b.srtBytes,0)} chunk_retries=${data.reduce((a,b)=>a+b.retries,0)} initial_http_429=${rejected} provider_http_429=${events.filter(t=>t.phase==='asr'&&t.httpStatus===429).length} slot_wait_ms=${phase('asr_slot_wait')} extraction_ms=${phase('extract')} asr_ms=${phase('asr')} checkpoint_ms=${['checkpoint_initialize','checkpoint_claim','checkpoint_complete','checkpoint_fail'].reduce((n,p)=>n+phase(p),0)} merge_ms=${phase('merge')} download_ms=${phase('download')} upload_ms=${phase('upload_result')} peak_active_slots=${maxActiveSlots} peak_rss_kib=${maxRss} peak_ps_cpu_percent=${maxCpu.toFixed(1)} peak_temp_bytes=${maxDisk}`);
 }
 clearInterval(metricsTimer);child.kill('SIGTERM');for(let i=0;i<150&&!exit;i++)await sleep(100);assert.deepEqual(exit,{code:0,signal:null});
 if(worker){worker.kill('SIGTERM');for(let i=0;i<150&&!workerExit;i++)await sleep(100);assert.deepEqual(workerExit,{code:0,signal:null});}passed=true;
}catch(e){process.exitCode=1;console.error('REAL SPEECH QA FAILED:',e.code||e.message);console.error('SANITIZED TIMING ERRORS:',JSON.stringify(timings.filter(t=>t.outcome==='error').map(({phase,httpStatus,errorCode})=>({phase,httpStatus,errorCode}))));}
finally{
 clearInterval(metricsTimer);if(child&&!exit){child.kill('SIGKILL');for(let i=0;i<50&&!exit;i++)await sleep(100);}if(child&&!exit)cleanup=false;
 if(worker&&!workerExit){worker.kill('SIGKILL');for(let i=0;i<50&&!workerExit;i++)await sleep(100);}if(worker&&!workerExit)cleanup=false;
 if(created)try{const all=await pool.query(`SELECT id FROM "${schema}".toolnet_srt_jobs`);for(const row of all.rows)if(!jobs.includes(row.id))jobs.push(row.id);}catch{cleanup=false;}
 for(const id of jobs){try{let token;do{const page=await s3.send(new ListObjectsV2Command({Bucket:env.R2_BUCKET,Prefix:`toolnet-srt/jobs/${id}/`,ContinuationToken:token}));for(const item of page.Contents??[])await s3.send(new DeleteObjectCommand({Bucket:env.R2_BUCKET,Key:item.Key}));token=page.IsTruncated?page.NextContinuationToken:undefined;}while(token);}catch{cleanup=false;}}
 s3.destroy();if(created&&cleanup)try{await pool.query(`DROP SCHEMA "${schema}" CASCADE`);}catch{cleanup=false;}await pool.end().catch(()=>{cleanup=false;});
 if(!cleanup){process.exitCode=1;console.error('TEST CLEANUP FAILED:',schema);}else console.log('TEST RESOURCES CLEANED: PASS');
}
if(passed&&cleanup)console.log('TOOLNET-SRT REAL SPEECH QA PASSED');
