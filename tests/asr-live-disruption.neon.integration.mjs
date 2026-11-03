import assert from 'node:assert/strict';
import { randomBytes,randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fork } from 'node:child_process';
import { createServer,connect } from 'node:net';
import { once } from 'node:events';
import pg from 'pg';
import { createPostgresAsrSemaphore } from '../server/storage/postgres-asr-semaphore.mjs';
if(!process.env.DATABASE_URL||!process.env.QA_AUDIO_30)throw Error('TEST_ENV_MISSING');
const schema=`srt_asr_live_${randomBytes(6).toString('hex')}`,url=new URL(process.env.DATABASE_URL);
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:3,connectionTimeoutMillis:10000});
const sleep=ms=>new Promise(r=>setTimeout(r,ms));let created=false,cleanup=true;
const children=[],proxies=[];
const scoped={async connect(){const c=await pool.connect();return {async query(sql,args){if(sql==='BEGIN'){await c.query('BEGIN');await c.query(`SET LOCAL search_path TO "${schema}"`);return;}return c.query(sql,args);},release(error){c.release(error);}};}};
async function until(check,ms=30000){const end=Date.now()+ms;while(Date.now()<end){const result=await check();if(result)return result;await sleep(100);}throw Error('WAIT_TIMEOUT');}
async function proxy(){const sockets=new Set();let blocked=false;
  const server=createServer(client=>{if(blocked){client.destroy();return;}const upstream=connect(Number(url.port||5432),url.hostname);
    sockets.add(client);sockets.add(upstream);client.on('error',()=>{});upstream.on('error',()=>{});
    client.on('close',()=>sockets.delete(client));upstream.on('close',()=>sockets.delete(upstream));client.pipe(upstream).pipe(client);});
  server.listen(0,'127.0.0.1');await once(server,'listening');
  const item={port:server.address().port,cut(){blocked=true;for(const socket of sockets)socket.destroy();},async close(){item.cut();await new Promise(resolve=>server.close(resolve));}};
  proxies.push(item);return item;
}
function spawnWorker(owner,port){
  const child=fork(new URL('./fixtures/asr-disruption-child.mjs',import.meta.url),[],{
    env:{...process.env,TEST_ASR_SCHEMA:schema,TEST_PROXY_PORT:String(port),TEST_ASR_OWNER:JSON.stringify(owner),TEST_ASR_AUDIO:process.env.QA_AUDIO_30},
    stdio:['ignore','ignore','ignore','ipc'],
  });
  const state={child,messages:[],exit:null};child.on('message',m=>state.messages.push(m));child.on('exit',(code,signal)=>state.exit={code,signal});children.push(state);return state;
}
try {
  await pool.query(`CREATE SCHEMA "${schema}"`);created=true;
  await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs(id uuid PRIMARY KEY,status text,lease_owner text,lease_token uuid,lease_expires_at timestamptz)`);
  const c=await scoped.connect();try{await c.query('BEGIN');await c.query(await readFile(new URL('../server/storage/migrations/006_asr_semaphore.sql',import.meta.url),'utf8'));await c.query('COMMIT');}finally{c.release();}
  const firstOwner={jobId:randomUUID(),jobLeaseToken:randomUUID(),workerId:'live-worker-0',chunkIndex:0};
  const owners=[firstOwner,{...firstOwner,chunkIndex:1},
    {jobId:randomUUID(),jobLeaseToken:randomUUID(),workerId:'live-worker-1',chunkIndex:0}];
  for(const owner of [owners[0],owners[2]])await pool.query(`INSERT INTO "${schema}".toolnet_srt_jobs VALUES($1,'processing',$2,$3,now()+interval '5 minutes')`,[owner.jobId,owner.workerId,owner.jobLeaseToken]);
  const firstProxy=await proxy(),first=spawnWorker(owners[0],firstProxy.port);
  await until(()=>{const result=first.messages.find(m=>m.type==='result');if(result)throw Error(`PARTITION_CHILD_${result.code}_${result.detail??'NONE'}`);if(first.exit)throw Error('PARTITION_CHILD_EXITED');return first.messages.some(m=>m.type==='provider_request_started');});
  firstProxy.cut();
  await until(()=>first.messages.find(m=>m.type==='result'),45000);
  assert.equal(first.messages.find(m=>m.type==='result').code,'ASR_SLOT_LOST');
  await until(()=>first.exit,10000);
  assert.equal(first.exit.code,0);
  const afterPartition=await pool.query(`SELECT count(*)::int AS n FROM "${schema}".toolnet_srt_asr_slots WHERE job_id=$1`,[owners[0].jobId]);
  assert.equal(afterPartition.rows[0].n,1);
  console.log(`${process.env.TEST_FAKE_PROVIDER==='true'?'MOCK':'REAL'} PROVIDER REQUEST + NEON PROXY PARTITION: PASS`);
  const secondProxy=await proxy(),second=spawnWorker(owners[1],secondProxy.port);
  await until(()=>{if(second.exit)throw Error('KILL_CHILD_EXITED');return second.messages.some(m=>m.type==='provider_request_started');});
  second.child.kill('SIGKILL');await until(()=>second.exit,5000);assert.equal(second.exit.signal,'SIGKILL');
  const slots=await pool.query(`SELECT count(*)::int AS n FROM "${schema}".toolnet_srt_asr_slots WHERE reclaim_after>now()`);
  assert.equal(slots.rows[0].n,2);
  const semaphore=createPostgresAsrSemaphore({pool:scoped,limit:2,leaseSeconds:30,quarantineSeconds:300});
  assert.equal(await semaphore.acquire(owners[2]),null);
  console.log(`SIGKILL DURING ${process.env.TEST_FAKE_PROVIDER==='true'?'MOCK':'REAL'} PROVIDER REQUEST + QUARANTINE: PASS`);
} catch(error){process.exitCode=1;console.error('LIVE DISRUPTION FAILED:',error?.code||error?.message||'UNKNOWN');
  console.error('SANITIZED CHILD STATE:',JSON.stringify(children.map(c=>({exit:c.exit,events:c.messages.filter(m=>['stage','result','fatal','provider_request_started'].includes(m.type))}))));}
finally {
  for(const item of children)if(!item.exit)item.child.kill('SIGKILL');
  for(const item of children)if(!item.exit)await until(()=>item.exit,5000).catch(()=>{cleanup=false;});
  for(const item of proxies)await item.close().catch(()=>{cleanup=false;});
  if(created&&cleanup)try{await pool.query(`DROP SCHEMA "${schema}" CASCADE`);}catch{cleanup=false;}
  await pool.end().catch(()=>{cleanup=false;});
  if(!cleanup){process.exitCode=1;console.error('TEST CLEANUP FAILED:',schema);}else console.log('TEST SCHEMA CLEANED: PASS');
}
