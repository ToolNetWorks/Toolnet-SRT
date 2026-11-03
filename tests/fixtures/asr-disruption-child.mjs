import pg from 'pg';
import { createPostgresAsrSemaphore } from '../../server/storage/postgres-asr-semaphore.mjs';
import { runWithAsrSlot } from '../../server/storage/asr-slot-runner.mjs';
import { transcribe } from '../../dist/index.js';
process.on('uncaughtExceptionMonitor',error=>{const code=typeof error?.code==='string'&&/^[A-Z0-9_]{1,20}$/.test(error.code)?error.code:null;
  const msg=String(error?.message??''),stack=String(error?.stack??'');
  const category=/connection terminated/i.test(msg)?'CONNECTION_TERMINATED':/client has encountered/i.test(msg)?'CLIENT_CONNECTION_ERROR':/socket hang up/i.test(msg)?'SOCKET_HANGUP':/timeout/i.test(msg)?'TIMEOUT':'OTHER';
  const origin=stack.includes('pg-pool')?'PG_POOL':stack.includes('node_modules/pg/')?'PG_CLIENT':stack.includes('asr-slot-runner')?'SLOT_RUNNER':'OTHER';
  process.send?.({type:'fatal',name:error?.name??'Error',code,category,origin});});
const schema=process.env.TEST_ASR_SCHEMA,port=Number(process.env.TEST_PROXY_PORT);
if(!/^srt_asr_live_[a-f0-9]{12}$/.test(schema??'')||!Number.isSafeInteger(port))throw Error('INVALID_TEST_CONFIG');
const url=new URL(process.env.DATABASE_URL),owner=JSON.parse(process.env.TEST_ASR_OWNER);
const pool=new pg.Pool({host:'127.0.0.1',port,user:decodeURIComponent(url.username),password:decodeURIComponent(url.password),
  database:decodeURIComponent(url.pathname.slice(1)),ssl:{rejectUnauthorized:true,servername:url.hostname},max:2,connectionTimeoutMillis:5000});
pool.on('error',()=>{});
const scoped={async connect(){const c=await pool.connect();return {async query(sql,args){
  if(sql==='BEGIN'){await c.query('BEGIN');await c.query(`SET LOCAL search_path TO "${schema}"`);return;}
  return c.query(sql,args);
},release(error){c.release(error);}};}};
const gate=createPostgresAsrSemaphore({pool:scoped,limit:2,leaseSeconds:30,quarantineSeconds:300});
// Model the HTTP server's active handles while the isolated worker runs.
const keepAlive=setInterval(()=>{},1000);
let notified=false;const original=globalThis.fetch;
const notify=message=>new Promise(resolve=>{if(process.send&&process.connected)process.send(message,()=>resolve());else resolve();});
const observedGate={...gate,
  async acquire(value){await notify({type:'stage',name:'acquire'});const result=await gate.acquire(value);await notify({type:'stage',name:'acquired'});return result;},
  async heartbeat(value){await notify({type:'stage',name:'heartbeat'});return gate.heartbeat(value);},
};
globalThis.fetch=(...args)=>{
  const result=original(...args);
  // Notify only once a real submitted ASR task is being polled.
  if(!notified && String(args[0]).includes('/lv/v1/audio_subtitle/query')){
    notified=true;process.send?.({type:'provider_request_started'});
  }
  return result;
};
try {
  await runWithAsrSlot({semaphore:observedGate,owner,heartbeatMs:1000,dbOperationTimeoutMs:1200,requestTimeoutMs:30000,
    ensureOwnership:async()=>{},transcribe:process.env.TEST_FAKE_PROVIDER==='true'
      ? async signal=>{await notify({type:'provider_request_started'});return new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(Error('ABORTED')),{once:true}));}
      : signal=>transcribe({input:process.env.TEST_ASR_AUDIO,signal,queryTimeoutMs:30000})});
  await notify({type:'result',code:'COMPLETED'});
} catch(error) {
  const code=['ASR_SLOT_LOST','ASR_REQUEST_TIMEOUT','ASR_DATABASE_TIMEOUT'].includes(error?.message)?error.message:'OTHER_FAILURE';
  const detail=typeof error?.code==='string'&&/^[A-Z0-9_]{1,20}$/.test(error.code)?error.code:null;
  await notify({type:'result',code,detail});
} finally {
  globalThis.fetch=original;
  clearInterval(keepAlive);
  await pool.end().catch(()=>{});
  process.disconnect?.();
}
