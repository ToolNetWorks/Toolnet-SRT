import pg from 'pg';
import { createPostgresAsrSemaphore } from '../../server/storage/postgres-asr-semaphore.mjs';
const schema=process.env.TEST_ASR_SCHEMA;
if(!/^srt_asr_sem_[a-f0-9]{12}$/.test(schema??''))throw Error('INVALID_SCHEMA');
const owner=JSON.parse(process.env.TEST_ASR_OWNER);
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1});
const scoped={async connect(){const c=await pool.connect();let inTx=false;return {
  async query(sql,args){
    if(sql==='BEGIN'){await c.query('BEGIN');inTx=true;await c.query(`SET LOCAL search_path TO "${schema}"`);return;}
    if(inTx){const result=await c.query(sql,args);if(sql==='COMMIT'||sql==='ROLLBACK')inTx=false;return result;}
    await c.query('BEGIN');try{await c.query(`SET LOCAL search_path TO "${schema}"`);const r=await c.query(sql,args);await c.query('COMMIT');return r;}catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}
  },release(error){c.release(error);}};}};
const semaphore=createPostgresAsrSemaphore({pool:scoped,limit:2,leaseSeconds:30});
let slot;
try{
  slot=await semaphore.acquire(owner);
  await new Promise(resolve=>process.send({slot:slot??null},resolve));
  await new Promise(resolve=>process.once('message',resolve));
  if(slot)await semaphore.release(slot);
}finally{await pool.end();process.disconnect?.();}
