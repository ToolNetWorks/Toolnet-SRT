import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { createPostgresJobStore } from '../server/storage/postgres.mjs';
if(!process.env.DATABASE_URL)throw Error('DATABASE_URL_MISSING');
const schema=`srt_migration_audit_${randomBytes(6).toString('hex')}`;
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1});
let created=false,client,passed=false,cleanup=true;
async function tx(operation){await client.query('BEGIN');try{await client.query(`SET LOCAL search_path TO "${schema}"`);const r=await operation();await client.query('COMMIT');return r;}catch(e){await client.query('ROLLBACK').catch(()=>{});throw e;}}
try{
 client=await pool.connect();await client.query(`CREATE SCHEMA "${schema}"`);created=true;
 await tx(()=>createPostgresJobStore({pool:client}).initialize());
 for(const name of ['002_job_leases.sql','003_remote_fetch_queue.sql','004_remote_idempotency.sql','005_chunk_checkpoints.sql']){
  let sql=await readFile(new URL(`../server/storage/migrations/${name}`,import.meta.url),'utf8');
  // 002 owns BEGIN/COMMIT; remove its wrappers so SET LOCAL is in the same transaction under a pooler.
  if(name.startsWith('002_'))sql=sql.replace(/^BEGIN;\s*/,'').replace(/\s*COMMIT;\s*$/,'');
  await tx(()=>client.query(sql));await tx(()=>client.query(sql));
 }
 const indexes=await client.query('SELECT indexname FROM pg_indexes WHERE schemaname=$1',[schema]);
 const names=new Set(indexes.rows.map(row=>row.indexname));
 for(const name of ['idx_toolnet_srt_jobs_claim','idx_toolnet_srt_jobs_lease_expiry','toolnet_srt_remote_fetch_pending_idx','toolnet_srt_remote_fetch_expired_idx','toolnet_srt_remote_idempotency_pkey','toolnet_srt_job_chunks_pkey'])assert.ok(names.has(name),name);
 const constraints=await client.query(`SELECT conrelid::regclass::text AS tbl,contype,confdeltype FROM pg_constraint WHERE connamespace=$1::regnamespace`,[schema]);
 const fks=constraints.rows.filter(row=>row.contype==='f'&&row.confdeltype==='c');assert.equal(fks.length,3);
 console.log('FRESH SCHEMA MIGRATIONS 002-005 / REPEAT: PASS');
 console.log('QUEUE INDEXES / CASCADE FOREIGN KEYS: PASS');passed=true;
}catch(e){process.exitCode=1;console.error('MIGRATION AUDIT FAILED:',e.code||e.message);}
finally{if(created&&client)try{await client.query(`DROP SCHEMA "${schema}" CASCADE`);}catch{cleanup=false;}client?.release();await pool.end().catch(()=>{cleanup=false;});if(!cleanup){process.exitCode=1;console.error('SCHEMA CLEANUP FAILED:',schema);}else console.log('TEST SCHEMA CLEANED: PASS');}
if(passed&&cleanup)console.log('TOOLNET-SRT MIGRATION AUDIT PASSED');
