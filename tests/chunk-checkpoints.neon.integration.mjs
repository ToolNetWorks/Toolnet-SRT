import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { createPostgresJobStore } from '../server/storage/postgres.mjs';
import { createPostgresLeaseManager } from '../server/storage/leases.mjs';
import { createPostgresChunkCheckpoints } from '../server/storage/postgres-chunk-checkpoints.mjs';
if (!process.env.DATABASE_URL) throw Error('DATABASE_URL_MISSING');
const schema = `srt_chunks_${randomBytes(6).toString('hex')}`;
const control = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1 });
const workers = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 3 });
let created = false, cleanup = true, passed = false;
const db = {
  async connect() {
    const c = await workers.connect();
    return { async query(sql, params) {
      if (sql === 'BEGIN') { try { await c.query('BEGIN'); await c.query(`SET LOCAL search_path TO "${schema}"`); }
        catch(e) { await c.query('ROLLBACK').catch(() => {}); throw e; } return; }
      return c.query(sql, params);
    }, release(error) { c.release(error); } };
  },
  async query(sql, params) {
    const c = await this.connect();
    try { await c.query('BEGIN'); const result = await c.query(sql, params); await c.query('COMMIT'); return result; }
    catch(e) { await c.query('ROLLBACK').catch(() => {}); throw e; } finally { c.release(); }
  },
};
try {
  const base = (await control.query('SELECT current_schema() AS name')).rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(base ?? '')) throw Error('INVALID_BASE_SCHEMA');
  await control.query(`CREATE SCHEMA "${schema}"`); created = true;
  await control.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
  const migration = await readFile(new URL('../server/storage/migrations/005_chunk_checkpoints.sql',import.meta.url),'utf8');
  await db.query(migration); await db.query(migration);
  const store = createPostgresJobStore({ pool: db });
  const leases = createPostgresLeaseManager({ pool: db });
  const checkpoints = createPostgresChunkCheckpoints({ pool: db });
  const id = randomUUID();
  await store.saveJob({ id, status:'queued', stage:'queued', progress:0, completedSegments:0, totalSegments:null,
    extension:'.mp3', sourceKey:`toolnet-srt/jobs/${id}/source.mp3`, resultKey:null });
  const first = await leases.claimJob('worker-a');
  const a = { jobId:id, workerId:'worker-a', leaseToken:first.lease_token };
  const plan = [{index:0,startOffsetMs:0,durationMs:600000,overlapMs:0},
    {index:1,startOffsetMs:590000,durationMs:600000,overlapMs:10000}];
  assert.equal((await checkpoints.initialize(a, plan)).length,2);
  assert.equal((await checkpoints.claim(a,0)).attempt_count,1);
  assert.equal((await checkpoints.complete(a,0,[{text:'hello',startMs:100,endMs:200}])).status,'completed');
  assert.equal((await checkpoints.claim(a,1)).attempt_count,1);
  await control.query(`UPDATE "${schema}".toolnet_srt_jobs SET lease_expires_at=NOW()-interval '1 second' WHERE id=$1`,[id]);
  const second = await leases.claimJob('worker-b');
  const b = { jobId:id, workerId:'worker-b', leaseToken:second.lease_token };
  assert.notEqual(a.leaseToken,b.leaseToken);
  assert.equal(await checkpoints.complete(a,1,[]),null);
  const restored = await checkpoints.initialize(b, plan);
  assert.equal(restored[0].status,'completed'); assert.equal(restored[0].segments[0].text,'hello');
  assert.equal(await checkpoints.claim(b,0),null);
  assert.equal((await checkpoints.claim(b,1)).attempt_count,2);
  assert.equal((await checkpoints.fail(b,1,'CHUNK_ASR_FAILED')).status,'failed');
  assert.equal(await checkpoints.claim(b,1,{maxAttempts:2}),null);
  assert.equal((await checkpoints.claim(b,1)).attempt_count,3);
  assert.equal((await checkpoints.complete(b,1,[])).status,'completed');
  await assert.rejects(checkpoints.initialize(b,[plan[0],{...plan[1],startOffsetMs:1}]),/CHUNK_PLAN_MISMATCH/);
  console.log('CHECKPOINT PERSISTENCE / RETRY / LEASE FENCING: PASS');
  console.log('MIGRATION IDEMPOTENCY / PLAN VALIDATION: PASS'); passed = true;
} catch(error) { process.exitCode=1; console.error('CHUNK CHECKPOINT TEST FAILED:',error.code || error.message); }
finally {
  await workers.end().catch(()=>{cleanup=false;});
  if(created) try { await control.query(`DROP SCHEMA "${schema}" CASCADE`); } catch { cleanup=false; }
  await control.end().catch(()=>{cleanup=false;});
  if (!cleanup) { process.exitCode=1; console.error('SCHEMA CLEANUP FAILED:',schema); }
  else console.log('TEST SCHEMA CLEANED: PASS');
}
if(passed && cleanup) console.log('TOOLNET-SRT CHUNK CHECKPOINT NEON PASSED');
