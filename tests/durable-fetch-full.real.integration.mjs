import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { S3Client, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { createPostgresRemoteQueue } from '../server/storage/postgres-remote-queue.mjs';
import { createPostgresRemoteLeases } from '../server/storage/postgres-remote-leases.mjs';
import { createPostgresLeaseManager } from '../server/storage/leases.mjs';
import { createPostgresProgressManager } from '../server/storage/progress.mjs';
import { createRemoteUrlVault } from '../server/storage/remote-url-vault.mjs';
import { createR2Storage } from '../server/storage/r2.mjs';
import { createAttemptResultStorage } from '../server/storage/attempt-results.mjs';
import { createDurableFetchWorker } from '../server/storage/durable-fetch-worker.mjs';
import { createDurableFetchScheduler } from '../server/storage/durable-fetch-scheduler.mjs';
import { createDurableWorker } from '../server/storage/durable-worker.mjs';
import { createDurableScheduler } from '../server/storage/durable-scheduler.mjs';

const env = process.env;
if (!env.DATABASE_URL || !env.R2_ACCOUNT_ID || !env.R2_BUCKET) throw new Error('TEST_ENV_MISSING');
const schema = `srt_full_e2e_${randomBytes(6).toString('hex')}`;
const control = new pg.Pool({ connectionString: env.DATABASE_URL, max: 1 });
const workers = new pg.Pool({ connectionString: env.DATABASE_URL, max: 5 });
const client = new S3Client({ region: 'auto', endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`, forcePathStyle: true,
  requestChecksumCalculation: 'WHEN_REQUIRED', credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey: env.R2_SECRET_ACCESS_KEY } });
const storage = createR2Storage({ env, client });
const results = createAttemptResultStorage({ env, client });
const keys = new Set(); let created = false, cleanup = true, passed = false;
let fetchScheduler, asrScheduler;
const pool = {
  async connect() {
    const c = await workers.connect();
    return { async query(sql, params) {
      if (sql === 'BEGIN') {
        try { await c.query('BEGIN'); await c.query(`SET LOCAL search_path TO "${schema}"`); }
        catch (e) { await c.query('ROLLBACK').catch(() => {}); throw e; }
        return;
      }
      return c.query(sql, params);
    }, release(error) { c.release(error); } };
  },
  async query(sql, params) {
    const c = await this.connect();
    try { await c.query('BEGIN'); const result = await c.query(sql, params); await c.query('COMMIT'); return result; }
    catch (e) { await c.query('ROLLBACK').catch(() => {}); throw e; }
    finally { c.release(); }
  },
};
async function until(check, timeout = 180000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { if (await check()) return; await new Promise(r => setTimeout(r, 250)); }
  throw new Error('E2E_TIMEOUT');
}
try {
  const base = (await control.query('SELECT current_schema() AS name')).rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(base ?? '')) throw Error('INVALID_BASE_SCHEMA');
  await control.query(`CREATE SCHEMA "${schema}"`); created = true;
  await control.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
  await pool.query(await readFile(new URL('../server/storage/migrations/003_remote_fetch_queue.sql', import.meta.url), 'utf8'));
  const vault = createRemoteUrlVault({ key: randomBytes(32).toString('base64url') });
  const remote = createPostgresRemoteLeases({ pool });
  const leases = createPostgresLeaseManager({ pool });
  const progress = createPostgresProgressManager({ pool });
  const queue = createPostgresRemoteQueue({ pool, vault, allowedHosts: ['storage.googleapis.com'] });
  const job = await queue.enqueue('https://storage.googleapis.com/cloud-samples-data/speech/hello.wav');
  const recordingStorage = { ...storage,
    async uploadRemoteSource(args) {
      const { remoteSourceKey } = await import('../server/storage/source-keys.mjs');
      keys.add(remoteSourceKey(args.jobId, args.fetchToken, args.filename));
      return storage.uploadRemoteSource(args);
    },
  };
  const recordingResults = { ...results, async putResult(args) {
    const { attemptResultKey } = await import('../server/storage/attempt-results.mjs');
    keys.add(attemptResultKey(args.jobId, args.leaseToken));
    return results.putResult(args);
  } };
  fetchScheduler = createDurableFetchScheduler({ pollIntervalMs: 100, errorBackoffMs: 500,
    workerFactory({ workerId }) { return createDurableFetchWorker({ leases: remote, vault, storage: recordingStorage, workerId,
      allowedHosts: ['storage.googleapis.com'], maxBytes: 5 * 1024 * 1024, heartbeatIntervalMs: 5000 }); } });
  asrScheduler = createDurableScheduler({ pollIntervalMs: 100, errorBackoffMs: 500,
    workerFactory({ workerId }) { return createDurableWorker({ leases, progress, storage, results: recordingResults, workerId }); } });
  fetchScheduler.start(); asrScheduler.start();
  await until(async () => {
    const row = (await pool.query('SELECT status, error_code FROM toolnet_srt_jobs WHERE id=$1', [job.id])).rows[0];
    if (row?.status === 'failed') throw Error(`JOB_FAILED_${row.error_code}`);
    return row?.status === 'completed';
  });
  assert.equal((await fetchScheduler.stop()).stopped, true);
  assert.equal((await asrScheduler.stop()).stopped, true);
  const row = (await pool.query('SELECT source_key, result_key, progress FROM toolnet_srt_jobs WHERE id=$1', [job.id])).rows[0];
  assert.match(row.source_key, /\/remote\//); assert.equal(row.progress, 100);
  const srt = await results.getResult({ key: row.result_key });
  assert.match(srt.toString('utf8'), /-->/);
  console.log('NEON ENQUEUE + FETCH SCHEDULER + REAL PINNED HTTPS: PASS');
  console.log('R2 SOURCE + NEON COMPLETE + ASR SCHEDULER: PASS');
  console.log('REAL JIANYING ASR + R2 SRT: PASS');
  passed = true;
} catch (error) { console.error('FULL REAL E2E FAILED:', error?.code || error?.message || error?.name); process.exitCode = 1; }
finally {
  for (const s of [fetchScheduler, asrScheduler]) if (s) try { if (!(await s.stop({ timeoutMs: 30000 })).stopped) cleanup = false; } catch { cleanup = false; }
  await workers.end().catch(() => { cleanup = false; });
  if (created && cleanup) try { await control.query(`DROP SCHEMA "${schema}" CASCADE`); } catch { cleanup = false; console.error('SCHEMA CLEANUP FAILED:', schema); }
  await control.end().catch(() => { cleanup = false; });
  for (const key of keys) try { await client.send(new DeleteObjectCommand({ Bucket: env.R2_BUCKET, Key: key })); } catch { cleanup = false; }
  client.destroy();
  if (!cleanup) { process.exitCode = 1; console.error('TEST CLEANUP FAILED:', schema); }
  else console.log('TEST RESOURCES CLEANED: PASS');
}
if (passed && cleanup) console.log('TOOLNET-SRT FULL REAL FETCH E2E PASSED');
