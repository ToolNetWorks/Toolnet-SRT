import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { createPostgresRemoteQueue } from '../server/storage/postgres-remote-queue.mjs';
import { createPostgresLeaseManager } from '../server/storage/leases.mjs';
import { createRemoteUrlVault } from '../server/storage/remote-url-vault.mjs';

if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL_MISSING');
const schema = `srt_idempotency_${randomBytes(6).toString('hex')}`;
const control = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1, connectionTimeoutMillis: 10000 });
const workers = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 12, connectionTimeoutMillis: 10000 });
const vault = createRemoteUrlVault({ key: randomBytes(32).toString('base64url') });
const secret = randomBytes(32).toString('base64url');
const url = 'https://media.example/video.mp4?token=private123';
let schemaCreated = false, cleanupOk = true, passed = 0, failed = 0;
let failAfterCommit = false;
const scoped = {
  async connect() {
    const client = await workers.connect();
    return {
      async query(sql, params) {
        if (sql === 'BEGIN') {
          try {
            await client.query('BEGIN');
            await client.query(`SET LOCAL search_path TO "${schema}"`);
            await client.query("SET LOCAL statement_timeout = '20000ms'");
            const check = await client.query('SELECT current_schema() AS name');
            assert.equal(check.rows[0].name, schema);
          } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
          return;
        }
        const result = await client.query(sql, params);
        if (sql === 'COMMIT' && failAfterCommit) {
          failAfterCommit = false;
          throw new Error('SIMULATED_LOST_COMMIT_ACK');
        }
        return result;
      },
      release(error) { client.release(error); },
    };
  },
  async query(sql, params) {
    const client = await this.connect();
    try {
      await client.query('BEGIN');
      const result = await client.query(sql, params);
      await client.query('COMMIT');
      return result;
    } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
    finally { client.release(); }
  },
};
const config = { pool: scoped, vault, allowedHosts: ['media.example'], idempotencySecret: secret };
const queue = createPostgresRemoteQueue(config);
async function check(name, fn) {
  try { await fn(); passed++; console.log(`${name}: PASS`); }
  catch (error) { failed++; console.error(`${name}: FAIL`); throw error; }
}
async function counts() {
  const { rows } = await control.query(`SELECT
    (SELECT count(*)::int FROM "${schema}".toolnet_srt_jobs) AS jobs,
    (SELECT count(*)::int FROM "${schema}".toolnet_srt_remote_fetch_jobs) AS fetches,
    (SELECT count(*)::int FROM "${schema}".toolnet_srt_remote_idempotency) AS keys`);
  return rows[0];
}
try {
  const base = (await control.query('SELECT current_schema() AS name')).rows[0]?.name;
  assert.match(base, /^[a-z_][a-z0-9_]*$/i);
  await control.query(`CREATE SCHEMA "${schema}"`); schemaCreated = true;
  await control.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
  const migrations = await Promise.all(['003_remote_fetch_queue.sql', '004_remote_idempotency.sql'].map(name =>
    readFile(new URL(`../server/storage/migrations/${name}`, import.meta.url), 'utf8')));
  const setup = await scoped.connect();
  try {
    await setup.query('BEGIN');
    for (const migration of migrations) await setup.query(migration);
    await setup.query(migrations[1]);
    await setup.query('COMMIT');
  } catch (error) { await setup.query('ROLLBACK').catch(() => {}); throw error; }
  finally { setup.release(); }
  await check('MIGRATION 004 REPEATED', async () => {
    const result = await control.query(`SELECT to_regclass($1) AS name`, [`${schema}.toolnet_srt_remote_idempotency`]);
    assert.ok(result.rows[0].name);
  });
  let first;
  await check('FIRST ENQUEUE', async () => {
    first = await queue.enqueue(url, { idempotencyKey: 'key-first', scope: 'client-a' });
    assert.equal(first.status, 'queued'); assert.equal(first.stage, 'fetch_pending');
    assert.deepEqual(await counts(), { jobs: 1, fetches: 1, keys: 1 });
  });
  await check('SAME REQUEST RETRY', async () => {
    const again = await queue.enqueue(url, { idempotencyKey: 'key-first', scope: 'client-a' });
    assert.equal(again.id, first.id);
  });
  await check('CONCURRENT ENQUEUE', async () => {
    const before = await counts();
    const results = await Promise.all(Array.from({ length: 10 }, () =>
      queue.enqueue(url, { idempotencyKey: 'parallel', scope: 'client-a' })));
    assert.equal(new Set(results.map(job => job.id)).size, 1);
    const after = await counts();
    for (const name of ['jobs', 'fetches', 'keys']) assert.equal(after[name], before[name] + 1);
  });
  await check('DIFFERENT URL CONFLICT', async () => {
    const before = await counts();
    await assert.rejects(queue.enqueue(url + 'x', { idempotencyKey: 'key-first', scope: 'client-a' }),
      { code: 'IDEMPOTENCY_KEY_CONFLICT', status: 409 });
    assert.deepEqual(await counts(), before);
    const results = await Promise.allSettled([
      queue.enqueue(url, { idempotencyKey: 'parallel-conflict' }),
      queue.enqueue(url + 'x', { idempotencyKey: 'parallel-conflict' }),
    ]);
    assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
    const rejection = results.find(r => r.status === 'rejected');
    assert.equal(rejection.reason.code, 'IDEMPOTENCY_KEY_CONFLICT');
  });
  await check('DISTINCT KEYS AND SCOPES', async () => {
    const jobs = await Promise.all([
      queue.enqueue(url, { idempotencyKey: 'a', scope: 'client-a' }),
      queue.enqueue(url, { idempotencyKey: 'b', scope: 'client-a' }),
      queue.enqueue(url, { idempotencyKey: 'a', scope: 'client-b' }),
    ]);
    assert.equal(new Set(jobs.map(job => job.id)).size, 3);
  });
  await check('TRANSACTION ROLLBACK', async () => {
    const before = await counts();
    const bad = createPostgresRemoteQueue({ ...config, vault: { encrypt: () => '' } });
    await assert.rejects(bad.enqueue(url, { idempotencyKey: 'rollback' }),
      { code: 'REMOTE_QUEUE_WRITE_FAILED', status: 503 });
    const brokenPool = {
      async connect() {
        const client = await scoped.connect();
        return {
          query(sql, params) {
            if (sql.includes('INSERT INTO toolnet_srt_remote_idempotency')) {
              throw new Error('PRIVATE_SQL_DETAILS');
            }
            return client.query(sql, params);
          },
          release(error) { client.release(error); },
        };
      },
    };
    const broken = createPostgresRemoteQueue({ ...config, pool: brokenPool });
    await assert.rejects(broken.enqueue(url, { idempotencyKey: 'rollback-late' }),
      { code: 'REMOTE_QUEUE_WRITE_FAILED', status: 503, message: 'REMOTE_QUEUE_WRITE_FAILED' });
    assert.deepEqual(await counts(), before);
  });
  await check('LOST COMMIT ACK RECOVERY', async () => {
    failAfterCommit = true;
    await assert.rejects(queue.enqueue(url, { idempotencyKey: 'lost-ack' }),
      { code: 'REMOTE_QUEUE_WRITE_FAILED', status: 503 });
    const afterCommit = await counts();
    const committed = (await control.query(`SELECT job_id FROM "${schema}".toolnet_srt_remote_idempotency ORDER BY created_at DESC LIMIT 1`)).rows[0].job_id;
    const recovered = await queue.enqueue(url, { idempotencyKey: 'lost-ack' });
    assert.equal(recovered.id, committed);
    assert.deepEqual(await counts(), afterCommit);
    const matching = await control.query(`SELECT count(*)::int AS n FROM "${schema}".toolnet_srt_remote_idempotency WHERE job_id=$1`, [recovered.id]);
    assert.equal(matching.rows[0].n, 1);
  });
  await check('URL ENCRYPTED AND PUBLIC RESPONSE REDACTED', async () => {
    const row = (await control.query(`SELECT q.encrypted_url, i.key_digest, i.request_digest
      FROM "${schema}".toolnet_srt_remote_fetch_jobs q JOIN "${schema}".toolnet_srt_remote_idempotency i ON i.job_id=q.job_id WHERE q.job_id=$1`, [first.id])).rows[0];
    assert.equal(vault.decrypt(row.encrypted_url, first.id), url);
    assert.ok(!row.encrypted_url.includes('private123'));
    assert.ok(!JSON.stringify(first).includes('private123'));
    assert.ok(!JSON.stringify(first).includes('key-first'));
    assert.equal(row.key_digest.length, 32); assert.equal(row.request_digest.length, 32);
  });
  await check('SOURCE NULL AND ASR CLAIM BLOCKED', async () => {
    const row = await control.query(`SELECT count(*)::int AS n FROM "${schema}".toolnet_srt_jobs WHERE source_key IS NOT NULL`);
    assert.equal(row.rows[0].n, 0);
    const asr = createPostgresLeaseManager({ pool: scoped });
    assert.equal(await asr.claimJob('asr-worker'), null);
  });
  await check('KEY VALIDATION BEFORE DATABASE', async () => {
    let calls = 0;
    const guarded = createPostgresRemoteQueue({ ...config, pool: { connect() { calls++; throw new Error('unexpected'); } } });
    for (const key of ['', 'has space', 'x'.repeat(256)]) {
      await assert.rejects(guarded.enqueue(url, { idempotencyKey: key }), { code: 'INVALID_IDEMPOTENCY_KEY', status: 400 });
    }
    assert.equal(calls, 0);
    assert.throws(() => createPostgresRemoteQueue({ ...config, idempotencySecret: 'bad' }), /INVALID_REMOTE_IDEMPOTENCY_KEY/);
  });
} catch (error) {
  process.exitCode = 1;
  console.error('IDEMPOTENCY TEST FAILED:', error?.code || error?.message || 'UNKNOWN');
} finally {
  await workers.end().catch(() => { cleanupOk = false; });
  if (schemaCreated) {
    try { await control.query(`DROP SCHEMA "${schema}" CASCADE`); console.log('TEST SCHEMA CLEANED: PASS'); }
    catch { cleanupOk = false; console.error('TEST SCHEMA CLEANUP FAILED:', schema); }
  }
  await control.end().catch(() => { cleanupOk = false; });
  if (!cleanupOk) process.exitCode = 1;
  console.log(`IDEMPOTENCY CHECKS: ${passed} PASS, ${failed} FAIL`);
}
if (!process.exitCode) console.log('TOOLNET-SRT REMOTE IDEMPOTENCY NEON PASSED');
