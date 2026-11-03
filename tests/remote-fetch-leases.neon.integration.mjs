import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { createPostgresRemoteQueue } from '../server/storage/postgres-remote-queue.mjs';
import { createPostgresRemoteLeases } from '../server/storage/postgres-remote-leases.mjs';
import { createRemoteUrlVault } from '../server/storage/remote-url-vault.mjs';

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL_MISSING');
  process.exit(1);
}

const schema = `srt_fetch_lease_${randomBytes(6).toString('hex')}`;
const control = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1 });
const workers = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 5 });
const vault = createRemoteUrlVault({ key: randomBytes(32).toString('base64url') });
const url = 'https://media.example/a.mp4?token=private123';
let schemaCreated = false;
let passed = false;
let cleanupPassed = true;

// This wrapper scopes every transaction, including concurrent claims,
// to the isolated Neon schema. Never use the production search_path.
const isolatedPool = {
  async connect() {
    const raw = await workers.connect();
    return {
      async query(sql, params) {
        if (sql === 'BEGIN') {
          try {
            await raw.query('BEGIN');
            await raw.query(`SET LOCAL search_path TO "${schema}"`);
            const check = await raw.query('SELECT current_schema() AS schema');
            assert.equal(check.rows[0].schema, schema);
          } catch (error) {
            await raw.query('ROLLBACK').catch(() => {});
            throw error;
          }
          return;
        }
        return raw.query(sql, params);
      },
      release() { raw.release(); },
    };
  },
};

async function fetchRows(jobId) {
  const { rows } = await control.query(`
    SELECT q.status AS fetch_status, q.attempt_count,
           q.encrypted_url, q.lease_token,
           j.status AS job_status, j.source_key, j.stage
    FROM "${schema}".toolnet_srt_remote_fetch_jobs q
    JOIN "${schema}".toolnet_srt_jobs j ON j.id = q.job_id
    WHERE q.job_id = $1::uuid
  `, [jobId]);
  assert.equal(rows.length, 1);
  return rows[0];
}

async function mutate(sql, args = []) {
  return control.query(sql, args);
}

try {
  const { rows } = await control.query('SELECT current_schema() AS name');
  const base = rows[0]?.name;
  assert.match(base, /^[a-z_][a-z0-9_]*$/i);
  await control.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await control.query(`
    CREATE TABLE "${schema}".toolnet_srt_jobs (
      LIKE "${base}".toolnet_srt_jobs INCLUDING ALL
    )
  `);
  const migration = await readFile(new URL(
    '../server/storage/migrations/003_remote_fetch_queue.sql', import.meta.url
  ), 'utf8');
  const setup = await isolatedPool.connect();
  try {
    await setup.query('BEGIN');
    await setup.query(migration);
    await setup.query('COMMIT');
  } finally { setup.release(); }

  const queue = createPostgresRemoteQueue({
    pool: isolatedPool, vault, allowedHosts: ['media.example'],
  });
  const leases = createPostgresRemoteLeases({
    pool: isolatedPool, maxAttempts: 3,
    leaseSeconds: 30, retryDelaySeconds: 1,
  });

  const first = await queue.enqueue(url);
  const a = await leases.claimNext('worker-a');
  assert.equal(a.jobId, first.id);
  assert.equal(a.attemptCount, 1);
  assert.equal(vault.decrypt(a.encryptedUrl, first.id), url);
  assert.equal(await leases.claimNext('worker-b'), null);
  assert.equal((await fetchRows(first.id)).source_key, null);
  console.log('ATOMIC CLAIM AND ASR GATE: PASS');

  assert.equal(await leases.heartbeat({ ...a, token: '00000000-0000-4000-8000-000000000000' }), false);
  assert.equal(await leases.heartbeat(a), true);
  assert.equal(await leases.completeFetch({ ...a,
    sourceKey: `toolnet-srt/jobs/${a.jobId}/remote/${a.token}/source.mp4`,
  }), true);
  assert.equal(await leases.heartbeat(a), false);
  assert.equal(await leases.completeFetch({ ...a,
    sourceKey: `toolnet-srt/jobs/${a.jobId}/remote/${a.token}/source.mp4`,
  }), false);
  const ready = await fetchRows(first.id);
  assert.equal(ready.fetch_status, 'ready');
  assert.equal(ready.encrypted_url, null);
  assert.equal(ready.stage, 'queued');
  assert.ok(ready.source_key.includes(a.token));
  console.log('FENCED COMPLETION AND URL ERASURE: PASS');

  const second = await queue.enqueue(url);
  const old = await leases.claimNext('worker-old');
  assert.equal(old.jobId, second.id);
  await mutate(`
    UPDATE "${schema}".toolnet_srt_remote_fetch_jobs
    SET lease_expires_at = NOW() - INTERVAL '1 second'
    WHERE job_id = $1::uuid
  `, [second.id]);
  assert.equal(await leases.heartbeat(old), false);
  const newer = await leases.claimNext('worker-new');
  assert.equal(newer.jobId, second.id);
  assert.equal(newer.attemptCount, 2);
  assert.notEqual(newer.token, old.token);
  assert.equal(await leases.completeFetch({ ...old,
    sourceKey: `toolnet-srt/jobs/${old.jobId}/remote/${old.token}/source.mp4`,
  }), false);
  console.log('EXPIRED LEASE RECLAIM AND STALE TOKEN REJECTION: PASS');

  const retry = await leases.failFetch({ ...newer,
    errorCode: 'SOURCE_HTTP_STATUS', retryable: true,
  });
  assert.equal(retry.updated, true);
  assert.equal(retry.status, 'pending');
  assert.equal((await fetchRows(second.id)).job_status, 'queued');
  await mutate(`
    UPDATE "${schema}".toolnet_srt_remote_fetch_jobs
    SET available_at = NOW() - INTERVAL '1 second'
    WHERE job_id = $1::uuid
  `, [second.id]);
  const last = await leases.claimNext('worker-final');
  assert.equal(last.attemptCount, 3);
  const failed = await leases.failFetch({ ...last, retryable: true });
  assert.equal(failed.status, 'failed');
  const terminal = await fetchRows(second.id);
  assert.equal(terminal.fetch_status, 'failed');
  assert.equal(terminal.job_status, 'failed');
  assert.equal(terminal.encrypted_url, null);
  assert.equal(await leases.claimNext('worker-final'), null);
  console.log('RETRY LIMIT AND SECRET ERASURE: PASS');

  const third = await queue.enqueue(url);
  const abandoned = await leases.claimNext('worker-abandoned');
  assert.equal(abandoned.jobId, third.id);
  await mutate(`
    UPDATE "${schema}".toolnet_srt_remote_fetch_jobs
    SET attempt_count = 3,
        lease_expires_at = NOW() - INTERVAL '1 second'
    WHERE job_id = $1::uuid
  `, [third.id]);
  assert.equal(await leases.reapExhausted(), 1);
  assert.equal(await leases.reapExhausted(), 0);
  const reaped = await fetchRows(third.id);
  assert.equal(reaped.job_status, 'failed');
  assert.equal(reaped.encrypted_url, null);
  console.log('EXHAUSTED LEASE REAPER: PASS');

  const fourth = await queue.enqueue(url);
  const fifth = await queue.enqueue(url);
  const [w1, w2] = await Promise.all([
    leases.claimNext('parallel-one'), leases.claimNext('parallel-two'),
  ]);
  assert.ok(w1 && w2);
  assert.notEqual(w1.jobId, w2.jobId);
  assert.deepEqual(new Set([w1.jobId, w2.jobId]),
    new Set([fourth.id, fifth.id]));
  console.log('CONCURRENT CLAIM SKIP LOCKED: PASS');

  const sixth = await queue.enqueue(url);
  await mutate(`
    UPDATE "${schema}".toolnet_srt_jobs
    SET status = 'cancelled'
    WHERE id = $1::uuid
  `, [sixth.id]);
  assert.equal(await leases.claimNext('parallel-three'), null);
  console.log('CANCELLED PARENT NOT CLAIMED: PASS');

  passed = true;
} catch (error) {
  process.exitCode = 1;
  console.error('REMOTE LEASE E2E FAILED:', error.code || error.message);
} finally {
  await workers.end().catch(() => { cleanupPassed = false; });
  if (schemaCreated) {
    try {
      await control.query(`DROP SCHEMA "${schema}" CASCADE`);
      console.log('TEST SCHEMA CLEANED: PASS');
    } catch {
      cleanupPassed = false;
      console.error('SCHEMA CLEANUP FAILED:', schema);
    }
  }
  await control.end().catch(() => { cleanupPassed = false; });
  if (!cleanupPassed) process.exitCode = 1;
}
if (passed && cleanupPassed) {
  console.log('TOOLNET-SRT REMOTE FETCH LEASES PASSED');
}
