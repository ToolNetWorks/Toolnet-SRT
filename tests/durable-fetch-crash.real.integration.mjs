import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { request } from 'node:https';
import { Transform } from 'node:stream';
import pg from 'pg';
import { S3Client, DeleteObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { createPostgresRemoteQueue } from '../server/storage/postgres-remote-queue.mjs';
import { createPostgresRemoteLeases } from '../server/storage/postgres-remote-leases.mjs';
import { createRemoteUrlVault } from '../server/storage/remote-url-vault.mjs';
import { createR2Storage } from '../server/storage/r2.mjs';
import { createDurableFetchWorker } from '../server/storage/durable-fetch-worker.mjs';
import { createDurableFetchScheduler } from '../server/storage/durable-fetch-scheduler.mjs';
import { downloadPinnedHttps } from '../server/storage/pinned-https.mjs';
import { remoteSourceKey } from '../server/storage/source-keys.mjs';
import { createPostgresLeaseManager } from '../server/storage/leases.mjs';
import { createDurableWorker } from '../server/storage/durable-worker.mjs';
const schemaRe = /^srt_fetch_crash_[a-f0-9]{12}$/;
const env = process.env;
const pause = ms => new Promise(r => setTimeout(r, ms));
async function until(check, ms = 90000) { const end = Date.now()+ms; while (Date.now()<end) { if (await check()) return; await pause(100); } throw Error('WAIT_TIMEOUT'); }
function scoped(pool, schema) {
  if (!schemaRe.test(schema)) throw Error('INVALID_SCHEMA');
  return { async connect() { const c = await pool.connect(); return { async query(sql, params) {
    if (sql === 'BEGIN') { try { await c.query('BEGIN'); await c.query(`SET LOCAL search_path TO "${schema}"`); }
      catch (e) { await c.query('ROLLBACK').catch(() => {}); throw e; } return; }
    return c.query(sql, params);
  }, release(error) { c.release(error); } }; } };
}
async function send(type, data = {}) { if (process.send && process.connected) await new Promise(r => process.send({ type, ...data }, r)); }
function s3client() { return new S3Client({ region: 'auto', endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`, forcePathStyle: true,
  requestChecksumCalculation: 'WHEN_REQUIRED', credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey: env.R2_SECRET_ACCESS_KEY } }); }
async function child() {
  const role = process.argv[3], schema = env.TEST_SCHEMA;
  if (!schemaRe.test(schema ?? '') || !['download','upload','commit','recovery'].includes(role)) throw Error('BAD_CHILD');
  const pgpool = new pg.Pool({ connectionString: env.DATABASE_URL, max: 3 });
  const db = scoped(pgpool, schema), rawClient = s3client();
  const wrappedClient = { async send(command, options) {
    if (role === 'upload' && command instanceof PutObjectCommand) {
      const gate = new Transform({ transform(chunk, encoding, callback) {
        this.push(chunk); void send('upload_blocked'); /* Keep the upload stream open until SIGKILL. */
      } });
      command.input.Body.pipe(gate); command.input.Body = gate;
    }
    return rawClient.send(command, options);
  } };
  const baseStorage = createR2Storage({ env, client: wrappedClient });
  const storage = { ...baseStorage, async uploadRemoteSource(args) {
    await send('object', { key: remoteSourceKey(args.jobId, args.fetchToken, args.filename) });
    return baseStorage.uploadRemoteSource(args);
  } };
  const baseLeases = createPostgresRemoteLeases({ pool: db, leaseSeconds: 30 });
  const leases = { ...baseLeases, async claimNext(workerId) {
    const claim = await baseLeases.claimNext(workerId);
    if (claim) await send('claim', { jobId: claim.jobId, token: claim.token, workerId: claim.workerId });
    return claim;
  }, async completeFetch(args) {
    if (role === 'commit') { await send('commit_blocked'); return new Promise(() => {}); }
    return baseLeases.completeFetch(args);
  } };
  const vault = createRemoteUrlVault({ key: env.TEST_VAULT_KEY });
  const scheduler = createDurableFetchScheduler({ pollIntervalMs: 200, errorBackoffMs: 500,
    workerFactory({ workerId }) { return createDurableFetchWorker({ leases, vault, storage, workerId, allowedHosts: ['storage.googleapis.com'],
      tempRoot: env.TEST_TEMP_ROOT, maxBytes: 8*1024*1024, heartbeatIntervalMs: 2000,
      async download(url, path, options) {
        if (role === 'download') {
          const requestFn = (url, opts, callback) => request(url, opts, response => {
            const gate = new Transform({ transform(chunk, encoding, done) { this.push(chunk); void send('download_blocked'); } });
            gate.statusCode = response.statusCode; gate.headers = response.headers;
            response.pipe(gate); callback(gate);
          });
          return downloadPinnedHttps(url, path, { ...options, requestFn });
        }
        await writeFile(path, role === 'upload' ? Buffer.alloc(4*1024*1024, 1) : 'winning source');
      },
    }); },
  });
  scheduler.start();
  process.on('message', message => { if (message === 'stop') void (async () => {
    const result = await scheduler.stop({ timeoutMs: 10000 });
    if (!result.stopped) throw Error('NOT_STOPPED');
    await pgpool.end(); rawClient.destroy(); await send('stopped'); process.disconnect();
  })().catch(() => { process.exitCode = 1; }); });
}
function spawn(role, schema, vaultKey, dir) {
  const child = fork(fileURLToPath(import.meta.url), ['--child', role], { env: { ...env, TEST_SCHEMA: schema, TEST_VAULT_KEY: vaultKey, TEST_TEMP_ROOT: dir }, stdio: ['ignore','ignore','pipe','ipc'] });
  const instance = { child, messages: [], exit: null };
  child.on('message', x => instance.messages.push(x)); child.on('exit', (code, signal) => { instance.exit = { code, signal }; });
  child.stderr.on('data', () => {}); return instance;
}
async function parent() {
  if (!env.DATABASE_URL || !env.R2_BUCKET) throw Error('MISSING_ENV');
  const schema = `srt_fetch_crash_${randomBytes(6).toString('hex')}`, vaultKey = randomBytes(32).toString('base64url');
  const dir = await mkdtemp(join(tmpdir(), 'fetch-crash-test-'));
  const control = new pg.Pool({ connectionString: env.DATABASE_URL, max: 1 });
  const work = new pg.Pool({ connectionString: env.DATABASE_URL, max: 4 });
  const db = scoped(work, schema), s3 = s3client(), storage = createR2Storage({ env, client: s3 });
  const processes = [], keys = new Set(); let created = false, success = false, cleanup = true;
  try {
    const base = (await control.query('SELECT current_schema() AS name')).rows[0]?.name;
    if (!/^[a-z_][a-z0-9_]*$/i.test(base ?? '')) throw Error('BAD_BASE');
    await control.query(`CREATE SCHEMA "${schema}"`); created = true;
    await control.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
    const setup = await db.connect();
    try { await setup.query('BEGIN'); await setup.query(await readFile(new URL('../server/storage/migrations/003_remote_fetch_queue.sql', import.meta.url),'utf8')); await setup.query('COMMIT'); }
    catch(e) { await setup.query('ROLLBACK').catch(() => {}); throw e; } finally { setup.release(); }
    const vault = createRemoteUrlVault({ key: vaultKey });
    const queue = createPostgresRemoteQueue({ pool: db, vault, allowedHosts: ['storage.googleapis.com'] });
    const jobs = await Promise.all(Array.from({length:3}, () => queue.enqueue('https://storage.googleapis.com/cloud-samples-data/speech/hello.wav')));
    for (const role of ['download','upload','commit']) processes.push(spawn(role, schema, vaultKey, dir));
    await until(() => processes.every((p,i) => {
      if (p.exit) throw Error('CHILD_EXITED_EARLY');
      return p.messages.some(m => m.type === `${['download','upload','commit'][i]}_blocked`);
    }));
    const claims = processes.map(p => p.messages.find(m => m.type === 'claim'));
    assert.equal(new Set(claims.map(c => c.jobId)).size, 3);
    for (const p of processes) { for (const m of p.messages) if (m.type === 'object') keys.add(m.key); p.child.kill('SIGKILL'); }
    await until(() => processes.every(p => p.exit));
    assert.ok(processes.every(p => p.exit.signal === 'SIGKILL'));
    console.log('SIGKILL DURING REAL DOWNLOAD / UPLOAD / BEFORE COMMIT: PASS');
    await until(async () => {
      const r = await control.query(`SELECT count(*)::int AS n FROM "${schema}".toolnet_srt_remote_fetch_jobs WHERE lease_expires_at > NOW()`);
      return r.rows[0].n === 0;
    });
    const restarted = spawn('recovery', schema, vaultKey, dir); processes.push(restarted);
    await until(async () => {
      if (restarted.exit) throw Error('RECOVERY_EXITED');
      return (await control.query(`SELECT count(*)::int AS n FROM "${schema}".toolnet_srt_remote_fetch_jobs WHERE status='ready'`)).rows[0].n === 3;
    });
    for (const m of restarted.messages) if (m.type === 'object') keys.add(m.key);
    restarted.child.send('stop'); await until(() => restarted.exit !== null, 15000); assert.equal(restarted.exit.code, 0);
    const leases = createPostgresRemoteLeases({ pool: db });
    for (const old of claims) {
      assert.equal(await leases.heartbeat(old), false);
      assert.equal(await leases.completeFetch({ ...old, sourceKey: remoteSourceKey(old.jobId, old.token, 'source.mp4') }), false);
      const row = (await control.query(`SELECT j.source_key, q.attempt_count FROM "${schema}".toolnet_srt_jobs j JOIN "${schema}".toolnet_srt_remote_fetch_jobs q ON q.job_id=j.id WHERE j.id=$1`, [old.jobId])).rows[0];
      assert.equal(row.attempt_count, 2); assert.ok(!row.source_key.includes(old.token));
      const path = join(dir, `${old.jobId}.mp4`);
      await storage.downloadFile({ jobId: old.jobId, filename: 'source.mp4', localPath: path, sourceKey: row.source_key });
      assert.equal(await readFile(path, 'utf8'), 'winning source');
    }
    const queryDb = { async query(sql, params) {
      const c = await db.connect();
      try { await c.query('BEGIN'); const result = await c.query(sql, params); await c.query('COMMIT'); return result; }
      catch (error) { await c.query('ROLLBACK').catch(() => {}); throw error; }
      finally { c.release(); }
    } };
    let asrInput;
    const asr = createDurableWorker({ leases: createPostgresLeaseManager({ pool: queryDb }), storage,
      results: { async putResult({ jobId, leaseToken }) { return { key: `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt` }; } },
      workerId: 'crash-asr-check', split: async input => { asrInput = await readFile(input, 'utf8'); return [{ path: 'mock', startMs: 0 }]; },
      transcribe: async () => [{ text: 'test', startMs: 0, endMs: 100 }], toSrt: () => 'test',
    });
    assert.equal((await asr.runOnce()).status, 'completed'); assert.equal(asrInput, 'winning source');
    assert.equal(jobs.length, 3);
    console.log('REAL EXPIRY + RECLAIM + OLD TOKEN FENCING: PASS');
    console.log('ALL WINNING R2 SOURCES AND ASR INPUT VERIFIED: PASS'); success = true;
  } catch(e) { process.exitCode = 1; console.error('FETCH CRASH TEST FAILED:', e.code || e.message || e.name); }
  finally {
    for (const p of processes) if (!p.exit) p.child.kill('SIGKILL');
    await Promise.all(processes.map(p => until(() => p.exit, 5000).catch(() => { cleanup = false; })));
    for (const p of processes) for (const m of p.messages) if (m.type === 'object') keys.add(m.key);
    await work.end().catch(() => { cleanup = false; });
    if (created && cleanup) try { await control.query(`DROP SCHEMA "${schema}" CASCADE`); } catch { cleanup = false; }
    await control.end().catch(() => { cleanup = false; });
    for (const key of keys) try { await s3.send(new DeleteObjectCommand({Bucket:env.R2_BUCKET,Key:key})); } catch { cleanup = false; }
    s3.destroy(); await rm(dir,{ recursive:true,force:true }).catch(() => { cleanup = false; });
    if (!cleanup) { process.exitCode = 1; console.error('CRASH TEST CLEANUP FAILED:', schema); }
    else console.log('CRASH TEST RESOURCES CLEANED: PASS');
  }
  if (success && cleanup) console.log('TOOLNET-SRT FETCH CRASH RECOVERY PASSED');
}
if (process.argv[2] === '--child') await child(); else await parent();
