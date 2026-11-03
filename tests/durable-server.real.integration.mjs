import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { once } from 'node:events';
import pg from 'pg';
import { S3Client, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
const env = process.env;
if (!env.DATABASE_URL || !env.R2_BUCKET) throw Error('MISSING_TEST_ENV');
const schema = `srt_http_server_${randomBytes(6).toString('hex')}`;
const pool = new pg.Pool({ connectionString: env.DATABASE_URL, max: 1 });
const r2 = new S3Client({ region: 'auto', endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`, forcePathStyle: true,
  requestChecksumCalculation: 'WHEN_REQUIRED', credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey: env.R2_SECRET_ACCESS_KEY } });
const testJobs = [];
let created = false, child, exit, worker, workerExit, workerReady = false, success = false, cleanup = true;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
try {
  // Neon transaction poolers may expose an empty search_path by default.
  const base = 'public';
  if (!(await pool.query("SELECT to_regclass('public.toolnet_srt_jobs') AS table_ref")).rows[0]?.table_ref) throw Error('BASE_TABLE_MISSING');
  await pool.query(`CREATE SCHEMA "${schema}"`); created = true;
  await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
  const setup = await pool.connect();
  try {
    await setup.query('BEGIN'); await setup.query(`SET LOCAL search_path TO "${schema}"`);
    for (const name of ['003_remote_fetch_queue.sql', '004_remote_idempotency.sql', '005_chunk_checkpoints.sql',
      ...(env.DURABLE_EXPERIMENTAL_ASR_SEMAPHORE==='true'?['006_asr_semaphore.sql']:[])]) {
      await setup.query(await readFile(new URL(`../server/storage/migrations/${name}`, import.meta.url), 'utf8'));
    }
    await setup.query('COMMIT');
  } catch (error) { await setup.query('ROLLBACK').catch(() => {}); throw error; } finally { setup.release(); }
  const socket = createServer(); socket.listen(0, '127.0.0.1'); await once(socket, 'listening');
  const port = socket.address().port; await new Promise(resolve => socket.close(resolve));
  const childEnv = { ...env, PORT: String(port), TEST_SERVER_SCHEMA: schema, DURABLE_JOBS_ENABLED: 'true',
      TOOLNET_SRT_API_KEY: 'isolated-test-key', TOOLNET_SRT_ALLOWED_MEDIA_HOSTS: '',
      TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY: randomBytes(32).toString('base64url'),
      TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY: randomBytes(32).toString('base64url'),
      DURABLE_POLL_INTERVAL_MS: '100', DURABLE_FETCH_POLL_INTERVAL_MS: '100', DURABLE_SHUTDOWN_GRACE_MS: '10000' };
  child = spawn(process.execPath, ['--import', './tests/fixtures/isolated-server-db.mjs', 'server/index.mjs'], {
    env: { ...childEnv, ...(env.QA_SPLIT_WORKER === 'true' ? { DURABLE_RUN_WORKERS: 'false' } : {}) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  child.on('exit', (code, signal) => { exit = { code, signal }; }); child.stderr.on('data', () => {});
  const url = `http://127.0.0.1:${port}`; let health;
  for (let i = 0; i < 200; i++) {
    if (exit) throw Error('SERVER_EXITED_EARLY');
    try { health = await fetch(url + '/health'); break; } catch { await sleep(100); }
  }
  assert.equal(health?.status, 200);
  if (env.QA_SPLIT_WORKER === 'true') {
    worker = spawn(process.execPath, ['--import', './tests/fixtures/isolated-server-db.mjs', 'server/durable-worker-service.mjs'], {
      env: childEnv, stdio: ['ignore', 'pipe', 'pipe'],
    });
    worker.on('exit', (code, signal) => { workerExit = { code, signal }; });
    worker.stdout.on('data', chunk => { if (chunk.toString().includes('DURABLE_WORKER_READY')) workerReady = true; });
    worker.stderr.on('data', () => {});
    for (let i = 0; i < 200 && !workerReady && !workerExit; i++) await sleep(100);
    assert.equal(workerReady, true, 'WORKER_NOT_READY');
    console.log('SEPARATE WORKER READY: PASS');
  }
  const headers = { 'X-API-Key': 'isolated-test-key', 'Idempotency-Key': 'server-e2e', 'Content-Type': 'application/json' };
  const body = JSON.stringify({ url: 'https://media.example/audio.mp4?token=test-only' });
  const response = await fetch(url + '/v1/jobs/url', { method: 'POST', headers, body });
  assert.equal(response.status, 202); const job = await response.json();
  testJobs.push(job.id);
  assert.ok(!JSON.stringify(job).includes('test-only'));
  assert.equal((await (await fetch(url + '/v1/jobs/url', { method: 'POST', headers, body })).json()).id, job.id);
  assert.equal((await fetch(url + '/v1/jobs/' + job.id)).status, 401);
  const row = await pool.query(`SELECT source_key FROM "${schema}".toolnet_srt_jobs WHERE id=$1`, [job.id]);
  assert.equal(row.rows[0].source_key, null);
  console.log('SERVER FEATURE FLAG + ISOLATED PREFLIGHT + HTTP QUEUE: PASS');
  const realUrl = env.QA_REMOTE_TEST_URL || 'https://storage.googleapis.com/cloud-samples-data/speech/hello.wav';
  const realHeaders = { ...headers, 'Idempotency-Key': 'server-real-asr' };
  const realResponse = await fetch(url + '/v1/jobs/url', { method: 'POST', headers: realHeaders, body: JSON.stringify({ url: realUrl }) });
  assert.equal(realResponse.status, 202);
  const realJob = await realResponse.json(); testJobs.push(realJob.id);
  const rpc = await fetch(url + '/mcp', { method: 'POST', headers: { 'X-API-Key': 'isolated-test-key', 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'submit_transcription_url', arguments: { url: realUrl, idempotency_key: 'server-real-asr' } } }) });
  assert.equal(JSON.parse((await rpc.json()).result.content[0].text).id, realJob.id);
  let status;
  for (let i = 0; i < 900; i++) {
    const reply = await fetch(url + '/v1/jobs/' + realJob.id, { headers: { 'X-API-Key': 'isolated-test-key' } });
    status = await reply.json();
    if (status.status === 'completed' || status.status === 'failed') break;
    await sleep(200);
  }
  assert.equal(status.status, 'completed', status.errorCode);
  const checkpoints = await pool.query(`SELECT status, segments FROM "${schema}".toolnet_srt_job_chunks WHERE job_id=$1`, [realJob.id]);
  assert.ok(checkpoints.rows.length > 0);
  assert.ok(checkpoints.rows.every(chunk => chunk.status === 'completed' && Array.isArray(chunk.segments)));
  const subtitle = await fetch(url + '/v1/jobs/' + realJob.id + '/result', { headers: { 'X-API-Key': 'isolated-test-key' } });
  assert.equal(subtitle.status, 200); assert.match(await subtitle.text(), /-->/);
  console.log('REAL SERVER HTTP + MCP + JIANYING + R2 SRT: PASS');
  const upload = await fetch(url + '/v1/jobs', { method: 'POST',
    headers: { 'X-API-Key': 'isolated-test-key', 'Content-Type': 'audio/mpeg' }, body: Buffer.from('test-upload') });
  assert.equal(upload.status, 202);
  const uploadJob = await upload.json(); testJobs.push(uploadJob.id);
  assert.equal(uploadJob.status, 'queued'); assert.equal(Object.hasOwn(uploadJob, 'sourceKey'), false);
  console.log('REAL SERVER STREAMING UPLOAD: PASS');
  child.kill('SIGTERM');
  for (let i = 0; i < 150 && !exit; i++) await sleep(100);
  assert.deepEqual(exit, { code: 0, signal: null });
  if (worker) {
    worker.kill('SIGTERM');
    for (let i = 0; i < 150 && !workerExit; i++) await sleep(100);
    assert.deepEqual(workerExit, { code: 0, signal: null });
    console.log('SEPARATE WORKER DRAIN: PASS');
  }
  console.log('SERVER SIGTERM + RUNTIME DRAIN: PASS'); success = true;
} catch (error) { process.exitCode = 1; console.error('REAL SERVER TEST FAILED:', error.code || error.message); }
finally {
  if (child && !exit) { child.kill('SIGKILL'); for (let i=0; i<50 && !exit; i++) await sleep(100); }
  if (worker && !workerExit) { worker.kill('SIGKILL'); for (let i=0; i<50 && !workerExit; i++) await sleep(100); }
  if (child && !exit) cleanup = false;
  if (worker && !workerExit) cleanup = false;
  for (const id of testJobs) {
    try {
      let continuation;
      do {
        const page = await r2.send(new ListObjectsV2Command({ Bucket: env.R2_BUCKET, Prefix: `toolnet-srt/jobs/${id}/`, ContinuationToken: continuation }));
        for (const item of page.Contents ?? []) await r2.send(new DeleteObjectCommand({ Bucket: env.R2_BUCKET, Key: item.Key }));
        continuation = page.IsTruncated ? page.NextContinuationToken : undefined;
      } while (continuation);
    } catch { cleanup = false; }
  }
  r2.destroy();
  if (created && cleanup) try { await pool.query(`DROP SCHEMA "${schema}" CASCADE`); } catch { cleanup = false; }
  await pool.end().catch(() => { cleanup = false; });
  if (!cleanup) { process.exitCode = 1; console.error('SCHEMA CLEANUP FAILED:', schema); }
  else console.log('TEST SCHEMA CLEANED: PASS');
}
if (success && cleanup) console.log('TOOLNET-SRT DURABLE SERVER REAL PASSED');
