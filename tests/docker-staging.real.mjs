// Creates localhost-only staging containers against a dedicated test schema and
// R2 prefix. It never applies migrations to the base schema.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { once } from 'node:events';
import { resolve } from 'node:path';
import pg from 'pg';

const required = ['DATABASE_URL','R2_ACCOUNT_ID','R2_ACCESS_KEY_ID','R2_SECRET_ACCESS_KEY','R2_BUCKET'];
for (const name of required) if (!process.env[name] || /[\r\n]/.test(process.env[name])) throw new Error(`MISSING_${name}`);
const tag = randomBytes(6).toString('hex');
const schema = `srt_http_server_${tag}`;
const prefix = `staging/toolnet-srt/${tag}/`;
const apiName = `toolnet-staging-api-${tag}`;
const workerName = `toolnet-staging-worker-${tag}`;
const stagingImage = process.env.QA_STAGING_IMAGE || 'toolnet-srt:staging-check';
const envPath = resolve(`.env.staging-${tag}`);
const apiKey = randomBytes(32).toString('base64url');
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1, connectionTimeoutMillis: 10000 });
let schemaCreated = false, apiCreated = false, workerCreated = false, stagingReady = false;
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
function docker(...args) {
  const result = spawnSync('docker', args, { encoding: 'utf8', timeout: 90000 });
  if (result.status !== 0) throw new Error(`DOCKER_${args[0].toUpperCase()}_FAILED`);
  return result.stdout.trim();
}
try {
  // Neon transaction poolers can start with an empty search_path.
  const base = 'public';
  if (!(await pool.query("SELECT to_regclass('public.toolnet_srt_jobs') AS table_ref")).rows[0]?.table_ref) throw new Error('BASE_TABLE_MISSING');
  await pool.query(`CREATE SCHEMA "${schema}"`); schemaCreated = true;
  await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${base}".toolnet_srt_jobs INCLUDING ALL)`);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO "${schema}"`);
    for (const name of ['003_remote_fetch_queue.sql','004_remote_idempotency.sql','005_chunk_checkpoints.sql']) {
      await client.query(await readFile(new URL(`../server/storage/migrations/${name}`, import.meta.url), 'utf8'));
    }
    await client.query('COMMIT');
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
  finally { client.release(); }
  const socket = createServer(); socket.listen(0, '127.0.0.1'); await once(socket, 'listening');
  const port = socket.address().port; await new Promise(resolve => socket.close(resolve));
  const values = Object.fromEntries(required.map(name => [name, process.env[name]]));
  Object.assign(values, {
    TEST_SERVER_SCHEMA: schema, TEST_STAGING_R2_PREFIX: prefix, DURABLE_JOBS_ENABLED: 'true', DURABLE_RUN_WORKERS: 'false',
    TOOLNET_SRT_API_KEY: apiKey,
    TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY: randomBytes(32).toString('base64url'),
    TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY: randomBytes(32).toString('base64url'),
    DURABLE_WORKER_CONCURRENCY: '1', DURABLE_ASR_GLOBAL_CONCURRENCY: '1', DURABLE_CHUNK_SECONDS: '60',
    DURABLE_POLL_INTERVAL_MS: '250', DURABLE_FETCH_POLL_INTERVAL_MS: '250', DURABLE_SHUTDOWN_GRACE_MS: '30000',
  });
  await writeFile(envPath, Object.entries(values).map(([k,v]) => `${k}=${v}\n`).join(''), { mode: 0o600, flag: 'wx' });
  const fixture = resolve('tests/fixtures');
  const common = ['--env-file',envPath,'--mount',`type=bind,src=${fixture},dst=/app/tests/fixtures,readonly`,
    '--restart','unless-stopped'];
  docker('run','-d','--name',apiName,...common,'-p',`127.0.0.1:${port}:10000`,
    stagingImage,'node','--import','./tests/fixtures/isolated-docker-staging.mjs','server/index.mjs'); apiCreated = true;
  docker('run','-d','--name',workerName,...common,
    stagingImage,'node','--import','./tests/fixtures/isolated-docker-staging.mjs','server/durable-worker-service.mjs'); workerCreated = true;
  const baseUrl = `http://127.0.0.1:${port}`;
  for (let i=0;i<200;i++) { try { if ((await fetch(baseUrl+'/health')).ok) break; } catch {} if(i===199) throw Error('STAGING_API_TIMEOUT'); await pause(250); }
  for (let i=0;i<200;i++) { if(docker('logs',workerName).includes('DURABLE_WORKER_READY')) break; if(i===199) throw Error('STAGING_WORKER_TIMEOUT'); await pause(250); }
  const headers = { 'X-API-Key': apiKey };
  async function waitJob(id) {
    for(let i=0;i<900;i++) { const response=await fetch(`${baseUrl}/v1/jobs/${id}`,{headers}); assert.equal(response.status,200);
      const job=await response.json(); if(job.status==='failed'||job.status==='cancelled') throw Error(`STAGING_JOB_${job.status.toUpperCase()}`);
      if(job.status==='completed') return job; await pause(1000); }
    throw Error('STAGING_JOB_TIMEOUT');
  }
  async function result(id) { const response=await fetch(`${baseUrl}/v1/jobs/${id}/result`,{headers}); assert.equal(response.status,200);
    assert.match(await response.text(),/\d{2}:\d{2}:\d{2},\d{3} -->/); }
  const audio = await readFile('/tmp/toolnet-qa-5min.mp3');
  let response = await fetch(baseUrl+'/v1/jobs',{method:'POST',headers:{...headers,'Content-Type':'audio/mpeg'},body:audio});
  assert.equal(response.status,202); const upload = await response.json();
  await waitJob(upload.id); await result(upload.id);
  const chunks = await pool.query(`SELECT count(*)::int AS n FROM "${schema}".toolnet_srt_job_chunks WHERE job_id=$1 AND status='completed'`,[upload.id]);
  assert.ok(chunks.rows[0].n >= 2); console.log(`UPLOAD STATUS RESULT: PASS (${chunks.rows[0].n} chunks)`);
  // Verify queued URL survives a stopped worker and is picked up after restart.
  docker('stop','-t','40',workerName);
  response = await fetch(baseUrl+'/v1/jobs/url',{method:'POST',headers:{...headers,'Content-Type':'application/json','Idempotency-Key':'staging-url-restart'},
    body:JSON.stringify({url:process.env.QA_REMOTE_TEST_URL || 'https://storage.googleapis.com/cloud-samples-data/speech/hello.wav'})});
  assert.equal(response.status,202); const remote=await response.json();
  response = await fetch(`${baseUrl}/v1/jobs/${remote.id}`,{headers}); assert.equal((await response.json()).status,'queued');
  docker('start',workerName); await waitJob(remote.id); await result(remote.id);
  console.log('URL RESTART STATUS RESULT: PASS');
  console.log(`STAGING_READY http://127.0.0.1:${port} ${envPath}`);
  stagingReady = true;
} catch (error) {
  console.error('STAGING_FAILED:', error?.code || error?.message || 'UNKNOWN');
  process.exitCode = 1;
} finally {
  await pool.end().catch(() => {});
  if (!stagingReady && (apiCreated || workerCreated || schemaCreated)) {
    console.error('STAGING_RESOURCES_RETAINED_FOR_INSPECTION:', schema, prefix, apiName, workerName, envPath);
  }
}
