import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { createDurableRuntime } from "../server/storage/durable-runtime.mjs";
import { createDurableAssembly } from "../server/storage/durable-assembly.mjs";
import { createDurablePreflight } from "../server/storage/durable-preflight.mjs";

const env = { ...process.env, DURABLE_JOBS_ENABLED: "true", DURABLE_WORKER_CONCURRENCY: "1",
  DURABLE_POLL_INTERVAL_MS: "100", DURABLE_FETCH_POLL_INTERVAL_MS: "100", DURABLE_ERROR_BACKOFF_MS: "500", DURABLE_SHUTDOWN_GRACE_MS: "10000",
  TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY: randomBytes(32).toString('base64url'), TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY: randomBytes(32).toString('base64url'),
  TOOLNET_SRT_ALLOWED_MEDIA_HOSTS: 'media.example' };
if (!env.DATABASE_URL) { console.error("DATABASE_URL_MISSING"); process.exit(1); }
const schema = `toolnet_srt_runtime_${randomBytes(6).toString("hex")}`;
const controlPool = new pg.Pool({ connectionString: env.DATABASE_URL, max: 1,
  connectionTimeoutMillis: 10000, application_name: "toolnet-srt-runtime-control" });
let runtime = null, schemaCreated = false, cleanupSafe = true, passed = false;
const signalListenersBefore = { SIGTERM: process.listenerCount("SIGTERM"), SIGINT: process.listenerCount("SIGINT") };

function createIsolatedPool() {
  const pool = new pg.Pool({ connectionString: env.DATABASE_URL, max: 3,
    connectionTimeoutMillis: 10000, application_name: "toolnet-srt-runtime-isolated" });
  return {
    async connect() {
      const client = await pool.connect();
      return {
        async query(sql, params) {
          if (sql === 'BEGIN') {
            try {
              await client.query('BEGIN');
              await client.query(`SET LOCAL search_path TO "${schema}"`);
              await client.query("SET LOCAL statement_timeout = '20000ms'");
            } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
            return;
          }
          return client.query(sql, params);
        },
        release(error) { client.release(error); },
      };
    },
    async query(sql, params = []) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(`SET LOCAL search_path TO "${schema}"`);
        await client.query("SET LOCAL statement_timeout = '20000ms'");
        const result = await client.query(sql, params);
        await client.query("COMMIT");
        return result;
      } catch (error) {
        await client.query("ROLLBACK").catch(() => {});
        throw error;
      } finally { client.release(); }
    },
    async end() { await pool.end(); },
  };
}
async function waitFor(check, timeoutMs = 12000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) { if (check()) return; await new Promise(resolve => setTimeout(resolve, 100)); }
  throw new Error("SCHEDULER_POLL_TIMEOUT");
}
async function countTestJobs() {
  const result = await controlPool.query(`SELECT COUNT(*)::integer AS count FROM "${schema}".toolnet_srt_jobs`);
  return result.rows[0].count;
}

try {
  const base = await controlPool.query("SELECT current_schema() AS name");
  const baseSchema = base.rows[0]?.name;
  if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
  await controlPool.query(`CREATE SCHEMA "${schema}"`);
  schemaCreated = true;
  await controlPool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
  const setup = await controlPool.connect();
  try {
    await setup.query('BEGIN');
    await setup.query(`SET LOCAL search_path TO "${schema}"`);
    for (const name of ['003_remote_fetch_queue.sql', '004_remote_idempotency.sql', '005_chunk_checkpoints.sql']) {
      await setup.query(await readFile(new URL(`../server/storage/migrations/${name}`, import.meta.url), 'utf8'));
    }
    await setup.query('COMMIT');
  } catch (error) { await setup.query('ROLLBACK').catch(() => {}); throw error; }
  finally { setup.release(); }
  assert.equal(await countTestJobs(), 0);
  console.log("ISOLATED NEON SCHEMA: PASS");

  const assemble = createDurableAssembly({ factories: { poolFactory: createIsolatedPool } });
  runtime = createDurableRuntime({ env, assemble, preflightFactory: options => createDurablePreflight({ ...options,
    poolFactory() {
      const pool = new pg.Pool({ connectionString: env.DATABASE_URL, max: 1 });
      return { query(sql, params) { return pool.query(sql.replaceAll('public.', `"${schema}".`), params); }, end() { return pool.end(); } };
    },
  }) });
  assert.equal(runtime.getStatus().state, "idle");
  assert.throws(() => runtime.getManager(), /DURABLE_RUNTIME_NOT_READY/);
  const started = await runtime.start();
  assert.equal(started.state, "running"); assert.equal(started.enabled, true);
  console.log("REAL NEON + R2 PREFLIGHT: PASS");
  console.log("DURABLE ASSEMBLY STARTUP: PASS");
  const manager = runtime.getManager();
  for (const name of ["createUpload", "getJob", "getResult", "cancelJob"]) assert.equal(typeof manager[name], "function");
  console.log("DURABLE MANAGER READY: PASS");
  await assert.rejects(manager.getJob(randomUUID()), { code: "JOB_NOT_FOUND", status: 404 });
  console.log("ISOLATED NEON MANAGER READ: PASS");
  await waitFor(() => {
    const status = runtime.getStatus().scheduler;
    if (status?.errors > 0) throw new Error("SCHEDULER_DATABASE_ERROR");
    return status?.emptyPolls >= 2;
  });
  const scheduler = runtime.getStatus().scheduler;
  assert.equal(runtime.getStatus().fetchScheduler.state, 'running');
  assert.equal(runtime.getStatus().fetchScheduler.errors, 0);
  assert.equal(scheduler.state, "running"); assert.equal(scheduler.completed, 0); assert.equal(scheduler.errors, 0);
  assert.equal(await countTestJobs(), 0);
  console.log("REAL SCHEDULER IDLE POLLING: PASS");
  console.log("NO TEST JOB CREATED: PASS");
  const queued = await manager.createRemote('https://media.example/test?token=test-only', { idempotencyKey: 'runtime-test-key' });
  const duplicate = await manager.createRemote('https://media.example/test?token=test-only', { idempotencyKey: 'runtime-test-key' });
  assert.equal(queued.id, duplicate.id);
  assert.ok(!JSON.stringify(queued).includes('test-only'));
  assert.equal(await countTestJobs(), 1);
  console.log('ASSEMBLED ATOMIC REMOTE QUEUE: PASS');
  const stopped = await runtime.shutdown();
  assert.equal(stopped.stopped, true); assert.equal(stopped.state, "stopped");
  assert.equal(runtime.getStatus().scheduler.state, "stopped");
  assert.equal(runtime.getStatus().fetchScheduler.state, "stopped");
  assert.throws(() => runtime.getManager(), /DURABLE_RUNTIME_NOT_READY/);
  assert.equal(process.listenerCount("SIGTERM"), signalListenersBefore.SIGTERM);
  assert.equal(process.listenerCount("SIGINT"), signalListenersBefore.SIGINT);
  console.log("GRACEFUL RUNTIME SHUTDOWN: PASS");
  console.log("SIGNAL LISTENERS CLEANED: PASS");
  passed = true;
} catch (error) {
  process.exitCode = 1;
  console.error("DURABLE RUNTIME E2E FAILED:", error?.code || error?.message || "UNKNOWN");
} finally {
  if (runtime) {
    try { if (!(await runtime.shutdown()).stopped) cleanupSafe = false; }
    catch { cleanupSafe = false; }
  }
  if (schemaCreated && cleanupSafe) {
    try { await controlPool.query(`DROP SCHEMA "${schema}" CASCADE`); console.log("TEST SCHEMA CLEANED: PASS"); }
    catch { cleanupSafe = false; console.error("TEST SCHEMA CLEANUP FAILED:", schema); }
  }
  if (!cleanupSafe) { process.exitCode = 1; console.error("VERIFY WORKER STATE BEFORE CLEANUP:", schema); }
  await controlPool.end().catch(() => { process.exitCode = 1; });
}
if (passed && cleanupSafe) console.log("TOOLNET-SRT DURABLE RUNTIME REAL E2E PASSED");
