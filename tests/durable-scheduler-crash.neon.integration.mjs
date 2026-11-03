import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { createPostgresJobStore } from "../server/storage/postgres.mjs";
import { createPostgresLeaseManager } from "../server/storage/leases.mjs";
import { createPostgresProgressManager } from "../server/storage/progress.mjs";
import { createDurableWorker } from "../server/storage/durable-worker.mjs";
import { createDurableScheduler } from "../server/storage/durable-scheduler.mjs";

const SCHEMA_RE = /^toolnet_srt_crash_[a-f0-9]{12}$/;
// Durable worker and lease manager both require at least 30 seconds.
const LEASE_SECONDS = 30;
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, timeoutMs = 70000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) { if (await check()) return; await pause(100); }
  throw new Error("WAIT_TIMEOUT");
}

function createScopedDb(pool, schema) {
  if (!SCHEMA_RE.test(schema)) throw new Error("INVALID_TEST_SCHEMA");
  return {
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
  };
}

async function notify(type, extra = {}) {
  if (!process.send) return;
  await new Promise(resolve => { process.send({ type, ...extra }, resolve); });
}

async function runChild() {
  const role = process.argv[3], schema = process.env.TEST_SCHEMA, jobId = process.env.TEST_JOB_ID;
  if (!["A", "B"].includes(role) || !SCHEMA_RE.test(schema ?? "")) throw new Error("INVALID_CHILD_CONFIG");
  if (!/^[0-9a-f-]{36}$/i.test(jobId ?? "")) throw new Error("INVALID_TEST_JOB_ID");
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 2,
    connectionTimeoutMillis: 10000, application_name: `toolnet-srt-crash-${role}` });
  const db = createScopedDb(pool, schema);
  const leases = createPostgresLeaseManager({ pool: db });
  const progress = createPostgresProgressManager({ pool: db });
  const workerId = `crash-worker-${role.toLowerCase()}`;
  let completionReported = false;
  const scheduler = createDurableScheduler({ maxConcurrency: 1, pollIntervalMs: 150, errorBackoffMs: 300,
    workerIdPrefix: `crash-test-${role.toLowerCase()}`,
    workerFactory() {
      return createDurableWorker({ leases, progress, workerId, leaseSeconds: LEASE_SECONDS, heartbeatIntervalMs: 2000,
        storage: { async downloadFile({ localPath }) { await writeFile(localPath, "mock audio"); } },
        results: { async putResult({ jobId: resultJobId, leaseToken }) {
          const key = `toolnet-srt/jobs/${resultJobId}/attempts/${leaseToken}/result.srt`;
          await notify("uploaded", { leaseToken, key });
          return { key };
        } },
        split: async () => [{ path: "mock-segment.mp3", startMs: 0 }],
        transcribe: async () => {
          if (role === "A") { await notify("asr_blocked"); return new Promise(() => {}); }
          return [{ text: "Recovered", startMs: 0, endMs: 500 }];
        },
        toSrt: () => "1\n00:00:00,000 --> 00:00:00,500\nRecovered\n",
      });
    },
  });
  process.on("message", message => {
    if (message !== "stop") return;
    void (async () => {
      const result = await scheduler.stop({ timeoutMs: 10000 });
      if (!result.stopped) throw new Error("WORKER_DID_NOT_STOP");
      await pool.end();
      await notify("stopped");
      process.disconnect?.();
    })().catch(error => { console.error("CHILD_STOP_ERROR:", error.name); process.exitCode = 1; });
  });
  scheduler.start();
  if (role === "B") {
    const ticker = setInterval(() => {
      if (completionReported || scheduler.getStatus().completed < 1) return;
      completionReported = true;
      clearInterval(ticker);
      void notify("completed");
    }, 50);
  }
}

function spawnWorker(role, schema, jobId) {
  const child = fork(fileURLToPath(import.meta.url), ["--child", role], {
    env: { ...process.env, TEST_SCHEMA: schema, TEST_JOB_ID: jobId },
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  const agent = { child, messages: [], exit: null, stderr: "" };
  child.on("message", message => { agent.messages.push(message); });
  child.on("exit", (code, signal) => { agent.exit = { code, signal }; });
  child.stderr.on("data", chunk => { agent.stderr += chunk.toString().slice(0, 2000); });
  return agent;
}
async function killWorker(agent) {
  if (!agent || agent.exit) return;
  agent.child.kill("SIGKILL");
  await until(() => agent.exit !== null, 5000).catch(() => {});
}

async function runParent() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL_MISSING");
  const schema = `toolnet_srt_crash_${randomBytes(6).toString("hex")}`;
  const jobId = randomUUID();
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 2,
    connectionTimeoutMillis: 10000, application_name: "toolnet-srt-crash-parent" });
  let schemaCreated = false, passed = false, cleanupPassed = true, workerA, workerB;
  try {
    const base = await pool.query("SELECT current_schema() AS name");
    const baseSchema = base.rows[0]?.name;
    if (!/^[a-z_][a-z0-9_]*$/i.test(baseSchema ?? "")) throw new Error("INVALID_BASE_SCHEMA");
    await pool.query(`CREATE SCHEMA "${schema}"`);
    schemaCreated = true;
    await pool.query(`CREATE TABLE "${schema}".toolnet_srt_jobs (LIKE "${baseSchema}".toolnet_srt_jobs INCLUDING ALL)`);
    const db = createScopedDb(pool, schema);
    const store = createPostgresJobStore({ pool: db });
    const leases = createPostgresLeaseManager({ pool: db });
    const progress = createPostgresProgressManager({ pool: db });
    await store.saveJob({ id: jobId, status: "queued", stage: "queued", progress: 0, completedSegments: 0,
      totalSegments: null, extension: ".mp3", sourceKey: `toolnet-srt/jobs/${jobId}/source.mp3`, resultKey: null });
    console.log("NEON ISOLATED JOB: PASS");

    workerA = spawnWorker("A", schema, jobId);
    await until(() => {
      if (workerA.exit) throw new Error(`CHILD_A_EXITED: ${workerA.stderr}`);
      return workerA.messages.some(message => message.type === "asr_blocked");
    });
    const before = await db.query(`SELECT status, progress, attempt_count, lease_token, lease_expires_at
      FROM toolnet_srt_jobs WHERE id = $1::uuid`, [jobId]);
    const first = before.rows[0];
    assert.equal(first.status, "processing");
    assert.equal(Number(first.progress), 10);
    assert.equal(Number(first.attempt_count), 1);
    assert.ok(first.lease_token);
    console.log("PROCESS A ACTIVE IN ASR: PASS");
    assert.equal(workerA.child.kill("SIGKILL"), true);
    await until(() => workerA.exit !== null, 5000);
    assert.equal(workerA.exit.signal, "SIGKILL");
    console.log("PROCESS A SIGKILL: PASS");

    // Wait for actual database lease expiry without modifying its timestamp.
    await until(async () => {
      const result = await db.query("SELECT lease_expires_at <= NOW() AS expired FROM toolnet_srt_jobs WHERE id = $1::uuid", [jobId]);
      return result.rows[0]?.expired === true;
    });
    console.log("REAL LEASE EXPIRY: PASS");
    workerB = spawnWorker("B", schema, jobId);
    await until(() => {
      if (workerB.exit) throw new Error(`CHILD_B_EXITED: ${workerB.stderr}`);
      return workerB.messages.some(message => message.type === "completed");
    });
    const uploads = workerB.messages.filter(message => message.type === "uploaded");
    assert.equal(uploads.length, 1);
    assert.notEqual(uploads[0].leaseToken, first.lease_token);
    const after = await db.query("SELECT status, progress, attempt_count, result_key FROM toolnet_srt_jobs WHERE id = $1::uuid", [jobId]);
    const final = after.rows[0];
    assert.equal(final.status, "completed");
    assert.equal(Number(final.progress), 100);
    assert.equal(Number(final.attempt_count), 2);
    assert.equal(final.result_key, uploads[0].key);
    console.log("PROCESS B RECLAIM + COMPLETE: PASS");
    console.log("ONE RESULT / TWO ATTEMPTS: PASS");

    const staleHeartbeat = await leases.heartbeatJob({ jobId, workerId: "crash-worker-a", leaseToken: first.lease_token });
    const staleProgress = await progress.updateProgress({ jobId, workerId: "crash-worker-a", leaseToken: first.lease_token,
      stage: "finalizing", progress: 99, completedSegments: 1, totalSegments: 1 });
    const staleComplete = await leases.completeJob({ jobId, workerId: "crash-worker-a", leaseToken: first.lease_token,
      resultKey: `toolnet-srt/jobs/${jobId}/attempts/${first.lease_token}/result.srt` });
    assert.equal(staleHeartbeat, null); assert.equal(staleProgress, null); assert.equal(staleComplete, null);
    console.log("OLD TOKEN FENCED AFTER RESTART: PASS");
    workerB.child.send("stop");
    await until(() => workerB.exit !== null, 10000);
    assert.equal(workerB.exit.code, 0);
    console.log("PROCESS B GRACEFUL STOP: PASS");
    passed = true;
  } finally {
    await killWorker(workerA);
    await killWorker(workerB);
    if ((workerA && !workerA.exit) || (workerB && !workerB.exit)) {
      cleanupPassed = false;
      console.error("CHILD STILL ACTIVE: SKIPPING SCHEMA CLEANUP");
    }
    if (schemaCreated && cleanupPassed) {
      try { await pool.query(`DROP SCHEMA "${schema}" CASCADE`); console.log("TEST SCHEMA CLEANED: PASS"); }
      catch (error) { cleanupPassed = false; console.error("TEST SCHEMA CLEANUP FAILED:", schema, error.code); }
    }
    await pool.end();
  }
  if (!passed || !cleanupPassed) throw new Error("CRASH_RECOVERY_TEST_INCOMPLETE");
  console.log("TOOLNET-SRT PROCESS CRASH RECOVERY PASSED");
}

if (process.argv[2] === "--child") await runChild();
else await runParent();
