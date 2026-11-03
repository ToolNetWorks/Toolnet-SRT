import pg from "pg";
import { S3Client } from "@aws-sdk/client-s3";
import { parsePositiveInt, splitMedia } from "../job-utils.mjs";
import { createPostgresJobStore } from "./postgres.mjs";
import { createPostgresLeaseManager } from "./leases.mjs";
import { createPostgresProgressManager } from "./progress.mjs";
import { createPostgresCancellation } from "./cancellation.mjs";
import { createR2Storage } from "./r2.mjs";
import { createAttemptResultStorage } from "./attempt-results.mjs";
import { createDurableUploadIngress } from "./durable-ingress.mjs";
import { createDurableRemoteIngress } from "./durable-remote-ingress.mjs";
import { createDurableJobManager } from "./durable-job-manager.mjs";
import { createDurableWorker } from "./durable-worker.mjs";
import { createDurableScheduler } from "./durable-scheduler.mjs";

const defaults = {
  poolFactory: env => new pg.Pool({ connectionString: env.DATABASE_URL,
    max: Math.max(3, Number(env.DURABLE_WORKER_CONCURRENCY || 1) + 2), connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000, application_name: "toolnet-srt-durable" }),
  clientFactory: env => new S3Client({ region: "auto", endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey: env.R2_SECRET_ACCESS_KEY }, maxAttempts: 3 }),
  jobStore: createPostgresJobStore,
  leases: createPostgresLeaseManager,
  progress: createPostgresProgressManager,
  cancellation: createPostgresCancellation,
  storage: createR2Storage,
  results: createAttemptResultStorage,
  ingress: createDurableUploadIngress,
  remoteIngress: createDurableRemoteIngress,
  manager: createDurableJobManager,
  worker: createDurableWorker,
  scheduler: createDurableScheduler,
  loadAsr: () => import("../../dist/index.js"),
};

export function createDurableAssembly({ factories = {} } = {}) {
  const dependencies = { ...defaults, ...factories };
  return async function assemble({ env, config }) {
    if (!config?.enabled) throw new Error("DURABLE_MODE_NOT_ENABLED");
    let pool = null, client = null, closePromise = null;
    function close() {
      if (closePromise) return closePromise;
      closePromise = (async () => {
        const failures = [];
        if (client) { try { await client.destroy(); } catch { failures.push("R2_CLOSE_FAILED"); } }
        if (pool) { try { await pool.end(); } catch { failures.push("NEON_CLOSE_FAILED"); } }
        if (failures.length) throw new Error(failures.join(","));
      })();
      return closePromise;
    }
    try {
      pool = await dependencies.poolFactory(env);
      if (typeof pool?.query !== "function" || typeof pool?.end !== "function") throw new Error("INVALID_DATABASE_POOL");
      const jobStore = dependencies.jobStore({ env, pool });
      if (typeof jobStore?.ping !== "function") throw new Error("INVALID_POSTGRES_JOB_STORE");
      // Preflight checks schema; startup does not run migrations.
      await jobStore.ping();
      client = await dependencies.clientFactory(env);
      if (typeof client?.send !== "function" || typeof client?.destroy !== "function") throw new Error("INVALID_R2_CLIENT");
      const storage = dependencies.storage({ env, client });
      const results = dependencies.results({ env, client });
      const leases = dependencies.leases({ pool });
      const progress = dependencies.progress({ pool });
      const cancellation = dependencies.cancellation({ pool });
      const maxBytes = parsePositiveInt(env.TOOLNET_SRT_MAX_UPLOAD_MB, 256, 2048) * 1024 * 1024;
      const ingress = dependencies.ingress({ jobStore, storage, maxBytes });
      const allowedHosts = [...new Set((env.TOOLNET_SRT_ALLOWED_MEDIA_HOSTS || "")
        .split(",").map(host => host.trim().toLowerCase()).filter(Boolean))];
      const remoteIngress = dependencies.remoteIngress({ ingress, allowedHosts, maxBytes });
      const manager = dependencies.manager({ jobStore, resultStorage: results, cancellation, ingress, remoteIngress });
      let asrPromise;
      function loadAsr() {
        if (!asrPromise) asrPromise = Promise.resolve().then(() => dependencies.loadAsr())
          .catch(error => { asrPromise = null; throw error; });
        return asrPromise;
      }
      async function transcribe(input, signal) {
        const module = await loadAsr();
        if (typeof module.transcribe !== "function") throw new Error("ASR_TRANSCRIBE_UNAVAILABLE");
        return module.transcribe({ input, signal, queryTimeoutMs: 300000 });
      }
      async function toSrt(segments) {
        const module = await loadAsr();
        if (typeof module.toSrt !== "function") throw new Error("ASR_SRT_UNAVAILABLE");
        return module.toSrt(segments);
      }
      const scheduler = dependencies.scheduler({ maxConcurrency: config.concurrency, pollIntervalMs: config.pollIntervalMs,
        errorBackoffMs: config.errorBackoffMs, workerIdPrefix: "toolnet-srt",
        workerFactory({ workerId }) {
          return dependencies.worker({ leases, storage, results, progress, workerId, split: splitMedia, transcribe, toSrt,
            leaseSeconds: config.leaseSeconds, heartbeatIntervalMs: config.heartbeatIntervalMs, maxAttempts: config.maxAttempts });
        },
      });
      if (typeof scheduler?.start !== "function" || typeof scheduler?.stop !== "function" || typeof scheduler?.getStatus !== "function" ||
          typeof manager?.createUpload !== "function" || typeof manager?.createRemote !== "function" || typeof manager?.getJob !== "function" ||
          typeof manager?.getResult !== "function" || typeof manager?.cancelJob !== "function") {
        throw new Error("INVALID_ASSEMBLY_COMPONENTS");
      }
      return { manager, scheduler, close };
    } catch {
      try { await close(); } catch { throw new Error("DURABLE_ASSEMBLY_CLEANUP_FAILED"); }
      throw new Error("DURABLE_ASSEMBLY_FAILED");
    }
  };
}
