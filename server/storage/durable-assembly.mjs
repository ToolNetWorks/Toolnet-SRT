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
import { createPostgresRemoteLeases } from "./postgres-remote-leases.mjs";
import { createRemoteUrlVault } from "./remote-url-vault.mjs";
import { createFetchDiskQuota } from "./fetch-disk-quota.mjs";
import { createRemoteOrphanCleanup } from "./remote-orphan-cleanup.mjs";
import { createDurableFetchWorker } from "./durable-fetch-worker.mjs";
import { createDurableFetchScheduler } from "./durable-fetch-scheduler.mjs";
import { createPostgresRemoteQueue } from "./postgres-remote-queue.mjs";
import { createPostgresChunkCheckpoints } from "./postgres-chunk-checkpoints.mjs";
import { createPostgresAsrSemaphore } from "./postgres-asr-semaphore.mjs";
import { createLocalLimiter } from './local-limiter.mjs';
import { extractTimelineChunk } from './chunk-extraction.mjs';
import { createPublicPostgresPool } from './postgres-public-pool.mjs';

const defaults = {
  poolFactory: env => createPublicPostgresPool(new pg.Pool({ connectionString: env.DATABASE_URL,
    max: Math.max(3, Number(env.DURABLE_WORKER_CONCURRENCY || 1) + 2), connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000, application_name: "toolnet-srt-durable" })),
  clientFactory: env => new S3Client({ region: "auto", endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey: env.R2_SECRET_ACCESS_KEY },
    forcePathStyle: true, requestChecksumCalculation: 'WHEN_REQUIRED', maxAttempts: 3 }),
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
  fetchLeases: createPostgresRemoteLeases,
  vault: createRemoteUrlVault,
  fetchWorker: createDurableFetchWorker,
  fetchScheduler: createDurableFetchScheduler,
  remoteQueue: createPostgresRemoteQueue,
  chunkCheckpoints: createPostgresChunkCheckpoints,
  asrSemaphore: createPostgresAsrSemaphore,
  loadAsr: () => import("../../dist/index.js"),
};

export function createDurableAssembly({ factories = {}, onTiming = null } = {}) {
  if (onTiming !== null && typeof onTiming !== 'function') throw new Error('INVALID_TIMING_CALLBACK');
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
      // pg.Pool can emit errors for disconnected idle clients; pg removes the
      // client and will reconnect on the next checkout. Never log credentials.
      pool.on?.('error', () => { try { console.error('[ToolNet-SRT] idle database connection lost'); } catch {} });
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
      const ffmpegLimiter=createLocalLimiter({limit:config.ffmpegConcurrency ?? 2});
      const chunkCheckpoints = config.chunkCheckpointsEnabled ? dependencies.chunkCheckpoints({ pool }) : null;
      const asrSemaphore = config.experimentalAsrSemaphore ? dependencies.asrSemaphore({ pool, limit:config.asrGlobalConcurrency,
        leaseSeconds:config.asrSlotLeaseSeconds, adaptive:config.asrAdaptiveConcurrency, quarantineSeconds:config.asrQuarantineSeconds }) : null;
      const maxBytes = parsePositiveInt(env.TOOLNET_SRT_MAX_UPLOAD_MB, 256, 2048) * 1024 * 1024;
      const diskQuota = config.fetchConcurrency !== undefined ? createFetchDiskQuota({ maxBytes:config.fetchDiskQuotaBytes }) : null;
      const neededBytes = maxBytes + (config.chunkCheckpointsEnabled ? (asrSemaphore ? config.asrGlobalConcurrency : 1) * ((config.chunkSeconds ?? 600) * 32000 + 65536) : 0);
      if (diskQuota && config.fetchDiskQuotaBytes < neededBytes) throw new Error('DISK_QUOTA_BELOW_WORKER_RESERVATION');
      const ingress = dependencies.ingress({ jobStore, storage, maxBytes, diskQuota });
      // Durable fetch validates and pins every resolved public IP at dial time.
      // Keep legacy v0.2 allowlist behavior separate from Durable mode.
      const allowedHosts = null;
      const remoteIngress = dependencies.remoteIngress({ ingress, allowedHosts, maxBytes });
      const vault = config.fetchConcurrency !== undefined ? dependencies.vault({ key: env.TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY }) : null;
      const remoteQueue = vault ? dependencies.remoteQueue({ pool, vault, allowedHosts, idempotencySecret: env.TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY }) : null;
      const manager = dependencies.manager({ jobStore, resultStorage: results, cancellation, ingress, remoteIngress, remoteQueue });
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
          return dependencies.worker({ leases, storage, results, progress, workerId,
            split:(input,dir,options)=>ffmpegLimiter.run(()=>splitMedia(input,dir,options),options?.signal),
            extractChunk:(input,output,options)=>ffmpegLimiter.run(()=>extractTimelineChunk(input,output,options),options?.signal), transcribe, toSrt,
            leaseSeconds: config.leaseSeconds, heartbeatIntervalMs: config.heartbeatIntervalMs, maxAttempts: config.maxAttempts,
            chunkCheckpoints, chunkSeconds: config.chunkSeconds ?? 600, chunkOverlapSeconds: config.chunkOverlapSeconds ?? 0,
            chunkMaxAttempts: config.chunkMaxAttempts ?? 3, chunkSeekPrerollMs: config.chunkSeekPrerollMs ?? null,
            asrSemaphore, onTiming, asrSlotHeartbeatMs:config.asrSlotHeartbeatMs ?? 20000, asrRequestTimeoutMs:config.asrRequestTimeoutMs ?? 300000,
            chunkConcurrency:asrSemaphore ? config.asrGlobalConcurrency : 1,
            diskQuota:chunkCheckpoints ? diskQuota : null, maxSourceBytes:maxBytes });
        },
      });
      if (typeof scheduler?.start !== "function" || typeof scheduler?.stop !== "function" || typeof scheduler?.getStatus !== "function" ||
          typeof manager?.createUpload !== "function" || typeof manager?.createRemote !== "function" || typeof manager?.getJob !== "function" ||
          typeof manager?.getResult !== "function" || typeof manager?.cancelJob !== "function") {
        throw new Error("INVALID_ASSEMBLY_COMPONENTS");
      }
      let fetchScheduler = null;
      if (config.fetchConcurrency !== undefined) {
        if (config.fetchDiskQuotaBytes < maxBytes) throw new Error('FETCH_DISK_QUOTA_BELOW_MAX_UPLOAD');
        const fetchLeases = dependencies.fetchLeases({ pool, maxAttempts: config.fetchMaxAttempts, leaseSeconds: config.fetchLeaseSeconds });
        const orphanCleanup = createRemoteOrphanCleanup({ pool, client, bucket: env.R2_BUCKET });
        fetchScheduler = dependencies.fetchScheduler({ maxConcurrency: config.fetchConcurrency,
          pollIntervalMs: config.fetchPollIntervalMs, errorBackoffMs: config.errorBackoffMs,
          orphanCleanup,
          workerFactory({ workerId }) {
            return dependencies.fetchWorker({ leases: fetchLeases, vault, storage, workerId, allowedHosts, maxBytes, diskQuota,
              heartbeatIntervalMs: config.fetchHeartbeatMs, downloadTimeoutMs: config.fetchDownloadTimeoutMs });
          },
        });
      }
      return { manager, scheduler, fetchScheduler, close };
    } catch {
      try { await close(); } catch { throw new Error("DURABLE_ASSEMBLY_CLEANUP_FAILED"); }
      throw new Error("DURABLE_ASSEMBLY_FAILED");
    }
  };
}
