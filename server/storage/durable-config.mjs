const REQUIRED_SECRETS = Object.freeze([
  "DATABASE_URL", "R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET", "TOOLNET_SRT_API_KEY", "TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY", "TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY",
]);
const INTEGER_OPTIONS = Object.freeze({
  DURABLE_WORKER_CONCURRENCY: { fallback: 1, min: 1, max: 32 },
  DURABLE_POLL_INTERVAL_MS: { fallback: 3000, min: 10, max: 60000 },
  DURABLE_ERROR_BACKOFF_MS: { fallback: 10000, min: 10, max: 120000 },
  DURABLE_SHUTDOWN_GRACE_MS: { fallback: 30000, min: 1, max: 300000 },
  DURABLE_JOB_LEASE_SECONDS: { fallback: 120, min: 30, max: 3600 },
  DURABLE_HEARTBEAT_MS: { fallback: 20000, min: 1000, max: 300000 },
  DURABLE_MAX_ATTEMPTS: { fallback: 3, min: 1, max: 10 },
  DURABLE_FETCH_DISK_QUOTA_MB: { fallback: 2048, min: 1, max: 65536 },
  DURABLE_FETCH_CONCURRENCY: { fallback: 1, min: 1, max: 32 },
  DURABLE_FETCH_POLL_INTERVAL_MS: { fallback: 3000, min: 10, max: 60000 },
  DURABLE_FETCH_LEASE_SECONDS: { fallback: 120, min: 30, max: 3600 },
  DURABLE_FETCH_HEARTBEAT_MS: { fallback: 20000, min: 1000, max: 300000 },
  DURABLE_FETCH_MAX_ATTEMPTS: { fallback: 3, min: 1, max: 10 },
  DURABLE_FETCH_DOWNLOAD_TIMEOUT_MS: { fallback: 120000, min: 1000, max: 300000 },
  DURABLE_CHUNK_SECONDS: { fallback: 600, min: 60, max: 900 },
  DURABLE_CHUNK_OVERLAP_SECONDS: { fallback: 0, min: 0, max: 60 },
  DURABLE_CHUNK_MAX_ATTEMPTS: { fallback: 3, min: 1, max: 20 },
  DURABLE_EXPERIMENTAL_CHUNK_SEEK_PREROLL_MS: { fallback: 0, min: 0, max: 60000 },
  DURABLE_ASR_GLOBAL_CONCURRENCY: { fallback: 1, min: 1, max: 2 },
  DURABLE_ASR_SLOT_LEASE_SECONDS: { fallback: 120, min: 30, max: 3600 },
  DURABLE_ASR_SLOT_HEARTBEAT_MS: { fallback: 20000, min: 1000, max: 300000 },
  DURABLE_ASR_REQUEST_TIMEOUT_MS: { fallback: 300000, min: 1000, max: 3600000 },
  DURABLE_ASR_QUARANTINE_SECONDS: { fallback: 900, min: 300, max: 86400 },
  DURABLE_FFMPEG_CONCURRENCY: { fallback: 2, min: 1, max: 8 },
});

function readInteger(env, name, { fallback, min, max }) {
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  if (typeof raw !== "string" || !/^[0-9]+$/.test(raw)) throw new Error(`INVALID_${name}`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`INVALID_${name}`);
  return value;
}

function validateDatabaseUrl(value) {
  try {
    const parsed = new URL(value);
    if (!["postgres:", "postgresql:"].includes(parsed.protocol) || !parsed.hostname || !parsed.pathname || parsed.pathname === "/") {
      throw new Error("INVALID_DATABASE_URL");
    }
  } catch {
    throw new Error("INVALID_DATABASE_URL");
  }
}

export function readDurableConfig(env = process.env) {
  const flag = env.DURABLE_JOBS_ENABLED ?? "false";
  if (flag !== "false" && flag !== "true") throw new Error("INVALID_DURABLE_JOBS_ENABLED");
  if (flag === "false") return Object.freeze({ enabled: false });
  const workersFlag = env.DURABLE_RUN_WORKERS ?? 'true';
  if (!['true','false'].includes(workersFlag)) throw new Error('INVALID_DURABLE_RUN_WORKERS');
  const chunkFlag = env.DURABLE_CHUNK_CHECKPOINTS_ENABLED ?? 'true';
  if (!['true','false'].includes(chunkFlag)) throw new Error('INVALID_DURABLE_CHUNK_CHECKPOINTS_ENABLED');
  const semaphoreFlag = env.DURABLE_EXPERIMENTAL_ASR_SEMAPHORE ?? 'false';
  if (!['true','false'].includes(semaphoreFlag)) throw new Error('INVALID_DURABLE_EXPERIMENTAL_ASR_SEMAPHORE');
  const adaptiveFlag = env.DURABLE_ASR_ADAPTIVE_CONCURRENCY ?? 'true';
  if (!['true','false'].includes(adaptiveFlag)) throw new Error('INVALID_DURABLE_ASR_ADAPTIVE_CONCURRENCY');
  const missing = REQUIRED_SECRETS.filter(name => typeof env[name] !== "string" || !env[name].trim());
  if (missing.length) throw new Error("MISSING_DURABLE_CONFIGURATION: " + missing.join(", "));
  validateDatabaseUrl(env.DATABASE_URL);
  const encryptionKey = env.TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY;
  if (!/^[A-Za-z0-9_-]{43}$/.test(encryptionKey) || Buffer.from(encryptionKey, 'base64url').toString('base64url') !== encryptionKey) {
    throw new Error('INVALID_REMOTE_URL_ENCRYPTION_KEY');
  }
  const idemKey = env.TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY;
  if (!/^[A-Za-z0-9_-]{43}$/.test(idemKey) || Buffer.from(idemKey, 'base64url').toString('base64url') !== idemKey || idemKey === encryptionKey) {
    throw new Error('INVALID_REMOTE_IDEMPOTENCY_KEY');
  }
  const values = {};
  for (const [name, limits] of Object.entries(INTEGER_OPTIONS)) values[name] = readInteger(env, name, limits);
  if (values.DURABLE_HEARTBEAT_MS >= values.DURABLE_JOB_LEASE_SECONDS * 500) {
    throw new Error("HEARTBEAT_MUST_BE_LESS_THAN_HALF_LEASE");
  }
  if (values.DURABLE_FETCH_HEARTBEAT_MS >= values.DURABLE_FETCH_LEASE_SECONDS * 500) throw new Error('FETCH_HEARTBEAT_MUST_BE_LESS_THAN_HALF_LEASE');
  if (values.DURABLE_CHUNK_OVERLAP_SECONDS >= values.DURABLE_CHUNK_SECONDS) throw new Error('INVALID_CHUNK_OVERLAP');
  if (values.DURABLE_EXPERIMENTAL_CHUNK_SEEK_PREROLL_MS > 0 && values.DURABLE_EXPERIMENTAL_CHUNK_SEEK_PREROLL_MS < 1000) throw new Error('INVALID_DURABLE_EXPERIMENTAL_CHUNK_SEEK_PREROLL_MS');
  if (values.DURABLE_ASR_SLOT_HEARTBEAT_MS >= values.DURABLE_ASR_SLOT_LEASE_SECONDS * 500) throw new Error('INVALID_DURABLE_ASR_SLOT_HEARTBEAT_MS');
  if (values.DURABLE_ASR_QUARANTINE_SECONDS * 1000 < values.DURABLE_ASR_REQUEST_TIMEOUT_MS) throw new Error('ASR_QUARANTINE_BELOW_TIMEOUT');
  if (values.DURABLE_ASR_GLOBAL_CONCURRENCY > 1 && semaphoreFlag !== 'true') throw new Error('ASR_SEMAPHORE_REQUIRED_FOR_CONCURRENCY');
  return Object.freeze({
    enabled: true,
    runWorkers: workersFlag === 'true',
    concurrency: values.DURABLE_WORKER_CONCURRENCY,
    pollIntervalMs: values.DURABLE_POLL_INTERVAL_MS,
    errorBackoffMs: values.DURABLE_ERROR_BACKOFF_MS,
    shutdownGraceMs: values.DURABLE_SHUTDOWN_GRACE_MS,
    leaseSeconds: values.DURABLE_JOB_LEASE_SECONDS,
    heartbeatIntervalMs: values.DURABLE_HEARTBEAT_MS,
    maxAttempts: values.DURABLE_MAX_ATTEMPTS,
    fetchDiskQuotaBytes: values.DURABLE_FETCH_DISK_QUOTA_MB * 1024 * 1024,
    fetchConcurrency: values.DURABLE_FETCH_CONCURRENCY,
    fetchPollIntervalMs: values.DURABLE_FETCH_POLL_INTERVAL_MS,
    fetchLeaseSeconds: values.DURABLE_FETCH_LEASE_SECONDS,
    fetchHeartbeatMs: values.DURABLE_FETCH_HEARTBEAT_MS,
    fetchMaxAttempts: values.DURABLE_FETCH_MAX_ATTEMPTS,
    fetchDownloadTimeoutMs: values.DURABLE_FETCH_DOWNLOAD_TIMEOUT_MS,
    chunkCheckpointsEnabled: chunkFlag === 'true',
    chunkSeconds: values.DURABLE_CHUNK_SECONDS,
    chunkOverlapSeconds: values.DURABLE_CHUNK_OVERLAP_SECONDS,
    chunkMaxAttempts: values.DURABLE_CHUNK_MAX_ATTEMPTS,
    chunkSeekPrerollMs: values.DURABLE_EXPERIMENTAL_CHUNK_SEEK_PREROLL_MS || null,
    experimentalAsrSemaphore: semaphoreFlag === 'true',
    asrGlobalConcurrency: values.DURABLE_ASR_GLOBAL_CONCURRENCY,
    asrSlotLeaseSeconds: values.DURABLE_ASR_SLOT_LEASE_SECONDS,
    asrSlotHeartbeatMs: values.DURABLE_ASR_SLOT_HEARTBEAT_MS,
    asrRequestTimeoutMs: values.DURABLE_ASR_REQUEST_TIMEOUT_MS,
    asrQuarantineSeconds: values.DURABLE_ASR_QUARANTINE_SECONDS,
    asrAdaptiveConcurrency: adaptiveFlag === 'true',
    ffmpegConcurrency: values.DURABLE_FFMPEG_CONCURRENCY,
  });
}
