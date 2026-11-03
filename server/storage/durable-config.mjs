const REQUIRED_SECRETS = Object.freeze([
  "DATABASE_URL", "R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET", "TOOLNET_SRT_API_KEY",
]);
const INTEGER_OPTIONS = Object.freeze({
  DURABLE_WORKER_CONCURRENCY: { fallback: 1, min: 1, max: 32 },
  DURABLE_POLL_INTERVAL_MS: { fallback: 3000, min: 10, max: 60000 },
  DURABLE_ERROR_BACKOFF_MS: { fallback: 10000, min: 10, max: 120000 },
  DURABLE_SHUTDOWN_GRACE_MS: { fallback: 30000, min: 1, max: 300000 },
  DURABLE_JOB_LEASE_SECONDS: { fallback: 120, min: 30, max: 3600 },
  DURABLE_HEARTBEAT_MS: { fallback: 20000, min: 1000, max: 300000 },
  DURABLE_MAX_ATTEMPTS: { fallback: 3, min: 1, max: 10 },
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
  const missing = REQUIRED_SECRETS.filter(name => typeof env[name] !== "string" || !env[name].trim());
  if (missing.length) throw new Error("MISSING_DURABLE_CONFIGURATION: " + missing.join(", "));
  validateDatabaseUrl(env.DATABASE_URL);
  const values = {};
  for (const [name, limits] of Object.entries(INTEGER_OPTIONS)) values[name] = readInteger(env, name, limits);
  if (values.DURABLE_HEARTBEAT_MS >= values.DURABLE_JOB_LEASE_SECONDS * 500) {
    throw new Error("HEARTBEAT_MUST_BE_LESS_THAN_HALF_LEASE");
  }
  return Object.freeze({
    enabled: true,
    concurrency: values.DURABLE_WORKER_CONCURRENCY,
    pollIntervalMs: values.DURABLE_POLL_INTERVAL_MS,
    errorBackoffMs: values.DURABLE_ERROR_BACKOFF_MS,
    shutdownGraceMs: values.DURABLE_SHUTDOWN_GRACE_MS,
    leaseSeconds: values.DURABLE_JOB_LEASE_SECONDS,
    heartbeatIntervalMs: values.DURABLE_HEARTBEAT_MS,
    maxAttempts: values.DURABLE_MAX_ATTEMPTS,
  });
}
