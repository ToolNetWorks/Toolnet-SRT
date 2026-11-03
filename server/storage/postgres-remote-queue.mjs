import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { JobError } from "../job-utils.mjs";
import { validateRemoteSource } from "./pinned-https.mjs";
import { createPostgresJobStore } from "./postgres.mjs";

function publicJob(job) {
  return {
    id: job.id, status: job.status, stage: job.stage, progress: job.progress,
    completedSegments: job.completedSegments, totalSegments: job.totalSegments,
    errorCode: job.errorCode, createdAt: job.createdAt, updatedAt: job.updatedAt,
    resultPath: job.status === "completed" ? `/v1/jobs/${job.id}/result` : null,
  };
}

function digestKey(encoded) {
  if (typeof encoded !== "string" || !/^[A-Za-z0-9_-]{43}$/.test(encoded)) {
    throw new Error("INVALID_REMOTE_IDEMPOTENCY_KEY");
  }
  const key = Buffer.from(encoded, "base64url");
  if (key.length !== 32 || key.toString("base64url") !== encoded) {
    throw new Error("INVALID_REMOTE_IDEMPOTENCY_KEY");
  }
  return key;
}

export function createPostgresRemoteQueue({
  pool, vault, allowedHosts = [], jobStoreFactory = createPostgresJobStore,
  idempotencySecret = process.env.TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY,
} = {}) {
  if (typeof pool?.connect !== "function") throw new Error("REMOTE_QUEUE_POOL_REQUIRED");
  if (typeof vault?.encrypt !== "function") throw new Error("REMOTE_URL_VAULT_REQUIRED");
  if (!Array.isArray(allowedHosts) || allowedHosts.some(host => typeof host !== "string")) throw new Error("INVALID_REMOTE_ALLOWED_HOSTS");
  if (typeof jobStoreFactory !== "function") throw new Error("INVALID_JOB_STORE_FACTORY");
  const hosts = [...new Set(allowedHosts.map(host => host.trim().toLowerCase()).filter(Boolean))];
  // Legacy callers without an idempotency key do not need this new secret.
  const secret = idempotencySecret === undefined ? null : digestKey(idempotencySecret);

  async function enqueue(rawUrl, options = {}) {
    if (!options || typeof options !== "object" || Array.isArray(options)) throw new JobError("INVALID_IDEMPOTENCY_KEY", 400);
    const { idempotencyKey, scope = "default" } = options;
    const keyed = idempotencyKey !== undefined;
    if (keyed && (typeof idempotencyKey !== "string" || !/^[\x21-\x7e]{1,255}$/.test(idempotencyKey))) {
      throw new JobError("INVALID_IDEMPOTENCY_KEY", 400);
    }
    if (keyed && (typeof scope !== "string" || !/^[\x21-\x7e]{1,128}$/.test(scope))) {
      throw new JobError("INVALID_IDEMPOTENCY_SCOPE", 400);
    }
    if (keyed && !secret) throw new Error("INVALID_REMOTE_IDEMPOTENCY_KEY");
    // DNS resolution and download belong to the fetch worker.
    const validatedUrl = validateRemoteSource(rawUrl, hosts);
    if (validatedUrl.hash) throw new JobError("INVALID_SOURCE_URL", 400);
    const normalized = validatedUrl.toString();
    const hmac = value => createHmac("sha256", secret).update(value).digest();
    const keyDigest = keyed ? hmac(JSON.stringify(["remote-key-v1", scope, idempotencyKey])) : null;
    const requestDigest = keyed ? hmac(JSON.stringify(["remote-request-v1", normalized])) : null;
    const id = randomUUID(), now = new Date().toISOString();
    let encryptedUrl;
    try {
      encryptedUrl = vault.encrypt(normalized, id);
    } catch (error) {
      if (error.message === "INVALID_REMOTE_SOURCE_URL") throw new JobError("INVALID_SOURCE_URL", 400);
      throw new Error("REMOTE_URL_ENCRYPTION_FAILED");
    }
    const job = { id, status: "queued", stage: "fetch_pending", progress: 0, completedSegments: 0,
      totalSegments: null, extension: ".mp4", sourceKey: null, resultKey: null, errorCode: null,
      createdAt: now, updatedAt: now };
    let client = null, transactionStarted = false, commitAttempted = false, discardClient = false;
    async function lookup() {
      const result = await client.query(`
        SELECT i.request_digest, j.* FROM toolnet_srt_remote_idempotency i
        JOIN toolnet_srt_jobs j ON j.id = i.job_id WHERE i.key_digest = $1
      `, [keyDigest]);
      const row = result.rows[0];
      if (!row) return null;
      if (!Buffer.isBuffer(row.request_digest) || row.request_digest.length !== requestDigest.length ||
          !timingSafeEqual(row.request_digest, requestDigest)) {
        throw new JobError("IDEMPOTENCY_KEY_CONFLICT", 409);
      }
      return {
        id: row.id, status: row.status, stage: row.stage, progress: row.progress,
        completedSegments: row.completed_segments, totalSegments: row.total_segments,
        errorCode: row.error_code, createdAt: new Date(row.created_at).toISOString(),
        updatedAt: new Date(row.updated_at).toISOString(),
      };
    }
    try {
      client = await pool.connect();
      await client.query("BEGIN");
      transactionStarted = true;
      if (keyed) {
        const existing = await lookup();
        if (existing) {
          commitAttempted = true;
          await client.query("COMMIT");
          transactionStarted = false;
          return publicJob(existing);
        }
      }
      const jobStore = jobStoreFactory({ pool: client });
      if (typeof jobStore?.saveJob !== "function") throw new Error("INVALID_JOB_STORE");
      const saved = await jobStore.saveJob(job);
      await client.query(`
        INSERT INTO toolnet_srt_remote_fetch_jobs (job_id, encrypted_url, status, attempt_count, available_at)
        VALUES ($1::uuid, $2, 'pending', 0, NOW())
      `, [id, encryptedUrl]);
      if (keyed) {
        const inserted = await client.query(`
          INSERT INTO toolnet_srt_remote_idempotency (key_digest, request_digest, job_id)
          VALUES ($1, $2, $3::uuid) ON CONFLICT (key_digest) DO NOTHING RETURNING job_id
        `, [keyDigest, requestDigest, id]);
        if (!inserted.rows.length) {
          // Undo both provisional rows before reading the winning transaction.
          await client.query("ROLLBACK");
          transactionStarted = false;
          await client.query("BEGIN");
          transactionStarted = true;
          const winner = await lookup();
          if (!winner) throw new Error("IDEMPOTENCY_WINNER_MISSING");
          commitAttempted = true;
          await client.query("COMMIT");
          transactionStarted = false;
          return publicJob(winner);
        }
      }
      commitAttempted = true;
      await client.query("COMMIT");
      transactionStarted = false;
      return publicJob(saved ?? job);
    } catch (error) {
      if (client && transactionStarted && !commitAttempted) {
        await client.query("ROLLBACK").catch(() => { discardClient = true; });
      }
      if (error instanceof JobError && error.code === "IDEMPOTENCY_KEY_CONFLICT") throw error;
      throw new JobError("REMOTE_QUEUE_WRITE_FAILED", 503);
    } finally {
      // A failed COMMIT may have succeeded on the server; discard the connection.
      client?.release(discardClient || (commitAttempted && transactionStarted) ? true : undefined);
    }
  }
  return { enqueue };
}
