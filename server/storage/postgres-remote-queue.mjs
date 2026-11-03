import { randomUUID } from "node:crypto";
import { JobError } from "../job-utils.mjs";
import { validateRemoteSource } from "./pinned-https.mjs";
import { createPostgresJobStore } from "./postgres.mjs";

export function createPostgresRemoteQueue({ pool, vault, allowedHosts = [], jobStoreFactory = createPostgresJobStore } = {}) {
  if (typeof pool?.connect !== "function") throw new Error("REMOTE_QUEUE_POOL_REQUIRED");
  if (typeof vault?.encrypt !== "function") throw new Error("REMOTE_URL_VAULT_REQUIRED");
  if (!Array.isArray(allowedHosts) || allowedHosts.some(host => typeof host !== "string")) throw new Error("INVALID_REMOTE_ALLOWED_HOSTS");
  if (typeof jobStoreFactory !== "function") throw new Error("INVALID_JOB_STORE_FACTORY");
  const hosts = [...new Set(allowedHosts.map(host => host.trim().toLowerCase()).filter(Boolean))];

  async function enqueue(rawUrl) {
    // DNS resolution and download belong to the fetch worker.
    const validatedUrl = validateRemoteSource(rawUrl, hosts);
    if (validatedUrl.hash) throw new JobError("INVALID_SOURCE_URL", 400);
    const id = randomUUID(), now = new Date().toISOString();
    let encryptedUrl;
    try {
      encryptedUrl = vault.encrypt(validatedUrl.toString(), id);
    } catch (error) {
      if (error.message === "INVALID_REMOTE_SOURCE_URL") throw new JobError("INVALID_SOURCE_URL", 400);
      throw new Error("REMOTE_URL_ENCRYPTION_FAILED");
    }
    const job = { id, status: "queued", stage: "fetch_pending", progress: 0, completedSegments: 0,
      totalSegments: null, extension: ".mp4", sourceKey: null, resultKey: null, errorCode: null,
      createdAt: now, updatedAt: now };
    let client = null, transactionStarted = false;
    try {
      client = await pool.connect();
      await client.query("BEGIN");
      transactionStarted = true;
      const jobStore = jobStoreFactory({ pool: client });
      if (typeof jobStore?.saveJob !== "function") throw new Error("INVALID_JOB_STORE");
      await jobStore.saveJob(job);
      await client.query(`
        INSERT INTO toolnet_srt_remote_fetch_jobs (job_id, encrypted_url, status, attempt_count, available_at)
        VALUES ($1::uuid, $2, 'pending', 0, NOW())
      `, [id, encryptedUrl]);
      await client.query("COMMIT");
      transactionStarted = false;
      return { id, status: "queued", stage: "fetch_pending", progress: 0, completedSegments: 0,
        totalSegments: null, errorCode: null, createdAt: now, updatedAt: now, resultPath: null };
    } catch {
      if (client && transactionStarted) await client.query("ROLLBACK").catch(() => {});
      throw new JobError("REMOTE_QUEUE_WRITE_FAILED", 503);
    } finally { client?.release(); }
  }
  return { enqueue };
}
