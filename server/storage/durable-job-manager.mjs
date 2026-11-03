import { JobError } from "../job-utils.mjs";

const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const UUID_RE = new RegExp(`^${UUID}$`, "i");

function validateJobId(id) {
  if (typeof id !== "string" || !UUID_RE.test(id)) throw new JobError("JOB_NOT_FOUND", 404);
}

function publicJob(job) {
  return {
    id: job.id, status: job.status, stage: job.stage, progress: job.progress,
    completedSegments: job.completedSegments, totalSegments: job.totalSegments,
    errorCode: job.errorCode, createdAt: job.createdAt, updatedAt: job.updatedAt,
    resultPath: job.status === "completed" ? `/v1/jobs/${job.id}/result` : null,
  };
}

export function createDurableJobManager({ jobStore, resultStorage, cancellation, ingress = null, remoteIngress = null, remoteQueue = null } = {}) {
  if (typeof jobStore?.getJob !== "function") throw new Error("JOB_STORE_REQUIRED");
  if (typeof resultStorage?.getResult !== "function") throw new Error("RESULT_STORAGE_REQUIRED");
  if (typeof cancellation?.cancelJob !== "function") throw new Error("CANCELLATION_MANAGER_REQUIRED");
  if (ingress !== null && typeof ingress?.createUpload !== "function") throw new Error("INVALID_DURABLE_INGRESS");
  if (remoteIngress !== null && typeof remoteIngress?.createRemote !== "function") throw new Error("INVALID_DURABLE_REMOTE_INGRESS");
  if (remoteQueue !== null && typeof remoteQueue?.enqueue !== "function") throw new Error("INVALID_REMOTE_QUEUE");

  async function createUpload(req, mimeType) {
    if (!ingress) throw new JobError("DURABLE_UPLOAD_NOT_CONFIGURED", 503);
    const job = await ingress.createUpload(req, mimeType);
    if (!job || !UUID_RE.test(job.id ?? "") || job.status !== "queued") {
      throw new JobError("INVALID_QUEUED_JOB", 502);
    }
    return publicJob(job);
  }

  async function createRemote(rawUrl, options = {}) {
    if (remoteQueue) {
      if (typeof options?.idempotencyKey !== 'string') throw new JobError('INVALID_IDEMPOTENCY_KEY', 400);
      return remoteQueue.enqueue(rawUrl, { idempotencyKey: options.idempotencyKey, scope: 'single-tenant' });
    }
    if (!remoteIngress) throw new JobError("REMOTE_URL_DISABLED", 503);
    const job = await remoteIngress.createRemote(rawUrl);
    if (!job || !UUID_RE.test(job.id ?? "") || job.status !== "queued") {
      throw new JobError("INVALID_QUEUED_JOB", 502);
    }
    return publicJob(job);
  }

  async function findJob(id) {
    validateJobId(id);
    const job = await jobStore.getJob(id);
    if (!job) throw new JobError("JOB_NOT_FOUND", 404);
    return job;
  }

  async function getJob(id) {
    return publicJob(await findJob(id));
  }

  async function getResult(id) {
    const job = await findJob(id);
    if (job.status !== "completed") throw new JobError("JOB_NOT_COMPLETED", 409);
    const validKey = new RegExp(`^toolnet-srt/jobs/${job.id}/attempts/${UUID}/result\\.srt$`, "i");
    if (typeof job.resultKey !== "string" || !validKey.test(job.resultKey)) {
      throw new JobError("RESULT_NOT_AVAILABLE", 503);
    }
    const result = await resultStorage.getResult({ key: job.resultKey });
    if (!Buffer.isBuffer(result) || !result.length) throw new JobError("RESULT_NOT_AVAILABLE", 503);
    return result;
  }

  async function cancelJob(id) {
    validateJobId(id);
    const cancelled = await cancellation.cancelJob(id);
    if (!cancelled) {
      const job = await jobStore.getJob(id);
      if (!job) throw new JobError("JOB_NOT_FOUND", 404);
      throw new JobError("JOB_ALREADY_FINISHED", 409);
    }
    return getJob(id);
  }

  return { createUpload, createRemote, getJob, getResult, cancelJob };
}
