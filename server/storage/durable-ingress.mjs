import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createWriteStream } from 'node:fs';

import { MIME_EXTENSIONS, JobError } from "../job-utils.mjs";

const DEFAULT_MAX_BYTES = 256 * 1024 * 1024;

// Keep the HTTP request socket available for a 413 response on chunked uploads.
function receiveUpload(source, destination, maxBytes) {
  return new Promise((resolve, reject) => {
    const output = createWriteStream(destination, { mode: 0o600 });
    let received = 0, settled = false;
    const finish = error => {
      if (settled) return;
      settled = true;
      source.off('data', onData); source.off('end', onEnd);
      source.off('error', onError); source.off('aborted', onAborted);
      output.off('drain', onDrain);
      if (error) { output.destroy(); source.resume(); reject(error); }
      else resolve(received);
    };
    const onError = error => finish(error);
    const onAborted = () => finish(new JobError('UPLOAD_ABORTED', 400));
    const onDrain = () => source.resume();
    const onData = chunk => {
      received += chunk.length;
      if (received > maxBytes) return finish(new JobError('UPLOAD_TOO_LARGE', 413));
      if (!output.write(chunk)) source.pause();
    };
    const onEnd = () => { if (!received) return finish(new JobError('EMPTY_UPLOAD', 400)); output.end(); };
    source.on('data', onData); source.once('end', onEnd);
    source.once('error', onError); source.once('aborted', onAborted);
    output.on('error', onError); output.on('drain', onDrain); output.once('finish', () => finish());
  });
}

export function createDurableUploadIngress({ jobStore, storage, maxBytes = DEFAULT_MAX_BYTES, diskQuota = null } = {}) {
  if (typeof jobStore?.saveJob !== "function") throw new Error("JOB_STORE_REQUIRED");
  if (typeof storage?.uploadFile !== "function") throw new Error("R2_STORAGE_REQUIRED");
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 2048 * 1024 * 1024) {
    throw new Error("INVALID_MAX_UPLOAD_BYTES");
  }
  if (diskQuota !== null && typeof diskQuota?.reserve !== 'function') throw new Error('INVALID_DISK_QUOTA');

  async function createUpload(req, mimeType) {
    const extension = MIME_EXTENSIONS.get(mimeType);
    if (!extension) throw new JobError("UNSUPPORTED_MEDIA_TYPE", 415);
    if (!req || typeof req.pipe !== "function") throw new JobError("INVALID_UPLOAD_STREAM", 400);

    const contentLength = req.headers?.["content-length"];
    if (contentLength !== undefined) {
      if (!/^\d+$/.test(String(contentLength))) throw new JobError("INVALID_CONTENT_LENGTH", 400);
      const size = Number(contentLength);
      if (!Number.isSafeInteger(size) || size > maxBytes) throw new JobError("UPLOAD_TOO_LARGE", 413);
    }

    const jobId = randomUUID();
    const filename = `source${extension}`;
    const directory = await mkdtemp(join(tmpdir(), "toolnet-srt-ingress-"));
    const localPath = join(directory, filename);
    let releaseQuota;

    try {
      if (diskQuota) releaseQuota = await diskQuota.reserve(maxBytes,directory);
      // Phase 1: Receive and validate the source.
      await receiveUpload(req, localPath, maxBytes);

      // Phase 2: Persist source on R2 first.
      const uploaded = await storage.uploadFile({ jobId, filename, localPath, contentType: mimeType });
      const expectedKey = `toolnet-srt/jobs/${jobId}/${filename}`;
      if (uploaded?.key !== expectedKey) throw new Error("R2_SOURCE_KEY_MISMATCH");

      // Phase 3: Publish queued job in Neon.
      const now = new Date().toISOString();
      const job = {
        id: jobId,
        status: "queued",
        stage: "queued",
        progress: 0,
        completedSegments: 0,
        totalSegments: null,
        extension,
        errorCode: null,
        sourceKey: uploaded.key,
        resultKey: null,
        createdAt: now,
        updatedAt: now,
      };
      try {
        return await jobStore.saveJob(job);
      } catch (error) {
        // A DB write may commit even if its response is lost. Retain the R2
        // object until a future reconciliation process identifies orphans.
        console.error("[ToolNet-SRT] queued job persistence uncertain", jobId, error.code || error.name || "UNKNOWN_ERROR");
        throw error;
      }
    } finally {
      // Temporary disk is not the source of truth.
      await rm(directory, { recursive: true, force: true });
      releaseQuota?.();
    }
  }

  return { createUpload };
}
