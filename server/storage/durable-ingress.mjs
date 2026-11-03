import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { MIME_EXTENSIONS, JobError, streamLimited } from "../job-utils.mjs";

const DEFAULT_MAX_BYTES = 256 * 1024 * 1024;

export function createDurableUploadIngress({ jobStore, storage, maxBytes = DEFAULT_MAX_BYTES } = {}) {
  if (typeof jobStore?.saveJob !== "function") throw new Error("JOB_STORE_REQUIRED");
  if (typeof storage?.uploadFile !== "function") throw new Error("R2_STORAGE_REQUIRED");
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 2048 * 1024 * 1024) {
    throw new Error("INVALID_MAX_UPLOAD_BYTES");
  }

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

    try {
      // Phase 1: Receive and validate the source.
      await streamLimited(req, localPath, maxBytes);

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
    }
  }

  return { createUpload };
}
