import { randomUUID } from "node:crypto";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { splitMedia } from "../job-utils.mjs";
import { validateSourceKey } from "./source-keys.mjs";

const EXTENSION_RE = /^\.(mp3|mp4|wav|m4a|flac|ogg|mov|mkv|webm)$/;
const WORKER_RE = /^[a-zA-Z0-9._:-]{1,128}$/;

function assertActive(signal) {
  if (signal.aborted) throw new Error("PROCESSING_ABORTED");
}

async function defaultTranscribe(input, signal) {
  const { transcribe } = await import("../../dist/index.js");
  return transcribe({ input, signal, queryTimeoutMs: 300000 });
}

async function defaultToSrt(segments) {
  const { toSrt } = await import("../../dist/index.js");
  return toSrt(segments);
}

export function createDurableWorker({
  leases,
  storage,
  results,
  progress = null,
  workerId = `worker-${randomUUID()}`,
  split = splitMedia,
  transcribe = defaultTranscribe,
  toSrt = defaultToSrt,
  leaseSeconds = 120,
  heartbeatIntervalMs = 30000,
  maxAttempts = 3,
  maxSegments = 240,
  segmentSeconds = 60,
  timeoutMs = 2 * 60 * 60 * 1000,
} = {}) {
  if (typeof leases?.claimJob !== "function" || typeof leases?.heartbeatJob !== "function" ||
      typeof leases?.completeJob !== "function" || typeof leases?.failJob !== "function") {
    throw new Error("LEASE_MANAGER_REQUIRED");
  }
  if (typeof storage?.downloadFile !== "function") throw new Error("R2_SOURCE_STORAGE_REQUIRED");
  if (typeof results?.putResult !== "function") throw new Error("R2_RESULT_STORAGE_REQUIRED");
  if (progress != null && typeof progress.updateProgress !== "function") throw new Error("INVALID_PROGRESS_MANAGER");
  if (!WORKER_RE.test(workerId)) throw new Error("INVALID_WORKER_ID");
  if (!Number.isSafeInteger(leaseSeconds) || leaseSeconds < 30 || leaseSeconds > 3600) {
    throw new Error("INVALID_LEASE_SECONDS");
  }
  if (!Number.isSafeInteger(heartbeatIntervalMs) || heartbeatIntervalMs < 1000 || heartbeatIntervalMs >= leaseSeconds * 500) {
    throw new Error("INVALID_HEARTBEAT_INTERVAL");
  }
  if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 20) throw new Error("INVALID_MAX_ATTEMPTS");
  if (!Number.isSafeInteger(maxSegments) || maxSegments < 1 || maxSegments > 1000) throw new Error("INVALID_MAX_SEGMENTS");
  if (!Number.isSafeInteger(segmentSeconds) || segmentSeconds < 1 || segmentSeconds > 900) throw new Error("INVALID_SEGMENT_SECONDS");
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1000 || timeoutMs > 24 * 60 * 60 * 1000) {
    throw new Error("INVALID_WORKER_TIMEOUT");
  }

  let running = false;

  async function runOnce() {
    if (running) throw new Error("WORKER_ALREADY_RUNNING");
    running = true;
    let job = null;
    let directory = null;
    let timeout = null;
    let heartbeatTimer = null;
    const controller = new AbortController();
    let heartbeatFailure = false;
    let progressLeaseLost = false;
    let heartbeatInFlight = Promise.resolve();

    try {
      job = await leases.claimJob(workerId, { leaseSeconds, maxAttempts });
      if (!job) return { status: "idle" };

      const jobId = job.id;
      const leaseToken = job.lease_token;
      const extension = job.extension;
      if (!EXTENSION_RE.test(extension ?? "")) throw new Error("INVALID_SOURCE_EXTENSION");
      const filename = `source${extension}`;
      try { validateSourceKey(jobId, filename, job.source_key); }
      catch { throw new Error("SOURCE_KEY_MISMATCH"); }

      let completedSegments = 0;
      let totalSegments = null;
      const reportProgress = async (stage, value) => {
        assertActive(controller.signal);
        // Production durable mode must provide a progress manager.
        if (!progress) return;
        const updated = await progress.updateProgress({ jobId, workerId, leaseToken, stage,
          progress: value, completedSegments, totalSegments });
        if (!updated || updated.id !== jobId) {
          progressLeaseLost = true;
          controller.abort();
          throw new Error("LEASE_LOST");
        }
        assertActive(controller.signal);
      };

      const heartbeat = async () => {
        const refreshed = await leases.heartbeatJob({ jobId, workerId, leaseToken, leaseSeconds });
        if (!refreshed) throw new Error("LEASE_LOST");
      };
      const ensureOwnership = async () => {
        assertActive(controller.signal);
        await heartbeatInFlight;
        if (heartbeatFailure) throw new Error("LEASE_LOST");
        await heartbeat();
        assertActive(controller.signal);
      };

      timeout = setTimeout(() => controller.abort(), timeoutMs);
      heartbeatTimer = setInterval(() => {
        heartbeatInFlight = heartbeatInFlight.then(heartbeat).catch(() => {
          heartbeatFailure = true;
          controller.abort();
        });
      }, heartbeatIntervalMs);
      timeout.unref?.();
      heartbeatTimer.unref?.();

      directory = await mkdtemp(join(tmpdir(), "toolnet-srt-worker-"));
      const input = join(directory, filename);
      const segmentDir = join(directory, "segments");
      await ensureOwnership();
      await reportProgress("preparing", 1);
      await reportProgress("downloading", 3);
      await storage.downloadFile({ jobId, filename, localPath: input, sourceKey: job.source_key });
      assertActive(controller.signal);
      await reportProgress("segmenting", 5);
      await mkdir(segmentDir, { recursive: true });
      const segments = await split(input, segmentDir, {
        segmentSeconds,
        signal: controller.signal,
        timeoutMs: Math.min(timeoutMs, 30 * 60 * 1000),
      });
      if (!Array.isArray(segments) || !segments.length) throw new Error("NO_AUDIO_SEGMENTS");
      if (segments.length > maxSegments) throw new Error("TOO_MANY_SEGMENTS");

      totalSegments = segments.length;
      await reportProgress("transcribing", 10);
      const combined = [];
      for (const [index, segment] of segments.entries()) {
        assertActive(controller.signal);
        const recognized = await transcribe(segment.path, controller.signal);
        if (!Array.isArray(recognized)) throw new Error("INVALID_ASR_RESULT");
        for (const item of recognized) {
          if (!Number.isFinite(item.startMs) || !Number.isFinite(item.endMs) ||
              typeof item.text !== "string" || !item.text.trim()) continue;
          const startMs = Math.max(0, Math.round(item.startMs + segment.startMs));
          const endMs = Math.max(startMs + 1, Math.round(item.endMs + segment.startMs));
          combined.push({ text: item.text, startMs, endMs });
        }
        completedSegments = index + 1;
        const percent = 10 + Math.floor(85 * completedSegments / totalSegments);
        await reportProgress("transcribing", percent);
      }
      if (!combined.length) throw new Error("NO_SPEECH_DETECTED");
      combined.sort((a, b) => a.startMs - b.startMs);
      const srt = await toSrt(combined);
      if ((typeof srt !== "string" && !Buffer.isBuffer(srt)) || !srt.length) {
        throw new Error("INVALID_SRT_RESULT");
      }

      await ensureOwnership();
      await reportProgress("uploading_result", 96);
      const uploaded = await results.putResult({ jobId, leaseToken, srt });
      const expectedResultKey = `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt`;
      if (uploaded?.key !== expectedResultKey) throw new Error("RESULT_KEY_MISMATCH");
      await ensureOwnership();
      await reportProgress("finalizing", 99);
      const completed = await leases.completeJob({ jobId, workerId, leaseToken, resultKey: uploaded.key });
      if (!completed) throw new Error("LEASE_LOST");
      return { status: "completed", jobId, resultKey: uploaded.key };
    } catch (error) {
      if (!job) throw error;
      const errorCode = heartbeatFailure || progressLeaseLost ? "LEASE_LOST" : controller.signal.aborted ? "PROCESSING_ABORTED" : "PROCESSING_FAILED";
      try {
        const failed = await leases.failJob({ jobId: job.id, workerId, leaseToken: job.lease_token, errorCode, maxAttempts });
        return { status: failed?.status ?? "lease_lost", jobId: job.id, errorCode };
      } catch (failureError) {
        // Leave the job to be reclaimed after lease expiry.
        console.error("[ToolNet-SRT] job failure update failed", job.id, failureError.code || failureError.name);
        throw failureError;
      }
    } finally {
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      if (timeout) clearTimeout(timeout);
      controller.abort();
      if (directory) await rm(directory, { recursive: true, force: true }).catch(() => {});
      running = false;
    }
  }

  return { runOnce, isRunning: () => running, workerId };
}
