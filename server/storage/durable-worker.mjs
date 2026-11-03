import { randomUUID } from "node:crypto";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { splitMedia } from "../job-utils.mjs";
import { validateSourceKey } from "./source-keys.mjs";
import { transcribeCheckpointed } from "./chunk-transcription.mjs";
import { runWithAsrSlot } from "./asr-slot-runner.mjs";

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
  chunkCheckpoints = null,
  asrSemaphore = null,
  asrSlotHeartbeatMs = 20000,
  asrRequestTimeoutMs = 300000,
  diskQuota = null,
  maxSourceBytes = 256 * 1024 * 1024,
  workerId = `worker-${randomUUID()}`,
  split = splitMedia,
  extractChunk = undefined,
  transcribe = defaultTranscribe,
  toSrt = defaultToSrt,
  leaseSeconds = 120,
  heartbeatIntervalMs = 30000,
  maxAttempts = 3,
  maxSegments = 240,
  segmentSeconds = 60,
  chunkSeconds = 600,
  chunkOverlapSeconds = 0,
  chunkMaxAttempts = 3,
  chunkConcurrency = 1,
  chunkSeekPrerollMs = null,
  onTiming = null,
  timeoutMs = chunkCheckpoints ? 12 * 60 * 60 * 1000 : 2 * 60 * 60 * 1000,
} = {}) {
  if (typeof leases?.claimJob !== "function" || typeof leases?.heartbeatJob !== "function" ||
      typeof leases?.completeJob !== "function" || typeof leases?.failJob !== "function") {
    throw new Error("LEASE_MANAGER_REQUIRED");
  }
  if (typeof storage?.downloadFile !== "function") throw new Error("R2_SOURCE_STORAGE_REQUIRED");
  if (typeof results?.putResult !== "function") throw new Error("R2_RESULT_STORAGE_REQUIRED");
  if (diskQuota !== null && typeof diskQuota?.reserve !== 'function') throw new Error('INVALID_DISK_QUOTA');
  if (!Number.isSafeInteger(maxSourceBytes) || maxSourceBytes < 1 || maxSourceBytes > 2147483648) throw new Error('INVALID_MAX_SOURCE_BYTES');
  if (progress != null && typeof progress.updateProgress !== "function") throw new Error("INVALID_PROGRESS_MANAGER");
  if (chunkCheckpoints != null && !['initialize','claim','complete','fail'].every(name => typeof chunkCheckpoints[name] === 'function')) {
    throw new Error('INVALID_CHUNK_CHECKPOINTS');
  }
  if (asrSemaphore != null && !['acquire','heartbeat','release','rateLimited'].every(name => typeof asrSemaphore[name] === 'function')) throw new Error('INVALID_ASR_SEMAPHORE');
  if (!Number.isSafeInteger(asrSlotHeartbeatMs) || asrSlotHeartbeatMs < 1000 || asrSlotHeartbeatMs > 300000 ||
      (Number.isSafeInteger(asrSemaphore?.leaseSeconds) && asrSlotHeartbeatMs >= asrSemaphore.leaseSeconds * 500)) throw new Error('INVALID_ASR_SLOT_HEARTBEAT');
  if (!Number.isSafeInteger(asrRequestTimeoutMs) || asrRequestTimeoutMs < 1000 || asrRequestTimeoutMs > 3600000) throw new Error('INVALID_ASR_REQUEST_TIMEOUT');
  if (onTiming !== null && typeof onTiming !== 'function') throw new Error('INVALID_TIMING_CALLBACK');
  if (extractChunk !== undefined && typeof extractChunk !== 'function') throw new Error('INVALID_CHUNK_EXTRACTOR');
  if (!Number.isSafeInteger(chunkSeconds) || chunkSeconds < 1 || chunkSeconds > 900 ||
      !Number.isSafeInteger(chunkOverlapSeconds) || chunkOverlapSeconds < 0 || chunkOverlapSeconds >= chunkSeconds) throw new Error('INVALID_CHUNK_CONFIG');
  if (!Number.isSafeInteger(chunkMaxAttempts) || chunkMaxAttempts < 1 || chunkMaxAttempts > 20) throw new Error('INVALID_CHUNK_MAX_ATTEMPTS');
  if (![1,2].includes(chunkConcurrency) || (chunkConcurrency > 1 && !asrSemaphore)) throw new Error('INVALID_CHUNK_CONCURRENCY');
  if (chunkSeekPrerollMs !== null && (!Number.isSafeInteger(chunkSeekPrerollMs) || chunkSeekPrerollMs < 1000 || chunkSeekPrerollMs > 60000)) throw new Error('INVALID_SEEK_PREROLL');
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
    let releaseDiskQuota = null;
    const controller = new AbortController();
    let heartbeatFailure = false;
    let progressLeaseLost = false;
    let heartbeatInFlight = Promise.resolve();
    const measure = async (phase, operation) => {
      const started = performance.now();
      try { return await operation(); }
      finally { try { const result = onTiming?.({ jobId: job?.id ?? null, phase, durationMs: performance.now() - started });
        if (result?.then) Promise.resolve(result).catch(() => {}); } catch {} }
    };

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
      const transcribeOwned = async (path, signal, { chunkIndex }) => {
        if (!asrSemaphore) return transcribe(path, signal);
        return runWithAsrSlot({ semaphore: asrSemaphore, owner: { jobId, workerId, jobLeaseToken: leaseToken, chunkIndex },
          signal, ensureOwnership, heartbeatMs: asrSlotHeartbeatMs, requestTimeoutMs: asrRequestTimeoutMs,
          onRateLimit: () => controller.abort(),
          transcribe: slotSignal => transcribe(path, slotSignal),
          onWait: durationMs => onTiming?.({ jobId, phase: 'asr_slot_wait', chunkIndex, durationMs }),
        });
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
      if (diskQuota) {
        releaseDiskQuota = await diskQuota.reserve(maxSourceBytes,directory);
      }
      const input = join(directory, filename);
      const segmentDir = join(directory, "segments");
      await ensureOwnership();
      await reportProgress("preparing", 1);
      await reportProgress("downloading", 3);
      await measure('download', () => storage.downloadFile({ jobId, filename, localPath: input, sourceKey: job.source_key, maxBytes:maxSourceBytes, signal:controller.signal }));
      assertActive(controller.signal);
      await reportProgress("segmenting", 5);
      await mkdir(segmentDir, { recursive: true });
      let combined;
      if (chunkCheckpoints) {
        combined = await transcribeCheckpointed({ input, directory: segmentDir,
          lease: { jobId, workerId, leaseToken }, checkpoints: chunkCheckpoints, transcribe: transcribeOwned, signal: controller.signal,
          chunkSeconds, overlapSeconds: chunkOverlapSeconds, chunkMaxAttempts, chunkConcurrency, seekPrerollMs: chunkSeekPrerollMs, ensureOwnership,
          reserveChunkSpace:diskQuota ? bytes => diskQuota.reserve(bytes,directory) : null,
          extract:extractChunk,
          onTiming: onTiming ? event => onTiming({ jobId, ...event }) : null,
          onProgress: async (done, total, value) => {
            completedSegments = done; totalSegments = total;
            await reportProgress('transcribing', value);
          },
        });
      } else {
      const segments = await split(input, segmentDir, {
        segmentSeconds,
        signal: controller.signal,
        timeoutMs: Math.min(timeoutMs, 30 * 60 * 1000),
      });
      if (!Array.isArray(segments) || !segments.length) throw new Error("NO_AUDIO_SEGMENTS");
      if (segments.length > maxSegments) throw new Error("TOO_MANY_SEGMENTS");

      totalSegments = segments.length;
      await reportProgress("transcribing", 10);
      combined = [];
      for (const [index, segment] of segments.entries()) {
        assertActive(controller.signal);
        const recognized = await transcribeOwned(segment.path, controller.signal, { chunkIndex:index });
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
      }
      if (!combined.length) throw new Error("NO_SPEECH_DETECTED");
      combined.sort((a, b) => a.startMs - b.startMs);
      const srt = await measure('format_srt', () => toSrt(combined));
      if ((typeof srt !== "string" && !Buffer.isBuffer(srt)) || !srt.length) {
        throw new Error("INVALID_SRT_RESULT");
      }

      await ensureOwnership();
      await reportProgress("uploading_result", 96);
      const uploaded = await measure('upload_result', () => results.putResult({ jobId, leaseToken, srt }));
      const expectedResultKey = `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt`;
      if (uploaded?.key !== expectedResultKey) throw new Error("RESULT_KEY_MISMATCH");
      await ensureOwnership();
      await reportProgress("finalizing", 99);
      const completed = await leases.completeJob({ jobId, workerId, leaseToken, resultKey: uploaded.key });
      if (!completed) throw new Error("LEASE_LOST");
      return { status: "completed", jobId, resultKey: uploaded.key };
    } catch (error) {
      if (!job) throw error;
      const errorCode = heartbeatFailure || progressLeaseLost || error?.message === 'LEASE_LOST' ? "LEASE_LOST" : controller.signal.aborted ? "PROCESSING_ABORTED" : "PROCESSING_FAILED";
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
      let cleaned=true;
      if (directory) await rm(directory, { recursive: true, force: true }).catch(() => { cleaned=false; });
      if (cleaned) releaseDiskQuota?.();
      running = false;
    }
  }

  return { runOnce, isRunning: () => running, workerId };
}
