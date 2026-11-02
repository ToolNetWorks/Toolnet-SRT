import { randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdir, readdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { MIME_EXTENSIONS, JobError, parsePositiveInt, splitMedia, downloadAllowed, streamLimited, validateRemoteUrl } from './job-utils.mjs';

const STATUS = new Set(['queued', 'processing', 'completed', 'failed', 'cancelled']);
const cleanId = id => /^[0-9a-f]{8}-[0-9a-f-]{27,36}$/i.test(id);

export class JobManager {
  #jobs = new Map();
  #pending = [];
  #controllers = new Map();
  #remoteSources = new Map();
  #initializing;
  #running = false;

  constructor(options = {}) {
    this.dataDir = options.dataDir ?? process.env.TOOLNET_SRT_DATA_DIR ?? '/tmp/toolnet-srt-jobs';
    this.maxBytes = options.maxBytes ?? parsePositiveInt(process.env.TOOLNET_SRT_MAX_UPLOAD_MB, 256, 2048) * 1024 * 1024;
    this.maxJobs = options.maxJobs ?? parsePositiveInt(process.env.TOOLNET_SRT_MAX_JOBS, 20, 200);
    this.maxSegments = options.maxSegments ?? parsePositiveInt(process.env.TOOLNET_SRT_MAX_SEGMENTS, 240, 1000);
    this.segmentSeconds = options.segmentSeconds ?? parsePositiveInt(process.env.TOOLNET_SRT_SEGMENT_SECONDS, 60, 900);
    this.maxDurationMs = options.maxDurationMs ?? parsePositiveInt(process.env.TOOLNET_SRT_JOB_TIMEOUT_MS, 2 * 60 * 60 * 1000, 24 * 60 * 60 * 1000);
    this.retentionMs = options.retentionMs ?? parsePositiveInt(process.env.TOOLNET_SRT_RETENTION_HOURS, 24, 168) * 3600 * 1000;
    this.allowedHosts = options.allowedHosts ?? (process.env.TOOLNET_SRT_ALLOWED_MEDIA_HOSTS || '')
      .split(',').map(x => x.trim().toLowerCase()).filter(Boolean);
    this.transcribe = options.transcribe ?? (async (input, signal) => {
      const { transcribe } = await import('../dist/index.js');
      return transcribe({ input, signal, queryTimeoutMs: 300000 });
    });
    this.toSrt = options.toSrt ?? (async segments => {
      const { toSrt } = await import('../dist/index.js');
      return toSrt(segments);
    });
    this.split = options.split ?? splitMedia;
    this.download = options.download ?? downloadAllowed;
    this.canRun = options.canRun ?? (() => true);
  }

  async initOnce() {
    if (!this.#initializing) this.#initializing = this.#init();
    return this.#initializing;
  }

  async #init() {
    await mkdir(this.dataDir, { recursive: true, mode: 0o700 });
    for (const id of await readdir(this.dataDir)) {
      if (!cleanId(id)) continue;
      let job;
      try { job = JSON.parse(await readFile(this.#metadataPath(id), 'utf8')); }
      catch { continue; }
      if (!STATUS.has(job.status) || job.id !== id) continue;
      if (Date.now() - Date.parse(job.createdAt) > this.retentionMs) {
        await rm(this.#folder(id), { recursive: true, force: true });
        continue;
      }
      this.#jobs.set(id, job);
      if (['queued', 'processing'].includes(job.status)) {
        if (existsSync(this.#inputPath(id, job.extension))) {
          job.status = 'queued';
          job.stage = 'recovered';
          this.#pending.push(id);
        } else {
          job.status = 'failed';
          job.stage = 'failed';
          job.errorCode = 'SOURCE_LOST_ON_RESTART';
        }
        await this.#save(job);
      } else if (job.status === 'completed' && !existsSync(this.#resultPath(id))) {
        job.status = 'failed';
        job.errorCode = 'RESULT_LOST_ON_RESTART';
        await this.#save(job);
      }
    }
    this.kick();
    const timer = setInterval(() => {
      void this.cleanupExpired().catch(error => console.error('Job cleanup failed', error));
    }, 15 * 60 * 1000);
    timer.unref();
  }

  async cleanupExpired() {
    for (const [id, job] of this.#jobs.entries()) {
      if (['queued', 'processing'].includes(job.status)) continue;
      if (Date.now() - Date.parse(job.createdAt) <= this.retentionMs) continue;
      await rm(this.#folder(id), { recursive: true, force: true });
      this.#jobs.delete(id);
    }
  }

  #folder(id) { return join(this.dataDir, id); }
  #metadataPath(id) { return join(this.#folder(id), 'job.json'); }
  #resultPath(id) { return join(this.#folder(id), 'result.srt'); }
  #inputPath(id, extension) { return join(this.#folder(id), `source${extension}`); }

  async #save(job) {
    job.updatedAt = new Date().toISOString();
    const tmp = join(this.#folder(job.id), `job.${randomUUID()}.tmp`);
    await writeFile(tmp, JSON.stringify(job, null, 2), { mode: 0o600 });
    await rename(tmp, this.#metadataPath(job.id));
  }

  #newJob(extension) {
    const now = new Date().toISOString();
    return {
      id: randomUUID(), status: 'queued', stage: 'uploading',
      progress: 0, completedSegments: 0, totalSegments: null,
      extension, createdAt: now, updatedAt: now,
      errorCode: null,
    };
  }

  #available() {
    const count = [...this.#jobs.values()].filter(j => ['queued', 'processing'].includes(j.status)).length;
    if (count >= this.maxJobs) throw new JobError('QUEUE_FULL', 429);
  }

  async createUpload(req, mimeType) {
    await this.initOnce();
    this.#available();
    const extension = MIME_EXTENSIONS.get(mimeType);
    if (!extension) throw new JobError('UNSUPPORTED_MEDIA_TYPE', 415);
    const contentLength = req.headers?.['content-length'];
    if (contentLength !== undefined && (!/^\d+$/.test(contentLength) || Number(contentLength) > this.maxBytes)) {
      throw new JobError('UPLOAD_TOO_LARGE', 413);
    }
    const job = this.#newJob(extension);
    await mkdir(this.#folder(job.id), { recursive: false, mode: 0o700 });
    try {
      await streamLimited(req, this.#inputPath(job.id, extension), this.maxBytes);
      job.stage = 'queued';
      await this.#save(job);
      this.#jobs.set(job.id, job);
      this.#pending.push(job.id);
      this.kick();
      return this.publicJob(job);
    } catch (error) {
      await rm(this.#folder(job.id), { recursive: true, force: true });
      throw error;
    }
  }

  async createRemote(rawUrl) {
    await this.initOnce();
    this.#available();
    if (!this.allowedHosts.length) throw new JobError('REMOTE_URL_DISABLED', 503);
    const url = await validateRemoteUrl(rawUrl, this.allowedHosts);
    // Source URLs may carry signed tokens; never persist them in job metadata.
    const job = this.#newJob('.mp4');
    job.stage = 'queued';
    await mkdir(this.#folder(job.id), { recursive: false, mode: 0o700 });
    await this.#save(job);
    this.#jobs.set(job.id, job);
    this.#remoteSources.set(job.id, url.toString());
    this.#pending.push(job.id);
    this.kick();
    return this.publicJob(job);
  }

  publicJob(job) {
    return {
      id: job.id, status: job.status, stage: job.stage,
      progress: job.progress, completedSegments: job.completedSegments,
      totalSegments: job.totalSegments, errorCode: job.errorCode,
      createdAt: job.createdAt, updatedAt: job.updatedAt,
      resultPath: job.status === 'completed' ? `/v1/jobs/${job.id}/result` : null,
    };
  }

  async getJob(id) {
    await this.initOnce();
    if (!cleanId(id)) throw new JobError('JOB_NOT_FOUND', 404);
    const job = this.#jobs.get(id);
    if (!job) throw new JobError('JOB_NOT_FOUND', 404);
    return this.publicJob(job);
  }

  async getResult(id) {
    const job = await this.getJob(id);
    if (job.status !== 'completed') throw new JobError('JOB_NOT_COMPLETED', 409);
    return readFile(this.#resultPath(id));
  }

  async cancelJob(id) {
    await this.initOnce();
    const job = this.#jobs.get(id);
    if (!job || !cleanId(id)) throw new JobError('JOB_NOT_FOUND', 404);
    if (['completed', 'failed', 'cancelled'].includes(job.status)) {
      throw new JobError('JOB_ALREADY_FINISHED', 409);
    }
    job.status = 'cancelled';
    job.stage = 'cancelled';
    this.#controllers.get(id)?.abort();
    this.#pending = this.#pending.filter(p => p !== id);
    this.#remoteSources.delete(id);
    if (!this.#controllers.has(id)) {
      await rm(this.#inputPath(id, job.extension), { force: true }).catch(() => {});
    }
    await this.#save(job);
    return this.publicJob(job);
  }

  isBusy() { return this.#running; }

  kick() {
    if (this.#running || !this.canRun() || !this.#pending.length) return;
    const id = this.#pending.shift();
    const job = this.#jobs.get(id);
    if (!job || job.status !== 'queued') {
      queueMicrotask(() => this.kick());
      return;
    }
    this.#running = true;
    void this.#run(job).finally(() => {
      this.#running = false;
      this.#controllers.delete(id);
      this.#remoteSources.delete(id);
      this.kick();
    });
  }

  async #run(job) {
    const controller = new AbortController();
    this.#controllers.set(job.id, controller);
    const timer = setTimeout(() => controller.abort(), this.maxDurationMs);
    const folder = this.#folder(job.id);
    const input = this.#inputPath(job.id, job.extension);
    const segmentDir = join(folder, 'segments');
    try {
      job.status = 'processing';
      job.stage = 'preparing';
      job.progress = 1;
      await this.#save(job);
      const remoteUrl = this.#remoteSources.get(job.id);
      if (remoteUrl) {
        await this.download(remoteUrl, input, {
          allowedHosts: this.allowedHosts, maxBytes: this.maxBytes, signal: controller.signal,
        });
        this.#remoteSources.delete(job.id);
      }
      if (controller.signal.aborted) throw new JobError('CANCELLED', 409);
      await mkdir(segmentDir, { mode: 0o700 });
      job.stage = 'segmenting'; job.progress = 5;
      await this.#save(job);
      const segments = await this.split(input, segmentDir, {
        segmentSeconds: this.segmentSeconds, signal: controller.signal,
        timeoutMs: Math.min(this.maxDurationMs, 30 * 60 * 1000),
      });
      if (segments.length > this.maxSegments) throw new JobError('TOO_MANY_SEGMENTS', 422);
      job.totalSegments = segments.length;
      job.stage = 'transcribing'; job.progress = 10;
      await this.#save(job);
      const combined = [];
      for (const [index, segment] of segments.entries()) {
        if (controller.signal.aborted) throw new JobError('CANCELLED', 409);
        let recognized;
        for (let attempt = 0; attempt < 2; attempt++) {
          try {
            recognized = await this.transcribe(segment.path, controller.signal);
            break;
          } catch (error) {
            if (controller.signal.aborted || attempt === 1) throw error;
            await new Promise(resolve => setTimeout(resolve, 1500));
          }
        }
        if (!Array.isArray(recognized)) throw new JobError('INVALID_ASR_RESULT', 502);
        for (const item of recognized) {
          if (!Number.isFinite(item.startMs) || !Number.isFinite(item.endMs)) continue;
          const startMs = Math.max(0, Math.round(item.startMs + segment.startMs));
          const endMs = Math.max(startMs + 1, Math.round(item.endMs + segment.startMs));
          if (!item.text?.trim()) continue;
          combined.push({ text: item.text, startMs, endMs });
        }
        job.completedSegments = index + 1;
        job.progress = 10 + Math.floor(85 * (index + 1) / segments.length);
        await this.#save(job);
      }
      if (!combined.length) throw new JobError('NO_SPEECH_DETECTED', 422);
      combined.sort((a, b) => a.startMs - b.startMs);
      const srt = await this.toSrt(combined);
      await writeFile(this.#resultPath(job.id), srt, { mode: 0o600 });
      if (job.status === 'cancelled') return;
      job.status = 'completed'; job.stage = 'completed'; job.progress = 100;
      await this.#save(job);
    } catch (error) {
      if (job.status === 'cancelled') return;
      job.status = 'failed';
      job.stage = 'failed';
      job.errorCode = controller.signal.aborted ? 'JOB_CANCELLED_OR_TIMED_OUT' :
        error instanceof JobError ? error.code : 'TRANSCRIPTION_FAILED';
      console.error('[ToolNet-SRT] job failed', job.id, job.errorCode);
      await this.#save(job).catch(console.error);
    } finally {
      clearTimeout(timer);
      await rm(input, { force: true }).catch(() => {});
      await rm(segmentDir, { recursive: true, force: true }).catch(() => {});
    }
  }
}
