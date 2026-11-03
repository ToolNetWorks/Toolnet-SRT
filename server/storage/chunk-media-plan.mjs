import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { probeChunkMedia } from './chunk-media-probe.mjs';

// Stream source frame timestamps rather than estimating boundaries from earlier chunks.
export async function planChunkMedia(input, { chunkSeconds = 600, overlapSeconds = 0, signal, timeoutMs = 1800000,
  probe = probeChunkMedia, spawnFn = spawn } = {}) {
  if (!Number.isSafeInteger(chunkSeconds) || chunkSeconds < 1 || chunkSeconds > 900 ||
      !Number.isSafeInteger(overlapSeconds) || overlapSeconds < 0 || overlapSeconds >= chunkSeconds) throw new Error('INVALID_CHUNK_CONFIG');
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1000 || timeoutMs > 7200000) throw new Error('INVALID_CHUNK_PROBE_TIMEOUT');
  const metadata = await probe(input, { signal, timeoutMs: Math.min(timeoutMs,300000) });
  const step = (chunkSeconds-overlapSeconds)*1000;
  const targets = [];
  const firstAudioOffset = Math.max(0, metadata.audioStartMs - metadata.formatStartMs);
  for (let time = firstAudioOffset; time < metadata.durationMs; time += step) {
    if (targets.length >= 1000) throw new Error('TOO_MANY_CHUNKS');
    targets.push(time);
  }
  const frameData = await new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new Error('MEDIA_PROBE_ABORTED'));
    const child = spawnFn('ffprobe',['-v','error','-select_streams','a:0','-show_frames',
      '-show_entries','frame=best_effort_timestamp_time,pkt_duration_time','-of','csv=p=0',input],{stdio:['ignore','pipe','ignore']});
    const lines = createInterface({ input:child.stdout, crlfDelay:Infinity });
    const found = []; let next = 0, last = -Infinity, lastEnd = null, settled = false, timedOut = false;
    const timer = setTimeout(() => { timedOut = true; child.kill('SIGKILL'); },timeoutMs);
    const abort = () => child.kill('SIGKILL');
    signal?.addEventListener('abort',abort,{once:true});
    const finish = (error, value) => { if (settled) return; settled=true; clearTimeout(timer); signal?.removeEventListener('abort',abort);
      lines.close(); error ? reject(error) : resolve(value); };
    lines.on('line', line => {
      const [value, duration] = line.trim().split(',');
      if (!value) return;
      if (value === 'N/A') { child.kill('SIGKILL'); finish(new Error('INVALID_MEDIA_FRAME_TIMESTAMP')); return; }
      const pts = Number(value) * 1000;
      if (!Number.isFinite(pts)) { child.kill('SIGKILL'); finish(new Error('INVALID_MEDIA_FRAME_TIMESTAMP')); return; }
      if (pts + 1 < last) { child.kill('SIGKILL'); finish(new Error('NON_MONOTONIC_AUDIO_TIMELINE')); return; }
      last = pts;
      const frameDuration = Number(duration);
      lastEnd = Number.isFinite(frameDuration) && frameDuration > 0 ? pts + frameDuration * 1000 : null;
      const offset = Math.round(pts - metadata.formatStartMs);
      while (next < targets.length && offset >= targets[next]) {
        if (offset >= 0 && offset < metadata.durationMs && found.at(-1) !== offset) found.push(offset);
        next++;
      }
    });
    child.once('error', () => finish(new Error('MEDIA_PROBE_FAILED')));
    child.once('close',code => {
      if (signal?.aborted) return finish(new Error('MEDIA_PROBE_ABORTED'));
      if (timedOut) return finish(new Error('MEDIA_PROBE_TIMEOUT'));
      if (code !== 0) return finish(new Error('MEDIA_PROBE_FAILED'));
      if (!found.length) return finish(new Error('NO_AUDIO_FRAMES'));
      finish(null,{ offsets: found, lastEnd });
    });
    if (signal?.aborted) abort();
  });
  const { offsets, lastEnd } = frameData;
  const audioEnd = lastEnd === null
    ? (metadata.audioDurationMs === null ? null : metadata.audioStartMs + metadata.audioDurationMs - metadata.formatStartMs)
    : Math.round(lastEnd - metadata.formatStartMs);
  if (!Number.isSafeInteger(audioEnd) || audioEnd <= offsets.at(-1)) throw new Error('INVALID_AUDIO_END_TIMESTAMP');
  while (offsets.length > 1 && audioEnd - offsets.at(-1) <= overlapSeconds * 1000) offsets.pop();
  const chunks = [];
  for (const [index, startOffsetMs] of offsets.entries()) {
    const end = index + 1 < offsets.length
      ? Math.min(metadata.durationMs, audioEnd, offsets[index + 1] + overlapSeconds * 1000)
      : Math.min(metadata.durationMs, audioEnd);
    const durationMs = end - startOffsetMs;
    const previousEnd = index ? chunks[index-1].startOffsetMs + chunks[index-1].durationMs : startOffsetMs;
    chunks.push({ index, startOffsetMs, durationMs, overlapMs: Math.max(0, Math.min(durationMs-1,previousEnd-startOffsetMs)) });
  }
  return { ...metadata, chunks };
}
