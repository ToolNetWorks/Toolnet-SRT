import { runFfmpeg } from '../job-utils.mjs';
import { stat } from 'node:fs/promises';

// frame offset is relative to the container start; convert back to original PTS
// before trimming. WAV avoids encoder-delay ambiguity in ASR timestamps.
export async function extractTimelineChunk(input, output, { formatStartMs, chunk, signal, timeoutMs = 1800000, seekPrerollMs = null } = {}) {
  if (typeof input !== 'string' || !input || typeof output !== 'string' || !output ||
      !Number.isSafeInteger(formatStartMs) || !Number.isSafeInteger(chunk?.startOffsetMs) || chunk.startOffsetMs < 0 ||
      !Number.isSafeInteger(chunk.durationMs) || chunk.durationMs < 1) throw new Error('INVALID_CHUNK_EXTRACTION');
  if (seekPrerollMs !== null && (!Number.isSafeInteger(seekPrerollMs) || seekPrerollMs < 1000 || seekPrerollMs > 60000)) throw new Error('INVALID_SEEK_PREROLL');
  const start = (formatStartMs + chunk.startOffsetMs) / 1000;
  const end = start + chunk.durationMs / 1000;
  // Optional experimental input seeking; the final trim still uses the
  // original timeline. Leave it off for formats whose seeking is unverified.
  const seek = seekPrerollMs === null ? 0 : Math.max(0, chunk.startOffsetMs - seekPrerollMs);
  await runFfmpeg(['-hide_banner','-nostdin','-loglevel','error','-y','-copyts',
    ...(seek ? ['-ss', String(seek / 1000)] : []), '-i',input,'-map','0:a:0','-vn',
    '-af',`atrim=start=${start}:end=${end},asetpts=PTS-STARTPTS`,
    '-ac','1','-ar','16000','-c:a','pcm_s16le',output],signal,timeoutMs);
  if ((await stat(output)).size <= 78) throw new Error('EMPTY_AUDIO_CHUNK');
}
