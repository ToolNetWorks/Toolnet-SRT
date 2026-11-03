import { spawn } from 'node:child_process';

// Keep original timestamps as reported by FFprobe; do not derive offsets by
// accumulating chunk durations. Frame-level chunk boundary mapping is separate.
export async function probeChunkMedia(input, { signal, timeoutMs = 30000, spawnFn = spawn } = {}) {
  if (typeof input !== 'string' || !input || input.includes('\0')) throw new Error('INVALID_MEDIA_INPUT');
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1000 || timeoutMs > 300000) throw new Error('INVALID_PROBE_TIMEOUT');
  const output = await new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new Error('MEDIA_PROBE_ABORTED'));
    const child = spawnFn('ffprobe', ['-v','error','-select_streams','a:0','-show_entries',
      'format=format_name,start_time,duration:stream=start_time,duration','-of','json',input], { stdio:['ignore','pipe','ignore'] });
    let data = '', settled = false, timedOut = false;
    const timer = setTimeout(() => { timedOut = true; child.kill('SIGKILL'); }, timeoutMs);
    const abort = () => child.kill('SIGKILL');
    signal?.addEventListener('abort', abort, { once:true });
    const finish = (error, value) => {
      if (settled) return; settled = true; clearTimeout(timer); signal?.removeEventListener('abort', abort);
      error ? reject(error) : resolve(value);
    };
    child.stdout.on('data', chunk => {
      data += chunk.toString('utf8');
      if (data.length > 1024 * 1024) { child.kill('SIGKILL'); finish(new Error('MEDIA_PROBE_OUTPUT_TOO_LARGE')); }
    });
    child.once('error', () => finish(new Error('MEDIA_PROBE_FAILED')));
    child.once('close', code => {
      if (signal?.aborted) return finish(new Error('MEDIA_PROBE_ABORTED'));
      if (timedOut) return finish(new Error('MEDIA_PROBE_TIMEOUT'));
      if (code !== 0) return finish(new Error('MEDIA_PROBE_FAILED'));
      finish(null, data);
    });
    if (signal?.aborted) abort();
  });
  let data;
  try { data = JSON.parse(output); } catch { throw new Error('INVALID_MEDIA_PROBE'); }
  const isWav = data?.format?.format_name === 'wav';
  const formatStart = data?.format?.start_time == null && isWav ? 0 : Number(data?.format?.start_time);
  const duration = Number(data?.format?.duration);
  const audioStart = data?.streams?.[0]?.start_time == null && isWav ? 0 : Number(data?.streams?.[0]?.start_time);
  const audioDuration = data?.streams?.[0]?.duration == null ? null : Number(data.streams[0].duration);
  if (!Number.isFinite(formatStart) || !Number.isFinite(duration) || duration <= 0 ||
      !Number.isFinite(audioStart) || (audioDuration !== null && (!Number.isFinite(audioDuration) || audioDuration <= 0))) throw new Error('INVALID_MEDIA_TIMELINE');
  return { formatStartMs: Math.round(formatStart * 1000), durationMs: Math.round(duration * 1000),
    audioStartMs: Math.round(audioStart * 1000), audioDurationMs: audioDuration === null ? null : Math.round(audioDuration * 1000) };
}
