import { spawn } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { readFile, stat } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { Transform, Readable } from 'node:stream';
import { resolve as resolveDns } from 'node:dns/promises';
import { isIP } from 'node:net';
import { join } from 'node:path';

export const MIME_EXTENSIONS = new Map([
  ['video/mp4', '.mp4'], ['video/x-matroska', '.mkv'],
  ['video/webm', '.webm'], ['video/quicktime', '.mov'],
  ['audio/mpeg', '.mp3'], ['audio/wav', '.wav'],
  ['audio/x-wav', '.wav'], ['audio/mp4', '.m4a'],
  ['audio/flac', '.flac'], ['audio/x-flac', '.flac'],
  ['audio/ogg', '.ogg'], ['audio/webm', '.webm'],
]);

export class JobError extends Error {
  constructor(code, status = 400) {
    super(code);
    this.name = 'JobError';
    this.code = code;
    this.status = status;
  }
}

export function parsePositiveInt(value, fallback, max = 1000000) {
  if (value == null || value === '') return fallback;
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 1 || number > max) {
    throw new Error('INVALID_NUMERIC_CONFIGURATION');
  }
  return number;
}

export async function streamLimited(source, destination, maxBytes, signal) {
  let received = 0;
  const limiter = new Transform({
    transform(chunk, _encoding, callback) {
      received += chunk.length;
      if (received > maxBytes) return callback(new JobError('UPLOAD_TOO_LARGE', 413));
      callback(null, chunk);
    },
  });
  await pipeline(source, limiter, createWriteStream(destination, { mode: 0o600 }), { signal });
  if (received === 0) throw new JobError('EMPTY_UPLOAD');
  return received;
}

function isPrivateAddress(address) {
  const ip = isIP(address);
  if (ip === 4) {
    const [a, b] = address.split('.').map(Number);
    return a === 0 || a === 10 || a === 127 || a >= 224 ||
      (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127) ||
      (a === 192 && b === 0) || (a === 198 && [18, 19].includes(b));
  }
  if (ip === 6) {
    const x = address.toLowerCase();
    return x === '::' || x === '::1' || x.startsWith('fc') ||
      x.startsWith('fd') || x.startsWith('fe8') || x.startsWith('fe9') ||
      x.startsWith('fea') || x.startsWith('feb') || x.startsWith('::ffff:');
  }
  return true;
}

export async function validateRemoteUrl(raw, allowedHosts) {
  let url;
  try { url = new URL(raw); } catch { throw new JobError('INVALID_SOURCE_URL'); }
  if (url.protocol !== 'https:' || url.username || url.password || url.port && url.port !== '443') {
    throw new JobError('INVALID_SOURCE_URL');
  }
  if (!allowedHosts.includes(url.hostname.toLowerCase())) {
    throw new JobError('SOURCE_HOST_NOT_ALLOWED', 403);
  }
  if (isIP(url.hostname)) throw new JobError('SOURCE_HOST_NOT_ALLOWED', 403);
  let addresses;
  try { addresses = await resolveDns(url.hostname); }
  catch { throw new JobError('SOURCE_DNS_FAILED', 400); }
  if (!addresses.length || addresses.some(isPrivateAddress)) {
    throw new JobError('SOURCE_ADDRESS_NOT_PUBLIC', 403);
  }
  return url;
}

export async function downloadAllowed(raw, destination, { allowedHosts, maxBytes, signal }) {
  // Permit only explicitly approved HTTPS origins (e.g. a dedicated R2 custom domain).
  // Redirects are disabled to avoid leaving the allowlist.
  await validateRemoteUrl(raw, allowedHosts);
  const response = await fetch(raw, {
    method: 'GET', redirect: 'error', signal,
    headers: { 'User-Agent': 'ToolNet-SRT/0.2' },
  });
  if (!response.ok || !response.body) throw new JobError('SOURCE_DOWNLOAD_FAILED', 502);
  const contentLength = response.headers.get('content-length');
  if (contentLength && Number(contentLength) > maxBytes) {
    await response.body.cancel().catch(() => {});
    throw new JobError('UPLOAD_TOO_LARGE', 413);
  }
  await streamLimited(Readable.fromWeb(response.body), destination, maxBytes, signal);
}

export async function runFfmpeg(args, signal, timeoutMs) {
  await new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new JobError('CANCELLED', 409));
    const process = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    let settled = false;
    const timer = setTimeout(() => process.kill('SIGKILL'), timeoutMs);
    const onAbort = () => process.kill('SIGTERM');
    signal?.addEventListener('abort', onAbort, { once: true });
    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      error ? reject(error) : resolve();
    };
    process.stderr.on('data', chunk => {
      stderr = (stderr + chunk.toString()).slice(-600);
    });
    process.on('error', () => finish(new JobError('FFMPEG_UNAVAILABLE', 502)));
    process.on('close', code => {
      if (signal?.aborted) return finish(new JobError('CANCELLED', 409));
      if (code !== 0) return finish(new JobError(`FFMPEG_FAILED_${code}`, 502));
      finish();
    });
  });
}

export async function splitMedia(input, directory, { segmentSeconds, signal, timeoutMs }) {
  const csvFile = join(directory, 'segments.csv');
  await runFfmpeg([
    '-hide_banner', '-nostdin', '-loglevel', 'error', '-y',
    '-i', input, '-map', '0:a:0', '-vn', '-ac', '1', '-ar', '16000',
    '-c:a', 'libmp3lame', '-b:a', '32k',
    '-f', 'segment', '-segment_time', String(segmentSeconds),
    '-segment_list', csvFile, '-segment_list_type', 'csv',
    '-reset_timestamps', '1', join(directory, 'chunk_%05d.mp3'),
  ], signal, timeoutMs);

  const csv = await readFile(csvFile, 'utf8');
  const segments = [];
  for (const line of csv.split(/\r?\n/)) {
    if (!line.trim()) continue;
    const parts = line.replaceAll('"', '').split(',');
    if (parts.length !== 3 || !/^chunk_\d{5}\.mp3$/.test(parts[0])) {
      throw new JobError('INVALID_SEGMENT_LIST', 502);
    }
    const startMs = Math.max(0, Math.round(Number(parts[1]) * 1000));
    const endMs = Math.round(Number(parts[2]) * 1000);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
      throw new JobError('INVALID_SEGMENT_TIMESTAMPS', 502);
    }
    segments.push({ path: join(directory, parts[0]), startMs, endMs });
  }
  if (!segments.length) throw new JobError('NO_AUDIO_TRACK', 422);
  return segments;
}
