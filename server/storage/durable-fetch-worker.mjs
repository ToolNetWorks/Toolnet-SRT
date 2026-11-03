import { mkdtemp, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { downloadPinnedHttps, validateRemoteSource } from './pinned-https.mjs';
import { remoteSourceKey } from './source-keys.mjs';
import { createFetchDiskQuota } from './fetch-disk-quota.mjs';

const permanent = new Set(['INVALID_SOURCE_URL', 'SOURCE_HOST_NOT_ALLOWED', 'SOURCE_ADDRESS_NOT_PUBLIC',
  'SOURCE_REDIRECT_BLOCKED', 'SOURCE_HTTP_CLIENT_ERROR', 'UPLOAD_TOO_LARGE', 'EMPTY_UPLOAD',
  'REMOTE_URL_DECRYPT_FAILED', 'INVALID_STORAGE_FILENAME']);

export function createDurableFetchWorker({ leases, vault, storage, workerId, allowedHosts = [],
  maxBytes = 256 * 1024 * 1024, downloadTimeoutMs = 120000, heartbeatIntervalMs = 20000,
  download = downloadPinnedHttps, tempRoot = tmpdir(), diskQuota = createFetchDiskQuota({ maxBytes }) } = {}) {
  if (!['claimNext', 'heartbeat', 'completeFetch', 'failFetch', 'reapExhausted'].every(x => typeof leases?.[x] === 'function')) throw new Error('REMOTE_LEASES_REQUIRED');
  if (typeof vault?.decrypt !== 'function') throw new Error('REMOTE_URL_VAULT_REQUIRED');
  if (typeof storage?.uploadRemoteSource !== 'function' || typeof storage?.getRemoteSourceInfo !== 'function') throw new Error('REMOTE_STORAGE_REQUIRED');
  if (typeof workerId !== 'string' || !/^[a-zA-Z0-9_.:-]{1,128}$/.test(workerId)) throw new Error('INVALID_WORKER_ID');
  if (allowedHosts !== null && (!Array.isArray(allowedHosts) || allowedHosts.some(x => typeof x !== 'string'))) throw new Error('INVALID_ALLOWED_HOSTS');
  for (const [value, min, max, name] of [[maxBytes, 1, 2147483648, 'MAX_BYTES'], [downloadTimeoutMs, 1000, 300000, 'DOWNLOAD_TIMEOUT'], [heartbeatIntervalMs, 10, 300000, 'HEARTBEAT']]) {
    if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`INVALID_${name}`);
  }
  if (typeof download !== 'function') throw new Error('INVALID_DOWNLOAD');
  if (typeof diskQuota?.reserve !== 'function') throw new Error('INVALID_FETCH_DISK_QUOTA');

  async function runOnce({ signal, onClaim } = {}) {
    if (signal?.aborted) return { status: 'idle' };
    await leases.reapExhausted();
    if (signal?.aborted) return { status: 'idle' };
    const claim = await leases.claimNext(workerId);
    if (!claim) return { status: 'idle' };
    onClaim?.();
    const controller = new AbortController();
    const abort = () => controller.abort();
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    let directory, timer, releaseQuota, inFlight = Promise.resolve(), leaseLost = false, committing = false;
    const ensure = () => { if (controller.signal.aborted) throw new Error(leaseLost ? 'LEASE_LOST' : 'FETCH_ABORTED'); };
    const heartbeat = async () => {
      try { if (!await leases.heartbeat(claim)) { leaseLost = true; controller.abort(); } }
      catch { leaseLost = true; controller.abort(); }
    };
    const interval = setInterval(() => { inFlight = inFlight.then(heartbeat); }, heartbeatIntervalMs);
    try {
      ensure();
      const url = vault.decrypt(claim.encryptedUrl, claim.jobId);
      validateRemoteSource(url, allowedHosts);
      const filename = `source${claim.extension}`;
      const key = remoteSourceKey(claim.jobId, claim.token, filename);
      directory = await mkdtemp(join(tempRoot, 'toolnet-srt-fetch-'));
      releaseQuota = await diskQuota.reserve(maxBytes, directory);
      const localPath = join(directory, filename);
      timer = setTimeout(() => controller.abort(), downloadTimeoutMs);
      timer.unref?.();
      await download(url, localPath, { allowedHosts, maxBytes, signal: controller.signal });
      clearTimeout(timer); timer = null;
      ensure();
      const file = await stat(localPath);
      if (!file.isFile() || !file.size) throw new Error('EMPTY_UPLOAD');
      if (file.size > maxBytes) throw new Error('UPLOAD_TOO_LARGE');
      await heartbeat(); ensure();
      const uploaded = await storage.uploadRemoteSource({ jobId: claim.jobId, fetchToken: claim.token, filename, localPath, signal: controller.signal });
      ensure();
      if (uploaded?.key !== key) throw new Error('R2_UPLOAD_FAILED');
      const info = await storage.getRemoteSourceInfo({ jobId: claim.jobId, fetchToken: claim.token, filename, signal: controller.signal });
      ensure();
      if (!info || info.key !== key || info.size !== file.size) throw new Error('R2_VERIFICATION_FAILED');
      await heartbeat(); ensure();
      committing = true;
      if (!await leases.completeFetch({ ...claim, sourceKey: key })) return { status: 'lease_lost', claimed: true };
      return { status: 'completed', claimed: true };
    } catch (error) {
      if (committing || leaseLost) return { status: 'lease_lost', claimed: true };
      const code = typeof error?.code === 'string' && /^[A-Z][A-Z0-9_]{0,79}$/.test(error.code) ? error.code
        : typeof error?.message === 'string' && /^[A-Z][A-Z0-9_]{0,79}$/.test(error.message) ? error.message : 'SOURCE_DOWNLOAD_FAILED';
      const safeCode = permanent.has(code) ? code : controller.signal.aborted ? 'SOURCE_DOWNLOAD_TIMEOUT' :
        ['SOURCE_HTTP_STATUS', 'SOURCE_DNS_FAILED', 'INSUFFICIENT_DISK_SPACE', 'FETCH_DISK_QUOTA_EXCEEDED', 'R2_UPLOAD_FAILED', 'R2_VERIFICATION_FAILED'].includes(code) ? code : 'SOURCE_DOWNLOAD_FAILED';
      const result = await leases.failFetch({ ...claim, errorCode: safeCode, retryable: !permanent.has(safeCode) });
      return { status: result.updated ? (result.status === 'pending' ? 'retried' : 'failed') : 'lease_lost', claimed: true };
    } finally {
      clearTimeout(timer); clearInterval(interval);
      await inFlight;
      signal?.removeEventListener('abort', abort);
      if (directory) await rm(directory, { recursive: true, force: true });
      releaseQuota?.();
    }
  }
  return { runOnce };
}
