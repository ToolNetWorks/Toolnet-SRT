import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { writeFile, access } from 'node:fs/promises';
import { createDurableFetchWorker } from '../server/storage/durable-fetch-worker.mjs';
import { remoteSourceKey } from '../server/storage/source-keys.mjs';

function harness({ downloadError, leaseLost = false, uploadError = false, completeError = false, block = false } = {}) {
  const jobId = randomUUID(), token = randomUUID();
  const claim = { jobId, token, workerId: 'fetch-test', encryptedUrl: 'cipher', extension: '.mp4', attemptCount: 1 };
  const events = []; let path;
  const leases = {
    async reapExhausted() {}, async claimNext() { events.push('claim'); return claim; },
    async heartbeat() { events.push('heartbeat'); return !leaseLost; },
    async completeFetch(input) { events.push('complete'); assert.equal(input.sourceKey, remoteSourceKey(jobId, token, 'source.mp4')); if (completeError) throw Error('lost commit ack'); return true; },
    async failFetch(input) { events.push(input); return { updated: true, status: input.retryable ? 'pending' : 'failed' }; },
  };
  const storage = {
    async uploadRemoteSource({ localPath }) { events.push('upload'); if (uploadError) throw Error('secret url'); return { key: remoteSourceKey(jobId, token, 'source.mp4') }; },
    async getRemoteSourceInfo() { events.push('head'); return { key: remoteSourceKey(jobId, token, 'source.mp4'), size: 5 }; },
  };
  const worker = createDurableFetchWorker({ leases, storage, workerId: 'fetch-test', allowedHosts: ['media.example'], maxBytes: 1024,
    heartbeatIntervalMs: 10, vault: { decrypt(cipher, id) { assert.equal(cipher, 'cipher'); assert.equal(id, jobId); return 'https://media.example/a?token=secret'; } },
    async download(url, localPath, options) {
      assert.equal(url, 'https://media.example/a?token=secret'); assert.ok(options.signal); path = localPath;
      if (block) return new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(Error('aborted')), { once: true }));
      if (downloadError) throw Object.assign(Error('secret'), { code: downloadError });
      await writeFile(localPath, 'hello');
    },
  });
  return { worker, events, path: () => path };
}

test('Fetch downloads, verifies and commits token-scoped source, then removes temp file', async () => {
  const h = harness(); assert.equal((await h.worker.runOnce()).status, 'completed');
  assert.ok(h.events.indexOf('upload') < h.events.indexOf('head')); assert.ok(h.events.indexOf('head') < h.events.indexOf('complete'));
  await assert.rejects(access(h.path()), { code: 'ENOENT' });
});
test('Lost lease aborts before upload', async () => {
  const h = harness({ leaseLost: true }); assert.equal((await h.worker.runOnce()).status, 'lease_lost');
  assert.ok(!h.events.includes('upload')); assert.ok(!h.events.includes('complete'));
});
test('Permanent size error and transient R2 error are distinguished', async () => {
  for (const [options, expected, retryable] of [[{ downloadError: 'UPLOAD_TOO_LARGE' }, 'failed', false], [{ uploadError: true }, 'retried', true]]) {
    const h = harness(options); assert.equal((await h.worker.runOnce()).status, expected);
    assert.equal(h.events.find(x => typeof x === 'object').retryable, retryable);
    assert.ok(!JSON.stringify(h.events).includes('secret'));
  }
});
test('Unknown commit outcome is not retried or deleted', async () => {
  const h = harness({ completeError: true }); assert.equal((await h.worker.runOnce()).status, 'lease_lost');
  assert.ok(!h.events.some(x => typeof x === 'object'));
});
test('Shutdown aborts a download and cleans its directory', async () => {
  const h = harness({ block: true }); const controller = new AbortController();
  const task = h.worker.runOnce({ signal: controller.signal });
  while (!h.path()) await new Promise(resolve => setTimeout(resolve, 5));
  controller.abort(); assert.equal((await task).status, 'retried');
  await assert.rejects(access(h.path()), { code: 'ENOENT' });
});
test('Expired lease interrupts an in-flight download', async () => {
  const h = harness({ block: true, leaseLost: true });
  assert.equal((await h.worker.runOnce()).status, 'lease_lost');
  assert.ok(!h.events.includes('upload')); assert.ok(!h.events.includes('complete'));
  await assert.rejects(access(h.path()), { code: 'ENOENT' });
});
