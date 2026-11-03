import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import { readFile, mkdtemp, rm, access } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { downloadPinnedHttps, isPublicIp, validateRemoteSource } from '../server/storage/pinned-https.mjs';

const signed = 'https://media.example/audio.mp3?token=secret123';
const allowedHosts = ['media.example'];
const records = [{ address: '1.1.1.1', family: 4 }];
async function withPath(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'srt-pinned-test-'));
  try { return await fn(join(dir, 'source.mp4')); }
  finally { await rm(dir, { recursive: true, force: true }); }
}
function fakeRequest({ status = 200, body = 'hello', length, onDial = () => {} } = {}) {
  return (url, options, onResponse) => {
    const request = new EventEmitter();
    request.end = () => {
      options.lookup(url.hostname, { all: true }, (error, addresses) => {
        if (error) { request.emit('error', error); return; }
        onDial({ options, addresses });
        const response = Readable.from([Buffer.from(body)]);
        response.statusCode = status;
        response.headers = length === undefined ? {} : { 'content-length': String(length) };
        onResponse(response);
      });
    };
    return request;
  };
}

test('Reject private and reserved IP ranges', () => {
  const blocked = ['127.0.0.1','10.1.1.2','169.254.169.254','100.64.0.1','192.168.0.1','198.18.1.1','::1','::ffff:127.0.0.1','fc00::1','fe80::1','64:ff9b::a9fe:a9fe','2002:c0a8:101::1','2001:db8::1'];
  for (const address of blocked) assert.equal(isPublicIp(address), false, address);
  for (const address of ['1.1.1.1','8.8.8.8','2606:4700:4700::1111']) assert.equal(isPublicIp(address), true, address);
  console.log('PRIVATE IP PROTECTION: PASS');
});
test('Only exact HTTPS host is permitted', () => {
  assert.equal(validateRemoteSource(signed, allowedHosts).hostname, 'media.example');
  for (const url of ['http://media.example/file','https://user:pass@media.example/file','https://media.example.evil/file','https://127.0.0.1/file','https://media.example:8443/file']) {
    assert.throws(() => validateRemoteSource(url, allowedHosts), { name: 'JobError' });
  }
  console.log('HTTPS HOST ALLOWLIST: PASS');
});
test('Public-host mode accepts different domains but rejects local hostnames and literals', () => {
  for (const host of ['storage.googleapis.com', 'www.google.com', 'media.example']) {
    assert.equal(validateRemoteSource(`https://${host}/audio`, null).hostname, host);
  }
  for (const url of ['https://localhost/a','https://localhost./a','https://a.localhost/a',
    'https://printer.local/a','https://metadata/a','https://127.0.0.1/a','https://[::1]/a',
    'https://169.254.169.254/a','https://0x7f000001/a','https://user@public.example/a']) {
    assert.throws(() => validateRemoteSource(url, null), { code: 'INVALID_SOURCE_URL' });
  }
  for (const ip of ['fec0::1','4000::1','::ffff:8.8.8.8']) assert.equal(isPublicIp(ip), false);
});
test('Public-host mode pins DNS and rejects any private rebinding answer', async () => {
  await withPath(async path => {
    let connected = false;
    await assert.rejects(downloadPinnedHttps('https://arbitrary.example/audio', path, {
      allowedHosts: null, maxBytes: 100,
      lookupAll: async () => [{ address:'1.1.1.1',family:4 },{ address:'169.254.169.254',family:4 }],
      requestFn: () => { connected = true; },
    }), { code:'SOURCE_ADDRESS_NOT_PUBLIC', status:403 });
    assert.equal(connected, false);
    let dial;
    await downloadPinnedHttps('https://another.example/audio', path, {
      allowedHosts: null, maxBytes:100, lookupAll:async () => records,
      requestFn:fakeRequest({ onDial: value => { dial=value; } }),
    });
    assert.equal(dial.options.servername, 'another.example');
    assert.equal(dial.options.rejectUnauthorized, true);
    assert.deepEqual(dial.addresses, records);
  });
});
test('HTTPS dials validated pinned address', async () => {
  await withPath(async path => {
    let dial;
    const result = await downloadPinnedHttps(signed, path, { allowedHosts, maxBytes: 100, lookupAll: async () => records, requestFn: fakeRequest({ onDial: value => { dial = value; } }) });
    assert.equal(result.bytes, 5); assert.equal(await readFile(path, 'utf8'), 'hello');
    assert.equal(dial.addresses[0].address, '1.1.1.1'); assert.equal(dial.options.servername, 'media.example');
    assert.equal(dial.options.rejectUnauthorized, true); assert.equal(dial.options.agent, false);
  });
  console.log('PINNED HTTPS CONNECTION: PASS');
});
test('Mixed public and private DNS is rejected', async () => {
  await withPath(async path => {
    let connected = false;
    await assert.rejects(downloadPinnedHttps(signed, path, { allowedHosts, maxBytes: 100,
      lookupAll: async () => [...records, { address: '127.0.0.1', family: 4 }],
      requestFn: () => { connected = true; throw new Error('UNEXPECTED_CONNECTION'); } }),
      { code: 'SOURCE_ADDRESS_NOT_PUBLIC', status: 403 });
    assert.equal(connected, false);
  });
  console.log('DNS REBINDING GUARD: PASS');
});
test('Empty DNS results are rejected', async () => {
  await withPath(async path => {
    await assert.rejects(downloadPinnedHttps(signed, path, { allowedHosts, maxBytes: 100, lookupAll: async () => [],
      requestFn: () => { throw new Error('UNEXPECTED_CONNECTION'); } }), { code: 'SOURCE_DNS_FAILED' });
  });
  console.log('EMPTY DNS REJECTED: PASS');
});
test('Redirect responses are blocked', async () => {
  await withPath(async path => {
    await assert.rejects(downloadPinnedHttps(signed, path, { allowedHosts, maxBytes: 100, lookupAll: async () => records, requestFn: fakeRequest({ status: 302 }) }), { code: 'SOURCE_REDIRECT_BLOCKED' });
    await assert.rejects(access(path), { code: 'ENOENT' });
  });
  console.log('REDIRECT BLOCKED: PASS');
});
test('Declared oversized file is rejected', async () => {
  await withPath(async path => {
    await assert.rejects(downloadPinnedHttps(signed, path, { allowedHosts, maxBytes: 10, lookupAll: async () => records, requestFn: fakeRequest({ length: 100 }) }), { code: 'UPLOAD_TOO_LARGE' });
    await assert.rejects(access(path), { code: 'ENOENT' });
  });
  console.log('CONTENT LENGTH LIMIT: PASS');
});
test('Stream size is enforced and cleaned', async () => {
  await withPath(async path => {
    await assert.rejects(downloadPinnedHttps(signed, path, { allowedHosts, maxBytes: 10, lookupAll: async () => records, requestFn: fakeRequest({ body: 'x'.repeat(100) }) }), { code: 'UPLOAD_TOO_LARGE' });
    await assert.rejects(access(path), { code: 'ENOENT' });
  });
  console.log('STREAM SIZE LIMIT: PASS');
});
test('Abort during DNS prevents connection', async () => {
  await withPath(async path => {
    const controller = new AbortController(); let connected = false;
    const operation = downloadPinnedHttps(signed, path, { allowedHosts, maxBytes: 10, signal: controller.signal,
      lookupAll: async () => new Promise(resolve => setTimeout(() => resolve(records), 50)),
      requestFn: () => { connected = true; throw new Error('UNEXPECTED_CONNECTION'); } });
    controller.abort();
    await assert.rejects(operation, { code: 'SOURCE_DOWNLOAD_TIMEOUT' });
    assert.equal(connected, false);
  });
  console.log('DNS ABORT PROTECTION: PASS');
});
