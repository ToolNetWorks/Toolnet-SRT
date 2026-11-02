import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { JobManager } from '../server/jobs.mjs';
import { validateRemoteUrl } from '../server/job-utils.mjs';
import { createMcpProtocol } from '../mcp/protocol.mjs';
import { createExtendedRoutes } from '../server/extended-routes.mjs';
import { ToolNetSRTClient } from '../sdk/client.mjs';

function srtTimestamp(ms) {
  const m = Math.round(ms);
  const pad = (n, w = 2) => String(n).padStart(w, '0');
  return `${pad(Math.floor(m / 3600000))}:${pad(Math.floor(m / 60000) % 60)}:${pad(Math.floor(m / 1000) % 60)},${pad(m % 1000, 3)}`;
}
function toSrt(segments) {
  return segments.map((s, i) => `${i + 1}\n${srtTimestamp(s.startMs)} --> ${srtTimestamp(s.endMs)}\n${s.text}\n`).join('\n');
}
const fakeASR = async () => [{ text: '字幕测试', startMs: 100, endMs: 800 }];
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function waitUntil(get, predicate, timeout = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const result = await get();
    if (predicate(result)) return result;
    await sleep(100);
  }
  throw new Error('Timed out waiting for job');
}

async function fixture() {
  const dir = await mkdtemp(join(tmpdir(), 'toolnet-srt-test-'));
  const file = join(dir, 'sample.mp3');
  const result = spawnSync('ffmpeg', [
    '-hide_banner', '-loglevel', 'error', '-y', '-f', 'lavfi',
    '-i', 'sine=frequency=500:duration=7', '-c:a', 'libmp3lame', '-b:a', '32k', file,
  ]);
  if (result.status !== 0) throw new Error('FFmpeg not available for tests');
  const bytes = await readFile(file);
  return { dir, bytes, cleanup: () => rm(dir, { force: true, recursive: true }) };
}

function upload(bytes, mime = 'audio/mpeg') {
  const req = Readable.from([bytes]);
  req.headers = { 'content-type': mime, 'content-length': String(bytes.length) };
  return req;
}

test('long audio segmented, async queue, timestamp offsets, SRT result', async () => {
  const { dir, bytes, cleanup } = await fixture();
  try {
    const manager = new JobManager({ dataDir: join(dir, 'data'), segmentSeconds: 3,
      transcribe: fakeASR, toSrt, maxBytes: 10 * 1024 * 1024 });
    const created = await manager.createUpload(upload(bytes), 'audio/mpeg');
    assert.match(created.id, /^[0-9a-f-]{36}$/);
    const finished = await waitUntil(() => manager.getJob(created.id), j => j.status === 'completed' || j.status === 'failed');
    assert.equal(finished.status, 'completed', finished.errorCode);
    assert.ok(finished.totalSegments >= 2);
    assert.equal(finished.progress, 100);
    const srt = (await manager.getResult(created.id)).toString('utf8');
    assert.match(srt, /00:00:00,100 --> 00:00:00,800/);
    assert.match(srt, /00:00:03,\d{3} --> 00:00:03,\d{3}/);
    assert.match(srt, /字幕测试/);
  } finally { await cleanup(); }
});

test('job cancellation and local restart recovery', async () => {
  const { dir, bytes, cleanup } = await fixture();
  try {
    const dataDir = join(dir, 'data');
    const stopped = new JobManager({ dataDir, canRun: () => false, transcribe: fakeASR, toSrt });
    const queued = await stopped.createUpload(upload(bytes), 'audio/mpeg');
    assert.equal((await stopped.getJob(queued.id)).status, 'queued');
    const cancelled = await stopped.cancelJob(queued.id);
    assert.equal(cancelled.status, 'cancelled');
    const second = await stopped.createUpload(upload(bytes), 'audio/mpeg');
    const recovered = new JobManager({ dataDir, segmentSeconds: 3, transcribe: fakeASR, toSrt });
    const done = await waitUntil(() => recovered.getJob(second.id), j => j.status === 'completed' || j.status === 'failed');
    assert.equal(done.status, 'completed', done.errorCode);
    assert.equal((await recovered.getJob(queued.id)).status, 'cancelled');
  } finally { await cleanup(); }
});

test('remote source blocks non-HTTPS / localhost and default disables remote', async () => {
  await assert.rejects(validateRemoteUrl('http://media.example.com/a.mp4', ['media.example.com']), /INVALID_SOURCE_URL/);
  await assert.rejects(validateRemoteUrl('https://127.0.0.1/a.mp4', ['127.0.0.1']), /SOURCE_HOST_NOT_ALLOWED/);
  await assert.rejects(validateRemoteUrl('https://example.com/a.mp4', []), /SOURCE_HOST_NOT_ALLOWED/);
  const dir = await mkdtemp(join(tmpdir(), 'toolnet-url-test-'));
  try {
    const manager = new JobManager({ dataDir: dir, allowedHosts: [] });
    await assert.rejects(manager.createRemote('https://media.example.com/a.mp4'), /REMOTE_URL_DISABLED/);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test('MCP initialize/list/call + HTTP docs API and client auth', async () => {
  const { dir, bytes, cleanup } = await fixture();
  const key = 'unit-test-only-secret';
  const manager = new JobManager({ dataDir: join(dir, 'data'), segmentSeconds: 3,
    transcribe: fakeASR, toSrt });
  const routes = createExtendedRoutes({ manager,
    authenticate: value => value === key,
    apiKeyPresent: () => true });
  const http = createServer((req, res) => {
    const pathname = new URL(req.url, 'http://localhost').pathname;
    if (routes.isExtended(pathname)) void routes.handle(req, res, pathname);
    else { res.writeHead(404); res.end(); }
  });
  http.listen(0, '127.0.0.1');
  await once(http, 'listening');
  const baseUrl = `http://127.0.0.1:${http.address().port}`;
  try {
    const docRes = await fetch(baseUrl + '/docs');
    assert.equal(docRes.status, 200);
    assert.match(await docRes.text(), /ToolNet-SRT API/);
    assert.equal((await (await fetch(baseUrl + '/openapi.json')).json()).openapi, '3.1.0');
    assert.equal((await fetch(baseUrl + '/v1/jobs')).status, 401);
    const client = new ToolNetSRTClient({ baseUrl, apiKey: key });
    const created = await client.createJobFromBytes(bytes, 'audio/mpeg');
    await client.waitForJob(created.id, { intervalMs: 100 });
    assert.match(await client.getResult(created.id), /字幕测试/);
    const req = (id, method, params) => ({ jsonrpc: '2.0', id, method, params });
    const call = async (body) => {
      const res = await fetch(baseUrl + '/mcp', { method: 'POST', headers: {
        'X-API-Key': key, Accept: 'application/json, text/event-stream', 'Content-Type': 'application/json',
      }, body: JSON.stringify(body) });
      return { status: res.status, body: await res.json() };
    };
    const init = await call(req(1, 'initialize', { protocolVersion: '2025-11-25' }));
    assert.equal(init.status, 200);
    assert.equal(init.body.result.serverInfo.name, 'toolnet-srt');
    const list = await call(req(2, 'tools/list', {}));
    assert.equal(list.body.result.tools.length, 4);
    const result = await call(req(3, 'tools/call', { name: 'get_transcription_srt', arguments: { job_id: created.id } }));
    assert.match(result.body.result.content[0].text, /字幕测试/);
    const forbidden = await fetch(baseUrl + '/mcp', { method: 'POST', headers: {
      'X-API-Key': key, Origin: 'https://evil.example', Accept: 'application/json', 'Content-Type': 'application/json',
    }, body: JSON.stringify(req(4, 'ping')) });
    assert.equal(forbidden.status, 403);
    const protocol = createMcpProtocol({ getJob: id => client.getJob(id) });
    assert.equal((await protocol(req(5, 'ping'))).result.constructor, Object);
  } finally { http.close(); await cleanup(); }
});
