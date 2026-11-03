import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createExtendedRoutes } from '../server/extended-routes.mjs';
import { createDurableJobManager } from '../server/storage/durable-job-manager.mjs';
import { JobError } from '../server/job-utils.mjs';
import { ToolNetSRTClient } from '../sdk/client.mjs';
import { createDurableUploadIngress } from '../server/storage/durable-ingress.mjs';

async function harness() {
  const jobs = new Map(), requests = new Map();
  const manager = createDurableJobManager({
    jobStore: { async getJob(id) { return jobs.get(id) ?? null; } },
    resultStorage: { async getResult() { return Buffer.from('1\n00:00:00,000 --> 00:00:01,000\nhello\n'); } },
    cancellation: { async cancelJob(id) { const job = jobs.get(id); if (!job || job.status !== 'queued') return null; job.status = 'cancelled'; job.stage = 'cancelled'; return job; } },
    ingress: { async createUpload(req, mime) { if (mime !== 'audio/mpeg') throw new JobError('UNSUPPORTED_MEDIA_TYPE', 415);
      let size = 0; for await (const chunk of req) size += chunk.length; if (size > 5) throw new JobError('UPLOAD_TOO_LARGE', 413);
      const job = makeJob(); jobs.set(job.id, job); return job; } },
    remoteQueue: { async enqueue(url, options) {
      assert.equal(options.scope, 'single-tenant');
      if (!/^[\x21-\x7e]{1,255}$/.test(options.idempotencyKey)) throw new JobError('INVALID_IDEMPOTENCY_KEY', 400);
      const previous = requests.get(options.idempotencyKey);
      if (previous) { if (previous.url !== url) throw new JobError('IDEMPOTENCY_KEY_CONFLICT', 409); return previous.job; }
      const job = makeJob(); job.stage = 'fetch_pending'; jobs.set(job.id, job); requests.set(options.idempotencyKey, { url, job }); return job;
    } },
  });
  function makeJob() { return { id: randomUUID(), status: 'queued', stage: 'queued', progress: 0,
    completedSegments: 0, totalSegments: null, errorCode: null, resultKey: null, resultPath: null,
    createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }; }
  const routes = createExtendedRoutes({ manager, authenticate: v => v === 'test-key', apiKeyPresent: () => true, durable: true });
  const server = createServer((req,res) => void routes.handle(req,res,new URL(req.url,'http://localhost').pathname));
  server.listen(0, '127.0.0.1'); await once(server,'listening');
  return { jobs, base: `http://127.0.0.1:${server.address().port}`, close: () => new Promise(r => server.close(r)) };
}
test('Durable HTTP and MCP share idempotency and preserve job/result contract', async () => {
  const h = await harness(); const headers = { 'X-API-Key':'test-key', 'Content-Type':'application/json', 'Idempotency-Key':'request-1' };
  try {
    assert.equal((await fetch(h.base+'/v1/jobs/url',{ method:'POST', headers:{ 'Content-Type':'application/json' },body:'{}' })).status,401);
    assert.equal((await fetch(h.base+'/mcp',{ method:'POST', headers:{ 'X-API-Key':'bad', 'Content-Type':'application/json', Accept:'application/json' }, body:'{}' })).status,401);
    for (const method of ['initialize', 'tools/list']) {
      const reply = await fetch(h.base+'/mcp',{ method:'POST', headers:{ 'X-API-Key':'test-key', 'Content-Type':'application/json', Accept:'application/json' },
        body: JSON.stringify({jsonrpc:'2.0',id:9,method,params:{protocolVersion:'2025-11-25'}}) });
      const data = await reply.json(); assert.equal(reply.status,200);
      if (method === 'tools/list') assert.deepEqual(data.result.tools.map(tool=>tool.name), [
        'submit_transcription_url','get_transcription_job','get_transcription_srt','cancel_transcription_job']);
      else assert.equal(data.result.serverInfo.name,'toolnet-srt');
    }
    const create = () => fetch(h.base+'/v1/jobs/url',{ method:'POST',headers,body:JSON.stringify({url:'https://media.example/a?token=private'}) });
    const first = await create(); assert.equal(first.status,202); const job = await first.json();
    assert.ok(!JSON.stringify(job).includes('private'));
    assert.equal((await (await create()).json()).id,job.id);
    assert.equal((await fetch(h.base+'/v1/jobs/url',{ method:'POST',headers,body:JSON.stringify({url:'https://media.example/b'}) })).status,409);
    assert.equal((await fetch(h.base+'/v1/jobs/url',{ method:'POST',headers:{'X-API-Key':'test-key','Content-Type':'application/json'},body:'{"url":"https://media.example/a"}' })).status,400);
    const rpc = async (id,name,args) => (await (await fetch(h.base+'/mcp',{method:'POST',headers:{'X-API-Key':'test-key','Content-Type':'application/json',Accept:'application/json'},body:JSON.stringify({jsonrpc:'2.0',id,method:'tools/call',params:{name,arguments:args}})})).json()).result;
    const retried = await rpc(1,'submit_transcription_url',{url:'https://media.example/a?token=private',idempotency_key:'request-1'});
    assert.equal(JSON.parse(retried.content[0].text).id,job.id);
    assert.equal(JSON.parse((await rpc(3,'get_transcription_job',{job_id:job.id})).content[0].text).id, job.id);
    assert.equal((await (await fetch(h.base+`/v1/jobs/${job.id}`,{headers:{'X-API-Key':'test-key'}})).json()).id,job.id);
    assert.equal((await fetch(h.base+`/v1/jobs/${job.id}/result`,{headers:{'X-API-Key':'test-key'}})).status,409);
    const stored = h.jobs.get(job.id); stored.status='completed'; stored.resultKey=`toolnet-srt/jobs/${job.id}/attempts/${randomUUID()}/result.srt`;
    const result = await fetch(h.base+`/v1/jobs/${job.id}/result`,{headers:{'X-API-Key':'test-key'}});
    assert.match(result.headers.get('content-type'),/application\/x-subrip/); assert.match(await result.text(),/hello/);
    assert.match((await rpc(2,'get_transcription_srt',{job_id:job.id})).content[0].text,/hello/);
    const upload = await fetch(h.base+'/v1/jobs',{method:'POST',headers:{'X-API-Key':'test-key','Content-Type':'audio/mpeg'},body:'123'});
    assert.equal(upload.status,202); const created = await upload.json();
    assert.equal(JSON.parse((await rpc(4,'cancel_transcription_job',{job_id:created.id})).content[0].text).status, 'cancelled');
    assert.equal((await fetch(h.base+`/v1/jobs/${created.id}`,{method:'DELETE',headers:{'X-API-Key':'test-key'}})).status,409);
    assert.equal((await fetch(h.base+'/v1/jobs',{method:'POST',headers:{'X-API-Key':'test-key','Content-Type':'audio/mpeg'},body:'123456'})).status,413);
    assert.equal((await fetch(h.base+'/v1/jobs',{method:'POST',headers:{'X-API-Key':'test-key','Content-Type':'text/plain'},body:'123'})).status,415);
    const client = new ToolNetSRTClient({baseUrl:h.base,apiKey:'test-key'});
    assert.equal((await client.createJobFromUrl('https://media.example/a?token=private',{idempotencyKey:'request-1'})).id,job.id);
  } finally { await h.close(); }
});
test('MCP stdio uses the same HTTP service and idempotency option', async () => {
  const h = await harness();
  try {
    const child = spawn(process.execPath,['mcp/stdio.mjs'],{env:{...process.env,TOOLNET_SRT_BASE_URL:h.base,TOOLNET_SRT_API_KEY:'test-key'},stdio:['pipe','pipe','pipe']});
    let output=''; child.stdout.on('data',c => output+=c); child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'submit_transcription_url',arguments:{url:'https://media.example/a',idempotency_key:'stdio-key'}}})+'\n'); child.stdin.end();
    const [code] = await once(child,'exit'); assert.equal(code,0); const response = JSON.parse(output);
    assert.equal(JSON.parse(response.result.content[0].text).status,'queued');
  } finally { await h.close(); }
});

test('Oversized chunked durable upload returns 413 without writing R2', async () => {
  let uploads = 0;
  const ingress = createDurableUploadIngress({ maxBytes: 5, jobStore: { async saveJob() {} },
    storage: { async uploadFile() { uploads++; } } });
  const routes = createExtendedRoutes({ manager: { createUpload: ingress.createUpload }, durable: true,
    authenticate: () => true, apiKeyPresent: () => true });
  const server = createServer((req,res) => void routes.handle(req,res,'/v1/jobs'));
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  try {
    const status = await new Promise((resolve,reject) => {
      const req = request({ host:'127.0.0.1', port:server.address().port, path:'/v1/jobs', method:'POST',
        headers:{ 'X-API-Key':'test', 'Content-Type':'audio/mpeg', 'Transfer-Encoding':'chunked' } }, res => { res.resume(); resolve(res.statusCode); });
      req.on('error',reject); req.write('1234'); req.write('5678'); req.end();
    });
    assert.equal(status,413); assert.equal(uploads,0);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
