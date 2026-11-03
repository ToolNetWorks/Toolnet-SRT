import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { once } from 'node:events';
const wait = ms => new Promise(r => setTimeout(r, ms));
async function freePort() { const s = createServer(); s.listen(0,'127.0.0.1'); await once(s,'listening'); const n=s.address().port; await new Promise(r=>s.close(r)); return n; }
function start(extra) { return spawn(process.execPath, ['server/index.mjs'], { env: { PATH:process.env.PATH, HOME:process.env.HOME, ...extra }, stdio:['ignore','pipe','pipe'] }); }
test('Disabled durable mode starts without database or R2', async () => {
  const port = await freePort(), child = start({ PORT:String(port), DURABLE_JOBS_ENABLED:'false', TOOLNET_SRT_API_KEY:'test' });
  let exit; child.on('exit',(code,signal)=>exit={code,signal});
  try {
    let response;
    for (let i=0;i<100;i++) { if (exit) throw Error('EXITED_EARLY'); try { response=await fetch(`http://127.0.0.1:${port}/health`); break; } catch { await wait(20); } }
    assert.equal(response.status,200);
  } finally { child.kill('SIGTERM'); if (!exit) await once(child,'exit'); }
});
test('Enabled durable mode fails closed when config is incomplete', async () => {
  const port = await freePort(), child = start({ PORT:String(port), DURABLE_JOBS_ENABLED:'true', TOOLNET_SRT_API_KEY:'test' });
  const [code] = await once(child,'exit'); assert.notEqual(code,0);
  await assert.rejects(fetch(`http://127.0.0.1:${port}/health`));
});
