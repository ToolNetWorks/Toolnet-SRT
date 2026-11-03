import { describe, expect, it, vi } from 'vitest';
import { uploadSign, uploadAuth, uploadFile, uploadCommit } from './upload.js';
import { parseSegments, pollForResult } from './transcribe.js';
import { JianYingASR } from './jianying.js';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as helpers from './helpers.js';

const sensitive = 'sensitive-private-token';
const context = { tdid:'test',crc32Hex:'abc',fileBuffer:Buffer.from('x'),fileSize:1,
  checkedFetch: vi.fn(async () => new Response(JSON.stringify({ret:'1', data:{secret_access_key:sensitive}}),{status:200})) };
describe('ASR error sanitization', () => {
  it('does not expose upstream credentials in sign and auth errors', async () => {
    await expect(uploadSign(context)).rejects.toThrow(/^Upload sign failed$/);
    await expect(uploadAuth(context,{accessKey:'key',secretKey:'secret',sessionToken:'token'})).rejects.toThrow(/^Upload auth failed/);
  });
  it('does not expose response data in upload or result errors', async () => {
    const ctx = {...context, checkedFetch: vi.fn(async () => new Response(JSON.stringify({success:1, token:sensitive}),{status:200}))};
    await expect(uploadFile(ctx,{accessKey:'a',secretKey:'s',sessionToken:'t',storeUri:'path',authToken:'auth',uploadId:'id',uploadHost:'example.test'})).rejects.toThrow(/^File upload failed$/);
    expect(() => parseSegments({ret:'0',data:{private:sensitive}} as never)).toThrow(/^Malformed query response: missing utterances$/);
    await expect(pollForResult('id','tdid',async()=>new Response(JSON.stringify({ret:'1',errmsg:sensitive}),{status:200}),{queryTimeoutMs:1000})).rejects.toThrow(/^Query failed$/);
  });
  it('does not include signed URL or response body in HTTP errors', async () => {
    const dir=mkdtempSync(join(tmpdir(),'asr-error-')); const path=join(dir,'sample.mp3'); writeFileSync(path,'x');
    const original=globalThis.fetch;
    try {
      vi.spyOn(helpers, 'generateTdid').mockReturnValue('00000000-0000-4000-8000-000000000000');
      globalThis.fetch=vi.fn(async()=>new Response(sensitive,{status:503})) as typeof fetch;
      await expect(new JianYingASR({input:path}).transcribe()).rejects.toThrow(/^HTTP 503 from ASR service$/);
    } finally { vi.restoreAllMocks(); globalThis.fetch=original; rmSync(dir,{recursive:true,force:true}); }
  });
  it('exposes the 429 retry delay without exposing the response body', async () => {
    const dir=mkdtempSync(join(tmpdir(),'asr-rate-')); const path=join(dir,'sample.mp3'); writeFileSync(path,'x');
    const original=globalThis.fetch;
    try {
      vi.spyOn(helpers, 'generateTdid').mockReturnValue('00000000-0000-4000-8000-000000000000');
      globalThis.fetch=vi.fn(async()=>new Response(sensitive,{status:429,headers:{'retry-after':'3'}})) as typeof fetch;
      await expect(new JianYingASR({input:path}).transcribe()).rejects.toMatchObject({
        message:'HTTP 429 from ASR service', status:429, retryAfterMs:3000,
      });
    } finally { vi.restoreAllMocks(); globalThis.fetch=original; rmSync(dir,{recursive:true,force:true}); }
  });
  it('does not swallow a rate limit during best-effort commit', async () => {
    const error = Object.assign(new Error('rate limited'), { status: 429, retryAfterMs: 5000 });
    const ctx = { ...context, checkedFetch: vi.fn(async () => { throw error; }) };
    await expect(uploadCommit(ctx,{accessKey:'a',secretKey:'s',sessionToken:'t',storeUri:'path',authToken:'auth',uploadId:'id',uploadHost:'example.test'})).rejects.toBe(error);
  });
});
