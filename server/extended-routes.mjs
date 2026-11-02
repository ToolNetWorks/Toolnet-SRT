import { DOCS_HTML, OPENAPI } from './docs.mjs';
import { createMcpProtocol } from '../mcp/protocol.mjs';
import { JobError } from './job-utils.mjs';

function send(res, status, data, type = 'application/json; charset=utf-8') {
  if (res.headersSent || res.destroyed) return;
  const body = type.startsWith('application/json') ? JSON.stringify(data) : data;
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
  res.end(body);
}

async function readJson(req, maxBytes = 16384) {
  if (!String(req.headers['content-type'] || '').toLowerCase().startsWith('application/json')) {
    throw new JobError('JSON_CONTENT_TYPE_REQUIRED', 415);
  }
  let received = 0;
  const chunks = [];
  for await (const chunk of req) {
    received += chunk.length;
    if (received > maxBytes) throw new JobError('JSON_BODY_TOO_LARGE', 413);
    chunks.push(chunk);
  }
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { throw new JobError('INVALID_JSON'); }
}

export function createExtendedRoutes({ manager, authenticate, apiKeyPresent }) {
  const protocol = createMcpProtocol({
    createJobFromUrl: url => manager.createRemote(url),
    getJob: id => manager.getJob(id),
    getResult: id => manager.getResult(id),
    cancelJob: id => manager.cancelJob(id),
  });
  const mcpOrigins = (process.env.TOOLNET_SRT_MCP_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);

  async function handle(req, res, pathname) {
    if (req.method === 'GET' && pathname === '/docs') {
      send(res, 200, DOCS_HTML, 'text/html; charset=utf-8');
      return;
    }
    if (req.method === 'GET' && pathname === '/openapi.json') {
      send(res, 200, OPENAPI);
      return;
    }
    if (!apiKeyPresent()) {
      send(res, 503, { error: 'API_KEY_NOT_CONFIGURED' });
      return;
    }
    if (!authenticate(req.headers['x-api-key'])) {
      send(res, 401, { error: 'UNAUTHORIZED' });
      return;
    }
    try {
      if (pathname === '/mcp') {
        if (req.method !== 'POST') {
          send(res, 405, { error: 'METHOD_NOT_ALLOWED' });
          return;
        }
        const origin = req.headers.origin;
        if (origin && !mcpOrigins.includes(origin)) {
          send(res, 403, { error: 'ORIGIN_NOT_ALLOWED' });
          return;
        }
        const version = req.headers['mcp-protocol-version'];
        if (version && !['2025-03-26', '2025-06-18', '2025-11-25'].includes(version)) {
          send(res, 400, { error: 'UNSUPPORTED_MCP_VERSION' });
          return;
        }
        const accept = req.headers.accept || '';
        if (!accept.includes('application/json') && !accept.includes('*/*')) {
          send(res, 406, { error: 'NOT_ACCEPTABLE' });
          return;
        }
        const message = await readJson(req);
        const response = await protocol(message);
        if (!response) { res.writeHead(202); res.end(); return; }
        send(res, 200, response);
        return;
      }
      if (pathname === '/v1/jobs' && req.method === 'POST') {
        const mime = String(req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
        send(res, 202, await manager.createUpload(req, mime));
        return;
      }
      if (pathname === '/v1/jobs/url' && req.method === 'POST') {
        const data = await readJson(req);
        if (typeof data?.url !== 'string') throw new JobError('SOURCE_URL_REQUIRED');
        send(res, 202, await manager.createRemote(data.url));
        return;
      }
      const match = pathname.match(/^\/v1\/jobs\/([^/]+)(\/result)?$/);
      if (match && !match[2] && req.method === 'GET') {
        send(res, 200, await manager.getJob(match[1]));
        return;
      }
      if (match && !match[2] && req.method === 'DELETE') {
        send(res, 200, await manager.cancelJob(match[1]));
        return;
      }
      if (match && match[2] && req.method === 'GET') {
        const bytes = await manager.getResult(match[1]);
        res.writeHead(200, {
          'Content-Type': 'application/x-subrip; charset=utf-8',
          'Content-Disposition': 'attachment; filename="subtitle.srt"',
          'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
          'Content-Length': bytes.byteLength,
        });
        res.end(bytes);
        return;
      }
      send(res, 405, { error: 'METHOD_NOT_ALLOWED' });
    } catch (error) {
      const status = error instanceof JobError ? error.status : 500;
      const code = error instanceof JobError ? error.code : 'INTERNAL_ERROR';
      if (status >= 500) console.error('[ToolNet-SRT] HTTP error:', code, error?.message);
      send(res, status, { error: code });
    }
  }

  return {
    isExtended(pathname) {
      return pathname === '/docs' || pathname === '/openapi.json' ||
        pathname === '/mcp' || pathname === '/v1/jobs' || pathname.startsWith('/v1/jobs/');
    },
    handle,
  };
}
