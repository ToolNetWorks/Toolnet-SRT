const jobSchema = {
  type: 'object', properties: {
    id: { type: 'string', format: 'uuid' },
    status: { type: 'string', enum: ['queued', 'processing', 'completed', 'failed', 'cancelled'] },
    stage: { type: 'string' }, progress: { type: 'integer', minimum: 0, maximum: 100 },
    completedSegments: { type: 'integer' }, totalSegments: { type: ['integer', 'null'] },
    errorCode: { type: ['string', 'null'] },
    resultPath: { type: ['string', 'null'] },
  },
};
const secured = [{ ApiKeyAuth: [] }];

export const OPENAPI = {
  openapi: '3.1.0',
  info: { title: 'ToolNet-SRT API', version: '0.2.0', description: 'SRT transcription with segmented background jobs. V1 synchronous endpoint remains supported.' },
  servers: [{ url: 'https://toolnet-srt.onrender.com' }],
  components: {
    securitySchemes: { ApiKeyAuth: { type: 'apiKey', in: 'header', name: 'X-API-Key' } },
    schemas: { Job: jobSchema },
  },
  paths: {
    '/api/health': { get: { summary: 'Lightweight health check', responses: { '200': { description: 'OK' } } } },
    '/health': { get: { summary: 'Legacy health check', responses: { '200': { description: 'Ready' } } } },
    '/v1/transcribe': { post: { summary: 'Legacy synchronous transcription, SRT output', security: secured,
      requestBody: { required: true, content: { 'audio/mpeg': { schema: { type: 'string', format: 'binary' } } } },
      responses: { '200': { description: 'SRT body', content: { 'application/x-subrip': { schema: { type: 'string' } } } } },
    } },
    '/v1/jobs': { post: { summary: 'Upload audio/video binary and enqueue transcription', security: secured,
      requestBody: { required: true, content: { 'video/mp4': { schema: { type: 'string', format: 'binary' } }, 'audio/mpeg': { schema: { type: 'string', format: 'binary' } } } },
      responses: { '202': { description: 'Queued', content: { 'application/json': { schema: { $ref: '#/components/schemas/Job' } } } } },
    } },
    '/v1/jobs/url': { post: { summary: 'Create job from HTTPS media URL', security: secured,
      parameters: [{ name: 'Idempotency-Key', in: 'header', required: false, description: 'Required in durable mode; use the same key when retrying the same URL.', schema: { type: 'string', minLength: 1, maxLength: 255 } }],
      requestBody: { required: true, content: { 'application/json': { schema: { type: 'object', properties: { url: { type: 'string', format: 'uri' } }, required: ['url'] } } } },
      responses: { '202': { description: 'Queued', content: { 'application/json': { schema: { $ref: '#/components/schemas/Job' } } } } },
    } },
    '/v1/jobs/{jobId}': {
      parameters: [{ name: 'jobId', in: 'path', required: true, schema: { type: 'string', format: 'uuid' } }],
      get: { summary: 'Get job progress', security: secured, responses: { '200': { description: 'Job status', content: { 'application/json': { schema: { $ref: '#/components/schemas/Job' } } } } } },
      delete: { summary: 'Cancel queued or processing job', security: secured, responses: { '200': { description: 'Cancelled' } } },
    },
    '/v1/jobs/{jobId}/result': {
      parameters: [{ name: 'jobId', in: 'path', required: true, schema: { type: 'string', format: 'uuid' } }],
      get: { summary: 'Download completed SRT subtitle', security: secured, responses: { '200': { description: 'SRT', content: { 'application/x-subrip': { schema: { type: 'string' } } } } } },
    },
    '/mcp': { post: { summary: 'MCP Streamable HTTP JSON-RPC endpoint (stateless)', security: secured, responses: { '200': { description: 'JSON-RPC result' } } } },
  },
};

export const DOCS_HTML = `<!doctype html>
<html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>ToolNet-SRT API Docs</title>
<style>body{background:#fbfbfb;color:#202020;max-width:950px;margin:40px auto;padding:0 20px;font:16px/1.6 system-ui,sans-serif}code,pre{background:#ececec;border-radius:6px}pre{overflow:auto;padding:18px}code{padding:2px 4px}h1,h2{letter-spacing:-.02em}a{color:#205caa}small{color:#555}table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #ddd;text-align:left;padding:10px}hr{border:0;border-top:1px solid #ddd}</style></head>
<body><h1>ToolNet-SRT API</h1><p>Nhận dạng giọng nói và tạo phụ đề <b>SRT</b>. API dùng <code>X-API-Key</code>. V1 vẫn giữ nguyên.</p>
<p><a href="/openapi.json">OpenAPI 3.1 JSON</a> · <a href="/api/health">Health</a></p>
<h2>Endpoints</h2><table><tr><th>Method</th><th>Path</th><th>Mục đích</th></tr>
<tr><td>POST</td><td><code>/v1/transcribe</code></td><td>Đồng bộ, cho file nhỏ (giữ tương thích)</td></tr>
<tr><td>POST</td><td><code>/v1/jobs</code></td><td>Upload file binary và tạo job</td></tr>
<tr><td>POST</td><td><code>/v1/jobs/url</code></td><td>Nhận URL HTTPS công khai</td></tr>
<tr><td>GET</td><td><code>/v1/jobs/{id}</code></td><td>Trạng thái, tiến độ, số đoạn</td></tr>
<tr><td>GET</td><td><code>/v1/jobs/{id}/result</code></td><td>Tải SRT khi completed</td></tr>
<tr><td>DELETE</td><td><code>/v1/jobs/{id}</code></td><td>Hủy job</td></tr>
<tr><td>POST</td><td><code>/mcp</code></td><td>MCP Streamable HTTP JSON-RPC</td></tr></table>
<h2>1. Tạo job</h2><pre>curl -X POST 'https://toolnet-srt.onrender.com/v1/jobs' \\
  -H 'X-API-Key: YOUR_API_KEY' \\
  -H 'Content-Type: video/mp4' \\
  --data-binary @video.mp4</pre>
<p>Trả về <code>202</code> cùng <code>id</code>, <code>status</code>, <code>progress</code>. Upload raw binary, không phải multipart.</p>
<h2>2. Theo dõi</h2><pre>curl -H 'X-API-Key: YOUR_API_KEY' \\
  'https://toolnet-srt.onrender.com/v1/jobs/YOUR_JOB_ID'</pre>
<h2>3. Lấy SRT</h2><pre>curl -H 'X-API-Key: YOUR_API_KEY' \\
  'https://toolnet-srt.onrender.com/v1/jobs/YOUR_JOB_ID/result' \\
  -o subtitles.srt</pre>
<h2>4. Tạo job từ URL HTTPS</h2><pre>curl -X POST 'https://toolnet-srt.onrender.com/v1/jobs/url' \\
  -H 'X-API-Key: YOUR_API_KEY' -H 'Content-Type: application/json' \\
  -H 'Idempotency-Key: YOUR_STABLE_REQUEST_ID' \\
  -d '{"url":"https://media.example.com/video.mp4"}'</pre>
<p>Durable mode chấp nhận hostname HTTPS công khai, kiểm tra DNS và khóa kết nối vào IP đã xác thực. Không cho phép redirect hoặc địa chỉ nội bộ. Chế độ cũ vẫn yêu cầu allowlist. Durable mode yêu cầu key ổn định cho mỗi yêu cầu và dùng lại key khi retry.</p>
<h2>5. SDK và MCP</h2><p>SDK: <code>sdk/client.mjs</code>; MCP stdio: <code>node mcp/stdio.mjs</code>; MCP qua HTTP: <code>POST /mcp</code> (đặt <code>X-API-Key</code> ở cấu hình MCP client).</p>
<hr><p><small>Chế độ mặc định lưu job trên filesystem cục bộ. Durable mode sử dụng Neon và R2 khi đã được cấu hình và khởi động thành công. API key dùng chung cấp quyền cho toàn bộ job của một tenant.</small></p></body></html>`;
