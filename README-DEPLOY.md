# ToolNet-SRT — v0.2 upgrade (features 2, 3, 4, 5)

**Not included:** multi-format output. The service returns SRT only. Existing `POST /v1/transcribe` behavior is preserved.

## Contents
- `server/job-utils.mjs`: bounded upload, segment FFmpeg, remote URL allowlisting.
- `server/jobs.mjs`: asynchronous FIFO job queue, progress, cancellation, local metadata checkpoints.
- `server/extended-routes.mjs`: REST routes, `POST /mcp` (stateless Streamable HTTP).
- `server/docs.mjs`: public `/docs` HTML and `/openapi.json`.
- `sdk/client.mjs`: browser/Node ESM REST client.
- `mcp/stdio.mjs`, `mcp/protocol.mjs`: MCP stdio (local AI tools) and shared protocol core.
- `scripts/apply-upgrade.py`: safely patches the existing `server/index.mjs`, package metadata, and .gitignore.
- `tests/`: Node built-in unit/integration tests (run `node --test tests/toolnet-upgrade.node-test.mjs`).

## Install on the VPS

1. **Backup the current repo:** `cd /root/Toolnet-SRT && git status --short && git log -1 --oneline`.
2. Upload/extract this archive to the repository root, preserving directory structure.
3. Run `python3 scripts/apply-upgrade.py` (can safely run twice).
4. Run `npm install --package-lock-only --ignore-scripts && npm run build`.
5. Run `node --test tests/toolnet-upgrade.node-test.mjs && node --check server/index.mjs`.
6. Run `npm test && npm pack --dry-run`.
7. Test the new endpoints locally on a test port before committing or deploying to Render.

Do not publish `0.2.0` on npm until you've tested and reviewed the package. Once published, npm versions are immutable; requires interactive authentication.

## Environment

| Variable | Default | Details |
|--|--|--|
| `PORT` | `10000` | HTTP listening port |
| `TOOLNET_SRT_API_KEY` | required | API authentication |
| `TOOLNET_SRT_DATA_DIR` | `/tmp/toolnet-srt-jobs` | Persistent only when backed by persistent disk |
| `TOOLNET_SRT_MAX_UPLOAD_MB` | `256` | Per-request upload/download limit, configurable up to 2048 MiB |
| `TOOLNET_SRT_MAX_JOBS` | `20` | Active queued/running jobs limit |
| `TOOLNET_SRT_MAX_SEGMENTS` | `240` | 240 x 60-sec segments = about 4 hours |
| `TOOLNET_SRT_SEGMENT_SECONDS` | `60` | Per-segment audio length (60 sec already tested; raise after validation) |
| `TOOLNET_SRT_JOB_TIMEOUT_MS` | `7200000` | 2-hour job deadline |
| `TOOLNET_SRT_RETENTION_HOURS` | `24` | Expiration check on job-store initialization |
| `TOOLNET_SRT_ALLOWED_MEDIA_HOSTS` | empty | Comma-separated exact HTTPS hosts for remote ingest |
| `TOOLNET_SRT_MCP_ORIGINS` | empty | Comma-separated permitted MCP browser Origin values |
| `TOOLNET_SRT_BASE_URL` | Render URL | MCP stdio client target |

## REST API (SRT output only)

```
GET    /api/health
GET    /docs
GET    /openapi.json
POST   /v1/transcribe           (existing synchronous API)
POST   /v1/jobs                 (raw audio or video binary with Content-Type)
POST   /v1/jobs/url             ({"url":"https://..."}, only allowlisted host)
GET    /v1/jobs/:id             (status/progress)
GET    /v1/jobs/:id/result      (SRT after completion)
DELETE /v1/jobs/:id             (cancel)
POST   /mcp                    (JSON-RPC, stateless)
```

### Example: file upload

```bash
curl -sS -X POST -H "X-API-Key: $TOOLNET_SRT_API_KEY" \
  -H 'Content-Type: video/mp4' --data-binary @sample.mp4 \
  'http://127.0.0.1:10086/v1/jobs'
```

Response contains `id` and `status`, typically `queued` or `processing`.

```bash
curl -H "X-API-Key: $TOOLNET_SRT_API_KEY" 'http://127.0.0.1:10086/v1/jobs/ID'
curl -H "X-API-Key: $TOOLNET_SRT_API_KEY" 'http://127.0.0.1:10086/v1/jobs/ID/result' -o subtitle.srt
```

### SDK

```js
import { readFile } from 'node:fs/promises';
import { ToolNetSRTClient } from './sdk/client.mjs';

const client = new ToolNetSRTClient({
  baseUrl: 'https://toolnet-srt.onrender.com',
  apiKey: process.env.TOOLNET_SRT_API_KEY,
});
const bytes = await readFile('sample.mp4');
const job = await client.createJobFromBytes(bytes, 'video/mp4');
await client.waitForJob(job.id);
const subtitles = await client.getResult(job.id);
console.log(subtitles);
```

After npm v0.2.0 publish, use `import { ToolNetSRTClient } from 'toolnet-srt/client'`.

### MCP stdio

Use an MCP client that can spawn a local process:

```json
{
  "mcpServers": {
    "toolnet-srt": {
      "command": "node",
      "args": ["/root/Toolnet-SRT/mcp/stdio.mjs"],
      "env": {
        "TOOLNET_SRT_BASE_URL": "https://toolnet-srt.onrender.com",
        "TOOLNET_SRT_API_KEY": "CONFIGURE_THIS_LOCALLY"
      }
    }
  }
}
```

The MCP `submit_transcription_url` tool requires `TOOLNET_SRT_ALLOWED_MEDIA_HOSTS` on the server. Other tools can query or cancel existing jobs created over REST.

### MCP Streamable HTTP

- URL: `https://toolnet-srt.onrender.com/mcp`
- Headers: `X-API-Key`, `Accept: application/json, text/event-stream`, `Content-Type: application/json`.
- A client library should initiate with `initialize`, then use `tools/list` and `tools/call`.
- No SSE event stream or protocol sessions are used (stateless implementation).

## Reliability limits

**Render Free is not durable job infrastructure.** Render Free spins down after inactivity and discards every local file on redeploy/restart/spin-down. This upgrade stores jobs and media in local folders. In-flight/queued jobs **will be lost** after a Free instance restart; completed results are also lost.

For real multi-hour production use: run this worker on the user's persistent VPS with `TOOLNET_SRT_DATA_DIR` on persistent disk, or add Neon (job metadata) + R2 (media/results) and a persistent worker. The current queue is single-process/single-instance and **not distributed**. Do not scale the Web Service beyond one instance without a shared locking/queue backend.

The URL ingestion endpoint is secured with an explicit HTTPS hostname allowlist. Prefer a dedicated download-only object-storage domain; do not configure arbitrary user-supplied hosts. The server never writes signed media URLs to job metadata.
