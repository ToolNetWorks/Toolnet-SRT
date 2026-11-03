# ToolNet-SRT

Audio & Video Subtitle Transcription Toolkit

ToolNet-SRT is an open-source toolkit for automatically transcribing audio and video into subtitle files, powered by JianYing (CapCut) speech recognition.

## Features

- Automatic audio and video transcription
- Chinese speech recognition
- SRT, ASS, JSON and TXT output
- Word-level timestamps in JSON
- TypeScript SDK and command-line interface (CLI)
- FFmpeg-based audio extraction
- No API key required for the JianYing ASR engine

## Requirements

- Node.js 18+
- Node.js 20.19+ or 22.12+ for development tests and the Durable server
- FFmpeg and FFprobe
- Internet connectivity

## Installation from npm

Install the CLI globally:

```bash
npm install -g toolnet-srt
toolnet-srt --help
```

Or install the package in a project to use the SDK:

```bash
npm install toolnet-srt
```

You can also run the CLI without a global installation:

```bash
npx toolnet-srt --help
```

## Installation from source

```bash
git clone https://github.com/ToolNetWorks/Toolnet-SRT.git
cd Toolnet-SRT
npm ci
npm run build
```

## CLI usage

After installing globally, use the CLI to transcribe audio or video:

```bash
toolnet-srt input.mp4 -o output.srt
toolnet-srt input.mp3 -o output.json
toolnet-srt input.wav -o output.ass
```

When running from a source checkout, replace `toolnet-srt` with
`node bin/jianying-subtitle.mjs` after building.

## TypeScript SDK

After installing the package:

```ts
import { transcribe, toSrt } from "toolnet-srt";

const segments = await transcribe({ input: "audio.mp3" });
const srt = toSrt(segments);
console.log(srt);
```

## Render HTTP API

The hosted API is available at `https://toolnet-srt.onrender.com`.
Check its health with:

```bash
curl -fsS https://toolnet-srt.onrender.com/api/health
```

Set your API token locally, then send the audio as the raw request body:

```bash
read -rsp "API token: " TOOLNET_SRT_API_KEY
echo

curl --fail-with-body --show-error --max-time 420 \
  -H "X-API-Key: $TOOLNET_SRT_API_KEY" \
  -H "Content-Type: audio/mpeg" \
  --data-binary @audio.mp3 \
  -o subtitle.srt \
  https://toolnet-srt.onrender.com/v1/transcribe

unset TOOLNET_SRT_API_KEY
```

Use the matching content type for the input: `video/mp4`, `audio/mpeg`,
`audio/wav`, `audio/x-wav`, `audio/mp4`, or `audio/flac`. Uploads are
limited to 40 MiB. The service accepts one transcription at a time and may
return HTTP 429 while busy. The token is required for transcription, but not
for the health endpoint. Keep it private.

When hosting the service yourself, set the `TOOLNET_SRT_API_KEY` environment
variable on the server to the token clients should send in `X-API-Key`.

## Development

```bash
npm ci
npm run build
npm test
```

## Roadmap

- Long-video chunk processing
- Subtitle merging and timestamp correction
- Optional subtitle translation
- Render deployment
- External application integration

These roadmap features are not yet guaranteed to be available in the current release.

## Privacy

Audio is uploaded to ByteDance/JianYing servers for speech recognition. Do not submit confidential audio or video.

## License

GNU General Public License v3.0 only. See [LICENSE](LICENSE) for the complete license text and [NOTICE.md](NOTICE.md) for attribution.

Copyright (C) 2026 xifan2333. ToolNetWorks retains copyright in its own original contributions.

## Durable jobs (staging configuration)

Durable mode is disabled by default. It requires Neon migrations 002–005,
Cloudflare R2, `DURABLE_JOBS_ENABLED=true`, a stable 32-byte base64url
`TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY`, a different stable 32-byte base64url
`TOOLNET_SRT_REMOTE_IDEMPOTENCY_KEY`. Durable mode accepts HTTPS URLs from
public hosts and validates every DNS answer before pinning the connection.
Generate secrets securely and preserve them across restarts; rotating them while
jobs or idempotency records exist changes behavior. Apply migrations only to the
intended database after reviewing the deployment plan.

Durable workers checkpoint ASR results per chunk in Neon. `DURABLE_CHUNK_SECONDS`
defaults to 600 seconds; `DURABLE_CHUNK_OVERLAP_SECONDS` defaults to 0 and
`DURABLE_CHUNK_MAX_ATTEMPTS` defaults to 3. Frame timestamps are measured with
FFprobe on the source timeline. `DURABLE_CHUNK_CHECKPOINTS_ENABLED=false` keeps
the earlier worker behavior. Chunk extraction currently decodes from the start
of the local source for each chunk; performance should be measured for long
files. Overlap deduplication removes matching text with substantial temporal
overlap; differing ASR transcripts can still produce duplicates.

The API key is a **single-tenant credential**: every holder can read and cancel
all jobs. Do not use it to isolate mutually untrusted customers. Durable mode
limits job creation to 60 requests per minute and four concurrent requests per
server process; this is not a distributed rate limit.

Submit a URL with a unique key, and reuse that key on retries:

For a separate continuously running worker, set `DURABLE_JOBS_ENABLED=true` and
`DURABLE_RUN_WORKERS=false` on the HTTP service. Run
`node server/durable-worker-service.mjs` under a process supervisor on the worker
host, with `DURABLE_JOBS_ENABLED=true` and the same database, bucket, remote URL
encryption key and idempotency key as the HTTP service. The worker
does not expose HTTP and drains its schedulers on shutdown. Keep one worker
process with the default ASR concurrency of one. This arrangement requires a
tested staging deployment; Render Free can stop the HTTP service while idle and
is not a replacement for an always-on worker.

```bash
curl -X POST "$TOOLNET_SRT_BASE_URL/v1/jobs/url" \
  -H "X-API-Key: $TOOLNET_SRT_API_KEY" \
  -H "Idempotency-Key: request-unique-id" \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://allowed.example/audio.mp3"}'
```

The existing `submit_transcription_url` MCP tool accepts an optional
`idempotency_key` argument, required by durable mode. MCP HTTP and stdio use the
same API key and job service. For the HTTP client SDK:

```ts
import { ToolNetSRTClient } from 'toolnet-srt/client';
const client = new ToolNetSRTClient({ baseUrl, apiKey });
const job = await client.createJobFromUrl(url, { idempotencyKey: requestId });
await client.waitForJob(job.id);
const srt = await client.getResult(job.id);
```

### Experimental chunk seek

`DURABLE_EXPERIMENTAL_CHUNK_SEEK_PREROLL_MS` is disabled by default (`0`). For isolated benchmarking, setting it to `2000` seeks near each chunk before decoding while keeping the original timeline trim. Seek accuracy depends on container, codec, timestamps and decoder preroll; validate against full decoding for your media before using it. It does not enable parallel ASR or enforce a provider-wide concurrency limit.

`DURABLE_EXPERIMENTAL_ASR_SEMAPHORE=true` is an isolated-testing switch, off by default. It requires migration 006 in the selected test schema. The synchronous `/v1/transcribe` route creates a durable job and waits for its SRT, so its ASR calls also use the semaphore. `DURABLE_ASR_GLOBAL_CONCURRENCY` defaults to 1 and allows 2 only with the experimental switch. All participating instances must use identical semaphore settings. Slot quarantine is conservative and does not prove that the upstream provider has stopped processing after a network partition; do not enable this experiment in production without resolving that limitation and completing staging validation.
