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
