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

## Installation from source

```bash
git clone https://github.com/ToolNetWorks/Toolnet-SRT.git
cd Toolnet-SRT
npm ci
npm run build
```

## CLI usage

Current upstream-compatible CLI:

```bash
node bin/jianying-subtitle.mjs input.mp4 -o output.srt
node bin/jianying-subtitle.mjs input.mp3 -o output.json
node bin/jianying-subtitle.mjs input.wav -o output.ass
```

The dedicated ToolNet npm package and CLI name will be introduced in a separate release.

## TypeScript SDK

After building from source:

```ts
import { transcribe, toSrt } from "./dist/index.js";

const segments = await transcribe({ input: "audio.mp3" });
const srt = toSrt(segments);
console.log(srt);
```

## Development

```bash
npm ci
npm run build
npm test
```

## Roadmap

- Dedicated ToolNet npm package
- CLI named `toolnet-srt`
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
