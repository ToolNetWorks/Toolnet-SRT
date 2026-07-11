# jianying-subtitle

Transcribe audio/video to subtitles using the JianYing (CapCut) free ASR API.
Works as a **CLI**, a **TypeScript library**, and a **[pi](https://github.com/earendil-works/pi-mono) extension**.

- Audio formats: `mp3`, `wav`, `flac`, `m4a` (uploaded directly)
- Video: any format ffmpeg can read (audio track is extracted automatically; supported codecs are stream-copied without re-encoding, otherwise transcoded to WAV; requires `ffmpeg`/`ffprobe` on `PATH`)
- Output: `srt`, `ass`, `json` (with word-level timestamps), `txt`
- No API key required

## Privacy

Audio is uploaded to ByteDance (CapCut) cloud servers for processing. Do not use for confidential content.

## CLI

```bash
npm install -g jianying-subtitle

jianying-subtitle input.mp4                      # → input.json
jianying-subtitle input.mp3 -o subs/out.srt      # custom output path
jianying-subtitle input.wav -o out.json          # format inferred from extension
jianying-subtitle input.mp4 -v                   # verbose progress
```

## Library

```bash
npm install jianying-subtitle
```

```ts
import { transcribe, writeSubtitles, toSrt } from "jianying-subtitle";

const segments = await transcribe({
  input: "audio.mp3",
  onProgress: (percent, message) => console.log(percent, message),
});

console.log(toSrt(segments));
writeSubtitles(segments, "out", "srt", { also: ["json"] });
```

Subpath exports: `jianying-subtitle/asr`, `jianying-subtitle/subtitle`.

## pi extension

Install as a [pi package](https://github.com/earendil-works/pi-mono):

```bash
pi install npm:jianying-subtitle
```

This registers a `transcribe_media` tool the model can call, e.g.:

> "Transcribe ./talk.mp4 to srt and txt"

The tool extracts video audio if needed (fast lossless stream copy when the
codec is already supported), streams progress, **waits for ASR to finish**,
writes the subtitle files, and returns a transcript preview in the same tool
result (synchronous — no background job / polling).

## How it works

Upload → submit ASR task → poll → parse, against the same endpoints the
JianYing desktop client uses. See `src/asr/` for details.

## License

GPL-3.0-only
