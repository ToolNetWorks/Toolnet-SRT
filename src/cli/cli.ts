import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

import { parseArgs, type CliOptions } from "./args.js";
import { resolveOutput } from "./output.js";
import { transcribe } from "../asr/jianying.js";
import { writeSubtitle } from "../subtitle/index.js";
import type { AssStyleOptions } from "../subtitle/ass.js";
import { videoToAudio, cleanupTempFile } from "../utils/media.js";

const require = createRequire(import.meta.url);
const pkg = require("../../package.json") as { version: string };
const PKG_VERSION = pkg.version;

const AUDIO_EXTS = new Set(["mp3", "wav", "flac", "m4a"]);

function printHelp(): void {
  process.stdout.write(`jianying-subtitle - JianYing (CapCut) free ASR subtitle transcription

Usage:
  jianying-subtitle <input> [options]

Options:
  -o, --output <path>      Output file path (default: <input>.json)
                           Format is inferred from extension: .srt, .ass, .json, .txt
  --ass-style <json>       ASS style overrides as JSON (only for .ass output)
                           Example: '{"fontSize":18,"primaryColour":"&H00FFFF00"}'
  -v, --verbose            Verbose logging
  --version                Show version
  -h, --help               Show this help

Examples:
  jianying-subtitle audio.mp3           # → audio.json
  jianying-subtitle video.mp4 -o out.srt
  jianying-subtitle video.mp4 -o out.ass
  jianying-subtitle video.mp4 -o out.ass --ass-style '{"fontSize":18}'
  jianying-subtitle video.mp4 -o out.json
  jianying-subtitle video.mp4 -o out.txt

Batch usage is intentionally left to the shell:
  for f in audio/*.mp3; do jianying-subtitle "$f"; done
`);
}

async function runSingle(opts: CliOptions): Promise<void> {
  if (!opts.input) {
    throw new Error("No input file specified");
  }

  const { outputPath, format } = resolveOutput(opts.input, opts.output);

  if (opts.verbose) {
    process.stderr.write(`Input  : ${opts.input}\n`);
    process.stderr.write(`Output : ${outputPath}\n`);
    process.stderr.write(`Format : ${format}\n`);
  }

  const ext = path.extname(opts.input).slice(1).toLowerCase();
  const isAudio = AUDIO_EXTS.has(ext);
  let audioInput = opts.input;
  let tempAudio: string | undefined;

  if (!isAudio) {
    if (opts.verbose) process.stderr.write("Converting video to audio...\n");
    tempAudio = await videoToAudio(opts.input);
    audioInput = tempAudio;
  }

  try {
    const segments = await transcribe({
      input: audioInput,
      onProgress: opts.verbose
        ? (pct, msg) => process.stderr.write(`[${pct}%] ${msg}\n`)
        : undefined,
    });

    let assStyle: AssStyleOptions | undefined;
    if (opts.assStyle) {
      try {
        assStyle = JSON.parse(opts.assStyle) as AssStyleOptions;
      } catch {
        throw new Error(
          `Invalid --ass-style JSON: ${opts.assStyle}`,
        );
      }
    }
    writeSubtitle({ segments, output: outputPath, format, assStyle });
    process.stdout.write(`${outputPath}\n`);
  } finally {
    if (tempAudio) cleanupTempFile(tempAudio);
  }
}

export async function main(): Promise<void> {
  const opts = parseArgs(process.argv.slice(2));

  if (opts.help) {
    printHelp();
    return;
  }

  if (opts.version) {
    console.log(`jianying-subtitle v${PKG_VERSION}`);
    return;
  }

  if (opts.input) {
    await runSingle(opts);
  } else {
    printHelp();
    throw new Error("No input file specified");
  }
}

const isEntrypoint =
  process.argv[1] !== undefined &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isEntrypoint) {
  main().catch((err) => {
    process.stderr.write(
      `${err instanceof Error ? err.message : String(err)}\n`,
    );
    process.exit(1);
  });
}
