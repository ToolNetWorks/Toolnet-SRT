// ---------------------------------------------------------------------------
// CLI option types (simple, no external parser dependency)
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Parse CLI args
// ---------------------------------------------------------------------------
function printHelp() {
    console.log(`jianying-subtitle - 剪映 (CapCut) 免费 ASR 字幕转录

Usage:
  jianying-subtitle <input> [options]
  jianying-subtitle batch <dir> [options]

Commands:
  <input>        Transcribe an audio/video file to subtitles
  batch <dir>    Transcribe all audio files in a directory

Options:
  -o, --output <path>      Output file path (default: <input>.srt)
  -f, --format <fmt>       Output format: srt, ass, json, txt (default: srt)
                           Use comma-separated for multiple: srt,json,txt
  -w, --word-timestamps    Word-level timestamps instead of sentence-level
  --no-cache               Disable caching
  --cache-dir <path>       Cache directory (default: ~/.cache/jianying-subtitle)
  --ext <ext>              File extension for batch mode (default: mp3,wav,flac,m4a)
  -v, --verbose            Verbose logging
  --version                Show version
  -h, --help               Show this help

Examples:
  jianying-subtitle audio.mp3
  jianying-subtitle audio.mp3 -o output.srt
  jianying-subtitle audio.mp3 -f json,srt,txt
  jianying-subtitle audio.mp3 -w -o words.srt
  jianying-subtitle batch ./audio/ --ext mp3
  jianying-subtitle batch ./audio/ -o ./output/
`);
}
function parseArgs(raw) {
    const opts = {
        wordTimestamps: false,
        noCache: false,
        verbose: false,
        help: false,
        version: false,
    };
    let i = 0;
    // Check for 'batch' subcommand
    if (raw[i] === "batch") {
        i++;
        opts.batchDir = raw[i];
        if (!opts.batchDir || opts.batchDir.startsWith("-")) {
            console.error("Error: batch requires a directory path");
            process.exit(1);
        }
        i++;
    }
    else if (raw[i] && !raw[i].startsWith("-")) {
        opts.input = raw[i];
        i++;
    }
    while (i < raw.length) {
        const arg = raw[i];
        switch (arg) {
            case "-o":
            case "--output": {
                opts.output = raw[++i];
                break;
            }
            case "-f":
            case "--format": {
                opts.format = raw[++i];
                break;
            }
            case "-w":
            case "--word-timestamps": {
                opts.wordTimestamps = true;
                break;
            }
            case "--no-cache": {
                opts.noCache = true;
                break;
            }
            case "--cache-dir": {
                opts.cacheDir = raw[++i];
                break;
            }
            case "--ext": {
                opts.batchExt = raw[++i];
                break;
            }
            case "-v":
            case "--verbose": {
                opts.verbose = true;
                break;
            }
            case "--version": {
                opts.version = true;
                break;
            }
            case "-h":
            case "--help": {
                opts.help = true;
                break;
            }
            default: {
                // positional (treat as input)
                if (!opts.input && !opts.batchDir) {
                    opts.input = arg;
                }
            }
        }
        i++;
    }
    return opts;
}
// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
import * as path from "node:path";
import * as fs from "node:fs";
import { transcribe } from "./asr/jianying.js";
import { writeSubtitles } from "./subtitle/index.js";
import { videoToAudio, cleanupTempFile } from "./utils/media.js";
const PKG_VERSION = "0.1.0";
const AUDIO_EXTS = new Set(["mp3", "wav", "flac", "m4a"]);
function resolveFormats(raw) {
    if (!raw)
        return ["srt"];
    const fmts = raw.split(",").map((s) => s.trim());
    const valid = new Set(["srt", "ass", "json", "txt"]);
    for (const f of fmts) {
        if (!valid.has(f)) {
            console.error(`Error: unknown format "${f}". Supported: srt, ass, json, txt`);
            process.exit(1);
        }
    }
    return fmts;
}
function getOutputPath(input, output, format) {
    if (output) {
        if (fs.existsSync(output) && fs.statSync(output).isDirectory()) {
            const stem = path.basename(input, path.extname(input));
            return path.join(output, `${stem}.${format ?? "srt"}`);
        }
        return output;
    }
    const dir = path.dirname(path.resolve(input));
    const stem = path.basename(input, path.extname(input));
    return path.join(dir, `${stem}.${format ?? "srt"}`);
}
async function runSingle(opts) {
    if (!opts.input) {
        console.error("Error: no input file specified");
        printHelp();
        process.exit(1);
    }
    // Determine if input is audio or video
    const ext = path.extname(opts.input).slice(1).toLowerCase();
    const isAudio = AUDIO_EXTS.has(ext);
    let audioInput = opts.input;
    let tempAudio;
    if (!isAudio) {
        if (opts.verbose)
            console.error("Converting video to audio...");
        tempAudio = await videoToAudio(opts.input);
        audioInput = tempAudio;
    }
    const formats = resolveFormats(opts.format);
    const primaryFmt = formats[0];
    const output = getOutputPath(opts.input, opts.output, primaryFmt);
    if (opts.verbose) {
        console.log(`Input:  ${opts.input}`);
        console.log(`Audio:  ${audioInput}`);
        console.log(`Output: ${output}`);
        console.log(`Format: ${formats.join(", ")}`);
    }
    try {
        const segments = await transcribe({
            input: audioInput,
            cache: !opts.noCache,
            wordTimestamps: opts.wordTimestamps,
            cacheDir: opts.cacheDir,
            onProgress: opts.verbose
                ? (pct, msg) => console.error(`[${pct}%] ${msg}`)
                : undefined,
        });
        const extras = formats.slice(1);
        writeSubtitles(segments, output, extras.length ? { also: extras } : undefined);
        console.log(output);
    }
    finally {
        if (tempAudio)
            cleanupTempFile(tempAudio);
    }
}
async function runBatch(opts) {
    if (!opts.batchDir) {
        console.error("Error: no batch directory specified");
        process.exit(1);
    }
    const dir = path.resolve(opts.batchDir);
    if (!fs.existsSync(dir) || !fs.statSync(dir).isDirectory()) {
        console.error(`Error: not a directory: ${dir}`);
        process.exit(1);
    }
    const exts = (opts.batchExt ?? "mp3,wav,flac,m4a")
        .split(",")
        .map((s) => s.trim().toLowerCase());
    const files = fs
        .readdirSync(dir)
        .filter((f) => exts.includes(path.extname(f).slice(1).toLowerCase()))
        .map((f) => path.join(dir, f))
        .sort();
    if (files.length === 0) {
        console.error(`No audio files found in ${dir} (ext: ${exts.join(", ")})`);
        process.exit(1);
    }
    console.log(`Found ${files.length} file(s) in ${dir}`);
    const formats = resolveFormats(opts.format);
    const primaryFmt = formats[0];
    const extras = formats.slice(1);
    for (let fi = 0; fi < files.length; fi++) {
        const file = files[fi];
        const output = getOutputPath(file, opts.output, primaryFmt);
        console.log(`[${fi + 1}/${files.length}] ${path.basename(file)}`);
        try {
            const segments = await transcribe({
                input: file,
                cache: !opts.noCache,
                wordTimestamps: opts.wordTimestamps,
                cacheDir: opts.cacheDir,
                onProgress: opts.verbose
                    ? (pct, msg) => console.error(`  [${pct}%] ${msg}`)
                    : undefined,
            });
            writeSubtitles(segments, output, extras.length ? { also: extras } : undefined);
            console.log(`  → ${output} (${segments.length} segments)`);
        }
        catch (err) {
            console.error(`  ✗ ${err.message}`);
        }
    }
}
export async function main() {
    const opts = parseArgs(process.argv.slice(2));
    if (opts.help) {
        printHelp();
        process.exit(0);
    }
    if (opts.version) {
        console.log(`jianying-subtitle v${PKG_VERSION}`);
        process.exit(0);
    }
    if (opts.batchDir) {
        await runBatch(opts);
    }
    else if (opts.input) {
        await runSingle(opts);
    }
    else {
        printHelp();
        process.exit(1);
    }
}
main().catch((err) => {
    console.error(err.message);
    process.exit(1);
});
//# sourceMappingURL=cli.js.map