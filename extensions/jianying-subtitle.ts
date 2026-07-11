/**
 * pi extension: thin wrapper around the jianying-subtitle library.
 *
 * Registers tools that the LLM can call to transcribe audio/video files into
 * subtitles using the JianYing (CapCut) free ASR API.
 *
 * Runs synchronously: the tool call awaits ASR completion and returns the
 * result in the same turn (progress streams via onUpdate). This keeps the
 * agent conversation continuous without background job / follow-up machinery.
 *
 * Loaded by pi via jiti, so TypeScript sources are imported directly
 * (no build step required).
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { truncateHead } from "@earendil-works/pi-coding-agent";
import { StringEnum } from "@earendil-works/pi-ai";
import { Type } from "typebox";

import { transcribe } from "../src/asr/jianying.ts";
import { SUPPORTED_EXTS } from "../src/asr/constants.ts";
import { writeSubtitles } from "../src/subtitle/write.ts";
import type { SubtitleFormat } from "../src/subtitle/write.ts";
import type { AssStyleOptions } from "../src/subtitle/ass.ts";
import { toTxt } from "../src/subtitle/txt.ts";
import { videoToAudio, cleanupTempFile } from "../src/utils/media.ts";

const FORMATS = ["srt", "ass", "json", "txt"] as const;

type TranscriptionResult = {
  input: string;
  files: string[];
  segmentCount: number;
  preview: string;
  previewTruncated: boolean;
  previewOutputLines?: number;
  previewTotalLines?: number;
};

/** Strip a known subtitle extension so `output` can be a file or a base path. */
function toOutputBase(output: string): string {
  const ext = path.extname(output).slice(1).toLowerCase();
  return (FORMATS as readonly string[]).includes(ext)
    ? output.slice(0, -(ext.length + 1))
    : output;
}

function normalizeInputPath(cwd: string, input: string): string {
  // Some models prefix paths with @ — normalize like the built-in tools do.
  const rawInput = input.startsWith("@") ? input.slice(1) : input;
  return path.resolve(cwd, rawInput);
}

function normalizeFormats(formats?: readonly SubtitleFormat[]): SubtitleFormat[] {
  return (formats?.length ? [...new Set(formats)] : ["srt"]) as SubtitleFormat[];
}

function defaultOutputBase(inputPath: string): string {
  return inputPath.slice(0, -path.extname(inputPath).length || undefined);
}

function assertInputFile(inputPath: string): void {
  if (!fs.existsSync(inputPath) || !fs.statSync(inputPath).isFile()) {
    throw new Error(`File not found: ${inputPath}`);
  }
}

function formatResult(result: TranscriptionResult): string {
  if (result.segmentCount === 0) {
    return "Transcription finished but no speech was detected. No files written.";
  }

  let text =
    `Transcribed ${result.segmentCount} segments from ${result.input}\n` +
    `Files written:\n${result.files.map((f) => `  - ${f}`).join("\n")}\n\n` +
    `Transcript preview:\n${result.preview}`;
  if (result.previewTruncated) {
    text += `\n[Preview truncated: ${result.previewOutputLines} of ${result.previewTotalLines} lines shown. Full transcript is in the written files.]`;
  }
  return text;
}

async function runTranscription(options: {
  inputPath: string;
  outBase: string;
  formats: SubtitleFormat[];
  assStyle?: AssStyleOptions;
  signal?: AbortSignal;
  report?: (percent: number, message: string) => void;
}): Promise<TranscriptionResult> {
  const { inputPath, outBase, formats, assStyle, signal, report } = options;
  const primary = formats[0];
  const also = formats.slice(1);
  const ext = path.extname(inputPath).slice(1).toLowerCase();
  let audioPath = inputPath;
  let tmpAudio: string | undefined;

  try {
    if (!SUPPORTED_EXTS.has(ext)) {
      report?.(5, "Extracting audio track with ffmpeg (stream-copy when possible)...");
      tmpAudio = await videoToAudio(inputPath, { signal });
      audioPath = tmpAudio;
    }

    if (signal?.aborted) throw new Error("Cancelled");

    const segments = await transcribe({
      input: audioPath,
      signal,
      onProgress: (percent, message) => {
        if (signal?.aborted) throw new Error("Cancelled");
        report?.(percent, message);
      },
    });

    if (segments.length === 0) {
      return {
        input: inputPath,
        files: [],
        segmentCount: 0,
        preview: "",
        previewTruncated: false,
      };
    }

    writeSubtitles(segments, outBase, primary, { also, assStyle });
    const files = formats.map((fmt) => `${outBase}.${fmt}`);

    const preview = truncateHead(toTxt(segments), {
      maxLines: 120,
      maxBytes: 8 * 1024,
    });

    return {
      input: inputPath,
      files,
      segmentCount: segments.length,
      preview: preview.content,
      previewTruncated: preview.truncated,
      previewOutputLines: preview.outputLines,
      previewTotalLines: preview.totalLines,
    };
  } finally {
    if (tmpAudio) cleanupTempFile(tmpAudio);
  }
}

export default function (pi: ExtensionAPI) {
  pi.registerTool({
    name: "transcribe_media",
    label: "Transcribe media",
    description:
      "Transcribe an audio or video file to subtitles using the JianYing (CapCut) free ASR API. " +
      `Audio formats (${[...SUPPORTED_EXTS].join(", ")}) are uploaded directly; ` +
      "video files are extracted to audio first (fast lossless stream copy when possible; requires ffmpeg/ffprobe on PATH). " +
      "Runs synchronously: waits for ASR to finish, writes subtitle files next to the input (or to `output`), " +
      "and returns a transcript preview in the same tool result. Progress streams while running. " +
      "Best for Chinese/English speech; typically takes 10s–2min depending on length. " +
      "Note: audio is uploaded to ByteDance cloud servers for processing — do not use for confidential content.",
    promptSnippet:
      "Transcribe audio/video files to subtitles (srt/ass/json/txt) via JianYing free ASR",
    promptGuidelines: [
      "Call transcribe_media once per source file; it blocks until that file is done and returns the written paths.",
      "For multiple sources, call once per file (sequentially or in parallel tool calls in one turn) and only continue after every call has returned successfully.",
      "Always request formats that include \"json\" when word-level timestamps are needed (e.g. [\"json\", \"srt\"]).",
    ],
    parameters: Type.Object({
      input: Type.String({
        description: "Path to the audio or video file to transcribe",
      }),
      output: Type.Optional(
        Type.String({
          description:
            "Output path (extension optional; subtitle extensions are appended per format). " +
            "Defaults to the input path with the extension replaced.",
        }),
      ),
      formats: Type.Optional(
        Type.Array(StringEnum(FORMATS), {
          description:
            'Subtitle formats to write (default: ["srt"]). First entry is the primary format.',
        }),
      ),
      assStyle: Type.Optional(
        Type.Object({
          fontName: Type.Optional(Type.String({ description: "Font name (default: Arial)" })),
          fontSize: Type.Optional(Type.Number({ description: "Font size in points (default: 14)" })),
          primaryColour: Type.Optional(Type.String({ description: "Primary/fill colour in &HAABBGGRR format (default: &H00FFFFFF — white)" })),
          secondaryColour: Type.Optional(Type.String({ description: "Secondary/karaoke colour (default: &H000000FF)" })),
          outlineColour: Type.Optional(Type.String({ description: "Outline colour (default: &H00000000)" })),
          backColour: Type.Optional(Type.String({ description: "Back/shadow colour (default: &H80000000)" })),
          bold: Type.Optional(Type.Number({ description: "Bold: -1=true, 0=false (default: -1)" })),
          italic: Type.Optional(Type.Number({ description: "Italic: -1=true, 0=false (default: 0)" })),
          underline: Type.Optional(Type.Number({ description: "Underline: -1=true, 0=false (default: 0)" })),
          strikeOut: Type.Optional(Type.Number({ description: "StrikeOut: -1=true, 0=false (default: 0)" })),
          scaleX: Type.Optional(Type.Number({ description: "Horizontal scale % (default: 100)" })),
          scaleY: Type.Optional(Type.Number({ description: "Vertical scale % (default: 100)" })),
          spacing: Type.Optional(Type.Number({ description: "Letter spacing in pixels (default: 0)" })),
          angle: Type.Optional(Type.Number({ description: "Rotation angle in degrees (default: 0)" })),
          borderStyle: Type.Optional(Type.Number({ description: "Border style: 1=outline+shadow, 3=opaque box (default: 1)" })),
          outline: Type.Optional(Type.Number({ description: "Outline width in pixels (default: 2)" })),
          shadow: Type.Optional(Type.Number({ description: "Shadow depth in pixels (default: 2)" })),
          alignment: Type.Optional(Type.Number({ description: "Alignment 1-9: 2=bottom-centre (default: 2)" })),
          marginL: Type.Optional(Type.Number({ description: "Left margin in pixels (default: 20)" })),
          marginR: Type.Optional(Type.Number({ description: "Right margin in pixels (default: 20)" })),
          marginV: Type.Optional(Type.Number({ description: "Vertical margin in pixels (default: 20)" })),
        }),
      ),
    }),

    async execute(_toolCallId, params, signal, onUpdate, ctx) {
      const inputPath = normalizeInputPath(ctx.cwd, params.input);
      assertInputFile(inputPath);

      const formats = normalizeFormats(params.formats as SubtitleFormat[] | undefined);
      const outBase = params.output
        ? toOutputBase(path.resolve(ctx.cwd, params.output))
        : defaultOutputBase(inputPath);
      const assStyle = params.assStyle as AssStyleOptions | undefined;

      const report = (percent: number, message: string) =>
        onUpdate?.({
          content: [{ type: "text", text: `[${percent}%] ${message}` }],
          details: { progress: percent, message },
        });

      const result = await runTranscription({
        inputPath,
        outBase,
        formats,
        assStyle,
        signal,
        report,
      });

      return {
        content: [{ type: "text" as const, text: formatResult(result) }],
        details: { input: inputPath, segmentCount: result.segmentCount, files: result.files },
      };
    },
  });
}
