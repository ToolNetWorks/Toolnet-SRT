/**
 * pi extension: thin wrapper around the jianying-subtitle library.
 *
 * Registers tools that the LLM can call to transcribe audio/video files into
 * subtitles using the JianYing (CapCut) free ASR API.
 *
 * All heavy lifting is delegated to the existing library code in ../src — this
 * file only adapts it to pi's tool interface and adds an in-memory background
 * job layer for long-running transcription.
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
const MAX_JOB_HISTORY = 50;

type TranscriptionResult = {
  input: string;
  files: string[];
  segmentCount: number;
  preview: string;
  previewTruncated: boolean;
  previewOutputLines?: number;
  previewTotalLines?: number;
};

type JobStatus = "running" | "done" | "failed" | "cancelled";

type TranscriptionJob = {
  id: string;
  status: JobStatus;
  input: string;
  outputBase: string;
  formats: SubtitleFormat[];
  createdAt: number;
  updatedAt: number;
  progress: number;
  message: string;
  controller: AbortController;
  result?: TranscriptionResult;
  error?: string;
  promise: Promise<void>;
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

function isCancelled(err: unknown): boolean {
  return err instanceof Error && err.message === "Cancelled";
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

function formatJob(job: TranscriptionJob): string {
  const lines = [
    `Job: ${job.id}`,
    `Status: ${job.status}`,
    `Progress: ${job.progress}% — ${job.message}`,
    `Input: ${job.input}`,
    `Output base: ${job.outputBase}`,
    `Formats: ${job.formats.join(", ")}`,
  ];

  if (job.result) lines.push("", formatResult(job.result));
  if (job.error) lines.push(`Error: ${job.error}`);
  return lines.join("\n");
}

function summarizeJobs(jobs: Map<string, TranscriptionJob>): string {
  if (jobs.size === 0) return "No transcription jobs in memory.";
  return [...jobs.values()]
    .sort((a, b) => b.createdAt - a.createdAt)
    .map((job) => `${job.id}\t${job.status}\t${job.progress}%\t${job.message}\t${job.input}`)
    .join("\n");
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
  const jobs = new Map<string, TranscriptionJob>();
  let nextJobSeq = 1;

  const pruneJobs = () => {
    const finished = [...jobs.values()]
      .filter((job) => job.status !== "running")
      .sort((a, b) => b.updatedAt - a.updatedAt);
    for (const job of finished.slice(MAX_JOB_HISTORY)) {
      jobs.delete(job.id);
    }
  };

  pi.on("session_shutdown", () => {
    for (const job of jobs.values()) {
      if (job.status === "running") {
        job.controller.abort();
      }
    }
  });

  pi.registerTool({
    name: "transcribe_media",
    label: "Transcribe media",
    description:
      "Transcribe an audio or video file to subtitles using the JianYing (CapCut) free ASR API. " +
      `Audio formats (${[...SUPPORTED_EXTS].join(", ")}) are uploaded directly; ` +
      "video files are extracted to audio first (fast lossless stream copy when possible; requires ffmpeg/ffprobe on PATH). " +
      "Writes subtitle files next to the input (or to `output`) and returns a transcript preview. " +
      "Set `background: true` for long media or when the user asks to run asynchronously; then use transcribe_media_status to check progress. " +
      "Best for Chinese/English speech; typically takes 10s–2min depending on length. " +
      "Note: audio is uploaded to ByteDance cloud servers for processing — do not use for confidential content.",
    promptSnippet:
      "Transcribe audio/video files to subtitles (srt/ass/json/txt) via JianYing free ASR",
    promptGuidelines: [
      "Use transcribe_media with background: true for long media or when the user asks to run transcription asynchronously, then call transcribe_media_status with the returned jobId.",
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
      background: Type.Optional(
        Type.Boolean({
          description:
            "Run as an in-memory background job and return immediately. Use transcribe_media_status to poll progress/result.",
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

      if (params.background) {
        const controller = new AbortController();
        const jobId = `jy_${Date.now().toString(36)}_${nextJobSeq++}`;
        const job: TranscriptionJob = {
          id: jobId,
          status: "running",
          input: inputPath,
          outputBase: outBase,
          formats,
          createdAt: Date.now(),
          updatedAt: Date.now(),
          progress: 0,
          message: "Queued",
          controller,
          promise: Promise.resolve(),
        };
        jobs.set(jobId, job);

        const report = (progress: number, message: string) => {
          job.progress = progress;
          job.message = message;
          job.updatedAt = Date.now();
        };

        job.promise = runTranscription({
          inputPath,
          outBase,
          formats,
          assStyle,
          signal: controller.signal,
          report,
        })
          .then((result) => {
            job.status = "done";
            job.progress = 100;
            job.message = result.segmentCount === 0 ? "No speech detected" : "Done";
            job.result = result;
            job.updatedAt = Date.now();
            if (ctx.hasUI && !controller.signal.aborted) {
              ctx.ui.notify(`Transcription job ${job.id} finished`, "info");
            }
          })
          .catch((err: unknown) => {
            job.status = isCancelled(err) || controller.signal.aborted ? "cancelled" : "failed";
            job.message = job.status === "cancelled" ? "Cancelled" : "Failed";
            job.error = err instanceof Error ? err.message : String(err);
            job.updatedAt = Date.now();
            if (ctx.hasUI && job.status === "failed") {
              ctx.ui.notify(`Transcription job ${job.id} failed: ${job.error}`, "error");
            }
          })
          .finally(pruneJobs);

        return {
          content: [
            {
              type: "text" as const,
              text:
                `Started background transcription job: ${jobId}\n` +
                `Input: ${inputPath}\n` +
                `Output base: ${outBase}\n` +
                `Formats: ${formats.join(", ")}\n\n` +
                `Use transcribe_media_status({ jobId: "${jobId}" }) to check progress/results, or transcribe_media_cancel({ jobId: "${jobId}" }) to cancel.\n` +
                `Note: jobs are in-memory and are cancelled on pi session shutdown/reload.`,
            },
          ],
          details: { jobId, status: job.status, input: inputPath, outputBase: outBase, formats },
        };
      }

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

  pi.registerTool({
    name: "transcribe_media_status",
    label: "Transcription status",
    description:
      "Check the progress/result of a transcribe_media background job. If jobId is omitted, list recent in-memory jobs.",
    promptSnippet: "Check background transcription job progress/result",
    parameters: Type.Object({
      jobId: Type.Optional(Type.String({ description: "Background job id returned by transcribe_media" })),
    }),
    async execute(_toolCallId, params) {
      if (!params.jobId) {
        return {
          content: [{ type: "text" as const, text: summarizeJobs(jobs) }],
          details: { jobs: [...jobs.values()].map((job) => ({ id: job.id, status: job.status, progress: job.progress })) },
          terminate: true,
        };
      }

      const job = jobs.get(params.jobId);
      if (!job) throw new Error(`Unknown transcription job: ${params.jobId}`);

      return {
        content: [{ type: "text" as const, text: formatJob(job) }],
        details: {
          jobId: job.id,
          status: job.status,
          progress: job.progress,
          message: job.message,
          input: job.input,
          outputBase: job.outputBase,
          files: job.result?.files ?? [],
          error: job.error,
        },
        terminate: job.status !== "running",
      };
    },
  });

  pi.registerTool({
    name: "transcribe_media_cancel",
    label: "Cancel transcription",
    description: "Cancel a running transcribe_media background job.",
    promptSnippet: "Cancel background transcription job",
    parameters: Type.Object({
      jobId: Type.String({ description: "Background job id returned by transcribe_media" }),
    }),
    async execute(_toolCallId, params) {
      const job = jobs.get(params.jobId);
      if (!job) throw new Error(`Unknown transcription job: ${params.jobId}`);
      if (job.status !== "running") {
        return {
          content: [{ type: "text" as const, text: `Job ${job.id} is already ${job.status}.` }],
          details: { jobId: job.id, status: job.status },
          terminate: true,
        };
      }

      job.controller.abort();
      job.status = "cancelled";
      job.progress = Math.min(job.progress, 99);
      job.message = "Cancellation requested";
      job.updatedAt = Date.now();

      return {
        content: [{ type: "text" as const, text: `Cancellation requested for job ${job.id}.` }],
        details: { jobId: job.id, status: job.status },
        terminate: true,
      };
    },
  });
}
