import * as fs from "node:fs";
import * as path from "node:path";
import { crc32 } from "../utils/crypto.js";
import type { SubtitleSegment, TranscribeOptions } from "./types.js";
import { SUPPORTED_EXTS } from "./constants.js";
import { generateTdid } from "./helpers.js";
import {
  uploadSign,
  uploadAuth,
  uploadFile,
  uploadCheck,
  uploadCommit,
  type UploadPipelineContext,
  type UploadState,
} from "./upload.js";
import { submitTask, pollForResult, parseSegments } from "./transcribe.js";

export type {
  JianYingResponse,
  SubtitleSegment,
  SubtitleWord,
  TranscribeOptions,
} from "./types.js";

/**
 * JianYing (CapCut) ASR client.
 *
 * Orchestrates the full pipeline:
 * upload → submit → poll → parse.
 */
export class JianYingASR {
  private tdid: string;
  private crc32Hex!: string;
  private fileBuffer!: Buffer;
  private fileSize!: number;

  constructor(private options: TranscribeOptions) {
    this.tdid = generateTdid();
  }

  /**
   * Wrapped `fetch` that checks the HTTP status code without exposing
   * signed URLs or upstream response bodies in errors.
   */
  private checkedFetch = async (
    url: string,
    init: RequestInit,
  ): Promise<Response> => {
    if (this.options.signal?.aborted) throw new Error("Cancelled");
    const res = await fetch(url, { ...init, signal: this.options.signal });
    if (!res.ok) {
      const error = new Error(`HTTP ${res.status} from ASR service`) as Error & {
        status: number; retryAfterMs?: number;
      };
      error.status = res.status;
      const retryAfter = res.headers.get('retry-after');
      if (res.status === 429 && retryAfter) {
        const seconds = /^\d+$/.test(retryAfter) ? Number(retryAfter) : NaN;
        const timestamp = Number.isFinite(seconds) ? NaN : Date.parse(retryAfter);
        const delay = Number.isFinite(seconds) ? seconds * 1000 : timestamp - Date.now();
        if (Number.isSafeInteger(delay) && delay >= 0) error.retryAfterMs = delay;
      }
      throw error;
    }
    return res;
  };

  /** Read the audio file from disk and compute its CRC32 checksum. */
  private loadFile(): void {
    const { input } = this.options;
    const stat = fs.statSync(input);
    if (!stat.isFile()) {
      throw new Error(`File not found: ${input}`);
    }

    const ext = path.extname(input).slice(1).toLowerCase();
    if (!SUPPORTED_EXTS.has(ext)) {
      throw new Error(
        `Unsupported format: .${ext}. Supported: ${[...SUPPORTED_EXTS].join(", ")}`,
      );
    }

    this.fileBuffer = fs.readFileSync(input);
    this.fileSize = stat.size;
    this.crc32Hex = crc32(this.fileBuffer);
  }

  private get ctx(): UploadPipelineContext {
    return {
      tdid: this.tdid,
      crc32Hex: this.crc32Hex,
      fileBuffer: this.fileBuffer,
      fileSize: this.fileSize,
      checkedFetch: this.checkedFetch,
    };
  }

  /**
   * Run the full ASR pipeline and return timestamped segments.
   */
  async transcribe(): Promise<SubtitleSegment[]> {
    const { onProgress } = this.options;

    if (this.options.signal?.aborted) throw new Error("Cancelled");

    this.loadFile();

    onProgress?.(20, "Requesting upload credentials...");
    const creds = await uploadSign(this.ctx);

    onProgress?.(30, "Authorizing upload...");
    const dest = await uploadAuth(this.ctx, creds);

    const st: UploadState = { ...creds, ...dest };

    onProgress?.(40, "Uploading audio...");
    await uploadFile(this.ctx, st);

    onProgress?.(50, "Verifying upload...");
    await uploadCheck(this.ctx, st);
    await uploadCommit(this.ctx, st);

    onProgress?.(60, "Submitting transcription task...");
    const taskId = await submitTask(st.storeUri, this.tdid, this.checkedFetch);

    onProgress?.(70, "Waiting for result...");
    const resp = await pollForResult(
      taskId,
      this.tdid,
      this.checkedFetch,
      this.options,
    );

    onProgress?.(90, "Parsing result...");
    const segments = parseSegments(resp);

    onProgress?.(100, "Done");
    return segments;
  }
}

/**
 * Convenience function: transcribe an audio file and return segments.
 */
export async function transcribe(
  options: TranscribeOptions,
): Promise<SubtitleSegment[]> {
  return new JianYingASR(options).transcribe();
}
