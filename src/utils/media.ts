import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

/**
 * Extract the audio track from a video file into a temporary WAV file.
 *
 * The output is 16-bit PCM mono at 44.1 kHz — exactly what the
 * JianYing ASR API expects.
 *
 * Requires `ffmpeg` and `ffprobe` on the system `PATH`.
 *
 * @returns Path to the temporary WAV file. The caller is responsible
 *          for removing it with {@link cleanupTempFile}.
 * @throws If the input has no audio track, ffmpeg exits non-zero,
 *         or ffmpeg / ffprobe are not installed.
 */
export async function videoToAudio(input: string): Promise<string> {
  const tmpPath = path.join(
    os.tmpdir(),
    `jianying-subtitle-${Date.now()}-${Math.random().toString(36).slice(2)}.wav`,
  );

  const hasAudio = await checkAudioTrack(input);
  if (!hasAudio) {
    throw new Error(`Input video has no audio track: ${input}`);
  }

  return new Promise<string>((resolve, reject) => {
    const proc = spawn(
      "ffmpeg",
      [
        "-y",
        "-i",
        input,
        "-vn",
        "-acodec",
        "pcm_s16le",
        "-ar",
        "44100",
        "-ac",
        "1",
        tmpPath,
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );

    let stderr = "";

    proc.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code) => {
      if (
        code !== 0 ||
        !fs.existsSync(tmpPath) ||
        fs.statSync(tmpPath).size === 0
      ) {
        cleanupTempFile(tmpPath);
        reject(
          new Error(`ffmpeg failed (exit ${code}): ${stderr.slice(-200)}`.trim()),
        );
        return;
      }
      resolve(tmpPath);
    });

    proc.on("error", (err) => {
      cleanupTempFile(tmpPath);
      reject(new Error(`ffmpeg not found: ${err.message}`));
    });
  });
}

/** Check whether a media file contains at least one audio stream. */
async function checkAudioTrack(input: string): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const proc = spawn(
      "ffprobe",
      [
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "csv=p=0",
        input,
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );

    let stdout = "";

    proc.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });

    proc.on("close", (code) => {
      resolve(code === 0 && stdout.trim() === "audio");
    });

    proc.on("error", () => resolve(false));
  });
}

/**
 * Remove a temporary file if it exists.
 * Errors (e.g. permission denied) are silently ignored.
 */
export function cleanupTempFile(tmpPath: string): void {
  try {
    if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
  } catch {
    /* best-effort */
  }
}
