import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

/** Extract audio from a video file to a temporary WAV file. Returns the temp path. */
export async function videoToAudio(input: string): Promise<string> {
  const tmpPath = path.join(
    os.tmpdir(),
    `jianying-subtitle-${Date.now()}-${Math.random().toString(36).slice(2)}.wav`,
  );

  // Check if input has audio
  const hasAudio = await checkAudioTrack(input);
  if (!hasAudio) {
    throw new Error(`Input video has no audio track: ${input}`);
  }

  return new Promise<string>((resolve, reject) => {
    const proc = spawn("ffmpeg", [
      "-y",
      "-i", input,
      "-vn",                 // no video
      "-acodec", "pcm_s16le", // 16-bit PCM
      "-ar", "44100",        // 44100 Hz sample rate
      "-ac", "1",            // mono
      tmpPath,
    ], { stdio: ["ignore", "pipe", "pipe"] });

    let stderr = "";

    proc.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code) => {
      if (code === 0 && fs.existsSync(tmpPath) && fs.statSync(tmpPath).size > 0) {
        resolve(tmpPath);
      } else {
        reject(new Error(`ffmpeg failed (exit ${code}): ${stderr.slice(-200)}`));
      }
    });

    proc.on("error", (err) => {
      reject(new Error(`ffmpeg not found or failed to start: ${err.message}`));
    });
  });
}

/** Check if a video file has an audio track. */
async function checkAudioTrack(input: string): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const proc = spawn("ffprobe", [
      "-v", "error",
      "-select_streams", "a:0",
      "-show_entries", "stream=codec_type",
      "-of", "csv=p=0",
      input,
    ], { stdio: ["ignore", "pipe", "pipe"] });

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

/** Clean up a temp file if it exists. */
export function cleanupTempFile(tmpPath: string): void {
  try {
    if (fs.existsSync(tmpPath)) {
      fs.unlinkSync(tmpPath);
    }
  } catch {
    // ignore
  }
}
