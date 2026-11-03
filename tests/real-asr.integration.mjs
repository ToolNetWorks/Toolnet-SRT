import { spawn } from "node:child_process";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const source = process.env.TOOLNET_SRT_TEST_AUDIO;
if (!source) {
  console.error("TEST_AUDIO_PATH_REQUIRED");
  process.exit(1);
}
const input = resolve(source);
const info = await stat(input);
if (!info.isFile() || info.size === 0) throw new Error("INVALID_TEST_AUDIO");
const directory = await mkdtemp(join(tmpdir(), "toolnet-srt-real-asr-"));
const audio = join(directory, "audio.mp3");
const controller = new AbortController();
const timeout = setTimeout(() => controller.abort(), 300000);

async function convertAudio() {
  await new Promise((resolveTask, rejectTask) => {
    const process = spawn("ffmpeg", [
      "-hide_banner", "-nostdin", "-loglevel", "error", "-y", "-i", input,
      "-map", "0:a:0", "-vn", "-ac", "1", "-ar", "16000", "-b:a", "48k", audio,
    ], { signal: controller.signal, stdio: ["ignore", "ignore", "pipe"] });
    process.once("error", rejectTask);
    process.once("close", code => {
      if (code !== 0) return rejectTask(new Error("FFMPEG_CONVERSION_FAILED"));
      resolveTask();
    });
  });
}

try {
  await convertAudio();
  console.log("AUDIO CONVERSION: PASS");
  const { transcribe, toSrt } = await import("../dist/index.js");
  const segments = await transcribe({ input: audio, signal: controller.signal, queryTimeoutMs: 120000 });
  if (!Array.isArray(segments) || !segments.length) throw new Error("ASR_RETURNED_NO_SEGMENTS");
  console.log("REAL ASR REQUEST: PASS");
  const valid = segments.filter(item =>
    typeof item.text === "string" && item.text.trim().length > 0 &&
    Number.isFinite(item.startMs) && Number.isFinite(item.endMs) && item.endMs > item.startMs
  );
  if (!valid.length) throw new Error("INVALID_ASR_SEGMENTS");
  console.log("VALID ASR SEGMENTS:", valid.length);
  const srt = await toSrt(valid);
  if (typeof srt !== "string" || !srt.includes("-->")) throw new Error("INVALID_SRT_OUTPUT");
  console.log("SRT GENERATION: PASS");
  console.log("SRT BYTES:", Buffer.byteLength(srt));
  console.log("TOOLNET-SRT REAL ASR TEST PASSED");
} catch (error) {
  console.error("REAL ASR FAILED:", error.code || error.message || error.name);
  process.exitCode = 1;
} finally {
  clearTimeout(timeout);
  await rm(directory, { recursive: true, force: true });
}
