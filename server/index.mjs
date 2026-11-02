import { createServer } from "node:http";
import { createHash, timingSafeEqual } from "node:crypto";
import { spawn } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { JobManager } from "./jobs.mjs";
import { createExtendedRoutes } from "./extended-routes.mjs";

// Load ASR only when a transcription request needs it.
let asrModulePromise;

function getAsrModule() {
  if (asrModulePromise) return asrModulePromise;

  asrModulePromise = import("../dist/index.js")
    .catch(error => {
      asrModulePromise = undefined;
      throw error;
    });

  return asrModulePromise;
}

const PORT = Number(process.env.PORT || 10000);
const API_KEY = process.env.TOOLNET_SRT_API_KEY || "";
const MAX_UPLOAD = 40 * 1024 * 1024;
const MAX_AUDIO = 20 * 1024 * 1024;

const formats = new Map([
  ["video/mp4", ".mp4"],
  ["audio/mpeg", ".mp3"],
  ["audio/wav", ".wav"],
  ["audio/x-wav", ".wav"],
  ["audio/mp4", ".m4a"],
  ["audio/flac", ".flac"],
]);

let busy = false;
const jobManager = new JobManager({ canRun: () => !busy });
const extendedRoutes = createExtendedRoutes({
  manager: jobManager,
  authenticate,
  apiKeyPresent: () => Boolean(API_KEY),
});

function authenticate(value) {
  if (!API_KEY || typeof value !== "string") return false;

  const digest = text =>
    createHash("sha256").update(text).digest();

  return timingSafeEqual(digest(value), digest(API_KEY));
}

function json(res, status, message) {
  if (res.destroyed || res.headersSent) return;

  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });

  res.end(JSON.stringify({ message }));
}

async function saveUpload(req, destination) {
  let received = 0;

  const limiter = new Transform({
    transform(chunk, encoding, callback) {
      received += chunk.length;

      if (received > MAX_UPLOAD) {
        return callback(new Error("UPLOAD_TOO_LARGE"));
      }

      callback(null, chunk);
    },
  });

  await pipeline(
    req,
    limiter,
    createWriteStream(destination, { mode: 0o600 }),
  );

  if (received === 0) {
    throw new Error("EMPTY_UPLOAD");
  }
}

async function extractAudio(input, output, signal) {
  const args = [
    "-hide_banner",
    "-nostdin",
    "-v", "error",
    "-y",
    "-i", input,
    "-map", "0:a:0",
    "-vn",
    "-ac", "1",
    "-ar", "16000",
    "-b:a", "48k",
    "-f", "mp3",
    output,
  ];

  await new Promise((resolve, reject) => {
    const process = spawn("ffmpeg", args, {
      stdio: ["ignore", "ignore", "pipe"],
    });

    let stderr = "";
    let finished = false;

    const timeout = setTimeout(() => {
      process.kill("SIGKILL");
    }, 120000);

    const stop = () => process.kill("SIGTERM");
    signal.addEventListener("abort", stop, { once: true });

    const finish = error => {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);
      signal.removeEventListener("abort", stop);

      if (error) return reject(error);
      resolve();
    };

    process.stderr.on("data", chunk => {
      stderr = (stderr + chunk.toString()).slice(-1000);
    });

    process.on("error", finish);

    process.on("close", code => {
      if (code !== 0) {
        return finish(new Error("FFMPEG_FAILED"));
      }

      finish();
    });
  });

  const info = await stat(output);

  if (info.size === 0 || info.size > MAX_AUDIO) {
    throw new Error("INVALID_AUDIO_SIZE");
  }
}

async function handleTranscribe(req, res) {
  if (!API_KEY) {
    return json(res, 503, "API key not configured");
  }

  if (!authenticate(req.headers["x-api-key"])) {
    return json(res, 401, "Unauthorized");
  }

  if (busy || jobManager.isBusy()) {
    return json(res, 429, "Worker busy");
  }

  const mime = String(req.headers["content-type"] || "")
    .split(";")[0].trim().toLowerCase();

  const extension = formats.get(mime);

  if (!extension) {
    return json(res, 415, "Unsupported media type");
  }

  const length = req.headers["content-length"];

  if (length !== undefined) {
    const size = Number(length);

    if (!Number.isSafeInteger(size) || size > MAX_UPLOAD) {
      return json(res, 413, "Upload too large");
    }
  }

  busy = true;

  const controller = new AbortController();
  const onClose = () => {
    if (!res.writableEnded) controller.abort();
  };

  res.once("close", onClose);

  let directory;

  try {
    directory = await mkdtemp(join(tmpdir(), "toolnet-srt-"));

    const input = join(directory, `input${extension}`);
    const audio = join(directory, "audio.mp3");

    await saveUpload(req, input);
    await extractAudio(input, audio, controller.signal);

    const { transcribe, toSrt } = await getAsrModule();

    const segments = await transcribe({
      input: audio,
      signal: controller.signal,
      queryTimeoutMs: 300000,
    });

    if (!segments.length) {
      throw new Error("NO_SPEECH_DETECTED");
    }

    const output = Buffer.from(toSrt(segments), "utf8");

    if (controller.signal.aborted) return;

    res.writeHead(200, {
      "Content-Type": "application/x-subrip; charset=utf-8",
      "Content-Disposition": 'attachment; filename="subtitle.srt"',
      "Content-Length": output.length,
      "Cache-Control": "no-store",
    });

    res.end(output);
    console.log("Transcription completed");

  } catch (error) {
    console.error("Transcription error:", error.name);

    const status =
      error.message === "UPLOAD_TOO_LARGE" ? 413 :
      error.message === "EMPTY_UPLOAD" ? 400 : 502;

    json(res, status, "Transcription failed");

  } finally {
    busy = false;
    jobManager.kick();
    res.off("close", onClose);

    if (directory) {
      await rm(directory, {
        recursive: true,
        force: true,
      }).catch(() => {});
    }
  }
}

const server = createServer((req, res) => {
  const path = new URL(
    req.url || "/", "http://localhost"
  ).pathname;

  if (extendedRoutes.isExtended(path)) {
    void extendedRoutes.handle(req, res, path);
    return;
  }

  if (req.method === "GET" && path === "/api/health") {
    res.writeHead(200, {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    });

    res.end("OK");
    return;
  }

  if (req.method === "GET" && path === "/health") {
    return json(res, 200, "ToolNet-SRT ready");
  }

  if (req.method === "POST" && path === "/v1/transcribe") {
    void handleTranscribe(req, res);
    return;
  }

  json(res, 404, "Not found");
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ToolNet-SRT listening on ${PORT}`);
});
