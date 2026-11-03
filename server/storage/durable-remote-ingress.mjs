import { createReadStream } from "node:fs";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JobError } from "../job-utils.mjs";
import { validateRemoteSource, downloadPinnedHttps } from "./pinned-https.mjs";

const MAX_ALLOWED_BYTES = 2048 * 1024 * 1024;

export function createDurableRemoteIngress({ ingress, allowedHosts = [], maxBytes = 256 * 1024 * 1024,
  downloadTimeoutMs = 120000, validate = validateRemoteSource, download = downloadPinnedHttps } = {}) {
  if (typeof ingress?.createUpload !== "function") throw new Error("DURABLE_INGRESS_REQUIRED");
  if (!Array.isArray(allowedHosts) || allowedHosts.some(host => typeof host !== "string")) throw new Error("INVALID_ALLOWED_HOSTS");
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_ALLOWED_BYTES) throw new Error("INVALID_MAX_BYTES");
  if (!Number.isSafeInteger(downloadTimeoutMs) || downloadTimeoutMs < 1000 || downloadTimeoutMs > 300000) throw new Error("INVALID_DOWNLOAD_TIMEOUT");
  if (typeof validate !== "function" || typeof download !== "function") throw new Error("INVALID_REMOTE_DEPENDENCY");
  const hosts = [...new Set(allowedHosts.map(host => host.trim().toLowerCase()).filter(Boolean))];

  async function createRemote(rawUrl) {
    if (!hosts.length) throw new JobError("REMOTE_URL_DISABLED", 503);
    const verified = await validate(rawUrl, hosts);
    if (!(verified instanceof URL) || verified.protocol !== "https:" || !hosts.includes(verified.hostname.toLowerCase())) {
      throw new JobError("INVALID_SOURCE_URL", 400);
    }
    const directory = await mkdtemp(join(tmpdir(), "toolnet-srt-remote-"));
    const localPath = join(directory, "source.mp4");
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), downloadTimeoutMs);
    timer.unref?.();
    let source = null;
    try {
      await download(verified.toString(), localPath, { allowedHosts: hosts, maxBytes, signal: controller.signal });
      clearTimeout(timer);
      const info = await stat(localPath);
      if (!info.isFile() || info.size === 0) throw new JobError("EMPTY_UPLOAD", 400);
      if (info.size > maxBytes) throw new JobError("UPLOAD_TOO_LARGE", 413);
      source = createReadStream(localPath);
      source.headers = { "content-length": String(info.size) };
      return await ingress.createUpload(source, "video/mp4");
    } catch (error) {
      if (controller.signal.aborted) throw new JobError("SOURCE_DOWNLOAD_TIMEOUT", 504);
      throw error;
    } finally {
      clearTimeout(timer);
      source?.destroy();
      await rm(directory, { recursive: true, force: true, maxRetries: 2, retryDelay: 100 });
    }
  }

  return { createRemote };
}
