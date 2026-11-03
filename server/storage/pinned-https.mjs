import { lookup as dnsLookup } from "node:dns/promises";
import { request as httpsRequest } from "node:https";
import { BlockList, isIP } from "node:net";
import { open, rm } from "node:fs/promises";
import { pipeline } from "node:stream/promises";
import { Transform } from "node:stream";
import { JobError } from "../job-utils.mjs";

const denied4 = new BlockList();
const denied6 = new BlockList();
for (const [network, bits] of [
  ["0.0.0.0", 8], ["10.0.0.0", 8], ["100.64.0.0", 10], ["127.0.0.0", 8],
  ["169.254.0.0", 16], ["172.16.0.0", 12], ["192.0.0.0", 24], ["192.0.2.0", 24],
  ["192.88.99.0", 24], ["192.168.0.0", 16], ["198.18.0.0", 15],
  ["198.51.100.0", 24], ["203.0.113.0", 24], ["224.0.0.0", 4], ["240.0.0.0", 4],
]) denied4.addSubnet(network, bits, "ipv4");
for (const [network, bits] of [
  ["::", 96], ["::ffff:0:0", 96], ["64:ff9b::", 96], ["64:ff9b:1::", 48], ["100::", 64],
  ["2001::", 23], ["2001:db8::", 32], ["2002::", 16], ["3fff::", 20], ["fc00::", 7],
  ["fe80::", 10], ["ff00::", 8],
]) denied6.addSubnet(network, bits, "ipv6");

export function isPublicIp(address) {
  const family = isIP(address);
  if (!family) return false;
  return family === 4 ? !denied4.check(address, "ipv4") : !denied6.check(address, "ipv6");
}

export function validateRemoteSource(rawUrl, allowedHosts) {
  if (!Array.isArray(allowedHosts) || !allowedHosts.length) throw new JobError("REMOTE_URL_DISABLED", 503);
  let url;
  try { url = new URL(rawUrl); } catch { throw new JobError("INVALID_SOURCE_URL", 400); }
  if (url.protocol !== "https:" || url.username || url.password || (url.port && url.port !== "443") ||
      !url.hostname || isIP(url.hostname.replace(/^\[|\]$/g, ""))) throw new JobError("INVALID_SOURCE_URL", 400);
  if (!allowedHosts.some(host => typeof host === "string" && host.toLowerCase() === url.hostname.toLowerCase())) {
    throw new JobError("SOURCE_HOST_NOT_ALLOWED", 403);
  }
  return url;
}

async function resolveAbortably(hostname, lookupAll, signal) {
  if (signal?.aborted) throw new JobError("SOURCE_DOWNLOAD_TIMEOUT", 504);
  return new Promise((resolve, reject) => {
    const cleanup = () => signal?.removeEventListener("abort", onAbort);
    const onAbort = () => { cleanup(); reject(new JobError("SOURCE_DOWNLOAD_TIMEOUT", 504)); };
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) { onAbort(); return; }
    Promise.resolve().then(() => lookupAll(hostname, { all: true, verbatim: true })).then(
      value => { cleanup(); resolve(value); },
      () => { cleanup(); reject(new JobError("SOURCE_DNS_FAILED", 400)); },
    );
  });
}

export async function downloadPinnedHttps(rawUrl, destination, {
  allowedHosts, maxBytes, signal, lookupAll = dnsLookup, requestFn = httpsRequest,
} = {}) {
  const url = validateRemoteSource(rawUrl, allowedHosts);
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) throw new Error("INVALID_MAX_BYTES");
  const records = await resolveAbortably(url.hostname, lookupAll, signal);
  if (!Array.isArray(records) || !records.length || records.length > 64) throw new JobError("SOURCE_DNS_FAILED", 400);
  if (records.some(record => !record || ![4, 6].includes(record.family) ||
      isIP(record.address) !== record.family || !isPublicIp(record.address))) {
    throw new JobError("SOURCE_ADDRESS_NOT_PUBLIC", 403);
  }
  if (signal?.aborted) throw new JobError("SOURCE_DOWNLOAD_TIMEOUT", 504);
  const chosen = records[0];
  const pinnedLookup = (hostname, options, callback) => {
    if (hostname !== url.hostname) return callback(new Error("PINNED_HOST_MISMATCH"));
    if (options?.all) return callback(null, [chosen]);
    callback(null, chosen.address, chosen.family);
  };
  let created = false;
  try {
    return await new Promise((resolve, reject) => {
      try {
        let activeResponse = null;
        const request = requestFn(url, {
          method: "GET", agent: false, servername: url.hostname, rejectUnauthorized: true,
          lookup: pinnedLookup, signal, headers: { "User-Agent": "ToolNet-SRT/0.3", Accept: "*/*" },
        }, response => {
          activeResponse = response;
          void (async () => {
            if (response.statusCode >= 300 && response.statusCode < 400) throw new JobError("SOURCE_REDIRECT_BLOCKED", 403);
            if (response.statusCode >= 400 && response.statusCode < 500) throw new JobError("SOURCE_HTTP_CLIENT_ERROR", 400);
            if (response.statusCode !== 200) throw new JobError("SOURCE_HTTP_STATUS", 502);
            const length = response.headers["content-length"];
            if (length !== undefined && (!/^\d+$/.test(String(length)) || Number(length) > maxBytes)) {
              throw new JobError("UPLOAD_TOO_LARGE", 413);
            }
            let received = 0;
            const limiter = new Transform({ transform(chunk, encoding, callback) {
              received += chunk.length;
              if (received > maxBytes) return callback(new JobError("UPLOAD_TOO_LARGE", 413));
              callback(null, chunk);
            } });
            const file = await open(destination, "wx", 0o600);
            created = true;
            await pipeline(response, limiter, file.createWriteStream(), { signal });
            if (received === 0) throw new JobError("EMPTY_UPLOAD", 400);
            resolve({ bytes: received });
          })().catch(error => { response.destroy(); reject(error); });
        });
        request.once("error", error => {
          if (activeResponse) activeResponse.destroy(error);
          else reject(error);
        });
        request.end();
      } catch (error) { reject(error); }
    });
  } catch (error) {
    // Only remove files created by this invocation; an existing file is never deleted.
    if (created) await rm(destination, { force: true }).catch(() => {});
    if (error instanceof JobError) throw error;
    if (signal?.aborted) throw new JobError("SOURCE_DOWNLOAD_TIMEOUT", 504);
    throw new JobError("SOURCE_DOWNLOAD_FAILED", 502);
  }
}
