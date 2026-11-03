import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { Readable } from "node:stream";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { downloadPinnedHttps, isPublicIp, validateRemoteSource } from "../server/storage/pinned-https.mjs";

const url = "https://media.example/video?token=private123";
const opts = { allowedHosts: ["media.example"], maxBytes: 16 };
const publicDns = async () => [{ address: "8.8.8.8", family: 4 }];
async function withPath(fn) {
  const dir = await mkdtemp(join(tmpdir(), "pinned-https-test-"));
  try { await fn(join(dir, "output")); } finally { await rm(dir, { recursive: true, force: true }); }
}
function requestMock({ status = 200, chunks = ["hello"], headers = {}, inspect = () => {} } = {}) {
  return (target, options, callback) => {
    inspect(target, options);
    const req = new EventEmitter();
    req.end = () => queueMicrotask(() => {
      const response = Readable.from(chunks.map(chunk => Buffer.from(chunk)));
      response.statusCode = status;
      response.headers = headers;
      callback(response);
    });
    return req;
  };
}

test("Reject private and transition addresses", () => {
  for (const ip of ["127.0.0.1", "169.254.169.254", "10.0.0.1", "100.64.1.2", "::1", "::ffff:8.8.8.8", "2002::1", "fc00::1", "fe80::1"]) assert.equal(isPublicIp(ip), false, ip);
  assert.equal(isPublicIp("8.8.8.8"), true);
  assert.equal(isPublicIp("2606:4700:4700::1111"), true);
});
test("Reject URLs outside HTTPS allowlist and IP literals", () => {
  for (const input of ["http://media.example/a", "https://127.0.0.1/a", "https://user@media.example/a", "https://media.example:444/a"]) {
    assert.throws(() => validateRemoteSource(input, opts.allowedHosts), { code: "INVALID_SOURCE_URL" });
  }
  assert.throws(() => validateRemoteSource("https://evil.example/a", opts.allowedHosts), { code: "SOURCE_HOST_NOT_ALLOWED" });
});
test("Pin approved address and preserve hostname TLS", async () => withPath(async path => {
  const requestFn = requestMock({ inspect(target, options) {
    assert.equal(target.hostname, "media.example"); assert.equal(options.servername, "media.example");
    assert.equal(options.rejectUnauthorized, true); assert.equal(options.agent, false);
    options.lookup("media.example", {}, (err, address, family) => { assert.ifError(err); assert.equal(address, "8.8.8.8"); assert.equal(family, 4); });
    options.lookup("media.example", { all: true }, (err, records) => { assert.ifError(err); assert.deepEqual(records, [{ address: "8.8.8.8", family: 4 }]); });
    options.lookup("other.example", {}, err => assert.ok(err));
  } });
  assert.deepEqual(await downloadPinnedHttps(url, path, { ...opts, lookupAll: publicDns, requestFn }), { bytes: 5 });
  assert.equal(await readFile(path, "utf8"), "hello");
}));
test("Reject any unsafe DNS answer before connecting", async () => withPath(async path => {
  let connected = false;
  await assert.rejects(downloadPinnedHttps(url, path, { ...opts,
    lookupAll: async () => [{ address: "8.8.8.8", family: 4 }, { address: "127.0.0.1", family: 4 }],
    requestFn: () => { connected = true; },
  }), { code: "SOURCE_ADDRESS_NOT_PUBLIC" });
  assert.equal(connected, false);
}));
test("Reject redirects", async () => withPath(async path => {
  await assert.rejects(downloadPinnedHttps(url, path, { ...opts, lookupAll: publicDns, requestFn: requestMock({ status: 302 }) }), { code: "SOURCE_REDIRECT_BLOCKED" });
}));
test("Enforce declared and streamed size limits", async () => withPath(async path => {
  await assert.rejects(downloadPinnedHttps(url, path, { ...opts, lookupAll: publicDns, requestFn: requestMock({ headers: { "content-length": "17" } }) }), { code: "UPLOAD_TOO_LARGE" });
  await assert.rejects(downloadPinnedHttps(url, path, { ...opts, lookupAll: publicDns, requestFn: requestMock({ chunks: ["x".repeat(17)] }) }), { code: "UPLOAD_TOO_LARGE" });
  await assert.rejects(readFile(path), { code: "ENOENT" });
}));
test("Reject empty downloads and remove partial file", async () => withPath(async path => {
  await assert.rejects(downloadPinnedHttps(url, path, { ...opts, lookupAll: publicDns, requestFn: requestMock({ chunks: [] }) }), { code: "EMPTY_UPLOAD" });
  await assert.rejects(readFile(path), { code: "ENOENT" });
}));
test("Never delete an existing destination", async () => withPath(async path => {
  await writeFile(path, "keep");
  await assert.rejects(downloadPinnedHttps(url, path, { ...opts, lookupAll: publicDns, requestFn: requestMock() }), { code: "SOURCE_DOWNLOAD_FAILED" });
  assert.equal(await readFile(path, "utf8"), "keep");
}));
test("DNS and network errors do not expose signed URLs", async () => withPath(async path => {
  await assert.rejects(downloadPinnedHttps(url, path, { ...opts, lookupAll: async () => { throw new Error(url); } }), error => error.code === "SOURCE_DNS_FAILED" && !error.message.includes("private123"));
  await assert.rejects(downloadPinnedHttps(url, path, { ...opts, lookupAll: publicDns, requestFn: () => { throw new Error(url); } }), error => error.code === "SOURCE_DOWNLOAD_FAILED" && !error.message.includes("private123"));
}));
test("Abort stalled DNS lookup", async () => withPath(async path => {
  const controller = new AbortController();
  const pending = downloadPinnedHttps(url, path, { ...opts, lookupAll: () => new Promise(() => {}), signal: controller.signal });
  controller.abort();
  await assert.rejects(pending, { code: "SOURCE_DOWNLOAD_TIMEOUT", status: 504 });
}));
