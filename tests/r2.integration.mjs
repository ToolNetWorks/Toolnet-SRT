import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createR2Storage, isR2Configured } from "../server/storage/r2.mjs";

if (!isR2Configured()) {
  console.error("R2_ENV_MISSING");
  process.exit(1);
}

const jobId = randomUUID();
const filename = "result.srt";
const content = Buffer.from(
  "1\n" + "00:00:00,000 --> 00:00:01,000\n" + "ToolNet-SRT R2 Integration Test\n",
  "utf8",
);
function sha256(data) {
  return createHash("sha256").update(data).digest("hex");
}
const directory = await mkdtemp(join(tmpdir(), "toolnet-srt-r2-"));
const sourcePath = join(directory, "result.srt");
const downloadPath = join(directory, "downloaded.srt");
const storage = createR2Storage();
let uploadAttempted = false;
let passed = false;

try {
  await writeFile(sourcePath, content);
  console.log("R2 CLIENT: READY");
  uploadAttempted = true;
  const uploaded = await storage.uploadFile({ jobId, filename, localPath: sourcePath, contentType: "application/x-subrip" });
  assert.equal(uploaded.size, content.length);
  console.log("R2 UPLOAD: PASS");
  const metadata = await storage.getFileInfo({ jobId, filename });
  assert.ok(metadata);
  assert.equal(metadata.size, content.length);
  console.log("R2 HEAD OBJECT: PASS");
  await storage.downloadFile({ jobId, filename, localPath: downloadPath });
  const downloaded = await readFile(downloadPath);
  assert.equal(sha256(downloaded), sha256(content));
  console.log("R2 DOWNLOAD: PASS");
  console.log("R2 SHA256 VERIFY: PASS");
  passed = true;
} catch (error) {
  console.error("R2 TEST FAILED:", error.code || error.name || "UNKNOWN_ERROR");
  process.exitCode = 1;
} finally {
  if (uploadAttempted) {
    try {
      await storage.deleteFile({ jobId, filename });
      const remaining = await storage.getFileInfo({ jobId, filename });
      assert.equal(remaining, null);
      console.log("R2 DELETE: PASS");
      console.log("R2 CLEANUP: PASS");
    } catch (error) {
      console.error("R2 CLEANUP FAILED:", error.code || error.name || "UNKNOWN_ERROR");
      console.error("TEST JOB ID:", jobId);
      process.exitCode = 1;
      passed = false;
    }
  }
  await rm(directory, { recursive: true, force: true });
}
if (passed) console.log("TOOLNET-SRT R2 INTEGRATION TEST PASSED");
