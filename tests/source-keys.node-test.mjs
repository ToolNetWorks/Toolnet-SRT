import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Readable } from "node:stream";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createR2Storage } from "../server/storage/r2.mjs";
import { remoteSourceKey, validateSourceKey } from "../server/storage/source-keys.mjs";
import { createDurableWorker } from "../server/storage/durable-worker.mjs";

test("Source keys are scoped to job, attempt and filename", () => {
  const jobId = randomUUID(), token = randomUUID(), other = randomUUID();
  const key = remoteSourceKey(jobId, token, "source.mp4");
  assert.equal(validateSourceKey(jobId, "source.mp4", key), key);
  assert.equal(validateSourceKey(jobId, "source.mp4", `toolnet-srt/jobs/${jobId}/source.mp4`), `toolnet-srt/jobs/${jobId}/source.mp4`);
  for (const value of [`toolnet-srt/jobs/${other}/remote/${token}/source.mp4`,
    `${key}/extra`, `toolnet-srt/jobs/${jobId}/remote/not-a-token/source.mp4`,
    `toolnet-srt/jobs/${jobId}/remote/${token}/source.wav`]) {
    assert.throws(() => validateSourceKey(jobId, "source.mp4", value), /INVALID_SOURCE_KEY/);
  }
});

test("R2 uploads each remote attempt once and downloads the selected key", async () => {
  const dir = await mkdtemp(join(tmpdir(), "source-keys-"));
  try {
    const id = randomUUID(), token = randomUUID(), objects = new Map(), commands = [];
    const storage = createR2Storage({ env: { R2_ACCOUNT_ID: "a".repeat(32), R2_ACCESS_KEY_ID: "test",
      R2_SECRET_ACCESS_KEY: "test", R2_BUCKET: "test" }, client: {
      async send(command) {
        commands.push(command);
        if (command.constructor.name === "PutObjectCommand") {
          if (command.input.IfNoneMatch === "*" && objects.has(command.input.Key)) {
            const error = new Error("PreconditionFailed"); error.$metadata = { httpStatusCode: 412 }; throw error;
          }
          const chunks = []; for await (const chunk of command.input.Body) chunks.push(chunk);
          objects.set(command.input.Key, Buffer.concat(chunks)); return {};
        }
        if (command.constructor.name === "GetObjectCommand") return { Body: Readable.from([objects.get(command.input.Key)]) };
        throw new Error("UNEXPECTED_COMMAND");
      },
    } });
    const input = join(dir, "input"), output = join(dir, "output");
    await writeFile(input, "remote source");
    const uploaded = await storage.uploadRemoteSource({ jobId: id, fetchToken: token, filename: "source.mp4", localPath: input });
    assert.equal(uploaded.key, remoteSourceKey(id, token, "source.mp4"));
    assert.equal(commands[0].input.IfNoneMatch, "*");
    await assert.rejects(storage.uploadRemoteSource({ jobId: id, fetchToken: token, filename: "source.mp4", localPath: input }));
    await storage.downloadFile({ jobId: id, filename: "source.mp4", sourceKey: uploaded.key, localPath: output });
    assert.equal(await readFile(output, "utf8"), "remote source");
    await assert.rejects(storage.downloadFile({ jobId: randomUUID(), filename: "source.mp4", sourceKey: uploaded.key, localPath: output }), /INVALID_SOURCE_KEY/);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Durable worker forwards remote source key and rejects mismatches", async () => {
  const id = randomUUID(), fetchToken = randomUUID(), leaseToken = randomUUID();
  const key = remoteSourceKey(id, fetchToken, "source.mp4");
  const downloaded = [];
  let currentKey = key;
  const worker = createDurableWorker({
    leases: { async claimJob() { return { id, lease_token: leaseToken, extension: ".mp4", source_key: currentKey }; },
      async heartbeatJob() { return { id }; }, async completeJob() { return { status: "completed" }; },
      async failJob() { return { status: "failed" }; } },
    storage: { async downloadFile(args) { downloaded.push(args.sourceKey); } },
    results: { async putResult() { return { key: `toolnet-srt/jobs/${id}/attempts/${leaseToken}/result.srt` }; } },
    split: async () => [{ path: "mock", startMs: 0 }], transcribe: async () => [{ text: "test", startMs: 0, endMs: 10 }],
    toSrt: () => "subtitle",
  });
  assert.equal((await worker.runOnce()).status, "completed");
  assert.deepEqual(downloaded, [key]);
  currentKey = remoteSourceKey(randomUUID(), fetchToken, "source.mp4");
  assert.equal((await worker.runOnce()).status, "failed");
  assert.equal(downloaded.length, 1);
});
