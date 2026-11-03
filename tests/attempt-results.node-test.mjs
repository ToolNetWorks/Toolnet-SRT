import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Readable } from "node:stream";
import { attemptResultKey, createAttemptResultStorage } from "../server/storage/attempt-results.mjs";

const env = { R2_ACCOUNT_ID: "a".repeat(32), R2_ACCESS_KEY_ID: "mock", R2_SECRET_ACCESS_KEY: "mock", R2_BUCKET: "mock-bucket" };
function setup() {
  const objects = new Map();
  const client = {
    async send(command) {
      const { Key, Body, IfNoneMatch } = command.input;
      if (command.constructor.name === "PutObjectCommand") {
        assert.equal(IfNoneMatch, "*");
        if (objects.has(Key)) {
          const error = new Error("PreconditionFailed");
          error.$metadata = { httpStatusCode: 412 };
          throw error;
        }
        objects.set(Key, Buffer.from(Body));
        return {};
      }
      if (command.constructor.name === "GetObjectCommand") {
        const value = objects.get(Key);
        if (!value) throw new Error("OBJECT_NOT_FOUND");
        return { ContentLength: value.length, Body: Readable.from([value]) };
      }
      throw new Error("UNEXPECTED_COMMAND");
    },
  };
  return { storage: createAttemptResultStorage({ env, client }), objects };
}

test("SRT upload and download", async () => {
  const { storage } = setup();
  const jobId = randomUUID(), leaseToken = randomUUID();
  const uploaded = await storage.putResult({ jobId, leaseToken, srt: "1\n00:00:00,000 --> 00:00:01,000\nHello\n" });
  assert.equal(uploaded.key, attemptResultKey(jobId, leaseToken));
  const downloaded = await storage.getResult({ key: uploaded.key });
  assert.match(downloaded.toString("utf8"), /Hello/);
});

test("Same attempt cannot overwrite SRT", async () => {
  const { storage } = setup();
  const jobId = randomUUID(), leaseToken = randomUUID();
  const args = { jobId, leaseToken, srt: "FIRST" };
  await storage.putResult(args);
  await assert.rejects(storage.putResult({ ...args, srt: "OVERWRITTEN" }), /R2_ATTEMPT_RESULT_ALREADY_EXISTS/);
  const data = await storage.getResult({ key: attemptResultKey(jobId, leaseToken) });
  assert.equal(data.toString(), "FIRST");
});

test("New lease uses a different key", async () => {
  const { storage, objects } = setup();
  const jobId = randomUUID();
  const first = await storage.putResult({ jobId, leaseToken: randomUUID(), srt: "OLD" });
  const second = await storage.putResult({ jobId, leaseToken: randomUUID(), srt: "NEW" });
  assert.notEqual(first.key, second.key);
  assert.equal(objects.size, 2);
});

test("Reject invalid result keys", async () => {
  const { storage } = setup();
  await assert.rejects(storage.getResult({ key: "../private/secret.env" }), /INVALID_RESULT_KEY/);
});

test("Reject empty result", async () => {
  const { storage } = setup();
  await assert.rejects(storage.putResult({ jobId: randomUUID(), leaseToken: randomUUID(), srt: "" }), /INVALID_SRT_SIZE/);
});
