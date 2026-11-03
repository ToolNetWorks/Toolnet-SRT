import test from "node:test";
import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { createRemoteUrlVault } from "../server/storage/remote-url-vault.mjs";

const key = randomBytes(32).toString("base64url");
const url = "https://media.example/video.mp4?token=secret123";
const id = randomUUID();

test("Encrypt and decrypt signed HTTPS URL", () => {
  const vault = createRemoteUrlVault({ key });
  const envelope = vault.encrypt(url, id);
  assert.equal(vault.decrypt(envelope, id), url);
  assert.equal(envelope.includes("secret123"), false);
});

test("Encryption uses a new nonce each time", () => {
  const vault = createRemoteUrlVault({ key });
  assert.notEqual(vault.encrypt(url, id), vault.encrypt(url, id));
});

test("Wrong job cannot decrypt", () => {
  const vault = createRemoteUrlVault({ key });
  const encrypted = vault.encrypt(url, id);
  assert.throws(() => vault.decrypt(encrypted, randomUUID()), /REMOTE_URL_DECRYPT_FAILED/);
});

test("Altered ciphertext is rejected", () => {
  const vault = createRemoteUrlVault({ key });
  const parts = vault.encrypt(url, id).split(".");
  parts[3] = (parts[3][0] === "A" ? "B" : "A") + parts[3].slice(1);
  assert.throws(() => vault.decrypt(parts.join("."), id), /REMOTE_URL_DECRYPT_FAILED/);
});

test("Wrong key cannot decrypt", () => {
  const first = createRemoteUrlVault({ key });
  const second = createRemoteUrlVault({ key: randomBytes(32).toString("base64url") });
  assert.throws(() => second.decrypt(first.encrypt(url, id), id), /REMOTE_URL_DECRYPT_FAILED/);
});

test("Invalid encryption key rejected", () => {
  assert.throws(() => createRemoteUrlVault({ key: "secret" }), /INVALID_REMOTE_URL_ENCRYPTION_KEY/);
});

test("Unsafe URLs rejected before encryption", () => {
  const vault = createRemoteUrlVault({ key });
  for (const value of ["http://media.example/a", "https://user:pass@media.example/a", "https://media.example/a#token", "x".repeat(8200)]) {
    assert.throws(() => vault.encrypt(value, id), /INVALID_REMOTE_SOURCE_URL/);
  }
});

test("Invalid job ID rejected", () => {
  const vault = createRemoteUrlVault({ key });
  assert.throws(() => vault.encrypt(url, "../../bad"), /INVALID_JOB_ID/);
});
