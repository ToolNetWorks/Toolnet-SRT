import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

const JOB_ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const KEY_RE = /^[A-Za-z0-9_-]{43}$/;
const URL_MAX_BYTES = 8192;
const ENVELOPE_RE = /^v1\.([A-Za-z0-9_-]{16})\.([A-Za-z0-9_-]{22})\.([A-Za-z0-9_-]+)$/;

function jobContext(jobId) {
  if (typeof jobId !== "string" || !JOB_ID_RE.test(jobId)) throw new Error("INVALID_JOB_ID");
  return Buffer.from(`toolnet-srt/remote-url/v1/${jobId.toLowerCase()}`, "utf8");
}

function getKey(encoded) {
  if (typeof encoded !== "string" || !KEY_RE.test(encoded)) throw new Error("INVALID_REMOTE_URL_ENCRYPTION_KEY");
  const key = Buffer.from(encoded, "base64url");
  if (key.length !== 32 || key.toString("base64url") !== encoded) throw new Error("INVALID_REMOTE_URL_ENCRYPTION_KEY");
  return key;
}

export function createRemoteUrlVault({ key = process.env.TOOLNET_SRT_REMOTE_URL_ENCRYPTION_KEY } = {}) {
  const secret = getKey(key);

  function encrypt(rawUrl, jobId) {
    const aad = jobContext(jobId);
    if (typeof rawUrl !== "string" || !rawUrl || Buffer.byteLength(rawUrl, "utf8") > URL_MAX_BYTES) {
      throw new Error("INVALID_REMOTE_SOURCE_URL");
    }
    let url;
    try { url = new URL(rawUrl); } catch { throw new Error("INVALID_REMOTE_SOURCE_URL"); }
    if (url.protocol !== "https:" || url.username || url.password || url.hash || (url.port && url.port !== "443")) {
      throw new Error("INVALID_REMOTE_SOURCE_URL");
    }
    const nonce = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", secret, nonce);
    cipher.setAAD(aad);
    const encrypted = Buffer.concat([cipher.update(rawUrl, "utf8"), cipher.final()]);
    const tag = cipher.getAuthTag();
    return ["v1", nonce.toString("base64url"), tag.toString("base64url"), encrypted.toString("base64url")].join(".");
  }

  function decrypt(envelope, jobId) {
    const aad = jobContext(jobId);
    const match = typeof envelope === "string" && envelope.length <= 16000 ? ENVELOPE_RE.exec(envelope) : null;
    if (!match) throw new Error("REMOTE_URL_DECRYPT_FAILED");
    try {
      const nonce = Buffer.from(match[1], "base64url");
      const tag = Buffer.from(match[2], "base64url");
      const ciphertext = Buffer.from(match[3], "base64url");
      if (nonce.length !== 12 || tag.length !== 16 || ciphertext.length === 0 || ciphertext.length > URL_MAX_BYTES) {
        throw new Error("INVALID_ENVELOPE");
      }
      const decipher = createDecipheriv("aes-256-gcm", secret, nonce);
      decipher.setAAD(aad);
      decipher.setAuthTag(tag);
      const plain = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
      return new TextDecoder("utf-8", { fatal: true }).decode(plain);
    } catch { throw new Error("REMOTE_URL_DECRYPT_FAILED"); }
  }

  return { encrypt, decrypt };
}
