import { createHash } from "node:crypto";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";

const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const UUID_RE = new RegExp(`^${UUID}$`, "i");
const KEY_RE = new RegExp(`^toolnet-srt/jobs/${UUID}/attempts/${UUID}/result\\.srt$`, "i");
const MAX_SRT_BYTES = 16 * 1024 * 1024;

export function attemptResultKey(jobId, leaseToken) {
  if (!UUID_RE.test(jobId ?? "")) throw new Error("INVALID_JOB_ID");
  if (!UUID_RE.test(leaseToken ?? "")) throw new Error("INVALID_LEASE_TOKEN");
  return `toolnet-srt/jobs/${jobId}/attempts/${leaseToken}/result.srt`;
}

export function createAttemptResultStorage({ env = process.env, client } = {}) {
  const required = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"];
  if (required.some(key => !env[key]?.trim())) throw new Error("R2_CONFIGURATION_MISSING");
  const accountId = env.R2_ACCOUNT_ID.trim();
  if (!/^[0-9a-f]{32}$/i.test(accountId)) throw new Error("INVALID_R2_ACCOUNT_ID");
  const bucket = env.R2_BUCKET.trim();
  const s3 = client ?? new S3Client({
    region: "auto",
    endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
    forcePathStyle: true,
    maxAttempts: 3,
    requestChecksumCalculation: "WHEN_REQUIRED",
    credentials: {
      accessKeyId: env.R2_ACCESS_KEY_ID,
      secretAccessKey: env.R2_SECRET_ACCESS_KEY,
    },
  });

  async function putResult({ jobId, leaseToken, srt }) {
    const key = attemptResultKey(jobId, leaseToken);
    if (typeof srt !== "string" && !Buffer.isBuffer(srt)) throw new Error("INVALID_SRT_CONTENT");
    const body = Buffer.isBuffer(srt) ? Buffer.from(srt) : Buffer.from(srt, "utf8");
    if (body.length === 0 || body.length > MAX_SRT_BYTES) throw new Error("INVALID_SRT_SIZE");
    const sha256 = createHash("sha256").update(body).digest("hex");
    try {
      await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentLength: body.length,
        ContentType: "application/x-subrip",
        IfNoneMatch: "*",
      }));
    } catch (error) {
      if (error.$metadata?.httpStatusCode === 412) throw new Error("R2_ATTEMPT_RESULT_ALREADY_EXISTS");
      throw error;
    }
    return { key, size: body.length, sha256 };
  }

  async function getResult({ key }) {
    if (typeof key !== "string" || !KEY_RE.test(key)) throw new Error("INVALID_RESULT_KEY");
    const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    if (!response.Body) throw new Error("R2_EMPTY_RESPONSE");
    if (response.ContentLength != null && response.ContentLength > MAX_SRT_BYTES) {
      response.Body.destroy?.();
      throw new Error("SRT_TOO_LARGE");
    }
    const chunks = [];
    let bytes = 0;
    for await (const chunk of response.Body) {
      const buffer = Buffer.from(chunk);
      bytes += buffer.length;
      if (bytes > MAX_SRT_BYTES) {
        response.Body.destroy?.();
        throw new Error("SRT_TOO_LARGE");
      }
      chunks.push(buffer);
    }
    if (!bytes) throw new Error("R2_EMPTY_RESPONSE");
    return Buffer.concat(chunks);
  }

  return { putResult, getResult };
}
