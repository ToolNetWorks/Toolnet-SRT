import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { randomUUID } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rename, rm, stat } from "node:fs/promises";
import { dirname } from "node:path";
import { pipeline } from "node:stream/promises";
import { Transform } from "node:stream";
import { remoteSourceKey, validateSourceKey } from "./source-keys.mjs";

const JOB_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const FILE_PATTERN =
  /^(?:source\.(?:mp3|mp4|wav|m4a|flac|ogg|mov|mkv|webm)|result\.srt|segment_\d{5}\.mp3)$/;
const MAX_SINGLE_UPLOAD = 5 * 1024 ** 3;

export function isR2Configured(env = process.env) {
  return [
    "R2_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "R2_BUCKET",
  ].every(name => Boolean(env[name]?.trim()));
}

export function jobObjectKey(jobId, filename) {
  if (!JOB_ID_PATTERN.test(jobId)) {
    throw new Error("INVALID_JOB_ID");
  }
  if (!FILE_PATTERN.test(filename)) {
    throw new Error("INVALID_STORAGE_FILENAME");
  }
  return `toolnet-srt/jobs/${jobId}/${filename}`;
}

export function createR2Storage({ env = process.env, client } = {}) {
  if (!isR2Configured(env)) {
    throw new Error("R2_CONFIGURATION_MISSING");
  }
  const accountId = env.R2_ACCOUNT_ID.trim();
  if (!/^[0-9a-f]{32}$/i.test(accountId)) {
    throw new Error("INVALID_R2_ACCOUNT_ID");
  }
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

  async function upload({ key, localPath, contentType, createOnce = false, signal }) {
    const file = await stat(localPath);
    if (!file.isFile() || file.size === 0) {
      throw new Error("INVALID_UPLOAD_FILE");
    }
    if (file.size > MAX_SINGLE_UPLOAD) {
      throw new Error("MULTIPART_UPLOAD_REQUIRED");
    }
    const body = createReadStream(localPath);
    try {
      await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentLength: file.size,
        ContentType: contentType,
        ...(createOnce ? { IfNoneMatch: "*" } : {}),
      }), signal ? { abortSignal: signal } : undefined);
    } finally {
      body.destroy();
    }
    return { key, size: file.size };
  }

  async function uploadFile({ jobId, filename, localPath, contentType = "application/octet-stream" }) {
    return upload({ key: jobObjectKey(jobId, filename), localPath, contentType });
  }

  async function uploadRemoteSource({ jobId, fetchToken, filename, localPath, contentType = "application/octet-stream", signal }) {
    return upload({ key: remoteSourceKey(jobId, fetchToken, filename), localPath, contentType, createOnce: true, signal });
  }

  async function getRemoteSourceInfo({ jobId, fetchToken, filename, signal }) {
    const key = remoteSourceKey(jobId, fetchToken, filename);
    try {
      const result = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }), signal ? { abortSignal: signal } : undefined);
      return { key, size: result.ContentLength ?? 0 };
    } catch (error) {
      if (error.name === "NotFound" || error.$metadata?.httpStatusCode === 404) return null;
      throw error;
    }
  }

  async function downloadFile({ jobId, filename, localPath, sourceKey, maxBytes = MAX_SINGLE_UPLOAD, signal }) {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_SINGLE_UPLOAD) throw new Error('INVALID_DOWNLOAD_LIMIT');
    const key = sourceKey === undefined ? jobObjectKey(jobId, filename) : validateSourceKey(jobId, filename, sourceKey);
    await mkdir(dirname(localPath), { recursive: true, mode: 0o700 });
    const tempPath = `${localPath}.${randomUUID()}.part`;
    try {
      const result = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }), signal ? { abortSignal:signal } : undefined);
      if (!result.Body) {
        throw new Error("R2_EMPTY_RESPONSE");
      }
      if (result.ContentLength !== undefined && (!Number.isSafeInteger(result.ContentLength) || result.ContentLength > maxBytes)) {
        result.Body.destroy?.();
        throw new Error('R2_SOURCE_TOO_LARGE');
      }
      let received=0;
      const limiter=new Transform({ transform(chunk,_encoding,callback){received+=chunk.length;
        if(received>maxBytes)return callback(new Error('R2_SOURCE_TOO_LARGE'));callback(null,chunk);} });
      await pipeline(result.Body, limiter, createWriteStream(tempPath, { mode: 0o600 }), ...(signal ? [{signal}] : []));
      await rename(tempPath, localPath);
      return { key, localPath };
    } catch (error) {
      await rm(tempPath, { force: true }).catch(() => {});
      throw error;
    }
  }

  async function getFileInfo({ jobId, filename }) {
    const key = jobObjectKey(jobId, filename);
    try {
      const result = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      return {
        key,
        size: result.ContentLength ?? 0,
        contentType: result.ContentType ?? null,
        lastModified: result.LastModified ?? null,
      };
    } catch (error) {
      if (error.name === "NotFound" || error.$metadata?.httpStatusCode === 404) {
        return null;
      }
      throw error;
    }
  }

  async function deleteFile({ jobId, filename }) {
    const key = jobObjectKey(jobId, filename);
    await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
    return { deleted: true, key };
  }

  return { uploadFile, uploadRemoteSource, getRemoteSourceInfo, downloadFile, getFileInfo, deleteFile };
}
