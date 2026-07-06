import { awsSign } from "../utils/aws-sign.js";
import { API_BASE, VOD_BASE, UA_UPLOAD } from "./constants.js";
import { generateSignParams, buildHeaders } from "./helpers.js";

/** Shape of the upload-sign step response. */
interface UploadSignResponse {
  ret: string;
  errmsg?: string;
  data?: {
    access_key_id?: string;
    secret_access_key?: string;
    session_token?: string;
  };
}

/** Shape of the upload-authorization step response. */
interface UploadAuthResponse {
  Result?: {
    UploadAddress?: {
      StoreInfos?: Array<{
        StoreUri?: string;
        Auth?: string;
        UploadID?: string;
      }>;
      UploadHosts?: string[];
    };
  };
  ResponseMetadata?: unknown;
}

/**
 * Shared context threaded through every upload step.
 * Callers create this once and pass it to each pipeline function.
 */
export interface UploadPipelineContext {
  tdid: string;
  crc32Hex: string;
  fileBuffer: Buffer;
  fileSize: number;
  checkedFetch: (url: string, init: RequestInit) => Promise<Response>;
}

/**
 * Mutable state accumulated during the upload pipeline.
 * Fields are populated step by step and must all be present
 * before calling the final commit.
 */
export interface UploadState {
  accessKey: string;
  secretKey: string;
  sessionToken: string;
  storeUri: string;
  authToken: string;
  uploadId: string;
  uploadHost: string;
}

/**
 * Step 1 — Request temporary AWS credentials.
 *
 * Posts a `biz: "pc-recognition"` payload to the JianYing upload-sign
 * endpoint and receives an S3-compatible access key, secret, and
 * session token.
 */
export async function uploadSign(
  ctx: UploadPipelineContext,
): Promise<Pick<UploadState, "accessKey" | "secretKey" | "sessionToken">> {
  const url = `${API_BASE}/lv/v1/upload_sign`;
  const payload = JSON.stringify({ biz: "pc-recognition" });
  const { sign, deviceTime } = generateSignParams(
    "/lv/v1/upload_sign",
    ctx.tdid,
  );
  const headers = buildHeaders(deviceTime, sign, ctx.tdid);

  const res = await ctx.checkedFetch(url, {
    method: "POST",
    body: payload,
    headers,
  });
  const json = (await res.json()) as UploadSignResponse;

  if (json.ret !== "0") {
    throw new Error(
      `Upload sign failed (ret=${json.ret}): ${JSON.stringify(json).slice(0, 500)}`,
    );
  }
  return {
    accessKey: json.data!.access_key_id!,
    secretKey: json.data!.secret_access_key!,
    sessionToken: json.data!.session_token!,
  };
}

/**
 * Step 2 — Obtain an upload destination from ByteDance VOD.
 *
 * Signs an `ApplyUploadInner` request with an AWS V4 signature using
 * the credentials from step 1. The response contains a `StoreUri`,
 * `UploadID`, and an auth token needed for the subsequent PUT.
 */
export async function uploadAuth(
  ctx: UploadPipelineContext,
  creds: Pick<UploadState, "accessKey" | "secretKey" | "sessionToken">,
): Promise<
  Pick<UploadState, "storeUri" | "authToken" | "uploadId" | "uploadHost">
> {
  const requestParams = [
    "Action=ApplyUploadInner",
    `FileSize=${ctx.fileSize}`,
    "FileType=object",
    "IsInner=1",
    "SpaceName=lv-mac-recognition",
    "Version=2020-11-19",
    "s=5y0udbjapi",
  ].join("&");

  const signResult = awsSign({
    secretKey: creds.secretKey,
    accessKey: creds.accessKey,
    sessionToken: creds.sessionToken,
    requestParams,
  });

  const headers: Record<string, string> = {
    ...signResult.headers,
    authorization: signResult.authorization,
  };

  const res = await ctx.checkedFetch(`${VOD_BASE}/?${requestParams}`, {
    method: "GET",
    headers,
  });
  const text = await res.text();

  let json: UploadAuthResponse;
  try {
    json = JSON.parse(text) as UploadAuthResponse;
  } catch {
    throw new Error(
      `Upload auth returned non-JSON (HTTP ${res.status}): ${text.slice(0, 500)}`,
    );
  }

  if (!json.Result?.UploadAddress) {
    throw new Error(
      `Upload auth failed (HTTP ${res.status}): ${JSON.stringify(json).slice(0, 500)}`,
    );
  }

  const si = json.Result.UploadAddress.StoreInfos![0];
  return {
    storeUri: si.StoreUri!,
    authToken: si.Auth!,
    uploadId: si.UploadID!,
    uploadHost: json.Result.UploadAddress.UploadHosts![0],
  };
}

/**
 * Step 3 — PUT the raw audio bytes to the ByteDance VOD endpoint.
 */
export async function uploadFile(
  ctx: UploadPipelineContext,
  st: UploadState,
): Promise<void> {
  const url =
    `https://${st.uploadHost}/${st.storeUri}?partNumber=1&uploadID=${st.uploadId}`;
  const headers: Record<string, string> = {
    "User-Agent": UA_UPLOAD,
    Authorization: st.authToken,
    "Content-CRC32": ctx.crc32Hex,
  };

  const res = await ctx.checkedFetch(url, {
    method: "PUT",
    body: new Uint8Array(ctx.fileBuffer),
    headers,
  });
  const json = (await res.json()) as { success: number };
  if (json.success !== 0) {
    throw new Error(`File upload failed: ${JSON.stringify(json)}`);
  }
}

/**
 * Step 4 — POST a CRC32 check to verify the upload was received intact.
 */
export async function uploadCheck(
  ctx: UploadPipelineContext,
  st: UploadState,
): Promise<void> {
  const url =
    `https://${st.uploadHost}/${st.storeUri}?uploadID=${st.uploadId}`;
  const headers: Record<string, string> = {
    "User-Agent": UA_UPLOAD,
    Authorization: st.authToken,
    "Content-CRC32": ctx.crc32Hex,
  };

  const payload = `1:${ctx.crc32Hex}`;
  const res = await ctx.checkedFetch(url, {
    method: "POST",
    body: payload,
    headers,
  });
  await res.json();
}

/**
 * Step 5 — Finalize (commit) the upload.
 *
 * The Python reference implementation does not check the response,
 * and the file is already accessible after step 3, so failures here
 * are silently ignored.
 */
export async function uploadCommit(
  ctx: UploadPipelineContext,
  st: UploadState,
): Promise<void> {
  const url = [
    `https://${st.uploadHost}/${st.storeUri}`,
    `?uploadID=${st.uploadId}`,
    `&partNumber=1`,
    `&x-amz-security-token=${st.sessionToken}`,
  ].join("");

  const headers: Record<string, string> = {
    "User-Agent": UA_UPLOAD,
    Authorization: st.authToken,
    "Content-CRC32": ctx.crc32Hex,
  };

  try {
    await ctx.checkedFetch(url, {
      method: "PUT",
      body: new Uint8Array(ctx.fileBuffer),
      headers,
    });
  } catch {
    /* best-effort */
  }
}
