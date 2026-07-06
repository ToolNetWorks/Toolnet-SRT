import { createHmac } from "node:crypto";
import { sha256 } from "./crypto.js";

/**
 * AWS V4 signature for ByteDance VOD upload authorization.
 *
 * Used to sign the `ApplyUploadInner` request to ByteDance's
 * S3-compatible VOD service.
 */

function hmacSha256(key: string | Buffer, msg: string): Buffer {
  return createHmac("sha256", key).update(msg).digest();
}

function getSignatureKey(
  secretAccessKey: string,
  dateStamp: string,
  regionName: string,
  serviceName: string,
): Buffer {
  const kDate = hmacSha256(`AWS4${secretAccessKey}`, dateStamp);
  const kRegion = hmacSha256(kDate, regionName);
  const kService = hmacSha256(kRegion, serviceName);
  return hmacSha256(kService, "aws4_request");
}

export interface AwsSignParams {
  secretKey: string;
  accessKey: string;
  sessionToken: string;
  requestParams: string;
  method?: string;
  payload?: string;
  region?: string;
  service?: string;
}

export interface AwsSignResult {
  authorization: string;
  amzDate: string;
  headers: Record<string, string>;
}

/** Generate an AWS V4 signature and authorization header. */
export function awsSign(params: AwsSignParams): AwsSignResult {
  const {
    secretKey,
    accessKey,
    sessionToken,
    requestParams,
    method = "GET",
    payload = "",
    region = "cn",
    service = "vod",
  } = params;

  const now = new Date();
  const amzDate =
    now.toISOString().replace(/[:-]/g, "").replace(/\.\d{3}Z/, "Z");
  const dateStamp = amzDate.slice(0, 8);

  const headers: Record<string, string> = {
    "x-amz-date": amzDate,
    "x-amz-security-token": sessionToken,
  };

  const headerKeys = Object.keys(headers);
  const canonicalHeaders =
    headerKeys.map((k) => `${k}:${headers[k]}`).join("\n") + "\n";
  const signedHeaders = headerKeys.join(";");
  const payloadHash = sha256(payload);

  const canonicalRequest = [
    method,
    "/",
    requestParams,
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join("\n");

  const algorithm = "AWS4-HMAC-SHA256";
  const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;
  const stringToSign = [
    algorithm,
    amzDate,
    credentialScope,
    sha256(canonicalRequest),
  ].join("\n");

  const signingKey = getSignatureKey(
    secretKey,
    dateStamp,
    region,
    service,
  );
  const signature = createHmac("sha256", signingKey)
    .update(stringToSign)
    .digest("hex");

  const authorization =
    `${algorithm} Credential=${accessKey}/${credentialScope}, ` +
    `SignedHeaders=${signedHeaders}, Signature=${signature}`;

  return { authorization, amzDate, headers };
}
