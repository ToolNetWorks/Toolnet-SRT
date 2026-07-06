import { createHmac } from "node:crypto";
import { sha256 } from "./crypto.js";
/**
 * AWS V4 signature generator.
 * Used to authorize upload to ByteDance VOD (AWS S3-compatible API).
 */
function hmacSha256(key, msg) {
    return createHmac("sha256", key).update(msg).digest();
}
function getSignatureKey(secretAccessKey, dateStamp, regionName, serviceName) {
    const kDate = hmacSha256(`AWS4${secretAccessKey}`, dateStamp);
    const kRegion = hmacSha256(kDate, regionName);
    const kService = hmacSha256(kRegion, serviceName);
    return hmacSha256(kService, "aws4_request");
}
/**
 * Generate AWS V4 signature for a request.
 */
export function awsSign(params) {
    const { secretKey, accessKey, sessionToken, requestParams, method = "GET", payload = "", region = "cn", service = "vod", } = params;
    const now = new Date();
    const amzDate = now.toISOString().replace(/[:-]/g, "").replace(/\.\d{3}Z/, "Z");
    const dateStamp = amzDate.slice(0, 8);
    const headers = {
        "x-amz-date": amzDate,
        "x-amz-security-token": sessionToken,
    };
    const canonicalUri = "/";
    const canonicalQueryString = requestParams;
    const headerKeys = Object.keys(headers);
    const canonicalHeaders = headerKeys.map((k) => `${k}:${headers[k]}`).join("\n") + "\n";
    const signedHeaders = headerKeys.join(";");
    const payloadHash = sha256(payload);
    const canonicalRequest = [
        method,
        canonicalUri,
        canonicalQueryString,
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
    const signingKey = getSignatureKey(secretKey, dateStamp, region, service);
    const signature = createHmac("sha256", signingKey)
        .update(stringToSign)
        .digest("hex");
    const authorization = `${algorithm} Credential=${accessKey}/${credentialScope}, ` +
        `SignedHeaders=${signedHeaders}, Signature=${signature}`;
    return { authorization, amzDate, headers };
}
//# sourceMappingURL=aws-sign.js.map