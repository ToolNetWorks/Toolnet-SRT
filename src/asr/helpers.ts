import * as os from "node:os";
import { md5 } from "../utils/crypto.js";
import { PF, APP_VERSION } from "./constants.js";

/**
 * Generate a device ID (TDID) that mimics the official CapCut client.
 *
 * Formula: `fr = 390 + last digit of current year`, followed by a
 * MAC-based network-hash in even years or a fixed string in odd years.
 */
export function generateTdid(): string {
  const yearLastDigit = String(new Date().getFullYear())[3];
  const fr = 390 + Number(yearLastDigit);
  const odd = Number(yearLastDigit) % 2 !== 0;
  const ed = odd
    ? "3278516897751"
    : String(getMacNode()).padStart(13, "0");
  return `${fr}${ed}`;
}

/** Read the first non-zero MAC address from the system's network interfaces. */
function getMacNode(): number {
  const ifaces = os.networkInterfaces();
  for (const iface of Object.values(ifaces)) {
    if (!iface) continue;
    for (const detail of iface) {
      if (detail.mac && detail.mac !== "00:00:00:00:00:00") {
        return parseInt(detail.mac.replace(/:/g, ""), 16);
      }
    }
  }
  return 0;
}

/**
 * Generate the local MD5-based request signature.
 *
 * The signing formula is:
 * `MD5("9e2c|" + urlSuffix + "|" + pf + "|" + appvr + "|" + ts + "|" + tdid + "|11ac")`
 *
 * No external signing service is required — this is computed entirely
 * offline and matches the official CapCut client behaviour.
 */
export function generateSignParams(
  url: string,
  tdid: string,
  pf: string = PF,
  appvr: string = APP_VERSION,
): { sign: string; deviceTime: string } {
  const deviceTime = String(Math.floor(Date.now() / 1000));
  const v = url.length >= 7 ? url.slice(-7) : url;
  const raw = `9e2c|${v}|${pf}|${appvr}|${deviceTime}|${tdid}|11ac`;
  return { sign: md5(raw), deviceTime };
}

/**
 * Build the minimal set of request headers required by the JianYing API.
 *
 * Includes the MD5 signature, device timestamp, platform, and app version.
 */
export function buildHeaders(
  deviceTime: string,
  sign: string,
  tdid: string,
): Record<string, string> {
  return {
    "User-Agent":
      "Cronet/TTNetVersion:d4572e53 2024-06-12 QuicVersion:4bf243e0 2023-04-17",
    appvr: APP_VERSION,
    "device-time": deviceTime,
    pf: PF,
    sign,
    "sign-ver": "1",
    tdid,
  };
}

/**
 * Parse a JSON response body.
 *
 * Throws a descriptive error when the body is not valid JSON, including
 * the first 500 characters of the raw text.
 */
export function parseJsonResponse<T>(
  text: string,
  label: string,
  status: number,
): T {
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new Error(
      `${label} returned non-JSON (HTTP ${status}): ${text.slice(0, 500)}`,
    );
  }
}

/**
 * Promise-based sleep. Used for polling intervals.
 */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(new Error("Cancelled"));

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timeout);
      reject(new Error("Cancelled"));
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
