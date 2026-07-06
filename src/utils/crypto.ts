import { createHash } from "node:crypto";

/** Compute CRC32 (IEEE 802.3) checksum of a buffer. Returns 8-char hex. */
export function crc32(buf: Buffer): string {
  let crc = 0xffffffff;
  for (let i = 0; i < buf.length; i++) {
    crc ^= buf[i];
    for (let j = 0; j < 8; j++) {
      if (crc & 1) {
        crc = (crc >>> 1) ^ 0xedb88320;
      } else {
        crc = crc >>> 1;
      }
    }
  }
  return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
}

/** Compute MD5 hex digest of a string. */
export function md5(s: string): string {
  return createHash("md5").update(s).digest("hex");
}

/** Compute SHA-256 hex digest of a string or buffer. */
export function sha256(s: string | Buffer): string {
  return createHash("sha256").update(s).digest("hex");
}
