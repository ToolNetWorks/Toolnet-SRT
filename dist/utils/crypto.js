import { createHash } from "node:crypto";
/**
 * Compute CRC32 (IEEE 802.3) of a buffer.
 * Returns 8-char lowercase hex string.
 */
export function crc32(buf) {
    let crc = 0xffffffff;
    for (let i = 0; i < buf.length; i++) {
        crc ^= buf[i];
        for (let j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >>> 1) ^ 0xedb88320;
            }
            else {
                crc = crc >>> 1;
            }
        }
    }
    return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
}
/**
 * Compute MD5 hex string.
 */
export function md5(s) {
    return createHash("md5").update(s).digest("hex");
}
/**
 * Compute SHA256 hex string.
 */
export function sha256(s) {
    return createHash("sha256").update(s).digest("hex");
}
/**
 * Normalize a buffer to signed 32-bit representation.
 */
export function toSigned32(value) {
    return value << 0;
}
//# sourceMappingURL=crypto.js.map