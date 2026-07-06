/**
 * Compute CRC32 (IEEE 802.3) of a buffer.
 * Returns 8-char lowercase hex string.
 */
export declare function crc32(buf: Buffer): string;
/**
 * Compute MD5 hex string.
 */
export declare function md5(s: string): string;
/**
 * Compute SHA256 hex string.
 */
export declare function sha256(s: string | Buffer): string;
/**
 * Normalize a buffer to signed 32-bit representation.
 */
export declare function toSigned32(value: number): number;
