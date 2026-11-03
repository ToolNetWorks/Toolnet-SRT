import { statfs } from 'node:fs/promises';

// Reservations are shared by all fetch workers in one runtime and deliberately
// reserve the full maximum before download starts.
export function createFetchDiskQuota({ maxBytes, freeSpace = statfs } = {}) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) throw new Error('INVALID_FETCH_DISK_QUOTA');
  if (typeof freeSpace !== 'function') throw new Error('INVALID_DISK_SPACE_CHECK');
  let reserved = 0;
  async function reserve(bytes, path) {
    if (!Number.isSafeInteger(bytes) || bytes < 1 || bytes > maxBytes || reserved + bytes > maxBytes) {
      throw new Error('FETCH_DISK_QUOTA_EXCEEDED');
    }
    // Reserve synchronously before statfs so simultaneous calls cannot overbook.
    reserved += bytes;
    try {
      const space = await freeSpace(path);
      if (space.bavail * space.bsize < reserved) throw new Error('INSUFFICIENT_DISK_SPACE');
    } catch (error) { reserved -= bytes; throw error; }
    let released = false;
    return () => { if (!released) { released = true; reserved -= bytes; } };
  }
  return { reserve, getStatus: () => ({ maxBytes, reservedBytes: reserved }) };
}
