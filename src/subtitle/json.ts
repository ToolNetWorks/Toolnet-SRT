import type { SubtitleSegment } from "../asr/types.js";

/**
 * Convert segments to a JSON string (pretty-printed array).
 *
 * Each segment includes `text`, `startMs`, `endMs`, and
 * word-level `words` when available.
 */
export function toJson(segments: SubtitleSegment[]): string {
  return JSON.stringify(segments, null, 2);
}
