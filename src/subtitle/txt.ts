import type { SubtitleSegment } from "../asr/types.js";

/**
 * Convert segments to plain text, one utterance per line.
 */
export function toTxt(segments: SubtitleSegment[]): string {
  return segments.map((s) => s.text).join("\n");
}
