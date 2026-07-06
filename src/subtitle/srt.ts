import type { SubtitleSegment } from "../asr/types.js";

function msToSrtTime(ms: number): string {
  const h = Math.floor(ms / 3_600_000);
  const m = Math.floor((ms % 3_600_000) / 60_000);
  const s = Math.floor((ms % 60_000) / 1_000);
  const milli = Math.floor(ms % 1_000);
  return [
    String(h).padStart(2, "0"),
    String(m).padStart(2, "0"),
    String(s).padStart(2, "0"),
  ].join(":") +
    "," +
    String(milli).padStart(3, "0");
}

/**
 * Convert segments to SRT (SubRip) format.
 *
 * Canonical format with sequential numbering,
 * `HH:MM:SS,mmm --> HH:MM:SS,mmm` timestamps, and UTF-8 text.
 */
export function toSrt(segments: SubtitleSegment[]): string {
  return segments
    .map((seg, i) => {
      return [
        String(i + 1),
        `${msToSrtTime(seg.startMs)} --> ${msToSrtTime(seg.endMs)}`,
        seg.text,
        "",
      ].join("\n");
    })
    .join("\n");
}
