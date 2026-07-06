import type { SubtitleSegment } from "../asr/types.js";
export declare function toSrt(segments: SubtitleSegment[]): string;
export declare function toAss(segments: SubtitleSegment[]): string;
export declare function toJson(segments: SubtitleSegment[]): string;
export declare function toTxt(segments: SubtitleSegment[]): string;
export type SubtitleFormat = "srt" | "ass" | "json" | "txt";
export type WriteSubtitleOptions = {
    segments: SubtitleSegment[];
    output: string;
};
/**
 * Write segments to a subtitle file, inferring format from extension.
 * Falls back to .srt if extension is not recognized.
 */
export declare function writeSubtitle(opts: WriteSubtitleOptions): void;
/**
 * Write segments to multiple formats at once.
 *
 * Example:
 *   writeSubtitles(segments, "/tmp/output.srt", { also: ["json", "txt"] })
 *   → /tmp/output.srt, /tmp/output.json, /tmp/output.txt
 */
export declare function writeSubtitles(segments: SubtitleSegment[], output: string, extras?: {
    also?: SubtitleFormat[];
}): void;
