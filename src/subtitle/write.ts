import * as fs from "node:fs";
import * as path from "node:path";
import type { SubtitleSegment } from "../asr/types.js";
import { toSrt } from "./srt.js";
import { toAss, type AssStyleOptions } from "./ass.js";
import { toJson } from "./json.js";
import { toTxt } from "./txt.js";

/** Supported subtitle output formats. */
export type SubtitleFormat = "srt" | "ass" | "json" | "txt";

/** Options for {@link writeSubtitle}. */
export type WriteSubtitleOptions = {
  segments: SubtitleSegment[];
  output: string;
  /**
   * Explicit format override.
   *
   * When provided it is used instead of inferring the format from
   * the output file extension. Useful when the output path is an
   * extensionless base.
   */
  format?: SubtitleFormat;
  /**
   * ASS style overrides.
   *
   * Only used when writing `ass` format — ignored for other formats.
   * Omitted fields fall back to built-in defaults (Arial/14pt white-on-black).
   */
  assStyle?: AssStyleOptions;
};

/**
 * Resolve the output format.
 *
 * If an explicit format is provided it is returned directly.
 * Otherwise the output file extension is matched against the known
 * set of subtitle extensions, falling back to `"srt"`.
 */
function resolveFormat(
  output: string,
  explicit?: SubtitleFormat,
): SubtitleFormat {
  if (explicit) return explicit;
  const ext = path.extname(output).slice(1).toLowerCase();
  const valid = new Set<SubtitleFormat>(["srt", "ass", "json", "txt"]);
  return valid.has(ext as SubtitleFormat) ? (ext as SubtitleFormat) : "srt";
}

/**
 * Write a single subtitle file.
 *
 * The format defaults to the output file extension. Pass {@link
 * WriteSubtitleOptions.format} to override (for example when the
 * output path is an extensionless base that will have the extension
 * appended later).
 *
 * Parent directories are created automatically.
 */
export function writeSubtitle(opts: WriteSubtitleOptions): void {
  const fmt = resolveFormat(opts.output, opts.format);
  let content: string;

  switch (fmt) {
    case "ass":
      content = toAss(opts.segments, opts.assStyle);
      break;
    case "json":
      content = toJson(opts.segments);
      break;
    case "txt":
      content = toTxt(opts.segments);
      break;
    case "srt":
    default:
      content = toSrt(opts.segments);
      break;
  }

  fs.mkdirSync(path.dirname(path.resolve(opts.output)), { recursive: true });
  fs.writeFileSync(opts.output, content, "utf-8");
}

/** Additional formats requested alongside the primary output. */
export type WriteSubtitlesExtras = {
  also?: SubtitleFormat[];
  /** ASS style overrides (only used when writing `.ass` files). */
  assStyle?: AssStyleOptions;
};

/**
 * Write segments to multiple subtitle formats at once.
 *
 * Two calling conventions are supported:
 *
 * **Legacy** — format is inferred from the output file extension.
 * ```ts
 * writeSubtitles(segments, "/tmp/out.srt", { also: ["json", "txt"] });
 * // → out.srt, out.json, out.txt
 * ```
 *
 * **Explicit primary format** — output is an extensionless base path.
 * ```ts
 * writeSubtitles(segments, "/tmp/out", "srt", { also: ["json"] });
 * // → out.srt, out.json
 * ```
 */
export function writeSubtitles(
  segments: SubtitleSegment[],
  output: string,
  extras?: WriteSubtitlesExtras,
): void;
export function writeSubtitles(
  segments: SubtitleSegment[],
  output: string,
  primaryFormat: SubtitleFormat,
  extras?: WriteSubtitlesExtras,
): void;
export function writeSubtitles(
  segments: SubtitleSegment[],
  output: string,
  formatOrExtras?: SubtitleFormat | WriteSubtitlesExtras,
  extras?: WriteSubtitlesExtras,
): void {
  // legacy — format inferred from extension
  if (formatOrExtras == null || typeof formatOrExtras === "object") {
    const legacyExtras = formatOrExtras;
    writeSubtitle({ segments, output, assStyle: legacyExtras?.assStyle });
    if (legacyExtras?.also) {
      const base = output.replace(/\.[^.]+$/, "");
      const primaryExt = path.extname(output).slice(1).toLowerCase();
      for (const fmt of legacyExtras.also) {
        if (fmt === primaryExt) continue;
        writeSubtitle({ segments, output: `${base}.${fmt}`, format: fmt, assStyle: legacyExtras.assStyle });
      }
    }
    return;
  }

  // explicit primary format
  const primaryFormat = formatOrExtras;
  writeSubtitle({
    segments,
    output: `${output}.${primaryFormat}`,
    format: primaryFormat,
    assStyle: extras?.assStyle,
  });
  if (extras?.also) {
    for (const fmt of extras.also) {
      if (fmt === primaryFormat) continue;
      writeSubtitle({ segments, output: `${output}.${fmt}`, format: fmt, assStyle: extras.assStyle });
    }
  }
}
