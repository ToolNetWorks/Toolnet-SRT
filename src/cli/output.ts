import * as path from "node:path";
import * as fs from "node:fs";
import type { SubtitleFormat } from "../subtitle/index.js";

const SUBTITLE_EXTS = new Set<SubtitleFormat>(["srt", "ass", "json", "txt"]);

/**
 * Determine the output file path and format.
 *
 * - No `-o` → `<input-dir>/<stem>.json`
 * - `-o out.srt` → `out.srt`, format `srt`
 * - `-o out` (no extension) → `out.json`
 * - Unsupported extension → error
 * - Directory paths → error
 */
export function resolveOutput(
  input: string,
  output?: string,
): { outputPath: string; format: SubtitleFormat } {
  const stem = path.basename(input, path.extname(input));

  if (!output) {
    return {
      outputPath: path.join(
        path.dirname(path.resolve(input)),
        `${stem}.json`,
      ),
      format: "json",
    };
  }

  const trailingSlash = /[\\/]$/.test(output);
  const isExistingDir =
    !trailingSlash &&
    fs.existsSync(output) &&
    fs.statSync(output).isDirectory();
  if (trailingSlash || isExistingDir) {
    throw new Error(
      `Output path is a directory: ${output}. ` +
        `Use a file path, e.g. -o ${output}${stem}.srt`,
    );
  }

  const ext = path.extname(output).slice(1).toLowerCase() as SubtitleFormat;
  if (ext) {
    if (!SUBTITLE_EXTS.has(ext)) {
      throw new Error(
        `Unsupported output format: .${ext}. Supported: srt, ass, json, txt`,
      );
    }
    return { outputPath: output, format: ext };
  }

  return { outputPath: `${output}.json`, format: "json" };
}
