/**
 * Options parsed from CLI arguments.
 * @internal CLI interface — not part of the public programmatic API.
 */
export interface CliOptions {
  input?: string;
  output?: string;
  verbose: boolean;
  help: boolean;
  version: boolean;
  /** JSON string with ASS style overrides (only used for `.ass` output). */
  assStyle?: string;
}

/**
 * Read the next positional value for a flag.
 * @throws If the value is missing or looks like another flag.
 */
export function readArgValue(raw: string[], i: number, flag: string): string {
  const v = raw[i + 1];
  if (v === undefined || v.startsWith("-")) {
    throw new Error(`Missing value for flag: ${flag}`);
  }
  return v;
}

/**
 * Parse command-line arguments into a typed options object.
 *
 * Supported flags: `-o`, `--output`, `-v`, `--verbose`,
 * `--version`, `-h`, `--help`. Unknown flags cause an error.
 * The first non-flag argument is treated as the input file.
 */
export function parseArgs(raw: string[]): CliOptions {
  const opts: CliOptions = { verbose: false, help: false, version: false };

  for (let i = 0; i < raw.length; i++) {
    const arg = raw[i];
    switch (arg) {
      case "-o":
      case "--output": {
        opts.output = readArgValue(raw, i, arg);
        i++;
        break;
      }
      case "--ass-style": {
        opts.assStyle = readArgValue(raw, i, arg);
        i++;
        break;
      }
      case "-v":
      case "--verbose":
        opts.verbose = true;
        break;
      case "--version":
        opts.version = true;
        break;
      case "-h":
      case "--help":
        opts.help = true;
        break;
      default: {
        if (arg.startsWith("-")) {
          throw new Error(`Unknown option: ${arg}`);
        }
        if (opts.input) {
          throw new Error(`Unexpected argument: ${arg}`);
        }
        opts.input = arg;
      }
    }
  }

  return opts;
}
