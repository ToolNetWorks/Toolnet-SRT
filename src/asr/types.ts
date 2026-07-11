/** A single subtitle segment with timing information. */
export interface SubtitleSegment {
  text: string;
  startMs: number;
  endMs: number;
  /** Word-level timestamps for advanced consumers and JSON output. */
  words?: SubtitleWord[];
}

export interface SubtitleWord {
  text: string;
  startMs: number;
  endMs: number;
}

/** Raw response body from the JianYing ASR query endpoint. */
export interface JianYingResponse {
  ret: string;
  errmsg?: string;
  /** `null` when the task is still processing; `{ utterances: [...] }` when ready. */
  data?: {
    id: string;
    utterances: Array<{
      text: string;
      start_time: number;
      end_time: number;
      words?: Array<{
        text: string;
        start_time: number;
        end_time: number;
      }>;
    }>;
  } | null;
}

/** Options for {@link transcribe}. */
export interface TranscribeOptions {
  /**
   * Path to an audio file (mp3 / wav / flac / m4a).
   * Video files must be converted to audio before calling
   * {@link transcribe} — the CLI handles this automatically.
   */
  input: string;
  /** Progress callback. `percent` ranges from 0 to 100. */
  onProgress?: (percent: number, message: string) => void;
  /** Abort signal for cancelling uploads, polling, and result fetching. */
  signal?: AbortSignal;
  /** Maximum time to wait for the ASR result (default: 5 minutes). */
  queryTimeoutMs?: number;
  /** Interval between query polls (default: 2 seconds). */
  queryIntervalMs?: number;
}
