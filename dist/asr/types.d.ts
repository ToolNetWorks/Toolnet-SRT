/** A single subtitle segment with timing. */
export interface SubtitleSegment {
    text: string;
    startMs: number;
    endMs: number;
    /** Word-level timestamps (when needWordTimestamp is true). */
    words?: SubtitleWord[];
}
export interface SubtitleWord {
    text: string;
    startMs: number;
    endMs: number;
}
/** Raw response from JianYing ASR. */
export interface JianYingResponse {
    ret: string;
    errmsg?: string;
    data: {
        id: string;
        utterances: Array<{
            text: string;
            start_time: number;
            end_time: number;
            words: Array<{
                text: string;
                start_time: number;
                end_time: number;
            }>;
        }>;
    };
}
/** Options for transcribe(). */
export interface TranscribeOptions {
    /** Path to audio file (mp3/wav/flac/m4a) or video file. */
    input: string;
    /** Use disk cache to avoid re-transcribing same file. */
    cache?: boolean;
    /** Return word-level timestamps instead of sentence-level. */
    wordTimestamps?: boolean;
    /** Cache directory. Default: ~/.cache/jianying-subtitle */
    cacheDir?: string;
    /** Progress callback: (percent, message). */
    onProgress?: (percent: number, message: string) => void;
    /**
     * Timeout for polling query results in ms.
     * Default: 300_000 (5 minutes).
     */
    queryTimeoutMs?: number;
    /**
     * Polling interval for query results in ms.
     * Default: 2000 (2 seconds).
     */
    queryIntervalMs?: number;
    __audioBuffer?: Buffer;
}
