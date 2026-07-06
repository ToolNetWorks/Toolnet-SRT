/** Extract audio from a video file to a temporary WAV file. Returns the temp path. */
export declare function videoToAudio(input: string): Promise<string>;
/** Clean up a temp file if it exists. */
export declare function cleanupTempFile(tmpPath: string): void;
