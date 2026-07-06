import type { SubtitleSegment, TranscribeOptions } from "./types.js";
export declare class JianYingASR {
    private options;
    private tdid;
    private crc32Hex;
    private fileBuffer;
    private fileSize;
    private accessKey?;
    private secretKey?;
    private sessionToken?;
    private storeUri?;
    private uploadAuthToken?;
    private uploadId?;
    private uploadHost?;
    private cacheDir;
    constructor(options: TranscribeOptions);
    /**
     * Run full ASR pipeline: upload → submit → query → parse.
     */
    transcribe(): Promise<SubtitleSegment[]>;
    private loadFile;
    private cacheKey;
    private cachePath;
    private readCache;
    private writeCache;
    private uploadSign;
    private uploadAuth;
    private uploadFile;
    private uploadCheck;
    private uploadCommit;
    private submit;
    private pollQuery;
    private query;
    private parseResponse;
}
export declare function transcribe(options: TranscribeOptions): Promise<SubtitleSegment[]>;
