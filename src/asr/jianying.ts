import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { crc32, md5 } from "../utils/crypto.js";
import { awsSign } from "../utils/aws-sign.js";
import type {
  JianYingResponse,
  SubtitleSegment,
  SubtitleWord,
  TranscribeOptions,
} from "./types.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const API_BASE = "https://lv-pc-api-sinfonlinec.ulikecam.com";
const VOD_BASE = "https://vod.bytedanceapi.com";
const APP_VERSION = "6.6.0";
const PF = "4";

const UA_SIGN =
  "Cronet/TTNetVersion:d4572e53 2024-06-12 QuicVersion:4bf243e0 2023-04-17";
const UA_UPLOAD =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 Thea/1.0.1";

const SUPPORTED_EXTS = new Set(["mp3", "wav", "flac", "m4a"]);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function generateTdid(): string {
  const yearLastDigit = String(new Date().getFullYear())[3];
  const fr = 390 + Number(yearLastDigit);
  const odd = Number(yearLastDigit) % 2 !== 0;
  const ed = odd ? "3278516897751" : String(os.networkInterfaces() ? getMacNode() : 0).padStart(13, "0");
  return `${fr}${ed}`;
}

function getMacNode(): number {
  const ifaces = os.networkInterfaces();
  for (const iface of Object.values(ifaces)) {
    if (!iface) continue;
    for (const detail of iface) {
      if (detail.mac && detail.mac !== "00:00:00:00:00:00") {
        return parseInt(detail.mac.replace(/:/g, ""), 16);
      }
    }
  }
  return 0;
}

function generateSignParams(
  url: string,
  tdid: string,
  pf: string = PF,
  appvr: string = APP_VERSION,
) {
  const deviceTime = String(Math.floor(Date.now() / 1000));
  const v = url.length >= 7 ? url.slice(-7) : url;
  const raw = `9e2c|${v}|${pf}|${appvr}|${deviceTime}|${tdid}|11ac`;
  return { sign: md5(raw), deviceTime };
}

function buildHeaders(deviceTime: string, sign: string, tdid: string) {
  return {
    "User-Agent": UA_SIGN,
    appvr: APP_VERSION,
    "device-time": deviceTime,
    pf: PF,
    sign,
    "sign-ver": "1",
    tdid,
  };
}

// ---------------------------------------------------------------------------
// Main class
// ---------------------------------------------------------------------------

export class JianYingASR {
  private tdid: string;
  private crc32Hex!: string;
  private fileBuffer!: Buffer;
  private fileSize!: number;

  // AWS credentials from upload_sign
  private accessKey?: string;
  private secretKey?: string;
  private sessionToken?: string;

  // Upload details
  private storeUri?: string;
  private uploadAuthToken?: string;
  private uploadId?: string;
  private uploadHost?: string;

  // Cache
  private cacheDir: string;

  constructor(private options: TranscribeOptions) {
    this.tdid = generateTdid();
    this.cacheDir = options.cacheDir ?? path.join(os.homedir(), ".cache", "jianying-subtitle");
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  /**
   * Run full ASR pipeline: upload → submit → query → parse.
   */
  async transcribe(): Promise<SubtitleSegment[]> {
    const { onProgress } = this.options;

    // Load file
    await this.loadFile();

    // Check cache
    if (this.options.cache) {
      const cached = this.readCache();
      if (cached) {
        onProgress?.(100, "从缓存读取");
        return cached;
      }
    }

    // 1. Upload
    onProgress?.(20, "正在获取上传凭证...");
    await this.uploadSign();
    onProgress?.(30, "正在获取上传授权...");
    await this.uploadAuth();
    onProgress?.(40, "正在上传音频...");
    await this.uploadFile();
    onProgress?.(50, "正在提交任务...");
    await this.uploadCheck();
    await this.uploadCommit();

    // 2. Submit
    onProgress?.(60, "正在提交识别任务...");
    const taskId = await this.submit();

    // 3. Query
    onProgress?.(70, "正在等待识别结果...");
    const resp = await this.pollQuery(taskId);

    // 4. Parse
    onProgress?.(90, "正在解析结果...");
    const segments = this.parseResponse(resp);

    // Write cache
    if (this.options.cache) {
      this.writeCache(segments);
    }

    onProgress?.(100, "转录完成");
    return segments;
  }

  // -----------------------------------------------------------------------
  // File loading
  // -----------------------------------------------------------------------

  private async loadFile(): Promise<void> {
    const { input, __audioBuffer } = this.options;

    if (__audioBuffer) {
      this.fileBuffer = __audioBuffer;
      this.fileSize = __audioBuffer.length;
    } else {
      const stat = fs.statSync(input);
      if (!stat.isFile()) {
        throw new Error(`File not found: ${input}`);
      }

      const ext = path.extname(input).slice(1).toLowerCase();
      if (!SUPPORTED_EXTS.has(ext)) {
        throw new Error(
          `Unsupported format: .${ext}. Supported: ${[...SUPPORTED_EXTS].join(", ")}`,
        );
      }

      this.fileBuffer = fs.readFileSync(input);
      this.fileSize = stat.size;
    }

    this.crc32Hex = crc32(this.fileBuffer);
  }

  // -----------------------------------------------------------------------
  // Cache
  // -----------------------------------------------------------------------

  private cacheKey(): string {
    return `JianYingASR-${this.crc32Hex}-${this.options.wordTimestamps ? "1" : "0"}`;
  }

  private cachePath(): string {
    return path.join(this.cacheDir, `${this.cacheKey()}.json`);
  }

  private readCache(): SubtitleSegment[] | null {
    try {
      const p = this.cachePath();
      if (fs.existsSync(p)) {
        const data = JSON.parse(fs.readFileSync(p, "utf-8")) as SubtitleSegment[];
        return data;
      }
    } catch {
      // ignore
    }
    return null;
  }

  private writeCache(segments: SubtitleSegment[]): void {
    try {
      fs.mkdirSync(this.cacheDir, { recursive: true });
      fs.writeFileSync(this.cachePath(), JSON.stringify(segments, null, 2));
    } catch {
      // ignore
    }
  }

  // -----------------------------------------------------------------------
  // Step 1: Upload sign – get AWS credentials
  // -----------------------------------------------------------------------

  private async uploadSign(): Promise<void> {
    const url = `${API_BASE}/lv/v1/upload_sign`;
    const payload = JSON.stringify({ biz: "pc-recognition" });
    const { sign, deviceTime } = generateSignParams("/lv/v1/upload_sign", this.tdid);
    const headers = buildHeaders(deviceTime, sign, this.tdid);

    const res = await fetch(url, { method: "POST", body: payload, headers });
    const json = (await res.json()) as any;

    if (json.ret !== "0") {
      throw new Error(`Upload sign failed (ret=${json.ret}): ${JSON.stringify(json).slice(0, 500)}`);
    }
    this.accessKey = json.data.access_key_id;
    this.secretKey = json.data.secret_access_key;
    this.sessionToken = json.data.session_token;
  }

  // -----------------------------------------------------------------------
  // Step 2: Upload auth – get upload destination
  // -----------------------------------------------------------------------

  private async uploadAuth(): Promise<void> {
    if (!this.secretKey || !this.accessKey || !this.sessionToken) {
      throw new Error("Credentials not initialized");
    }

    const requestParams =
      `Action=ApplyUploadInner&FileSize=${this.fileSize}&FileType=object&IsInner=1&SpaceName=lv-mac-recognition&Version=2020-11-19&s=5y0udbjapi`;

    const signResult = awsSign({
      secretKey: this.secretKey,
      accessKey: this.accessKey,
      sessionToken: this.sessionToken,
      requestParams,
    });

    const headers: Record<string, string> = {
      ...signResult.headers,
      authorization: signResult.authorization,
    };

    const res = await fetch(`${VOD_BASE}/?${requestParams}`, {
      method: "GET",
      headers,
    });
    const text = await res.text();
    let json: any;
    try {
      json = JSON.parse(text);
    } catch {
      throw new Error(`Upload auth returned non-JSON (HTTP ${res.status}): ${text.slice(0, 500)}`);
    }

    if (!json.Result?.UploadAddress) {
      throw new Error(
        `Upload auth failed (HTTP ${res.status}): ${JSON.stringify(json).slice(0, 500)}`,
      );
    }

    const storeInfo = json.Result.UploadAddress.StoreInfos[0];
    this.storeUri = storeInfo.StoreUri;
    this.uploadAuthToken = storeInfo.Auth;
    this.uploadId = storeInfo.UploadID;
    this.uploadHost = json.Result.UploadAddress.UploadHosts[0];
  }

  // -----------------------------------------------------------------------
  // Step 3: Upload file
  // -----------------------------------------------------------------------

  private async uploadFile(): Promise<void> {
    const url = `https://${this.uploadHost}/${this.storeUri}?partNumber=1&uploadID=${this.uploadId}`;
    const headers: Record<string, string> = {
      "User-Agent": UA_UPLOAD,
      Authorization: this.uploadAuthToken!,
      "Content-CRC32": this.crc32Hex,
    };

    const res = await fetch(url, {
      method: "PUT",
      body: new Uint8Array(this.fileBuffer),
      headers,
    });
    const json = (await res.json()) as { success: number };
    if (json.success !== 0) {
      throw new Error(`File upload failed: ${JSON.stringify(json)}`);
    }
  }

  // -----------------------------------------------------------------------
  // Step 4: Upload check
  // -----------------------------------------------------------------------

  private async uploadCheck(): Promise<void> {
    const url = `https://${this.uploadHost}/${this.storeUri}?uploadID=${this.uploadId}`;
    const headers: Record<string, string> = {
      "User-Agent": UA_UPLOAD,
      Authorization: this.uploadAuthToken!,
      "Content-CRC32": this.crc32Hex,
    };

    const payload = `1:${this.crc32Hex}`;
    const res = await fetch(url, { method: "POST", body: payload, headers });
    await res.json();
  }

  // -----------------------------------------------------------------------
  // Step 5: Upload commit
  // -----------------------------------------------------------------------

  private async uploadCommit(): Promise<void> {
    const url =
      `https://${this.uploadHost}/${this.storeUri}?uploadID=${this.uploadId}` +
      `&partNumber=1&x-amz-security-token=${this.sessionToken}`;
    const headers: Record<string, string> = {
      "User-Agent": UA_UPLOAD,
      Authorization: this.uploadAuthToken!,
      "Content-CRC32": this.crc32Hex,
    };

    await fetch(url, {
      method: "PUT",
      body: new Uint8Array(this.fileBuffer),
      headers,
    });
  }

  // -----------------------------------------------------------------------
  // Step 6: Submit task
  // -----------------------------------------------------------------------

  private async submit(): Promise<string> {
    const payload = {
      adjust_endtime: 200,
      audio: this.storeUri,
      caption_type: 2,
      client_request_id: "45faf98c-160f-4fae-a649-6d89b0fe35be",
      max_lines: 1,
      songs_info: [{ end_time: 6000, id: "", start_time: 0 }],
      words_per_line: 16,
    };

    const { sign, deviceTime } = generateSignParams(
      "/lv/v1/audio_subtitle/submit",
      this.tdid,
    );
    const headers = buildHeaders(deviceTime, sign, this.tdid);

    const res = await fetch(`${API_BASE}/lv/v1/audio_subtitle/submit`, {
      method: "POST",
      body: JSON.stringify(payload),
      headers,
    });
    const json = (await res.json()) as { ret: string; errmsg?: string; data: { id: string } };

    if (json.ret !== "0") {
      throw new Error(
        `Submit failed: ${json.errmsg ?? "Unknown error"} (ret: ${json.ret})`,
      );
    }

    return json.data.id;
  }

  // -----------------------------------------------------------------------
  // Step 7: Query result (poll until done)
  // -----------------------------------------------------------------------

  private async pollQuery(taskId: string): Promise<JianYingResponse> {
    const { queryTimeoutMs = 300_000, queryIntervalMs = 2_000 } = this.options;
    const deadline = Date.now() + queryTimeoutMs;

    while (Date.now() < deadline) {
      const resp = await this.query(taskId);
      if (resp.data) return resp;
      // If still processing, retry
      await sleep(queryIntervalMs);
    }

    throw new Error("Query timed out");
  }

  private async query(taskId: string): Promise<JianYingResponse> {
    const payload = { id: taskId, pack_options: { need_attribute: true } };
    const { sign, deviceTime } = generateSignParams(
      "/lv/v1/audio_subtitle/query",
      this.tdid,
    );
    const headers = buildHeaders(deviceTime, sign, this.tdid);

    const res = await fetch(`${API_BASE}/lv/v1/audio_subtitle/query`, {
      method: "POST",
      body: JSON.stringify(payload),
      headers,
    });
    const json = (await res.json()) as JianYingResponse;

    if (json.ret !== "0") {
      throw new Error(
        `Query failed: ${json.errmsg ?? "Unknown error"} (ret: ${json.ret})`,
      );
    }
    return json;
  }

  // -----------------------------------------------------------------------
  // Step 8: Parse response
  // -----------------------------------------------------------------------

  private parseResponse(resp: JianYingResponse): SubtitleSegment[] {
    const { wordTimestamps } = this.options;

    if (wordTimestamps) {
      const words: SubtitleSegment[] = [];
      for (const utterance of resp.data?.utterances ?? []) {
        for (const w of utterance.words ?? []) {
          words.push({
            text: w.text.trim(),
            startMs: w.start_time,
            endMs: w.end_time,
          });
        }
      }
      return words;
    }

    return (resp.data?.utterances ?? []).map((u) => ({
      text: u.text,
      startMs: u.start_time,
      endMs: u.end_time,
    }));
  }
}

// ---------------------------------------------------------------------------
// Standalone function
// ---------------------------------------------------------------------------

export async function transcribe(
  options: TranscribeOptions,
): Promise<SubtitleSegment[]> {
  const asr = new JianYingASR(options);
  return asr.transcribe();
}

// ---------------------------------------------------------------------------
// Utils
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
