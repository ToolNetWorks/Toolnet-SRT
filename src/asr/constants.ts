/**
 * Base URL for the JianYing (CapCut) cloud ASR API.
 * All signed requests go through this endpoint.
 */
export const API_BASE = "https://lv-pc-api-sinfonlinec.ulikecam.com";

/**
 * ByteDance VOD upload endpoint. AWS S3-compatible API used to
 * stage the audio file before transcription.
 */
export const VOD_BASE = "https://vod.bytedanceapi.com";

/** Client version reported in API headers. */
export const APP_VERSION = "6.6.0";

/** Platform identifier reported in API headers. */
export const PF = "4";

/**
 * User-Agent string for signed API calls.
 * Mimics the official CapCut Android client.
 */
export const UA_SIGN =
  "Cronet/TTNetVersion:d4572e53 2024-06-12 QuicVersion:4bf243e0 2023-04-17";

/**
 * User-Agent string for file upload calls (Chrome-based, Thea wrapper).
 */
export const UA_UPLOAD =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 Thea/1.0.1";

/** Audio file extensions accepted by the ASR API. */
export const SUPPORTED_EXTS = new Set(["mp3", "wav", "flac", "m4a"]);
