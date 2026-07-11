import type { JianYingResponse, SubtitleSegment } from "./types.js";
import type { TranscribeOptions } from "./types.js";
import { API_BASE } from "./constants.js";
import { generateSignParams, buildHeaders, sleep } from "./helpers.js";

/**
 * Submit a transcription task to the JianYing API.
 *
 * Returns the task ID used to poll for results.
 */
export async function submitTask(
  storeUri: string,
  tdid: string,
  checkedFetch: (url: string, init: RequestInit) => Promise<Response>,
): Promise<string> {
  const payload = {
    adjust_endtime: 200,
    audio: storeUri,
    caption_type: 2,
    client_request_id: "45faf98c-160f-4fae-a649-6d89b0fe35be",
    max_lines: 1,
    songs_info: [{ end_time: 6000, id: "", start_time: 0 }],
    words_per_line: 16,
  };

  const { sign, deviceTime } = generateSignParams(
    "/lv/v1/audio_subtitle/submit",
    tdid,
  );
  const headers = {
    ...buildHeaders(deviceTime, sign, tdid),
    "Content-Type": "application/json",
  };

  const res = await checkedFetch(`${API_BASE}/lv/v1/audio_subtitle/submit`, {
    method: "POST",
    body: JSON.stringify(payload),
    headers,
  });
  const json = (await res.json()) as {
    ret: string;
    errmsg?: string;
    data?: { id?: unknown } | null;
  };

  if (json.ret !== "0") {
    throw new Error(
      `Submit failed: ${json.errmsg ?? "Unknown error"} (ret: ${json.ret})`,
    );
  }

  if (typeof json.data?.id !== "string" || json.data.id.trim() === "") {
    throw new Error(
      `Malformed submit response: missing task id: ${JSON.stringify(json).slice(0, 500)}`,
    );
  }

  return json.data.id;
}

/** Query the ASR result for a given task ID. */
async function queryTask(
  taskId: string,
  tdid: string,
  checkedFetch: (url: string, init: RequestInit) => Promise<Response>,
): Promise<JianYingResponse> {
  const payload = { id: taskId, pack_options: { need_attribute: true } };
  const { sign, deviceTime } = generateSignParams(
    "/lv/v1/audio_subtitle/query",
    tdid,
  );
  const headers = {
    ...buildHeaders(deviceTime, sign, tdid),
    "Content-Type": "application/json",
  };

  const res = await checkedFetch(`${API_BASE}/lv/v1/audio_subtitle/query`, {
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

/**
 * Poll the query endpoint until the transcription is ready.
 *
 * When `data` is `null` the task is still processing and we retry
 * after `queryIntervalMs`. If the timeout (`queryTimeoutMs`) is
 * reached before data arrives an error is thrown.
 */
export async function pollForResult(
  taskId: string,
  tdid: string,
  checkedFetch: (url: string, init: RequestInit) => Promise<Response>,
  options: Pick<TranscribeOptions, "queryTimeoutMs" | "queryIntervalMs" | "signal">,
): Promise<JianYingResponse> {
  const { queryTimeoutMs = 300_000, queryIntervalMs = 2_000, signal } = options;
  const deadline = Date.now() + queryTimeoutMs;

  while (Date.now() < deadline) {
    if (signal?.aborted) throw new Error("Cancelled");
    const resp = await queryTask(taskId, tdid, checkedFetch);
    if (resp.data == null) {
      await sleep(queryIntervalMs, signal);
      continue;
    }
    if (!Array.isArray(resp.data.utterances)) {
      throw new Error(
        `Malformed query response: missing utterances: ${JSON.stringify(resp).slice(0, 500)}`,
      );
    }
    return resp;
  }

  throw new Error("Query timed out");
}

/**
 * Parse a completed ASR response into {@link SubtitleSegment} objects.
 *
 * Each segment carries the utterance text, start/end time in
 * milliseconds, and word-level timestamps when the API provides them.
 */
export function parseSegments(resp: JianYingResponse): SubtitleSegment[] {
  if (!Array.isArray(resp.data?.utterances)) {
    throw new Error(
      `Malformed query response: missing utterances: ${JSON.stringify(resp).slice(0, 500)}`,
    );
  }

  return resp.data.utterances.map((u) => ({
    text: u.text,
    startMs: u.start_time,
    endMs: u.end_time,
    words: Array.isArray(u.words)
      ? u.words.map((w) => ({
          text: w.text.trim(),
          startMs: w.start_time,
          endMs: w.end_time,
        }))
      : undefined,
  }));
}
