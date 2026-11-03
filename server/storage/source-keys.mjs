const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SOURCE_RE = /^source\.(?:mp3|mp4|wav|m4a|flac|ogg|mov|mkv|webm)$/;

function validateParts(jobId, filename) {
  if (typeof jobId !== "string" || !UUID_RE.test(jobId)) throw new Error("INVALID_JOB_ID");
  if (typeof filename !== "string" || !SOURCE_RE.test(filename)) throw new Error("INVALID_STORAGE_FILENAME");
}

export function remoteSourceKey(jobId, fetchToken, filename) {
  validateParts(jobId, filename);
  if (typeof fetchToken !== "string" || !UUID_RE.test(fetchToken)) throw new Error("INVALID_FETCH_TOKEN");
  return `toolnet-srt/jobs/${jobId}/remote/${fetchToken}/${filename}`;
}

export function validateSourceKey(jobId, filename, key) {
  validateParts(jobId, filename);
  if (typeof key !== "string") throw new Error("INVALID_SOURCE_KEY");
  if (key === `toolnet-srt/jobs/${jobId}/${filename}`) return key;
  const prefix = `toolnet-srt/jobs/${jobId}/remote/`;
  if (!key.startsWith(prefix)) throw new Error("INVALID_SOURCE_KEY");
  const parts = key.slice(prefix.length).split("/");
  if (parts.length !== 2 || !UUID_RE.test(parts[0]) || parts[1] !== filename) throw new Error("INVALID_SOURCE_KEY");
  return key;
}
