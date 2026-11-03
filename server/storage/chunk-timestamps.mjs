// Timestamps are expressed on the source presentation timeline in milliseconds.
function textKey(value) { return value.normalize('NFKC').trim().replace(/\s+/g, ' ').toLocaleLowerCase(); }
export function mergeChunkSegments(chunks, sourceDurationMs) {
  if (!Number.isSafeInteger(sourceDurationMs) || sourceDurationMs < 1) throw new Error('INVALID_SOURCE_DURATION');
  if (!Array.isArray(chunks)) throw new Error('INVALID_CHUNKS');
  const entries = [];
  for (const chunk of chunks) {
    if (!Number.isSafeInteger(chunk.index) || chunk.index < 0 || !Number.isSafeInteger(chunk.startOffsetMs) || chunk.startOffsetMs < 0 ||
        !Number.isSafeInteger(chunk.durationMs) || chunk.durationMs < 1 || !Number.isSafeInteger(chunk.overlapMs) || chunk.overlapMs < 0 ||
        chunk.overlapMs >= chunk.durationMs || !Array.isArray(chunk.segments)) throw new Error('INVALID_CHUNK_TIMELINE');
    for (const item of chunk.segments) {
      if (typeof item?.text !== 'string' || !item.text.trim() || !Number.isFinite(item.startMs) || !Number.isFinite(item.endMs)) continue;
      const start = Math.max(0, Math.round(item.startMs));
      const end = Math.min(chunk.durationMs, Math.round(item.endMs));
      const startMs = Math.max(0, Math.min(sourceDurationMs, chunk.startOffsetMs + start));
      const endMs = Math.max(0, Math.min(sourceDurationMs, chunk.startOffsetMs + end));
      if (endMs <= startMs) continue;
      entries.push({ text: item.text.trim(), startMs, endMs, chunkIndex: chunk.index, overlapMs: chunk.overlapMs });
    }
  }
  entries.sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs || a.chunkIndex - b.chunkIndex);
  const kept = [], activeByText = new Map();
  for (const item of entries) {
    // Only remove exact normalized matches that overlap in time across chunks.
    const key = textKey(item.text);
    const candidates = (activeByText.get(key) ?? []).filter(previous => previous.endMs > item.startMs);
    const duplicate = candidates.some(previous => {
      const intersection = Math.min(previous.endMs, item.endMs) - Math.max(previous.startMs, item.startMs);
      const shorter = Math.min(previous.endMs - previous.startMs, item.endMs - item.startMs);
      return previous.chunkIndex !== item.chunkIndex &&
        intersection > 0 && intersection / shorter >= 0.5 && (previous.overlapMs > 0 || item.overlapMs > 0);
    });
    if (!duplicate) { kept.push(item); candidates.push(item); }
    activeByText.set(key,candidates);
  }
  return kept.map(({ text, startMs, endMs }) => ({ text, startMs, endMs }));
}
