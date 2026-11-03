import { DeleteObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';

const keyPattern = /^toolnet-srt\/jobs\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/remote\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/source\.(?:mp3|mp4|wav|m4a|flac|ogg|mov|mkv|webm)$/;

// Never delete objects without a matching queue row: ambiguous state is retained.
// The job and queue row remain locked during the bounded R2 delete so a
// concurrent completion cannot publish the candidate between check and delete.
export function createRemoteOrphanCleanup({ pool, client, bucket, minAgeMs = 86400000, deleteTimeoutMs = 10000 } = {}) {
  if (typeof pool?.connect !== 'function') throw new Error('REMOTE_CLEANUP_POOL_REQUIRED');
  if (typeof client?.send !== 'function' || typeof bucket !== 'string' || !bucket) throw new Error('REMOTE_CLEANUP_STORAGE_REQUIRED');
  if (!Number.isSafeInteger(minAgeMs) || minAgeMs < 86400000) throw new Error('INVALID_ORPHAN_MIN_AGE');
  if (!Number.isSafeInteger(deleteTimeoutMs) || deleteTimeoutMs < 1 || deleteTimeoutMs > 60000) throw new Error('INVALID_ORPHAN_DELETE_TIMEOUT');
  async function inspectAndDelete(key) {
    const match = keyPattern.exec(key);
    if (!match) return false;
    const db = await pool.connect();
    let transaction = false, uncertain = false;
    try {
      await db.query('BEGIN'); transaction = true;
      const result = await db.query(`SELECT j.source_key, j.status AS job_status, q.lease_token
        FROM toolnet_srt_jobs j JOIN toolnet_srt_remote_fetch_jobs q ON q.job_id = j.id
        WHERE j.id = $1::uuid FOR UPDATE OF j, q`, [match[1]]);
      const row = result.rows[0];
      if (!row || row.source_key === key || (row.lease_token === match[2] && row.job_status === 'queued')) {
        await db.query('COMMIT'); transaction = false; return false;
      }
      const references = await db.query('SELECT 1 FROM toolnet_srt_jobs WHERE source_key = $1 LIMIT 1', [key]);
      if (references.rows.length) { await db.query('COMMIT'); transaction = false; return false; }
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), deleteTimeoutMs);
      try { await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }), { abortSignal: controller.signal }); }
      finally { clearTimeout(timer); }
      await db.query('COMMIT'); transaction = false;
      return true;
    } catch {
      if (transaction) await db.query('ROLLBACK').catch(() => { uncertain = true; });
      throw new Error('REMOTE_CLEANUP_FAILED');
    } finally { db.release(uncertain ? true : undefined); }
  }
  async function sweep({ now = Date.now(), maxPages = 10, continuationToken } = {}) {
    if (!Number.isFinite(now) || !Number.isSafeInteger(maxPages) || maxPages < 1 || maxPages > 100 ||
        (continuationToken !== undefined && typeof continuationToken !== 'string')) throw new Error('INVALID_CLEANUP_OPTIONS');
    let continuation = continuationToken, scanned = 0, deleted = 0, pages = 0;
    do {
      const page = await client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: 'toolnet-srt/jobs/',
        MaxKeys: 1000, ...(continuation ? { ContinuationToken: continuation } : {}) }));
      for (const item of page.Contents ?? []) {
        if (!item.Key || !keyPattern.test(item.Key) || !(item.LastModified instanceof Date) ||
            !Number.isFinite(item.LastModified.getTime()) || now - item.LastModified.getTime() < minAgeMs) continue;
        scanned++;
        if (await inspectAndDelete(item.Key)) deleted++;
      }
      continuation = page.IsTruncated ? page.NextContinuationToken : null;
      if (page.IsTruncated && !continuation) throw new Error('REMOTE_CLEANUP_PAGINATION_FAILED');
      pages++;
    } while (continuation && pages < maxPages);
    return { scanned, deleted, continuation: continuation ?? null };
  }
  return { sweep };
}
