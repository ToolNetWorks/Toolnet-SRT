import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createRemoteOrphanCleanup } from '../server/storage/remote-orphan-cleanup.mjs';
const jobId = randomUUID(), stale = randomUUID(), winner = randomUUID(), active = randomUUID();
const key = token => `toolnet-srt/jobs/${jobId}/remote/${token}/source.mp4`;

test('Only old, unreferenced attempts with a different lease are deleted', async () => {
  const deleted = [], statements = [];
  let row = { source_key: key(winner), lease_token: active, job_status: 'queued' };
  const pool = { async connect() { return { async query(sql) {
    statements.push(sql);
    if (sql.includes('FOR UPDATE')) return { rows: [row] };
    return { rows: [] };
  }, release() {} }; } };
  const old = new Date(Date.now() - 2 * 86400000);
  const client = { async send(command) {
    if (command.constructor.name === 'ListObjectsV2Command') return { Contents: [
      ...[stale, winner, active].map(token => ({ Key: key(token), LastModified: old })),
      { Key: key(randomUUID()), LastModified: new Date() },
    ] };
    deleted.push(command.input.Key); return {};
  } };
  const cleanup = createRemoteOrphanCleanup({ pool, client, bucket: 'test' });
  const result = await cleanup.sweep();
  assert.deepEqual(deleted, [key(stale)]); assert.equal(result.deleted, 1);
  assert.ok(statements.some(sql => sql.includes('FOR UPDATE OF j, q')));
  row = null; await cleanup.sweep(); assert.deepEqual(deleted, [key(stale)]);
});

test('Database errors never result in object deletion', async () => {
  let deleted = false;
  const pool = { async connect() { return { async query(sql) { if (sql.includes('FOR UPDATE')) throw Error('db'); return { rows: [] }; }, release() {} }; } };
  const client = { async send(command) { if (command.constructor.name === 'ListObjectsV2Command') return { Contents: [{ Key: key(stale), LastModified: new Date(Date.now()-2*86400000) }] }; deleted = true; } };
  await assert.rejects(createRemoteOrphanCleanup({ pool, client, bucket: 'test' }).sweep(), /REMOTE_CLEANUP_FAILED/);
  assert.equal(deleted, false);
});

test('An object referenced by any other job is preserved', async () => {
  let deleted = false;
  const pool = { async connect() { return { async query(sql) {
    if (sql.includes('FOR UPDATE')) return { rows: [{ source_key: null, lease_token: null, job_status: 'queued' }] };
    if (sql.includes('WHERE source_key')) return { rows: [{}] };
    return { rows: [] };
  }, release() {} }; } };
  const client = { async send(command) {
    if (command.constructor.name === 'ListObjectsV2Command') return { Contents: [{ Key: key(stale), LastModified: new Date(Date.now()-2*86400000) }] };
    deleted = true;
  } };
  assert.equal((await createRemoteOrphanCleanup({ pool, client, bucket: 'test' }).sweep()).deleted, 0);
  assert.equal(deleted, false);
});
