import test from 'node:test';
import assert from 'node:assert/strict';
import { createPublicPostgresPool } from '../server/storage/postgres-public-pool.mjs';

function harness(fail = '') {
  const calls=[];
  const raw={ async query(sql) { calls.push(sql); if(sql===fail) throw Error('query failed'); return { rows:[{ ok:1 }] }; },
    release(error) { calls.push(error ? 'discard' : 'release'); } };
  return { calls, pool:createPublicPostgresPool({ async connect() { return raw; }, async end() {}, on() {} }) };
}
test('Standalone queries select public inside a transaction', async () => {
  const h=harness(); await h.pool.query('SELECT 1');
  assert.deepEqual(h.calls,['BEGIN','SET LOCAL search_path TO public','SELECT 1','COMMIT','release']);
});
test('Explicit transactions select public before queries', async () => {
  const h=harness(), c=await h.pool.connect(); await c.query('BEGIN'); await c.query('SELECT 1'); await c.query('COMMIT'); c.release();
  assert.deepEqual(h.calls,['BEGIN','SET LOCAL search_path TO public','SELECT 1','COMMIT','release']);
});
test('Failure rolls back and failed commit discards connection', async () => {
  const h=harness('SELECT 1'); await assert.rejects(h.pool.query('SELECT 1'));
  assert.deepEqual(h.calls,['BEGIN','SET LOCAL search_path TO public','SELECT 1','ROLLBACK','release']);
  const j=harness('COMMIT'); await assert.rejects(j.pool.query('SELECT 1'));
  assert.deepEqual(j.calls,['BEGIN','SET LOCAL search_path TO public','SELECT 1','COMMIT','discard']);
});
