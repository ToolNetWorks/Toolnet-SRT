// Neon transaction poolers may not retain a session search_path. Bind it
// transaction-locally on every transaction, including single pool queries.
export function createPublicPostgresPool(pool) {
  if (typeof pool?.connect !== 'function' || typeof pool?.end !== 'function') throw new Error('DATABASE_POOL_REQUIRED');
  async function connect() {
    const client = await pool.connect();
    let discard = false;
    return {
      async query(sql, params) {
        if (typeof sql === 'string' && /^\s*BEGIN\s*;?\s*$/i.test(sql)) {
          await client.query(sql, params);
          try { await client.query('SET LOCAL search_path TO public'); }
          catch (error) { await client.query('ROLLBACK').catch(() => { discard = true; }); throw error; }
          return;
        }
        return client.query(sql, params);
      },
      release(error) { client.release(error || (discard ? true : undefined)); },
    };
  }
  return {
    connect,
    async query(sql, params) {
      const client = await connect(); let open = false, discard = false, committing = false;
      try {
        await client.query('BEGIN'); open = true;
        const result = await client.query(sql, params);
        committing = true; await client.query('COMMIT'); open = false;
        return result;
      } catch (error) {
        if (committing) discard = true;
        else if (open) await client.query('ROLLBACK').catch(() => { discard = true; });
        throw error;
      } finally { client.release(discard ? true : undefined); }
    },
    end() { return pool.end(); },
    on(event, handler) { pool.on(event, handler); return this; },
  };
}
