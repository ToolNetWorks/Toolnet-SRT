// Test-process-only PostgreSQL isolation. No production code imports this file.
import pg from 'pg';
const schema = process.env.TEST_SERVER_SCHEMA;
if (!/^srt_http_server_[a-f0-9]{12}$/.test(schema ?? '')) throw Error('INVALID_TEST_SCHEMA');
const originalConnect = pg.Pool.prototype.connect;
const rewrite = sql => typeof sql === 'string' ? sql.replaceAll('public.', `"${schema}".`)
  .replace(/^SET LOCAL search_path TO public$/i, `SET LOCAL search_path TO "${schema}"`) : sql;
pg.Pool.prototype.connect = async function() {
  const raw = await originalConnect.call(this);
  return {
    async query(sql, params) {
      if (sql === 'BEGIN') {
        try { await raw.query('BEGIN'); await raw.query(`SET LOCAL search_path TO "${schema}"`); }
        catch (error) { await raw.query('ROLLBACK').catch(() => {}); throw error; }
        return;
      }
      return raw.query(rewrite(sql), params);
    },
    release(error) { raw.release(error); },
  };
};
pg.Pool.prototype.query = async function(sql, params) {
  const client = await this.connect();
  try { await client.query('BEGIN'); const result = await client.query(sql, params); await client.query('COMMIT'); return result; }
  catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
  finally { client.release(); }
};
