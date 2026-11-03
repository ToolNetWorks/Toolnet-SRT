import test from "node:test";
import assert from "node:assert/strict";
import { createDurablePreflight } from "../server/storage/durable-preflight.mjs";

const columns = ["id", "status", "stage", "progress", "completed_segments", "total_segments",
  "source_key", "result_key", "error_code", "lease_owner", "lease_token", "lease_expires_at",
  "attempt_count", "available_at", "created_at", "updated_at"];
const env = { DURABLE_JOBS_ENABLED: "true", DATABASE_URL: "postgresql://test:secret@db.invalid/db",
  R2_ACCOUNT_ID: "a".repeat(32), R2_ACCESS_KEY_ID: "fake-access", R2_SECRET_ACCESS_KEY: "fake-secret",
  R2_BUCKET: "fake-bucket", TOOLNET_SRT_API_KEY: "fake-api-key" };

function harness({ table = "toolnet_srt_jobs", availableColumns = columns, r2Error = false } = {}) {
  const events = [];
  const pool = {
    async query(sql) {
      events.push("neon-query");
      if (sql.includes("to_regclass")) return { rows: [{ table_ref: table }] };
      if (sql.includes("pg_attribute")) return { rows: availableColumns.map(attname => ({ attname })) };
      return { rows: [{ ok: 1 }] };
    },
    async end() { events.push("neon-close"); },
  };
  const r2 = {
    async send() { events.push("r2-head"); if (r2Error) throw new Error("PRIVATE_R2_ERROR"); return {}; },
    destroy() { events.push("r2-close"); },
  };
  return { preflight: createDurablePreflight({ env, poolFactory: () => pool, r2Factory: () => r2 }), events };
}

test("Disabled mode does not connect", async () => {
  let connected = false;
  const preflight = createDurablePreflight({ env: { DURABLE_JOBS_ENABLED: "false" },
    poolFactory() { connected = true; throw new Error("SHOULD_NOT_CONNECT"); } });
  const result = await preflight.check();
  assert.equal(result.enabled, false);
  assert.equal(connected, false);
});

test("Neon and R2 preflight succeed", async () => {
  const { preflight, events } = harness();
  const result = await preflight.check();
  assert.equal(result.neon, "ready"); assert.equal(result.r2, "ready"); assert.equal(result.workerStarted, false);
  assert.ok(events.includes("r2-head")); assert.ok(events.includes("neon-close")); assert.ok(events.includes("r2-close"));
});

test("Missing Neon table fails closed", async () => {
  const { preflight, events } = harness({ table: null });
  await assert.rejects(preflight.check(), /NEON_JOBS_TABLE_MISSING/);
  assert.ok(!events.includes("r2-head")); assert.ok(events.includes("neon-close"));
});

test("Missing lease column fails closed", async () => {
  const { preflight, events } = harness({ availableColumns: columns.filter(column => column !== "lease_token") });
  await assert.rejects(preflight.check(), /NEON_MIGRATION_REQUIRED: lease_token/);
  assert.ok(!events.includes("r2-head"));
});

test("R2 failure does not leak credentials", async () => {
  const { preflight, events } = harness({ r2Error: true });
  await assert.rejects(preflight.check(), error => {
    assert.equal(error.message, "R2_BUCKET_ACCESS_FAILED");
    assert.ok(!error.message.includes("fake-secret"));
    return true;
  });
  assert.ok(events.includes("r2-close")); assert.ok(events.includes("neon-close"));
});
