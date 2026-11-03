import test from "node:test";
import assert from "node:assert/strict";
import { readDurableConfig } from "../server/storage/durable-config.mjs";

const valid = { DURABLE_JOBS_ENABLED: "true", DATABASE_URL: "postgresql://test:secret@db.invalid/dbname",
  R2_ACCOUNT_ID: "test-account-id", R2_ACCESS_KEY_ID: "test-access-key", R2_SECRET_ACCESS_KEY: "secret-access-value",
  R2_BUCKET: "test-bucket", TOOLNET_SRT_API_KEY: "secret-api-key" };

test("Disabled by default", () => { assert.deepEqual(readDurableConfig({}), { enabled: false }); });
test("Invalid flag rejected", () => { assert.throws(() => readDurableConfig({ DURABLE_JOBS_ENABLED: "1" }), /INVALID_DURABLE_JOBS_ENABLED/); });
test("Missing credentials rejected", () => { assert.throws(() => readDurableConfig({ DURABLE_JOBS_ENABLED: "true" }), /MISSING_DURABLE_CONFIGURATION/); });
test("Invalid database URL rejected", () => {
  assert.throws(() => readDurableConfig({ ...valid, DATABASE_URL: "https://invalid.example/db" }), /INVALID_DATABASE_URL/);
});
test("Valid defaults and no secret exposure", () => {
  const config = readDurableConfig(valid);
  assert.equal(config.enabled, true); assert.equal(config.concurrency, 1); assert.equal(config.leaseSeconds, 120);
  assert.equal(config.maxAttempts, 3); assert.equal(Object.isFrozen(config), true);
  const serialized = JSON.stringify(config);
  for (const secret of ["test-access-key", "secret-access-value", "secret-api-key", "test:secret"]) assert.ok(!serialized.includes(secret));
});
test("Custom worker configuration", () => {
  const config = readDurableConfig({ ...valid, DURABLE_WORKER_CONCURRENCY: "2", DURABLE_POLL_INTERVAL_MS: "250",
    DURABLE_JOB_LEASE_SECONDS: "30", DURABLE_HEARTBEAT_MS: "5000" });
  assert.equal(config.concurrency, 2); assert.equal(config.pollIntervalMs, 250); assert.equal(config.leaseSeconds, 30);
});
test("Invalid numeric values rejected", () => {
  for (const value of ["1.5", "-1", "abc", "33"]) {
    assert.throws(() => readDurableConfig({ ...valid, DURABLE_WORKER_CONCURRENCY: value }), /INVALID_DURABLE_WORKER_CONCURRENCY/);
  }
});
test("Unsafe heartbeat interval rejected", () => {
  assert.throws(() => readDurableConfig({ ...valid, DURABLE_JOB_LEASE_SECONDS: "30", DURABLE_HEARTBEAT_MS: "20000" }),
    /HEARTBEAT_MUST_BE_LESS_THAN_HALF_LEASE/);
});
