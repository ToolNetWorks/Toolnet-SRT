import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createDurableLifecycle } from "../server/storage/durable-lifecycle.mjs";

async function until(check, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error("WAIT_TIMEOUT");
}
function createChild(graceMs = 3000) {
  const child = spawn(process.execPath, ["tests/fixtures/durable-lifecycle-child.mjs"], {
    cwd: process.cwd(), env: { ...process.env, TEST_GRACE_MS: String(graceMs) },
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  const messages = [];
  let exit = null, stderr = "";
  child.on("message", message => { messages.push(message); });
  child.on("exit", (code, signal) => { exit = { code, signal }; });
  child.stderr.on("data", chunk => { stderr += chunk.toString(); });
  return { child, messages, getExit: () => exit, getStderr: () => stderr };
}
async function cleanup(instance) {
  if (instance.getExit()) return;
  instance.child.kill("SIGKILL");
  await until(() => instance.getExit() !== null, 3000).catch(() => {});
}
function hasMessage(instance, type) { return instance.messages.some(message => message?.type === type); }

for (const signal of ["SIGTERM", "SIGINT"]) {
  test(`${signal} drains active worker`, async () => {
    const instance = createChild();
    try {
      await until(() => hasMessage(instance, "working"));
      assert.equal(instance.child.kill(signal), true);
      await new Promise(resolve => setTimeout(resolve, 80));
      assert.equal(hasMessage(instance, "finished"), false);
      assert.equal(instance.getExit(), null);
      instance.child.send("release");
      await until(() => hasMessage(instance, "drained"));
      await until(() => instance.getExit() !== null);
      assert.deepEqual(instance.getExit(), { code: 0, signal: null });
      const drained = instance.messages.find(message => message.type === "drained");
      assert.equal(drained.reason, signal);
      assert.equal(drained.calls, 1);
      assert.equal(hasMessage(instance, "timeout"), false);
      console.log(`${signal} GRACEFUL DRAIN: PASS`);
    } finally { await cleanup(instance); }
  });
}

test("Deadline does not terminate active worker", async () => {
  const instance = createChild(100);
  try {
    await until(() => hasMessage(instance, "working"));
    instance.child.kill("SIGTERM");
    await until(() => hasMessage(instance, "timeout"));
    assert.equal(instance.getExit(), null);
    assert.equal(hasMessage(instance, "finished"), false);
    instance.child.send("release");
    await until(() => hasMessage(instance, "drained"));
    await until(() => instance.getExit() !== null);
    assert.equal(instance.getExit().code, 0);
    console.log("TIMEOUT PRESERVES ACTIVE WORKER: PASS");
  } finally { await cleanup(instance); }
});

test("SIGKILL terminates process without graceful drain", async () => {
  const instance = createChild();
  try {
    await until(() => hasMessage(instance, "working"));
    instance.child.kill("SIGKILL");
    await until(() => instance.getExit() !== null);
    assert.equal(instance.getExit().signal, "SIGKILL");
    assert.equal(hasMessage(instance, "drained"), false);
    console.log("FORCED PROCESS TERMINATION: PASS");
  } finally { await cleanup(instance); }
});

test("Lifecycle rejects invalid configuration", () => {
  assert.throws(() => createDurableLifecycle(), /SCHEDULER_REQUIRED/);
  const scheduler = { start() {}, async stop() { return { stopped: true }; }, getStatus() { return { state: "stopped" }; } };
  assert.throws(() => createDurableLifecycle({ scheduler, gracePeriodMs: -1 }), /INVALID_GRACE_PERIOD/);
  assert.throws(() => createDurableLifecycle({ scheduler, signals: ["SIGKILL"] }), /INVALID_SIGNALS/);
  console.log("LIFECYCLE VALIDATION: PASS");
});
