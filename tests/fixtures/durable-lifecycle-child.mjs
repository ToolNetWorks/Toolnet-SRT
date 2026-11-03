import { createDurableScheduler } from "../../server/storage/durable-scheduler.mjs";
import { createDurableLifecycle } from "../../server/storage/durable-lifecycle.mjs";

let releaseAsr, calls = 0;
const asrGate = new Promise(resolve => { releaseAsr = resolve; });
function report(message) {
  return new Promise(resolve => {
    if (!process.send || !process.connected) { resolve(); return; }
    process.send(message, () => resolve());
  });
}
const scheduler = createDurableScheduler({
  pollIntervalMs: 20, errorBackoffMs: 20,
  workerFactory() {
    return { async runOnce() {
      calls++;
      if (calls > 1) return { status: "idle" };
      await report({ type: "working", calls });
      await asrGate;
      await report({ type: "finished", calls });
      return { status: "completed" };
    } };
  },
});
const lifecycle = createDurableLifecycle({
  scheduler,
  gracePeriodMs: Number(process.env.TEST_GRACE_MS ?? 3000),
  async onDrained({ reason }) {
    await report({ type: "drained", reason, calls });
    if (process.connected) process.disconnect();
  },
  async onTimeout({ reason }) { await report({ type: "timeout", reason, calls }); },
});
process.on("message", message => {
  if (message !== "release") return;
  releaseAsr();
  if (lifecycle.getStatus().state === "timed_out") {
    setTimeout(() => { void lifecycle.shutdown("post-timeout"); }, 20);
  }
});
lifecycle.start();
