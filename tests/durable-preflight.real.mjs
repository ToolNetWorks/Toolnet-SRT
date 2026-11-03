import { createDurablePreflight } from "../server/storage/durable-preflight.mjs";

try {
  const result = await createDurablePreflight().check();
  if (!result.enabled || result.neon !== "ready" || result.r2 !== "ready" || result.workerStarted !== false) {
    throw new Error("DURABLE_PREFLIGHT_INCOMPLETE");
  }
  console.log("NEON CONNECTION: PASS");
  console.log("NEON SCHEMA: PASS");
  console.log("R2 BUCKET ACCESS: PASS");
  console.log("WORKER NOT STARTED: PASS");
  console.log("TOOLNET-SRT DURABLE PREFLIGHT PASSED");
} catch (error) {
  console.error("PREFLIGHT FAILED:", error.message);
  process.exitCode = 1;
}
