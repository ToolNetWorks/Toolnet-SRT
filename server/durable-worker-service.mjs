import { createDurableRuntime } from './storage/durable-runtime.mjs';
import { createDurableAssembly } from './storage/durable-assembly.mjs';

// This process must share the API's database, bucket and durable secrets.
// It has no HTTP listener; the lifecycle drains both schedulers on signals.
if (process.env.DURABLE_JOBS_ENABLED !== 'true') {
  console.error('DURABLE_WORKER_REQUIRES_DURABLE_JOBS');
  process.exitCode = 1;
} else {
  const env = { ...process.env, DURABLE_RUN_WORKERS: 'true' };
  const runtime = createDurableRuntime({ env, assemble: createDurableAssembly() });
  try {
    await runtime.start();
    console.log('DURABLE_WORKER_READY');
  } catch {
    // Errors from client libraries may include connection credentials.
    console.error('DURABLE_WORKER_START_FAILED');
    process.exitCode = 1;
  }
}
