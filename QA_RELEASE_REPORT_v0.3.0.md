# ToolNet-SRT v0.3.0 — release QA

Status: **not approved for production release**. Staging deployment and its public E2E have not been completed. The production application and production database schema have not been changed by these checks; no package was published.

Subsequent ASR work integrated the semaphore and two-chunk processing behind an **off-by-default experimental flag** and added migration 006. Temporary-schema Neon tests cover independent contenders, cooldown and SIGKILL expiry; two real-ASR 3-hour Durable Runtime runs completed in 440.7 seconds at concurrency 1 and 263.2 seconds at concurrency 2. These do not prove global provider limits during network partitions or recognition quality without a time-aligned speech reference. The synchronous `/v1/transcribe` route has since been changed to wait for a durable job; its revised path passed an isolated real short-audio integration check. Migration 006 was not applied to production; see [the performance report](./PERFORMANCE_REPORT_v0.3.0.md).

Latest regression run after the experimental integration: build, Vitest (10/10), Node tests (185/185), `git diff --check`, `npm pack --dry-run`, selected Neon/R2 integration tests and the isolated real-ASR E2E tests passed. These results do not close the blockers above.

Later work queues synchronous `/v1/transcribe` through the durable manager, bounds stalled slot-database operations, and shares disk reservations within one runtime. A real isolated Neon/R2/JianYing E2E verified the synchronous API, MCP job/result, a five-minute input and concurrent job submissions; the final fail-closed cohort rule was introduced afterward and was rechecked on short audio through the full runtime. A separate private TCP proxy cut Neon while a real JianYing result query was initiated, and another test process was SIGKILLed at the same stage; reservations remained quarantined, but provider-side cancellation could not be confirmed. Latest local regression: 194/194 Node and 10/10 Vitest, build, `git diff --check`, and `npm pack --dry-run` passed. The earlier 3-hour E2E timings are not a measurement of the final cohort policy.

## Local release checks

- TypeScript build, `npm test` (8/8), Node test suite (171/171), `git diff --check`, and `npm pack --dry-run` passed. The Node suite requires permission to bind localhost; its sandboxed run could not start three HTTP test files, and the unrestricted rerun passed.
- Docker image built locally; a container verified the production PostgreSQL and R2 dependencies and FFmpeg. Development dependency audit reported zero known vulnerabilities at the time of checking. The package version remains 0.2.0 and was not published.
- A targeted rerun of 26 Neon/R2 integration scripts passed. Neon tests use isolated temporary schemas; migration 002–005 application and repeat application passed there. R2 integration tests use unique test objects and clean them up. No production migration was applied.
- Process crash tests cover SIGKILL during download, during R2 upload, and after upload before database commit. A new process reclaims expired leases; the old token is fenced and the ASR input is the winning attempt's source. Chunk checkpoint recovery was also tested on Neon.
- API/MCP real integration ran against isolated Neon/R2 test resources. Production staging API/MCP verification remains outstanding.

## Actual speech runs

Runs use real spoken audio and JianYing ASR, real FFmpeg, isolated Neon and R2, and the local durable HTTP server. The media is audio, not video. Memory is the sampled total RSS of the server and child processes; CPU is the peak sampled `ps` lifetime-average CPU percentage across processes, and temporary disk is sampled usage. These observations are not Render capacity guarantees.

| Input | Elapsed | Chunks | Retries | Peak RSS | Peak CPU | Peak temporary disk |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 30 seconds | 16.4 s | 1 | 0 | 100,796 KiB | 10.4% | 1,141,133 B |
| 5 minutes | 43.9 s | 5 | 0 | 136,644 KiB | 89.4% | 3,721,335 B |
| 30 minutes | 71.6 s | 3 | 0 | 321,000 KiB | 124.0% | 26,636,975 B |
| 1 hour | 144.2 s | 6 | 0 | 343,360 KiB | 111.3% | 34,043,247 B |
| 3 hours | 502.1 s | 18 | 1 | 352,116 KiB | 115.8% | 66,296,721 B |

Separate real-audio tests submitted three and five concurrent jobs. With ASR scheduler concurrency one, the five-job run encountered one expected HTTP 429 and retried the rejected submission; these tests do not establish five concurrent ASR workers. The 3-hour run completed with one chunk retry; the reason for that retry was not retained by this measurement harness.

Synthetic timeline-marker checks exercise long offsets and chunk boundaries; their zero error cannot establish real ASR timestamp accuracy. No independent human-aligned speech reference has yet been used to measure real timestamp errors. VFR, audio delay, nonzero source timestamps, real speech alignment and the requested long real-video cases require further acceptance evidence.

## Remaining release blockers

- An independently provisioned staging service, isolated database/schema and isolated R2 prefix are needed; staging deployment and public API/MCP E2E have not been verified.
- The disk quota and FFmpeg limiter are local to one runtime; the synchronous endpoint initial upload and non-checkpoint paths remain outside that reservation, and independent processes are not coordinated. Long jobs need a deployment-wide disk and memory budget, safe crash cleanup, and load testing under the actual resource limits.
- Orphan cleanup deliberately retains objects where the database job cannot be found; cleanup policy and retention must be reviewed. Overlap deduplication matches exact text, so semantically equivalent ASR variations can remain.
- Real-speech timestamp accuracy against independent references, realistic long-video cases, platform restart, network/R2 failure injection on staging and full load testing are not yet signed off.
- The current authentication model is a single shared tenant; independent customer ownership isolation is not implemented. Rate limits are per process.
- The checked package has not been upgraded or published as 0.3.0. Changes are uncommitted and were not pushed by this QA run.

Render's free web services can spin down after inactivity, have ephemeral local storage and do not provide free background workers. They are not a reliable production guarantee for long-running transcription. See [Render free services](https://render.com/docs/free) and [compute plans](https://render.com/docs/compute-plans).

## Rollback preparation

Before any separately approved production rollout, record the currently running Render deployment and source commit, verify backups and restoration, prepare feature-flag rollback to the previously verified application, and verify required secrets without printing them. If a release fails, restore the previous application version and disable durable mode; preserve database migrations, encrypted URLs, encryption keys and R2 objects. Do not automatically reverse migrations or delete data.

## Workspace changes at QA

The following paths were modified or untracked during QA; this includes existing local backup files, which have not been deleted or approved for release.

- `.dockerignore`
- `Dockerfile`
- `README.md`
- `mcp/protocol.mjs`
- `package-lock.json`
- `package.json`
- `sdk/client.mjs`
- `server/docs.mjs`
- `server/extended-routes.mjs`
- `server/index.mjs`
- `server/storage/durable-assembly.mjs`
- `server/storage/durable-config.mjs`
- `server/storage/durable-ingress.mjs`
- `server/storage/durable-job-manager.mjs`
- `server/storage/durable-preflight.mjs`
- `server/storage/durable-runtime.mjs`
- `server/storage/durable-worker.mjs`
- `server/storage/pinned-https.mjs`
- `server/storage/postgres-remote-leases.mjs`
- `server/storage/postgres-remote-queue.mjs`
- `server/storage/r2.mjs`
- `src/asr/helpers.ts`
- `src/asr/jianying.ts`
- `src/asr/transcribe.ts`
- `src/asr/upload.ts`
- `tests/durable-config.node-test.mjs`
- `tests/durable-preflight.node-test.mjs`
- `tests/durable-runtime.node-test.mjs`
- `tests/durable-runtime.real.integration.mjs`
- `tsconfig.json`
- `QA_RELEASE_REPORT_v0.3.0.md`
- `RELEASE_NOTES_v0.3.0.md`
- `backup-readme.md.bak`
- `sdk/client.d.mts`
- `server/storage/chunk-extraction.mjs`
- `server/storage/chunk-media-plan.mjs`
- `server/storage/chunk-media-probe.mjs`
- `server/storage/chunk-timestamps.mjs`
- `server/storage/chunk-transcription.mjs`
- `server/storage/durable-fetch-scheduler.mjs`
- `server/storage/durable-fetch-worker.mjs`
- `server/storage/durable-remote-ingress.mjs.bak-step71`
- `server/storage/durable-worker.mjs.bak-step55`
- `server/storage/fetch-disk-quota.mjs`
- `server/storage/leases.mjs.bak-step53`
- `server/storage/migrations/004_remote_idempotency.sql`
- `server/storage/migrations/005_chunk_checkpoints.sql`
- `server/storage/postgres-chunk-checkpoints.mjs`
- `server/storage/postgres.mjs.bak-step43`
- `server/storage/remote-orphan-cleanup.mjs`
- `src/asr/error-sanitization.test.ts`
- `tests/chunk-checkpoints.neon.integration.mjs`
- `tests/chunk-media-probe.node-test.mjs`
- `tests/chunk-timeline-long.integration.mjs`
- `tests/chunk-timestamps.node-test.mjs`
- `tests/chunk-transcription.node-test.mjs`
- `tests/chunk-worker-crash.neon.integration.mjs`
- `tests/chunk-worker-long.neon.integration.mjs`
- `tests/durable-fetch-crash.real.integration.mjs`
- `tests/durable-fetch-full.real.integration.mjs`
- `tests/durable-fetch-scheduler.node-test.mjs`
- `tests/durable-fetch-worker.integration.mjs`
- `tests/durable-fetch-worker.node-test.mjs`
- `tests/durable-http-mcp.node-test.mjs`
- `tests/durable-http-mcp.real.integration.mjs`
- `tests/durable-server-startup.node-test.mjs`
- `tests/durable-server.real.integration.mjs`
- `tests/fetch-disk-quota.node-test.mjs`
- `tests/fixtures/isolated-server-db.mjs`
- `tests/migrations-release.neon.integration.mjs`
- `tests/postgres-remote-idempotency.neon.integration.mjs`
- `tests/release-speech.real.integration.mjs`
- `tests/remote-orphan-cleanup.node-test.mjs`
- `vitest.config.mts`

## Latest concurrency safeguards

The experimental semaphore fails closed during uncertain Neon operations, and the active-slot cohort policy admits only one parent job/worker at a time. A 429 aborts sibling chunks while the shared cooldown is recorded if Neon is reachable. This deliberately limits cross-job throughput. If the cooldown write cannot be confirmed, slots are retained for a finite quarantine; the provider does not confirm server-side cancellation, so a strict provider-wide concurrency cap cannot be guaranteed. A per-runtime FFmpeg limiter and shared upload/fetch/checkpoint-worker disk reservation do not establish cross-process RAM or disk limits. Keep global concurrency 1 and experimental options disabled until these limitations and independent transcript accuracy are resolved.

## Final-policy integration update

With the experimental global limit 2 and the final cohort rule, a separate isolated real Neon/R2/JianYing runtime run passed synchronous HTTP, MCP job/SRT, 30-second and five-minute audio, and 2/3/5 submitted-job groups. The five-minute input used 5/5 chunks, observed at most 2 active slots, had no provider 429 and no chunk retry. The five-job group saw one local API admission 429 and no provider 429. Test resources were cleaned. A previous deliberately interrupted run returned `JOB_FAILED_TEST_STOP` after passing sync and MCP, as documented in the performance report; it was not counted as PASS. The final code was not rebenchmarked with a three-hour real-ASR input.

After these changes, the local Node suite passed 194/194, Vitest 10/10, build, `git diff --check`, and `npm pack --dry-run`. Isolated Neon/R2 regressions passed for the scheduler, HTTP/MCP, runtime, atomic idempotency, chunk checkpoints, remote fetch leases and queue. A Neon SIGKILL test with mock ASR passed lease expiry, completed-checkpoint reuse and retry of the failed chunk. The synthetic long-timeline test passed its five marker checks, but does not measure real speech recognition accuracy.

## Split API and worker, 2026-11-03

The API-only runtime can be configured with `DURABLE_RUN_WORKERS=false`; `server/durable-worker-service.mjs` starts the schedulers in a separate process. The default remains combined mode for compatibility. The worker must share the API's database, R2 bucket, remote URL keys and allowed hosts; use a single worker with ASR concurrency 1 until a deployment-wide limit is verified.

In a local isolated-schema test using real Neon, R2 and JianYing, separate API and worker processes processed an allowed HTTPS source and returned SRT through HTTP. A separate 3-hour real-audio upload through the same split setup completed 18/18 chunks and returned a 3,756-cue SRT in 483,757 ms, with no reported chunk retries or provider 429; temporary test resources were cleaned. SRT cue numbering and ordered, nonnegative timestamps within the source duration passed a structural check. Comparison with the earlier 3,748-cue run paired 3,630 cues by nearby start time, with 461 text differences, 118 and 126 unpaired cues, and no immediately adjacent normalized duplicates. These are heuristic output differences, not evidence that speech is correct or complete; an independently aligned reference and inspection of boundaries are still required.

The separate-process setup has not been deployed to a staging service. Render production is still v0.2.0; a successful health response does not validate v0.3. No production migration, push, publish or deployment occurred. Build, Vitest 10/10, Node tests 196/196 and `git diff --check` passed locally after the split implementation. The Neon integration uses temporary test schema and R2 objects, not a staging deployment.
