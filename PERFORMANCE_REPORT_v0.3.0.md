# ToolNet-SRT long-audio performance experiment

The public-domain 3-hour AAC/M4A audio used in the prior release QA is the input. All provider benchmarks are sequential and run outside the production server. Results here do not authorize a production release.

## Confirmed extraction bottleneck

The original extractor opens the original file for each chunk and decodes from the beginning before `atrim`. Planning the 18 chunks took 4,395 ms in one run. The sequential extraction phase took 107,576 ms, with individual extraction times rising from 878 ms to 11,168 ms. The complete run emitted 345,606,052 bytes of WAV data and used a peak reported RSS of 59,536 KiB. These are local measurements; they were not collected in the same E2E run as the earlier 502,100 ms result.

Another sequential comparison decoded the same 18 chunks both ways and compared SHA-256 hashes of the resulting WAV files. All 18 matched. Baseline extraction took 105,326 ms; input seeking with 2 seconds of preroll took 15,458 ms: an **85.3% reduction in extraction time**, not an E2E speedup measurement. A separate 5-minute MP3 comparison matched all five chunks, taking 2,317 ms normally and 1,116 ms with seek. Tests also compare the outputs for examples with delayed audio, nonzero container start and variable frame-rate video. This does not establish safety for every codec/container or every media timeline.

`DURABLE_EXPERIMENTAL_CHUNK_SEEK_PREROLL_MS` defaults to 0 (off); `2000` enables the experimental seek in an isolated test. It retains source-timeline trimming and the same WAV format. Keep it disabled for unverified media.

## Rate-limit and timing changes

JianYing HTTP errors expose only a status and, for HTTP 429, a parsed retry delay; signed URLs and provider response bodies are not reported. A failed chunk observes a valid `Retry-After` and uses exponential backoff with jitter inside the current attempt. The retry deadline is not yet persisted across process restart or job-level timeout. At that earlier stage there was no provider-wide distributed limiter; the subsequent experimental integration and its limitations are described below.

The worker supports an optional injected timing callback for download, probe/planning, extraction, ASR, checkpoint operations, retry waiting, merge, SRT formatting and result upload. It receives timing/status metadata only and is disabled by default. CPU, memory and I/O require separate process/host measurements; the callback does not measure them.

## Provider experiments

Two standalone runs on the same 3-hour source run in sequence, first concurrency 1 then 2, with no Neon/R2, no durable checkpointing, and the experimental seek enabled. They stop on an HTTP 429 instead of trying to bypass the provider limit. Consequently these results cannot be used as durable HTTP E2E timings or proof of crash recovery.

| Provider experiment | Wall time | Completed | 429 / retry | Probe + plan | Sum of extract | Sum of ASR | Peak sampled Node RSS | Sampled chunk-file peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| One chunk | 336,883 ms | 18/18 | 0 / 0 | 4,028 ms | 17,970 ms | 314,704 ms | 288,235,520 B | 19,200,432 B |
| Two chunks | 193,329 ms | 18/18 | 0 / 0 | 4,447 ms | 18,181 ms | 356,863 ms | 365,502,464 B | 38,400,862 B |

The second run took 42.6% less wall time **in this standalone experiment**. Summed phase times overlap when running two chunks; they are not a wall-time breakdown. `/usr/bin/time` reported 12% and 21% CPU respectively over each run. Sampled Node RSS excludes FFmpeg children, and the file-size sample may miss partially written files. These are not total host resource limits and cannot establish suitability for Render Free.

The first output contained 3,785 cues and the second 3,755; both passed basic nonnegative, ordered timestamp checks, had no cue beyond the 10,800,019 ms source duration, and contained no immediately adjacent identical cues. Exact normalized text matched 3,177 cues; 608 and 578 cues respectively had no identical-text match. Matching cues by start time within one second produced 3,608 pairs, of which 438 had different text; the 95th percentile of paired start-time differences was 0 ms. A case-normalized bag-of-words comparison counted 26,948 and 26,899 words, with 26,674 matching occurrences. These comparisons are between two ASR outputs, not against ground truth; they cannot determine whether words or sentences are missing, duplicated, or incorrect. They also do not prove the two output qualities equivalent.

That standalone benchmark did not test concurrency 3. One successful concurrency-2 run does not prove provider limits or sufficient resources for multiple users. At the time, the durable worker was sequential; later experimental integration is described below. Checkpoint rows remain fenced by the parent job lease rather than a separately expiring lease per chunk.

## Regression evidence

The synthetic 3-hour timeline test passed with markers at 10 minutes, 30 minutes, 1 hour, 2 hours and 3 hours; experimental seek output was byte-identical to full decoding at those tested positions. The synthetic markers are not ground truth for JianYing speech recognition. A Neon test with seek enabled also passed SIGKILL, actual lease expiration, checkpoint reclaim and reuse of the completed chunk. An isolated real HTTP/R2/Neon/JianYing run with seek enabled passed 30-second and 5-minute audio and three/five submitted jobs; the five-job case encountered one HTTP 429 from the local API's creation limit and retried that submission. It used a single ASR worker and is not a two-chunk durable-runtime benchmark.

Latest checks: TypeScript build, `npm test` (10/10), Node test suite (179/179), `git diff --check` and `npm pack --dry-run` passed. No production database migration, push, package publication, or deployment was performed.

## Distributed semaphore prototype

Migration 006 and an isolated PostgreSQL semaphore prototype now exist. A Neon test in a temporary schema exercises concurrent claims, a persisted 429 cooldown, adaptive reduction, mismatched configuration, token fencing, four independent contender processes and SIGKILL followed by actual slot expiry. The test does not contact JianYing and does not establish a strict upper bound on provider-side work during a network partition or a stalled process. A separate slot-runner prototype tests acquisition, heartbeat, cancellation, release and the handling of an unknown cooldown commit.

At the time of that earlier pass neither prototype was wired into the runtime. The standalone timings above are not Durable Runtime E2E timings. The subsequent integration and its remaining limits are described below.

## Subsequent experimental Durable Runtime integration

The preceding prototype status is superseded for isolated testing: `DURABLE_EXPERIMENTAL_ASR_SEMAPHORE=true` wires both Durable Worker ASR branches through the PostgreSQL semaphore, with one or two parallel checkpointed chunks. It remains **off by default**. The synchronous `/v1/transcribe` route now queues a durable job and waits for its result, and the revised path passed a real short-audio integration check with Neon, R2 and JianYing. Default v0.2 mode is unchanged. Preflight requires migration 006, which was applied only to temporary schemas.

Expired slot leases are held for an additional quarantine (default 900 s) before reclamation. A timeout, heartbeat failure, cancellation or uncertain provider outcome does not release the reservation immediately. This is a conservative time bound, not proof that JianYing stopped server-side work; if the provider can run longer, a strict global upper bound is not established. If a 429 is received while the database is unreachable, its cooldown cannot be recorded globally; the later cohort rule blocks other participating jobs while a reservation remains active or quarantined. After finite quarantine, provider-side activity is still uncertain. These are blockers for enabling this mechanism in production. Different instances must also use identical settings, and old instances that do not participate in the semaphore must not run concurrently.

Two real-JianYing Durable Runtime runs used the same 3-hour audio, seek preroll 2 s, isolated Neon schemas and R2 objects, and were run sequentially. The results below include enqueue/upload, scheduler, checkpointing, SRT upload and HTTP result retrieval. Timing phase sums may overlap and ASR time includes slot waiting. CPU is the maximum sampled sum of per-process `ps` lifetime averages, not instantaneous CPU; RSS and temporary-file sizes are sampled estimates.

| Measurement | Global concurrency 1 | Global concurrency 2 |
| --- | ---: | ---: |
| End-to-end wall time | 440,719 ms | 263,153 ms |
| Completed chunks | 18 | 18 |
| Provider 429 / chunk retry | 0 / 0 | 0 / 0 |
| Summed slot wait | 16,764 ms | 17,232 ms |
| Summed extraction | 18,780 ms | 19,136 ms |
| Summed ASR including slot wait | 368,166 ms | 420,630 ms |
| Summed checkpoint DB work | 24,679 ms | 24,338 ms |
| SRT merge | 35 ms | 19 ms |
| R2 source download / result upload | 955 / 303 ms | 1,461 / 198 ms |
| Sampled RSS | 367,048 KiB | 454,956 KiB |
| Sampled `ps` CPU | 118.7% | 169.8% |
| Sampled temporary disk | 63,896,011 B | 83,096,441 B |
| SRT cues / bytes | 3,748 / 277,554 | 3,810 / 279,354 |

Concurrency 2 reduced wall time by **40.3%** in this pair. A snapshot observed two active slots; this is not a continuous measurement. Both SRT files had ordered, nonnegative timestamps within the 10,800,019 ms source, and neither had immediately adjacent identical cues. There were 3,056 exact normalized-text matches; 692 and 754 cues had no exact match in the other run. Among 3,588 cues paired by start time within 1,000 ms, the 95th percentile differences were 240 ms for starts and 1,200 ms for ends. These are differences between ASR outputs, not errors against ground truth. No independent time-aligned transcript exists for this comparison, so WER/CER, missing speech, and equal recognition quality cannot be established. The original files are retained locally under `/tmp/toolnet-durable-asr-1.srt` and `/tmp/toolnet-durable-asr-2.srt`.

The proposed production setting remains global concurrency 1 with experimental flags off. Render Free is unsuitable for treating the observed near-455,000 KiB RSS as safe: these are sampled local figures and do not bound all child processes, FFmpeg work, ingress, or shared disk across instances. Do not accept concurrency 2 for staging until deployment-wide resource limits, provider uncertainty and independent speech quality checks have been addressed; no production recommendation for concurrency 2 is made.

A later change shares the existing disk reservation between upload ingress, remote fetch and checkpointed ASR workers within a single runtime. Checkpoint PCM space is reserved from actual planned durations, and R2 source downloads now enforce a streaming size limit and accept cancellation. The path passed an isolated real Neon/R2/JianYing test with two workers and 2, 3 and 5 submitted jobs. This is not a cross-process reservation or an operating-system memory limit; the synchronous endpoint's initial temporary file and other processes are outside that budget.

The slot runner now times out stalled database operations and aborts an in-flight request on a heartbeat timeout. An unknown acquire or 429 write outcome remains conservative for that slot. A later fail-closed cohort rule reserves all active slots for one parent job/worker at a time; a 429 aborts sibling calls locally, and other workers wait while that cohort is active or quarantined. This limits cross-job throughput and is undergoing final integration checks. A finite quarantine does not prove that the provider terminated an asynchronous job. Concurrency 2 remains unsuitable for staging acceptance until failure modes and this conservative policy have been fully validated.

After the A/B run, a short-audio stress run submitted 2, 3 and 5 jobs to two workers with a global limit of two. All jobs completed; the sampled active-slot maximum was two in each case. The five-job submission encountered one API admission HTTP 429, retried after the other jobs, and saw no provider 429. This sample is not a continuous proof of the global cap. Separate Neon tests used independent Node processes to contend for slots, tested a new process during persisted cooldown, and killed a slot owner. The job lease expiry in the crash test was real; the additional 15-minute slot quarantine was advanced in the isolated test schema rather than waited out. A worker checkpoint crash test used mock ASR and real Neon. In a subsequent real-provider test a private TCP proxy cut the test worker's Neon connection while the client had initiated a JianYing result query; a second process was SIGKILLed at the same stage. The client aborted and both reservations remained quarantined. These observations do not establish that the provider stopped its tasks. Unit tests additionally exercised stalled requests, cancellation, and heartbeat failure.

An additional heuristic comparison of the two Durable Runtime SRTs paired the nearest cue starts within one second. It found 3,588 pairs: 3,046 equal after punctuation/case normalization and 542 with different text. Another 160 cues in the first and 222 in the second had no nearby pair; 24 difference records were within five seconds of a 10-minute chunk boundary. Neither file contained immediately adjacent normalized duplicates under this heuristic. These are candidates for inspection, **not confirmed missing or duplicated dialogue**: cue segmentation and ASR output can vary, and no aligned reference is available. The original SRTs and the detailed comparison are retained locally; the comparison file is `/tmp/toolnet-durable-asr-comparison.json` with permissions 0600.

This pass's latest local checks: TypeScript build, `npm test` (10/10), Node test suite (194/194), `git diff --check`, and `npm pack --dry-run` passed. Real ASR/R2/Neon E2E tests were confined to temporary schemas and unique objects; migration 006 was not applied to production.

A local per-runtime FIFO limiter now bounds concurrent FFmpeg extraction and splitting (`DURABLE_FFMPEG_CONCURRENCY`, default 2). It and the disk reservations are process-local; they are not a deployment-wide CPU, RAM, or disk quota. A PostgreSQL pool error handler logs a sanitized code so an idle-connection error during a partition does not crash the runtime. The initial synchronous-upload temporary file and legacy non-checkpoint paths remain outside the shared disk reservation.

## Final-policy short-audio integration, 2026-11-03 UTC

After the cohort rule and local resource-limit changes, an isolated Durable Runtime used real Neon, R2 and JianYing with the experimental global concurrency set to 2. Synchronous `/v1/transcribe` and MCP job/result retrieval passed. The 30-second input completed in 14,976 ms; the 5-minute input completed in 36,195 ms with 5/5 chunks, no chunk retries and a sampled maximum of 2 active slots. Groups of 2, 3 and 5 submitted 30-second jobs completed in 25,881 ms, 31,602 ms and 53,919 ms respectively. Provider HTTP 429 count was zero; the five-job group had one API admission 429 and retried the submission. Sampled peak RSS for these individual cases was 145,040, 117,788, 118,440, 168,584 and 169,332 KiB respectively. Sampling is not a hard resource bound. All temporary test schema and objects were cleaned.

The previous three-hour 440,719 / 263,153 ms A/B measurements predate this final cohort policy, local limiter and quota work: they are not a final-code 3-hour benchmark. A deliberately interrupted earlier test run after the sync and MCP checks failed with `JOB_FAILED_TEST_STOP`; the isolated test job was stopped to avoid starting another provider call while a previous real request was in quarantine, and test resources were cleaned. It is not counted as a passing run.
