# ToolNet-SRT v0.3.0: staging and proposed release

The staging API and worker run in separate Docker containers. The API listens only
on `127.0.0.1:33315`; the test schema and R2 prefix are recorded in the protected
`.env.staging-45f7cf10c484` file. The staging-only import adapter restricts Neon
queries and R2 object keys to those test resources. This adapter is not part of
the production image's default startup, so this staging setup does not validate
the final production configuration or a public Render deployment.

An audio upload, URL enqueue, status and SRT download passed with real Neon, R2
and JianYing. The five-minute upload produced five completed chunk checkpoints.
The URL job stayed queued while the worker was stopped and completed after it
restarted. SRT timestamps passed structural checks. This does not prove speech
recognition completeness or timestamp accuracy against a human-aligned reference.

## Proposed production steps — approval required

The database configured in `.env` was inspected read-only on 2026-11-03. Its
`public.toolnet_srt_jobs` exists, the six relevant lease/source columns exist,
`source_key` is nullable, and the table contained no jobs at the time of the
check. The remote fetch, idempotency and chunk checkpoint tables were absent.
Repeat these checks immediately before any migration: the state may change.

1. Verify the target Render service, the VPS host, credentials and a restorable
   backup. Record the active deployment, source commit and environment so that
   the current v0.2.0 application can be restored. Inspect the Git candidate
   file list and exclude all five local `.bak` files and every `.env*` file.
2. After approval, create a reviewed commit, push the approved revision, and
   confirm the remote commit. Ensure GitHub push will not trigger an automatic
   production deploy; disable auto-deploy first if necessary. Build the same
   image revision for both the HTTP service and the worker.
3. Recheck production job states and table/index definitions. Apply reviewed
   migrations 003, 004 and 005 **in that order** to the intended `public` schema
   after taking a backup. Migration 002 adds lease columns/indexes and should be
   reviewed against existing definitions before applying; its `IF NOT EXISTS`
   clauses do not validate an existing column or index. Migration 006 is not
   required while the experimental ASR semaphore stays disabled. Confirm table,
   constraint and index definitions after migration. Never run the test-only
   schema/prefix adapter against production.
4. Provision the same production database, R2 bucket, stable URL encryption key,
   idempotency key and hostname allowlist on the HTTP service and VPS. The API
   additionally needs its existing `TOOLNET_SRT_API_KEY`. The local `.env` has
   Neon/R2 credentials but does not contain the production encryption key,
   idempotency key, allowlist or API key; their Render values were not inspected.
   Never substitute the staging keys or rotate the encryption key while URLs are
   queued. Store the production VPS environment file outside the repository with
   root-only permissions. The worker also validates `TOOLNET_SRT_API_KEY` even
   though it has no HTTP listener.
5. Production HTTP service: `DURABLE_JOBS_ENABLED=true`,
   `DURABLE_RUN_WORKERS=false`, `DURABLE_CHUNK_CHECKPOINTS_ENABLED=true`,
   `DURABLE_EXPERIMENTAL_ASR_SEMAPHORE=false`. Worker: use the same environment
   and `DURABLE_WORKER_CONCURRENCY=1`, `DURABLE_FETCH_CONCURRENCY=1`,
   `DURABLE_ASR_GLOBAL_CONCURRENCY=1`. `server/durable-worker-service.mjs`
   forces `DURABLE_RUN_WORKERS=true` for that process. Verify the VPS has enough
   RAM and temporary disk and run only one worker instance.
6. After approval and migration, start the separately supervised worker before
   routing new durable traffic, verify it is ready, then deploy the approved API
   image to Render. Recheck the job queue before starting the worker because it
   immediately claims eligible jobs. Example on a Docker-enabled VPS, replacing
   both placeholders with reviewed values:

   ```sh
   docker run -d --name toolnet-srt-worker --restart unless-stopped \
     --env-file /secure/path/toolnet-srt-worker.env \
     APPROVED_IMAGE_BY_DIGEST node server/durable-worker-service.mjs
   ```

   No worker HTTP port is published. `--restart unless-stopped` is process
   supervision, not proof that the host has sufficient capacity or is always on.
   Record the container/image and verify logs, readiness and restart behavior.
7. Verify API readiness and one short end-to-end job, then monitor queue, retries,
   resource use and errors. Do not publish npm without its own authorization.

## Rollback

Stop accepting new jobs, drain the worker when possible, and restore the prior
Render image and configuration if needed. Preserve database tables, encrypted
URLs, object keys and R2 objects. Do not drop migrations or delete queued jobs as
an automatic rollback. A return to v0.2 does not itself process durable jobs;
retain a compatible worker or explicitly plan recovery of the durable queue.
