# @core/jobs — rules for AI agents

## Purpose
Durable background jobs on the same Postgres: queue (`FOR UPDATE SKIP LOCKED`), retries with exponential backoff,
timeouts, de-duplication keys, cron schedules (5 fields, with timezone) and an HTTP endpoint so serverless hosts can
run due jobs from their cron. No Redis, no extra service. Table owner of `jobs` and `job_schedules`.

## Map
- `index.ts` — public API: `defineJob`, `enqueue`, `schedule`, `unschedule`, `runDue`, `runWorker`, `retryJob`, `pruneJobs`.
- `queue.ts` — claim/run logic. `registry.ts` — in-memory job types. `schema.ts` — tables.
- `adapters/hono.ts` — `cronRoutes()`. `adapters/next.ts` — `cronRoute` (GET/POST).

## Integration
1. Env: `CRON_SECRET` (≥ 32 random chars). Generate and apply migrations (see `src/lib/db/AGENTS.md`).
2. Define jobs in one module imported by every process (web and worker), e.g. `src/jobs.ts`:
   ```ts
   import { z } from 'zod';
   import { defineJob } from './lib/jobs/index.js';
   export const sendWelcome = defineJob('email.welcome', z.object({ userId: z.string() }), async ({ userId }, { signal }) => { /* … */ });
   ```
   Enqueue: `await sendWelcome.enqueue({ userId }, { dedupeKey: `welcome:${userId}` })`.
3. Run them, one of:
   - Serverless (Vercel, Netlify, Workers): mount the cron endpoint (`app.route('/api/cron', cronRoutes())` or
     `app/api/cron/route.ts` with `export { cronRoute as GET, cronRoute as POST }`) and call it every minute with
     `Authorization: Bearer $CRON_SECRET` (Vercel Cron sends it automatically when `CRON_SECRET` is set).
   - Long-running Node: a worker process calling `runWorker({ concurrency: 4 })`.
4. Recurring work: `await schedule('report.daily', '0 8 * * *', { timezone: 'Europe/Madrid' })` once at startup.
5. Verify: enqueue a job, call the cron endpoint, the `jobs` row becomes `done`.

## Conventions
- Handlers must be idempotent: a job can run more than once (retries, expired locks).
- Payloads carry IDs and small values; load fresh data inside the handler. Never put secrets in payloads.
- Pass `ctx.signal` to `fetch()` and long operations so timeouts stop them.
- Job types are dotted lowercase names owned by a module (`newsletter.send-batch`).

## Don't
- Don't call slow external APIs (email, suppliers, payments) inside a request when a job fits.
- Don't edit `jobs` rows by hand; use `retryJob` for failed ones.
- Don't expose the cron endpoint without `CRON_SECRET`.
