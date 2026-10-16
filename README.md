# @core/jobs

[![genpm](https://api.genpm.net/v1/badge/@core/jobs.svg)](https://genpm.net/p/@core/jobs) ![license](https://img.shields.io/badge/license-MIT-blue)

> Background jobs on Postgres: durable queue, retries with backoff, cron schedules and a serverless cron endpoint

A [GenPM](https://genpm.net) module: **source code + AI rules + MCP config**, injected into your project instead of hidden in `node_modules`. The code lands in `src/lib/jobs`, pinned to a verified commit, and you own it.

## Install

```bash
genpm add @core/jobs
```

No genpm yet? `curl -fsSL https://genpm.net/install.sh | sh` (macOS/Linux), `irm https://genpm.net/install.ps1 | iex` (Windows), or one-off with `npx @genpm-net/cli add @core/jobs`.

Or let your AI agent do it: run `genpm mcp setup` once, then ask *"add @core/jobs with genpm"* in Claude Code, Cursor, VS Code, Windsurf, Codex or Gemini CLI. It shows you a dry-run plan before writing anything.

## What it does

Durable background jobs on the same Postgres: queue (`FOR UPDATE SKIP LOCKED`), retries with exponential backoff,
timeouts, de-duplication keys, cron schedules (5 fields, with timezone) and an HTTP endpoint so serverless hosts can
run due jobs from their cron. No Redis, no extra service. Table owner of `jobs` and `job_schedules`.

## Setup

After `genpm add`, follow these steps (your AI agent finds the same steps in `src/lib/jobs/AGENTS.md` and can do them for you):

1. Env: `CRON_SECRET` (≥ 32 random chars). Generate and apply migrations (see `src/lib/db/AGENTS.md`).
2. Define jobs in one module imported by every process (web and worker), e.g. `src/jobs.ts`:
   ```ts
   import { z } from 'zod';
   import { defineJob } from './lib/jobs/index.ts';
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

## What's inside

Paths are relative to `src/lib/jobs`.

- `index.ts` — public API: `defineJob`, `enqueue`, `schedule`, `unschedule`, `runDue`, `runWorker`, `retryJob`, `pruneJobs`.
- `queue.ts` — claim/run logic. `registry.ts` — in-memory job types. `schema.ts` — tables.
- `adapters/hono.ts` — `cronRoutes()`. `adapters/next.ts` — `cronRoute` (GET/POST).

## At a glance

- **GenPM dependencies** (installed with it): [@core/db](https://genpm.net/p/@core/db)
- **npm dependencies** (proposed, never installed without asking): `cron-parser`, `zod`
- **Environment variables** (added to `.env.example`): `CRON_SECRET`
- **AI rules**: [`AGENTS.md`](AGENTS.md), loaded by your agent only when it works in `src/lib/jobs`

## Security

GenPM installs a 40-character commit SHA (never a branch), verifies an Ed25519-signed manifest, never runs install scripts and scans AI rules for prompt injection. Details: [genpm.net/docs/security](https://genpm.net/docs/security).

## License

MIT — see [LICENSE](LICENSE). Browse the full catalog at [genpm.net](https://genpm.net).
