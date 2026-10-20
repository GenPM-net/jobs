import { eq } from 'drizzle-orm';
import { Hono } from 'hono';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import { testDb } from '../db/__fixtures__/pglite.ts';
import type { Db } from '../db/index.ts';
import { cronRoutes } from './adapters/hono.ts';
import { cronRoute } from './adapters/next.ts';
import * as schema from './schema.ts';
import {
  backoffMs,
  claimJobs,
  claimScheduleTick,
  clearJobs,
  defineJob,
  enqueue,
  isAuthorizedCron,
  jobSchedules,
  jobs,
  nextCronDate,
  pruneJobs,
  retryJob,
  runDue,
  schedule,
} from './index.ts';

let db: Db;
const seen: string[] = [];
const later = (ms: number) => new Date(Date.now() + ms);

beforeEach(async () => {
  db = await testDb(schema);
  clearJobs();
  seen.length = 0;
  process.env.CRON_SECRET = 's'.repeat(40);
});
afterEach(() => clearJobs());

describe('enqueue', () => {
  it('validates payloads and requires a defined type', async () => {
    const send = defineJob('email.send', z.object({ to: z.string().email() }), async (p) => {
      seen.push(p.to);
    });
    await expect(send.enqueue({ to: 'nope' })).rejects.toThrow();
    await expect(enqueue('missing', {})).rejects.toThrow(/unknown job type/);
    expect(() => defineJob('Bad Type', z.object({}), async () => {})).toThrow(/invalid job type/);
    expect((await send.enqueue({ to: 'a@b.co' }))?.status).toBe('queued');
  });

  it('dedupeKey avoids duplicates only while pending or running', async () => {
    const j = defineJob('sync', z.object({}), async () => {
      seen.push('run');
    });
    expect(await j.enqueue({}, { dedupeKey: 'k' })).not.toBeNull();
    expect(await j.enqueue({}, { dedupeKey: 'k' })).toBeNull();
    await runDue();
    expect(await j.enqueue({}, { dedupeKey: 'k' })).not.toBeNull();
  });
});

describe('runDue', () => {
  it('runs due jobs in order and leaves future ones', async () => {
    const j = defineJob('log', z.object({ n: z.number() }), async (p) => {
      seen.push(String(p.n));
    });
    await j.enqueue({ n: 2 }, { runAt: new Date(Date.now() - 1000) });
    await j.enqueue({ n: 1 }, { runAt: new Date(Date.now() - 2000) });
    await j.enqueue({ n: 3 }, { runAt: later(60_000) });
    const r = await runDue();
    expect(r).toMatchObject({ claimed: 2, done: 2 });
    expect(seen).toEqual(['1', '2']);
    expect((await runDue({ now: later(61_000) })).done).toBe(1);
    expect(seen).toEqual(['1', '2', '3']);
  });

  it('retries with backoff and fails after maxAttempts', async () => {
    let calls = 0;
    const j = defineJob(
      'flaky',
      z.object({}),
      async () => {
        calls++;
        throw new Error('boom');
      },
      { maxAttempts: 2 },
    );
    const job = await j.enqueue({});
    expect((await runDue()).retried).toBe(1);
    let [row] = await db.select().from(jobs).where(eq(jobs.id, job!.id));
    expect(row).toMatchObject({ status: 'queued', attempts: 1, lastError: 'boom' });
    expect(row!.runAt.getTime()).toBeGreaterThan(Date.now() + 5_000);
    expect((await runDue()).claimed).toBe(0);
    expect((await runDue({ now: later(backoffMs(1) + 1000) })).failed).toBe(1);
    [row] = await db.select().from(jobs).where(eq(jobs.id, job!.id));
    expect(row).toMatchObject({ status: 'failed', attempts: 2 });
    expect(calls).toBe(2);
    expect(await retryJob(job!.id)).toBe(true);
    expect(await retryJob(job!.id)).toBe(false);
  });

  it('times out slow handlers and aborts their signal', async () => {
    let aborted = false;
    const j = defineJob(
      'slow',
      z.object({}),
      (_p, ctx) =>
        new Promise<void>((resolve) => {
          ctx.signal.addEventListener('abort', () => {
            aborted = true;
            resolve();
          });
        }),
      { timeoutMs: 50, maxAttempts: 1 },
    );
    await j.enqueue({});
    expect((await runDue()).failed).toBe(1);
    expect(aborted).toBe(true);
    const [row] = await db.select().from(jobs);
    expect(row!.lastError).toMatch(/timeout/);
  });

  it('a claimed job is not taken again until its lock expires', async () => {
    defineJob('x', z.object({}), async () => {});
    await enqueue('x', {});
    expect(await claimJobs(10)).toHaveLength(1);
    expect(await claimJobs(10)).toHaveLength(0);
    const again = await claimJobs(10, later(6 * 60_000));
    expect(again).toHaveLength(1);
    expect(again[0]!.attempts).toBe(2);
  });

  it('a job that kills its worker on the last attempt fails instead of being reclaimed forever', async () => {
    let calls = 0;
    defineJob('crashy', z.object({}), async () => {
      calls++;
    }, { maxAttempts: 2 });
    const job = await enqueue('crashy', {});
    // Dos "caídas" del worker: se reclama, el proceso muere y el bloqueo caduca.
    expect(await claimJobs(10)).toHaveLength(1);
    expect(await claimJobs(10, later(6 * 60_000))).toHaveLength(1);
    const r = await runDue({ now: later(12 * 60_000) });
    expect(r).toMatchObject({ claimed: 0, failed: 1 });
    expect(calls).toBe(0);
    const [row] = await db.select().from(jobs).where(eq(jobs.id, job!.id));
    expect(row).toMatchObject({ status: 'failed', attempts: 2, lastError: 'lock expired', lockedUntil: null });
    expect(await claimJobs(10, later(60 * 60_000))).toHaveLength(0);
  });

  it('runs with concurrency and invalid payloads fail without retry', async () => {
    const j = defineJob('c', z.object({ n: z.number() }), async (p) => {
      await new Promise((r) => setTimeout(r, 10));
      seen.push(String(p.n));
    });
    for (let n = 0; n < 6; n++) await j.enqueue({ n });
    await db.insert(jobs).values({ type: 'c', payload: { n: 'bad' }, runAt: new Date() }); // hora de la app, como enqueue (Postgres guarda µs)
    const r = await runDue({ concurrency: 3 });
    expect(r).toMatchObject({ claimed: 7, done: 6, failed: 1 });
    expect(seen.sort()).toEqual(['0', '1', '2', '3', '4', '5']);
  });
});

describe('schedules', () => {
  it('computes next cron dates in a timezone', () => {
    const after = new Date('2026-01-01T00:02:00Z');
    expect(nextCronDate('*/5 * * * *', after).toISOString()).toBe('2026-01-01T00:05:00.000Z');
    expect(nextCronDate('0 9 * * *', after, 'Europe/Madrid').toISOString()).toBe('2026-01-01T08:00:00.000Z');
  });

  it('enqueues once per due schedule, even after missed runs', async () => {
    defineJob('report', z.object({ kind: z.string().default('daily') }), async (p) => {
      seen.push(p.kind);
    });
    await schedule('report', '0 * * * *');
    await expect(schedule('report', '* * *')).rejects.toThrow(/5 fields/);
    const [s] = await db.select().from(jobSchedules);
    const r = await runDue({ now: new Date(s!.nextRunAt.getTime() + 3 * 3_600_000) });
    expect(r).toMatchObject({ scheduled: 1, done: 1 });
    expect(seen).toEqual(['daily']);
    const [after] = await db.select().from(jobSchedules);
    expect(after!.nextRunAt.getTime()).toBeGreaterThan(s!.nextRunAt.getTime());
  });

  it('overlapping cron calls enqueue each tick once, even from a stale read of the schedule', async () => {
    defineJob('tick', z.object({}), async () => {
      seen.push('tick');
    });
    await schedule('tick', '0 * * * *');
    const [s] = await db.select().from(jobSchedules);
    const now = new Date(s!.nextRunAt.getTime() + 60_000);
    // Los dos crons leyeron el mismo horario vencido; solo uno puede llevarse el tick.
    expect(await claimScheduleTick(s!, now)).toBe(true);
    expect(await claimScheduleTick(s!, now)).toBe(false);
    const [r1, r2] = await Promise.all([runDue({ now }), runDue({ now })]);
    expect(r1.scheduled + r2.scheduled).toBe(0);
    const [after] = await db.select().from(jobSchedules);
    const due = new Date(after!.nextRunAt.getTime() + 1000);
    const runs = await Promise.all([runDue({ now: due }), runDue({ now: due })]);
    expect(runs.reduce((n, r) => n + r.scheduled, 0)).toBe(1);
    expect(seen).toEqual(['tick']);
    // Aunque la tarea ya terminó (la deduplicación ya no protege), un tercer cron con la misma hora no repite el tick.
    expect((await runDue({ now: due })).scheduled).toBe(0);
  });

  it('a job being run is not claimed again by a concurrent cron that uses the same (future) now', async () => {
    let release!: () => void;
    const started = new Promise<void>((ok) => {
      defineJob('slow', z.object({}), async () => {
        seen.push('slow');
        ok();
        await new Promise<void>((r) => (release = r));
      });
    });
    const now = new Date(Date.now() + 3_600_000); // una hora por delante del reloj real
    await db.insert(jobs).values({ type: 'slow', payload: {}, runAt: new Date(now.getTime() - 1000) });
    const first = runDue({ now });
    await started; // A ya la reclamó y la está ejecutando (con su bloqueo ajustado al timeout)
    expect((await runDue({ now })).claimed).toBe(0);
    release();
    await first;
    expect(seen).toEqual(['slow']);
  });

  it('prunes old done jobs and keeps failed ones', async () => {
    const j = defineJob('p', z.object({}), async () => {});
    await j.enqueue({});
    await runDue();
    await db.update(jobs).set({ finishedAt: new Date(Date.now() - 10 * 86_400_000) });
    await db.insert(jobs).values({ type: 'p', payload: {}, status: 'failed', finishedAt: new Date(0) });
    expect(await pruneJobs(7)).toBe(1);
    expect(await db.select().from(jobs)).toHaveLength(1);
  });
});

describe('cron endpoint', () => {
  it('requires CRON_SECRET', async () => {
    expect(isAuthorizedCron(`Bearer ${'s'.repeat(40)}`)).toBe(true);
    expect(isAuthorizedCron('Bearer nope')).toBe(false);
    expect(isAuthorizedCron(null)).toBe(false);
    expect(() => isAuthorizedCron('x', 'short')).toThrow(/CRON_SECRET/);
  });

  it('Hono and Next run due jobs only when authorized', async () => {
    defineJob('h', z.object({}), async () => {
      seen.push('h');
    });
    await enqueue('h', {});
    const app = new Hono().route('/api/cron', cronRoutes());
    expect((await app.request('/api/cron', { method: 'POST' })).status).toBe(401);
    const ok = await app.request('/api/cron', { method: 'POST', headers: { authorization: `Bearer ${'s'.repeat(40)}` } });
    expect(await ok.json()).toMatchObject({ done: 1 });
    await enqueue('h', {});
    expect((await cronRoute(new Request('http://x/api/cron'))).status).toBe(401);
    const res = await cronRoute(new Request('http://x/api/cron', { headers: { authorization: `Bearer ${'s'.repeat(40)}` } }));
    expect(await res.json()).toMatchObject({ done: 1 });
    expect(seen).toEqual(['h', 'h']);
  });
});
