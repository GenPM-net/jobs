// Cola durable sobre Postgres: encolar, tomar con FOR UPDATE SKIP LOCKED, ejecutar con reintentos y backoff.
import { CronExpressionParser } from 'cron-parser';
import { and, eq, inArray, lt, lte, or, sql } from 'drizzle-orm';
import type { ZodType } from 'zod';
import { type Executor, getDb } from '../db/index.js';
import { getJob, type JobContext, registerJob } from './registry.js';
import { type Job, jobSchedules, jobs } from './schema.js';

const MAX_ERROR = 2000;

export type EnqueueOptions = {
  runAt?: Date;
  /** Si ya hay una tarea pendiente o en curso con esta clave, no se crea otra (devuelve null). */
  dedupeKey?: string;
  maxAttempts?: number;
};

export type JobHandle<P> = {
  type: string;
  enqueue(payload: P, opts?: EnqueueOptions, db?: Executor): Promise<Job | null>;
};

/**
 * Declara un tipo de tarea. El handler debe ser idempotente (puede ejecutarse más de una vez) y recibir payloads
 * pequeños (IDs, no objetos enteros ni secretos).
 */
export function defineJob<P>(
  type: string,
  schema: ZodType<P>,
  handler: (payload: P, ctx: JobContext) => Promise<void>,
  opts: { maxAttempts?: number; timeoutMs?: number } = {},
): JobHandle<P> {
  registerJob({ type, schema, handler, maxAttempts: opts.maxAttempts ?? 5, timeoutMs: opts.timeoutMs ?? 60_000 });
  return { type, enqueue: (payload, o, db) => enqueue(type, payload, o, db) };
}

export async function enqueue(
  type: string,
  payload: unknown,
  opts: EnqueueOptions = {},
  db: Executor = getDb(),
): Promise<Job | null> {
  const def = getJob(type);
  if (!def) throw new Error(`unknown job type: ${type} (call defineJob first)`);
  const data = def.schema.parse(payload);
  const [row] = await db
    .insert(jobs)
    .values({
      type,
      payload: data,
      runAt: opts.runAt ?? new Date(),
      maxAttempts: opts.maxAttempts ?? def.maxAttempts,
      dedupeKey: opts.dedupeKey ?? null,
    })
    .onConflictDoNothing()
    .returning();
  return row ?? null;
}

/** Espera exponencial: 10 s, 20 s, 40 s… con tope de 1 h. */
export function backoffMs(attempt: number): number {
  return Math.min(10_000 * 2 ** Math.max(0, attempt - 1), 3_600_000);
}

/** Toma hasta `limit` tareas vencidas (o cuyo bloqueo caducó) y las marca `running`. */
export async function claimJobs(limit: number, now: Date = new Date(), db: Executor = getDb()): Promise<Job[]> {
  const due = db
    .select({ id: jobs.id })
    .from(jobs)
    .where(
      or(
        and(eq(jobs.status, 'queued'), lte(jobs.runAt, now)),
        and(eq(jobs.status, 'running'), lt(jobs.lockedUntil, now)),
      ),
    )
    .orderBy(jobs.runAt)
    .limit(limit)
    .for('update', { skipLocked: true });
  const claimed = await db
    .update(jobs)
    .set({
      status: 'running',
      attempts: sql`${jobs.attempts} + 1`,
      // El bloqueo real se ajusta al timeout de cada tipo en runJob; aquí 5 min como red de seguridad.
      lockedUntil: new Date(now.getTime() + 5 * 60_000),
    })
    .where(inArray(jobs.id, due))
    .returning();
  return claimed.sort((a, b) => a.runAt.getTime() - b.runAt.getTime());
}

async function runJob(job: Job, db: Executor): Promise<'done' | 'retry' | 'failed'> {
  const def = getJob(job.type);
  const fail = async (message: string, retry: boolean) => {
    const final = !retry || job.attempts >= job.maxAttempts;
    await db
      .update(jobs)
      .set({
        status: final ? 'failed' : 'queued',
        lastError: message.slice(0, MAX_ERROR),
        lockedUntil: null,
        runAt: final ? job.runAt : new Date(Date.now() + backoffMs(job.attempts)),
        finishedAt: final ? new Date() : null,
      })
      .where(eq(jobs.id, job.id));
    return final ? 'failed' : 'retry';
  };
  if (!def) return fail(`unknown job type: ${job.type}`, true);
  const parsed = def.schema.safeParse(job.payload);
  if (!parsed.success) return fail(`invalid payload: ${parsed.error.message}`, false);

  await db
    .update(jobs)
    .set({ lockedUntil: new Date(Date.now() + def.timeoutMs + 30_000) })
    .where(eq(jobs.id, job.id));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`timeout after ${def.timeoutMs} ms`)), def.timeoutMs);
  try {
    await Promise.race([
      def.handler(parsed.data, { jobId: job.id, attempt: job.attempts, signal: controller.signal }),
      new Promise<never>((_, reject) =>
        controller.signal.addEventListener('abort', () => reject(controller.signal.reason)),
      ),
    ]);
    // Un handler que termina "bien" justo al abortarse sigue contando como timeout.
    if (controller.signal.aborted) throw controller.signal.reason;
  } catch (e) {
    return fail(e instanceof Error ? e.message : String(e), true);
  } finally {
    clearTimeout(timer);
  }
  await db
    .update(jobs)
    .set({ status: 'done', lockedUntil: null, lastError: null, finishedAt: new Date() })
    .where(eq(jobs.id, job.id));
  return 'done';
}

export type RunResult = { claimed: number; done: number; retried: number; failed: number; scheduled: number };

/**
 * Ejecuta lo que toca ahora: encola los horarios vencidos y procesa hasta `limit` tareas. Pensado para llamarse
 * desde un cron HTTP (serverless) o en bucle desde `runWorker`.
 */
export async function runDue(
  opts: { limit?: number; concurrency?: number; now?: Date } = {},
  db: Executor = getDb(),
): Promise<RunResult> {
  const now = opts.now ?? new Date();
  const scheduled = await tickSchedules(now, db);
  const claimed = await claimJobs(opts.limit ?? 20, now, db);
  const result: RunResult = { claimed: claimed.length, done: 0, retried: 0, failed: 0, scheduled };
  const queue = [...claimed];
  const worker = async () => {
    for (let job = queue.shift(); job; job = queue.shift()) {
      const r = await runJob(job, db);
      if (r === 'done') result.done++;
      else if (r === 'retry') result.retried++;
      else result.failed++;
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, opts.concurrency ?? 1) }, worker));
  return result;
}

/** Bucle de worker para procesos Node de larga duración. Para con `signal`. */
export async function runWorker(
  opts: { concurrency?: number; pollMs?: number; signal?: AbortSignal } = {},
  db: Executor = getDb(),
): Promise<void> {
  const pollMs = opts.pollMs ?? 1000;
  while (!opts.signal?.aborted) {
    const r = await runDue({ limit: (opts.concurrency ?? 1) * 5, concurrency: opts.concurrency ?? 1 }, db);
    if (r.claimed === 0) await new Promise((res) => setTimeout(res, pollMs));
  }
}

export function nextCronDate(cron: string, after: Date, timezone = 'UTC'): Date {
  return CronExpressionParser.parse(cron, { currentDate: after, tz: timezone }).next().toDate();
}

/** Programa (o reprograma) `type` con una expresión cron de 5 campos. Un horario por tipo. */
export async function schedule(
  type: string,
  cron: string,
  opts: { payload?: unknown; timezone?: string } = {},
  db: Executor = getDb(),
): Promise<void> {
  const def = getJob(type);
  if (!def) throw new Error(`unknown job type: ${type} (call defineJob first)`);
  if (cron.trim().split(/\s+/).length !== 5) throw new Error('cron must have 5 fields');
  const payload = def.schema.parse(opts.payload ?? {});
  const timezone = opts.timezone ?? 'UTC';
  const nextRunAt = nextCronDate(cron, new Date(), timezone);
  await db
    .insert(jobSchedules)
    .values({ type, cron, timezone, payload, nextRunAt })
    .onConflictDoUpdate({ target: jobSchedules.type, set: { cron, timezone, payload, nextRunAt } });
}

export async function unschedule(type: string, db: Executor = getDb()): Promise<void> {
  await db.delete(jobSchedules).where(eq(jobSchedules.type, type));
}

/** Encola una ejecución por horario vencido (una sola aunque se perdieran varias) y calcula la siguiente. */
async function tickSchedules(now: Date, db: Executor): Promise<number> {
  const due = await db
    .select()
    .from(jobSchedules)
    .where(lte(jobSchedules.nextRunAt, now))
    .for('update', { skipLocked: true });
  let n = 0;
  for (const s of due) {
    if (getJob(s.type)) {
      const job = await enqueue(s.type, s.payload, { dedupeKey: `schedule:${s.type}`, runAt: now }, db);
      if (job) n++;
    }
    await db
      .update(jobSchedules)
      .set({ lastRunAt: now, nextRunAt: nextCronDate(s.cron, now, s.timezone) })
      .where(eq(jobSchedules.type, s.type));
  }
  return n;
}

/** Borra tareas terminadas (`done`) más antiguas que `olderThanDays`; las `failed` se conservan para revisarlas. */
export async function pruneJobs(olderThanDays = 7, db: Executor = getDb()): Promise<number> {
  const cutoff = new Date(Date.now() - olderThanDays * 86_400_000);
  const rows = await db
    .delete(jobs)
    .where(and(eq(jobs.status, 'done'), lt(jobs.finishedAt, cutoff)))
    .returning({ id: jobs.id });
  return rows.length;
}

/** Vuelve a encolar una tarea `failed` (acción del admin). */
export async function retryJob(id: string, db: Executor = getDb()): Promise<boolean> {
  const rows = await db
    .update(jobs)
    .set({ status: 'queued', attempts: 0, runAt: new Date(), lastError: null, finishedAt: null })
    .where(and(eq(jobs.id, id), eq(jobs.status, 'failed')))
    .returning({ id: jobs.id });
  return rows.length > 0;
}
