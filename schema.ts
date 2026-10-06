// Tablas de @core/jobs. Las recoge drizzle-kit vía src/lib/db/drizzle.config.ts.
import { sql } from 'drizzle-orm';
import { index, integer, jsonb, pgTable, text, timestamp, uniqueIndex } from 'drizzle-orm/pg-core';
import { primaryId, timestamps } from '../db/index.js';

const ts = (name: string) => timestamp(name, { withTimezone: true, mode: 'date' });

export const jobs = pgTable(
  'jobs',
  {
    id: primaryId('job'),
    type: text('type').notNull(),
    payload: jsonb('payload').$type<unknown>().notNull(),
    status: text('status', { enum: ['queued', 'running', 'done', 'failed'] })
      .notNull()
      .default('queued'),
    runAt: ts('run_at').notNull().defaultNow(),
    attempts: integer('attempts').notNull().default(0),
    maxAttempts: integer('max_attempts').notNull().default(5),
    /** Mientras `running`, nadie más la toma hasta esta hora (si el worker muere, se reintenta al vencer). */
    lockedUntil: ts('locked_until'),
    lastError: text('last_error'),
    /** Evita duplicados mientras haya una pendiente o en curso con la misma clave. */
    dedupeKey: text('dedupe_key'),
    finishedAt: ts('finished_at'),
    ...timestamps,
  },
  (t) => [
    index('jobs_due_idx').on(t.status, t.runAt),
    uniqueIndex('jobs_dedupe_idx')
      .on(t.dedupeKey)
      .where(sql`${t.dedupeKey} is not null and ${t.status} in ('queued', 'running')`),
  ],
);

export const jobSchedules = pgTable('job_schedules', {
  /** Un horario por tipo de tarea. */
  type: text('type').primaryKey(),
  cron: text('cron').notNull(),
  timezone: text('timezone').notNull().default('UTC'),
  payload: jsonb('payload').$type<unknown>().notNull(),
  nextRunAt: ts('next_run_at').notNull(),
  lastRunAt: ts('last_run_at'),
  ...timestamps,
});

export type Job = typeof jobs.$inferSelect;
export type JobSchedule = typeof jobSchedules.$inferSelect;
