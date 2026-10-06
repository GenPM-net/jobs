// Registro en memoria de tipos de tarea. Cada proceso (web y worker) importa los mismos `defineJob`.
import type { ZodType } from 'zod';

export type JobContext = {
  jobId: string;
  /** Intento actual (1 = primero). */
  attempt: number;
  /** Se aborta al vencer `timeoutMs`; pásala a fetch() y a operaciones largas. */
  signal: AbortSignal;
};

export type JobDefinition<P> = {
  type: string;
  schema: ZodType<P>;
  handler: (payload: P, ctx: JobContext) => Promise<void>;
  maxAttempts: number;
  timeoutMs: number;
};

const registry = new Map<string, JobDefinition<unknown>>();

export function registerJob<P>(def: JobDefinition<P>): void {
  if (!/^[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)*$/.test(def.type)) throw new Error(`invalid job type: ${def.type}`);
  registry.set(def.type, def as JobDefinition<unknown>);
}

export function getJob(type: string): JobDefinition<unknown> | undefined {
  return registry.get(type);
}

/** Solo tests. */
export function clearJobs(): void {
  registry.clear();
}
