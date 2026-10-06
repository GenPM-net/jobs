// Adaptador Hono: `app.route('/api/cron', cronRoutes())`. El cron del hosting hace POST con `Authorization: Bearer <CRON_SECRET>`.
import { Hono } from 'hono';
import { isAuthorizedCron, runDue } from '../index.js';

export function cronRoutes(opts: { limit?: number; concurrency?: number } = {}) {
  return new Hono().post('/', async (c) => {
    if (!isAuthorizedCron(c.req.header('authorization'))) return c.json({ error: 'unauthorized' }, 401);
    return c.json(await runDue(opts));
  });
}
