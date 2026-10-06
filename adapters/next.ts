// Adaptador Next.js: `app/api/cron/route.ts` → `export const POST = cronRoute; export const GET = cronRoute;`
// (Vercel Cron llama con GET y envía `Authorization: Bearer <CRON_SECRET>`). Añade `export const dynamic = 'force-dynamic'`.
import { isAuthorizedCron, runDue } from '../index.ts';

export async function cronRoute(req: Request): Promise<Response> {
  if (!isAuthorizedCron(req.headers.get('authorization'))) return Response.json({ error: 'unauthorized' }, { status: 401 });
  return Response.json(await runDue());
}
