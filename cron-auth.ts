// Protección del endpoint de cron: `Authorization: Bearer <CRON_SECRET>` comparado en tiempo constante.

export function isAuthorizedCron(authorization: string | null | undefined, secret = process.env.CRON_SECRET): boolean {
  if (!secret || secret.length < 32) throw new Error('CRON_SECRET must be set (>= 32 chars)');
  const given = new TextEncoder().encode(authorization ?? '');
  const expected = new TextEncoder().encode(`Bearer ${secret}`);
  let diff = given.length ^ expected.length;
  for (let i = 0; i < expected.length; i++) diff |= (given[i] ?? 0) ^ expected[i]!;
  return diff === 0;
}
