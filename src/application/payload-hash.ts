import { createHash } from 'node:crypto';
import { Money } from '../domain/money';
import type { SubmitWagerCommand } from './process-wager.service';

/** JSON canônico: chaves ordenadas recursivamente, sem espaços. */
function canonical(v: unknown): string {
  if (Array.isArray(v)) return '[' + v.map(canonical).join(',') + ']';
  if (v && typeof v === 'object') {
    const o = v as Record<string, unknown>;
    return '{' + Object.keys(o).sort().map((k) => JSON.stringify(k) + ':' + canonical(o[k])).join(',') + '}';
  }
  return JSON.stringify(v ?? null);
}

/** SHA-256 dos campos de negócio. O header Idempotency-Key e metadados de transporte NÃO entram. */
export function payloadHash(c: SubmitWagerCommand): string {
  const business = {
    providerId: c.providerId,
    externalTransactionId: c.externalTransactionId,
    playerId: c.playerId,
    walletId: c.walletId,
    roundId: c.roundId,
    gameId: c.gameId,
    kind: c.kind,
    money: Money.from(c.money).toJSON(), // normaliza '25' e '25.00' para o mesmo valor
    referenceExternalTransactionId: c.referenceExternalTransactionId ?? null,
  };
  return createHash('sha256').update(canonical(business)).digest('hex');
}