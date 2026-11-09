import { z } from 'zod';
import { ValidationError } from '../domain/errors';

const moneySchema = z.object({ amount: z.string(), currency: z.string() });

const wagerBase = z.object({
  providerId: z.string().min(1).max(100),
  externalTransactionId: z.string().min(1).max(200),
  playerId: z.string().uuid(),
  walletId: z.string().uuid(),
  roundId: z.string().min(1).max(200),
  gameId: z.string().min(1).max(200),
  kind: z.enum(['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']), // OPENING nunca entra por fora
  money: moneySchema,
  referenceExternalTransactionId: z.string().min(1).max(200).optional(),
});

const needsRef = (b: { kind: string; referenceExternalTransactionId?: string }) =>
  !['REFUND', 'ROLLBACK'].includes(b.kind) || !!b.referenceExternalTransactionId;
const refMsg = { message: 'referenceExternalTransactionId is required for REFUND and ROLLBACK' };

export const wagerBody = wagerBase.refine(needsRef, refMsg);

export const wagerMessage = z.object({
  messageId: z.string().min(1),
  type: z.literal('WagerTransactionRequested'),
  occurredAt: z.string(),
  data: wagerBase.extend({ idempotencyKey: z.string().min(1) }).refine(needsRef, refMsg),
});

export const walletBody = z.object({ playerId: z.string().uuid(), initialBalance: moneySchema });
export const uuidParam = z.string().uuid();

export function parse<S extends z.ZodTypeAny>(schema: S, data: unknown): z.infer<S> {
  const r = schema.safeParse(data);
  if (!r.success) {
    throw new ValidationError(r.error.issues.map((i) => i.path.join('.') + ': ' + i.message).join('; '));
  }
  return r.data;
}