import { DomainError, TransientError } from '../domain/errors';
import { logger } from '../infrastructure/logger';
import { metrics } from '../infrastructure/metrics';

/** Erros de domínio passam; qualquer outra falha vira TransientError (pode tentar de novo). */
export function toInfraError(e: unknown): Error {
  if (e instanceof DomainError || e instanceof TransientError) return e;
  const code = (e as any)?.code;
  if (code === '40P01' || code === '40001') metrics.lockConflicts.inc();
  logger.error({ pgCode: code, message: String((e as Error)?.message) }, 'infrastructure failure');
  return new TransientError('infrastructure failure');
}