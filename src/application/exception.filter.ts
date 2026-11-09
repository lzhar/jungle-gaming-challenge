import { Catch, HttpException, type ArgumentsHost, type ExceptionFilter } from '@nestjs/common';
import type { Response } from 'express';
import { DomainError, TransientError } from '../domain/errors';
import { logger } from '../infrastructure/logger';

const STATUS: Record<string, number> = {
  VALIDATION_ERROR: 400,        // payload inválido: corrigir antes de reenviar
  IDEMPOTENCY_CONFLICT: 409,    // mesma chave, payload diferente: não reenviar igual
  WALLET_ALREADY_EXISTS: 409,
  WALLET_NOT_FOUND: 404,
  NOT_FOUND: 404,
};

@Catch()
export class AppExceptionFilter implements ExceptionFilter {
  catch(e: unknown, host: ArgumentsHost) {
    const res = host.switchToHttp().getResponse<Response>();
    if (e instanceof HttpException) {
      return res.status(e.getStatus()).json({ error: 'HTTP_ERROR', message: e.message });
    }
    if (e instanceof DomainError) {
      return res.status(STATUS[e.code] ?? 422).json({ error: e.code, message: e.message });
    }
    if (e instanceof TransientError) {
      // falha transitória: pode reenviar com a MESMA Idempotency-Key
      res.setHeader('Retry-After', '2');
      return res.status(503).json({ error: 'TRANSIENT_ERROR', message: 'temporary failure, retry with the same Idempotency-Key' });
    }
    logger.error({ message: String((e as Error)?.message) }, 'unhandled error');
    return res.status(500).json({ error: 'INTERNAL_ERROR' });
  }
}