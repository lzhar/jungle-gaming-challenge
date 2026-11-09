import { Inject, Injectable } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import Decimal from 'decimal.js';
import { Db } from '../infrastructure/db';
import { logger } from '../infrastructure/logger';
import { metrics } from '../infrastructure/metrics';
import { DomainError, ValidationError, WalletNotFoundError } from '../domain/errors';
import { toInfraError } from './errors';

const notFound = () => new DomainError('NOT_FOUND', 'not found');
const money = (amount: string, currency: string) => ({ amount, currency });

const txView = (r: any) => ({
  transactionId: r.id, providerId: r.provider_id, externalTransactionId: r.external_transaction_id,
  walletId: r.wallet_id, kind: r.kind, status: r.status, failureCode: r.failure_code ?? undefined,
  money: money(r.amount, r.currency),
  balance: r.balance_after === null ? undefined : money(r.balance_after, r.currency),
  createdAt: r.created_at, processedAt: r.processed_at ?? undefined,
});

@Injectable()
export class QueryService {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}
  private db() { return new Db(this.orm.em.fork()); }

  async wallet(id: string) {
    try {
      const [r] = await this.db().q('SELECT * FROM wallets WHERE id = ?', [id]);
      if (!r) throw new WalletNotFoundError();
      return { id: r.id, playerId: r.player_id, balance: money(r.balance, r.currency), version: r.version };
    } catch (e) { throw toInfraError(e); }
  }

  /** Cursor opaco e estável: base64url do `seq` (bigserial) do último item. */
  async ledger(walletId: string, cursor?: string, limitRaw?: string) {
    try {
      const limit = Math.min(Math.max(Number(limitRaw ?? 50) || 50, 1), 200);
      let after = '0';
      if (cursor) {
        after = Buffer.from(cursor, 'base64url').toString('utf8');
        if (!/^\d+$/.test(after)) throw new ValidationError('invalid cursor');
      }
      const rows = await this.db().q(
        'SELECT * FROM wallet_ledger_entries WHERE wallet_id = ? AND seq > ? ORDER BY seq LIMIT ?',
        [walletId, after, limit + 1],
      );
      const page = rows.slice(0, limit);
      const nextCursor = rows.length > limit ? Buffer.from(String(page[page.length - 1].seq)).toString('base64url') : null;
      return {
        items: page.map((r: any) => ({
          id: r.id, transactionId: r.transaction_id, direction: r.direction,
          money: money(r.amount, r.currency ?? 'BRL'), balanceBefore: r.balance_before,
          balanceAfter: r.balance_after, createdAt: r.created_at,
        })),
        nextCursor,
      };
    } catch (e) { throw toInfraError(e); }
  }

  async transactionById(id: string) {
    try {
      const [r] = await this.db().q('SELECT * FROM wager_transactions WHERE id = ?', [id]);
      if (!r) throw notFound();
      return txView(r);
    } catch (e) { throw toInfraError(e); }
  }

  async transactionByExternal(providerId: string, externalId: string) {
    try {
      const [r] = await this.db().q('SELECT * FROM wager_transactions WHERE provider_id = ? AND external_transaction_id = ?', [providerId, externalId]);
      if (!r) throw notFound();
      return txView(r);
    } catch (e) { throw toInfraError(e); }
  }

  /** Compara saldo guardado x soma do ledger em UM único statement (um snapshot consistente). Nunca corrige sozinho. */
  async reconcile(walletId: string) {
    try {
      const [r] = await this.db().q(
        `SELECT w.currency, w.balance AS stored,
                COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0) AS calculated,
                COUNT(l.id) AS entries
           FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
          WHERE w.id = ? GROUP BY w.id`,
        [walletId],
      );
      if (!r) throw new WalletNotFoundError();
      const stored = new Decimal(r.stored);
      const calculated = new Decimal(r.calculated);
      const difference = stored.minus(calculated);
      const consistent = difference.isZero();
      if (!consistent) {
        metrics.divergences.inc();
        logger.error({ walletId }, 'RECONCILIATION DIVERGENCE');
      }
      return {
        walletId, storedBalance: money(stored.toFixed(2), r.currency),
        calculatedBalance: money(calculated.toFixed(2), r.currency),
        difference: money(difference.toFixed(2), r.currency),
        consistent, checkedEntries: Number(r.entries),
      };
    } catch (e) { throw toInfraError(e); }
  }
}