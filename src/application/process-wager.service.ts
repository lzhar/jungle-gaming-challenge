import { Inject, Injectable } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { Db } from '../infrastructure/db';
import { logger } from '../infrastructure/logger';
import { metrics } from '../infrastructure/metrics';
import { Store } from '../infrastructure/store';
import { IdempotencyConflictError, InsufficientFundsError } from '../domain/errors';
import {
  WagerTransactionPendingReference, WagerTransactionProcessed, WagerTransactionRejected,
  WalletBalanceChanged, type EventContext,
} from '../events';
import { FailureCode } from '../domain/failure-code';
import { LedgerDirection } from '../ledger-entry';
import { InboxMessage } from '../domain/messaging';
import { Money, type MoneyProps } from '../domain/money';
import type { Wallet } from '../wallet';
import { WagerTransaction, WagerTransactionKind as K, WagerTransactionStatus as S } from '../wager-transaction';
import { toInfraError } from './errors';
import { payloadHash } from './payload-hash';

export interface SubmitWagerCommand {
  idempotencyKey: string; providerId: string; externalTransactionId: string; playerId: string;
  walletId: string; roundId: string; gameId: string; kind: K; money: MoneyProps;
  referenceExternalTransactionId?: string;
}
export interface ExecContext {
  correlationId: string; causationId?: string;
  inbox?: { consumerName: string; messageId: string };
}
export interface WagerResult {
  transactionId: string; status: S; failureCode?: FailureCode; balance?: MoneyProps; idempotentReplay: boolean;
}

// Referência ausente: 8 tentativas com backoff 5s, 10s, 20s ... teto 300s (~15 min no total).
const MAX_PENDING_ATTEMPTS = 8;
const backoffMs = (attempt: number) => Math.min(5_000 * 2 ** attempt, 300_000);

const toResult = (tx: WagerTransaction, replay: boolean): WagerResult => ({
  transactionId: tx.id, status: tx.status, failureCode: tx.failureCode,
  balance: tx.state.balanceAfter?.toJSON(), idempotentReplay: replay,
});

@Injectable()
export class ProcessWagerService {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  async execute(cmd: SubmitWagerCommand, ctx: ExecContext): Promise<WagerResult> {
    const started = performance.now();
    try {
      const money = Money.from(cmd.money);
      const hash = payloadHash(cmd);

      const result = await this.orm.em.fork().transactional(async (em) => {
        const db = new Db(em);
        const store = new Store(db);
        const now = new Date();

        // 1) Inbox (só para mensagens vindas da fila). Duplicata cai no replay logo abaixo.
        if (ctx.inbox) {
          const inbox = InboxMessage.receive({ ...ctx.inbox, payloadHash: hash, now });
          await db.q(
            'INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at) VALUES (?,?,?,?) ON CONFLICT DO NOTHING',
            [inbox.consumerName, inbox.messageId, inbox.payloadHash, inbox.receivedAt],
          );
        }

        // 2) Lock da wallet: a partir daqui ninguém mexe nesse saldo até o commit.
        const wallet = await store.lockWallet(cmd.walletId);

        // 3) Idempotência (seguro porque estamos dentro do lock da wallet)
        const existing = await store.findByIdempotency(cmd.idempotencyKey, cmd.providerId, cmd.externalTransactionId);
        if (existing) {
          if (existing.state.idempotencyKey !== cmd.idempotencyKey || !existing.matchesPayload(hash)) {
            throw new IdempotencyConflictError();
          }
          return toResult(existing, true);
        }

        // 4) Nova transação: nasce PENDING e é aplicada na mesma transação SQL
        const tx = WagerTransaction.create({ ...cmd, money, payloadHash: hash, now });
        await store.insertTx(tx);
        const ec: EventContext = { correlationId: ctx.correlationId, causationId: ctx.causationId, now };
        await this.apply(store, wallet, tx, ec);

        if (ctx.inbox) {
          await db.q('UPDATE inbox_messages SET processed_at = ? WHERE consumer_name = ? AND message_id = ?', [now, ctx.inbox.consumerName, ctx.inbox.messageId]);
        }
        return toResult(tx, false);
      });

      // Métricas e logs só DEPOIS do commit (e sem valores monetários)
      if (result.idempotentReplay) metrics.duplicates.inc({ source: ctx.inbox ? 'sqs' : 'http' });
      else metrics.transactions.inc({ status: result.status });
      metrics.latency.observe((performance.now() - started) / 1000);
      logger.info({
        correlationId: ctx.correlationId, messageId: ctx.inbox?.messageId, transactionId: result.transactionId,
        walletId: cmd.walletId, providerId: cmd.providerId, status: result.status, replay: result.idempotentReplay,
      }, 'wager handled');
      return result;
    } catch (e) {
      throw toInfraError(e);
    }
  }

  /** Reprocessa UMA pendência vencida. Retorna false quando não há nada a fazer. */
  async reprocessOne(): Promise<boolean> {
    try {
      return await this.orm.em.fork().transactional(async (em) => {
        const store = new Store(new Db(em));
        const tx = await store.claimDuePending();
        if (!tx) return false;
        const wallet = await store.lockWallet(tx.state.walletId);
        const now = new Date();
        await this.apply(store, wallet, tx, { correlationId: tx.id, now });
        return true;
      });
    } catch (e) {
      toInfraError(e);
      return false;
    }
  }

  async reprocessDue(limit = 20): Promise<number> {
    let n = 0;
    while (n < limit && (await this.reprocessOne())) n++;
    return n;
  }

  // ---------------------------------------------------------------- regras de negócio

  private async apply(store: Store, wallet: Wallet, tx: WagerTransaction, ec: EventContext): Promise<void> {
    const s = tx.state;
    if (s.playerId !== wallet.playerId) return this.rejectTx(store, wallet, tx, FailureCode.PlayerMismatch, ec);
    if (s.money.currency !== wallet.currency) return this.rejectTx(store, wallet, tx, FailureCode.CurrencyMismatch, ec);

    let reference: WagerTransaction | undefined;
    if (tx.requiresReference()) {
      reference = await store.findByExternal(s.providerId, s.referenceExternalTransactionId!);
      const verdict = reference ? await this.checkReference(store, tx, reference) : 'WAIT';
      if (verdict === 'WAIT') return this.waitOrExpire(store, wallet, tx, ec);
      if (verdict) return this.rejectTx(store, wallet, tx, verdict, ec);
    }

    // LOSS: registra o resultado sem mexer no saldo e sem ledger
    if (!tx.affectsBalance()) {
      tx.markProcessed(undefined, wallet.balance, ec.now);
      await store.saveTx(tx);
      await store.enqueue([WagerTransactionProcessed.from(tx, ec)]);
      return;
    }

    try {
      const direction = tx.ledgerDirectionFor(reference);
      const entry = direction === LedgerDirection.Debit
        ? wallet.debit(tx.id, s.money, ec.now)
        : wallet.credit(tx.id, s.money, ec.now);
      await store.saveWallet(wallet, entry);
      tx.markProcessed(reference?.id, wallet.balance, ec.now);
      await store.saveTx(tx);
      await store.enqueue([WagerTransactionProcessed.from(tx, ec), WalletBalanceChanged.from(wallet, entry, ec)]);
    } catch (e) {
      if (!(e instanceof InsufficientFundsError)) throw e;
      // BET sem saldo e reversão que deixaria saldo negativo têm códigos diferentes
      const code = s.kind === K.Bet ? FailureCode.InsufficientFunds : FailureCode.ReversalWouldOverdraw;
      await this.rejectTx(store, wallet, tx, code, ec);
    }
  }

  /** Retorna um FailureCode (rejeitar), 'WAIT' (esperar a referência) ou undefined (ok). */
  private async checkReference(store: Store, tx: WagerTransaction, ref: WagerTransaction): Promise<FailureCode | 'WAIT' | undefined> {
    const s = tx.state;
    const r = ref.state;
    if (ref.status === S.Pending || ref.status === S.PendingReference) return 'WAIT';
    if (ref.status !== S.Processed) return FailureCode.ReferenceNotProcessed;
    if (r.providerId !== s.providerId || r.playerId !== s.playerId || r.walletId !== s.walletId || r.roundId !== s.roundId) {
      return FailureCode.ReferenceMismatch;
    }
    if (r.money.currency !== s.money.currency) return FailureCode.CurrencyMismatch;
    const allowed = s.kind === K.Refund ? [K.Bet] : [K.Bet, K.Win, K.Refund];
    if (!allowed.includes(r.kind)) return FailureCode.ReferenceKindInvalid;
    if (!r.money.equals(s.money)) return FailureCode.AmountMismatch;
    if (await store.isReversed(ref.id)) return FailureCode.AlreadyReversed;
    return undefined;
  }

  private async waitOrExpire(store: Store, wallet: Wallet, tx: WagerTransaction, ec: EventContext): Promise<void> {
    const attemptsSoFar = tx.state.attempts;
    if (attemptsSoFar + 1 >= MAX_PENDING_ATTEMPTS) {
      return this.rejectTx(store, wallet, tx, FailureCode.ReferenceNotFound, ec);
    }
    tx.markPendingReference(ec.now, backoffMs(attemptsSoFar));
    await store.saveTx(tx);
    if (attemptsSoFar === 0) await store.enqueue([WagerTransactionPendingReference.from(tx, ec)]);
  }

  private async rejectTx(store: Store, wallet: Wallet, tx: WagerTransaction, code: FailureCode, ec: EventContext): Promise<void> {
    tx.reject(code, wallet.balance, ec.now); // rejeitada: sem saldo alterado e sem ledger
    await store.saveTx(tx);
    await store.enqueue([WagerTransactionRejected.from(tx, ec)]);
  }
}