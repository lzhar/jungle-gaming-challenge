import type { Db } from './db';
import { metrics } from './metrics';
import { WalletNotFoundError, TransientError } from '../domain/errors';
import type { IntegrationEvent } from '../events';
import type { FailureCode } from '../domain/failure-code';
import type { WalletLedgerEntry } from '../ledger-entry';
import { OutboxMessage } from '../domain/messaging';
import { Money } from '../domain/money';
import { Wallet } from '../wallet';
import { WagerTransaction, type WagerTransactionKind, type WagerTransactionStatus } from '../wager-transaction';

const toWallet = (r: any) =>
  Wallet.rehydrate({
    id: r.id, playerId: r.player_id, currency: r.currency,
    balance: Money.from({ amount: r.balance, currency: r.currency }),
    version: r.version, createdAt: r.created_at, updatedAt: r.updated_at,
  });

const optMoney = (amount: string | null, currency: string) =>
  amount === null || amount === undefined ? undefined : Money.from({ amount, currency });

export const toTx = (r: any) =>
  WagerTransaction.rehydrate({
    id: r.id, providerId: r.provider_id, externalTransactionId: r.external_transaction_id,
    idempotencyKey: r.idempotency_key, payloadHash: r.payload_hash, walletId: r.wallet_id,
    playerId: r.player_id, roundId: r.round_id, gameId: r.game_id,
    kind: r.kind as WagerTransactionKind,
    money: Money.from({ amount: r.amount, currency: r.currency }),
    referenceExternalTransactionId: r.reference_external_transaction_id ?? undefined,
    status: r.status as WagerTransactionStatus,
    referenceTransactionId: r.reference_transaction_id ?? undefined,
    failureCode: (r.failure_code ?? undefined) as FailureCode | undefined,
    balanceAfter: optMoney(r.balance_after, r.currency),
    attempts: r.attempts, nextAttemptAt: r.next_attempt_at ?? undefined,
    createdAt: r.created_at, processedAt: r.processed_at ?? undefined,
  });

/** Acesso ao banco para as agregações. Sempre recebe um Db ligado à transação corrente. */
export class Store {
  constructor(private readonly db: Db) {}

  /** Lock pessimista por wallet: serializa tudo que toca o mesmo saldo. Outras wallets não são afetadas. */
  async lockWallet(id: string): Promise<Wallet> {
    const rows = await this.db.q('SELECT * FROM wallets WHERE id = ? FOR UPDATE', [id]);
    if (!rows[0]) throw new WalletNotFoundError();
    return toWallet(rows[0]);
  }

  async insertWallet(w: Wallet): Promise<boolean> {
    const rows = await this.db.q(
      'INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT (player_id, currency) DO NOTHING RETURNING id',
      [w.id, w.playerId, w.currency, w.balance.amount, w.version, w.createdAt, w.updatedAt],
    );
    return rows.length === 1;
  }

  async findByIdempotency(key: string, providerId: string, externalId: string) {
    const rows = await this.db.q(
      'SELECT * FROM wager_transactions WHERE idempotency_key = ? OR (provider_id = ? AND external_transaction_id = ?) LIMIT 1',
      [key, providerId, externalId],
    );
    return rows[0] ? toTx(rows[0]) : undefined;
  }

  async findByExternal(providerId: string, externalId: string) {
    const rows = await this.db.q('SELECT * FROM wager_transactions WHERE provider_id = ? AND external_transaction_id = ?', [providerId, externalId]);
    return rows[0] ? toTx(rows[0]) : undefined;
  }

  async isReversed(txId: string): Promise<boolean> {
    const rows = await this.db.q(
      "SELECT 1 FROM wager_transactions WHERE reference_transaction_id = ? AND status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK') LIMIT 1",
      [txId],
    );
    return rows.length > 0;
  }

  /** Pega uma pendência vencida. SKIP LOCKED permite vários workers sem brigar pela mesma linha. */
  async claimDuePending() {
    const rows = await this.db.q(
      "SELECT * FROM wager_transactions WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= now() ORDER BY next_attempt_at LIMIT 1 FOR UPDATE SKIP LOCKED",
    );
    return rows[0] ? toTx(rows[0]) : undefined;
  }

  async insertTx(tx: WagerTransaction): Promise<void> {
    const s = tx.state;
    await this.db.q(
      'INSERT INTO wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id, reference_transaction_id, status, failure_code, balance_after, attempts, next_attempt_at, created_at, processed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [
        s.id, s.providerId, s.externalTransactionId, s.idempotencyKey, s.payloadHash, s.walletId,
        s.playerId, s.roundId, s.gameId, s.kind, s.money.amount, s.money.currency,
        s.referenceExternalTransactionId ?? null, s.referenceTransactionId ?? null, s.status,
        s.failureCode ?? null, s.balanceAfter?.amount ?? null, s.attempts, s.nextAttemptAt ?? null,
        s.createdAt, s.processedAt ?? null,
      ],
    );
  }

  async saveTx(tx: WagerTransaction): Promise<void> {
    const s = tx.state;
    await this.db.q(
      'UPDATE wager_transactions SET status = ?, failure_code = ?, reference_transaction_id = ?, balance_after = ?, attempts = ?, next_attempt_at = ?, processed_at = ? WHERE id = ?',
      [s.status, s.failureCode ?? null, s.referenceTransactionId ?? null, s.balanceAfter?.amount ?? null, s.attempts, s.nextAttemptAt ?? null, s.processedAt ?? null, s.id],
    );
  }

  async insertLedger(e: WalletLedgerEntry): Promise<void> {
    await this.db.q(
      'INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at) VALUES (?,?,?,?,?,?,?,?)',
      [e.id, e.walletId, e.transactionId, e.direction, e.money.amount, e.balanceBefore.amount, e.balanceAfter.amount, e.createdAt],
    );
  }

  /** Atualiza o saldo com guarda de versão (defesa extra além do lock) e grava o lançamento. */
  async saveWallet(w: Wallet, entry: WalletLedgerEntry): Promise<void> {
    const rows = await this.db.q(
      'UPDATE wallets SET balance = ?, version = ?, updated_at = ? WHERE id = ? AND version = ? RETURNING id',
      [w.balance.amount, w.version, w.updatedAt, w.id, w.version - 1],
    );
    if (rows.length !== 1) {
      metrics.lockConflicts.inc();
      throw new TransientError('wallet version conflict');
    }
    await this.insertLedger(entry);
  }

  /** Outbox: grava na MESMA transação do dinheiro. */
  async enqueue(events: IntegrationEvent<unknown>[]): Promise<void> {
    for (const e of events) {
      const o = OutboxMessage.enqueue(e);
      await this.db.q(
        'INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts) VALUES (?,?,?,?::jsonb,?,0)',
        [o.id, o.aggregateId, o.eventType, JSON.stringify(o.payload), o.occurredAt],
      );
    }
  }
}