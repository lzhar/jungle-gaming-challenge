import { newId } from './shared-ids';
import type { LedgerDirection, WalletLedgerEntry } from './ledger-entry';
import type { MoneyProps } from './domain/money';
import type { Wallet } from './wallet';
import type { WagerTransaction } from './wager-transaction';

export interface EventContext { correlationId: string; causationId?: string; now: Date }

export interface IntegrationEventProps<T> {
  eventId: string; aggregateId: string; correlationId: string;
  causationId?: string; occurredAt: Date; data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(p: IntegrationEventProps<T>) {
    this.eventId = p.eventId;
    this.aggregateId = p.aggregateId;
    this.correlationId = p.correlationId;
    this.causationId = p.causationId;
    this.occurredAt = p.occurredAt;
    this.data = p.data;
  }

  toJSON() {
    return {
      eventId: this.eventId, eventType: this.eventType, aggregateId: this.aggregateId,
      correlationId: this.correlationId, causationId: this.causationId,
      occurredAt: this.occurredAt.toISOString(), version: this.version, data: this.data,
    };
  }
}

interface TxEventData {
  transactionId: string; walletId: string; providerId: string;
  externalTransactionId: string; kind: string; money: MoneyProps;
}
const txData = (tx: WagerTransaction): TxEventData => ({
  transactionId: tx.id, walletId: tx.state.walletId, providerId: tx.state.providerId,
  externalTransactionId: tx.state.externalTransactionId, kind: tx.kind, money: tx.state.money.toJSON(),
});
const envelope = (aggregateId: string, ctx: EventContext) => ({
  eventId: newId(), aggregateId, correlationId: ctx.correlationId,
  causationId: ctx.causationId, occurredAt: ctx.now,
});

export class WagerTransactionProcessed extends IntegrationEvent<TxEventData & { balanceAfter?: MoneyProps }> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;
  static from(tx: WagerTransaction, ctx: EventContext) {
    return new WagerTransactionProcessed({ ...envelope(tx.state.walletId, ctx), data: { ...txData(tx), balanceAfter: tx.state.balanceAfter?.toJSON() } });
  }
}

export class WagerTransactionRejected extends IntegrationEvent<TxEventData & { failureCode: string }> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;
  static from(tx: WagerTransaction, ctx: EventContext) {
    return new WagerTransactionRejected({ ...envelope(tx.state.walletId, ctx), data: { ...txData(tx), failureCode: tx.failureCode! } });
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<TxEventData & { referenceExternalTransactionId?: string }> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;
  static from(tx: WagerTransaction, ctx: EventContext) {
    return new WagerTransactionPendingReference({ ...envelope(tx.state.walletId, ctx), data: { ...txData(tx), referenceExternalTransactionId: tx.state.referenceExternalTransactionId } });
  }
}

interface WalletBalanceChangedData {
  walletId: string; transactionId: string; direction: LedgerDirection; money: MoneyProps;
  balanceBefore: MoneyProps; balanceAfter: MoneyProps; walletVersion: number;
}
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;
  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext) {
    return new WalletBalanceChanged({
      ...envelope(wallet.id, ctx),
      data: {
        walletId: wallet.id, transactionId: entry.transactionId, direction: entry.direction,
        money: entry.money.toJSON(), balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(), walletVersion: wallet.version,
      },
    });
  }
}