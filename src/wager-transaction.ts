import { newId } from './shared-ids';
import { InvalidTransactionStateError, ValidationError } from './domain/errors';
import type { FailureCode } from './domain/failure-code';
import { LedgerDirection } from './ledger-entry';
import { Money } from './domain/money';

export enum WagerTransactionKind {
  Opening = 'OPENING', Bet = 'BET', Win = 'WIN', Loss = 'LOSS', Refund = 'REFUND', Rollback = 'ROLLBACK',
}
export enum WagerTransactionStatus {
  Pending = 'PENDING', PendingReference = 'PENDING_REFERENCE', Processed = 'PROCESSED',
  Rejected = 'REJECTED', Failed = 'FAILED',
}
const S = WagerTransactionStatus;
const K = WagerTransactionKind;

/** Transições válidas (documente no ARCHITECTURE.md). Estados terminais não têm saída. */
const TRANSITIONS: Record<WagerTransactionStatus, WagerTransactionStatus[]> = {
  [S.Pending]: [S.Processed, S.Rejected, S.Failed, S.PendingReference],
  [S.PendingReference]: [S.Processed, S.Rejected, S.Failed, S.PendingReference],
  [S.Processed]: [], [S.Rejected]: [], [S.Failed]: [],
};

export interface WagerTransactionState {
  id: string; providerId: string; externalTransactionId: string; idempotencyKey: string;
  payloadHash: string; walletId: string; playerId: string; roundId: string; gameId: string;
  kind: WagerTransactionKind; money: Money; referenceExternalTransactionId?: string;
  status: WagerTransactionStatus; referenceTransactionId?: string; failureCode?: FailureCode;
  balanceAfter?: Money; attempts: number; nextAttemptAt?: Date; createdAt: Date; processedAt?: Date;
}

export type CreateWagerTransactionProps = Pick<WagerTransactionState,
  'providerId' | 'externalTransactionId' | 'idempotencyKey' | 'payloadHash' | 'walletId' |
  'playerId' | 'roundId' | 'gameId' | 'kind' | 'money' | 'referenceExternalTransactionId'> & { now: Date };

export class WagerTransaction {
  private constructor(private readonly s: WagerTransactionState) {}

  /** Nasce em PENDING. OPENING é interno e não pode ser criado por aqui. */
  static create(p: CreateWagerTransactionProps): WagerTransaction {
    if (p.kind === K.Opening) throw new ValidationError('OPENING is internal');
    if (!p.money.isPositive()) throw new ValidationError('amount must be positive');
    if ((p.kind === K.Refund || p.kind === K.Rollback) && !p.referenceExternalTransactionId) {
      throw new ValidationError('referenceExternalTransactionId is required for ' + p.kind);
    }
    const { now, ...rest } = p;
    return new WagerTransaction({ ...rest, id: newId(), status: S.Pending, attempts: 0, createdAt: now });
  }

  /** Crédito de abertura da wallet (uso interno). */
  static createOpening(p: { walletId: string; playerId: string; money: Money; now: Date }): WagerTransaction {
    return new WagerTransaction({
      id: newId(), providerId: 'system', externalTransactionId: 'opening:' + p.walletId,
      idempotencyKey: 'opening:' + p.walletId, payloadHash: 'opening', walletId: p.walletId,
      playerId: p.playerId, roundId: 'opening', gameId: 'opening', kind: K.Opening, money: p.money,
      status: S.Pending, attempts: 0, createdAt: p.now,
    });
  }

  static rehydrate(s: WagerTransactionState): WagerTransaction { return new WagerTransaction(s); }

  get state(): Readonly<WagerTransactionState> { return this.s; }
  get id(): string { return this.s.id; }
  get kind(): WagerTransactionKind { return this.s.kind; }
  get status(): WagerTransactionStatus { return this.s.status; }
  get failureCode(): FailureCode | undefined { return this.s.failureCode; }

  private transition(to: WagerTransactionStatus): void {
    if (!TRANSITIONS[this.s.status].includes(to)) {
      throw new InvalidTransactionStateError(this.s.status + ' -> ' + to);
    }
    this.s.status = to;
  }

  markProcessed(referenceTransactionId: string | undefined, balanceAfter: Money, at: Date): void {
    this.transition(S.Processed);
    Object.assign(this.s, { referenceTransactionId, balanceAfter, processedAt: at, nextAttemptAt: undefined });
  }
  markPendingReference(now: Date, delayMs: number): void {
    this.transition(S.PendingReference);
    this.s.attempts += 1;
    this.s.nextAttemptAt = new Date(now.getTime() + delayMs);
  }
  reject(code: FailureCode, balanceAfter: Money, at: Date): void {
    this.transition(S.Rejected);
    Object.assign(this.s, { failureCode: code, balanceAfter, processedAt: at, nextAttemptAt: undefined });
  }
  fail(code: FailureCode, at: Date): void {
    this.transition(S.Failed);
    Object.assign(this.s, { failureCode: code, processedAt: at, nextAttemptAt: undefined });
  }

  isTerminal(): boolean { return TRANSITIONS[this.s.status].length === 0; }
  affectsBalance(): boolean { return this.s.kind !== K.Loss; }
  requiresReference(): boolean { return this.s.kind === K.Refund || this.s.kind === K.Rollback; }
  matchesPayload(hash: string): boolean { return this.s.payloadHash === hash; }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.s.kind) {
      case K.Bet: return LedgerDirection.Debit;
      case K.Win: case K.Refund: case K.Opening: return LedgerDirection.Credit;
      case K.Rollback: {
        if (!reference) throw new InvalidTransactionStateError('ROLLBACK needs a reference');
        // inverso da referência: BET (débito) -> crédito; WIN/REFUND (crédito) -> débito
        return reference.kind === K.Bet ? LedgerDirection.Credit : LedgerDirection.Debit;
      }
      default: throw new InvalidTransactionStateError(this.s.kind + ' has no ledger direction');
    }
  }
}