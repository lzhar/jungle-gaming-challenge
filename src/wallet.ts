import { newId } from '../shared-ids';
import { CurrencyMismatchError, InsufficientFundsError, ValidationError } from './errors';
import { LedgerDirection, WalletLedgerEntry } from './ledger-entry';
import { Money } from './money';

export interface WalletState {
  id: string; playerId: string; currency: string; balance: Money;
  version: number; createdAt: Date; updatedAt: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(p: { id: string; playerId: string; initialBalance: Money; now?: Date }): Wallet {
    if (p.initialBalance.isNegative()) throw new ValidationError('initial balance must be >= 0');
    const now = p.now ?? new Date();
    return new Wallet(p.id, p.playerId, p.initialBalance.currency, p.initialBalance, 1, now, now);
  }

  static rehydrate(s: WalletState): Wallet {
    return new Wallet(s.id, s.playerId, s.currency, s.balance, s.version, s.createdAt, s.updatedAt);
  }

  get balance(): Money { return this._balance; }
  get version(): number { return this._version; }
  get updatedAt(): Date { return this._updatedAt; }

  debit(transactionId: string, amount: Money, now: Date): WalletLedgerEntry {
    this.assertSameCurrency(amount);
    if (this._balance.isLessThan(amount)) throw new InsufficientFundsError();
    return this.apply(transactionId, LedgerDirection.Debit, amount, this._balance.subtract(amount), now);
  }

  credit(transactionId: string, amount: Money, now: Date): WalletLedgerEntry {
    this.assertSameCurrency(amount);
    return this.apply(transactionId, LedgerDirection.Credit, amount, this._balance.add(amount), now);
  }

  /** Muda o saldo e produz o lançamento no mesmo passo: saldo e ledger nunca divergem. */
  private apply(txId: string, direction: LedgerDirection, amount: Money, after: Money, now: Date): WalletLedgerEntry {
    const entry = WalletLedgerEntry.create({
      id: newId(), walletId: this.id, transactionId: txId, direction,
      money: amount, balanceBefore: this._balance, balanceAfter: after, createdAt: now,
    });
    this._balance = after;
    this._version += 1; // só incrementa quando o saldo muda
    this._updatedAt = now;
    return entry;
  }

  private assertSameCurrency(m: Money): void {
    if (m.currency !== this.currency) throw new CurrencyMismatchError(this.currency, m.currency);
  }
}