import Decimal from 'decimal.js';
import { CurrencyMismatchError, ValidationError } from './errors';

Decimal.set({ precision: 40, rounding: Decimal.ROUND_HALF_EVEN });

export interface MoneyProps {
  amount: string;
  currency: string;
}

export class Money {
  private constructor(private readonly value: Decimal, public readonly currency: string) {}

  /** Entrada externa: string decimal, até 2 casas, sem sinal, sem notação científica. */
  static from(p: MoneyProps): Money {
    if (typeof p?.amount !== 'string' || !/^\d+(\.\d{1,2})?$/.test(p.amount)) {
      throw new ValidationError('invalid amount: ' + String(p?.amount));
    }
    if (typeof p.currency !== 'string' || !/^[A-Z]{3}$/.test(p.currency)) {
      throw new ValidationError('invalid currency');
    }
    return new Money(new Decimal(p.amount), p.currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: '0.00', currency });
  }

  add(o: Money): Money {
    this.assertSameCurrency(o);
    return new Money(this.value.plus(o.value), this.currency);
  }
  subtract(o: Money): Money {
    this.assertSameCurrency(o);
    return new Money(this.value.minus(o.value), this.currency);
  }
  negate(): Money { return new Money(this.value.negated(), this.currency); }

  isZero(): boolean { return this.value.isZero(); }
  isPositive(): boolean { return this.value.gt(0); }
  isNegative(): boolean { return this.value.lt(0); }
  isLessThan(o: Money): boolean { this.assertSameCurrency(o); return this.value.lt(o.value); }
  equals(o: Money): boolean { return this.currency === o.currency && this.value.eq(o.value); }

  get amount(): string { return this.value.toFixed(2); }
  toJSON(): MoneyProps { return { amount: this.amount, currency: this.currency }; }
  toString(): string { return this.amount + ' ' + this.currency; }

  private assertSameCurrency(o: Money): void {
    if (o.currency !== this.currency) throw new CurrencyMismatchError(this.currency, o.currency);
  }
}