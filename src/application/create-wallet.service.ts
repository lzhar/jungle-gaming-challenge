import { Inject, Injectable } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { Db } from '../infrastructure/db';
import { Store } from '../infrastructure/store';
import { WalletAlreadyExistsError } from '../domain/errors';
import { WagerTransactionProcessed, WalletBalanceChanged } from '../events';
import { LedgerDirection, WalletLedgerEntry } from '../ledger-entry';
import { Money, type MoneyProps } from '../domain/money';
import { Wallet } from '../wallet';
import { WagerTransaction } from '../wager-transaction';
import { newId } from '../shared-ids';
import { toInfraError } from './errors';

@Injectable()
export class CreateWalletService {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  async execute(playerId: string, initial: MoneyProps, correlationId: string) {
    try {
      const initialBalance = Money.from(initial);
      const now = new Date();
      const wallet = Wallet.open({ id: newId(), playerId, initialBalance, now });

      await this.orm.em.fork().transactional(async (em) => {
        const store = new Store(new Db(em));
        if (!(await store.insertWallet(wallet))) throw new WalletAlreadyExistsError();

        if (initialBalance.isPositive()) {
          // Saldo inicial > 0 gera OPENING + crédito no ledger, na mesma transação
          const tx = WagerTransaction.createOpening({ walletId: wallet.id, playerId, money: initialBalance, now });
          const entry = WalletLedgerEntry.create({
            id: newId(), walletId: wallet.id, transactionId: tx.id, direction: LedgerDirection.Credit,
            money: initialBalance, balanceBefore: Money.zero(initialBalance.currency),
            balanceAfter: initialBalance, createdAt: now,
          });
          tx.markProcessed(undefined, wallet.balance, now);
          await store.insertTx(tx);
          await store.insertLedger(entry);
          const ec = { correlationId, now };
          await store.enqueue([WagerTransactionProcessed.from(tx, ec), WalletBalanceChanged.from(wallet, entry, ec)]);
        }
      });
      return { id: wallet.id, playerId, balance: wallet.balance.toJSON(), version: wallet.version };
    } catch (e) {
      throw toInfraError(e);
    }
  }
}