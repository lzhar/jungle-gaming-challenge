// src/app.module.ts
import { Module } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { CreateWalletService } from './application/create-wallet.service';
import { ProcessWagerService } from './application/process-wager.service';
import { QueryService } from './application/query.service';
import { AuthGuard } from './http/auth.guard';
import { OpsController } from './http/ops.controller';
import { WageringController } from './http/wagering.controller';
import { createOrm, OrmShutdown } from './infrastructure/db';
import { WagerConsumer } from './infrastructure/wager.consumer';
import { OutboxPublisher } from './infrastructure/outbox.publisher';
import { PendingReferenceWorker } from './infrastructure/pending.worker';
import { Queues, sqsProvider } from './application/sqs';

@Module({
  controllers: [WageringController, OpsController],
  providers: [
    { provide: MikroORM, useFactory: () => createOrm() },
    OrmShutdown, sqsProvider, Queues, AuthGuard,
    ProcessWagerService, CreateWalletService, QueryService,
    WagerConsumer, OutboxPublisher, PendingReferenceWorker,
  ],
})
export class AppModule {}