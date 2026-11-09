import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Res, UseGuards } from '@nestjs/common';
import type { Response } from 'express';
import { CreateWalletService } from '../application/create-wallet.service';
import { ProcessWagerService } from '../application/process-wager.service';
import { QueryService } from '../application/query.service';
import { parse, uuidParam, wagerBody, walletBody } from '../application/schemas';
import { ValidationError } from '../domain/errors';
import { WagerTransactionKind } from '../wager-transaction';
import { AuthGuard } from './auth.guard';

@Controller()
@UseGuards(AuthGuard)
export class WageringController {
  constructor(
    @Inject(ProcessWagerService) private readonly wager: ProcessWagerService,
    @Inject(CreateWalletService) private readonly wallets: CreateWalletService,
    @Inject(QueryService) private readonly queries: QueryService,
  ) {}

  @Post('wallets')
  @HttpCode(201)
  createWallet(@Body() body: unknown, @Headers('x-correlation-id') cid?: string) {
    const b = parse(walletBody, body);
    return this.wallets.execute(b.playerId, b.initialBalance, cid ?? crypto.randomUUID());
  }

  @Get('wallets/:walletId')
  getWallet(@Param('walletId') id: string) {
    return this.queries.wallet(parse(uuidParam, id));
  }

  @Get('wallets/:walletId/ledger')
  ledger(@Param('walletId') id: string, @Query('cursor') cursor?: string, @Query('limit') limit?: string) {
    return this.queries.ledger(parse(uuidParam, id), cursor, limit);
  }

  @Post('wallets/:walletId/reconciliation')
  @HttpCode(200)
  reconcile(@Param('walletId') id: string) {
    return this.queries.reconcile(parse(uuidParam, id));
  }

  @Post('wagering/transactions')
  async submit(
    @Body() body: unknown,
    @Res({ passthrough: true }) res: Response,
    @Headers('idempotency-key') key?: string,
    @Headers('x-correlation-id') cid?: string,
  ) {
    if (!key || key.length > 300) throw new ValidationError('Idempotency-Key header is required');
    const b = parse(wagerBody, body);
    const result = await this.wager.execute(
      { ...b, kind: b.kind as WagerTransactionKind, idempotencyKey: key },
      { correlationId: cid ?? crypto.randomUUID() },
    );
    res.status(result.status === 'PROCESSED' ? 200 : result.status === 'PENDING_REFERENCE' ? 202 : 422);
    return result;
  }

  @Get('wagering/transactions/:transactionId')
  tx(@Param('transactionId') id: string) {
    return this.queries.transactionById(parse(uuidParam, id));
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  txByExternal(@Param('providerId') p: string, @Param('externalTransactionId') e: string) {
    return this.queries.transactionByExternal(p, e);
  }
}                                                                                               