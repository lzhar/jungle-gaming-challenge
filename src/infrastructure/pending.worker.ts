import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { config } from '../config';
import { ProcessWagerService } from '../application/process-wager.service';

@Injectable()
export class PendingReferenceWorker implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setInterval>;
  private busy?: Promise<void>;

  constructor(@Inject(ProcessWagerService) private readonly wager: ProcessWagerService) {}

  onModuleInit() {
    if (!config.workersEnabled) return;
    this.timer = setInterval(() => {
      this.busy ??= this.wager.reprocessDue().then(() => undefined).finally(() => { this.busy = undefined; });
    }, config.pendingIntervalMs);
  }

  async onModuleDestroy() {
    clearInterval(this.timer);
    await this.busy;
  }
}