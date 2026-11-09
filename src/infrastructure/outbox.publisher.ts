import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { config } from '../config';
import { OutboxMessage } from '../domain/messaging';
import { Db } from './db';
import { logger } from './logger';
import { metrics } from './metrics';
import { Queues, SQS } from '../application/sqs';

@Injectable()
export class OutboxPublisher implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setInterval>;
  private busy?: Promise<void>;

  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    @Inject(SQS) private readonly sqs: SQSClient,
    @Inject(Queues) private readonly queues: Queues,
  ) {}

  onModuleInit() {
    if (!config.workersEnabled) return;
    this.timer = setInterval(() => {
      this.busy ??= this.tick()
        .catch((e) => logger.error({ message: String(e?.message) }, 'outbox tick failed'))
        .finally(() => { this.busy = undefined; });
    }, config.outboxIntervalMs);
  }

  async onModuleDestroy() {
    clearInterval(this.timer);
    await this.busy;
  }

  async tick(): Promise<void> {
    const url = await this.queues.url('wager-events.fifo');
    await this.orm.em.fork().transactional(async (em) => {
      const db = new Db(em);
      // SKIP LOCKED: dois publishers nunca pegam a mesma linha ao mesmo tempo
      const rows = await db.q(
        'SELECT * FROM outbox_messages WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY occurred_at LIMIT 20 FOR UPDATE SKIP LOCKED',
      );
      for (const r of rows) {
        const msg = OutboxMessage.rehydrate({
          id: r.id, aggregateId: r.aggregate_id, eventType: r.event_type, payload: r.payload,
          occurredAt: r.occurred_at, attempts: r.attempts, nextAttemptAt: r.next_attempt_at ?? undefined,
        });
        try {
          await this.sqs.send(new SendMessageCommand({
            QueueUrl: url, MessageBody: JSON.stringify(msg.payload),
            MessageGroupId: msg.aggregateId, MessageDeduplicationId: msg.id,
          }));
          msg.markPublished(new Date());
        } catch {
          msg.scheduleRetry(new Date()); // backoff exponencial
          metrics.retries.inc({ source: 'outbox' });
        }
        await db.q('UPDATE outbox_messages SET published_at = ?, attempts = ?, next_attempt_at = ? WHERE id = ?',
          [msg.publishedAt ?? null, msg.attempts, msg.nextAttemptAt ?? null, msg.id]);
      }
    });

    const [lag] = await new Db(this.orm.em.fork()).q(
      'SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(occurred_at))), 0) AS lag FROM outbox_messages WHERE published_at IS NULL',
    );
    metrics.outboxLag.set(Number(lag.lag));
  }
}