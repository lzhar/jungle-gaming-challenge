import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import {
  ChangeMessageVisibilityCommand, DeleteMessageCommand, ReceiveMessageCommand,
  SendMessageCommand, SQSClient, type Message,
} from '@aws-sdk/client-sqs';
import { config } from '../config';
import { ProcessWagerService } from '../application/process-wager.service';
import { parse, wagerMessage } from '../application/schemas';
import { DomainError } from '../domain/errors';
import type { WagerTransactionKind } from '../wager-transaction';
import { logger } from './logger';
import { metrics } from './metrics';
import { Queues, SQS } from '../application/sqs';

const CONSUMER = 'wager-consumer';
const MAIN = 'wager-transactions.fifo';
const DLQ = 'wager-transactions-dlq.fifo';

@Injectable()
export class WagerConsumer implements OnModuleInit, OnModuleDestroy {
  private running = false;
  private loop?: Promise<void>;

  constructor(
    @Inject(SQS) private readonly sqs: SQSClient,
    @Inject(Queues) private readonly queues: Queues,
    @Inject(ProcessWagerService) private readonly wager: ProcessWagerService,
  ) {}

  onModuleInit() {
    if (!config.consumerEnabled) return;
    this.running = true;
    this.loop = this.poll();
  }

  /** SIGTERM: para de buscar, termina a mensagem atual e devolve a visibilidade das demais. */
  async onModuleDestroy() {
    this.running = false;
    await this.loop;
  }

  private async poll() {
    while (this.running) {
      try {
        const url = await this.queues.url(MAIN);
        const out = await this.sqs.send(new ReceiveMessageCommand({
          QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 2,
          MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
        }));
        // Sequencial: respeita a ordem do grupo FIFO e deixa o shutdown previsível.
        for (const m of out.Messages ?? []) {
          if (!this.running) { await this.setVisibility(url, m, 0); continue; }
          await this.handle(url, m);
        }
      } catch (e) {
        logger.error({ message: String((e as Error)?.message) }, 'consumer poll failed');
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }

  private async handle(url: string, m: Message) {
    const receiveCount = Number(m.Attributes?.ApproximateReceiveCount ?? 1);

    // Mensagem malformada: erro PERMANENTE -> DLQ
    let env;
    try {
      env = parse(wagerMessage, JSON.parse(m.Body ?? ''));
    } catch {
      return this.toDlq(url, m, 'INVALID_MESSAGE');
    }

    try {
      // MESMO caso de uso da API HTTP
      await this.wager.execute(
        { ...env.data, kind: env.data.kind as WagerTransactionKind },
        { correlationId: env.messageId, causationId: env.messageId, inbox: { consumerName: CONSUMER, messageId: env.messageId } },
      );
      // Hook APENAS para testes: simula morte DEPOIS do commit e ANTES do ack
      if (process.env.CRASH_BEFORE_ACK === '1') process.kill(process.pid, 'SIGKILL');
      await this.ack(url, m); // ack somente depois do commit
    } catch (e) {
      if (e instanceof DomainError) return this.toDlq(url, m, e.code); // permanente
      // Transitório: não dá ack; volta com backoff. Após 5 recebimentos o SQS manda para a DLQ (redrive).
      metrics.retries.inc({ source: 'sqs' });
      await this.setVisibility(url, m, Math.min(2 ** receiveCount, 60));
    }
    // Nota: rejeição de NEGÓCIO não lança erro: a transação REJECTED é gravada e a mensagem recebe ack.
  }

  private ack(url: string, m: Message) {
    return this.sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: m.ReceiptHandle! }));
  }

  private setVisibility(url: string, m: Message, seconds: number) {
    return this.sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: url, ReceiptHandle: m.ReceiptHandle!, VisibilityTimeout: seconds }));
  }

  private async toDlq(url: string, m: Message, reason: string) {
    const dlq = await this.queues.url(DLQ);
    await this.sqs.send(new SendMessageCommand({
      QueueUrl: dlq, MessageBody: m.Body ?? '',
      MessageGroupId: m.Attributes?.MessageGroupId ?? 'unknown', MessageDeduplicationId: m.MessageId,
    }));
    metrics.dlq.inc();
    logger.warn({ messageId: m.MessageId, reason }, 'message sent to DLQ');
    await this.ack(url, m);
  }
}