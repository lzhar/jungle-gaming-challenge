import { Inject, Injectable } from '@nestjs/common';
import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';
import { config } from '../config';

export const SQS = Symbol('SQS');
export const sqsProvider = {
  provide: SQS,
  useFactory: () =>
    new SQSClient({
      region: config.sqs.region,
      endpoint: config.sqs.endpoint,
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    }),
};

@Injectable()
export class Queues {
  private readonly cache = new Map<string, string>();
  constructor(@Inject(SQS) private readonly sqs: SQSClient) {}

  async url(name: string): Promise<string> {
    const hit = this.cache.get(name);
    if (hit) return hit;
    const out = await this.sqs.send(new GetQueueUrlCommand({ QueueName: name }));
    this.cache.set(name, out.QueueUrl!);
    return out.QueueUrl!;
  }

  /** Usado no readiness: sem cache, para detectar SQS fora do ar. */
  async ping(): Promise<void> {
    await this.sqs.send(new GetQueueUrlCommand({ QueueName: 'wager-transactions.fifo' }));
  }
}