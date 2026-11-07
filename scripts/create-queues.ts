import { SQSClient, CreateQueueCommand, GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import { config } from '../src/config';

const sqs = new SQSClient({
  region: config.sqs.region,
  endpoint: config.sqs.endpoint,
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});

const fifo = { FifoQueue: 'true', ContentBasedDeduplication: 'false' };

const dlq = await sqs.send(new CreateQueueCommand({ QueueName: 'wager-transactions-dlq.fifo', Attributes: fifo }));
const attrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl!, AttributeNames: ['QueueArn'] }));

await sqs.send(new CreateQueueCommand({
  QueueName: 'wager-transactions.fifo',
  Attributes: {
    ...fifo,
    VisibilityTimeout: '30',
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: attrs.Attributes!.QueueArn, maxReceiveCount: '5' }),
  },
}));
// fila de saída dos eventos de integração (publicada pelo outbox)
await sqs.send(new CreateQueueCommand({ QueueName: 'wager-events.fifo', Attributes: fifo }));
console.log('queues ready');