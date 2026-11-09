import { GetQueueUrlCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { config } from '../src/config';

const sqs = new SQSClient({
  region: config.sqs.region, endpoint: config.sqs.endpoint,
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: 'wager-transactions.fifo' }));

// bun scripts/send-message.ts <walletId> <playerId> <externalId> <kind> <amount> <messageId>
// bun scripts/send-message.ts raw     (mensagem que nao e JSON)
const [walletId, playerId, ext, kind, amount, messageId] = process.argv.slice(2);
const raw = walletId === 'raw';
const body = raw ? 'isto nao e json' : JSON.stringify({
  messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
  data: {
    providerId: 'provider-a', externalTransactionId: ext, idempotencyKey: 'provider-a:' + ext,
    playerId, walletId, roundId: 'r1', gameId: 'g1', kind, money: { amount, currency: 'BRL' },
  },
});
await sqs.send(new SendMessageCommand({
  QueueUrl, MessageBody: body, MessageGroupId: raw ? 'raw' : walletId,
  MessageDeduplicationId: crypto.randomUUID(),
}));
console.log('sent');
