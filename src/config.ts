export const config = {
  port: Number(process.env.PORT ?? 3000),
  databaseUrl: process.env.DATABASE_URL ?? 'postgresql://wager:wager@localhost:5432/wager',
  sqs: {
    endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
    region: process.env.AWS_REGION ?? 'us-east-1',
  },
  consumerEnabled: process.env.CONSUMER_ENABLED !== '0',
  workersEnabled: process.env.WORKERS_ENABLED !== '0',
  pendingIntervalMs: Number(process.env.PENDING_INTERVAL_MS ?? 5000),
  outboxIntervalMs: Number(process.env.OUTBOX_INTERVAL_MS ?? 1000),
};