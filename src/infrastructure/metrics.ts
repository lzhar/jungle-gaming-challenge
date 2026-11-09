import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

const counter = (name: string, help: string, labelNames: string[] = []) =>
  new client.Counter({ name, help, labelNames, registers: [registry] });

export const metrics = {
  transactions: counter('wager_transactions_total', 'Transactions by final status', ['status']),
  duplicates: counter('wager_duplicates_total', 'Duplicate requests/messages detected', ['source']),
  retries: counter('wager_retries_total', 'Retries', ['source']),
  dlq: counter('wager_dlq_messages_total', 'Messages sent to DLQ'),
  lockConflicts: counter('wager_lock_conflicts_total', 'Concurrency conflicts (deadlock, version)'),
  divergences: counter('wager_reconciliation_divergences_total', 'Ledger vs balance divergences'),
  outboxLag: new client.Gauge({ name: 'wager_outbox_lag_seconds', help: 'Age of oldest unpublished event', registers: [registry] }),
  latency: new client.Histogram({
    name: 'wager_processing_seconds', help: 'Wager processing latency',
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5], registers: [registry],
  }),
};