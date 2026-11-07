import type { IntegrationEvent } from '../events';

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}
  static receive(p: { messageId: string; consumerName: string; payloadHash: string; now: Date }) {
    return new InboxMessage(p.messageId, p.consumerName, p.payloadHash, p.now);
  }
  static rehydrate(s: { messageId: string; consumerName: string; payloadHash: string; receivedAt: Date; processedAt?: Date }) {
    return new InboxMessage(s.messageId, s.consumerName, s.payloadHash, s.receivedAt, s.processedAt);
  }
  get processedAt(): Date | undefined { return this._processedAt; }
  isProcessed(): boolean { return !!this._processedAt; }
  markProcessed(at: Date): void { this._processedAt = at; }
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}
  static enqueue(e: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(e.eventId, e.aggregateId, e.eventType, e.toJSON() as unknown as Record<string, unknown>, e.occurredAt, 0);
  }
  static rehydrate(s: { id: string; aggregateId: string; eventType: string; payload: Record<string, unknown>; occurredAt: Date; attempts: number; nextAttemptAt?: Date; publishedAt?: Date }) {
    return new OutboxMessage(s.id, s.aggregateId, s.eventType, s.payload, s.occurredAt, s.attempts, s.nextAttemptAt, s.publishedAt);
  }
  get attempts(): number { return this._attempts; }
  get nextAttemptAt(): Date | undefined { return this._nextAttemptAt; }
  get publishedAt(): Date | undefined { return this._publishedAt; }
  isPending(): boolean { return !this._publishedAt; }
  isDue(now: Date): boolean { return this.isPending() && (!this._nextAttemptAt || this._nextAttemptAt <= now); }
  markPublished(at: Date): void { this._publishedAt = at; }
  /** incrementa attempts e calcula o próximo nextAttemptAt com backoff exponencial (teto de 60s) */
  scheduleRetry(now: Date): void {
    this._attempts += 1;
    this._nextAttemptAt = new Date(now.getTime() + Math.min(1000 * 2 ** this._attempts, 60_000));
  }
}