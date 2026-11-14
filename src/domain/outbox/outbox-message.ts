import type { IntegrationEvent, IntegrationEventJSON } from "../shared/integration-events.ts";
import { OutboxMessageAlreadyPublishedError } from "./outbox-message.errors.ts";

/** Backoff: 1s, 2s, 4s, 8s... até 5 minutos. */
const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 5 * 60_000;

/** Tudo que fica salvo numa linha da outbox (usado no rehydrate). */
export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: IntegrationEventJSON<unknown>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt: Date;
  publishedAt?: Date;
  lastError?: string;
}

export class OutboxMessage {
  private constructor(
    readonly id: string,
    readonly aggregateId: string,
    readonly eventType: string,
    readonly payload: IntegrationEventJSON<unknown>,
    readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt: Date,
    private _publishedAt?: Date,
    private _lastError?: string,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(
        event.eventId,
        event.aggregateId,
        event.eventType,
        event.toJSON(),
        event.occurredAt,
        0,
        event.occurredAt,
    )
  }

    static rehydrate(state: OutboxMessageState): OutboxMessage {
        const {id,aggregateId, eventType, payload, occurredAt, attempts, nextAttemptAt, publishedAt, lastError } = state;
        return new OutboxMessage(id,aggregateId, eventType, payload, occurredAt, attempts, nextAttemptAt, publishedAt, lastError)  
    }

  get attempts(): number { return this._attempts; }
  get nextAttemptAt(): Date { return this._nextAttemptAt; }
  get publishedAt(): Date | undefined { return this._publishedAt; }
  get lastError(): string | undefined { return this._lastError; }

  /** Ainda não foi publicada? */
  isPending(): boolean {
    if (!this._publishedAt) {
        return true
    }
    return false
  }

  /** Está pendente E já chegou a hora de tentar? */
  isDue(now: Date): boolean {
    const timeNextAttemptAt = this._nextAttemptAt.getTime()
    if (this.isPending() && timeNextAttemptAt <= now.getTime()) {
        return true
    }
    return false
  }

  /** O SQS confirmou: marca como publicada. */
  markPublished(at: Date): void {
    if (!this.isPending()) {
        throw new OutboxMessageAlreadyPublishedError(this.id)
    }
    this._publishedAt = at
  }

  /** O SQS falhou: conta a tentativa e agenda a próxima com backoff. */
  scheduleRetry(now: Date, error?: string): void {
    if (!this.isPending()) {
        throw new OutboxMessageAlreadyPublishedError(this.id)
    }
    this._attempts += 1
    const espera = Math.min(BASE_DELAY_MS * 2 ** (this._attempts - 1), MAX_DELAY_MS)
    this._nextAttemptAt = new Date(now.getTime() + espera)
    if (error) {
        this._lastError = error
    }
  }
}
