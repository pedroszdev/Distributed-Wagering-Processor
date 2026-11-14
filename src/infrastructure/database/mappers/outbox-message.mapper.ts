import { OutboxMessage } from "../../../domain/outbox/outbox-message.ts";
import type { OutboxMessageRecord } from "../entities/outbox-message.entity.ts";

export function outboxMessageToRecord(entry: OutboxMessage): OutboxMessageRecord {
  return {
    id: entry.id,
    aggregateId: entry.aggregateId,
    eventType: entry.eventType,
    payload: entry.payload,
    occurredAt: entry.occurredAt,
    nextAttemptAt: entry.nextAttemptAt,
    attempts: entry.attempts,
    publishedAt: entry.publishedAt ?? null,
    lastError: entry.lastError ?? null,
  };
}

export function outboxMessageToDomain(record: OutboxMessageRecord): OutboxMessage {

  return OutboxMessage.rehydrate({
    id: record.id,
    aggregateId: record.aggregateId,
    eventType: record.eventType,
    payload: record.payload,
    occurredAt: record.occurredAt,
    nextAttemptAt: record.nextAttemptAt,
    attempts: record.attempts,
    publishedAt: record.publishedAt ?? undefined,
    lastError: record.lastError ?? undefined,
  });
}
