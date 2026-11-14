import { defineEntity, p, type InferEntity } from "@mikro-orm/postgresql";
import type { IntegrationEventJSON } from "../../../domain/shared/integration-events.ts";

export const OutboxMessageEntity = defineEntity({
  name: "OutboxMessageEntity",
  tableName: "outbox_messages",
  properties: {
    id: p.uuid().primary(),
    aggregateId: p.uuid(),
    eventType: p.text(),
    payload: p.json<IntegrationEventJSON<unknown>>().columnType("jsonb"),
    occurredAt: p.datetime(),
    nextAttemptAt: p.datetime(),
    attempts: p.integer(),
    publishedAt: p.datetime().nullable(),
    lastError: 	p.text().nullable(),
  },
});

export type OutboxMessageRecord = InferEntity<typeof OutboxMessageEntity>;
