import { InboxMessage } from "../../../domain/inbox/inbox-message.ts";
import type { InboxMessageRecord } from "../entities/inbox-message.entity.ts";

export function inboxMessageToRecord(message: InboxMessage): InboxMessageRecord {
  return {
    consumerName: message.consumerName,
    messageId: message.messageId,
    payloadHash: message.payloadHash,
    receivedAt: message.receivedAt,
    // O domínio usa undefined; a coluna nullable usa null.
    processedAt: message.processedAt ?? null,
  };
}

export function inboxMessageToDomain(record: InboxMessageRecord): InboxMessage {
  return InboxMessage.rehydrate({
    consumerName: record.consumerName,
    messageId: record.messageId,
    payloadHash: record.payloadHash,
    receivedAt: record.receivedAt,
    processedAt: record.processedAt ?? undefined,
  });
}
