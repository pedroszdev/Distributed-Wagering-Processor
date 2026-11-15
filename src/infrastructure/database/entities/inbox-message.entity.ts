import { defineEntity, p, type InferEntity } from "@mikro-orm/postgresql";

/** Formato de uma linha da tabela `inbox_messages` (PK composta). */
export const InboxMessageEntity = defineEntity({
  name: "InboxMessageEntity",
  tableName: "inbox_messages",
  properties: {
    consumerName: p.text().primary(),
    messageId: p.text().primary(),
    payloadHash: p.text(),
    receivedAt: p.datetime(),
    processedAt: p.datetime().nullable(),
  },
});

export type InboxMessageRecord = InferEntity<typeof InboxMessageEntity>;
