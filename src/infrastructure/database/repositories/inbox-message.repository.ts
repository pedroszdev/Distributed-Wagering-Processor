import type { EntityManager } from "@mikro-orm/postgresql";
import { Injectable } from "@nestjs/common";

export interface InboxEntry {
  consumerName: string;
  messageId: string;
  payloadHash: string;
}

/**
 * Inbox: "esta mensagem já foi processada por este consumidor?".
 * A chave (consumer_name, message_id) é a PK da tabela, então a deduplicação é
 * garantida pelo BANCO, não pela memória.
 */
@Injectable()
export class InboxMessageRepository {
  async tryInsert(em: EntityManager, entry: InboxEntry): Promise<boolean> {
    const now = new Date();
    const rows = await em.execute<{ message_id: string }[]>(
      `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       values (?, ?, ?, ?, ?)
       on conflict (consumer_name, message_id) do nothing
       returning message_id`,
      [entry.consumerName, entry.messageId, entry.payloadHash, now, now],
    );
    return rows.length === 1;
  }
}
