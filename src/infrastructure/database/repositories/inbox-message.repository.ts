import type { EntityManager } from "@mikro-orm/postgresql";
import { Injectable } from "@nestjs/common";
import type { InboxMessage } from "../../../domain/inbox/inbox-message.ts";
import { InboxMessageEntity } from "../entities/inbox-message.entity.ts";
import { inboxMessageToDomain, inboxMessageToRecord } from "../mappers/inbox-message.mapper.ts";

/**
 * Inbox: "esta mensagem já foi processada por este consumidor?".
 * A chave (consumer_name, message_id) é a PK da tabela, então a deduplicação é
 * garantida pelo BANCO, não pela memória.
 */
@Injectable()
export class InboxMessageRepository {
  /**
   * Grava a entrada. Devolve true na 1ª vez e false se ela já existia (duplicata).
   *
   * ON CONFLICT DO NOTHING em vez de "inserir e pegar o erro de unique": no
   * Postgres, um erro dentro da transação a deixa quebrada (todo comando seguinte
   * falha). Assim, a duplicata não é erro: só não insere e devolve 0 linhas.
   *
   * Duas entregas ao mesmo tempo: a 2ª ESPERA a 1ª terminar. Se a 1ª fizer commit,
   * a 2ª recebe 0 linhas (duplicata); se a 1ª fizer rollback, a 2ª insere e segue.
   */
  async tryInsert(em: EntityManager, message: InboxMessage): Promise<boolean> {
    const record = inboxMessageToRecord(message);
    const rows = await em.execute<{ message_id: string }[]>(
      `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       values (?, ?, ?, ?, ?)
       on conflict (consumer_name, message_id) do nothing
       returning message_id`,
      [record.consumerName, record.messageId, record.payloadHash, record.receivedAt, record.processedAt],
    );
    return rows.length === 1;
  }

  async findById(em: EntityManager, consumerName: string, messageId: string): Promise<InboxMessage | null> {
    const record = await em.findOne(InboxMessageEntity, { consumerName, messageId });
    return record ? inboxMessageToDomain(record) : null;
  }
}
