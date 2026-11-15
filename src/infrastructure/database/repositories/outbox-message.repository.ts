import { LockMode, QueryOrder, type EntityManager } from "@mikro-orm/postgresql";
import { Injectable } from "@nestjs/common";
import type { OutboxMessage } from "../../../domain/outbox/outbox-message.ts";
import { OutboxMessageEntity } from "../entities/outbox-message.entity.ts";
import { outboxMessageToDomain, outboxMessageToRecord } from "../mappers/outbox-message.mapper.ts";

@Injectable()
export class OutboxMessageRepository {
  async insert(em: EntityManager, message: OutboxMessage): Promise<void> {
    await em.insert(OutboxMessageEntity, outboxMessageToRecord(message))
  }

  /**
 * Pega até `limit` mensagens pendentes cujo horário já chegou, TRAVANDO as linhas.
 * SKIP LOCKED: linhas já travadas por outro publisher são puladas, não esperadas.
 * Só funciona dentro de uma transação (o lock dura até o commit/rollback).
 */
  async claimDue(em: EntityManager, now: Date, limit: number): Promise<OutboxMessage[]> {
    const records = await em.find(OutboxMessageEntity, 
      { publishedAt: null, nextAttemptAt: { $lte: now }},
      { orderBy: { nextAttemptAt: QueryOrder.ASC }, limit: limit, lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE });
    return records.map((record) => outboxMessageToDomain(record))
  }

  /** Grava o resultado da tentativa: publicada, ou attempts/nextAttemptAt/lastError novos. */
  async update(em: EntityManager, message: OutboxMessage): Promise<void> {
    await em.nativeUpdate(OutboxMessageEntity, 
      { id: message.id }, 
      { attempts: message.attempts, 
        nextAttemptAt: message.nextAttemptAt, 
        publishedAt: message.publishedAt ?? null, 
        lastError: message.lastError ?? null
      })
  }

  /**
   * O evento WagerTransactionPendingReference de uma transação: quando o worker
   * resolve a pendência, os eventos novos herdam o correlationId do pedido
   * ORIGINAL e apontam para esse evento como causa (rastreabilidade de ponta a ponta).
   */
  async findPendingReferenceEvent(
    em: EntityManager,
    transactionId: string,
  ): Promise<{ eventId: string; correlationId: string } | null> {
    const [row] = await em.execute<{ event_id: string; correlation_id: string }[]>(
      `select payload ->> 'eventId' as event_id, payload ->> 'correlationId' as correlation_id
         from outbox_messages
        where aggregate_id = ? and event_type = 'WagerTransactionPendingReference'
        order by occurred_at desc
        limit 1`,
      [transactionId],
    );
    return row ? { eventId: row.event_id, correlationId: row.correlation_id } : null;
  }
}
