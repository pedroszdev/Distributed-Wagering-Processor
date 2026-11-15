import { EntityManager } from "@mikro-orm/postgresql";
import { Injectable } from "@nestjs/common";
import type { OutboxMessage } from "../../domain/outbox/outbox-message.ts";
import { OutboxMessageRepository } from "../../infrastructure/database/repositories/outbox-message.repository.ts";
import { Metrics } from "../../infrastructure/observability/metrics.ts";
import { EventPublisher } from "./event-publisher.ts";

export interface OutboxBatchResult {
  claimed: number;   // quantos pegou
  published: number; // quantos foram para o SQS
  failed: number;    // quantos vão tentar de novo depois
}

@Injectable()
export class PublishOutboxBatchUseCase {
  constructor(
    private readonly em: EntityManager,
    private readonly outbox: OutboxMessageRepository,
    private readonly publisher: EventPublisher,
    private readonly metrics: Metrics,
  ) {}

  /**
   * Uma volta: trava um lote (SKIP LOCKED), publica EM LOTE no SQS e grava o
   * resultado, tudo na mesma transação. Se o processo morrer depois do SQS e
   * antes do commit, o lote volta a ficar pendente e sai de novo com o MESMO
   * eventId (duplicata segura).
   */
  async execute(batchSize: number): Promise<OutboxBatchResult> {
    const publishedMessages: OutboxMessage[] = [];
    const result = await this.em.fork().transactional(async (tx) => {
      const messages = await this.outbox.claimDue(tx, new Date(), batchSize)
      if (messages.length === 0) return { claimed: 0, published: 0, failed: 0 }

      // Falha por evento; se a chamada inteira falhar (SQS fora), todos falharam.
      let failures: Map<string, string>
      try {
        failures = (await this.publisher.publishBatch(messages)).failed
      } catch (error) {
        failures = new Map(messages.map((message) => [message.id, `${error}`]))
      }

      const now = new Date()
      const failedMessages: OutboxMessage[] = []
      for (const message of messages) {
        const failure = failures.get(message.id)
        if (failure === undefined) {
          message.markPublished(now)
          publishedMessages.push(message)
        } else {
          message.scheduleRetry(now, failure)
          failedMessages.push(message)
        }
      }

      // Os publicados num UPDATE só (um por evento viraria o próximo gargalo);
      // os que falharam (raros) um a um, porque cada um tem o seu backoff.
      await this.outbox.markPublished(tx, publishedMessages.map((message) => message.id), now)
      for (const message of failedMessages) {
        await this.outbox.update(tx, message)
      }
      return { claimed: messages.length, published: publishedMessages.length, failed: failedMessages.length }
    });

    // Métricas só DEPOIS do commit: se a transação caísse, esses eventos sairiam
    // de novo na próxima volta, e contar agora seria contar duas vezes.
    for (const message of publishedMessages) {
      this.metrics.outboxPublished.inc({ event_type: message.eventType });
      this.metrics.outboxPublishLagSeconds.observe(
        (message.publishedAt!.getTime() - message.occurredAt.getTime()) / 1_000,
      );
    }
    if (result.failed > 0) this.metrics.outboxPublishFailures.inc({}, result.failed);
    return result;
  }
}
