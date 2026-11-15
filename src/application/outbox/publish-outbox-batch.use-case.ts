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

  async execute(batchSize: number): Promise<OutboxBatchResult> {
    const publishedMessages: OutboxMessage[] = [];
    const result = await this.em.fork().transactional(async (tx) => {
      const messages = await this.outbox.claimDue(tx, new Date(), batchSize)
      let published = 0, failed = 0
      for (const message of messages) {
        try {
            await this.publisher.publish(message)
            message.markPublished(new Date())
            publishedMessages.push(message)
            published++
        }catch(error) {
            message.scheduleRetry(new Date(), `${error}`)
            failed++
        }
        await this.outbox.update(tx, message)
      }
      return { claimed: messages.length, published, failed }
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
