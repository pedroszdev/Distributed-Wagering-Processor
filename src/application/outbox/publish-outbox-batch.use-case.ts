import { EntityManager } from "@mikro-orm/postgresql";
import { Injectable } from "@nestjs/common";
import { OutboxMessageRepository } from "../../infrastructure/database/repositories/outbox-message.repository.ts";
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
  ) {}

  async execute(batchSize: number): Promise<OutboxBatchResult> {
    return this.em.fork().transactional(async (tx) => {
      const messages = await this.outbox.claimDue(tx, new Date(), batchSize)
      let published = 0, failed = 0
      for (const message of messages) {
        try {
            await this.publisher.publish(message)
            message.markPublished(new Date())
            published++
        }catch(error) {
            message.scheduleRetry(new Date(), `${error}`)
            failed++
        }
        await this.outbox.update(tx, message)
      }
      return { claimed: messages.length, published, failed }
    });
  }
}
