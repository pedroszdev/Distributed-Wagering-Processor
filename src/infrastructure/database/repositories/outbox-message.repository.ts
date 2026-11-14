import type { EntityManager } from "@mikro-orm/postgresql";
import { Injectable } from "@nestjs/common";
import type { OutboxMessage } from "../../../domain/outbox/outbox-message.ts";
import { OutboxMessageEntity } from "../entities/outbox-message.entity.ts";
import { outboxMessageToRecord } from "../mappers/outbox-message.mapper.ts";

@Injectable()
export class OutboxMessageRepository {
  async insert(em: EntityManager, message: OutboxMessage): Promise<void> {
    await em.insert(OutboxMessageEntity, outboxMessageToRecord(message))
  }
}
