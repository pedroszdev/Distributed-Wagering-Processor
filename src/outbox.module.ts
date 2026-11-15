import { Module } from "@nestjs/common";
import { DatabaseModule } from "./infrastructure/database/database.module.ts";
import { OutboxPublisherWorker } from "./infrastructure/messaging/outbox-publisher.worker.ts";
import { PublishOutboxBatchUseCase } from "./application/outbox/publish-outbox-batch.use-case.ts";
import { SqsEventPublisher } from "./infrastructure/messaging/sqs-event-publisher.ts";
import { EventPublisher } from "./application/outbox/event-publisher.ts";

@Module({
  imports: [DatabaseModule],
  providers: [
    PublishOutboxBatchUseCase,
    OutboxPublisherWorker,
    { provide: EventPublisher, useClass: SqsEventPublisher }
  ],
  exports: [PublishOutboxBatchUseCase], // os testes vão chamar a volta na mão
})
export class OutboxModule {}
