import { GetQueueUrlCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { Injectable } from "@nestjs/common";
import { EventPublisher } from "../../application/outbox/event-publisher.ts";
import type { OutboxMessage } from "../../domain/outbox/outbox-message.ts";
import { env } from "../../config/env.ts";

@Injectable()
export class SqsEventPublisher extends EventPublisher {
  private readonly client = new SQSClient({
    region: env.aws.region,
    endpoint: env.aws.sqsEndpoint,
    // Quem faz retry é a OUTBOX (com backoff e registro no banco), não o SDK.
    maxAttempts: 1,
    // Sem timeout, um SQS travado seguraria o lock das linhas para sempre.
    requestHandler: { connectionTimeout: 2_000, requestTimeout: 5_000 },
  });

  private queueUrl?: Promise<string>;

  override async publish(message: OutboxMessage): Promise<void> {
    const queueUrl = await this.getQueueUrl()
    const messageBody = JSON.stringify(message.payload)
    const messageGroupId = message.aggregateId
    const messageDeduplicationId = message.id

    await this.client.send(new SendMessageCommand({ 
        QueueUrl: queueUrl, MessageBody: messageBody, 
        MessageGroupId: messageGroupId, MessageDeduplicationId: messageDeduplicationId 
    }))
  }

  /** Descobre a URL da fila uma vez só. Se falhar, esquece e tenta de novo na próxima. */
  private getQueueUrl(): Promise<string> {
    this.queueUrl ??= this.client
      .send(new GetQueueUrlCommand({ QueueName: env.aws.eventsQueueName }))
      .then((response) => response.QueueUrl!)
      .catch((error: unknown) => {
        this.queueUrl = undefined; // não guardar a falha para sempre
        throw error;
      });
    return this.queueUrl;
  }
}
