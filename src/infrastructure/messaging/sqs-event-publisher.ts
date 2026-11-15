import { GetQueueUrlCommand, SendMessageBatchCommand, SQSClient } from "@aws-sdk/client-sqs";
import { Injectable } from "@nestjs/common";
import { EventPublisher, type PublishBatchResult } from "../../application/outbox/event-publisher.ts";
import type { OutboxMessage } from "../../domain/outbox/outbox-message.ts";
import { env } from "../../config/env.ts";

/** Limites do SendMessageBatch: 10 mensagens e 256 KiB por chamada (margem de segurança). */
const MAX_ENTRIES_PER_CALL = 10;
const MAX_BYTES_PER_CALL = 250 * 1024;

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

  /**
   * Publica em blocos de até 10 (SendMessageBatch): 10× menos idas ao SQS.
   *
   * Os blocos vão EM SÉRIE e na ordem recebida: dentro de um bloco, o SQS FIFO
   * mantém a ordem das entradas do mesmo grupo; em paralelo, dois blocos com
   * eventos do mesmo agregado poderiam se inverter.
   *
   * Cada entrada pode falhar sozinha (o SQS devolve `Failed[]`): só ela volta
   * para retry. Se a chamada inteira falhar, o bloco todo é marcado como falho.
   */
  override async publishBatch(messages: OutboxMessage[]): Promise<PublishBatchResult> {
    const failed = new Map<string, string>();
    if (messages.length === 0) return { failed };
    const queueUrl = await this.getQueueUrl(); // se falhar, lança: o lote todo falhou

    for (const chunk of chunks(messages)) {
      try {
        const response = await this.client.send(
          new SendMessageBatchCommand({
            QueueUrl: queueUrl,
            Entries: chunk.map((message) => ({
              Id: message.id, // único no lote; uuid só tem letras, números e "-"
              MessageBody: JSON.stringify(message.payload),
              MessageGroupId: message.aggregateId, // o FIFO ordena por agregado
              MessageDeduplicationId: message.id, // o SQS descarta repetição em 5 min
            })),
          }),
        );
        for (const failure of response.Failed ?? []) {
          failed.set(failure.Id!, `${failure.Code}: ${failure.Message ?? "falha no SQS"}`);
        }
      } catch (error) {
        for (const message of chunk) failed.set(message.id, String(error));
      }
    }
    return { failed };
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

/** Separa em blocos que respeitam os limites do SendMessageBatch, mantendo a ordem. */
function chunks(messages: OutboxMessage[]): OutboxMessage[][] {
  const result: OutboxMessage[][] = [];
  let current: OutboxMessage[] = [];
  let bytes = 0;
  for (const message of messages) {
    const size = Buffer.byteLength(JSON.stringify(message.payload));
    if (current.length === MAX_ENTRIES_PER_CALL || (current.length > 0 && bytes + size > MAX_BYTES_PER_CALL)) {
      result.push(current);
      current = [];
      bytes = 0;
    }
    current.push(message);
    bytes += size;
  }
  if (current.length > 0) result.push(current);
  return result;
}
