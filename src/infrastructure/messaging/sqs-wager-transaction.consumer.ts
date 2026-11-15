import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
  type Message,
} from "@aws-sdk/client-sqs";
import { Injectable, Logger, type BeforeApplicationShutdown, type OnApplicationBootstrap } from "@nestjs/common";
import { ProcessWagerTransactionUseCase } from "../../application/wagering/process-wager-transaction.use-case.ts";
import { env } from "../../config/env.ts";
import { decideOnError, type MessageDecision } from "./message-failure-policy.ts";
import { parseWagerTransactionMessage } from "./wager-transaction.message.ts";

/** O que aconteceu numa leitura da fila (os testes e, depois, as métricas usam). */
export interface PollResult {
  received: number;
  processed: number;
  duplicates: number;
  businessErrors: number;
  retried: number;
  deadLettered: number;
  /** Devolvidas para a fila sem processar (shutdown, ou grupo FIFO bloqueado). */
  released: number;
}

type Outcome = Exclude<keyof PollResult, "received">;

/**
 * Consumidor da fila de entrada (wager-transactions.fifo).
 *
 * Para cada mensagem: lê → chama o MESMO use case do HTTP (com a inbox na mesma
 * transação) → decide (message-failure-policy.ts):
 *   ack         → apaga da fila (processou, duplicata, ou regra de negócio disse não)
 *   retry       → NÃO apaga; só esconde por N segundos (backoff). Depois de
 *                 maxReceiveCount entregas, o próprio SQS move para a DLQ.
 *   dead-letter → mensagem malformada: manda para a DLQ já e apaga da fila.
 *
 * O ack (DeleteMessage) só acontece DEPOIS do commit. Se o processo morrer entre
 * o commit e o ack, o SQS entrega de novo e a inbox transforma em duplicata.
 */
@Injectable()
export class SqsWagerTransactionConsumer implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(SqsWagerTransactionConsumer.name);

  private readonly client = new SQSClient({
    region: env.aws.region,
    endpoint: env.aws.sqsEndpoint,
    // O long polling segura a requisição por até waitTimeSeconds: o timeout
    // precisa ser MAIOR que isso, senão toda leitura vazia viraria erro.
    requestHandler: { connectionTimeout: 2_000, requestTimeout: (env.sqsConsumer.waitTimeSeconds + 5) * 1_000 },
  });

  private readonly queueUrls = new Map<string, Promise<string>>();
  private running = false;
  private stopping = false;
  private abort = new AbortController();
  private loopDone?: Promise<void>;

  constructor(private readonly processWager: ProcessWagerTransactionUseCase) {}

  onApplicationBootstrap(): void {
    if (!env.sqsConsumer.enabled) return;
    this.running = true;
    this.stopping = false;
    this.abort = new AbortController();
    this.loopDone = this.loop();
  }

  /**
   * SIGTERM. Usa beforeApplicationShutdown (e não onApplicationShutdown) porque
   * roda ANTES de o MikroORM fechar as conexões: a mensagem em andamento ainda
   * consegue fazer commit. As que ainda não começaram voltam para a fila.
   */
  async beforeApplicationShutdown(): Promise<void> {
    this.running = false;
    this.stopping = true;
    this.abort.abort(); // interrompe o long polling na hora
    await this.loopDone;
    this.loopDone = undefined;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      try {
        await this.pollOnce(env.sqsConsumer.waitTimeSeconds);
      } catch (error) {
        if (!this.running) return; // foi o abort do shutdown
        // SQS fora do ar etc.: loga, espera um pouco e tenta de novo. O loop nunca morre.
        this.logger.error(`Falha ao ler a fila ${env.aws.queueName}: ${error}`);
        await Bun.sleep(1_000);
      }
    }
  }

  /**
   * Uma leitura da fila + o processamento de tudo que veio, uma mensagem por vez
   * (na ordem da fila FIFO). O loop chama isto; os testes chamam direto.
   */
  async pollOnce(waitTimeSeconds = 0): Promise<PollResult> {
    const queueUrl = await this.queueUrl(env.aws.queueName);
    const response = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: env.sqsConsumer.batchSize,
        WaitTimeSeconds: waitTimeSeconds,
        MessageSystemAttributeNames: ["ApproximateReceiveCount", "MessageGroupId"],
      }),
      { abortSignal: this.abort.signal },
    );
    const messages = response.Messages ?? [];

    const result: PollResult = {
      received: messages.length,
      processed: 0,
      duplicates: 0,
      businessErrors: 0,
      retried: 0,
      deadLettered: 0,
      released: 0,
    };
    // FIFO: se uma mensagem de um grupo vai para retry, as próximas do MESMO grupo
    // não podem passar na frente dela. Devolvemos essas para a fila (visibilidade 0);
    // o SQS FIFO só as entrega de novo depois da que falhou.
    const blockedGroups = new Set<string>();

    for (const message of messages) {
      const groupId = message.Attributes?.MessageGroupId;
      if (this.stopping || (groupId !== undefined && blockedGroups.has(groupId))) {
        await this.release(queueUrl, message);
        result.released++;
        continue;
      }
      const outcome = await this.handle(queueUrl, message);
      result[outcome]++;
      if (outcome === "retried" && groupId !== undefined) blockedGroups.add(groupId);
    }
    return result;
  }

  /** Processa UMA mensagem e aplica a decisão. Nunca lança: o lote continua. */
  private async handle(queueUrl: string, message: Message): Promise<Outcome> {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? "1");
    const startedAt = performance.now();
    const log: Record<string, unknown> = { sqsMessageId: message.MessageId, receiveCount };

    let decision: MessageDecision;
    try {
      const { messageId, input } = parseWagerTransactionMessage(message.Body ?? "");
      Object.assign(log, { messageId, providerId: input.providerId, walletId: input.walletId });

      const result = await this.processWager.execute(
        input,
        // A mensagem é a "causa" dos eventos gerados; sem correlationId no envelope,
        // a própria mensagem começa a correlação.
        { correlationId: messageId, causationId: messageId },
        { consumerName: env.sqsConsumer.name, messageId },
      );
      Object.assign(log, { transactionId: result.transactionId, status: result.status });
      decision = { action: "ack", reason: "processed" };
    } catch (error) {
      decision = decideOnError(error, receiveCount);
      Object.assign(log, { error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) });
    }
    log.durationMs = Math.round(performance.now() - startedAt);

    // Sem `money` nem o body no log: nada de payload financeiro completo (seção 12).
    return this.apply(queueUrl, message, decision, log);
  }

  private async apply(
    queueUrl: string,
    message: Message,
    decision: MessageDecision,
    log: Record<string, unknown>,
  ): Promise<Outcome> {
    try {
      switch (decision.action) {
        case "ack":
          await this.delete(queueUrl, message);
          if (decision.reason === "processed") {
            this.logger.log({ msg: "mensagem processada", ...log });
            return "processed";
          }
          if (decision.reason === "duplicate") {
            this.logger.log({ msg: "mensagem duplicada (inbox): ack sem efeito", ...log });
            return "duplicates";
          }
          this.logger.warn({ msg: "mensagem recusada por regra de negócio: ack", ...log });
          return "businessErrors";

        case "retry":
          // Não apaga: só esconde por N segundos. Quando voltar, o receiveCount sobe.
          await this.client.send(
            new ChangeMessageVisibilityCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: decision.delaySeconds,
            }),
          );
          this.logger.warn({ msg: "falha ao processar: nova tentativa agendada", delaySeconds: decision.delaySeconds, ...log });
          return "retried";

        case "dead-letter":
          await this.sendToDeadLetter(message, decision.reason);
          await this.delete(queueUrl, message);
          this.logger.error({ msg: "mensagem malformada: enviada para a DLQ", reason: decision.reason, ...log });
          return "deadLettered";
      }
    } catch (error) {
      // Falhou falar com o SQS (ack, visibilidade ou DLQ). A mensagem não foi
      // apagada, então volta sozinha depois do VisibilityTimeout. Se ela já tinha
      // sido processada, a inbox transforma a redelivery em duplicata.
      this.logger.error({ msg: "falha ao aplicar a decisão no SQS", action: decision.action, sqsError: String(error), ...log });
      return "retried";
    }
  }

  private async delete(queueUrl: string, message: Message): Promise<void> {
    await this.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }));
  }

  /** Devolve para a fila sem processar: fica visível de novo na hora. */
  private async release(queueUrl: string, message: Message): Promise<void> {
    try {
      await this.client.send(
        new ChangeMessageVisibilityCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0 }),
      );
    } catch (error) {
      // Sem problema: ela volta sozinha quando o VisibilityTimeout da fila vencer.
      this.logger.warn({ msg: "falha ao devolver a mensagem para a fila", sqsMessageId: message.MessageId, sqsError: String(error) });
    }
  }

  /** Copia a mensagem original para a DLQ, com o motivo como atributo. */
  private async sendToDeadLetter(message: Message, reason: string): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: await this.queueUrl(env.aws.dlqName),
        MessageBody: message.Body ?? "",
        MessageGroupId: message.Attributes?.MessageGroupId ?? "dead-letter",
        // O MessageId do SQS é único por envio: se mandarmos a mesma para a DLQ
        // duas vezes (falha no delete), o FIFO descarta a cópia.
        MessageDeduplicationId: message.MessageId,
        MessageAttributes: { deadLetterReason: { DataType: "String", StringValue: reason } },
      }),
    );
  }

  /** Descobre a URL de cada fila uma vez só. Se falhar, esquece e tenta de novo. */
  private queueUrl(name: string): Promise<string> {
    let url = this.queueUrls.get(name);
    if (!url) {
      url = this.client.send(new GetQueueUrlCommand({ QueueName: name })).then((response) => response.QueueUrl!);
      url.catch(() => this.queueUrls.delete(name));
      this.queueUrls.set(name, url);
    }
    return url;
  }
}
