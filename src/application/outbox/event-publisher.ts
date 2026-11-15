import type { OutboxMessage } from "../../domain/outbox/outbox-message.ts";

/**
 * "Alguém que publica um evento fora do sistema." A aplicação depende disto;
 * a infraestrutura entrega a implementação (SQS).
 * É classe abstrata (e não interface) porque o Nest usa a CLASSE como token de DI.
 */
export abstract class EventPublisher {
  abstract publish(message: OutboxMessage): Promise<void>;
}
