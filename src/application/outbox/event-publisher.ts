import type { OutboxMessage } from "../../domain/outbox/outbox-message.ts";

/** O que aconteceu com cada evento de um lote. */
export interface PublishBatchResult {
  /** id do evento → motivo da falha. Quem não está aqui foi publicado. */
  failed: Map<string, string>;
}

/**
 * "Alguém que publica eventos fora do sistema." A aplicação depende disto;
 * a infraestrutura entrega a implementação (SQS).
 * É classe abstrata (e não interface) porque o Nest usa a CLASSE como token de DI.
 *
 * Publica em LOTE: no teste de carga, uma chamada por evento deixou a outbox
 * para trás (~150 eventos/s publicados contra ~350/s gerados). A falha é POR
 * EVENTO: num lote de 10, se 1 falhar, só ele volta para retry.
 *
 * Se o lote inteiro falhar (ex.: SQS fora), pode lançar: quem chama trata como
 * "todos falharam".
 */
export abstract class EventPublisher {
  abstract publishBatch(messages: OutboxMessage[]): Promise<PublishBatchResult>;
}
