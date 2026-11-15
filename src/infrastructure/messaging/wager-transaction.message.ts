import { InvalidPayloadError } from "../../application/shared/invalid-payload.error.ts";
import { parseWagerTransactionRequest, type WagerTransactionInput } from "../../http/wager-transaction.request.ts";

/** O único tipo de mensagem que a fila wager-transactions.fifo aceita. */
export const WAGER_TRANSACTION_REQUESTED = "WagerTransactionRequested";

const MAX_MESSAGE_ID_LENGTH = 255;

export interface WagerTransactionMessage {
  /**
   * O id do PROVEDOR (vem no corpo), e não o MessageId que o SQS gera.
   * O SQS dá um MessageId novo a cada envio: se o provedor reenviar a mesma
   * mensagem, só o id do corpo continua igual. É ele que deduplica na inbox.
   */
  messageId: string;
  input: WagerTransactionInput;
}

/**
 * Lê o corpo de uma mensagem da fila (formato da seção 10 do desafio):
 *
 *   { messageId, type: "WagerTransactionRequested", occurredAt, data: { ... } }
 *
 * O `data` é o mesmo body do POST HTTP, mais o idempotencyKey (que no HTTP vem
 * no header). Por isso a validação dos campos é a MESMA função do HTTP.
 *
 * @throws InvalidPayloadError se a mensagem nunca vai poder ser processada.
 */
export function parseWagerTransactionMessage(body: string): WagerTransactionMessage {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    throw new InvalidPayloadError("a mensagem não é um JSON válido");
  }

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new InvalidPayloadError("a mensagem deve ser um objeto JSON");
  }
  const { messageId, type, data } = parsed as Record<string, unknown>;

  if (typeof messageId !== "string" || messageId.length === 0) {
    throw new InvalidPayloadError("messageId deve ser uma string não vazia");
  }
  if (messageId.length > MAX_MESSAGE_ID_LENGTH) {
    throw new InvalidPayloadError(`messageId pode ter até ${MAX_MESSAGE_ID_LENGTH} caracteres`);
  }

  if (type !== WAGER_TRANSACTION_REQUESTED) {
    throw new InvalidPayloadError(`tipo de mensagem desconhecido: ${String(type)}`);
  }

  if (typeof data !== "object" || data === null || Array.isArray(data)) {
    throw new InvalidPayloadError("data deve ser um objeto");
  }

  const { idempotencyKey } = data as Record<string, unknown>;
  const input = parseWagerTransactionRequest(data, typeof idempotencyKey === "string" ? idempotencyKey : undefined);

  return { messageId, input };
}
