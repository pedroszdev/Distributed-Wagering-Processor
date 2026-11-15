import { ConcurrentWalletUpdateError } from "../../application/shared/concurrent-update.error.ts";
import { DuplicateMessageError } from "../../application/wagering/wagering.errors.ts";
import { DomainError } from "../../domain/shared/domain-error.ts";
import { isTransientDatabaseError } from "../database/transient-error.ts";

/** Backoff do retry: 5s, 10s, 20s, 40s... até 5 min. */
const BASE_DELAY_SECONDS = 5;
const MAX_DELAY_SECONDS = 300;

/**
 * Códigos de "a mensagem está errada": reenviar igual nunca vai funcionar.
 * São os mesmos que o HTTP responde com 400 (domain-error.filter.ts): lá quem
 * corrige é o provedor; aqui a mensagem vai para a DLQ para alguém olhar.
 */
const MALFORMED_CODES = new Set([
  "INVALID_PAYLOAD",
  "INVALID_MONEY",
  "INVALID_AMOUNT",
  "CURRENCY_MISMATCH",
  "MISSING_REFERENCE",
  "UNEXPECTED_REFERENCE",
  "WALLET_PLAYER_MISMATCH",
]);

export type MessageDecision =
  | { action: "ack"; reason: "processed" | "duplicate" | "business-error" }
  | { action: "dead-letter"; reason: string }
  | { action: "retry"; delaySeconds: number };

/** Espera antes da próxima tentativa (receiveCount começa em 1). */
export function retryDelaySeconds(receiveCount: number): number {
  const attempt = Math.max(1, receiveCount);
  return Math.min(BASE_DELAY_SECONDS * 2 ** (attempt - 1), MAX_DELAY_SECONDS);
}

/**
 * O que fazer com uma mensagem cujo processamento lançou `error`.
 *
 * A ORDEM DOS IFs IMPORTA: DuplicateMessageError e ConcurrentWalletUpdateError
 * também são DomainError. Se o caso genérico viesse antes, a duplicata viraria
 * "erro de negócio" e o conflito de lock levaria ack SEM processar (aposta perdida).
 * Específicos primeiro, genérico por último.
 */
export function decideOnError(error: unknown, receiveCount: number): MessageDecision {
  // 1. Já processada antes: só confirma.
  if (error instanceof DuplicateMessageError) {
    return { action: "ack", reason: "duplicate" };
  }

  // 2. Passageiro (banco fora, deadlock, conflito de versão): tenta de novo depois.
  if (error instanceof ConcurrentWalletUpdateError || isTransientDatabaseError(error)) {
    return { action: "retry", delaySeconds: retryDelaySeconds(receiveCount) };
  }

  if (error instanceof DomainError) {
    // 3. A mensagem em si está errada: nunca vai funcionar → DLQ já.
    if (MALFORMED_CODES.has(error.code)) {
      return { action: "dead-letter", reason: error.code };
    }
    // 4. Regra de negócio disse não (conflito de idempotência, wallet não existe):
    //    tentar de novo daria o mesmo erro. É um resultado final → ack.
    return { action: "ack", reason: "business-error" };
  }

  // 5. Desconhecido (bug?): não dá para saber se é passageiro, então tenta de novo.
  //    Depois de maxReceiveCount entregas, o próprio SQS move para a DLQ.
  return { action: "retry", delaySeconds: retryDelaySeconds(receiveCount) };
}
