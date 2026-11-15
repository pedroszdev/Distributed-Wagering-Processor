import { DomainError } from "../../domain/shared/domain-error.ts";

export class IdempotencyConflictError extends DomainError {
  readonly code = "IDEMPOTENCY_CONFLICT";

  constructor(
    readonly idempotencyKey: string,
    readonly existingTransactionId: string,
  ) {
    super(`Idempotency-Key "${idempotencyKey}" já foi usada pela transação ${existingTransactionId} com um payload diferente`);
  }
}

export class WalletPlayerMismatchError extends DomainError {
  readonly code = "WALLET_PLAYER_MISMATCH";

  constructor(
    readonly walletId: string,
    readonly playerId: string,
  ) {
    super(`O player ${playerId} do pedido não é dono da wallet ${walletId}`);
  }
}

/**
 * A mesma mensagem da fila chegou de novo e já tinha sido processada (a inbox
 * já tem o registro). Não é falha: o consumidor só dá ack e segue.
 */
export class DuplicateMessageError extends DomainError {
  readonly code = "DUPLICATE_MESSAGE";

  constructor(
    readonly consumerName: string,
    readonly messageId: string,
  ) {
    super(`Mensagem ${messageId} já foi processada por ${consumerName}`);
  }
}

export class WagerTransactionNotFoundError extends DomainError {
  readonly code = "WAGER_TRANSACTION_NOT_FOUND";

  constructor(readonly lookup: string) {   // ← texto que diz o que foi procurado
    super(`Transação ${lookup} não encontrada`);
  }
}
