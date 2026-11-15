import { DomainError } from "../shared/domain-error.ts";

/** Faltou o que identifica a mensagem (consumidor, id, hash): não dá para deduplicar. */
export class InvalidInboxMessageError extends DomainError {
  readonly code = "INVALID_INBOX_MESSAGE";
  constructor(reason: string) {
    super(`Mensagem de inbox inválida: ${reason}`);
  }
}

export class InboxMessageAlreadyProcessedError extends DomainError {
  readonly code = "INBOX_MESSAGE_ALREADY_PROCESSED";
  constructor(
    readonly consumerName: string,
    readonly messageId: string,
  ) {
    super(`A mensagem ${messageId} já foi processada por ${consumerName}`);
  }
}
