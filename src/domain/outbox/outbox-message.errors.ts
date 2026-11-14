import { DomainError } from "../shared/domain-error.ts";

export class OutboxMessageAlreadyPublishedError extends DomainError {
  readonly code = "OUTBOX_MESSAGE_ALREADY_PUBLISHED";
  constructor(readonly messageId: string) {
    super(`A mensagem ${messageId} da outbox já foi publicada`);
  }
}
