import { InboxMessageAlreadyProcessedError, InvalidInboxMessageError } from "./inbox-message.errors.ts";

/** Até onde vai um id de mensagem (o mesmo limite do parse da fila). */
const MAX_ID_LENGTH = 255;

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
}

/** Tudo que fica salvo numa linha da inbox (usado no rehydrate). */
export interface InboxMessageState extends ReceiveInboxProps {
  processedAt?: Date;
}

/**
 * "Esta mensagem já foi processada por este consumidor?" (seção 6.5 do desafio).
 *
 * A identidade é o par (consumerName, messageId): a mesma mensagem lida por dois
 * consumidores diferentes são duas entradas. O `messageId` é o do PROVEDOR (vem no
 * corpo da mensagem), não o MessageId que o SQS gera a cada envio.
 *
 * A garantia de "uma vez só" é do BANCO (PRIMARY KEY + ON CONFLICT DO NOTHING,
 * na mesma transação da aposta). Esta classe guarda as regras da entrada em si:
 * quem a identifica, e que ela só é processada uma vez.
 */
export class InboxMessage {
  private constructor(
    readonly messageId: string,
    readonly consumerName: string,
    readonly payloadHash: string,
    readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  /** A mensagem acabou de chegar: ainda não processada. */
  static receive(props: ReceiveInboxProps): InboxMessage {
    assertId("messageId", props.messageId);
    assertId("consumerName", props.consumerName);
    if (!props.payloadHash) {
      throw new InvalidInboxMessageError("payloadHash é obrigatório");
    }
    return new InboxMessage(props.messageId, props.consumerName, props.payloadHash, props.receivedAt);
  }

  /** Reconstrução a partir do banco: não revalida (o estado já foi aceito antes). */
  static rehydrate(state: InboxMessageState): InboxMessage {
    const { messageId, consumerName, payloadHash, receivedAt, processedAt } = state;
    return new InboxMessage(messageId, consumerName, payloadHash, receivedAt, processedAt);
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined;
  }

  /** O processamento terminou. Uma vez só, e nunca antes de a mensagem ter chegado. */
  markProcessed(at: Date): void {
    if (this.isProcessed()) {
      throw new InboxMessageAlreadyProcessedError(this.consumerName, this.messageId);
    }
    if (at.getTime() < this.receivedAt.getTime()) {
      throw new InvalidInboxMessageError("processedAt não pode ser anterior a receivedAt");
    }
    this._processedAt = at;
  }

  /** A mesma mensagem (mesmo id) chegou com o mesmo conteúdo? */
  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }
}

function assertId(field: string, value: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new InvalidInboxMessageError(`${field} é obrigatório`);
  }
  if (value.length > MAX_ID_LENGTH) {
    throw new InvalidInboxMessageError(`${field} pode ter até ${MAX_ID_LENGTH} caracteres`);
  }
}
