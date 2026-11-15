import { AsyncLocalStorage } from "node:async_hooks";

/** Os campos que identificam "de qual pedido/mensagem é este log" (seção 12). */
export interface LogFields {
  correlationId?: string;
  messageId?: string;
  sqsMessageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  [key: string]: unknown;
}

const storage = new AsyncLocalStorage<LogFields>();

/**
 * Contexto de log que acompanha o pedido (HTTP) ou a mensagem (SQS) por todas as
 * chamadas assíncronas, sem precisar passar o correlationId de função em função.
 *
 *   logContext.run({ correlationId }, () => ...)   // abre um contexto
 *   logContext.assign({ transactionId })           // completa o contexto atual
 *
 * O JsonLogger junta esses campos em TODA linha de log escrita dentro do contexto.
 * Assim um log de qualquer camada (use case, repositório, filtro de erro) sai
 * com o correlationId do pedido que o causou.
 */
export const logContext = {
  run<T>(fields: LogFields, fn: () => T): T {
    // Herda o contexto de fora (ex.: o worker abre um contexto por item).
    return storage.run({ ...storage.getStore(), ...withoutUndefined(fields) }, fn);
  },

  /** Acrescenta campos ao contexto atual (sem efeito fora de um contexto). */
  assign(fields: LogFields): void {
    const store = storage.getStore();
    if (store) Object.assign(store, withoutUndefined(fields));
  },

  get(): LogFields {
    return storage.getStore() ?? {};
  },
};

function withoutUndefined(fields: LogFields): LogFields {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined));
}
