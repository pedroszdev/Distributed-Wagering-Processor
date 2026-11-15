/**
 * Quando reprocessar um REFUND/ROLLBACK que chegou antes da transação que ele
 * referencia (seção 7.1), e quando desistir.
 *
 * Backoff: base · 2^tentativas_falhas, com teto. Com os padrões:
 *   chegou → 1s → 2s → 4s → 8s → 16s → 32s → 60s → 60s ...
 * TTL: passado esse tempo desde que a transação chegou, se a referência AINDA não
 * existe, a transação vira REJECTED com REFERENCE_NOT_FOUND. Se a referência
 * aparecer depois do TTL mas antes da próxima tentativa, ela é processada
 * normalmente: o TTL só rejeita o que continua sem referência.
 */
export interface PendingReferencePolicy {
  ttlMs: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

/** 5 min de TTL: um provedor que demora mais que isso para mandar a BET está com problema. */
export const DEFAULT_PENDING_REFERENCE_POLICY: PendingReferencePolicy = {
  ttlMs: 5 * 60_000,
  baseDelayMs: 1_000,
  maxDelayMs: 60_000,
};

/** Token de injeção (o Nest precisa de um "nome" para um objeto de configuração). */
export const PENDING_REFERENCE_POLICY = Symbol("PENDING_REFERENCE_POLICY");

/** Próxima tentativa, dado quantas já falharam (0 = acabou de chegar). */
export function nextPendingAttemptAt(
  now: Date,
  failedAttempts: number,
  policy: PendingReferencePolicy = DEFAULT_PENDING_REFERENCE_POLICY,
): Date {
  const delay = Math.min(policy.baseDelayMs * 2 ** Math.max(0, failedAttempts), policy.maxDelayMs);
  return new Date(now.getTime() + delay);
}

/** Já passou do TTL? (Conta a partir de quando a transação chegou.) */
export function isPendingReferenceExpired(
  createdAt: Date,
  now: Date,
  policy: PendingReferencePolicy = DEFAULT_PENDING_REFERENCE_POLICY,
): boolean {
  return now.getTime() - createdAt.getTime() >= policy.ttlMs;
}
