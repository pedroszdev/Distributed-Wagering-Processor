/** Erros de rede: o banco não respondeu. */
const NETWORK_CODES = new Set(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE"]);

// Códigos do Postgres (SQLSTATE) que significam "tente de novo":
const RETRYABLE_SQLSTATES = new Set([
  "57P01", // admin_shutdown: o banco está desligando
  "57P02", // crash_shutdown
  "57P03", // cannot_connect_now: o banco está subindo
  "53300", // too_many_connections
  "40001", // serialization_failure
  "40P01", // deadlock_detected
  "55P03", // lock_not_available
]);

/** O `pg` usa essas mensagens SEM code quando a conexão cai no meio, ou o pool esgota. */
const RETRYABLE_MESSAGES = ["Connection terminated", "timeout exceeded when trying to connect"];

export function isTransientDatabaseError(error: unknown): boolean {
    if (!(error instanceof Error)) {
        return false
    }
    const code = (error as { code?: unknown }).code
    if (typeof code === 'string' ) {
        if (NETWORK_CODES.has(code)) return true
        if (RETRYABLE_SQLSTATES.has(code)) return true
        if (code.startsWith("08")) return true
    }
    return RETRYABLE_MESSAGES.some((m) => error.message.includes(m))
}
