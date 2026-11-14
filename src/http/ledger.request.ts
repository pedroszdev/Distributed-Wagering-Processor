import { InvalidPayloadError } from "../application/shared/invalid-payload.error.ts";
import { UUID_PATTERN } from "./wager-transaction.request.ts";

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

/** id → texto opaco para o cliente. */
export function encodeLedgerCursor(id: string): string {
  return Buffer.from(id).toString("base64url");
}

export function parseLedgerQuery(
  cursor: string | undefined,
  limit: string | undefined,
): { afterId?: string; limit: number } {
    const limitReal = !limit ? DEFAULT_LIMIT : limit
    const n = Number(limitReal)
    if (!Number.isInteger(n)) throw new InvalidPayloadError(`limit deve ser um inteiro entre 1 e ${MAX_LIMIT}`)
    if (n < 1 || n > MAX_LIMIT) throw new InvalidPayloadError(`limit deve ser um inteiro entre 1 e ${MAX_LIMIT}`)
    if (!cursor) {
        return { limit:n }
    }
    const afterId = Buffer.from(cursor, "base64url").toString()
    if (!UUID_PATTERN.test(afterId)) throw new InvalidPayloadError("cursor inválido")
    return {afterId, limit: n}
}
