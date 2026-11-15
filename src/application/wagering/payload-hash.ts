import type { WagerTransactionInput } from "../../http/wager-transaction.request.ts";

/**
 * Ordem das chaves do JSON canônico: por unidade de código UTF-16 (a mesma da
 * RFC 8785 / JCS, e a mesma do `sort()` padrão do JavaScript), escrita explícita.
 *
 * NÃO usar `localeCompare`: ele depende do idioma/ICU da máquina e põe "b" antes
 * de "B" e "é" antes de "Z". A ordem mudaria o JSON canônico e, com ele, o hash de
 * toda transação já gravada: um reenvio legítimo viraria conflito 409 em vez de
 * replay. O teste "golden" em payload-hash.test.ts trava essa ordem.
 */
export function compareCodeUnits(a: string, b: string): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

export function canonicalJson(value: unknown): string {

  if (Array.isArray(value)) {
    return "[" + value.map((item) => canonicalJson(item)).join(",") + "]";
  }

  if (value !== null && typeof value === "object") {
    const chavesOrdenada = Object.keys(value).sort(compareCodeUnits)
    const partes: string[] = [];
    const obj = value as Record<string, unknown>
    for (let chave of chavesOrdenada) {
        if (obj[chave] === undefined) continue
        partes.push(JSON.stringify(chave) + ":" + canonicalJson(obj[chave]))
    }
    return "{" + partes.join(",") + "}";
  }
  // string, número, booleano, null
  return JSON.stringify(value);
}

export function computePayloadHash(input: WagerTransactionInput): string {
    const businessFields = {
        providerId: input.providerId,
        externalTransactionId: input.externalTransactionId,
        playerId: input.playerId,
        walletId: input.walletId,
        roundId: input.roundId,
        gameId: input.gameId,
        kind: input.kind,
        money: input.money.toJSON(),   // ← o Money vira { amount, currency }, já normalizado
        referenceExternalTransactionId: input.referenceExternalTransactionId
    };

    const canonical = canonicalJson(businessFields);

    return new Bun.CryptoHasher("sha256").update(canonical).digest("hex");
}
