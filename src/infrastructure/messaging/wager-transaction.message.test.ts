import { describe, expect, test } from "bun:test";
import { InvalidPayloadError } from "../../application/shared/invalid-payload.error.ts";
import { WagerTransactionKind } from "../../domain/wager-transaction/wager-transaction.enums.ts";
import { parseWagerTransactionMessage } from "./wager-transaction.message.ts";

const PLAYER = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1";
const WALLET = "0192f291-27dd-7d3f-8071-5f8685deef37";

/** A mensagem de exemplo da seção 10 do desafio. */
function message(overrides: Record<string, unknown> = {}, dataOverrides: Record<string, unknown> = {}) {
  return {
    messageId: "msg-123",
    type: "WagerTransactionRequested",
    occurredAt: "2026-07-29T15:00:00.000Z",
    data: {
      providerId: "provider-a",
      externalTransactionId: "transaction-123",
      idempotencyKey: "provider-a:transaction-123",
      playerId: PLAYER,
      walletId: WALLET,
      roundId: "round-987",
      gameId: "fortune-chimp",
      kind: "BET",
      money: { amount: "25.00", currency: "BRL" },
      ...dataOverrides,
    },
    ...overrides,
  };
}

const parse = (value: unknown) => parseWagerTransactionMessage(typeof value === "string" ? value : JSON.stringify(value));

describe("parseWagerTransactionMessage", () => {
  test("a mensagem do exemplo do desafio vira o mesmo input do HTTP", () => {
    const { messageId, input } = parse(message());

    expect(messageId).toBe("msg-123");
    expect(input).toMatchObject({
      idempotencyKey: "provider-a:transaction-123",
      providerId: "provider-a",
      externalTransactionId: "transaction-123",
      playerId: PLAYER,
      walletId: WALLET,
      roundId: "round-987",
      gameId: "fortune-chimp",
      kind: WagerTransactionKind.Bet,
      referenceExternalTransactionId: undefined,
    });
    expect(input.money.toJSON()).toEqual({ amount: "25.00", currency: "BRL" });
  });

  test("REFUND com referência", () => {
    const { input } = parse(message({}, { kind: "REFUND", referenceExternalTransactionId: "bet-1" }));
    expect(input.kind).toBe(WagerTransactionKind.Refund);
    expect(input.referenceExternalTransactionId).toBe("bet-1");
  });

  test.each([
    ["não é JSON", "{ isso não é json"],
    ["JSON que não é objeto", "42"],
    ["null", "null"],
    ["array", "[]"],
  ])("%s → InvalidPayloadError", (_label, raw) => {
    expect(() => parse(raw)).toThrow(InvalidPayloadError);
  });

  test.each([
    ["sem messageId", { messageId: undefined }],
    ["messageId vazio", { messageId: "" }],
    ["messageId número", { messageId: 123 }],
    ["messageId gigante", { messageId: "x".repeat(256) }],
    ["tipo desconhecido", { type: "WalletCreated" }],
    ["sem tipo", { type: undefined }],
    ["sem data", { data: undefined }],
    ["data não é objeto", { data: "BET" }],
  ])("envelope inválido: %s → InvalidPayloadError", (_label, overrides) => {
    expect(() => parse(message(overrides))).toThrow(InvalidPayloadError);
  });

  test.each([
    ["sem idempotencyKey", { idempotencyKey: undefined }],
    ["idempotencyKey não é string", { idempotencyKey: 42 }],
    ["walletId não é UUID", { walletId: "abc" }],
    ["kind inválido", { kind: "JACKPOT" }],
    ["kind OPENING (interno)", { kind: "OPENING" }],
    ["valor negativo", { money: { amount: "-1.00", currency: "BRL" } }],
    ["valor como number", { money: { amount: 25, currency: "BRL" } }],
  ])("data inválido (mesma validação do HTTP): %s → erro de payload", (_label, dataOverrides) => {
    expect(() => parse(message({}, dataOverrides))).toThrow();
  });

  test("tipo desconhecido: a mensagem de erro diz qual tipo chegou", () => {
    expect(() => parse(message({ type: "WalletCreated" }))).toThrow("WalletCreated");
  });
});
