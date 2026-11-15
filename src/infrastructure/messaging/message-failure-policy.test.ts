import { describe, expect, test } from "bun:test";
import { DeadlockException } from "@mikro-orm/postgresql";
import { ConcurrentWalletUpdateError } from "../../application/shared/concurrent-update.error.ts";
import { InvalidPayloadError } from "../../application/shared/invalid-payload.error.ts";
import {
  DuplicateMessageError,
  IdempotencyConflictError,
  WalletPlayerMismatchError,
} from "../../application/wagering/wagering.errors.ts";
import { WalletNotFoundError } from "../../application/wallet/wallet.errors.ts";
import { CurrencyMismatchError, InvalidMoneyError } from "../../domain/money/money.errors.ts";
import { WagerTransactionKind } from "../../domain/wager-transaction/wager-transaction.enums.ts";
import { MissingReferenceError, UnexpectedReferenceError } from "../../domain/wager-transaction/wager-transaction.errors.ts";
import { InvalidInboxMessageError } from "../../domain/inbox/inbox-message.errors.ts";
import { decideOnError, retryDelaySeconds } from "./message-failure-policy.ts";

const withCode = (code: string, message = "erro") => Object.assign(new Error(message), { code });

describe("retryDelaySeconds", () => {
  test.each([
    [1, 5],
    [2, 10],
    [3, 20],
    [4, 40],
    [5, 80],
    [6, 160],
    [7, 300], // 320 → teto de 5 min
    [50, 300],
  ])("entrega %i → espera %is", (receiveCount, expected) => {
    expect(retryDelaySeconds(receiveCount)).toBe(expected);
  });

  test("receiveCount inválido (0 ou negativo) é tratado como a 1ª entrega", () => {
    expect(retryDelaySeconds(0)).toBe(5);
    expect(retryDelaySeconds(-3)).toBe(5);
  });
});

describe("decideOnError", () => {
  test("duplicata (inbox) → ack, sem tentar de novo", () => {
    expect(decideOnError(new DuplicateMessageError("consumer", "msg-1"), 1)).toEqual({ action: "ack", reason: "duplicate" });
  });

  test.each([
    ["conflito de versão da wallet", new ConcurrentWalletUpdateError("w-1")],
    ["banco fora (ECONNREFUSED)", withCode("ECONNREFUSED")],
    ["conexão caiu", new Error("Connection terminated unexpectedly")],
    ["deadlock (40P01)", withCode("40P01")],
    ["deadlock embrulhado pelo MikroORM", new DeadlockException(withCode("40P01", "deadlock detected"))],
  ])("transitório: %s → retry com backoff", (_label, error) => {
    expect(decideOnError(error, 1)).toEqual({ action: "retry", delaySeconds: 5 });
    expect(decideOnError(error, 3)).toEqual({ action: "retry", delaySeconds: 20 });
  });

  test.each([
    ["payload inválido", new InvalidPayloadError("walletId deve ser um UUID"), "INVALID_PAYLOAD"],
    ["dinheiro inválido", new InvalidMoneyError("escala"), "INVALID_MONEY"],
    ["moeda diferente da wallet", new CurrencyMismatchError("BRL", "USD"), "CURRENCY_MISMATCH"],
    ["REFUND sem referência", new MissingReferenceError(WagerTransactionKind.Refund), "MISSING_REFERENCE"],
    ["BET com referência", new UnexpectedReferenceError(WagerTransactionKind.Bet), "UNEXPECTED_REFERENCE"],
    ["player não é dono da wallet", new WalletPlayerMismatchError("w-1", "p-1"), "WALLET_PLAYER_MISMATCH"],
    ["entrada de inbox sem identidade", new InvalidInboxMessageError("messageId é obrigatório"), "INVALID_INBOX_MESSAGE"],
  ])("mensagem malformada: %s → DLQ na hora, com o código como motivo", (_label, error, code) => {
    expect(decideOnError(error, 1)).toEqual({ action: "dead-letter", reason: code });
  });

  test.each([
    ["conflito de idempotência", new IdempotencyConflictError("key", "tx-1")],
    ["wallet não existe", new WalletNotFoundError("w-1")],
  ])("erro de negócio: %s → ack (tentar de novo daria o mesmo erro)", (_label, error) => {
    expect(decideOnError(error, 1)).toEqual({ action: "ack", reason: "business-error" });
  });

  test.each([
    ["bug (TypeError)", new TypeError("Cannot read properties of undefined")],
    ["Error qualquer", new Error("algo estranho")],
    ["unique violation inesperada (23505)", withCode("23505")],
    ["valor que nem é Error", "ops"],
  ])("desconhecido: %s → retry (depois de 5 entregas o SQS manda para a DLQ)", (_label, error) => {
    expect(decideOnError(error, 2)).toEqual({ action: "retry", delaySeconds: 10 });
  });

  test("ConcurrentWalletUpdateError É um DomainError, mas NÃO pode virar ack (a aposta seria perdida)", () => {
    const decision = decideOnError(new ConcurrentWalletUpdateError("w-1"), 1);
    expect(decision.action).toBe("retry");
  });
});
