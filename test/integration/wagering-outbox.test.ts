import { afterAll, beforeAll, describe, expect, spyOn, test } from "bun:test";
import { ProcessWagerTransactionUseCase } from "../../src/application/wagering/process-wager-transaction.use-case.ts";
import { CreateWalletUseCase } from "../../src/application/wallet/create-wallet.use-case.ts";
import { Money } from "../../src/domain/money/money.ts";
import { FailureCode } from "../../src/domain/wager-transaction/failure-code.ts";
import { WagerTransactionKind } from "../../src/domain/wager-transaction/wager-transaction.enums.ts";
import type { WagerTransactionInput } from "../../src/http/wager-transaction.request.ts";
import { OutboxMessageRepository } from "../../src/infrastructure/database/repositories/outbox-message.repository.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, sql } from "./support/database.ts";
import { newId } from "./support/rows.ts";

/**
 * O use case grava os eventos certos na outbox, NA MESMA transação.
 * (Publicar no SQS é o item 7; aqui só conferimos o que ficou na tabela.)
 */

const { Bet, Win, Loss, Refund } = WagerTransactionKind;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

let api: TestApp;
let useCase: ProcessWagerTransactionUseCase;
let createWallet: CreateWalletUseCase;

beforeAll(async () => {
  api = await startTestApp();
  useCase = api.app.get(ProcessWagerTransactionUseCase);
  createWallet = api.app.get(CreateWalletUseCase);
});
afterAll(async () => {
  await api.close();
  await closeTestOrm();
});

// ============================================================================ helpers

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });

async function newWallet(amount = "100.00") {
  const playerId = newId();
  const wallet = await createWallet.execute({ playerId, initialBalance: brl(amount) });
  return { walletId: wallet.id, playerId };
}

function request(
  wallet: { walletId: string; playerId: string },
  kind: WagerTransactionKind,
  amount: string,
  overrides: Partial<WagerTransactionInput> = {},
): WagerTransactionInput {
  const externalTransactionId = overrides.externalTransactionId ?? `tx-${newId()}`;
  return {
    providerId: "provider-a",
    externalTransactionId,
    idempotencyKey: `provider-a:${externalTransactionId}`,
    playerId: wallet.playerId,
    walletId: wallet.walletId,
    roundId: "round-1",
    gameId: "game-1",
    kind,
    money: brl(amount),
    referenceExternalTransactionId: undefined,
    ...overrides,
  };
}

/** Todas as linhas da outbox desta wallet, na ordem em que foram criadas. */
async function outboxOf(walletId: string): Promise<any[]> {
  // Todo evento de wager carrega data.walletId, então dá para filtrar pelo JSON.
  return sql(
    `select * from outbox_messages
      where payload -> 'data' ->> 'walletId' = ?
      order by id`,
    [walletId],
  );
}

const typesOf = (rows: any[]) => rows.map((r) => r.event_type);

async function errorOf(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("era esperado um erro, mas a promise resolveu");
}

// ============================================================================ quais eventos

describe("quais eventos cada resultado gera (tabela da seção 11)", () => {
  test("BET processada → WagerTransactionProcessed + WalletBalanceChanged", async () => {
    const wallet = await newWallet("100.00");

    await useCase.execute(request(wallet, Bet, "30.00"));

    expect(typesOf(await outboxOf(wallet.walletId))).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
  });

  test("WIN processado → WagerTransactionProcessed + WalletBalanceChanged", async () => {
    const wallet = await newWallet("100.00");

    await useCase.execute(request(wallet, Win, "50.00"));

    expect(typesOf(await outboxOf(wallet.walletId))).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
  });

  test("LOSS → SÓ WagerTransactionProcessed (o saldo não mudou)", async () => {
    const wallet = await newWallet("100.00");

    await useCase.execute(request(wallet, Loss, "25.00"));

    expect(typesOf(await outboxOf(wallet.walletId))).toEqual(["WagerTransactionProcessed"]);
  });

  test("BET sem saldo → SÓ WagerTransactionRejected, com o failureCode", async () => {
    const wallet = await newWallet("100.00");

    const result = await useCase.execute(request(wallet, Bet, "500.00"));

    const rows = await outboxOf(wallet.walletId);
    expect(typesOf(rows)).toEqual(["WagerTransactionRejected"]);
    expect(rows[0].aggregate_id).toBe(result.transactionId);
    expect(rows[0].payload.data.failureCode).toBe(FailureCode.InsufficientFunds);
  });

  test("REFUND sem a BET → SÓ WagerTransactionPendingReference, com a referência que falta", async () => {
    const wallet = await newWallet("100.00");

    await useCase.execute(request(wallet, Refund, "25.00", { referenceExternalTransactionId: "bet-que-nao-chegou" }));

    const rows = await outboxOf(wallet.walletId);
    expect(typesOf(rows)).toEqual(["WagerTransactionPendingReference"]);
    expect(rows[0].payload.data.referenceExternalTransactionId).toBe("bet-que-nao-chegou");
  });

  test("REFUND processado → Processed (com referenceTransactionId) + WalletBalanceChanged CREDIT", async () => {
    const wallet = await newWallet("100.00");
    const bet = request(wallet, Bet, "25.00");
    const betResult = await useCase.execute(bet);

    const refund = await useCase.execute(
      request(wallet, Refund, "25.00", { referenceExternalTransactionId: bet.externalTransactionId }),
    );

    const rows = (await outboxOf(wallet.walletId)).filter((r) => r.payload.data.transactionId === refund.transactionId);
    expect(typesOf(rows)).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
    expect(rows[0].payload.data.referenceTransactionId).toBe(betResult.transactionId);
    expect(rows[1].payload.data.direction).toBe("CREDIT");
  });
});

// ============================================================================ conteúdo

describe("o conteúdo das linhas", () => {
  test("WagerTransactionProcessed: envelope + data completo, money como string decimal", async () => {
    const wallet = await newWallet("100.00");
    const input = request(wallet, Bet, "30");

    const result = await useCase.execute(input, { correlationId: "corr-123", causationId: "msg-9" });

    const [row] = await outboxOf(wallet.walletId);
    expect(row.payload).toEqual({
      eventId: row.id, // o id da linha É o eventId (o consumidor deduplica por ele)
      eventType: "WagerTransactionProcessed",
      aggregateId: result.transactionId,
      correlationId: "corr-123",
      causationId: "msg-9",
      occurredAt: expect.any(String),
      version: 1,
      data: {
        transactionId: result.transactionId,
        providerId: "provider-a",
        externalTransactionId: input.externalTransactionId,
        walletId: wallet.walletId,
        playerId: wallet.playerId,
        roundId: "round-1",
        gameId: "game-1",
        kind: "BET",
        money: { amount: "30.00", currency: "BRL" }, // "30" normalizado
        balanceAfter: { amount: "70.00", currency: "BRL" },
        // sem referenceTransactionId: BET não tem referência, e o campo nem aparece (nem como null)
      },
    });
    expect(new Date(row.payload.occurredAt).toISOString()).toBe(row.payload.occurredAt);
  });

  test("WalletBalanceChanged: aggregateId é a WALLET, com before/after e a versão nova", async () => {
    const wallet = await newWallet("100.00");

    const result = await useCase.execute(request(wallet, Bet, "30.00"));

    const [, row] = await outboxOf(wallet.walletId);
    expect(row.aggregate_id).toBe(wallet.walletId);
    expect(row.payload.data).toEqual({
      walletId: wallet.walletId,
      transactionId: result.transactionId,
      direction: "DEBIT",
      money: { amount: "30.00", currency: "BRL" },
      balanceBefore: { amount: "100.00", currency: "BRL" },
      balanceAfter: { amount: "70.00", currency: "BRL" },
      walletVersion: 2,
    });
  });

  test("as colunas de controle: pendente, 0 tentativas, já pode publicar", async () => {
    const wallet = await newWallet("100.00");

    await useCase.execute(request(wallet, Bet, "30.00"));

    for (const row of await outboxOf(wallet.walletId)) {
      expect(row.published_at).toBeNull();
      expect(row.attempts).toBe(0);
      expect(row.last_error).toBeNull();
      expect(new Date(row.next_attempt_at).getTime()).toBeLessThanOrEqual(Date.now());
      expect(row.event_type).toBe(row.payload.eventType);
      expect(row.aggregate_id).toBe(row.payload.aggregateId);
    }
  });

  test("sem contexto: gera um correlationId, e os eventos da MESMA chamada compartilham ele", async () => {
    const wallet = await newWallet("100.00");

    await useCase.execute(request(wallet, Bet, "30.00"));
    await useCase.execute(request(wallet, Bet, "10.00"));

    const rows = await outboxOf(wallet.walletId);
    const [first, second, third, fourth] = rows.map((r) => r.payload.correlationId);
    expect(first).toMatch(UUID);
    expect(second).toBe(first); // Processed + BalanceChanged da 1ª chamada
    expect(third).toBe(fourth); // ... da 2ª chamada
    expect(third).not.toBe(first); // chamadas diferentes → ids diferentes
    expect(rows[0].payload).not.toHaveProperty("causationId");
  });
});

// ============================================================================ sem eventos

describe("quando NÃO deve haver evento", () => {
  test("replay → nenhuma linha nova (o evento já foi gravado na 1ª vez)", async () => {
    const wallet = await newWallet("100.00");
    const bet = request(wallet, Bet, "30.00");
    await useCase.execute(bet);

    await useCase.execute(bet);
    await useCase.execute(bet);

    expect(await outboxOf(wallet.walletId)).toHaveLength(2);
  });

  test("conflito de idempotência → nenhuma linha nova", async () => {
    const wallet = await newWallet("100.00");
    const bet = request(wallet, Bet, "30.00");
    await useCase.execute(bet);

    await errorOf(useCase.execute({ ...bet, money: brl("31.00") }));

    expect(await outboxOf(wallet.walletId)).toHaveLength(2);
  });

  test("pedido inválido (moeda errada) → nenhuma linha", async () => {
    const wallet = await newWallet("100.00");

    await errorOf(useCase.execute(request(wallet, Bet, "10.00", { money: Money.from({ amount: "10.00", currency: "USD" }) })));

    expect(await outboxOf(wallet.walletId)).toEqual([]);
  });

  test("50 cópias do mesmo pedido em paralelo → exatamente 2 eventos, não 100", async () => {
    const wallet = await newWallet("100.00");
    const bet = request(wallet, Bet, "30.00");

    await Promise.all(Array.from({ length: 50 }, () => useCase.execute(bet)));

    expect(typesOf(await outboxOf(wallet.walletId))).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
  });
});

// ============================================================================ atomicidade

describe("atomicidade: tudo ou nada", () => {
  test("se gravar o 2º evento falhar, NADA fica: nem transação, nem saldo, nem ledger, nem o 1º evento", async () => {
    const wallet = await newWallet("100.00");
    const bet = request(wallet, Bet, "30.00");

    // A instância que o Nest injetou no use case. O spy deixa o 1º insert passar
    // e faz o 2º explodir — como se o banco caísse no meio da transação.
    const outbox = api.app.get(OutboxMessageRepository);
    const original = outbox.insert.bind(outbox);
    let calls = 0;
    const spy = spyOn(outbox, "insert").mockImplementation(async (em, message) => {
      calls++;
      if (calls === 2) throw new Error("banco caiu no meio");
      return original(em, message);
    });

    try {
      const error = await errorOf(useCase.execute(bet));
      expect((error as Error).message).toBe("banco caiu no meio");
    } finally {
      spy.mockRestore();
    }

    // O ROLLBACK desfez tudo, inclusive o 1º evento, que chegou a ser inserido.
    expect(await outboxOf(wallet.walletId)).toEqual([]);
    expect(await sql("select 1 from wager_transactions where external_transaction_id = ?", [bet.externalTransactionId])).toEqual([]);
    const [walletRow] = await sql("select balance_amount, version from wallets where id = ?", [wallet.walletId]);
    expect(walletRow).toEqual({ balance_amount: "100.00", version: 1 });
    const [{ n }] = await sql("select count(*)::int as n from wallet_ledger_entries where wallet_id = ?", [wallet.walletId]);
    expect(n).toBe(1); // só o lançamento do OPENING

    // E o mesmo pedido, tentado de novo, funciona normalmente (não é replay: nada foi gravado).
    const retry = await useCase.execute(bet);
    expect(retry.idempotentReplay).toBe(false);
    expect(retry.balance.amount).toBe("70.00");
    expect(await outboxOf(wallet.walletId)).toHaveLength(2);
  });
});
