import { afterAll, afterEach, beforeAll, describe, expect, mock, spyOn, test } from "bun:test";
import { ProcessWagerTransactionUseCase } from "../../src/application/wagering/process-wager-transaction.use-case.ts";
import { DuplicateMessageError } from "../../src/application/wagering/wagering.errors.ts";
import { CreateWalletUseCase } from "../../src/application/wallet/create-wallet.use-case.ts";
import { Money } from "../../src/domain/money/money.ts";
import { WagerTransactionKind, WagerTransactionStatus } from "../../src/domain/wager-transaction/wager-transaction.enums.ts";
import type { WagerTransactionInput } from "../../src/http/wager-transaction.request.ts";
import { LedgerEntryRepository } from "../../src/infrastructure/database/repositories/ledger-entry.repository.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, sql } from "./support/database.ts";
import { assertLedgerMatchesBalance } from "./support/ledger.ts";
import { newId } from "./support/rows.ts";

/**
 * Inbox: a entrada pela fila registra (consumerName, messageId) NA MESMA transação
 * da aposta. A 2ª entrega da mesma mensagem não tem efeito nenhum.
 */

const CONSUMER = "wager-transactions-consumer";

let api: TestApp;
let useCase: ProcessWagerTransactionUseCase;

beforeAll(async () => {
  api = await startTestApp();
  useCase = api.app.get(ProcessWagerTransactionUseCase);
});
afterAll(async () => {
  await api.close();
  await closeTestOrm();
});
afterEach(() => {
  mock.restore();
});

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });

async function newWallet(amount = "100.00") {
  const playerId = newId();
  const wallet = await api.app.get(CreateWalletUseCase).execute({ playerId, initialBalance: brl(amount) });
  return { walletId: wallet.id, playerId };
}

function bet(wallet: { walletId: string; playerId: string }, amount = "10.00"): WagerTransactionInput {
  const externalTransactionId = `tx-${newId()}`;
  return {
    providerId: "provider-queue",
    externalTransactionId,
    idempotencyKey: `provider-queue:${externalTransactionId}`,
    playerId: wallet.playerId,
    walletId: wallet.walletId,
    roundId: "round-1",
    gameId: "game-1",
    kind: WagerTransactionKind.Bet,
    money: brl(amount),
    referenceExternalTransactionId: undefined,
  };
}

const fromQueue = (messageId: string) => ({ consumerName: CONSUMER, messageId });
const context = (messageId: string) => ({ correlationId: messageId, causationId: messageId });

async function inboxRows(messageId: string): Promise<any[]> {
  return sql("select * from inbox_messages where message_id = ?", [messageId]);
}

async function balanceOf(walletId: string): Promise<string> {
  const [row] = await sql("select balance_amount from wallets where id = ?", [walletId]);
  return row.balance_amount;
}

async function countBets(walletId: string): Promise<number> {
  const [{ count }] = await sql(
    "select count(*)::int as count from wager_transactions where wallet_id = ? and kind = 'BET'",
    [walletId],
  );
  return count;
}

async function errorOf(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("era esperado um erro");
}

describe("inbox: a mesma mensagem duas vezes", () => {
  test("1ª entrega processa e grava a inbox; a 2ª é DuplicateMessageError e não muda nada", async () => {
    const wallet = await newWallet("100.00");
    const input = bet(wallet, "10.00");
    const messageId = `msg-${newId()}`;

    const first = await useCase.execute(input, context(messageId), fromQueue(messageId));
    expect(first.status).toBe(WagerTransactionStatus.Processed);
    const [row] = await inboxRows(messageId);
    expect(row).toMatchObject({ consumer_name: CONSUMER, message_id: messageId });
    expect(row.payload_hash).toMatch(/^[0-9a-f]{64}$/);
    expect(row.processed_at).not.toBeNull();

    const error = await errorOf(useCase.execute(input, context(messageId), fromQueue(messageId)));

    expect(error).toBeInstanceOf(DuplicateMessageError);
    expect((error as DuplicateMessageError).code).toBe("DUPLICATE_MESSAGE");
    expect(await balanceOf(wallet.walletId)).toBe("90.00");
    expect(await countBets(wallet.walletId)).toBe(1);
    expect(await inboxRows(messageId)).toHaveLength(1);
    await assertLedgerMatchesBalance(wallet.walletId);
  });

  test("20 entregas da MESMA mensagem em paralelo → 1 processa, 19 duplicatas, debitado uma vez", async () => {
    const wallet = await newWallet("100.00");
    const input = bet(wallet, "10.00");
    const messageId = `msg-${newId()}`;

    const results = await Promise.allSettled(
      Array.from({ length: 20 }, () => useCase.execute(input, context(messageId), fromQueue(messageId))),
    );

    const fulfilled = results.filter((r) => r.status === "fulfilled");
    const rejected = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(19);
    expect(rejected.every((r) => r.reason instanceof DuplicateMessageError)).toBe(true);
    expect(await balanceOf(wallet.walletId)).toBe("90.00");
    expect(await countBets(wallet.walletId)).toBe(1);
    await assertLedgerMatchesBalance(wallet.walletId);
  });

  test("a duplicata não gera evento novo na outbox", async () => {
    const wallet = await newWallet("100.00");
    const input = bet(wallet);
    const messageId = `msg-${newId()}`;
    await useCase.execute(input, context(messageId), fromQueue(messageId));
    const countEvents = async () =>
      (await sql("select count(*)::int as count from outbox_messages where payload ->> 'causationId' = ?", [messageId]))[0]
        .count;
    const before = await countEvents();

    await errorOf(useCase.execute(input, context(messageId), fromQueue(messageId)));

    expect(before).toBe(2); // WagerTransactionProcessed + WalletBalanceChanged
    expect(await countEvents()).toBe(2);
  });

  test("o causationId dos eventos é o messageId da fila", async () => {
    const wallet = await newWallet("100.00");
    const messageId = `msg-${newId()}`;
    await useCase.execute(bet(wallet), context(messageId), fromQueue(messageId));

    const events = await sql("select payload from outbox_messages where payload ->> 'causationId' = ?", [messageId]);
    expect(events.map((e) => e.payload.eventType).sort()).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
  });
});

describe("inbox: outras mensagens, mesma aposta", () => {
  test("provedor reenviou com OUTRO messageId → inbox nova, mas a idempotency key faz replay (sem débito duplo)", async () => {
    const wallet = await newWallet("100.00");
    const input = bet(wallet, "10.00");
    const first = `msg-${newId()}`;
    const resent = `msg-${newId()}`;

    const original = await useCase.execute(input, context(first), fromQueue(first));
    const replay = await useCase.execute(input, context(resent), fromQueue(resent));

    expect(replay).toEqual({ ...original, idempotentReplay: true });
    expect(await inboxRows(resent)).toHaveLength(1);
    expect(await balanceOf(wallet.walletId)).toBe("90.00");
  });

  test("o mesmo messageId em OUTRO consumidor é independente (a chave é consumidor + mensagem)", async () => {
    const wallet = await newWallet("100.00");
    const input = bet(wallet);
    const messageId = `msg-${newId()}`;

    await useCase.execute(input, context(messageId), { consumerName: "consumer-a", messageId });
    const other = await useCase.execute(input, context(messageId), { consumerName: "consumer-b", messageId });

    expect(other.idempotentReplay).toBe(true);
    expect(await inboxRows(messageId)).toHaveLength(2);
  });
});

describe("inbox: atomicidade com a aposta", () => {
  test("erro de negócio (wallet não existe) → rollback leva a inbox junto: nada fica gravado", async () => {
    const wallet = await newWallet("100.00");
    const messageId = `msg-${newId()}`;

    await errorOf(
      useCase.execute({ ...bet(wallet), walletId: newId() }, context(messageId), fromQueue(messageId)),
    );

    expect(await inboxRows(messageId)).toHaveLength(0);
  });

  test("falha no meio da gravação (ledger) → nem aposta, nem saldo, nem inbox; a redelivery processa normalmente", async () => {
    const wallet = await newWallet("100.00");
    const input = bet(wallet, "10.00");
    const messageId = `msg-${newId()}`;
    spyOn(api.app.get(LedgerEntryRepository), "insert").mockRejectedValueOnce(new Error("o banco caiu no meio"));

    await errorOf(useCase.execute(input, context(messageId), fromQueue(messageId)));

    expect(await inboxRows(messageId)).toHaveLength(0);
    expect(await countBets(wallet.walletId)).toBe(0);
    expect(await balanceOf(wallet.walletId)).toBe("100.00");

    // A redelivery (mesma mensagem) agora funciona: a inbox não "lembrou" da falha.
    const retry = await useCase.execute(input, context(messageId), fromQueue(messageId));
    expect(retry.status).toBe(WagerTransactionStatus.Processed);
    expect(await balanceOf(wallet.walletId)).toBe("90.00");
    expect(await inboxRows(messageId)).toHaveLength(1);
    await assertLedgerMatchesBalance(wallet.walletId);
  });

  test("REJECTED (sem saldo) também é resultado final: grava a inbox", async () => {
    const wallet = await newWallet("5.00");
    const messageId = `msg-${newId()}`;

    const result = await useCase.execute(bet(wallet, "10.00"), context(messageId), fromQueue(messageId));

    expect(result.status).toBe(WagerTransactionStatus.Rejected);
    expect(await inboxRows(messageId)).toHaveLength(1);
  });

  test("pelo HTTP (sem inbox) nada é gravado na inbox", async () => {
    const wallet = await newWallet("100.00");
    const input = bet(wallet);
    const [{ before }] = await sql("select count(*)::int as before from inbox_messages");

    await useCase.execute(input);

    const [{ after }] = await sql("select count(*)::int as after from inbox_messages");
    expect(after).toBe(before);
  });
});
