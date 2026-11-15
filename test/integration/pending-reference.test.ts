import { afterAll, afterEach, beforeAll, describe, expect, mock, spyOn, test } from "bun:test";
import {
  ResolvePendingReferencesUseCase,
  type PendingReferenceBatchResult,
} from "../../src/application/wagering/resolve-pending-references.use-case.ts";
import { env } from "../../src/config/env.ts";
import { PendingReferenceWorker } from "../../src/infrastructure/scheduling/pending-reference.worker.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, sql } from "./support/database.ts";
import { assertLedgerMatchesBalance } from "./support/ledger.ts";
import { newId } from "./support/rows.ts";

/**
 * Worker de PENDING_REFERENCE (seção 7.1 e concorrência #7 da seção 13):
 * REFUND/ROLLBACK que chegam ANTES da transação que referenciam.
 *
 * O worker fica DESLIGADO (PENDING_REFERENCE_WORKER_ENABLED=false no .env.test):
 * cada teste chama a "volta" (execute) na mão. Só o bloco "worker" liga o loop.
 */

const PROVIDER = "provider-pending";
const LEASE_MS = 30_000;

let api: TestApp;
let instanceB: TestApp;
let instanceC: TestApp;

beforeAll(async () => {
  [api, instanceB, instanceC] = await Promise.all([startTestApp(), startTestApp(), startTestApp()]);
});
afterAll(async () => {
  await Promise.all([api.close(), instanceB.close(), instanceC.close()]);
  await closeTestOrm();
});
afterEach(() => {
  mock.restore();
});

// ============================================================================ helpers

interface TestWallet {
  id: string;
  playerId: string;
}

async function newWallet(amount = "100.00"): Promise<TestWallet> {
  const res = await api.post("/wallets", { playerId: newId(), initialBalance: { amount, currency: "BRL" } });
  expect(res.status).toBe(201);
  return { id: res.body.id, playerId: res.body.playerId };
}

function payload(wallet: TestWallet, kind: string, amount: string, overrides: Record<string, unknown> = {}) {
  return {
    providerId: PROVIDER,
    externalTransactionId: `tx-${newId()}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: "round-1",
    gameId: "game-1",
    kind,
    money: { amount, currency: "BRL" },
    ...overrides,
  };
}

type Payload = ReturnType<typeof payload>;

function submit(body: Payload, options: { headers?: Record<string, string>; app?: TestApp } = {}) {
  return (options.app ?? api).post("/wagering/transactions", body, {
    "idempotency-key": `${body.providerId}:${body.externalTransactionId}`,
    ...options.headers,
  });
}

/** Um REFUND/ROLLBACK que chega ANTES da referência → 202 PENDING_REFERENCE. */
async function submitPending(body: Payload, headers: Record<string, string> = {}): Promise<string> {
  const res = await submit(body, { headers });
  expect(res.status).toBe(202);
  expect(res.body.status).toBe("PENDING_REFERENCE");
  return res.body.transactionId;
}

const resolverOf = (app: TestApp) => app.app.get(ResolvePendingReferencesUseCase);
const resolveBatch = (app: TestApp = api) => resolverOf(app).execute(20, LEASE_MS);

async function transactionRow(id: string): Promise<any> {
  const [row] = await sql("select * from wager_transactions where id = ?", [id]);
  return row;
}

async function balanceOf(walletId: string): Promise<string> {
  const [row] = await sql("select balance_amount from wallets where id = ?", [walletId]);
  return row.balance_amount;
}

async function eventsOf(transactionId: string): Promise<any[]> {
  return sql("select payload from outbox_messages where aggregate_id = ? order by occurred_at, id", [transactionId]).then(
    (rows) => rows.map((row) => row.payload),
  );
}

/** "O tempo passou": deixa a próxima tentativa vencida. */
async function makeDue(transactionId: string): Promise<void> {
  await sql("update wager_transactions set next_attempt_at = now() - interval '1 second' where id = ?", [transactionId]);
}

/** "O TTL passou": a transação chegou há 10 minutos (o TTL padrão é 5). */
async function makeExpired(transactionId: string): Promise<void> {
  await sql(
    "update wager_transactions set created_at = now() - interval '10 minutes', next_attempt_at = now() - interval '1 second' where id = ?",
    [transactionId],
  );
}

/** Pendentes do teste, para não depender do que outros arquivos deixaram no banco. */
async function pendingCount(ids: string[]): Promise<number> {
  const [{ count }] = await sql(
    `select count(*)::int as count from wager_transactions
      where status = 'PENDING_REFERENCE' and id in (${ids.map(() => "?").join(", ")})`,
    ids,
  );
  return count;
}

function emptyResult(): PendingReferenceBatchResult {
  return { claimed: 0, processed: 0, rejected: 0, rescheduled: 0, skipped: 0, failed: 0 };
}

function add(total: PendingReferenceBatchResult, part: PendingReferenceBatchResult): PendingReferenceBatchResult {
  for (const key of Object.keys(total) as (keyof PendingReferenceBatchResult)[]) total[key] += part[key];
  return total;
}

async function waitUntil(check: () => Promise<boolean> | boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("timeout esperando a condição");
    await Bun.sleep(25);
  }
}

// ============================================================================ testes

describe("a referência chega depois", () => {
  test("REFUND antes da BET → 202; a BET chega e ACORDA o REFUND; uma volta → PROCESSED, saldo devolvido", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refundId = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }));

    // Antes da BET: agendado para ~1s no futuro (1ª tentativa do backoff).
    expect(new Date((await transactionRow(refundId)).next_attempt_at).getTime()).toBeGreaterThan(Date.now());

    const betRes = await submit(bet);
    expect(betRes.status).toBe(201);
    // A BET acordou o REFUND: vencido AGORA, sem esperar o backoff.
    expect(new Date((await transactionRow(refundId)).next_attempt_at).getTime()).toBeLessThanOrEqual(Date.now());

    const result = await resolveBatch();

    expect(result).toMatchObject({ claimed: 1, processed: 1, rejected: 0, failed: 0 });
    const refund = await transactionRow(refundId);
    expect(refund).toMatchObject({
      status: "PROCESSED",
      reference_transaction_id: betRes.body.transactionId,
      balance_after_amount: "100.00",
      failure_code: null,
      attempts: 1,
      next_attempt_at: null,
    });
    expect(refund.processed_at).not.toBeNull();
    expect(await balanceOf(wallet.id)).toBe("100.00"); // 100 − 10 (BET) + 10 (REFUND)
    await assertLedgerMatchesBalance(wallet.id);
  });

  test("ROLLBACK de um WIN que ainda não chegou → o WIN chega → o ROLLBACK DEBITA de volta", async () => {
    const wallet = await newWallet("100.00");
    const win = payload(wallet, "WIN", "20.00");
    const rollbackId = await submitPending(payload(wallet, "ROLLBACK", "20.00", { referenceExternalTransactionId: win.externalTransactionId }));

    await submit(win);
    expect(await balanceOf(wallet.id)).toBe("120.00");

    expect((await resolveBatch()).processed).toBe(1);
    expect((await transactionRow(rollbackId)).status).toBe("PROCESSED");
    expect(await balanceOf(wallet.id)).toBe("100.00");
    await assertLedgerMatchesBalance(wallet.id);
  });

  test("os eventos da resolução continuam a história do pedido ORIGINAL (correlationId e causa)", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refundId = await submitPending(
      payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }),
      { "x-correlation-id": "corr-refund-original" },
    );
    await submit(bet, { headers: { "x-correlation-id": "corr-bet" } });

    await resolveBatch();

    const events = await eventsOf(refundId);
    expect(events.map((e) => e.eventType)).toEqual([
      "WagerTransactionPendingReference", // quando chegou
      "WagerTransactionProcessed", // quando o worker resolveu
    ]);
    const [pendingEvent, processedEvent] = events;
    expect(processedEvent.correlationId).toBe("corr-refund-original");
    expect(processedEvent.causationId).toBe(pendingEvent.eventId);
    expect(processedEvent.data).toMatchObject({ transactionId: refundId, kind: "REFUND", balanceAfter: { amount: "100.00", currency: "BRL" } });

    // O saldo mudou → também sai WalletBalanceChanged (o aggregate é a wallet).
    const [balanceEvent] = await sql(
      "select payload from outbox_messages where event_type = 'WalletBalanceChanged' and payload -> 'data' ->> 'transactionId' = ?",
      [refundId],
    );
    expect(balanceEvent.payload.correlationId).toBe("corr-refund-original");
  });

  test("depois de resolvido, o replay do REFUND devolve o resultado FINAL (200, PROCESSED, saldo daquele momento)", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refund = payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId });
    const refundId = await submitPending(refund);
    await submit(bet);
    await resolveBatch();

    const replay = await submit(refund);

    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({
      transactionId: refundId,
      status: "PROCESSED",
      balance: { amount: "100.00", currency: "BRL" },
      idempotentReplay: true,
    });
    const get = await api.get(`/wagering/transactions/${refundId}`);
    expect(get.body.status).toBe("PROCESSED");
  });

  test("cadeia: ROLLBACK de um REFUND que espera uma BET → a BET destrava o REFUND, que destrava o ROLLBACK", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refund = payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId });
    const rollbackId = await submitPending(payload(wallet, "ROLLBACK", "10.00", { referenceExternalTransactionId: refund.externalTransactionId }));
    const refundId = await submitPending(refund);
    await submit(bet);

    const first = await resolveBatch(); // resolve o REFUND (o ROLLBACK ainda não venceu)
    expect((await transactionRow(refundId)).status).toBe("PROCESSED");
    expect(first.processed).toBeGreaterThanOrEqual(1);
    // O REFUND resolvido acordou o ROLLBACK.
    expect(new Date((await transactionRow(rollbackId)).next_attempt_at).getTime()).toBeLessThanOrEqual(Date.now());

    await resolveBatch();
    expect((await transactionRow(rollbackId)).status).toBe("PROCESSED");
    expect(await balanceOf(wallet.id)).toBe("90.00"); // −10 BET, +10 REFUND, −10 ROLLBACK do REFUND
    await assertLedgerMatchesBalance(wallet.id);
  });
});

describe("a referência chega, mas a regra recusa → REJECTED (as mesmas regras da entrada normal)", () => {
  test("valor diferente da BET → AMOUNT_MISMATCH", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refundId = await submitPending(payload(wallet, "REFUND", "15.00", { referenceExternalTransactionId: bet.externalTransactionId }));
    await submit(bet);

    expect(await resolveBatch()).toMatchObject({ processed: 0, rejected: 1 });
    expect(await transactionRow(refundId)).toMatchObject({ status: "REJECTED", failure_code: "AMOUNT_MISMATCH", balance_after_amount: null });
    expect(await balanceOf(wallet.id)).toBe("90.00");
    expect((await eventsOf(refundId)).at(-1)).toMatchObject({ eventType: "WagerTransactionRejected", data: { failureCode: "AMOUNT_MISMATCH" } });
  });

  test("REFUND de um WIN → INVALID_REFERENCE_KIND", async () => {
    const wallet = await newWallet("100.00");
    const win = payload(wallet, "WIN", "10.00");
    const refundId = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: win.externalTransactionId }));
    await submit(win);

    await resolveBatch();
    expect(await transactionRow(refundId)).toMatchObject({ status: "REJECTED", failure_code: "INVALID_REFERENCE_KIND" });
  });

  test("ROLLBACK que deixaria o saldo negativo → REVERSAL_INSUFFICIENT_FUNDS (e não INSUFFICIENT_FUNDS)", async () => {
    const wallet = await newWallet("100.00");
    const win = payload(wallet, "WIN", "50.00");
    const rollbackId = await submitPending(payload(wallet, "ROLLBACK", "50.00", { referenceExternalTransactionId: win.externalTransactionId }));
    await submit(win); // 150
    await submit(payload(wallet, "BET", "140.00")); // 10: não dá para desfazer o WIN de 50

    await resolveBatch();
    expect(await transactionRow(rollbackId)).toMatchObject({ status: "REJECTED", failure_code: "REVERSAL_INSUFFICIENT_FUNDS" });
    expect(await balanceOf(wallet.id)).toBe("10.00");
    await assertLedgerMatchesBalance(wallet.id);
  });

  test("REFUND e ROLLBACK da MESMA BET, os dois esperando → um reverte, o outro é ALREADY_REVERSED (sem devolver 2×)", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refundId = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }));
    const rollbackId = await submitPending(payload(wallet, "ROLLBACK", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }));
    await submit(bet);

    expect(await resolveBatch()).toMatchObject({ processed: 1, rejected: 1 });
    const statuses = [await transactionRow(refundId), await transactionRow(rollbackId)].map((r) => [r.status, r.failure_code]);
    expect(statuses).toContainEqual(["PROCESSED", null]);
    expect(statuses).toContainEqual(["REJECTED", "ALREADY_REVERSED"]);
    expect(await balanceOf(wallet.id)).toBe("100.00");
    await assertLedgerMatchesBalance(wallet.id);
  });
});

describe("a referência não chega: backoff e TTL", () => {
  test("antes da hora (1ª tentativa em ~1s) → não é pega", async () => {
    const wallet = await newWallet("100.00");
    const id = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: `nunca-${newId()}` }));

    await resolveBatch();

    expect(await transactionRow(id)).toMatchObject({ status: "PENDING_REFERENCE", attempts: 0 });
  });

  test("vencida e ainda sem referência → conta a tentativa e agenda com backoff crescente (2s, 4s...)", async () => {
    const wallet = await newWallet("100.00");
    const id = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: `nunca-${newId()}` }));

    await makeDue(id);
    let before = Date.now();
    expect((await resolveBatch()).rescheduled).toBeGreaterThanOrEqual(1);
    let row = await transactionRow(id);
    expect(row).toMatchObject({ status: "PENDING_REFERENCE", attempts: 1 });
    expect(new Date(row.next_attempt_at).getTime()).toBeGreaterThanOrEqual(before + 2_000);

    await makeDue(id);
    before = Date.now();
    await resolveBatch();
    row = await transactionRow(id);
    expect(row.attempts).toBe(2);
    expect(new Date(row.next_attempt_at).getTime()).toBeGreaterThanOrEqual(before + 4_000);

    // Reagendar não gera evento novo: só o PendingReference de quando chegou.
    expect((await eventsOf(id)).map((e) => e.eventType)).toEqual(["WagerTransactionPendingReference"]);
  });

  test("TTL vencido e sem referência → REJECTED REFERENCE_NOT_FOUND, evento publicado, saldo intacto", async () => {
    const wallet = await newWallet("100.00");
    const refund = payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: `nunca-${newId()}` });
    const id = await submitPending(refund);

    await makeExpired(id);
    expect((await resolveBatch()).rejected).toBeGreaterThanOrEqual(1);

    expect(await transactionRow(id)).toMatchObject({ status: "REJECTED", failure_code: "REFERENCE_NOT_FOUND", next_attempt_at: null });
    expect(await balanceOf(wallet.id)).toBe("100.00");
    expect((await eventsOf(id)).at(-1)).toMatchObject({
      eventType: "WagerTransactionRejected",
      data: { transactionId: id, failureCode: "REFERENCE_NOT_FOUND" },
    });
    // O provedor reenvia → o resultado final, 422.
    const replay = await submit(refund);
    expect(replay.status).toBe(422);
    expect(replay.body).toMatchObject({ status: "REJECTED", failureCode: "REFERENCE_NOT_FOUND", idempotentReplay: true });
  });

  test("a referência apareceu DEPOIS do TTL, antes da próxima tentativa → processa (o TTL só rejeita quem continua sem referência)", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const id = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }));
    await makeExpired(id);
    await submit(bet);

    await resolveBatch();
    expect((await transactionRow(id)).status).toBe("PROCESSED");
  });

  test("transação que nunca esteve pendente (BET) não é afetada pelo worker", async () => {
    const wallet = await newWallet("100.00");
    const res = await submit(payload(wallet, "BET", "10.00"));
    await resolveBatch();
    expect(await transactionRow(res.body.transactionId)).toMatchObject({ status: "PROCESSED", attempts: 0, next_attempt_at: null });
  });
});

describe("vários workers e falhas", () => {
  test(
    "3 instâncias em paralelo sobre 15 wallets com REFUND pendente → cada um resolvido EXATAMENTE uma vez",
    async () => {
      const cases: { walletId: string; refundId: string }[] = [];
      for (let i = 0; i < 15; i++) {
        const wallet = await newWallet("100.00");
        const bet = payload(wallet, "BET", "10.00");
        const refundId = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }));
        await submit(bet);
        cases.push({ walletId: wallet.id, refundId });
      }
      const ids = cases.map((c) => c.refundId);

      const total = emptyResult();
      const deadline = Date.now() + 10_000;
      await Promise.all(
        [api, instanceB, instanceC, api, instanceB, instanceC].map(async (app) => {
          while ((await pendingCount(ids)) > 0 && Date.now() < deadline) add(total, await resolverOf(app).execute(3, LEASE_MS));
        }),
      );

      expect(total.processed).toBe(15);
      expect(total.failed).toBe(0);
      for (const { walletId, refundId } of cases) {
        expect((await transactionRow(refundId)).status).toBe("PROCESSED");
        expect(await balanceOf(walletId)).toBe("100.00");
        await assertLedgerMatchesBalance(walletId);
        // Um único crédito de REFUND por wallet: abertura + BET + REFUND.
        const [{ count }] = await sql("select count(*)::int as count from wallet_ledger_entries where wallet_id = ?", [walletId]);
        expect(count).toBe(3);
      }
    },
    20_000,
  );

  test(
    "BETs chegando AO MESMO TEMPO que os workers resolvem, tudo na mesma wallet → sem deadlock, saldo exato",
    async () => {
      const wallet = await newWallet("100.00");
      const bets = Array.from({ length: 10 }, () => payload(wallet, "BET", "5.00"));
      const refundIds: string[] = [];
      for (const bet of bets) {
        refundIds.push(await submitPending(payload(wallet, "REFUND", "5.00", { referenceExternalTransactionId: bet.externalTransactionId })));
      }
      // Todos vencidos já: os workers vão disputar a wallet com as BETs chegando.
      for (const id of refundIds) await makeDue(id);

      const total = emptyResult();
      const deadline = Date.now() + 15_000;
      const workers = [api, instanceB, instanceC].map(async (app) => {
        while ((await pendingCount(refundIds)) > 0 && Date.now() < deadline) {
          add(total, await resolverOf(app).execute(5, LEASE_MS));
          await Bun.sleep(20);
        }
      });
      const arrivals = Promise.all(bets.map((bet, i) => submit(bet, { app: [api, instanceB, instanceC][i % 3] })));
      const [betResults] = await Promise.all([arrivals, ...workers]);

      expect(betResults.every((r) => r.status === 201)).toBe(true);
      expect(total.failed).toBe(0);
      expect(await pendingCount(refundIds)).toBe(0);
      for (const id of refundIds) expect((await transactionRow(id)).status).toBe("PROCESSED");
      expect(await balanceOf(wallet.id)).toBe("100.00"); // 10 × (−5 +5)
      await assertLedgerMatchesBalance(wallet.id);
    },
    25_000,
  );

  test("o worker morre no meio → o item fica reservado (lease) e outro worker só pega quando a lease vence", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refundId = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }));
    await submit(bet);

    // A instância A reserva o item e "morre" ao resolver.
    spyOn(resolverOf(api), "resolve").mockRejectedValueOnce(new Error("processo morreu"));
    const before = Date.now();
    expect(await resolveBatch(api)).toMatchObject({ claimed: 1, failed: 1, processed: 0 });

    const row = await transactionRow(refundId);
    expect(row.status).toBe("PENDING_REFERENCE");
    expect(new Date(row.next_attempt_at).getTime()).toBeGreaterThanOrEqual(before + LEASE_MS - 1_000);

    // Enquanto a lease vale, a B não pega (ninguém processa duas vezes ao mesmo tempo).
    expect((await resolveBatch(instanceB)).claimed).toBe(0);

    // A lease venceu → a B assume.
    await makeDue(refundId);
    expect(await resolveBatch(instanceB)).toMatchObject({ claimed: 1, processed: 1 });
    expect(await balanceOf(wallet.id)).toBe("100.00");
  });

  test("dois workers com o MESMO item (lease vencida no meio) → o 2º vê que já foi resolvido e pula", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refundId = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }));
    await submit(bet);
    const ticket = { transactionId: refundId, walletId: wallet.id };

    const outcomes = await Promise.all([resolverOf(api).resolve(ticket), resolverOf(instanceB).resolve(ticket)]);

    expect(outcomes.sort()).toEqual(["processed", "skipped"]);
    expect(await balanceOf(wallet.id)).toBe("100.00");
    await assertLedgerMatchesBalance(wallet.id);
  });
});

describe("worker (o loop de verdade)", () => {
  const pendingEnv = env.pendingReference as { workerEnabled: boolean };

  test("ligado → a BET chega e o REFUND pendente é resolvido sozinho; no shutdown para de agendar", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "10.00");
    const refundId = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet.externalTransactionId }));

    const worker = api.app.get(PendingReferenceWorker);
    pendingEnv.workerEnabled = true;
    try {
      worker.onApplicationBootstrap();
      await submit(bet);
      await waitUntil(async () => (await transactionRow(refundId)).status === "PROCESSED", 5_000);
    } finally {
      await worker.beforeApplicationShutdown();
      pendingEnv.workerEnabled = false;
    }
    expect(await balanceOf(wallet.id)).toBe("100.00");

    // Parado: um novo par REFUND → BET fica pendente.
    const bet2 = payload(wallet, "BET", "10.00");
    const refund2 = await submitPending(payload(wallet, "REFUND", "10.00", { referenceExternalTransactionId: bet2.externalTransactionId }));
    await submit(bet2);
    await Bun.sleep(env.pendingReference.pollIntervalMs + 300);
    expect((await transactionRow(refund2)).status).toBe("PENDING_REFERENCE");
  });
});
