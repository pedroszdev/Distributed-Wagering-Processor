import { afterAll, beforeAll, describe, expect, spyOn, test } from "bun:test";
import { WalletRepository } from "../../src/infrastructure/database/repositories/wallet.repository.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, getTestOrm, sql } from "./support/database.ts";
import { assertLedgerMatchesBalance } from "./support/ledger.ts";
import { newId } from "./support/rows.ts";

/**
 * Concorrência #3 (seção 13): wallets DISTINTAS processadas em paralelo.
 *
 * A unidade de concorrência é a walletId (seção 8) e a restrição 6 da seção 5
 * proíbe "lock global compartilhado por todas as wallets". Aqui se prova que o
 * lock de uma wallet não segura as outras, e que muitas wallets em paralelo, em
 * várias instâncias, terminam cada uma com o saldo exato.
 */

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

interface TestWallet {
  id: string;
  playerId: string;
}

async function newWallet(amount = "100.00"): Promise<TestWallet> {
  const res = await api.post("/wallets", { playerId: newId(), initialBalance: { amount, currency: "BRL" } });
  expect(res.status).toBe(201);
  return { id: res.body.id, playerId: res.body.playerId };
}

function submitBet(wallet: TestWallet, amount: string, app: TestApp = api) {
  const externalTransactionId = `tx-${newId()}`;
  return app.post(
    "/wagering/transactions",
    {
      providerId: "provider-distinct",
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: "round-1",
      gameId: "game-1",
      kind: "BET",
      money: { amount, currency: "BRL" },
    },
    { "idempotency-key": `provider-distinct:${externalTransactionId}` },
  );
}

async function balanceOf(walletId: string): Promise<string> {
  const [row] = await sql("select balance_amount from wallets where id = ?", [walletId]);
  return row.balance_amount;
}

const TIMED_OUT = "TIMED_OUT" as const;
function withinTimeout<T>(promise: Promise<T>, ms: number): Promise<T | typeof TIMED_OUT> {
  return Promise.race([promise, Bun.sleep(ms).then(() => TIMED_OUT)]);
}

describe("concorrência #3: wallets distintas em paralelo", () => {
  test("com a wallet A TRAVADA (FOR UPDATE aberto), uma aposta na wallet B termina na hora: não existe lock global", async () => {
    const walletA = await newWallet();
    const walletB = await newWallet();
    const orm = await getTestOrm();

    let release!: () => void;
    const hold = new Promise<void>((resolve) => (release = resolve));
    let locked = false;
    // "Outra instância" segurando o lock da wallet A.
    const holder = orm.em.fork().transactional(async (tx) => {
      await tx.getConnection().execute("select id from wallets where id = ? for update", [walletA.id], "all", tx.getTransactionContext());
      locked = true;
      await hold;
    });
    while (!locked) await Bun.sleep(10);

    try {
      // B: não depende de A → responde já.
      const startedAt = Date.now();
      const onB = await withinTimeout(submitBet(walletB, "10.00"), 2_000);
      expect(onB).not.toBe(TIMED_OUT);
      expect((onB as { status: number }).status).toBe(201);
      expect(Date.now() - startedAt).toBeLessThan(1_000);

      // A: espera o lock (não falha, não passa por cima).
      const onA = submitBet(walletA, "10.00");
      expect(await withinTimeout(onA, 500)).toBe(TIMED_OUT);
      release();
      expect((await onA).status).toBe(201);
    } finally {
      release();
      await holder;
    }
    expect(await balanceOf(walletA.id)).toBe("90.00");
    expect(await balanceOf(walletB.id)).toBe("90.00");
  });

  test(
    "30 wallets × 10 apostas, tudo ao mesmo tempo em 3 instâncias → cada wallet com o saldo exato e o ledger íntegro",
    async () => {
      const wallets = await Promise.all(Array.from({ length: 30 }, () => newWallet("50.00")));
      const instances = [api, instanceB, instanceC];

      // 10 × 6.00 = 60.00 > 50.00 → em CADA wallet, exatamente 8 passam e 2 são rejeitadas.
      const requests = wallets.flatMap((wallet, w) =>
        Array.from({ length: 10 }, (_, i) => submitBet(wallet, "6.00", instances[(w + i) % 3])),
      );
      const responses = await Promise.all(requests);

      expect(responses.every((r) => r.status === 201 || r.status === 422)).toBe(true);
      for (const wallet of wallets) {
        const statuses = await sql(
          "select status, count(*)::int as count from wager_transactions where wallet_id = ? and kind = 'BET' group by status",
          [wallet.id],
        );
        expect(Object.fromEntries(statuses.map((s) => [s.status, s.count]))).toEqual({ PROCESSED: 8, REJECTED: 2 });
        expect(await balanceOf(wallet.id)).toBe("2.00"); // 50 − 8 × 6
        await assertLedgerMatchesBalance(wallet.id);
      }
    },
    30_000,
  );

  test(
    "wallets distintas NÃO fazem fila uma atrás da outra; a MESMA wallet faz (grupo de controle)",
    async () => {
      // Cada aposta fica ~150ms DENTRO do lock da wallet (gravação lenta simulada).
      const repository = api.app.get(WalletRepository);
      const original = repository.update.bind(repository);
      const spy = spyOn(repository, "update").mockImplementation(async (...args) => {
        await Bun.sleep(150);
        return original(...args);
      });
      try {
        // 10 wallets diferentes: os locks são independentes → ~150ms no total.
        const distinct = await Promise.all(Array.from({ length: 10 }, () => newWallet()));
        let startedAt = Date.now();
        await Promise.all(distinct.map((wallet) => submitBet(wallet, "1.00")));
        const distinctTook = Date.now() - startedAt;

        // Controle: 10 apostas na MESMA wallet → uma atrás da outra → ≥ 10 × 150ms.
        const same = await newWallet();
        startedAt = Date.now();
        await Promise.all(Array.from({ length: 10 }, () => submitBet(same, "1.00")));
        const sameTook = Date.now() - startedAt;

        expect(sameTook).toBeGreaterThanOrEqual(10 * 150);
        expect(distinctTook).toBeLessThan(sameTook / 3);
        expect(distinctTook).toBeLessThan(1_000);
      } finally {
        spy.mockRestore();
      }
    },
    15_000,
  );
});
