import { afterAll, afterEach, beforeAll, describe, expect, mock, spyOn, test } from "bun:test";
import { ReconcileWalletUseCase } from "../../src/application/wallet/reconcile-wallet.use-case.ts";
import { Metrics } from "../../src/infrastructure/observability/metrics.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, sql } from "./support/database.ts";
import { assertLedgerMatchesBalance } from "./support/ledger.ts";
import { newId } from "./support/rows.ts";

/**
 * POST /wallets/:walletId/reconciliation (seção 9): o saldo gravado bate com o
 * saldo reconstruído pelo ledger? Divergência é logada, contada e sinalizada,
 * NUNCA corrigida.
 */

let api: TestApp;

beforeAll(async () => {
  api = await startTestApp();
});
afterAll(async () => {
  await api.close();
  await closeTestOrm();
});
afterEach(() => {
  mock.restore();
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

function submit(wallet: TestWallet, kind: string, amount: string, overrides: Record<string, unknown> = {}) {
  const body = {
    providerId: "provider-recon",
    externalTransactionId: `tx-${newId()}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: "round-1",
    gameId: "game-1",
    kind,
    money: { amount, currency: "BRL" },
    ...overrides,
  };
  return api.post("/wagering/transactions", body, { "idempotency-key": `${body.providerId}:${body.externalTransactionId}` }).then(
    (res) => ({ ...res, externalTransactionId: body.externalTransactionId }),
  );
}

const reconcile = (walletId: string) => api.post(`/wallets/${walletId}/reconciliation`, {});
const brl = (amount: string) => ({ amount, currency: "BRL" });
const metric = () => api.app.get(Metrics).walletReconciliations;

/** Simula um BUG que mexeu no saldo sem passar pelo ledger. */
async function corruptBalance(walletId: string, amount: string): Promise<void> {
  await sql("update wallets set balance_amount = ? where id = ?", [amount, walletId]);
}

describe("wallet consistente", () => {
  test("depois de BET, WIN, LOSS, REFUND, ROLLBACK e uma rejeitada → consistente, no formato da seção 9", async () => {
    const wallet = await newWallet("100.00");
    const bet = await submit(wallet, "BET", "30.00"); // 70
    await submit(wallet, "WIN", "50.00"); // 120
    await submit(wallet, "LOSS", "0.00"); // sem lançamento
    await submit(wallet, "REFUND", "30.00", { referenceExternalTransactionId: bet.externalTransactionId }); // 150
    const win = await submit(wallet, "WIN", "10.00"); // 160
    await submit(wallet, "ROLLBACK", "10.00", { referenceExternalTransactionId: win.externalTransactionId }); // 150
    expect((await submit(wallet, "BET", "999.00")).status).toBe(422); // rejeitada: sem lançamento

    const res = await reconcile(wallet.id);

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      walletId: wallet.id,
      storedBalance: brl("150.00"),
      calculatedBalance: brl("150.00"),
      difference: brl("0.00"),
      consistent: true,
      checkedEntries: 6, // abertura + BET + WIN + REFUND + WIN + ROLLBACK
    });
    await assertLedgerMatchesBalance(wallet.id); // a outra régua concorda
  });

  test("wallet criada com saldo 0 → sem lançamentos, consistente", async () => {
    const wallet = await newWallet("0.00");
    const res = await reconcile(wallet.id);
    expect(res.body).toMatchObject({ storedBalance: brl("0.00"), calculatedBalance: brl("0.00"), consistent: true, checkedEntries: 0 });
  });

  test("conta na métrica como consistent", async () => {
    const wallet = await newWallet("100.00");
    const before = metric().get({ result: "consistent" });
    await reconcile(wallet.id);
    expect(metric().get({ result: "consistent" })).toBe(before + 1);
  });

  test(
    "reconciliando NO MEIO de 40 apostas concorrentes → sempre consistente (um snapshot só, sem travar a wallet)",
    async () => {
      const wallet = await newWallet("1000.00");
      const bets = Array.from({ length: 40 }, () => submit(wallet, "BET", "5.00"));
      const reconciliations = Array.from({ length: 20 }, async (_, i) => {
        await Bun.sleep(i * 5);
        return reconcile(wallet.id);
      });

      const [betResults, reports] = await Promise.all([Promise.all(bets), Promise.all(reconciliations)]);

      expect(betResults.every((r) => r.status === 201)).toBe(true);
      for (const report of reports) {
        expect(report.status).toBe(200);
        expect(report.body.consistent).toBe(true);
        expect(report.body.difference).toEqual(brl("0.00"));
      }
      expect((await reconcile(wallet.id)).body).toMatchObject({ storedBalance: brl("800.00"), checkedEntries: 41 });
    },
    20_000,
  );
});

describe("wallet divergente", () => {
  test("saldo gravado MAIOR que o ledger → consistent false, diferença positiva", async () => {
    const wallet = await newWallet("100.00");
    await submit(wallet, "BET", "10.00"); // 90 de verdade
    await corruptBalance(wallet.id, "95.00");

    const res = await reconcile(wallet.id);

    expect(res.status).toBe(200); // a divergência está no body, a requisição deu certo
    expect(res.body).toEqual({
      walletId: wallet.id,
      storedBalance: brl("95.00"),
      calculatedBalance: brl("90.00"),
      difference: brl("5.00"),
      consistent: false,
      checkedEntries: 2,
    });
  });

  test("saldo gravado MENOR que o ledger → diferença NEGATIVA", async () => {
    const wallet = await newWallet("100.00");
    await corruptBalance(wallet.id, "60.00");

    expect((await reconcile(wallet.id)).body).toMatchObject({ difference: brl("-40.00"), consistent: false });
  });

  test("NÃO corrige: o saldo continua errado depois (corrigir esconderia o bug)", async () => {
    const wallet = await newWallet("100.00");
    await corruptBalance(wallet.id, "123.45");

    await reconcile(wallet.id);
    await reconcile(wallet.id);

    const [row] = await sql("select balance_amount from wallets where id = ?", [wallet.id]);
    expect(row.balance_amount).toBe("123.45");
    const [{ count }] = await sql("select count(*)::int as count from wallet_ledger_entries where wallet_id = ?", [wallet.id]);
    expect(count).toBe(1); // nenhum lançamento de "ajuste"
  });

  test("conta na métrica como divergent e aparece no GET /metrics", async () => {
    const wallet = await newWallet("100.00");
    await corruptBalance(wallet.id, "1.00");
    const before = metric().get({ result: "divergent" });

    await reconcile(wallet.id);

    expect(metric().get({ result: "divergent" })).toBe(before + 1);
    const text = await (await fetch(`${api.url}/metrics`)).text();
    expect(text).toContain(`wallet_reconciliations_total{result="divergent"} ${before + 1}`);
  });

  test("loga como ERRO, com walletId e os valores (para alguém investigar)", async () => {
    const wallet = await newWallet("100.00");
    await corruptBalance(wallet.id, "1.00");
    const logger = (api.app.get(ReconcileWalletUseCase) as any).logger;
    const error = spyOn(logger, "error");

    await reconcile(wallet.id);

    expect(error).toHaveBeenCalledTimes(1);
    expect(error.mock.calls[0]![0]).toMatchObject({
      walletId: wallet.id,
      consistent: false,
      storedBalance: brl("1.00"),
      calculatedBalance: brl("100.00"),
      difference: brl("-99.00"),
    });
  });

  test("consistente não loga erro", async () => {
    const wallet = await newWallet("100.00");
    const error = spyOn((api.app.get(ReconcileWalletUseCase) as any).logger, "error");
    await reconcile(wallet.id);
    expect(error).not.toHaveBeenCalled();
  });
});

describe("erros", () => {
  test("wallet que não existe → 404", async () => {
    const res = await reconcile(newId());
    expect(res.status).toBe(404);
    expect(res.body.code).toBe("WALLET_NOT_FOUND");
  });

  test("walletId que não é UUID → 400", async () => {
    const res = await reconcile("abc");
    expect(res.status).toBe(400);
    expect(res.body.code).toBe("INVALID_PAYLOAD");
  });
});
