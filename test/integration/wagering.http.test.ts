import { afterAll, beforeAll, describe, expect, spyOn, test } from "bun:test";
import { ProcessWagerTransactionUseCase } from "../../src/application/wagering/process-wager-transaction.use-case.ts";
import { GetWalletUseCase } from "../../src/application/wallet/get-wallet.use-case.ts";
import { startTestApp, type HttpResult, type TestApp } from "./support/app.ts";
import { closeTestOrm, sql } from "./support/database.ts";
import { assertLedgerMatchesBalance } from "./support/ledger.ts";
import { newId } from "./support/rows.ts";

/**
 * A API de wagering de ponta a ponta: HTTP real → Nest → use case → Postgres real.
 *
 * O foco aqui é o CONTRATO HTTP (status, headers, formato do body). As regras de
 * negócio em si já estão cobertas em process-wager-transaction.test.ts; aqui só
 * conferimos que cada resultado chega ao provedor com o status certo:
 *
 *   201 nova · 200 replay · 202 pendente · 422 rejeitada
 *   400 payload inválido · 404 não existe · 409 conflito · 503 transitório
 */

let api: TestApp;

beforeAll(async () => {
  api = await startTestApp();
});
afterAll(async () => {
  await api.close();
  await closeTestOrm();
});

const PROVIDER = "provider-http";
const UNKNOWN_ID = "01a1db3a-0000-7000-8000-000000000000";

interface TestWallet {
  id: string;
  playerId: string;
}

async function newWallet(amount = "100.00"): Promise<TestWallet> {
  const res = await api.post("/wallets", { playerId: newId(), initialBalance: { amount, currency: "BRL" } });
  expect(res.status).toBe(201);
  return { id: res.body.id, playerId: res.body.playerId };
}

/** Um body válido de transação. Cada chamada gera um externalTransactionId novo. */
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

/** POST com a Idempotency-Key padrão "<provider>:<externalId>". */
function submit(body: Payload, options: { key?: string; headers?: Record<string, string>; app?: TestApp } = {}) {
  const key = options.key ?? `${body.providerId}:${body.externalTransactionId}`;
  return (options.app ?? api).post("/wagering/transactions", body, { "idempotency-key": key, ...options.headers });
}

async function balanceOf(walletId: string): Promise<string> {
  const [row] = await sql("select balance_amount from wallets where id = ?", [walletId]);
  return row.balance_amount;
}

async function countTransactions(walletId: string): Promise<number> {
  const [{ count }] = await sql(
    "select count(*)::int as count from wager_transactions where wallet_id = ? and kind <> 'OPENING'",
    [walletId],
  );
  return count;
}

async function countLedgerEntries(walletId: string): Promise<number> {
  const [{ count }] = await sql("select count(*)::int as count from wallet_ledger_entries where wallet_id = ?", [walletId]);
  return count;
}

/** Quantas respostas vieram com cada status: { 201: 50, 422: 30 }. */
function statusCount(results: HttpResult[]): Record<number, number> {
  const counts: Record<number, number> = {};
  for (const { status } of results) counts[status] = (counts[status] ?? 0) + 1;
  return counts;
}

// ---------------------------------------------------------------------------

describe("POST /wagering/transactions: status por resultado", () => {
  test("BET nova → 201 PROCESSED com o saldo novo", async () => {
    const wallet = await newWallet("100.00");

    const res = await submit(payload(wallet, "BET", "30.00"));

    expect(res.status).toBe(201);
    expect(res.body).toEqual({
      transactionId: expect.any(String),
      status: "PROCESSED",
      balance: { amount: "70.00", currency: "BRL" },
      idempotentReplay: false,
    });
    expect(res.body).not.toHaveProperty("failureCode");
    expect(await balanceOf(wallet.id)).toBe("70.00");
  });

  test("mesmo pedido de novo → 200 replay, mesma transação, sem debitar de novo", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "BET", "30.00");
    const first = await submit(body);

    const replay = await submit(body);

    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });
    expect(await balanceOf(wallet.id)).toBe("70.00");
    expect(await countTransactions(wallet.id)).toBe(1);
    expect(await countLedgerEntries(wallet.id)).toBe(2); // abertura + BET
  });

  test("WIN → 201 e credita", async () => {
    const wallet = await newWallet("100.00");
    const res = await submit(payload(wallet, "WIN", "25.00"));

    expect(res.status).toBe(201);
    expect(res.body.balance).toEqual({ amount: "125.00", currency: "BRL" });
  });

  test("LOSS → 201 PROCESSED e o saldo não muda", async () => {
    const wallet = await newWallet("100.00");
    const res = await submit(payload(wallet, "LOSS", "30.00"));

    expect(res.status).toBe(201);
    expect(res.body.status).toBe("PROCESSED");
    expect(res.body.balance).toEqual({ amount: "100.00", currency: "BRL" });
    expect(await countLedgerEntries(wallet.id)).toBe(1); // só a abertura
  });

  test("sem saldo → 422 REJECTED INSUFFICIENT_FUNDS, saldo intacto", async () => {
    const wallet = await newWallet("100.00");

    const res = await submit(payload(wallet, "BET", "100.01"));

    expect(res.status).toBe(422);
    expect(res.body).toEqual({
      transactionId: expect.any(String),
      status: "REJECTED",
      balance: { amount: "100.00", currency: "BRL" },
      idempotentReplay: false,
      failureCode: "INSUFFICIENT_FUNDS",
    });
    expect(await balanceOf(wallet.id)).toBe("100.00");
  });

  test("replay de uma rejeitada → 422 de novo (a resposta é sempre a mesma), com idempotentReplay", async () => {
    const wallet = await newWallet("10.00");
    const body = payload(wallet, "BET", "50.00");
    await submit(body);

    const replay = await submit(body);

    expect(replay.status).toBe(422);
    expect(replay.body.idempotentReplay).toBe(true);
    expect(replay.body.failureCode).toBe("INSUFFICIENT_FUNDS");
  });

  test("REFUND de referência que ainda não chegou → 202 PENDING_REFERENCE; o replay também é 202", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "REFUND", "30.00", { referenceExternalTransactionId: `tx-ainda-nao-chegou-${newId()}` });

    const res = await submit(body);
    const replay = await submit(body);

    expect(res.status).toBe(202);
    expect(res.body.status).toBe("PENDING_REFERENCE");
    expect(res.body.balance).toEqual({ amount: "100.00", currency: "BRL" });
    expect(replay.status).toBe(202);
    expect(replay.body.idempotentReplay).toBe(true);
  });

  test("REFUND de uma BET processada → 201 e devolve o valor", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "30.00");
    await submit(bet);

    const res = await submit(payload(wallet, "REFUND", "30.00", { referenceExternalTransactionId: bet.externalTransactionId }));

    expect(res.status).toBe(201);
    expect(res.body.balance).toEqual({ amount: "100.00", currency: "BRL" });
    await assertLedgerMatchesBalance(wallet.id);
  });
});

describe("POST /wagering/transactions: erros", () => {
  test("mesma Idempotency-Key com payload diferente → 409 IDEMPOTENCY_CONFLICT, nada muda", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "BET", "30.00");
    await submit(body);

    const res = await submit({ ...body, money: { amount: "31.00", currency: "BRL" } });

    expect(res.status).toBe(409);
    expect(res.body.code).toBe("IDEMPOTENCY_CONFLICT");
    expect(await balanceOf(wallet.id)).toBe("70.00");
    expect(await countTransactions(wallet.id)).toBe(1);
  });

  test("mesmo externalTransactionId com OUTRA key → 409", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "BET", "30.00");
    await submit(body);

    const res = await submit(body, { key: `outra-key-${newId()}` });

    expect(res.status).toBe(409);
    expect(res.body.code).toBe("IDEMPOTENCY_CONFLICT");
    expect(await countTransactions(wallet.id)).toBe(1);
  });

  test("sem o header Idempotency-Key → 400 INVALID_PAYLOAD", async () => {
    const wallet = await newWallet();
    const res = await api.post("/wagering/transactions", payload(wallet, "BET", "1.00"));

    expect(res.status).toBe(400);
    expect(res.body.code).toBe("INVALID_PAYLOAD");
  });

  test.each([
    ["kind inválido", { kind: "JACKPOT" }],
    ["kind OPENING (interno)", { kind: "OPENING" }],
    ["valor negativo", { money: { amount: "-1.00", currency: "BRL" } }],
    ["valor zero numa BET", { money: { amount: "0.00", currency: "BRL" } }],
    ["playerId que não é UUID", { playerId: "player-1" }],
    ["BET com referência", { referenceExternalTransactionId: "tx-qualquer" }],
  ])("%s → 400", async (_label, overrides) => {
    const wallet = await newWallet();
    const res = await submit(payload(wallet, "BET", "1.00", overrides));

    expect(res.status).toBe(400);
    expect(await countTransactions(wallet.id)).toBe(0);
  });

  test("REFUND sem referência → 400 MISSING_REFERENCE", async () => {
    const wallet = await newWallet();
    const res = await submit(payload(wallet, "REFUND", "1.00"));

    expect(res.status).toBe(400);
    expect(res.body.code).toBe("MISSING_REFERENCE");
  });

  test("body que não é JSON válido → 400", async () => {
    const res = await fetch(`${api.url}/wagering/transactions`, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": "k" },
      body: "{isso não é json",
    });
    expect(res.status).toBe(400);
  });

  test("wallet que não existe → 404 WALLET_NOT_FOUND", async () => {
    const res = await submit(payload({ id: UNKNOWN_ID, playerId: newId() }, "BET", "1.00"));

    expect(res.status).toBe(404);
    expect(res.body.code).toBe("WALLET_NOT_FOUND");
  });

  test("player que não é dono da wallet → 400 WALLET_PLAYER_MISMATCH", async () => {
    const wallet = await newWallet();
    const res = await submit(payload(wallet, "BET", "1.00", { playerId: newId() }));

    expect(res.status).toBe(400);
    expect(res.body.code).toBe("WALLET_PLAYER_MISMATCH");
  });

  test("moeda diferente da wallet → 400 CURRENCY_MISMATCH", async () => {
    const wallet = await newWallet();
    const res = await submit(payload(wallet, "BET", "1.00", { money: { amount: "1.00", currency: "USD" } }));

    expect(res.status).toBe(400);
    expect(res.body.code).toBe("CURRENCY_MISMATCH");
  });
});

describe("x-correlation-id", () => {
  async function correlationIdsOf(transactionId: string): Promise<string[]> {
    const rows = await sql(
      "select payload->>'correlationId' as correlation_id from outbox_messages where payload->'data'->>'transactionId' = ?",
      [transactionId],
    );
    return rows.map((r) => r.correlation_id);
  }

  test("o id enviado volta no header e vai para TODOS os eventos da transação", async () => {
    const wallet = await newWallet();

    const res = await submit(payload(wallet, "BET", "10.00"), { headers: { "x-correlation-id": "corr-do-provedor-1" } });

    expect(res.headers.get("x-correlation-id")).toBe("corr-do-provedor-1");
    expect(await correlationIdsOf(res.body.transactionId)).toEqual(["corr-do-provedor-1", "corr-do-provedor-1"]);
  });

  test("sem header: gera um UUID, devolve no header e usa nos eventos", async () => {
    const wallet = await newWallet();

    const res = await submit(payload(wallet, "BET", "10.00"));

    const generated = res.headers.get("x-correlation-id");
    expect(generated).toMatch(/^[0-9a-f-]{36}$/);
    expect(await correlationIdsOf(res.body.transactionId)).toEqual([generated!, generated!]);
  });

  test("também volta nas respostas de erro (é quando o cliente mais precisa dele)", async () => {
    const wallet = await newWallet();

    const res = await api.post("/wagering/transactions", payload(wallet, "BET", "1.00"), {
      "x-correlation-id": "corr-do-erro",
    }); // sem Idempotency-Key → 400

    expect(res.status).toBe(400);
    expect(res.headers.get("x-correlation-id")).toBe("corr-do-erro");
  });
});

// ---------------------------------------------------------------------------

describe("GET /wagering/transactions/:transactionId e /providers/:providerId/wagering/transactions/:externalTransactionId", () => {
  test("BET processada: a transação completa, igual pelos dois caminhos", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "BET", "30.00");
    const { body: created } = await submit(body);

    const byId = await api.get(`/wagering/transactions/${created.transactionId}`);
    const byExternal = await api.get(`/providers/${PROVIDER}/wagering/transactions/${body.externalTransactionId}`);

    expect(byId.status).toBe(200);
    expect(byId.body).toEqual({
      id: created.transactionId,
      providerId: PROVIDER,
      externalTransactionId: body.externalTransactionId,
      idempotencyKey: `${PROVIDER}:${body.externalTransactionId}`,
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: "round-1",
      gameId: "game-1",
      kind: "BET",
      money: { amount: "30.00", currency: "BRL" },
      status: "PROCESSED",
      referenceExternalTransactionId: null,
      referenceTransactionId: null,
      failureCode: null,
      balanceAfter: { amount: "70.00", currency: "BRL" },
      createdAt: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T.*Z$/),
      processedAt: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T.*Z$/),
    });
    expect(byId.body).not.toHaveProperty("payloadHash"); // detalhe interno
    expect(byExternal.status).toBe(200);
    expect(byExternal.body).toEqual(byId.body);
  });

  test("rejeitada → 200 (a BUSCA deu certo), com status REJECTED e o failureCode", async () => {
    const wallet = await newWallet("10.00");
    const { body: created } = await submit(payload(wallet, "BET", "50.00"));

    const res = await api.get(`/wagering/transactions/${created.transactionId}`);

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      status: "REJECTED",
      failureCode: "INSUFFICIENT_FUNDS",
      balanceAfter: null,
      processedAt: null,
    });
  });

  test("pendente → 200 com a referência externa e sem a interna", async () => {
    const wallet = await newWallet();
    const reference = `tx-futura-${newId()}`;
    const { body: created } = await submit(payload(wallet, "REFUND", "5.00", { referenceExternalTransactionId: reference }));

    const res = await api.get(`/wagering/transactions/${created.transactionId}`);

    expect(res.body).toMatchObject({
      status: "PENDING_REFERENCE",
      referenceExternalTransactionId: reference,
      referenceTransactionId: null,
    });
  });

  test("REFUND processado aponta para o id INTERNO da BET", async () => {
    const wallet = await newWallet("100.00");
    const bet = payload(wallet, "BET", "30.00");
    const { body: betResult } = await submit(bet);
    const { body: refund } = await submit(
      payload(wallet, "REFUND", "30.00", { referenceExternalTransactionId: bet.externalTransactionId }),
    );

    const res = await api.get(`/wagering/transactions/${refund.transactionId}`);

    expect(res.body.referenceExternalTransactionId).toBe(bet.externalTransactionId);
    expect(res.body.referenceTransactionId).toBe(betResult.transactionId);
  });

  test("id que não existe → 404 WAGER_TRANSACTION_NOT_FOUND", async () => {
    const res = await api.get(`/wagering/transactions/${UNKNOWN_ID}`);

    expect(res.status).toBe(404);
    expect(res.body.code).toBe("WAGER_TRANSACTION_NOT_FOUND");
  });

  test("id interno que não é UUID → 400 (não chega no Postgres)", async () => {
    const res = await api.get("/wagering/transactions/abc");

    expect(res.status).toBe(400);
    expect(res.body.code).toBe("INVALID_PAYLOAD");
  });

  test("o id externo é do PROVEDOR: o mesmo id em outro provider → 404", async () => {
    const wallet = await newWallet();
    const body = payload(wallet, "BET", "1.00");
    await submit(body);

    const res = await api.get(`/providers/outro-provider/wagering/transactions/${body.externalTransactionId}`);

    expect(res.status).toBe(404);
    expect(res.body.code).toBe("WAGER_TRANSACTION_NOT_FOUND");
  });

  test("POST deu timeout? o provedor descobre pelo GET externo: 404 → nunca chegou, reenviar é seguro", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "BET", "30.00");

    const before = await api.get(`/providers/${PROVIDER}/wagering/transactions/${body.externalTransactionId}`);
    await submit(body);
    const after = await api.get(`/providers/${PROVIDER}/wagering/transactions/${body.externalTransactionId}`);

    expect(before.status).toBe(404);
    expect(after.status).toBe(200);
    expect(after.body.status).toBe("PROCESSED");
  });
});

// ---------------------------------------------------------------------------

describe("GET /wallets/:walletId/ledger", () => {
  const ledgerOf = (walletId: string, query = "") => api.get(`/wallets/${walletId}/ledger${query}`);

  /** Percorre todas as páginas seguindo o nextCursor. */
  async function walkAllPages(walletId: string, limit: number) {
    const pages: any[][] = [];
    let cursor: string | null = null;
    do {
      const res: HttpResult = await ledgerOf(walletId, `?limit=${limit}${cursor ? `&cursor=${cursor}` : ""}`);
      expect(res.status).toBe(200);
      pages.push(res.body.items);
      cursor = res.body.nextCursor;
    } while (cursor);
    return pages;
  }

  test("o extrato em ordem de criação: cada linha começa onde a anterior terminou", async () => {
    const wallet = await newWallet("100.00");
    const { body: bet } = await submit(payload(wallet, "BET", "30.00"));
    await submit(payload(wallet, "WIN", "50.00"));

    const res = await ledgerOf(wallet.id);

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      walletId: wallet.id,
      items: [
        expect.objectContaining({ direction: "CREDIT", money: { amount: "100.00", currency: "BRL" }, balanceBefore: { amount: "0.00", currency: "BRL" }, balanceAfter: { amount: "100.00", currency: "BRL" } }),
        expect.objectContaining({ direction: "DEBIT", transactionId: bet.transactionId, money: { amount: "30.00", currency: "BRL" }, balanceBefore: { amount: "100.00", currency: "BRL" }, balanceAfter: { amount: "70.00", currency: "BRL" } }),
        expect.objectContaining({ direction: "CREDIT", money: { amount: "50.00", currency: "BRL" }, balanceBefore: { amount: "70.00", currency: "BRL" }, balanceAfter: { amount: "120.00", currency: "BRL" } }),
      ],
      nextCursor: null,
    });
    expect(Object.keys(res.body.items[0]).sort()).toEqual(
      ["balanceAfter", "balanceBefore", "createdAt", "direction", "id", "money", "transactionId"],
    );
  });

  test("paginando de 2 em 2: todas as linhas, cada uma uma vez, na mesma ordem do extrato completo", async () => {
    const wallet = await newWallet("100.00");
    for (let i = 1; i <= 6; i++) await submit(payload(wallet, "BET", `${i}.00`));
    const full = (await ledgerOf(wallet.id)).body.items; // 7 linhas: abertura + 6 BETs

    const pages = await walkAllPages(wallet.id, 2);

    expect(pages.map((p) => p.length)).toEqual([2, 2, 2, 1]);
    expect(pages.flat()).toEqual(full);
  });

  test("o cursor é opaco: não expõe o id cru", async () => {
    const wallet = await newWallet("100.00");
    await submit(payload(wallet, "BET", "1.00"));

    const res = await ledgerOf(wallet.id, "?limit=1");

    expect(res.body.nextCursor).toEqual(expect.any(String));
    expect(res.body.nextCursor).not.toBe(res.body.items[0].id);
    expect(res.body.nextCursor).toMatch(/^[A-Za-z0-9_-]+$/); // base64url: seguro na URL sem escapar
  });

  test("ESTÁVEL: lançamento novo no meio da paginação entra no fim, sem repetir nem pular nada", async () => {
    const wallet = await newWallet("100.00");
    await submit(payload(wallet, "BET", "10.00"));
    await submit(payload(wallet, "BET", "20.00"));

    const page1 = await ledgerOf(wallet.id, "?limit=2");
    await submit(payload(wallet, "BET", "5.00")); // chega enquanto o cliente pagina
    const page2 = await ledgerOf(wallet.id, `?limit=2&cursor=${page1.body.nextCursor}`);

    const amounts = [...page1.body.items, ...page2.body.items].map((i: any) => i.money.amount);
    expect(amounts).toEqual(["100.00", "10.00", "20.00", "5.00"]);
    expect(page2.body.nextCursor).toBeNull();
  });

  test("número de linhas múltiplo do limit: a última página cheia já diz nextCursor null (sem página vazia extra)", async () => {
    const wallet = await newWallet("100.00");
    await submit(payload(wallet, "BET", "1.00"));
    await submit(payload(wallet, "BET", "1.00"));
    await submit(payload(wallet, "BET", "1.00")); // 4 linhas

    const pages = await walkAllPages(wallet.id, 2);

    expect(pages.map((p) => p.length)).toEqual([2, 2]);
  });

  test("sem limit: padrão 50", async () => {
    const wallet = await newWallet("100.00");
    await Promise.all(Array.from({ length: 54 }, () => submit(payload(wallet, "BET", "1.00")))); // 55 linhas

    const res = await ledgerOf(wallet.id);

    expect(res.body.items).toHaveLength(50);
    expect(res.body.nextCursor).toEqual(expect.any(String));
    await assertLedgerMatchesBalance(wallet.id);
  });

  test("limit=200 (o máximo) é aceito", async () => {
    const wallet = await newWallet();
    expect((await ledgerOf(wallet.id, "?limit=200")).status).toBe(200);
  });

  test("wallet sem lançamentos → 200 com items vazio", async () => {
    const wallet = await newWallet("0.00");

    const res = await ledgerOf(wallet.id);

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ walletId: wallet.id, items: [], nextCursor: null });
  });

  test("cursor de OUTRA wallet não vaza nada: só aparecem linhas desta wallet", async () => {
    const a = await newWallet("100.00");
    const b = await newWallet("100.00");
    await submit(payload(a, "BET", "1.00"));
    await submit(payload(b, "BET", "1.00"));
    const cursorOfA = (await ledgerOf(a.id, "?limit=1")).body.nextCursor;

    const res = await ledgerOf(b.id, `?cursor=${cursorOfA}`);
    const ledgerOfB = (await ledgerOf(b.id)).body.items.map((i: any) => i.id);

    expect(res.status).toBe(200);
    for (const item of res.body.items) expect(ledgerOfB).toContain(item.id);
  });

  test.each([
    ["limit=0", "?limit=0"],
    ["limit negativo", "?limit=-1"],
    ["limit acima do máximo", "?limit=201"],
    ["limit decimal", "?limit=2.5"],
    ["limit não numérico", "?limit=abc"],
    ["limit repetido", "?limit=1&limit=2"],
    ["cursor inventado", "?cursor=lixo"],
    ["cursor base64 válido mas que não é um id", `?cursor=${Buffer.from("nao-sou-um-uuid").toString("base64url")}`],
  ])("%s → 400 INVALID_PAYLOAD", async (_label, query) => {
    const wallet = await newWallet();

    const res = await ledgerOf(wallet.id, query);

    expect(res.status).toBe(400);
    expect(res.body.code).toBe("INVALID_PAYLOAD");
  });

  test("walletId que não é UUID → 400", async () => {
    expect((await ledgerOf("abc")).status).toBe(400);
  });

  test("wallet que não existe → 404 (não uma página vazia)", async () => {
    const res = await ledgerOf(UNKNOWN_ID);

    expect(res.status).toBe(404);
    expect(res.body.code).toBe("WALLET_NOT_FOUND");
  });
});

// ---------------------------------------------------------------------------

describe("falha transitória → 503", () => {
  /** O erro CRU que o driver `pg` lança quando o Postgres não responde (visto testando). */
  const connectionRefused = () =>
    Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:5432"), { code: "ECONNREFUSED" });

  test("banco fora no POST → 503 + Retry-After, sem vazar detalhe interno; o reenvio depois funciona", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "BET", "30.00");
    const useCase = api.app.get(ProcessWagerTransactionUseCase);
    const spy = spyOn(useCase, "execute").mockRejectedValueOnce(connectionRefused());

    try {
      const res = await submit(body);

      expect(res.status).toBe(503);
      expect(res.headers.get("retry-after")).toBe("1");
      expect(res.body).toEqual({ code: "SERVICE_UNAVAILABLE", message: "Falha temporária; tente novamente" });
      expect(JSON.stringify(res.body)).not.toContain("5432");
    } finally {
      spy.mockRestore();
    }

    // O provedor faz o que o 503 pede: reenvia, com a MESMA key.
    const retry = await submit(body);
    expect(retry.status).toBe(201);
    expect(await balanceOf(wallet.id)).toBe("70.00");
  });

  test.each([
    ["conexão caiu no meio (sem code)", () => new Error("Connection terminated unexpectedly")],
    ["banco desligando (57P01)", () => Object.assign(new Error("terminating connection"), { code: "57P01" })],
    ["deadlock (40P01)", () => Object.assign(new Error("deadlock detected"), { code: "40P01" })],
  ])("%s no GET → 503", async (_label, makeError) => {
    const useCase = api.app.get(GetWalletUseCase);
    const spy = spyOn(useCase, "execute").mockRejectedValueOnce(makeError());

    try {
      const res = await api.get(`/wallets/${UNKNOWN_ID}`);
      expect(res.status).toBe(503);
    } finally {
      spy.mockRestore();
    }
  });

  test("bug comum continua 500 (não é mascarado como transitório)", async () => {
    const useCase = api.app.get(GetWalletUseCase);
    const spy = spyOn(useCase, "execute").mockRejectedValueOnce(new TypeError("Cannot read properties of undefined"));

    try {
      const res = await api.get(`/wallets/${UNKNOWN_ID}`);
      expect(res.status).toBe(500);
      expect(res.headers.get("retry-after")).toBeNull();
    } finally {
      spy.mockRestore();
    }
  });

  test("rota que não existe continua 404 do Nest (o super.catch trata)", async () => {
    expect((await api.get("/rota-que-nao-existe")).status).toBe(404);
  });
});

// ---------------------------------------------------------------------------

describe("concorrência pelo HTTP (item 6)", () => {
  test("80 BETs de 2.00 em paralelo numa wallet de 100.00: exatamente 50 passam, 30 são 422, saldo 0.00", async () => {
    const wallet = await newWallet("100.00");

    const results = await Promise.all(Array.from({ length: 80 }, () => submit(payload(wallet, "BET", "2.00"))));

    expect(statusCount(results)).toEqual({ 201: 50, 422: 30 });
    for (const r of results.filter((r) => r.status === 422)) expect(r.body.failureCode).toBe("INSUFFICIENT_FUNDS");
    expect(await balanceOf(wallet.id)).toBe("0.00");
    expect(await countLedgerEntries(wallet.id)).toBe(51);
    await assertLedgerMatchesBalance(wallet.id);
  }, 30_000);

  test("o MESMO pedido 30× em paralelo: 1 cria (201), 29 são replay (200), uma transação só, um débito só", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "BET", "30.00");

    const results = await Promise.all(Array.from({ length: 30 }, () => submit(body)));

    expect(statusCount(results)).toEqual({ 201: 1, 200: 29 });
    expect(new Set(results.map((r) => r.body.transactionId)).size).toBe(1);
    expect(results.every((r) => r.body.balance.amount === "70.00")).toBe(true);
    expect(await countTransactions(wallet.id)).toBe(1);
    expect(await balanceOf(wallet.id)).toBe("70.00");
  }, 30_000);

  test("mesmo externalTransactionId com 20 keys diferentes em paralelo: 1 × 201, 19 × 409", async () => {
    const wallet = await newWallet("100.00");
    const body = payload(wallet, "BET", "30.00");

    const results = await Promise.all(Array.from({ length: 20 }, (_, i) => submit(body, { key: `key-${i}-${newId()}` })));

    expect(statusCount(results)).toEqual({ 201: 1, 409: 19 });
    expect(await countTransactions(wallet.id)).toBe(1);
    expect(await balanceOf(wallet.id)).toBe("70.00");
  }, 30_000);

  test("BETs e WINs misturados em paralelo: o saldo final é exatamente o que o ledger diz", async () => {
    const wallet = await newWallet("50.00");
    const bodies = Array.from({ length: 60 }, (_, i) => payload(wallet, i % 2 === 0 ? "BET" : "WIN", "5.00"));

    const results = await Promise.all(bodies.map((b) => submit(b)));

    // A ordem em que o banco serializa é imprevisível, então algumas BETs podem
    // ser rejeitadas. O que NÃO pode variar: saldo = 50 + 5·(WINs) − 5·(BETs aceitas).
    expect(results.every((r) => r.status === 201 || r.status === 422)).toBe(true);
    const processed = (kind: string) =>
      results.filter((r, i) => bodies[i]!.kind === kind && r.status === 201).length;
    expect(processed("WIN")).toBe(30); // crédito nunca falta saldo
    const expected = 50 + 5 * processed("WIN") - 5 * processed("BET");
    expect(Number(await balanceOf(wallet.id))).toBe(expected);
    await assertLedgerMatchesBalance(wallet.id);
  }, 30_000);

  test("3 instâncias da API na mesma wallet: 60 BETs de 1.00 com saldo 50.00 → 50 passam, saldo 0.00", async () => {
    const others = [await startTestApp(), await startTestApp()];
    const instances = [api, ...others];

    try {
      const wallet = await newWallet("50.00");

      const results = await Promise.all(
        Array.from({ length: 60 }, (_, i) => submit(payload(wallet, "BET", "1.00"), { app: instances[i % 3] })),
      );

      expect(statusCount(results)).toEqual({ 201: 50, 422: 10 });
      expect(await balanceOf(wallet.id)).toBe("0.00");
      await assertLedgerMatchesBalance(wallet.id);

      // E o mesmo pedido chegando nas 3 instâncias ao mesmo tempo (retry do provedor
      // caindo em instâncias diferentes atrás do load balancer): uma transação só.
      const wallet2 = await newWallet("100.00");
      const body = payload(wallet2, "BET", "30.00");
      const same = await Promise.all(instances.flatMap((app) => [submit(body, { app }), submit(body, { app })]));

      expect(statusCount(same)).toEqual({ 201: 1, 200: 5 });
      expect(await countTransactions(wallet2.id)).toBe(1);
      expect(await balanceOf(wallet2.id)).toBe("70.00");
    } finally {
      for (const app of others) await app.close();
    }
  }, 60_000);
});
