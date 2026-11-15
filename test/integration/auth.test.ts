import { afterAll, afterEach, beforeAll, describe, expect, mock, spyOn, test } from "bun:test";
import { ProviderAuthenticator } from "../../src/http/auth/provider-authenticator.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, sql } from "./support/database.ts";
import { newId } from "./support/rows.ts";

/**
 * Autenticação (seção 2): desligada por padrão (NoopProviderAuthenticator), mas
 * com o ponto de extensão no código. Estes testes trocam o comportamento da porta
 * para provar que o guard, as rotas públicas e a regra "o provedor só age em nome
 * de si mesmo" já funcionam: plugar um IdP é escrever UMA classe.
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

const authenticator = () => api.app.get(ProviderAuthenticator);

async function newWallet() {
  const res = await api.post("/wallets", { playerId: newId(), initialBalance: { amount: "100.00", currency: "BRL" } });
  return { id: res.body.id as string, playerId: res.body.playerId as string };
}

function bet(wallet: { id: string; playerId: string }, providerId: string) {
  const externalTransactionId = `tx-${newId()}`;
  const body = {
    providerId,
    externalTransactionId,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: "r",
    gameId: "g",
    kind: "BET",
    money: { amount: "10.00", currency: "BRL" },
  };
  return { body, key: `${providerId}:${externalTransactionId}` };
}

describe("padrão: autenticação desligada (no-op)", () => {
  test("toda rota funciona sem credencial", async () => {
    const wallet = await newWallet();
    const { body, key } = bet(wallet, "provider-a");
    expect((await api.post("/wagering/transactions", body, { "idempotency-key": key })).status).toBe(201);
    expect((await api.get(`/wallets/${wallet.id}`)).status).toBe(200);
  });
});

describe("com um autenticador de verdade plugado na porta", () => {
  test("sem credencial válida → 401 UNAUTHENTICATED, e nada é gravado", async () => {
    const wallet = await newWallet();
    spyOn(authenticator(), "authenticate").mockResolvedValue({ authenticated: false, reason: "token ausente" });
    const { body, key } = bet(wallet, "provider-a");

    const res = await api.post("/wagering/transactions", body, { "idempotency-key": key });

    expect(res.status).toBe(401);
    expect(res.body).toEqual({ code: "UNAUTHENTICATED", message: "Não autenticado: token ausente" });
    const rows = await sql("select 1 from wager_transactions where external_transaction_id = ?", [body.externalTransactionId]);
    expect(rows).toHaveLength(0);
    expect((await api.get(`/wallets/${wallet.id}`)).status).toBe(401);
  });

  test("health checks e /metrics continuam abertos (seção 2)", async () => {
    spyOn(authenticator(), "authenticate").mockResolvedValue({ authenticated: false, reason: "sem token" });

    expect((await api.get("/health/live")).status).toBe(200);
    expect((await api.get("/health/ready")).status).not.toBe(401);
    expect((await fetch(`${api.url}/metrics`)).status).toBe(200);
  });

  test("o autenticador recebe os headers (ex.: Authorization: Bearer …) e a rota", async () => {
    const spy = spyOn(authenticator(), "authenticate").mockResolvedValue({ authenticated: true });
    await api.get(`/wallets/${newId()}`, { authorization: "Bearer abc.def.ghi" });

    const [request] = spy.mock.calls.at(-1)!;
    expect(request.headers.authorization).toBe("Bearer abc.def.ghi");
    expect(request).toMatchObject({ method: "GET", path: expect.stringContaining("/wallets/") });
  });

  test("provedor autenticado só opera em nome de SI MESMO: outro providerId no corpo → 403", async () => {
    const wallet = await newWallet();
    spyOn(authenticator(), "authenticate").mockResolvedValue({ authenticated: true, principal: { providerId: "provider-a" } });

    const own = bet(wallet, "provider-a");
    expect((await api.post("/wagering/transactions", own.body, { "idempotency-key": own.key })).status).toBe(201);

    const other = bet(wallet, "provider-b");
    const res = await api.post("/wagering/transactions", other.body, { "idempotency-key": other.key });
    expect(res.status).toBe(403);
    expect(res.body.code).toBe("PROVIDER_MISMATCH");
    const rows = await sql("select 1 from wager_transactions where external_transaction_id = ?", [other.body.externalTransactionId]);
    expect(rows).toHaveLength(0);
  });

  test("consultas também respeitam o provedor: a rota de outro → 403; a transação de outro por id → 404 (não revela)", async () => {
    const wallet = await newWallet();
    const { body, key } = bet(wallet, "provider-b");
    const created = await api.post("/wagering/transactions", body, { "idempotency-key": key });

    spyOn(authenticator(), "authenticate").mockResolvedValue({ authenticated: true, principal: { providerId: "provider-a" } });

    expect((await api.get(`/providers/provider-b/wagering/transactions/${body.externalTransactionId}`)).status).toBe(403);
    expect((await api.get(`/wagering/transactions/${created.body.transactionId}`)).status).toBe(404);
  });
});
