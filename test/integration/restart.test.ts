import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { CreateQueueCommand, SQSClient } from "@aws-sdk/client-sqs";
import { env } from "../../src/config/env.ts";
import { closeTestOrm, getTestOrm, sql } from "./support/database.ts";
import { assertLedgerMatchesBalance } from "./support/ledger.ts";
import { startAppProcess, type AppProcess } from "./support/process.ts";
import { newId } from "./support/rows.ts";

/**
 * Concorrência #8 (seção 13): reinício do serviço com comprovação da consistência final.
 *
 *   1. a aplicação (processo separado, com os workers ligados) recebe uma rajada
 *      de operações em 10 wallets;
 *   2. no meio, o processo é morto com SIGKILL: transações em andamento, eventos
 *      não publicados, PENDING_REFERENCE esperando;
 *   3. uma nova instância sobe; o cliente reenvia TUDO (at-least-once: ele não
 *      sabe o que chegou a ser gravado);
 *   4. consistência final: nenhuma operação duplicada ou perdida, saldo == ledger
 *      (pela reconciliação), toda pendência resolvida, todo evento publicado.
 */

const WORKERS_ON = {
  OUTBOX_PUBLISHER_ENABLED: "true",
  PENDING_REFERENCE_WORKER_ENABLED: "true",
  OUTBOX_POLL_INTERVAL_MS: "100",
  PENDING_REFERENCE_POLL_INTERVAL_MS: "100",
};

beforeAll(async () => {
  await getTestOrm(); // schema pronto antes de qualquer processo conectar
  const sqs = new SQSClient({ region: env.aws.region, endpoint: env.aws.sqsEndpoint });
  await sqs.send(new CreateQueueCommand({ QueueName: env.aws.eventsQueueName, Attributes: { FifoQueue: "true" } }));
  sqs.destroy();
});
afterAll(async () => {
  await closeTestOrm();
});

interface Operation {
  walletId: string;
  body: Record<string, unknown>;
  key: string;
}

/** Por wallet: 8 BETs de 5.00, 1 WIN de 3.00 e 1 REFUND da 1ª BET (enviado ANTES dela). */
function operationsFor(wallet: { id: string; playerId: string }): Operation[] {
  const base = { providerId: "provider-restart", playerId: wallet.playerId, walletId: wallet.id, roundId: "r", gameId: "g" };
  const op = (kind: string, amount: string, extra: Record<string, unknown> = {}): Operation => {
    const externalTransactionId = `tx-${newId()}`;
    return {
      walletId: wallet.id,
      body: { ...base, externalTransactionId, kind, money: { amount, currency: "BRL" }, ...extra },
      key: `provider-restart:${externalTransactionId}`,
    };
  };
  const bets = Array.from({ length: 8 }, () => op("BET", "5.00"));
  const refund = op("REFUND", "5.00", { referenceExternalTransactionId: bets[0]!.body.externalTransactionId });
  return [refund, ...bets, op("WIN", "3.00")];
}

function send(app: AppProcess, operation: Operation): Promise<number> {
  return fetch(`${app.url}/wagering/transactions`, {
    method: "POST",
    headers: { "content-type": "application/json", "idempotency-key": operation.key },
    body: JSON.stringify(operation.body),
  }).then((res) => res.status);
}

async function waitUntil(check: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("timeout esperando a consistência final");
    await Bun.sleep(100);
  }
}

describe("concorrência #8: reinício do serviço", () => {
  test(
    "SIGKILL no meio da rajada → nova instância + reenvio de tudo → consistência final comprovada",
    async () => {
      // ---------------------------------------------------------------- 1. a 1ª instância
      const first = await startAppProcess(WORKERS_ON);
      const wallets: { id: string; playerId: string }[] = [];
      for (let i = 0; i < 10; i++) {
        const res = await fetch(`${first.url}/wallets`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ playerId: newId(), initialBalance: { amount: "100.00", currency: "BRL" } }),
        });
        wallets.push((await res.json()) as { id: string; playerId: string });
      }
      const operations = wallets.flatMap(operationsFor);
      const walletIds = wallets.map((w) => w.id);

      // ---------------------------------------------------------------- 2. rajada + SIGKILL no meio
      let answered = 0;
      const inFlight = operations.map((operation) =>
        send(first, operation).then(
          (status) => {
            answered++;
            return status;
          },
          () => "connection-lost" as const,
        ),
      );
      while (answered < 25) await Bun.sleep(1); // ~1/4 respondido: o resto está em andamento
      await first.kill();
      const beforeCrash = await Promise.all(inFlight);
      const lost = beforeCrash.filter((s) => s === "connection-lost").length;
      expect(lost).toBeGreaterThan(0); // houve mesmo operações cortadas no meio

      // ---------------------------------------------------------------- 3. nova instância + reenvio
      const second = await startAppProcess(WORKERS_ON);
      try {
        const retried = await Promise.all(operations.map((operation) => send(second, operation)));
        // Nada de 5xx: cada reenvio é processado (201/202) ou é replay (200).
        expect(retried.every((status) => [200, 201, 202].includes(status))).toBe(true);

        // Os workers da 2ª instância terminam o que ficou para trás.
        const placeholders = walletIds.map(() => "?").join(", ");
        await waitUntil(async () => {
          const [{ pending }] = await sql(
            `select count(*)::int as pending from wager_transactions
              where status = 'PENDING_REFERENCE' and wallet_id in (${placeholders})`,
            walletIds,
          );
          const [{ unpublished }] = await sql(
            `select count(*)::int as unpublished from outbox_messages
              where published_at is null
                and (aggregate_id in (${placeholders})
                     or aggregate_id in (select id from wager_transactions where wallet_id in (${placeholders})))`,
            [...walletIds, ...walletIds],
          );
          return pending === 0 && unpublished === 0;
        }, 20_000);

        // ---------------------------------------------------------------- 4. consistência final
        for (const wallet of wallets) {
          // Nenhuma operação duplicada nem perdida: exatamente 1 transação por operação.
          const rows = await sql(
            "select kind, status, count(*)::int as count from wager_transactions where wallet_id = ? and kind <> 'OPENING' group by kind, status order by kind",
            [wallet.id],
          );
          expect(rows).toEqual([
            { kind: "BET", status: "PROCESSED", count: 8 },
            { kind: "REFUND", status: "PROCESSED", count: 1 },
            { kind: "WIN", status: "PROCESSED", count: 1 },
          ]);

          // Saldo exato: 100 − 8×5 + 5 (REFUND) + 3 (WIN) = 68, e a reconciliação concorda.
          const reconciliation = await fetch(`${second.url}/wallets/${wallet.id}/reconciliation`, { method: "POST" });
          expect(await reconciliation.json()).toMatchObject({
            storedBalance: { amount: "68.00", currency: "BRL" },
            calculatedBalance: { amount: "68.00", currency: "BRL" },
            consistent: true,
            checkedEntries: 11, // abertura + 8 BETs + REFUND + WIN
          });
          await assertLedgerMatchesBalance(wallet.id);

          // Nenhum evento confirmado perdido: cada transação processada tem o seu
          // WagerTransactionProcessed, um só, já publicado.
          const events = await sql(
            `select t.id, count(o.id)::int as events, bool_and(o.published_at is not null) as all_published
               from wager_transactions t
               left join outbox_messages o on o.aggregate_id = t.id and o.event_type = 'WagerTransactionProcessed'
              where t.wallet_id = ? and t.kind <> 'OPENING'
              group by t.id`,
            [wallet.id],
          );
          expect(events.every((e) => e.events === 1 && e.all_published === true)).toBe(true);
        }
      } finally {
        // ---------------------------------------------------------------- shutdown limpo
        // O Nest roda os hooks de shutdown e depois RE-EMITE o próprio SIGTERM para
        // sair: 143 (= 128 + 15) é o fim normal. Se um hook falhasse, seria exit(1).
        expect(await second.stop()).toBe(143);
      }
    },
    90_000,
  );
});
