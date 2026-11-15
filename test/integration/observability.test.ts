import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, mock, spyOn, test } from "bun:test";
import { CreateQueueCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { UniqueConstraintViolationException } from "@mikro-orm/postgresql";
import { EventPublisher } from "../../src/application/outbox/event-publisher.ts";
import { PublishOutboxBatchUseCase } from "../../src/application/outbox/publish-outbox-batch.use-case.ts";
import { ProcessWagerTransactionUseCase } from "../../src/application/wagering/process-wager-transaction.use-case.ts";
import { ResolvePendingReferencesUseCase } from "../../src/application/wagering/resolve-pending-references.use-case.ts";
import { env } from "../../src/config/env.ts";
import { Money } from "../../src/domain/money/money.ts";
import { WagerTransactionKind } from "../../src/domain/wager-transaction/wager-transaction.enums.ts";
import type { WagerTransactionInput } from "../../src/http/wager-transaction.request.ts";
import { WagerTransactionRepository } from "../../src/infrastructure/database/repositories/wager-transaction.repository.ts";
import { MetricsCollector } from "../../src/infrastructure/observability/metrics-collector.ts";
import { JsonLogger } from "../../src/infrastructure/observability/json-logger.ts";
import { Metrics } from "../../src/infrastructure/observability/metrics.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, sql } from "./support/database.ts";
import { newId } from "./support/rows.ts";

/**
 * Observabilidade (seção 12): logs JSON com correlationId/messageId/transactionId/
 * walletId/providerId, sem payload financeiro; métricas no GET /metrics.
 *
 * A app deste arquivo escreve os logs num array (JsonLogger com writer próprio),
 * para os testes lerem as linhas que sairiam no stdout.
 */

const logLines: string[] = [];
let api: TestApp;
let sqsClient: SQSClient;

beforeAll(async () => {
  sqsClient = new SQSClient({ region: env.aws.region, endpoint: env.aws.sqsEndpoint });
  for (const QueueName of [env.aws.eventsQueueName, env.aws.dlqName]) {
    await sqsClient.send(new CreateQueueCommand({ QueueName, Attributes: { FifoQueue: "true" } }));
  }
  api = await startTestApp({ logger: new JsonLogger("log", (line) => logLines.push(line)) });
});
afterAll(async () => {
  await api.close();
  sqsClient.destroy();
  await closeTestOrm();
});
beforeEach(() => {
  logLines.length = 0;
});
afterEach(() => {
  mock.restore();
});

// ============================================================================ helpers

const metrics = () => api.app.get(Metrics);
const logs = () => logLines.map((line) => JSON.parse(line));

interface TestWallet {
  id: string;
  playerId: string;
}

async function newWallet(amount = "100.00"): Promise<TestWallet> {
  const res = await api.post("/wallets", { playerId: newId(), initialBalance: { amount, currency: "BRL" } });
  return { id: res.body.id, playerId: res.body.playerId };
}

function payload(wallet: TestWallet, kind: string, amount: string, overrides: Record<string, unknown> = {}) {
  return {
    providerId: "provider-obs",
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

function submit(body: ReturnType<typeof payload>, headers: Record<string, string> = {}) {
  return api.post("/wagering/transactions", body, { "idempotency-key": `${body.providerId}:${body.externalTransactionId}`, ...headers });
}

async function scrape(): Promise<string> {
  const res = await fetch(`${api.url}/metrics`);
  expect(res.status).toBe(200);
  return res.text();
}

/** O valor de uma série no texto do /metrics (labels na ordem alfabética, como o registro escreve). */
function seriesValue(text: string, series: string): number | undefined {
  const line = text.split("\n").find((l) => l.startsWith(`${series} `));
  return line ? Number(line.slice(series.length + 1)) : undefined;
}

// ============================================================================ correlationId

describe("x-correlation-id em toda requisição", () => {
  test("o do cliente volta no header, em qualquer rota", async () => {
    for (const path of ["/health/live", "/wallets/" + newId(), "/rota-que-nao-existe"]) {
      const res = await api.get(path, { "x-correlation-id": "corr-cliente" });
      expect(res.headers.get("x-correlation-id")).toBe("corr-cliente");
    }
  });

  test("sem header → gera um UUID", async () => {
    const res = await api.get("/health/live");
    expect(res.headers.get("x-correlation-id")).toMatch(/^[0-9a-f-]{36}$/);
  });

  test("header gigante (> 128) não é aceito como correlationId: gera outro", async () => {
    const res = await api.get("/health/live", { "x-correlation-id": "x".repeat(500) });
    expect(res.headers.get("x-correlation-id")).toMatch(/^[0-9a-f-]{36}$/);
  });

  test("o correlationId do header é o mesmo que vai para os eventos da outbox", async () => {
    const wallet = await newWallet();
    const res = await submit(payload(wallet, "BET", "10.00"), { "x-correlation-id": "corr-ate-o-evento" });
    const events = await sql("select payload from outbox_messages where aggregate_id = ?", [res.body.transactionId]);
    expect(events.map((e) => e.payload.correlationId)).toEqual(["corr-ate-o-evento"]);
  });
});

// ============================================================================ logs

describe("logs estruturados (JSON)", () => {
  test("access log: uma linha por requisição, com correlationId, transactionId, walletId, providerId, rota e duração", async () => {
    const wallet = await newWallet();
    logLines.length = 0;
    const res = await submit(payload(wallet, "BET", "10.00"), { "x-correlation-id": "corr-log-1" });

    const access = logs().find((e) => e.msg === "http request" && e.correlationId === "corr-log-1");
    expect(access).toMatchObject({
      level: "info",
      context: "HTTP",
      method: "POST",
      route: "/wagering/transactions",
      status: 201,
      durationMs: expect.any(Number),
      transactionId: res.body.transactionId,
      walletId: wallet.id,
      providerId: "provider-obs",
    });
  });

  test("um log escrito DENTRO do pedido (filtro de erro) sai com o correlationId do header", async () => {
    const wallet = await newWallet();
    spyOn(api.app.get(ProcessWagerTransactionUseCase), "execute").mockRejectedValueOnce(
      Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:5432"), { code: "ECONNREFUSED" }),
    );
    logLines.length = 0;

    const res = await submit(payload(wallet, "BET", "10.00"), { "x-correlation-id": "corr-503" });

    expect(res.status).toBe(503);
    const warn = logs().find((e) => e.context === "DomainErrorFilter");
    expect(warn).toMatchObject({ level: "warn", correlationId: "corr-503" });
    // E o access log do 503 é ERROR (5xx).
    expect(logs().find((e) => e.msg === "http request" && e.correlationId === "corr-503")?.level).toBe("error");
  });

  test("4xx vira warn no access log", async () => {
    logLines.length = 0;
    await api.get(`/wallets/${newId()}`, { "x-correlation-id": "corr-404" });
    expect(logs().find((e) => e.correlationId === "corr-404")).toMatchObject({ level: "warn", status: 404, route: "/wallets/:walletId" });
  });

  test("nenhuma linha de log carrega o valor da aposta (sem payload financeiro)", async () => {
    const wallet = await newWallet("1000.00");
    logLines.length = 0;
    await submit(payload(wallet, "BET", "777.77"));
    await submit(payload(wallet, "BET", "999.99")); // rejeitada: sem saldo
    expect(logLines.length).toBeGreaterThan(0);
    expect(logLines.join("\n")).not.toContain("777.77");
    expect(logLines.join("\n")).not.toContain("999.99");
  });

  test("cada linha é JSON válido de uma linha só, com time e level", () => {
    for (const line of logLines) {
      expect(line).not.toContain("\n");
      const entry = JSON.parse(line);
      expect(entry).toHaveProperty("time");
      expect(entry).toHaveProperty("level");
    }
  });
});

// ============================================================================ métricas

describe("métricas: transações, duplicatas, latência e locks", () => {
  test("transações por canal, kind e STATUS (processada, rejeitada, pendente)", async () => {
    const wallet = await newWallet("10.00");
    const counter = metrics().wagerTransactions;
    const before = {
      processed: counter.get({ channel: "http", kind: "BET", status: "PROCESSED" }),
      rejected: counter.get({ channel: "http", kind: "BET", status: "REJECTED" }),
      pending: counter.get({ channel: "http", kind: "REFUND", status: "PENDING_REFERENCE" }),
    };

    await submit(payload(wallet, "BET", "5.00"));
    await submit(payload(wallet, "BET", "50.00"));
    await submit(payload(wallet, "REFUND", "5.00", { referenceExternalTransactionId: `nunca-${newId()}` }));

    expect(counter.get({ channel: "http", kind: "BET", status: "PROCESSED" })).toBe(before.processed + 1);
    expect(counter.get({ channel: "http", kind: "BET", status: "REJECTED" })).toBe(before.rejected + 1);
    expect(counter.get({ channel: "http", kind: "REFUND", status: "PENDING_REFERENCE" })).toBe(before.pending + 1);
    expect(seriesValue(await scrape(), 'wager_transactions_total{channel="http",kind="BET",status="PROCESSED"}')).toBe(
      before.processed + 1,
    );
  });

  test("duplicatas: replay por idempotency key e entrega repetida da fila (inbox)", async () => {
    const wallet = await newWallet();
    const duplicates = metrics().wagerDuplicates;
    const replaysBefore = duplicates.get({ mechanism: "idempotency_replay" });
    const inboxBefore = duplicates.get({ mechanism: "inbox" });

    const body = payload(wallet, "BET", "1.00");
    await submit(body);
    await submit(body); // replay

    const useCase = api.app.get(ProcessWagerTransactionUseCase);
    const externalTransactionId = `tx-${newId()}`;
    const input: WagerTransactionInput = {
      ...body,
      externalTransactionId,
      idempotencyKey: `provider-obs:${externalTransactionId}`,
      kind: WagerTransactionKind.Bet,
      money: Money.from(body.money),
      referenceExternalTransactionId: undefined,
    };
    const inbox = { consumerName: "obs-test", messageId: `msg-${newId()}` };
    await useCase.execute(input, { correlationId: "c" }, inbox);
    await useCase.execute(input, { correlationId: "c" }, inbox).catch(() => {}); // DuplicateMessageError

    expect(duplicates.get({ mechanism: "idempotency_replay" })).toBe(replaysBefore + 1);
    expect(duplicates.get({ mechanism: "inbox" })).toBe(inboxBefore + 1);
    // A entrada pela fila conta como canal "sqs".
    expect(metrics().wagerTransactions.get({ channel: "sqs", kind: "BET", status: "PROCESSED" })).toBeGreaterThanOrEqual(1);
  });

  test("latência do processamento e espera pelo lock da wallet viram histogramas", async () => {
    const wallet = await newWallet();
    const processingBefore = metrics().wagerProcessingSeconds.count({ channel: "http" });
    const lockBefore = metrics().walletLockWaitSeconds.count();

    await Promise.all([1, 2, 3].map(() => submit(payload(wallet, "BET", "1.00"))));

    expect(metrics().wagerProcessingSeconds.count({ channel: "http" })).toBe(processingBefore + 3);
    expect(metrics().walletLockWaitSeconds.count()).toBe(lockBefore + 3);
    const text = await scrape();
    expect(text).toContain("# TYPE wallet_lock_wait_seconds histogram");
    expect(text).toContain('wager_transaction_processing_seconds_bucket{channel="http",le="+Inf"}');
  });

  test("conflito de idempotência entre requisições simultâneas (o retry interno) conta como lock conflict", async () => {
    const wallet = await newWallet();
    const before = metrics().walletLockConflicts.get({ type: "idempotency_race" });
    const repository = api.app.get(WagerTransactionRepository);
    const race = Object.assign(
      new UniqueConstraintViolationException(Object.assign(new Error("duplicate key"), { code: "23505" })),
      { constraint: "wager_tx_provider_idempotency_key_uq" },
    );
    spyOn(repository, "insert").mockRejectedValueOnce(race);

    const res = await submit(payload(wallet, "BET", "1.00"));

    expect(res.status).toBe(201); // o retry interno resolveu
    expect(metrics().walletLockConflicts.get({ type: "idempotency_race" })).toBe(before + 1);
  });

  test("falha transitória que virou 503 é contada", async () => {
    const wallet = await newWallet();
    const before = metrics().transientErrors.get({ source: "http" });
    spyOn(api.app.get(ProcessWagerTransactionUseCase), "execute").mockRejectedValueOnce(
      Object.assign(new Error("Connection terminated unexpectedly"), {}),
    );

    expect((await submit(payload(wallet, "BET", "1.00"))).status).toBe(503);
    expect(metrics().transientErrors.get({ source: "http" })).toBe(before + 1);
  });

  test("duração HTTP por ROTA (o padrão), nunca pela URL com o id (cardinalidade)", async () => {
    const wallet = await newWallet();
    await api.get(`/wallets/${wallet.id}`);

    const text = await scrape();
    expect(text).toContain('http_request_duration_seconds_count{method="GET",route="/wallets/:walletId",status="200"}');
    expect(text).not.toContain(wallet.id);
  });
});

describe("métricas: outbox, PENDING_REFERENCE e DLQ", () => {
  test("eventos publicados por tipo e o lag (commit → publicação)", async () => {
    await sql("update outbox_messages set published_at = now() where published_at is null"); // isola o teste
    const wallet = await newWallet();
    await submit(payload(wallet, "BET", "1.00"));
    const published = metrics().outboxPublished;
    const before = published.get({ event_type: "WagerTransactionProcessed" });
    const lagBefore = metrics().outboxPublishLagSeconds.count();

    const result = await api.app.get(PublishOutboxBatchUseCase).execute(50);

    expect(result.published).toBe(2);
    expect(published.get({ event_type: "WagerTransactionProcessed" })).toBe(before + 1);
    expect(metrics().outboxPublishLagSeconds.count()).toBe(lagBefore + 2);
  });

  test("falha ao publicar conta como retry da outbox", async () => {
    await sql("update outbox_messages set published_at = now() where published_at is null");
    const wallet = await newWallet();
    await submit(payload(wallet, "BET", "1.00"));
    spyOn(api.app.get(EventPublisher), "publish").mockRejectedValue(new Error("SQS fora"));
    const before = metrics().outboxPublishFailures.get();

    await api.app.get(PublishOutboxBatchUseCase).execute(50);

    expect(metrics().outboxPublishFailures.get()).toBe(before + 2);
    await sql("update outbox_messages set published_at = now() where published_at is null");
  });

  test("outbox lag no scrape: idade do evento pendente mais antigo e quantos estão pendentes", async () => {
    await sql("update outbox_messages set published_at = now() where published_at is null");
    const id = newId();
    await sql(
      `insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at, next_attempt_at)
       values (?, ?, 'TestEvent', '{}'::jsonb, now() - interval '90 seconds', now() + interval '1 hour')`,
      [id, newId()],
    );

    const text = await scrape();

    expect(seriesValue(text, "outbox_pending_messages")).toBe(1);
    expect(seriesValue(text, "outbox_oldest_pending_age_seconds")).toBeGreaterThanOrEqual(90);
    await sql("update outbox_messages set published_at = now() where id = ?", [id]);
    expect(seriesValue(await scrape(), "outbox_oldest_pending_age_seconds")).toBe(0);
  });

  test("PENDING_REFERENCE: quantas estão esperando (scrape) e o desfecho de cada tentativa do worker", async () => {
    const wallet = await newWallet();
    const bet = payload(wallet, "BET", "1.00");
    await submit(payload(wallet, "REFUND", "1.00", { referenceExternalTransactionId: bet.externalTransactionId }));

    expect(seriesValue(await scrape(), "pending_reference_transactions")).toBeGreaterThanOrEqual(1);

    await submit(bet);
    const before = metrics().pendingReferenceResolutions.get({ outcome: "processed" });
    await api.app.get(ResolvePendingReferencesUseCase).execute(50, 30_000);
    expect(metrics().pendingReferenceResolutions.get({ outcome: "processed" })).toBeGreaterThanOrEqual(before + 1);
  });

  test("profundidade da DLQ lida do SQS de verdade no scrape", async () => {
    const { QueueUrl } = await sqsClient.send(
      new CreateQueueCommand({ QueueName: env.aws.dlqName, Attributes: { FifoQueue: "true" } }),
    );
    await sqsClient.send(
      new SendMessageCommand({ QueueUrl, MessageBody: "{}", MessageGroupId: "g", MessageDeduplicationId: newId() }),
    );

    const value = seriesValue(await scrape(), `sqs_dead_letter_queue_messages{queue="${env.aws.dlqName}"}`);
    expect(value).toBeGreaterThanOrEqual(1);
  });

  test("se uma fonte falha no scrape, o /metrics continua respondendo e conta a falha", async () => {
    const collector = api.app.get(MetricsCollector);
    spyOn(collector as any, "collectDatabase").mockRejectedValue(new Error("banco fora"));
    const before = metrics().metricsCollectionErrors.get({ source: "database" });

    const text = await scrape();

    expect(text).toContain("wager_transactions_total");
    expect(metrics().metricsCollectionErrors.get({ source: "database" })).toBe(before + 1);
  });

  test("o catálogo tem as métricas mínimas da seção 12", async () => {
    const text = await scrape();
    for (const name of [
      "wager_transactions_total", // transações por status
      "wager_duplicates_total", // duplicatas
      "sqs_messages_total", // retries (outcome=retried) — aparece quando houver mensagens
      "outbox_publish_failures_total", // retries da outbox
      "sqs_dead_letter_queue_messages", // DLQ
      "wallet_lock_conflicts_total", // conflitos de lock
      "wallet_lock_wait_seconds",
      "outbox_oldest_pending_age_seconds", // outbox lag
      "wager_transaction_processing_seconds", // latência
    ]) {
      expect(text).toContain(`# TYPE ${name} `);
    }
  });
});
