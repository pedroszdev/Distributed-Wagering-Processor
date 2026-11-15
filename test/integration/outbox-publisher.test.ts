import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, mock, spyOn, test } from "bun:test";
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { EventPublisher } from "../../src/application/outbox/event-publisher.ts";
import { PublishOutboxBatchUseCase } from "../../src/application/outbox/publish-outbox-batch.use-case.ts";
import { env } from "../../src/config/env.ts";
import type { OutboxMessage } from "../../src/domain/outbox/outbox-message.ts";
import { OutboxMessageRepository } from "../../src/infrastructure/database/repositories/outbox-message.repository.ts";
import { OutboxPublisherWorker } from "../../src/infrastructure/messaging/outbox-publisher.worker.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, getTestOrm, sql } from "./support/database.ts";
import { newId } from "./support/rows.ts";

/**
 * Publisher da outbox contra Postgres e SQS (MiniStack) REAIS.
 *
 * Cenário obrigatório da seção 11 do desafio:
 *   1. o Postgres confirma o commit;  2. o processo morre antes de publicar;
 *   3. outra instância assume;        4. o evento é publicado;
 *   5. uma publicação duplicada continua segura para o consumidor.
 *
 * O worker fica DESLIGADO nos testes (OUTBOX_PUBLISHER_ENABLED=false no .env.test):
 * cada teste chama a "volta" (PublishOutboxBatchUseCase.execute) na mão, para ter
 * controle total. Só o bloco "worker" liga o loop de verdade.
 */

// Três instâncias da aplicação, cada uma com o seu pool de conexões: são
// "processos" diferentes disputando a mesma outbox, como em produção.
let api: TestApp;
let instanceB: TestApp;
let instanceC: TestApp;

const sqs = new SQSClient({ region: env.aws.region, endpoint: env.aws.sqsEndpoint });
let queueUrl: string;

beforeAll(async () => {
  if (!env.aws.eventsQueueName.endsWith("-test.fifo")) {
    // Trava de segurança (como a do banco): os testes ESVAZIAM a fila.
    throw new Error(`Testes do publisher só rodam numa fila de teste (recebido: ${env.aws.eventsQueueName})`);
  }
  [api, instanceB, instanceC] = await Promise.all([startTestApp(), startTestApp(), startTestApp()]);

  // A fila de teste é criada aqui mesmo (CreateQueue é idempotente).
  const created = await sqs.send(
    new CreateQueueCommand({ QueueName: env.aws.eventsQueueName, Attributes: { FifoQueue: "true" } }),
  );
  queueUrl = created.QueueUrl!;
  await sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl })).catch(() => {});
});

afterAll(async () => {
  await Promise.all([api.close(), instanceB.close(), instanceC.close()]);
  sqs.destroy();
  await closeTestOrm();
});

beforeEach(async () => {
  // Outros arquivos de teste deixam eventos pendentes na outbox. Para cada teste
  // daqui enxergar SÓ os eventos que ele criou, marcamos o resto como publicado.
  await sql("update outbox_messages set published_at = now() where published_at is null");
  await drainQueue();
});

afterEach(() => {
  mock.restore(); // desfaz todos os spyOn
});

// ============================================================================ helpers

interface PendingEvent {
  id: string;
  aggregateId: string;
  payload: Record<string, unknown>;
}

/** Insere um evento pendente direto na outbox (por padrão, já vencido há 1s). */
async function insertPending(options: { nextAttemptAt?: Date } = {}): Promise<PendingEvent> {
  const id = newId();
  const aggregateId = newId();
  const occurredAt = new Date();
  const payload = {
    eventId: id,
    eventType: "TestEvent",
    aggregateId,
    correlationId: `corr-${id}`,
    occurredAt: occurredAt.toISOString(),
    version: 1,
    data: { test: true },
  };
  await sql(
    `insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at, next_attempt_at)
     values (?, ?, 'TestEvent', ?::jsonb, ?, ?)`,
    [id, aggregateId, JSON.stringify(payload), occurredAt, options.nextAttemptAt ?? new Date(Date.now() - 1_000)],
  );
  return { id, aggregateId, payload };
}

async function insertPendingMany(count: number): Promise<PendingEvent[]> {
  const events: PendingEvent[] = [];
  for (let i = 0; i < count; i++) events.push(await insertPending());
  return events;
}

async function outboxRow(id: string): Promise<any> {
  const [row] = await sql("select * from outbox_messages where id = ?", [id]);
  return row;
}

async function pendingCount(ids: string[]): Promise<number> {
  if (ids.length === 0) return 0;
  const [{ count }] = await sql(
    `select count(*)::int as count from outbox_messages
      where published_at is null and id in (${ids.map(() => "?").join(", ")})`,
    ids,
  );
  return count;
}

interface ReceivedMessage {
  body: any;
  groupId?: string;
  deduplicationId?: string;
}

/** Lê (e apaga) mensagens da fila até chegarem `expected` ou estourar o tempo. */
async function receive(expected: number, timeoutMs = 5_000): Promise<ReceivedMessage[]> {
  const received: ReceivedMessage[] = [];
  const deadline = Date.now() + timeoutMs;
  while (received.length < expected && Date.now() < deadline) {
    const response = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, AttributeNames: ["All"] }),
    );
    for (const message of response.Messages ?? []) {
      received.push({
        body: JSON.parse(message.Body!),
        groupId: message.Attributes?.MessageGroupId,
        deduplicationId: message.Attributes?.MessageDeduplicationId,
      });
      await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle! }));
    }
  }
  return received;
}

/** Confere que não sobrou nada na fila (espera 1s por alguma mensagem atrasada). */
async function expectQueueEmpty(): Promise<void> {
  const response = await sqs.send(
    new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }),
  );
  expect(response.Messages ?? []).toEqual([]);
}

async function drainQueue(): Promise<void> {
  for (;;) {
    const response = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 0 }),
    );
    if (!response.Messages?.length) return;
    for (const message of response.Messages) {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle! }));
    }
  }
}

/**
 * Espiona o publisher REAL (SQS) de uma instância. `before` roda antes de cada
 * publicação: dá para atrasar (para forçar paralelismo) ou lançar erro (SQS fora).
 */
function spyPublisher(app: TestApp, before?: (message: OutboxMessage) => Promise<void> | void) {
  const publisher = app.app.get(EventPublisher);
  const original = publisher.publish.bind(publisher);
  return spyOn(publisher, "publish").mockImplementation(async (message: OutboxMessage) => {
    await before?.(message);
    return original(message);
  });
}

const publishedIds = (spy: ReturnType<typeof spyPublisher>) => spy.mock.calls.map(([message]) => message.id);

const publishBatch = (app: TestApp) => app.app.get(PublishOutboxBatchUseCase);

const TIMED_OUT = "TIMED_OUT" as const;

/** O resultado da promise, ou TIMED_OUT se ela demorar mais que `ms`. */
function withinTimeout<T>(promise: Promise<T>, ms: number): Promise<T | typeof TIMED_OUT> {
  return Promise.race([promise, Bun.sleep(ms).then(() => TIMED_OUT)]);
}

async function waitUntil(check: () => Promise<boolean> | boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("timeout esperando a condição");
    await Bun.sleep(25);
  }
}

/** Uma aposta de verdade pelo HTTP. Devolve a wallet (os eventos têm data.walletId). */
async function placeBet(): Promise<string> {
  const playerId = newId();
  const wallet = await api.post("/wallets", { playerId, initialBalance: { amount: "100.00", currency: "BRL" } });
  const externalTransactionId = `tx-${newId()}`;
  const bet = await api.post(
    "/wagering/transactions",
    {
      providerId: "provider-outbox",
      externalTransactionId,
      playerId,
      walletId: wallet.body.id,
      roundId: "round-1",
      gameId: "game-1",
      kind: "BET",
      money: { amount: "10.00", currency: "BRL" },
    },
    { "idempotency-key": `provider-outbox:${externalTransactionId}`, "x-correlation-id": `corr-${externalTransactionId}` },
  );
  expect(bet.status).toBe(201);
  return wallet.body.id;
}

async function outboxOfWallet(walletId: string): Promise<any[]> {
  return sql("select * from outbox_messages where payload -> 'data' ->> 'walletId' = ? order by id", [walletId]);
}

// ============================================================================ testes

describe("publicação de ponta a ponta (Postgres → SQS real)", () => {
  test("aposta pelo HTTP → uma volta → os 2 eventos chegam na fila, iguais ao que está na outbox", async () => {
    const walletId = await placeBet();
    const rows = await outboxOfWallet(walletId);
    expect(rows.map((r) => r.event_type).sort()).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);

    const result = await publishBatch(api).execute(20);

    expect(result).toEqual({ claimed: 2, published: 2, failed: 0 });
    const received = await receive(2);
    expect(received).toHaveLength(2);
    for (const row of rows) {
      const message = received.find((m) => m.body.eventId === row.id)!;
      // O corpo é EXATAMENTE o envelope do evento, sem campos internos da outbox.
      expect(message.body).toEqual(row.payload);
      expect(message.body).not.toHaveProperty("_attempts");
      expect(message.body).not.toHaveProperty("attempts");
      // FIFO: ordena por agregado; dedup pelo id do evento.
      expect(message.groupId).toBe(row.aggregate_id);
      expect(message.deduplicationId).toBe(row.id);
    }
    for (const row of await outboxOfWallet(walletId)) {
      expect(row.published_at).not.toBeNull();
      expect(row.attempts).toBe(0);
      expect(row.last_error).toBeNull();
    }
  });

  test("o correlationId do header HTTP chega até a mensagem na fila", async () => {
    const walletId = await placeBet();
    const [row] = await outboxOfWallet(walletId);

    await publishBatch(api).execute(20);
    const received = await receive(2);

    expect(received.every((m) => m.body.correlationId === row.payload.correlationId)).toBe(true);
    expect(row.payload.correlationId).toStartWith("corr-tx-");
  });

  test("a volta seguinte não pega nada: evento publicado não sai de novo", async () => {
    await insertPending();
    await publishBatch(api).execute(20);
    await receive(1);

    expect(await publishBatch(api).execute(20)).toEqual({ claimed: 0, published: 0, failed: 0 });
    await expectQueueEmpty();
  });

  test("sem nada pendente → volta vazia, sem erro", async () => {
    expect(await publishBatch(api).execute(20)).toEqual({ claimed: 0, published: 0, failed: 0 });
  });
});

describe("o que entra no lote", () => {
  test("evento com next_attempt_at no futuro não é pego antes da hora", async () => {
    const future = await insertPending({ nextAttemptAt: new Date(Date.now() + 60_000) });

    expect((await publishBatch(api).execute(20)).claimed).toBe(0);
    expect((await outboxRow(future.id)).published_at).toBeNull();
  });

  test("respeita o tamanho do lote e pega os mais antigos (next_attempt_at) primeiro", async () => {
    const now = Date.now();
    const newest = await insertPending({ nextAttemptAt: new Date(now - 1_000) });
    const oldest = await insertPending({ nextAttemptAt: new Date(now - 3_000) });
    const middle = await insertPending({ nextAttemptAt: new Date(now - 2_000) });
    const spy = spyPublisher(api);

    const result = await publishBatch(api).execute(2);

    expect(result).toEqual({ claimed: 2, published: 2, failed: 0 });
    expect(publishedIds(spy)).toEqual([oldest.id, middle.id]);
    expect((await outboxRow(newest.id)).published_at).toBeNull();

    expect((await publishBatch(api).execute(2)).claimed).toBe(1); // sobrou o mais novo
    await receive(3);
  });
});

describe("falha no SQS: retry com backoff", () => {
  test("um evento falha → só ele é reagendado; os outros do lote são publicados", async () => {
    const [first, failing, last] = await insertPendingMany(3);
    spyPublisher(api, (message) => {
      if (message.id === failing!.id) throw new Error("SQS indisponível");
    });

    const before = Date.now();
    const result = await publishBatch(api).execute(20);
    const after = Date.now();

    expect(result).toEqual({ claimed: 3, published: 2, failed: 1 });
    expect((await outboxRow(first!.id)).published_at).not.toBeNull();
    expect((await outboxRow(last!.id)).published_at).not.toBeNull();

    const row = await outboxRow(failing!.id);
    expect(row.published_at).toBeNull();
    expect(row.attempts).toBe(1);
    expect(row.last_error).toContain("SQS indisponível");
    // 1ª falha → espera 1s
    expect(new Date(row.next_attempt_at).getTime()).toBeGreaterThanOrEqual(before + 1_000);
    expect(new Date(row.next_attempt_at).getTime()).toBeLessThanOrEqual(after + 1_000);

    const received = await receive(2);
    expect(received.map((m) => m.body.eventId).sort()).toEqual([first!.id, last!.id].sort());
  });

  test("o reagendado espera o backoff; vencido, é publicado; a 2ª falha dobra a espera", async () => {
    const event = await insertPending();
    let sqsDown = true;
    spyPublisher(api, () => {
      if (sqsDown) throw new Error("SQS indisponível");
    });

    await publishBatch(api).execute(20); // 1ª falha
    expect((await publishBatch(api).execute(20)).claimed).toBe(0); // ainda dentro do backoff

    // "o tempo passa": o backoff venceu
    await sql("update outbox_messages set next_attempt_at = now() - interval '1 second' where id = ?", [event.id]);
    const before = Date.now();
    await publishBatch(api).execute(20); // 2ª falha
    const second = await outboxRow(event.id);
    expect(second.attempts).toBe(2);
    expect(new Date(second.next_attempt_at).getTime()).toBeGreaterThanOrEqual(before + 2_000); // 1s → 2s

    // o SQS volta
    sqsDown = false;
    await sql("update outbox_messages set next_attempt_at = now() - interval '1 second' where id = ?", [event.id]);
    expect(await publishBatch(api).execute(20)).toEqual({ claimed: 1, published: 1, failed: 0 });

    const published = await outboxRow(event.id);
    expect(published.published_at).not.toBeNull();
    expect(published.attempts).toBe(2); // attempts conta as FALHAS
    const [message] = await receive(1);
    expect(message!.body.eventId).toBe(event.id);
  });

  test("SQS fora para o lote todo → nada publicado, tudo reagendado, nenhum erro sobe", async () => {
    const events = await insertPendingMany(5);
    spyPublisher(api, () => {
      throw new Error("connect ECONNREFUSED 127.0.0.1:4566");
    });

    expect(await publishBatch(api).execute(20)).toEqual({ claimed: 5, published: 0, failed: 5 });
    expect(await pendingCount(events.map((e) => e.id))).toBe(5);
    await expectQueueEmpty();
  });
});

describe("publishers concorrentes sobre a mesma outbox", () => {
  test(
    "3 instâncias × 2 publishers em paralelo sobre 100 eventos → cada evento publicado EXATAMENTE uma vez",
    async () => {
      const events = await insertPendingMany(100);
      const ids = events.map((e) => e.id);
      const instances = [api, instanceB, instanceC];
      // Atraso de 2ms por publicação: as transações ficam abertas ao mesmo tempo
      // de verdade, disputando as mesmas linhas.
      const spies = instances.map((app) => spyPublisher(app, () => Bun.sleep(2)));

      // Cada publisher roda voltas até achar o lote vazio.
      const runPublisher = async (app: TestApp) => {
        let published = 0;
        for (;;) {
          const result = await publishBatch(app).execute(10);
          published += result.published;
          if (result.claimed === 0) return published;
        }
      };
      const totals = await Promise.all([...instances, ...instances].map(runPublisher));

      // Contamos as chamadas ao publisher (e não as mensagens na fila), porque a
      // dedup do SQS FIFO esconderia uma publicação duplicada.
      const calls = spies.flatMap(publishedIds);
      expect(calls).toHaveLength(100);
      expect(new Set(calls).size).toBe(100);
      expect([...new Set(calls)].sort()).toEqual([...ids].sort());
      expect(totals.reduce((a, b) => a + b, 0)).toBe(100);
      // O trabalho foi DIVIDIDO (SKIP LOCKED), não feito por um só.
      expect(spies.filter((spy) => spy.mock.calls.length > 0).length).toBeGreaterThanOrEqual(2);

      expect(await pendingCount(ids)).toBe(0);
      const received = await receive(100, 15_000);
      expect(new Set(received.map((m) => m.body.eventId)).size).toBe(100);
    },
    30_000,
  );

  test("enquanto uma instância segura o lote, a outra PULA (não espera nem duplica)", async () => {
    const event = await insertPending();
    const orm = await getTestOrm(); // um terceiro pool, como outro processo
    const repository = new OutboxMessageRepository();

    let release!: () => void;
    const hold = new Promise<void>((resolve) => (release = resolve));
    let claimedByOther: OutboxMessage[] | undefined;
    const other = orm.em.fork().transactional(async (tx) => {
      claimedByOther = await repository.claimDue(tx, new Date(), 10);
      await hold;
    });
    await waitUntil(() => claimedByOther !== undefined);
    expect(claimedByOther!.map((m) => m.id)).toEqual([event.id]);

    try {
      // Se o lock fosse FOR UPDATE puro (sem SKIP LOCKED), a volta ficaria PARADA
      // esperando: o teste desiste depois de 2s em vez de travar a suíte.
      const result = await withinTimeout(publishBatch(api).execute(10), 2_000);
      expect(result).not.toBe(TIMED_OUT);
      expect(result).toEqual({ claimed: 0, published: 0, failed: 0 });
    } finally {
      release(); // solta o lock aconteça o que acontecer
      await other;
    }
  });
});

describe("recuperação: o processo morre no meio", () => {
  test("morreu ANTES do commit e depois do SQS → a linha volta a ficar livre e outra instância republica; a duplicata é segura", async () => {
    // Passo 1: o commit da aposta já aconteceu (o evento está na outbox).
    const event = await insertPending();

    // Passo 2: a instância A publica no SQS e "morre" antes de gravar o published_at.
    const spyA = spyPublisher(api);
    spyOn(api.app.get(OutboxMessageRepository), "update").mockRejectedValue(new Error("processo morreu antes do commit"));
    await expect(publishBatch(api).execute(10)).rejects.toThrow("processo morreu");

    expect(publishedIds(spyA)).toEqual([event.id]); // chegou a sair para o SQS
    const afterCrash = await outboxRow(event.id);
    expect(afterCrash.published_at).toBeNull(); // rollback: continua pendente
    expect(afterCrash.attempts).toBe(0);

    // Passos 3 e 4: a instância B assume e publica de novo.
    const spyB = spyPublisher(instanceB);
    expect(await publishBatch(instanceB).execute(10)).toEqual({ claimed: 1, published: 1, failed: 0 });
    expect(publishedIds(spyB)).toEqual([event.id]);
    expect((await outboxRow(event.id)).published_at).not.toBeNull();

    // Passo 5: a duplicata tem o MESMO eventId (e o mesmo MessageDeduplicationId).
    // O SQS FIFO descarta a cópia dentro de 5 min; depois disso, o consumidor
    // deduplica pelo eventId. Nos dois casos, todas as cópias são o mesmo evento.
    const received = await receive(2, 2_500);
    expect(received.length).toBeGreaterThanOrEqual(1);
    for (const message of received) {
      expect(message.body.eventId).toBe(event.id);
      expect(message.deduplicationId).toBe(event.id);
    }
  });

  test("a transação que travou o lote caiu → o lock solta e outra instância assume na hora", async () => {
    const event = await insertPending();
    const orm = await getTestOrm();
    const repository = new OutboxMessageRepository();

    let crash!: () => void;
    const dies = new Promise<void>((resolve) => (crash = resolve));
    let claimed = false;
    const doomed = orm.em
      .fork()
      .transactional(async (tx) => {
        await repository.claimDue(tx, new Date(), 10);
        claimed = true;
        await dies;
        throw new Error("instância caiu");
      })
      .catch((error: Error) => error);
    await waitUntil(() => claimed);

    try {
      const whileLocked = await withinTimeout(publishBatch(instanceB).execute(10), 2_000);
      expect(whileLocked).toEqual({ claimed: 0, published: 0, failed: 0 }); // travado: pula
    } finally {
      crash();
    }
    expect(((await doomed) as Error).message).toBe("instância caiu");

    expect(await publishBatch(instanceB).execute(10)).toEqual({ claimed: 1, published: 1, failed: 0 });
    const [message] = await receive(1);
    expect(message!.body.eventId).toBe(event.id);
  });

  test("reinicialização: eventos que ficaram pendentes são publicados pela instância que sobe depois", async () => {
    const events = await insertPendingMany(3);

    const restarted = await startTestApp();
    try {
      expect(await publishBatch(restarted).execute(10)).toEqual({ claimed: 3, published: 3, failed: 0 });
    } finally {
      await restarted.close();
    }
    expect(await pendingCount(events.map((e) => e.id))).toBe(0);
    expect(await receive(3)).toHaveLength(3);
  });
});

describe("worker (o loop de verdade)", () => {
  const outboxEnv = env.outbox as { publisherEnabled: boolean };

  /** Liga o worker da instância `api` (no .env.test ele vem desligado). */
  function startWorker(): OutboxPublisherWorker {
    outboxEnv.publisherEnabled = true;
    const worker = api.app.get(OutboxPublisherWorker);
    worker.onApplicationBootstrap();
    return worker;
  }

  async function stopWorker(worker: OutboxPublisherWorker): Promise<void> {
    await worker.onApplicationShutdown();
    outboxEnv.publisherEnabled = false;
  }

  test("desligado (OUTBOX_PUBLISHER_ENABLED=false) → ninguém publica sozinho", async () => {
    const event = await insertPending();
    await Bun.sleep(env.outbox.pollIntervalMs + 300);
    expect((await outboxRow(event.id)).published_at).toBeNull();
  });

  test("ligado → os eventos de uma aposta saem para o SQS sozinhos", async () => {
    const worker = startWorker();
    try {
      const walletId = await placeBet();
      const ids = (await outboxOfWallet(walletId)).map((row) => row.id);

      await waitUntil(async () => (await pendingCount(ids)) === 0, 5_000);
      const received = await receive(2);
      expect(received.map((m) => m.body.eventId).sort()).toEqual([...ids].sort());
    } finally {
      await stopWorker(worker);
    }
  });

  test("o loop não morre com erro (banco fora): loga e tenta de novo na próxima volta", async () => {
    const execute = spyOn(publishBatch(api), "execute").mockRejectedValueOnce(
      Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:5432"), { code: "ECONNREFUSED" }),
    );
    const event = await insertPending();

    const worker = startWorker();
    try {
      await waitUntil(async () => (await outboxRow(event.id)).published_at !== null, 5_000);
      expect(execute.mock.calls.length).toBeGreaterThanOrEqual(2); // falhou 1x e seguiu
    } finally {
      await stopWorker(worker);
    }
    await receive(1);
  });

  test("shutdown (SIGTERM) espera a volta em andamento terminar e para de agendar", async () => {
    const event = await insertPending();
    let publishing = false;
    spyPublisher(api, async () => {
      publishing = true;
      await Bun.sleep(300); // SQS lento: o shutdown chega no meio da publicação
    });

    const worker = startWorker();
    await waitUntil(() => publishing);
    await stopWorker(worker);

    // Quando o shutdown terminou, a volta já tinha feito commit.
    expect((await outboxRow(event.id)).published_at).not.toBeNull();

    // E o loop parou: um evento novo não é mais publicado.
    const later = await insertPending();
    await Bun.sleep(env.outbox.pollIntervalMs + 300);
    expect((await outboxRow(later.id)).published_at).toBeNull();
    await receive(1);
  });
});
