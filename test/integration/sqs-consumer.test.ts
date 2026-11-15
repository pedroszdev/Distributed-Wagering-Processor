import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, mock, spyOn, test } from "bun:test";
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SetQueueAttributesCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { ProcessWagerTransactionUseCase } from "../../src/application/wagering/process-wager-transaction.use-case.ts";
import { env } from "../../src/config/env.ts";
import type { WagerTransactionInput } from "../../src/http/wager-transaction.request.ts";
import {
  SqsWagerTransactionConsumer,
  type PollResult,
} from "../../src/infrastructure/messaging/sqs-wager-transaction.consumer.ts";
import { Metrics } from "../../src/infrastructure/observability/metrics.ts";
import { JsonLogger } from "../../src/infrastructure/observability/json-logger.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm, sql } from "./support/database.ts";
import { assertLedgerMatchesBalance } from "./support/ledger.ts";
import { newId } from "./support/rows.ts";

/**
 * Consumidor da fila de entrada contra Postgres e SQS (MiniStack) REAIS.
 *
 * O loop fica DESLIGADO (SQS_CONSUMER_ENABLED=false no .env.test): cada teste chama
 * pollOnce() na mão. Só o teste de SIGTERM liga o loop de verdade.
 *
 * A fila de teste tem maxReceiveCount = 2 e o backoff nos testes começa em 1s
 * (SQS_RETRY_BASE_DELAY_SECONDS=1), para retry e DLQ caberem em poucos segundos.
 */

const MAX_RECEIVE_COUNT = 2;
const VISIBILITY_TIMEOUT_SECONDS = 3; // quanto tempo uma mensagem sem ack fica escondida

let api: TestApp;
let instanceB: TestApp;
let instanceC: TestApp;

const sqs = new SQSClient({ region: env.aws.region, endpoint: env.aws.sqsEndpoint });
let queueUrl: string;
let dlqUrl: string;

beforeAll(async () => {
  for (const name of [env.aws.queueName, env.aws.dlqName]) {
    if (!name.endsWith("-test.fifo")) {
      // Trava de segurança: os testes ESVAZIAM essas filas.
      throw new Error(`Testes do consumidor só rodam em filas de teste (recebido: ${name})`);
    }
  }

  // A DLQ primeiro (a fila principal precisa do ARN dela no redrive).
  dlqUrl = (await sqs.send(new CreateQueueCommand({ QueueName: env.aws.dlqName, Attributes: { FifoQueue: "true" } })))
    .QueueUrl!;
  const { Attributes } = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ["QueueArn"] }));
  queueUrl = (await sqs.send(new CreateQueueCommand({ QueueName: env.aws.queueName, Attributes: { FifoQueue: "true" } })))
    .QueueUrl!;
  // SetQueueAttributes (e não no CreateQueue): se a fila já existia com outra
  // configuração, isto a deixa exatamente como os testes esperam.
  await sqs.send(
    new SetQueueAttributesCommand({
      QueueUrl: queueUrl,
      Attributes: {
        VisibilityTimeout: String(VISIBILITY_TIMEOUT_SECONDS),
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: Attributes!.QueueArn, maxReceiveCount: String(MAX_RECEIVE_COUNT) }),
      },
    }),
  );
  for (const url of [queueUrl, dlqUrl]) await sqs.send(new PurgeQueueCommand({ QueueUrl: url })).catch(() => {});

  [api, instanceB, instanceC] = await Promise.all([startTestApp(), startTestApp(), startTestApp()]);
});

afterAll(async () => {
  await Promise.all([api.close(), instanceB.close(), instanceC.close()]);
  sqs.destroy();
  await closeTestOrm();
});

beforeEach(async () => {
  await drain(queueUrl);
  await drain(dlqUrl);
});

afterEach(() => {
  mock.restore();
});

// ============================================================================ helpers

const consumerOf = (app: TestApp) => app.app.get(SqsWagerTransactionConsumer);
const useCaseOf = (app: TestApp) => app.app.get(ProcessWagerTransactionUseCase);

interface TestWallet {
  id: string;
  playerId: string;
}

async function newWallet(amount = "100.00"): Promise<TestWallet> {
  const res = await api.post("/wallets", { playerId: newId(), initialBalance: { amount, currency: "BRL" } });
  expect(res.status).toBe(201);
  return { id: res.body.id, playerId: res.body.playerId };
}

/** O corpo de uma mensagem no formato da seção 10 do desafio. */
function wagerMessage(wallet: TestWallet, kind: string, amount: string, overrides: Record<string, unknown> = {}) {
  const externalTransactionId = `tx-${newId()}`;
  return {
    messageId: `msg-${newId()}`,
    type: "WagerTransactionRequested",
    occurredAt: new Date().toISOString(),
    data: {
      providerId: "provider-queue",
      externalTransactionId,
      idempotencyKey: `provider-queue:${externalTransactionId}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: "round-1",
      gameId: "game-1",
      kind,
      money: { amount, currency: "BRL" },
      ...overrides,
    },
  };
}

type WagerMessage = ReturnType<typeof wagerMessage>;

/**
 * Envia para a fila. Cada envio tem um MessageDeduplicationId NOVO: é o provedor
 * reenviando (o SQS entrega de novo; quem deduplica é a nossa inbox).
 */
async function send(body: WagerMessage | string, groupId: string): Promise<void> {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: typeof body === "string" ? body : JSON.stringify(body),
      MessageGroupId: groupId,
      MessageDeduplicationId: newId(),
    }),
  );
}

function emptyResult(): PollResult {
  return { received: 0, processed: 0, duplicates: 0, businessErrors: 0, retried: 0, deadLettered: 0, released: 0 };
}

function add(total: PollResult, part: PollResult): PollResult {
  for (const key of Object.keys(total) as (keyof PollResult)[]) total[key] += part[key];
  return total;
}

/** Mensagens que chegaram a um destino final (ack ou DLQ). */
const finished = (r: PollResult) => r.processed + r.duplicates + r.businessErrors + r.deadLettered;

/** Chama pollOnce até `expected` mensagens terminarem (ou estourar o tempo). */
async function pollUntilFinished(
  consumer: SqsWagerTransactionConsumer,
  expected: number,
  timeoutMs = 10_000,
): Promise<PollResult> {
  const total = emptyResult();
  const deadline = Date.now() + timeoutMs;
  while (finished(total) < expected && Date.now() < deadline) {
    add(total, await consumer.pollOnce(1));
  }
  return total;
}

async function drain(url: string): Promise<void> {
  for (;;) {
    const response = await sqs.send(new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 0 }));
    if (!response.Messages?.length) return;
    for (const message of response.Messages) {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle! }));
    }
  }
}

interface DeadLetter {
  body: string;
  reason?: string;
}

/** Lê (e apaga) o que estiver na DLQ. */
async function readDeadLetters(expected: number, timeoutMs = 5_000): Promise<DeadLetter[]> {
  const letters: DeadLetter[] = [];
  const deadline = Date.now() + timeoutMs;
  while (letters.length < expected && Date.now() < deadline) {
    const response = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl: dlqUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ["All"] }),
    );
    for (const message of response.Messages ?? []) {
      letters.push({ body: message.Body!, reason: message.MessageAttributes?.deadLetterReason?.StringValue });
      await sqs.send(new DeleteMessageCommand({ QueueUrl: dlqUrl, ReceiptHandle: message.ReceiptHandle! }));
    }
  }
  return letters;
}

/** Confere que a fila principal está vazia (nada esquecido sem ack). */
async function expectQueueEmpty(waitSeconds = 1): Promise<void> {
  const response = await sqs.send(
    new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: waitSeconds }),
  );
  expect(response.Messages ?? []).toEqual([]);
}

async function balanceOf(walletId: string): Promise<string> {
  const [row] = await sql("select balance_amount from wallets where id = ?", [walletId]);
  return row.balance_amount;
}

async function transactionsOf(walletId: string): Promise<any[]> {
  return sql("select * from wager_transactions where wallet_id = ? and kind <> 'OPENING' order by created_at, id", [walletId]);
}

async function inboxCount(messageId: string): Promise<number> {
  const [{ count }] = await sql("select count(*)::int as count from inbox_messages where message_id = ?", [messageId]);
  return count;
}

const transientError = () => Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:5432"), { code: "ECONNREFUSED" });

async function waitUntil(check: () => Promise<boolean> | boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("timeout esperando a condição");
    await Bun.sleep(25);
  }
}

// ============================================================================ testes

describe("caminho feliz", () => {
  test("mensagem válida → mesmo use case do HTTP, ack, inbox gravada, eventos com causationId = messageId", async () => {
    const wallet = await newWallet("100.00");
    const message = wagerMessage(wallet, "BET", "25.00");
    await send(message, wallet.id);

    const result = await pollUntilFinished(consumerOf(api), 1);

    expect(result).toMatchObject({ received: 1, processed: 1, retried: 0, deadLettered: 0 });
    expect(await balanceOf(wallet.id)).toBe("75.00");
    const [transaction] = await transactionsOf(wallet.id);
    expect(transaction).toMatchObject({
      external_transaction_id: message.data.externalTransactionId,
      idempotency_key: message.data.idempotencyKey,
      status: "PROCESSED",
    });
    expect(await inboxCount(message.messageId)).toBe(1);

    const events = await sql("select payload from outbox_messages where payload ->> 'causationId' = ?", [message.messageId]);
    expect(events.map((e) => e.payload.eventType).sort()).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
    expect(events.every((e) => e.payload.correlationId === message.messageId)).toBe(true);

    await expectQueueEmpty();
    await assertLedgerMatchesBalance(wallet.id);
  });

  test("o mesmo pedido pela fila e pelo HTTP → um único débito (a idempotency key vale para os dois canais)", async () => {
    const wallet = await newWallet("100.00");
    const message = wagerMessage(wallet, "BET", "10.00");
    await send(message, wallet.id);
    await pollUntilFinished(consumerOf(api), 1);

    const http = await api.post("/wagering/transactions", message.data, { "idempotency-key": message.data.idempotencyKey });

    expect(http.status).toBe(200); // replay
    expect(http.body.idempotentReplay).toBe(true);
    expect(await balanceOf(wallet.id)).toBe("90.00");
  });

  test("REJECTED (sem saldo) é resultado final → ack, não vai para retry nem DLQ", async () => {
    const wallet = await newWallet("5.00");
    await send(wagerMessage(wallet, "BET", "10.00"), wallet.id);

    const result = await pollUntilFinished(consumerOf(api), 1);

    expect(result).toMatchObject({ processed: 1, retried: 0, deadLettered: 0 });
    expect((await transactionsOf(wallet.id))[0]).toMatchObject({ status: "REJECTED", failure_code: "INSUFFICIENT_FUNDS" });
    await expectQueueEmpty();
  });

  test("REFUND pela fila ANTES da BET → PENDING_REFERENCE e ack; a BET chega depois e é processada", async () => {
    const wallet = await newWallet("100.00");
    const betExternalId = `bet-${newId()}`;
    const refund = wagerMessage(wallet, "REFUND", "10.00", { referenceExternalTransactionId: betExternalId });
    const betMessage = wagerMessage(wallet, "BET", "10.00", {
      externalTransactionId: betExternalId,
      idempotencyKey: `provider-queue:${betExternalId}`,
    });
    await send(refund, wallet.id);
    await send(betMessage, wallet.id);

    const result = await pollUntilFinished(consumerOf(api), 2);

    expect(result).toMatchObject({ processed: 2, retried: 0, deadLettered: 0 });
    const statuses = (await transactionsOf(wallet.id)).map((t) => [t.kind, t.status]);
    expect(statuses).toEqual([
      ["REFUND", "PENDING_REFERENCE"], // fica para o worker de PENDING_REFERENCE resolver
      ["BET", "PROCESSED"],
    ]);
    expect(await balanceOf(wallet.id)).toBe("90.00");
    await assertLedgerMatchesBalance(wallet.id);
  });
});

describe("inbox e redelivery", () => {
  test("o provedor reenviou a MESMA mensagem → 1 processada + 1 duplicata, débito único", async () => {
    const wallet = await newWallet("100.00");
    const message = wagerMessage(wallet, "BET", "10.00");
    await send(message, wallet.id);
    await send(message, wallet.id);

    const result = await pollUntilFinished(consumerOf(api), 2);

    expect(result).toMatchObject({ processed: 1, duplicates: 1, retried: 0 });
    expect(await balanceOf(wallet.id)).toBe("90.00");
    expect(await transactionsOf(wallet.id)).toHaveLength(1);
    expect(await inboxCount(message.messageId)).toBe(1);
    await expectQueueEmpty();
  });

  test(
    "a mesma mensagem 12× consumida por 3 instâncias EM PARALELO → 1 processada, 11 duplicatas, débito único",
    async () => {
      const wallet = await newWallet("100.00");
      const message = wagerMessage(wallet, "BET", "10.00");
      // Grupos FIFO diferentes: senão o SQS entregaria uma cópia por vez
      // (o grupo fica bloqueado enquanto uma mensagem dele está em processamento).
      for (let i = 0; i < 12; i++) await send(message, `group-${i}`);

      const total = emptyResult();
      const deadline = Date.now() + 10_000;
      await Promise.all(
        [api, instanceB, instanceC].map(async (app) => {
          while (finished(total) < 12 && Date.now() < deadline) add(total, await consumerOf(app).pollOnce(1));
        }),
      );

      expect(total).toMatchObject({ processed: 1, duplicates: 11, retried: 0 });
      expect(await balanceOf(wallet.id)).toBe("90.00");
      expect(await transactionsOf(wallet.id)).toHaveLength(1);
      await assertLedgerMatchesBalance(wallet.id);
    },
    20_000,
  );

  test(
    "3 instâncias × 30 BETs DIFERENTES na mesma wallet → saldo exato, nenhuma perdida, nenhuma duplicada",
    async () => {
      const wallet = await newWallet("100.00");
      const messages = Array.from({ length: 30 }, () => wagerMessage(wallet, "BET", "5.00"));
      for (const [i, message] of messages.entries()) await send(message, `group-${i % 6}`);

      const total = emptyResult();
      const deadline = Date.now() + 15_000;
      await Promise.all(
        [api, instanceB, instanceC].map(async (app) => {
          while (finished(total) < 30 && Date.now() < deadline) add(total, await consumerOf(app).pollOnce(1));
        }),
      );

      expect(total.processed).toBe(30);
      const transactions = await transactionsOf(wallet.id);
      expect(transactions).toHaveLength(30);
      expect(transactions.filter((t) => t.status === "PROCESSED")).toHaveLength(20); // 20 × 5.00 = 100.00
      expect(transactions.filter((t) => t.status === "REJECTED")).toHaveLength(10);
      expect(await balanceOf(wallet.id)).toBe("0.00");
      await assertLedgerMatchesBalance(wallet.id);
    },
    25_000,
  );

  test(
    "worker morto DEPOIS do commit e ANTES do ack → outra instância recebe de novo, a inbox vira duplicata, débito único",
    async () => {
      const wallet = await newWallet("100.00");
      const message = wagerMessage(wallet, "BET", "10.00");
      await send(message, wallet.id);

      // Instância A: processa e faz commit, mas "morre" na hora de apagar da fila.
      const consumerA = consumerOf(api);
      spyOn(consumerA as any, "delete").mockRejectedValueOnce(new Error("processo morreu antes do ack"));
      const first = await consumerA.pollOnce(1);
      expect(first.received).toBe(1);
      expect(await balanceOf(wallet.id)).toBe("90.00"); // o commit aconteceu
      expect(await inboxCount(message.messageId)).toBe(1);

      // Sem ack, o SQS entrega de novo quando o VisibilityTimeout vence. Quem pega é a B.
      const second = await pollUntilFinished(consumerOf(instanceB), 1, (VISIBILITY_TIMEOUT_SECONDS + 4) * 1_000);

      expect(second).toMatchObject({ duplicates: 1, processed: 0 });
      expect(await balanceOf(wallet.id)).toBe("90.00");
      expect(await transactionsOf(wallet.id)).toHaveLength(1);
      await expectQueueEmpty();
      await assertLedgerMatchesBalance(wallet.id);
    },
    15_000,
  );
});

describe("erros: ack, retry ou DLQ", () => {
  test.each([
    ["não é JSON", () => "{ isso não é json", "INVALID_PAYLOAD"],
    ["tipo desconhecido", (w: TestWallet) => ({ ...wagerMessage(w, "BET", "1.00"), type: "WalletCreated" }), "INVALID_PAYLOAD"],
    ["walletId inválido", (w: TestWallet) => wagerMessage(w, "BET", "1.00", { walletId: "abc" }), "INVALID_PAYLOAD"],
    ["valor inválido", (w: TestWallet) => wagerMessage(w, "BET", "1.001"), "INVALID_MONEY"],
    [
      "moeda diferente da wallet (só o use case sabe)",
      (w: TestWallet) => wagerMessage(w, "BET", "1.00", { money: { amount: "1.00", currency: "USD" } }),
      "CURRENCY_MISMATCH",
    ],
  ])("malformada (%s) → DLQ NA HORA com o motivo, sem retry, nada gravado", async (_label, build, reason) => {
    const wallet = await newWallet("100.00");
    const body = build(wallet);
    await send(body as WagerMessage | string, wallet.id);

    const result = await pollUntilFinished(consumerOf(api), 1);

    expect(result).toMatchObject({ deadLettered: 1, retried: 0, processed: 0 });
    const [letter] = await readDeadLetters(1);
    expect(letter).toEqual({ body: typeof body === "string" ? body : JSON.stringify(body), reason });
    expect(await transactionsOf(wallet.id)).toHaveLength(0);
    await expectQueueEmpty();
  });

  test("erro de negócio (wallet não existe) → ack: tentar de novo daria o mesmo erro; nada na DLQ", async () => {
    const wallet = await newWallet("100.00");
    await send(wagerMessage(wallet, "BET", "10.00", { walletId: newId() }), wallet.id);

    const result = await pollUntilFinished(consumerOf(api), 1);

    expect(result).toMatchObject({ businessErrors: 1, retried: 0, deadLettered: 0 });
    await expectQueueEmpty();
    expect(await readDeadLetters(1, 1_500)).toEqual([]);
  });

  test("conflito de idempotência (mesma key, outro payload) → ack como erro de negócio; o original fica intacto", async () => {
    const wallet = await newWallet("100.00");
    const original = wagerMessage(wallet, "BET", "10.00");
    const conflicting = { ...original, messageId: `msg-${newId()}`, data: { ...original.data, money: { amount: "99.00", currency: "BRL" } } };
    await send(original, wallet.id);
    await send(conflicting, wallet.id);

    const result = await pollUntilFinished(consumerOf(api), 2);

    expect(result).toMatchObject({ processed: 1, businessErrors: 1 });
    expect(await balanceOf(wallet.id)).toBe("90.00");
  });

  test("transitório (banco fora) → NÃO apaga; some pelo backoff; volta e é processada quando o banco volta", async () => {
    const wallet = await newWallet("100.00");
    const message = wagerMessage(wallet, "BET", "10.00");
    await send(message, wallet.id);
    spyOn(useCaseOf(api), "execute").mockRejectedValueOnce(transientError());

    const first = await consumerOf(api).pollOnce(1);
    expect(first).toMatchObject({ received: 1, retried: 1 });
    expect(await balanceOf(wallet.id)).toBe("100.00");

    // Escondida pelo backoff (1s nos testes): uma leitura imediata não a encontra.
    expect((await consumerOf(api).pollOnce(0)).received).toBe(0);

    const second = await pollUntilFinished(consumerOf(api), 1, 5_000);
    expect(second.processed).toBe(1);
    expect(await balanceOf(wallet.id)).toBe("90.00");
    await expectQueueEmpty();
  });

  test(
    "transitório que não passa → depois de maxReceiveCount entregas o SQS (redrive) move para a DLQ; nada gravado",
    async () => {
      const wallet = await newWallet("100.00");
      const message = wagerMessage(wallet, "BET", "10.00");
      await send(message, wallet.id);
      spyOn(useCaseOf(api), "execute").mockRejectedValue(transientError());

      const total = emptyResult();
      let letters: DeadLetter[] = [];
      const deadline = Date.now() + 12_000;
      while (letters.length === 0 && Date.now() < deadline) {
        add(total, await consumerOf(api).pollOnce(1));
        letters = await readDeadLetters(1, 500);
      }

      expect(total.retried).toBe(MAX_RECEIVE_COUNT); // tentou o limite, nem mais nem menos
      expect(letters).toHaveLength(1);
      expect(JSON.parse(letters[0]!.body).messageId).toBe(message.messageId);
      expect(await transactionsOf(wallet.id)).toHaveLength(0);
      await expectQueueEmpty();
    },
    20_000,
  );

  test("bug (erro desconhecido) → retry, não ack: a aposta não pode sumir por causa de um erro que não entendemos", async () => {
    const wallet = await newWallet("100.00");
    await send(wagerMessage(wallet, "BET", "10.00"), wallet.id);
    spyOn(useCaseOf(api), "execute").mockRejectedValueOnce(new TypeError("Cannot read properties of undefined"));

    expect(await consumerOf(api).pollOnce(1)).toMatchObject({ retried: 1, processed: 0 });

    const after = await pollUntilFinished(consumerOf(api), 1, 5_000);
    expect(after.processed).toBe(1);
    expect(await balanceOf(wallet.id)).toBe("90.00");
  });
});

describe("ordem do grupo FIFO", () => {
  test("a 1ª do grupo falha → as seguintes do MESMO grupo voltam para a fila e só são processadas DEPOIS dela", async () => {
    const wallet = await newWallet("100.00");
    const messages = [1, 2, 3].map(() => wagerMessage(wallet, "BET", "10.00"));
    for (const message of messages) await send(message, wallet.id);

    const useCase = useCaseOf(api);
    const original = useCase.execute.bind(useCase);
    const succeeded: string[] = [];
    let failedOnce = false;
    spyOn(useCase, "execute").mockImplementation(async (input: WagerTransactionInput, ...rest) => {
      if (input.externalTransactionId === messages[0]!.data.externalTransactionId && !failedOnce) {
        failedOnce = true;
        throw transientError();
      }
      const result = await original(input, ...rest);
      succeeded.push(input.externalTransactionId);
      return result;
    });

    const first = await consumerOf(api).pollOnce(1);
    expect(first.retried).toBe(1);
    expect(first.processed).toBe(0); // as outras do grupo NÃO passaram na frente
    expect(first.released).toBe(first.received - 1);

    await pollUntilFinished(consumerOf(api), 3, 8_000);

    expect(succeeded).toEqual(messages.map((m) => m.data.externalTransactionId));
    expect(await balanceOf(wallet.id)).toBe("70.00");
  });

  test("a falha num grupo NÃO trava os outros grupos", async () => {
    const walletA = await newWallet("100.00");
    const walletB = await newWallet("100.00");
    const failing = wagerMessage(walletA, "BET", "10.00");
    const other = wagerMessage(walletB, "BET", "10.00");
    await send(failing, walletA.id);
    await send(other, walletB.id);

    const useCase = useCaseOf(api);
    const original = useCase.execute.bind(useCase);
    spyOn(useCase, "execute").mockImplementation(async (input: WagerTransactionInput, ...rest) => {
      if (input.externalTransactionId === failing.data.externalTransactionId) throw transientError();
      return original(input, ...rest);
    });

    const total = emptyResult();
    const deadline = Date.now() + 5_000;
    while (total.processed < 1 && Date.now() < deadline) add(total, await consumerOf(api).pollOnce(1));

    expect(total.processed).toBe(1);
    expect(await balanceOf(walletB.id)).toBe("90.00");
    expect(await balanceOf(walletA.id)).toBe("100.00");
  });
});

describe("observabilidade do consumidor", () => {
  test("métricas por desfecho e DLQ por motivo; logs JSON com messageId, correlationId, transactionId, walletId e providerId", async () => {
    // Uma instância só deste teste, com os logs indo para um array.
    const lines: string[] = [];
    const observed = await startTestApp({ logger: new JsonLogger("log", (line) => lines.push(line)) });
    try {
      const metrics = observed.app.get(Metrics);
      const wallet = await newWallet("100.00");
      const message = wagerMessage(wallet, "BET", "33.33");
      await send(message, wallet.id);
      await send(message, wallet.id); // duplicata
      await send("{ quebrada", "g-bad"); // DLQ

      await pollUntilFinished(consumerOf(observed), 3);

      expect(metrics.sqsMessages.get({ outcome: "processed" })).toBe(1);
      expect(metrics.sqsMessages.get({ outcome: "duplicate" })).toBe(1);
      expect(metrics.sqsMessages.get({ outcome: "dead_lettered" })).toBe(1);
      expect(metrics.sqsDeadLetters.get({ reason: "INVALID_PAYLOAD" })).toBe(1);
      expect(metrics.wagerDuplicates.get({ mechanism: "inbox" })).toBe(1);
      expect(metrics.sqsProcessingSeconds.count({ outcome: "processed" })).toBe(1);

      const entries = lines.map((line) => JSON.parse(line));
      const processed = entries.find((e) => e.msg === "mensagem processada");
      expect(processed).toMatchObject({
        level: "info",
        context: "SqsWagerTransactionConsumer",
        messageId: message.messageId,
        correlationId: message.messageId,
        sqsMessageId: expect.any(String),
        transactionId: expect.any(String),
        walletId: wallet.id,
        providerId: "provider-queue",
        durationMs: expect.any(Number),
      });
      expect(entries.find((e) => e.msg?.includes("DLQ"))).toMatchObject({ level: "error", reason: "INVALID_PAYLOAD" });
      expect(lines.join("\n")).not.toContain("33.33"); // sem o valor da aposta nos logs
    } finally {
      await observed.close();
    }
  });
});

describe("loop e shutdown (SIGTERM)", () => {
  const consumerEnv = env.sqsConsumer as { enabled: boolean };

  test(
    "ligado → processa sozinho; no SIGTERM termina a mensagem em andamento e devolve as outras para a fila",
    async () => {
      const wallet = await newWallet("100.00");
      const messages = [1, 2, 3].map(() => wagerMessage(wallet, "BET", "10.00"));
      for (const message of messages) await send(message, wallet.id);

      const useCase = useCaseOf(api);
      const original = useCase.execute.bind(useCase);
      let started = 0;
      spyOn(useCase, "execute").mockImplementation(async (...args) => {
        started++;
        await Bun.sleep(300); // a 1ª mensagem ainda está em andamento quando o SIGTERM chega
        return original(...args);
      });

      const consumer = consumerOf(api);
      consumerEnv.enabled = true;
      try {
        consumer.onApplicationBootstrap();
        await waitUntil(() => started === 1);

        const stopStartedAt = Date.now();
        await consumer.beforeApplicationShutdown();
        const stopTook = Date.now() - stopStartedAt;

        // A que estava em andamento terminou (commit + ack); as outras não começaram.
        expect(started).toBe(1);
        expect(await balanceOf(wallet.id)).toBe("90.00");
        expect(stopTook).toBeLessThan(2_000); // não esperou o long polling de 10s
      } finally {
        consumerEnv.enabled = false;
      }

      // As outras 2 voltaram para a fila e são processadas por quem sobrar.
      mock.restore();
      const rest = await pollUntilFinished(consumerOf(instanceB), 2, 8_000);
      expect(rest.processed).toBe(2);
      expect(await balanceOf(wallet.id)).toBe("70.00");
      expect(await transactionsOf(wallet.id)).toHaveLength(3);
      await assertLedgerMatchesBalance(wallet.id);
    },
    20_000,
  );

  test("SIGTERM com a fila vazia (parado no long polling) → encerra na hora", async () => {
    const consumer = consumerOf(api);
    consumerEnv.enabled = true;
    try {
      consumer.onApplicationBootstrap();
      await Bun.sleep(300); // entrou no long polling de 10s
      const startedAt = Date.now();
      await consumer.beforeApplicationShutdown();
      expect(Date.now() - startedAt).toBeLessThan(1_000);
    } finally {
      consumerEnv.enabled = false;
    }
  });

  test("desligado (SQS_CONSUMER_ENABLED=false) → ninguém consome sozinho", async () => {
    const wallet = await newWallet("100.00");
    await send(wagerMessage(wallet, "BET", "10.00"), wallet.id);
    await Bun.sleep(1_500);
    expect(await balanceOf(wallet.id)).toBe("100.00");
  });
});
