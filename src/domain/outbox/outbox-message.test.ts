import { describe, expect, test } from "bun:test";
import { IntegrationEvent } from "../shared/integration-events.ts";
import { OutboxMessage, type OutboxMessageState } from "./outbox-message.ts";
import { OutboxMessageAlreadyPublishedError } from "./outbox-message.errors.ts";

/**
 * Um evento de mentira só para o teste. A outbox não se importa com o tipo do
 * evento (ela só guarda o JSON), então um evento simples basta.
 */
class SomethingHappened extends IntegrationEvent<{ value: number }> {
  readonly eventType = "SomethingHappened";
  readonly version = 1;

  static create(overrides: { causationId?: string } = {}) {
    return new SomethingHappened({
      eventId: "event-1",
      aggregateId: "aggregate-1",
      correlationId: "correlation-1",
      occurredAt: new Date("2026-01-01T12:00:00.000Z"),
      data: { value: 42 },
      ...overrides,
    });
  }
}

const T0 = new Date("2026-01-01T12:00:00.000Z");
/** T0 + N milissegundos. */
const at = (ms: number) => new Date(T0.getTime() + ms);

const SECOND = 1_000;
const MINUTE = 60 * SECOND;

const state = (overrides: Partial<OutboxMessageState> = {}): OutboxMessageState => ({
  id: "event-1",
  aggregateId: "aggregate-1",
  eventType: "SomethingHappened",
  payload: SomethingHappened.create().toJSON(),
  occurredAt: T0,
  attempts: 0,
  nextAttemptAt: T0,
  ...overrides,
});

describe("enqueue", () => {
  test("copia o envelope do evento: id da mensagem = eventId", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    expect(message.id).toBe("event-1");
    expect(message.aggregateId).toBe("aggregate-1");
    expect(message.eventType).toBe("SomethingHappened");
    expect(message.occurredAt).toEqual(T0);
  });

  test("payload é o evento inteiro já em JSON (o que vai para o SQS)", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create({ causationId: "message-9" }));

    expect(message.payload).toEqual({
      eventId: "event-1",
      eventType: "SomethingHappened",
      aggregateId: "aggregate-1",
      correlationId: "correlation-1",
      causationId: "message-9",
      occurredAt: "2026-01-01T12:00:00.000Z",
      version: 1,
      data: { value: 42 },
    });
  });

  test("payload sobrevive a JSON.stringify/parse sem mudar (é JSON puro, sem Date nem Money)", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    expect(JSON.parse(JSON.stringify(message.payload))).toEqual(message.payload);
  });

  test("nasce pendente, sem tentativas, sem erro, e já pode ser publicada", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    expect(message.attempts).toBe(0);
    expect(message.nextAttemptAt).toEqual(T0);
    expect(message.publishedAt).toBeUndefined();
    expect(message.lastError).toBeUndefined();
    expect(message.isPending()).toBe(true);
    expect(message.isDue(T0)).toBe(true);
  });
});

describe("rehydrate", () => {
  test("devolve exatamente o estado salvo, inclusive publishedAt e lastError", () => {
    const saved = state({ attempts: 3, nextAttemptAt: at(MINUTE), publishedAt: at(2 * MINUTE), lastError: "timeout" });

    const message = OutboxMessage.rehydrate(saved);

    expect(message.id).toBe(saved.id);
    expect(message.payload).toEqual(saved.payload);
    expect(message.attempts).toBe(3);
    expect(message.nextAttemptAt).toEqual(at(MINUTE));
    expect(message.publishedAt).toEqual(at(2 * MINUTE));
    expect(message.lastError).toBe("timeout");
  });
});

describe("isPending e isDue", () => {
  test("pendente com nextAttemptAt no passado → due", () => {
    const message = OutboxMessage.rehydrate(state({ nextAttemptAt: T0 }));
    expect(message.isDue(at(1))).toBe(true);
  });

  test("pendente com nextAttemptAt EXATAMENTE agora → due (o <= conta o igual)", () => {
    const message = OutboxMessage.rehydrate(state({ nextAttemptAt: T0 }));
    expect(message.isDue(T0)).toBe(true);
  });

  test("pendente com nextAttemptAt no futuro → ainda não", () => {
    const message = OutboxMessage.rehydrate(state({ nextAttemptAt: at(SECOND) }));
    expect(message.isPending()).toBe(true);
    expect(message.isDue(at(SECOND - 1))).toBe(false);
  });

  test("publicada → não está pendente e nunca está due, nem no futuro distante", () => {
    const message = OutboxMessage.rehydrate(state({ publishedAt: T0 }));
    expect(message.isPending()).toBe(false);
    expect(message.isDue(at(365 * 24 * 60 * MINUTE))).toBe(false);
  });
});

describe("markPublished", () => {
  test("guarda a data e a mensagem deixa de ser pendente", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    message.markPublished(at(500));

    expect(message.publishedAt).toEqual(at(500));
    expect(message.isPending()).toBe(false);
    expect(message.isDue(at(MINUTE))).toBe(false);
  });

  test("publicar de novo → OutboxMessageAlreadyPublishedError, e a data original fica", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());
    message.markPublished(at(500));

    expect(() => message.markPublished(at(900))).toThrow(OutboxMessageAlreadyPublishedError);
    expect(message.publishedAt).toEqual(at(500));
  });

  test("funciona depois de várias falhas (o SQS voltou)", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());
    message.scheduleRetry(T0, "SQS fora");
    message.scheduleRetry(at(SECOND), "SQS fora");

    message.markPublished(at(10 * SECOND));

    expect(message.isPending()).toBe(false);
    expect(message.attempts).toBe(2); // o histórico de tentativas não é apagado
  });
});

describe("scheduleRetry: backoff exponencial", () => {
  test("1ª falha: attempts 1, próxima tentativa em 1s", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    message.scheduleRetry(T0);

    expect(message.attempts).toBe(1);
    expect(message.nextAttemptAt).toEqual(at(SECOND));
  });

  test("a espera dobra a cada falha: 1s, 2s, 4s, 8s, 16s", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());
    const waits: number[] = [];

    for (let i = 0; i < 5; i++) {
      message.scheduleRetry(T0);
      waits.push(message.nextAttemptAt.getTime() - T0.getTime());
    }

    expect(waits).toEqual([1, 2, 4, 8, 16].map((s) => s * SECOND));
  });

  test("a espera para no teto de 5 minutos e não passa dele", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    for (let i = 0; i < 30; i++) message.scheduleRetry(T0);

    // 2^29 segundos seriam ~17 anos; o teto segura em 5 minutos.
    expect(message.attempts).toBe(30);
    expect(message.nextAttemptAt).toEqual(at(5 * MINUTE));
  });

  test("a espera conta a partir do 'now' recebido, não do nextAttemptAt anterior", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());
    message.scheduleRetry(T0); // attempts 1

    message.scheduleRetry(at(MINUTE)); // attempts 2 → 2s depois do now

    expect(message.nextAttemptAt).toEqual(at(MINUTE + 2 * SECOND));
  });

  test("continua de onde parou depois do rehydrate (attempts vem do banco)", () => {
    const message = OutboxMessage.rehydrate(state({ attempts: 3 }));

    message.scheduleRetry(T0); // attempts 4 → 8s

    expect(message.attempts).toBe(4);
    expect(message.nextAttemptAt).toEqual(at(8 * SECOND));
  });

  test("logo depois do retry a mensagem NÃO está due; quando chega a hora, está", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    message.scheduleRetry(T0);

    expect(message.isPending()).toBe(true);
    expect(message.isDue(T0)).toBe(false);
    expect(message.isDue(at(SECOND))).toBe(true);
  });

  test("guarda o último erro", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    message.scheduleRetry(T0, "connection refused");
    message.scheduleRetry(T0, "throttled");

    expect(message.lastError).toBe("throttled");
  });

  test("nunca desiste: não existe limite de tentativas (o README proíbe perder evento)", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());

    for (let i = 0; i < 1_000; i++) message.scheduleRetry(T0);

    expect(message.isPending()).toBe(true);
    expect(message.isDue(at(5 * MINUTE))).toBe(true);
  });

  test("retry numa mensagem já publicada → OutboxMessageAlreadyPublishedError, nada muda", () => {
    const message = OutboxMessage.enqueue(SomethingHappened.create());
    message.markPublished(T0);

    expect(() => message.scheduleRetry(at(SECOND), "erro")).toThrow(OutboxMessageAlreadyPublishedError);
    expect(message.attempts).toBe(0);
    expect(message.lastError).toBeUndefined();
  });
});
