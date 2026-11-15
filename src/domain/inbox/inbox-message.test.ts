import { describe, expect, test } from "bun:test";
import { InboxMessage, type ReceiveInboxProps } from "./inbox-message.ts";
import { InboxMessageAlreadyProcessedError, InvalidInboxMessageError } from "./inbox-message.errors.ts";

const T0 = new Date("2026-01-01T12:00:00.000Z");
const at = (ms: number) => new Date(T0.getTime() + ms);

const validProps = (overrides: Partial<ReceiveInboxProps> = {}): ReceiveInboxProps => ({
  messageId: "msg-123",
  consumerName: "wager-transactions-consumer",
  payloadHash: "a".repeat(64),
  receivedAt: T0,
  ...overrides,
});

describe("InboxMessage.receive", () => {
  test("nasce NÃO processada, com os campos recebidos", () => {
    const message = InboxMessage.receive(validProps());

    expect(message.messageId).toBe("msg-123");
    expect(message.consumerName).toBe("wager-transactions-consumer");
    expect(message.payloadHash).toBe("a".repeat(64));
    expect(message.receivedAt).toEqual(T0);
    expect(message.isProcessed()).toBe(false);
    expect(message.processedAt).toBeUndefined();
  });

  test.each([
    ["messageId vazio", { messageId: "" }],
    ["messageId gigante", { messageId: "x".repeat(256) }],
    ["consumerName vazio", { consumerName: "" }],
    ["consumerName gigante", { consumerName: "c".repeat(256) }],
    ["payloadHash vazio", { payloadHash: "" }],
  ])("%s → InvalidInboxMessageError (sem identidade não dá para deduplicar)", (_label, overrides) => {
    expect(() => InboxMessage.receive(validProps(overrides))).toThrow(InvalidInboxMessageError);
  });

  test("255 caracteres ainda é aceito (o limite é inclusivo)", () => {
    expect(() => InboxMessage.receive(validProps({ messageId: "x".repeat(255) }))).not.toThrow();
  });
});

describe("markProcessed", () => {
  test("marca como processada no instante dado", () => {
    const message = InboxMessage.receive(validProps());
    message.markProcessed(at(50));
    expect(message.isProcessed()).toBe(true);
    expect(message.processedAt).toEqual(at(50));
  });

  test("no mesmo instante da chegada é válido (recebida e processada na mesma transação)", () => {
    const message = InboxMessage.receive(validProps());
    message.markProcessed(T0);
    expect(message.processedAt).toEqual(T0);
  });

  test("duas vezes → InboxMessageAlreadyProcessedError, e o primeiro instante é mantido", () => {
    const message = InboxMessage.receive(validProps());
    message.markProcessed(at(10));

    const error = (() => {
      try {
        message.markProcessed(at(20));
      } catch (e) {
        return e;
      }
    })();

    expect(error).toBeInstanceOf(InboxMessageAlreadyProcessedError);
    expect((error as InboxMessageAlreadyProcessedError).code).toBe("INBOX_MESSAGE_ALREADY_PROCESSED");
    expect(message.processedAt).toEqual(at(10));
  });

  test("antes de ter chegado → InvalidInboxMessageError (relógio andando para trás é bug)", () => {
    const message = InboxMessage.receive(validProps());
    expect(() => message.markProcessed(at(-1))).toThrow(InvalidInboxMessageError);
    expect(message.isProcessed()).toBe(false);
  });
});

describe("rehydrate", () => {
  test("reconstrói o estado salvo, inclusive já processada", () => {
    const message = InboxMessage.rehydrate({ ...validProps(), processedAt: at(5) });
    expect(message.isProcessed()).toBe(true);
    expect(message.processedAt).toEqual(at(5));
  });

  test("não revalida (o estado já foi aceito quando foi gravado)", () => {
    expect(() => InboxMessage.rehydrate({ ...validProps(), messageId: "" })).not.toThrow();
  });

  test("uma reidratada e processada não pode ser processada de novo", () => {
    const message = InboxMessage.rehydrate({ ...validProps(), processedAt: at(5) });
    expect(() => message.markProcessed(at(10))).toThrow(InboxMessageAlreadyProcessedError);
  });
});

describe("matchesPayload", () => {
  test("mesmo hash → true; outro → false", () => {
    const message = InboxMessage.receive(validProps());
    expect(message.matchesPayload("a".repeat(64))).toBe(true);
    expect(message.matchesPayload("b".repeat(64))).toBe(false);
  });
});
