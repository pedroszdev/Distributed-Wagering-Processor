import { describe, expect, test } from "bun:test";
import { JsonLogger } from "./json-logger.ts";
import { logContext } from "./log-context.ts";

/** Um logger que escreve num array, para os testes lerem as linhas. */
function capture(level: ConstructorParameters<typeof JsonLogger>[0] = "log") {
  const lines: string[] = [];
  const logger = new JsonLogger(level, (line) => lines.push(line), { service: "test" });
  const entries = () => lines.map((line) => JSON.parse(line));
  return { logger, lines, entries };
}

describe("JsonLogger", () => {
  test("uma linha JSON por log, com time, level, service, context e msg", () => {
    const { logger, lines, entries } = capture();

    logger.log("aplicação no ar", "NestApplication");

    expect(lines).toHaveLength(1);
    expect(lines[0]).not.toContain("\n");
    const [entry] = entries();
    expect(entry).toEqual({ time: expect.any(String), level: "info", service: "test", context: "NestApplication", msg: "aplicação no ar" });
    expect(new Date(entry.time).toISOString()).toBe(entry.time);
  });

  test("mensagem como objeto vira campos de primeiro nível (log estruturado)", () => {
    const { logger, entries } = capture();
    logger.log({ msg: "mensagem processada", messageId: "m-1", durationMs: 12 }, "Consumer");
    expect(entries()[0]).toMatchObject({ context: "Consumer", msg: "mensagem processada", messageId: "m-1", durationMs: 12 });
  });

  test("os níveis do Nest viram os nomes comuns (log → info)", () => {
    const { logger, entries } = capture("verbose");
    logger.verbose("v");
    logger.debug("d");
    logger.log("l");
    logger.warn("w");
    logger.error("e");
    logger.fatal("f");
    expect(entries().map((e) => e.level)).toEqual(["trace", "debug", "info", "warn", "error", "fatal"]);
  });

  test("filtra abaixo do nível mínimo", () => {
    const { logger, entries } = capture("warn");
    logger.debug("não sai");
    logger.log("não sai");
    logger.warn("sai");
    logger.error("sai");
    expect(entries().map((e) => e.msg)).toEqual(["sai", "sai"]);
  });

  test("error(msg, stack, context): o stack vai num campo próprio", () => {
    const { logger, entries } = capture();
    logger.error("falhou", "Error: x\n    at foo", "DomainErrorFilter");
    expect(entries()[0]).toMatchObject({ level: "error", msg: "falhou", stack: "Error: x\n    at foo", context: "DomainErrorFilter" });
  });

  test("error(msg, undefined, context) (o que o Logger do Nest manda sem stack)", () => {
    const { logger, entries } = capture();
    logger.error("falhou", undefined, "Ctx");
    expect(entries()[0]).toEqual({ time: expect.any(String), level: "error", service: "test", context: "Ctx", msg: "falhou" });
  });

  test("um Error como mensagem vira msg + errorName + stack", () => {
    const { logger, entries } = capture();
    logger.error(new TypeError("boom"));
    expect(entries()[0]).toMatchObject({ msg: "boom", errorName: "TypeError", stack: expect.stringContaining("TypeError") });
  });

  test("os campos do logContext entram sozinhos em toda linha", () => {
    const { logger, entries } = capture();
    logContext.run({ correlationId: "corr-1", walletId: "w-1" }, () => {
      logger.log("dentro");
      logContext.assign({ transactionId: "tx-1" });
      logger.log("depois do assign");
    });
    logger.log("fora");

    const [inside, afterAssign, outside] = entries();
    expect(inside).toMatchObject({ correlationId: "corr-1", walletId: "w-1" });
    expect(inside).not.toHaveProperty("transactionId");
    expect(afterAssign).toMatchObject({ correlationId: "corr-1", walletId: "w-1", transactionId: "tx-1" });
    expect(outside).not.toHaveProperty("correlationId");
  });

  test("o contexto atravessa await (é isso que leva o correlationId até o use case)", async () => {
    const { logger, entries } = capture();
    await logContext.run({ correlationId: "corr-async" }, async () => {
      await Bun.sleep(5);
      await Promise.resolve();
      logger.log("depois de awaits");
    });
    expect(entries()[0].correlationId).toBe("corr-async");
  });

  test("campos explícitos do log ganham dos do contexto", () => {
    const { logger, entries } = capture();
    logContext.run({ correlationId: "do-contexto" }, () => logger.log({ msg: "x", correlationId: "explícito" }));
    expect(entries()[0].correlationId).toBe("explícito");
  });

  test.each(["money", "payload", "body", "authorization", "password", "token", "input"])(
    "nunca escreve o campo sensível %s",
    (key) => {
      const { logger, lines } = capture();
      logger.log({ msg: "x", [key]: { amount: "999.99", currency: "BRL" } });
      expect(lines[0]).not.toContain("999.99");
      expect(JSON.parse(lines[0]!)[key]).toBe("[redacted]");
    },
  );

  test("não quebra com referência circular nem BigInt", () => {
    const { logger, entries } = capture();
    const circular: Record<string, unknown> = { msg: "c" };
    circular.self = circular;
    logger.log({ ...circular, big: 10n });
    expect(entries()[0]).toMatchObject({ msg: "c", big: "10" });
  });
});
