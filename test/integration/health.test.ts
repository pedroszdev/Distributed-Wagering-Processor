import { afterAll, afterEach, beforeAll, describe, expect, mock, spyOn, test } from "bun:test";
import { CreateQueueCommand, SQSClient } from "@aws-sdk/client-sqs";
import { env } from "../../src/config/env.ts";
import { HEALTH_CHECK_TIMEOUT_MS, HealthChecks } from "../../src/infrastructure/health/health-checks.ts";
import { startTestApp, type TestApp } from "./support/app.ts";
import { closeTestOrm } from "./support/database.ts";

/** Health checks (seção 9): abertos, live separado de ready, Postgres e SQS reais. */

let api: TestApp;

beforeAll(async () => {
  // O ready confere as filas que a instância usa; nos testes, as filas de teste.
  const sqs = new SQSClient({ region: env.aws.region, endpoint: env.aws.sqsEndpoint });
  for (const QueueName of [env.aws.queueName, env.aws.eventsQueueName]) {
    await sqs.send(new CreateQueueCommand({ QueueName, Attributes: { FifoQueue: "true" } }));
  }
  sqs.destroy();
  api = await startTestApp();
});
afterAll(async () => {
  await api.close();
  await closeTestOrm();
});
afterEach(() => {
  mock.restore();
});

const checks = () => api.app.get(HealthChecks);
const connectionRefused = () => Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:5432"), { code: "ECONNREFUSED" });

describe("GET /health/live", () => {
  test("200 enquanto o processo estiver de pé", async () => {
    const res = await api.get("/health/live");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: "ok" });
  });

  test("continua 200 com o banco FORA: reiniciar a instância não conserta o Postgres", async () => {
    spyOn(checks(), "database").mockResolvedValue({ status: "down", latencyMs: 1, reason: "unreachable" });
    expect((await api.get("/health/live")).status).toBe(200);
  });
});

describe("GET /health/ready", () => {
  test("Postgres e SQS de verdade no ar → 200 com o estado e a latência de cada um", async () => {
    const res = await api.get("/health/ready");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      status: "ok",
      checks: {
        database: { status: "up", latencyMs: expect.any(Number) },
        sqs: { status: "up", latencyMs: expect.any(Number) },
      },
    });
  });

  test("banco fora → 503, e o SQS continua aparecendo como up (dá para saber QUAL caiu)", async () => {
    const em = (checks() as any).em;
    spyOn(em, "fork").mockReturnValue({ getConnection: () => ({ execute: () => Promise.reject(connectionRefused()) }) });

    const res = await api.get("/health/ready");

    expect(res.status).toBe(503);
    expect(res.body.status).toBe("unavailable");
    expect(res.body.checks.database).toEqual({ status: "down", latencyMs: expect.any(Number), reason: "unreachable" });
    expect(res.body.checks.sqs.status).toBe("up");
  });

  test("não vaza host nem porta no endpoint aberto (o detalhe fica só no log)", async () => {
    const em = (checks() as any).em;
    spyOn(em, "fork").mockReturnValue({ getConnection: () => ({ execute: () => Promise.reject(connectionRefused()) }) });

    const res = await api.get("/health/ready");

    expect(JSON.stringify(res.body)).not.toContain("127.0.0.1");
    expect(JSON.stringify(res.body)).not.toContain("5432");
  });

  test("fila que não existe (SQS de verdade respondendo erro) → 503 com sqs down", async () => {
    const aws = env.aws as { queueName: string };
    const original = aws.queueName;
    aws.queueName = `nao-existe-${Bun.randomUUIDv7()}.fifo`;
    try {
      const res = await api.get("/health/ready");
      expect(res.status).toBe(503);
      expect(res.body.checks.sqs).toMatchObject({ status: "down", reason: "unreachable" });
      expect(res.body.checks.database.status).toBe("up");
    } finally {
      aws.queueName = original;
    }
  });

  test("dependência PENDURADA (não responde nem dá erro) → 503 por timeout, sem pendurar o endpoint", async () => {
    const em = (checks() as any).em;
    spyOn(em, "fork").mockReturnValue({ getConnection: () => ({ execute: () => new Promise(() => {}) }) });

    const startedAt = Date.now();
    const res = await api.get("/health/ready");
    const took = Date.now() - startedAt;

    expect(res.status).toBe(503);
    expect(res.body.checks.database).toMatchObject({ status: "down", reason: "timeout" });
    expect(took).toBeGreaterThanOrEqual(HEALTH_CHECK_TIMEOUT_MS - 100);
    expect(took).toBeLessThan(HEALTH_CHECK_TIMEOUT_MS + 1_500);
  });

  test("volta para 200 sozinho quando a dependência volta", async () => {
    const em = (checks() as any).em;
    const fork = spyOn(em, "fork").mockReturnValueOnce({
      getConnection: () => ({ execute: () => Promise.reject(connectionRefused()) }),
    });

    expect((await api.get("/health/ready")).status).toBe(503);
    fork.mockRestore();
    expect((await api.get("/health/ready")).status).toBe(200);
  });
});
