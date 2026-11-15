import { describe, expect, test } from "bun:test";
import {
  DEFAULT_PENDING_REFERENCE_POLICY,
  isPendingReferenceExpired,
  nextPendingAttemptAt,
} from "./pending-reference-policy.ts";

const NOW = new Date("2026-11-15T12:00:00.000Z");
const at = (ms: number) => new Date(NOW.getTime() + ms);

describe("nextPendingAttemptAt (backoff)", () => {
  test.each([
    [0, 1_000], // acabou de chegar → 1s (o que o use case principal agenda)
    [1, 2_000],
    [2, 4_000],
    [3, 8_000],
    [5, 32_000],
    [6, 60_000], // 64s → teto de 60s
    [20, 60_000],
  ])("%i tentativas falhas → próxima em %ims", (failedAttempts, delay) => {
    expect(nextPendingAttemptAt(NOW, failedAttempts)).toEqual(at(delay));
  });

  test("tentativas negativas contam como 0", () => {
    expect(nextPendingAttemptAt(NOW, -2)).toEqual(at(1_000));
  });

  test("respeita a política passada (ex.: a dos testes)", () => {
    const policy = { ttlMs: 10_000, baseDelayMs: 100, maxDelayMs: 300 };
    expect(nextPendingAttemptAt(NOW, 0, policy)).toEqual(at(100));
    expect(nextPendingAttemptAt(NOW, 1, policy)).toEqual(at(200));
    expect(nextPendingAttemptAt(NOW, 2, policy)).toEqual(at(300));
  });
});

describe("isPendingReferenceExpired (TTL)", () => {
  const { ttlMs } = DEFAULT_PENDING_REFERENCE_POLICY;
  const createdAt = NOW;

  test("dentro do prazo → não expirou", () => {
    expect(isPendingReferenceExpired(createdAt, at(ttlMs - 1))).toBe(false);
  });

  test("exatamente no prazo → expirou", () => {
    expect(isPendingReferenceExpired(createdAt, at(ttlMs))).toBe(true);
  });

  test("depois do prazo → expirou", () => {
    expect(isPendingReferenceExpired(createdAt, at(ttlMs + 60_000))).toBe(true);
  });

  test("o padrão é 5 minutos", () => {
    expect(ttlMs).toBe(5 * 60_000);
  });
});
