import { describe, expect, test } from "bun:test";
import { DeadlockException, UniqueConstraintViolationException } from "@mikro-orm/postgresql";
import { isTransientDatabaseError } from "./transient-error.ts";

const withCode = (code: string, message = "erro") => Object.assign(new Error(message), { code });

describe("isTransientDatabaseError", () => {
  test.each(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE"])("erro de rede %s → transitório", (code) => {
    expect(isTransientDatabaseError(withCode(code))).toBe(true);
  });

  test.each([
    ["57P01", "admin_shutdown"],
    ["57P02", "crash_shutdown"],
    ["57P03", "cannot_connect_now"],
    ["53300", "too_many_connections"],
    ["40001", "serialization_failure"],
    ["40P01", "deadlock_detected"],
    ["55P03", "lock_not_available"],
  ])("SQLSTATE %s (%s) → transitório", (code) => {
    expect(isTransientDatabaseError(withCode(code))).toBe(true);
  });

  test.each(["08000", "08003", "08006", "08P01"])("classe 08 inteira (connection exception): %s → transitório", (code) => {
    expect(isTransientDatabaseError(withCode(code))).toBe(true);
  });

  test.each(["Connection terminated unexpectedly", "timeout exceeded when trying to connect"])(
    "mensagem do pg SEM code: %s → transitório",
    (message) => {
      expect(isTransientDatabaseError(new Error(message))).toBe(true);
    },
  );

  test("exceção do MikroORM embrulhando um deadlock: o code original é copiado → transitório", () => {
    expect(isTransientDatabaseError(new DeadlockException(withCode("40P01", "deadlock detected")))).toBe(true);
  });

  test.each([
    ["unique violation (23505)", withCode("23505")],
    ["check violation (23514)", withCode("23514")],
    ["sintaxe SQL (42601)", withCode("42601")],
    ["code com cara de 08 mas no meio (X08)", withCode("X08")],
    ["bug comum", new TypeError("Cannot read properties of undefined")],
    ["Error sem code", new Error("algo deu errado")],
  ])("%s → NÃO é transitório (reenviar não adianta)", (_label, error) => {
    expect(isTransientDatabaseError(error)).toBe(false);
  });

  test("UniqueConstraintViolationException do MikroORM → não é transitório", () => {
    expect(isTransientDatabaseError(new UniqueConstraintViolationException(withCode("23505")))).toBe(false);
  });

  test.each([
    ["undefined", undefined],
    ["null", null],
    ["string", "ECONNREFUSED"],
    ["objeto com code mas que não é Error", { code: "ECONNREFUSED", message: "x" }],
  ])("%s → false (só Error de verdade)", (_label, value) => {
    expect(isTransientDatabaseError(value)).toBe(false);
  });

  test("code que não é string (ex.: número) não quebra", () => {
    expect(isTransientDatabaseError(Object.assign(new Error("x"), { code: 57 }))).toBe(false);
  });
});
