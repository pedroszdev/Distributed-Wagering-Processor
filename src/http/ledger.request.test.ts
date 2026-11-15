import { describe, expect, test } from "bun:test";
import { InvalidPayloadError } from "../application/shared/invalid-payload.error.ts";
import { encodeLedgerCursor, parseLedgerQuery } from "./ledger.request.ts";

const ID = "01a1db3a-d7bb-73cb-af17-d2af74e3dbe8";

describe("encodeLedgerCursor", () => {
  test("ida e volta: o parse devolve o id original", () => {
    expect(parseLedgerQuery(encodeLedgerCursor(ID), undefined).afterId).toBe(ID);
  });

  test("é opaco e seguro na URL: não é o id cru e só usa A-Z a-z 0-9 - _", () => {
    const cursor = encodeLedgerCursor(ID);
    expect(cursor).not.toBe(ID);
    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
  });
});

describe("parseLedgerQuery: limit", () => {
  test("sem limit → 50", () => {
    expect(parseLedgerQuery(undefined, undefined)).toEqual({ limit: 50 });
  });

  test("limit vazio (?limit=) → 50", () => {
    expect(parseLedgerQuery(undefined, "")).toEqual({ limit: 50 });
  });

  test("o texto vira NÚMERO (senão o use case faria '20' + 1 = '201')", () => {
    const { limit } = parseLedgerQuery(undefined, "20");
    expect(limit).toBe(20);
    expect(typeof limit).toBe("number");
  });

  test.each(["1", "200"])("limites do intervalo: %s é aceito", (value) => {
    expect(parseLedgerQuery(undefined, value).limit).toBe(Number(value));
  });

  test.each(["0", "-1", "201", "2.5", "abc", "1e3", "Infinity"])("limit=%s → InvalidPayloadError", (value) => {
    expect(() => parseLedgerQuery(undefined, value)).toThrow(InvalidPayloadError);
  });
});

describe("parseLedgerQuery: cursor", () => {
  test("sem cursor → sem afterId (começa do início)", () => {
    expect(parseLedgerQuery(undefined, "10")).not.toHaveProperty("afterId");
  });

  test("cursor válido + limit → os dois", () => {
    expect(parseLedgerQuery(encodeLedgerCursor(ID), "10")).toEqual({ afterId: ID, limit: 10 });
  });

  test.each([
    ["texto qualquer", "lixo"],
    ["base64 de algo que não é UUID", Buffer.from("nao-sou-um-uuid").toString("base64url")],
    ["o id CRU, sem codificar", ID],
  ])("%s → InvalidPayloadError (nunca chega no Postgres como uuid inválido)", (_label, cursor) => {
    expect(() => parseLedgerQuery(cursor, undefined)).toThrow(InvalidPayloadError);
  });
});
