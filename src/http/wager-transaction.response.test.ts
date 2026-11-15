import { describe, expect, test } from "bun:test";
import { HttpStatus } from "@nestjs/common";
import type { WagerTransactionResult } from "../application/wagering/process-wager-transaction.use-case.ts";
import { WagerTransactionStatus } from "../domain/wager-transaction/wager-transaction.enums.ts";
import { httpStatusFor } from "./wager-transaction.response.ts";

const result = (status: WagerTransactionStatus, idempotentReplay = false): WagerTransactionResult => ({
  transactionId: "tx-1",
  status,
  balance: { amount: "10.00", currency: "BRL" },
  idempotentReplay,
});

describe("httpStatusFor", () => {
  test.each([
    ["PROCESSED nova", WagerTransactionStatus.Processed, false, HttpStatus.CREATED],
    ["PROCESSED replay", WagerTransactionStatus.Processed, true, HttpStatus.OK],
    ["PENDING_REFERENCE nova", WagerTransactionStatus.PendingReference, false, HttpStatus.ACCEPTED],
    ["PENDING_REFERENCE replay", WagerTransactionStatus.PendingReference, true, HttpStatus.ACCEPTED],
    ["REJECTED nova", WagerTransactionStatus.Rejected, false, HttpStatus.UNPROCESSABLE_ENTITY],
    ["REJECTED replay", WagerTransactionStatus.Rejected, true, HttpStatus.UNPROCESSABLE_ENTITY],
  ])("%s", (_label, status, replay, expected) => {
    expect(httpStatusFor(result(status, replay))).toBe(expected);
  });

  test.each([WagerTransactionStatus.Pending, WagerTransactionStatus.Failed])(
    "%s nunca sai do use case; se sair, é bug → 500",
    (status) => {
      expect(httpStatusFor(result(status))).toBe(HttpStatus.INTERNAL_SERVER_ERROR);
    },
  );
});
