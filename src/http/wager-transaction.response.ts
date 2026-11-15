import { HttpStatus } from "@nestjs/common";
import type { WagerTransactionResult } from "../application/wagering/process-wager-transaction.use-case.ts";
import type { MoneyProps } from "../domain/money/money.ts";
import type { FailureCode } from "../domain/wager-transaction/failure-code.ts";
import type { WagerTransaction } from "../domain/wager-transaction/wager-transaction.ts";
import { WagerTransactionKind, WagerTransactionStatus } from "../domain/wager-transaction/wager-transaction.enums.ts";

export interface WagerTransactionView {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  status: WagerTransactionStatus;
  referenceExternalTransactionId: string | null;
  referenceTransactionId: string | null;
  failureCode: FailureCode | null;
  balanceAfter: MoneyProps | null;
  createdAt: string;          // ISO
  processedAt: string | null; // ISO
}

export function toWagerTransactionView(transaction: WagerTransaction): WagerTransactionView {
    return {
        id: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        idempotencyKey: transaction.idempotencyKey,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        roundId: transaction.roundId,
        gameId: transaction.gameId,
        kind: transaction.kind,
        money: transaction.money.toJSON(),
        status: transaction.status,
        referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
        referenceTransactionId: transaction.referenceTransactionId ?? null,
        failureCode: transaction.failureCode ?? null,
        balanceAfter: transaction.balanceAfter?.toJSON() ?? null,
        createdAt: transaction.createdAt.toISOString(),
        processedAt: transaction.processedAt?.toISOString() ?? null
    }
}

export function httpStatusFor(result: WagerTransactionResult): HttpStatus {
  switch (result.status) {
    case WagerTransactionStatus.Processed:
      return result.idempotentReplay ? HttpStatus.OK : HttpStatus.CREATED;
    case WagerTransactionStatus.PendingReference:
      return HttpStatus.ACCEPTED;
    case WagerTransactionStatus.Rejected:
      return HttpStatus.UNPROCESSABLE_ENTITY;
    default:
      return HttpStatus.INTERNAL_SERVER_ERROR;
  }
}
