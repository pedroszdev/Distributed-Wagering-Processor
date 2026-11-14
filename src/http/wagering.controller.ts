import { Body, Controller, Headers, Post, Res } from "@nestjs/common";
import type { Response } from "express";
import {
  ProcessWagerTransactionUseCase,
  type WagerTransactionResult,
} from "../application/wagering/process-wager-transaction.use-case.ts";
import { parseWagerTransactionRequest } from "./wager-transaction.request.ts";
import { httpStatusFor } from "./wager-transaction.response.ts";

@Controller()
export class WageringController {
  constructor(private readonly processTransaction: ProcessWagerTransactionUseCase) {}

  @Post("wagering/transactions")
  async submit(
    @Body() body: unknown,
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Headers("x-correlation-id") correlationIdHeader: string | undefined,
    @Res({ passthrough: true }) res: Response,
  ): Promise<WagerTransactionResult> {
    const correlationId = !correlationIdHeader ? Bun.randomUUIDv7() : correlationIdHeader
    res.setHeader("x-correlation-id", correlationId);
    const input = parseWagerTransactionRequest(body, idempotencyKey)
    const result = await this.processTransaction.execute(input, { correlationId })
    res.status(httpStatusFor(result))
    res.setHeader("x-correlation-id", correlationId)
    return result
  }
}
