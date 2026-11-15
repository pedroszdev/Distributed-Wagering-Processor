import { Body, Controller, Get, Headers, Param, ParseUUIDPipe, Post, Res } from "@nestjs/common";
import type { Response } from "express";
import {
  ProcessWagerTransactionUseCase,
  type WagerTransactionResult,
} from "../application/wagering/process-wager-transaction.use-case.ts";
import { parseWagerTransactionRequest } from "./wager-transaction.request.ts";
import { GetWagerTransactionUseCase } from "../application/wagering/get-wager-transaction.use-case.ts";
import { InvalidPayloadError } from "../application/shared/invalid-payload.error.ts";
import { httpStatusFor, toWagerTransactionView, WagerTransactionView } from "./wager-transaction.response.ts";
import { logContext } from "../infrastructure/observability/log-context.ts";

const transactionIdPipe = new ParseUUIDPipe({
  exceptionFactory: () => new InvalidPayloadError("transactionId deve ser um UUID"),
});

@Controller()
export class WageringController {
  constructor(
    private readonly processTransaction: ProcessWagerTransactionUseCase,
    private readonly getTransaction: GetWagerTransactionUseCase, 
  ) {}

  @Post("wagering/transactions")
  async submit(
    @Body() body: unknown,
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Res({ passthrough: true }) res: Response,
  ): Promise<WagerTransactionResult> {
    // O RequestContextMiddleware já leu (ou gerou) o x-correlation-id, devolveu no
    // header da resposta e abriu o contexto de log: aqui só reaproveitamos.
    const correlationId = logContext.get().correlationId ?? Bun.randomUUIDv7();
    const input = parseWagerTransactionRequest(body, idempotencyKey)
    const result = await this.processTransaction.execute(input, { correlationId })
    res.status(httpStatusFor(result))
    return result
  }

  @Get("wagering/transactions/:transactionId")
  async getById(@Param("transactionId", transactionIdPipe) transactionId: string): Promise<WagerTransactionView> {
    const response = await this.getTransaction.byId(transactionId)
    return toWagerTransactionView(response)

  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  async getByExternalId(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ): Promise<WagerTransactionView> {
    const response = await this.getTransaction.byExternalId(providerId, externalTransactionId)
    return toWagerTransactionView(response)
  }
}
