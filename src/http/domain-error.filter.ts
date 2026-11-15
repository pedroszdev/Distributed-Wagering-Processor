import { Catch, HttpStatus, Logger, type ArgumentsHost } from "@nestjs/common";
import type { Response } from "express";
import { DomainError } from "../domain/shared/domain-error.ts";
import { isTransientDatabaseError } from "../infrastructure/database/transient-error.ts";
import { BaseExceptionFilter } from "@nestjs/core";
import { Metrics } from "../infrastructure/observability/metrics.ts";

/**
 * Traduz o `code` do erro de domínio para o status HTTP.
 *
 * O provedor decide se reenvia olhando o STATUS (e o `code`), sem precisar ler a
 * mensagem de texto (seção 9 do README). No dia 2 esta tabela cresce:
 * conflito de idempotência (409), rejeição de negócio (422), pendente (202),
 * falha transitória (503).
 */
const STATUS_BY_CODE: Record<string, HttpStatus> = {
  // payload inválido: corrija antes de reenviar
  INVALID_PAYLOAD: HttpStatus.BAD_REQUEST,
  INVALID_MONEY: HttpStatus.BAD_REQUEST,
  INVALID_AMOUNT: HttpStatus.BAD_REQUEST,
  CURRENCY_MISMATCH: HttpStatus.BAD_REQUEST,
  MISSING_REFERENCE: HttpStatus.BAD_REQUEST,
  UNEXPECTED_REFERENCE: HttpStatus.BAD_REQUEST,
  IDEMPOTENCY_CONFLICT: HttpStatus.CONFLICT, 
  WALLET_NOT_FOUND: HttpStatus.NOT_FOUND,
  WALLET_ALREADY_EXISTS: HttpStatus.CONFLICT,
  CONCURRENT_UPDATE: HttpStatus.SERVICE_UNAVAILABLE,
  WALLET_PLAYER_MISMATCH: HttpStatus.BAD_REQUEST,
  WAGER_TRANSACTION_NOT_FOUND: HttpStatus.NOT_FOUND
};

@Catch()
export class DomainErrorFilter extends BaseExceptionFilter {
  private readonly logger = new Logger(DomainErrorFilter.name);

  constructor(private readonly metrics: Metrics) {
    super(); // o httpAdapterHost do BaseExceptionFilter chega por injeção de propriedade
  }

  override catch(error: unknown, host: ArgumentsHost): void {  
    const response = host.switchToHttp().getResponse<Response>();

    if (error instanceof DomainError) {
      const status = STATUS_BY_CODE[error.code] ?? HttpStatus.INTERNAL_SERVER_ERROR;

      // Erro de domínio sem status mapeado = bug (ex.: INVALID_TRANSACTION_STATE).
      if (status === HttpStatus.INTERNAL_SERVER_ERROR) {
        this.logger.error(`Erro de domínio não mapeado: ${error.code}`, error.stack);
      }

      response.status(status).json({ code: error.code, message: error.message });
      return 
    }

    if (isTransientDatabaseError(error)) {
      this.metrics.transientErrors.inc({ source: "http" });
      this.logger.warn(
        `Falha transitória no banco; respondendo 503: ${(error as Error).message}`,
      );
      response.setHeader('Retry-After', '1')
      response.status(HttpStatus.SERVICE_UNAVAILABLE).json({
        code: "SERVICE_UNAVAILABLE",
        message: "Falha temporária; tente novamente",
      });      
      return
    }

    super.catch(error, host);
  }

}
