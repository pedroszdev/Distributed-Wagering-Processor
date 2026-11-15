import { Injectable, Logger, type NestMiddleware } from "@nestjs/common";
import type { NextFunction, Request, Response } from "express";
import { logContext } from "../infrastructure/observability/log-context.ts";
import { Metrics } from "../infrastructure/observability/metrics.ts";

export const CORRELATION_ID_HEADER = "x-correlation-id";
const MAX_CORRELATION_ID_LENGTH = 128;

/**
 * Roda antes de TODA requisição HTTP:
 *
 *  1. correlationId: usa o do header x-correlation-id (se for razoável) ou gera
 *     um novo, e devolve no header da resposta — inclusive em respostas de erro;
 *  2. abre o logContext: todo log escrito durante esta requisição, em qualquer
 *     camada, sai com esse correlationId;
 *  3. no fim, mede a duração (métrica) e escreve uma linha de "access log".
 */
@Injectable()
export class RequestContextMiddleware implements NestMiddleware {
  private readonly logger = new Logger("HTTP");

  constructor(private readonly metrics: Metrics) {}

  use(req: Request, res: Response, next: NextFunction): void {
    const header = req.header(CORRELATION_ID_HEADER);
    // Header gigante ou vazio não vira correlationId (ele vai para logs e eventos).
    const correlationId =
      header && header.length <= MAX_CORRELATION_ID_LENGTH ? header : Bun.randomUUIDv7();
    res.setHeader(CORRELATION_ID_HEADER, correlationId);

    const startedAt = performance.now();
    res.on("finish", () => {
      const seconds = (performance.now() - startedAt) / 1_000;
      // A ROTA ("/wallets/:walletId"), nunca a URL: um id por série explodiria a
      // quantidade de séries no Prometheus.
      const route = (req.route?.path as string | undefined) ?? "unmatched";
      const status = res.statusCode;
      this.metrics.httpRequestDurationSeconds.observe(seconds, { method: req.method, route, status: String(status) });

      // O 'finish' roda fora do contexto da requisição: os campos vão explícitos.
      const line = { msg: "http request", correlationId, method: req.method, route, status, durationMs: Math.round(seconds * 1_000) };
      if (status >= 500) this.logger.error(line);
      else if (status >= 400) this.logger.warn(line);
      else this.logger.log(line);
    });

    logContext.run({ correlationId }, () => next());
  }
}
