import { Controller, Get, HttpStatus, Res } from "@nestjs/common";
import type { Response } from "express";
import { HealthChecks, type DependencyStatus } from "../infrastructure/health/health-checks.ts";
import { Public } from "./auth/provider-auth.guard.ts";

export interface ReadinessResponse {
  status: "ok" | "unavailable";
  checks: { database: DependencyStatus; sqs: DependencyStatus };
}

/**
 * Health checks (seção 9), abertos (sem autenticação, seção 2).
 *
 *   /health/live  → o PROCESSO está vivo? Não olha dependências: se o Postgres
 *                   cair, reiniciar a instância não resolve nada. Se o live
 *                   falhasse junto, o orquestrador reiniciaria tudo em cascata.
 *   /health/ready → a instância CONSEGUE atender agora (Postgres e SQS)?
 *                   503 → o balanceador para de mandar tráfego até voltar.
 */
@Public()
@Controller("health")
export class HealthController {
  constructor(private readonly checks: HealthChecks) {}

  @Get("live")
  live(): { status: "ok" } {
    return { status: "ok" };
  }

  @Get("ready")
  async ready(@Res({ passthrough: true }) res: Response): Promise<ReadinessResponse> {
    const [database, sqs] = await Promise.all([this.checks.database(), this.checks.queues()]);
    const ok = database.status === "up" && sqs.status === "up";
    res.status(ok ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
    return { status: ok ? "ok" : "unavailable", checks: { database, sqs } };
  }
}
