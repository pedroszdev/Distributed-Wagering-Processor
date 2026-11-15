import { Global, Module } from "@nestjs/common";
import { HealthController } from "./http/health.controller.ts";
import { MetricsController } from "./http/metrics.controller.ts";
import { RequestContextMiddleware } from "./http/request-context.middleware.ts";
import { HealthChecks } from "./infrastructure/health/health-checks.ts";
import { MetricsCollector } from "./infrastructure/observability/metrics-collector.ts";
import { Metrics } from "./infrastructure/observability/metrics.ts";

/**
 * Health checks, métricas e o contexto de requisição. @Global: qualquer módulo
 * injeta `Metrics` sem precisar importar este (métrica está em todo lugar).
 */
@Global()
@Module({
  controllers: [HealthController, MetricsController],
  providers: [Metrics, MetricsCollector, HealthChecks, RequestContextMiddleware],
  exports: [Metrics],
})
export class ObservabilityModule {}
