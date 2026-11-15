import { Global, Module } from "@nestjs/common";
import { HealthController } from "./http/health.controller.ts";
import { MetricsController } from "./http/metrics.controller.ts";
import { HealthChecks } from "./infrastructure/health/health-checks.ts";
import { Metrics } from "./infrastructure/observability/metrics.ts";

/**
 * Health checks e métricas. @Global: qualquer módulo injeta `Metrics` sem
 * precisar importar este módulo (métrica é transversal, está em todo lugar).
 */
@Global()
@Module({
  controllers: [HealthController, MetricsController],
  providers: [Metrics, HealthChecks],
  exports: [Metrics],
})
export class ObservabilityModule {}
