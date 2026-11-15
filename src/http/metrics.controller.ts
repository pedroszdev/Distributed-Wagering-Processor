import { Controller, Get, Header } from "@nestjs/common";
import { MetricsCollector } from "../infrastructure/observability/metrics-collector.ts";
import { Metrics } from "../infrastructure/observability/metrics.ts";

/** GET /metrics: formato de texto do Prometheus (aberto, como os health checks). */
@Controller("metrics")
export class MetricsController {
  constructor(
    private readonly metrics: Metrics,
    private readonly collector: MetricsCollector,
  ) {}

  @Get()
  @Header("content-type", "text/plain; version=0.0.4; charset=utf-8")
  async render(): Promise<string> {
    await this.collector.collect(); // gauges do banco/fila, lidos agora
    return this.metrics.render();
  }
}
