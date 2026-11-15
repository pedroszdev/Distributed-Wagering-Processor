import { Controller, Get, Header } from "@nestjs/common";
import { Metrics } from "../infrastructure/observability/metrics.ts";

/** GET /metrics: formato de texto do Prometheus (aberto, como os health checks). */
@Controller("metrics")
export class MetricsController {
  constructor(private readonly metrics: Metrics) {}

  @Get()
  @Header("content-type", "text/plain; version=0.0.4; charset=utf-8")
  render(): string {
    return this.metrics.render();
  }
}
