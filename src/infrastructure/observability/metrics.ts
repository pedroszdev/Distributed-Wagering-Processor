import { Injectable } from "@nestjs/common";
import { MetricsRegistry } from "./metrics-registry.ts";

/**
 * Todas as métricas da aplicação, num lugar só (o catálogo do GET /metrics).
 * Quem precisa registrar algo injeta `Metrics` e chama o contador certo.
 */
@Injectable()
export class Metrics {
  readonly registry = new MetricsRegistry();

  /** Seção 9: divergências "contabilizadas em métrica". */
  readonly walletReconciliations = this.registry.counter(
    "wallet_reconciliations_total",
    "Reconciliações de wallet executadas, por resultado (consistent | divergent).",
  );

  render(): string {
    return this.registry.render();
  }
}
