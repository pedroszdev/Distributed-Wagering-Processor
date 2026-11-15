import { Injectable, Logger, type BeforeApplicationShutdown, type OnApplicationBootstrap } from "@nestjs/common";
import { ResolvePendingReferencesUseCase } from "../../application/wagering/resolve-pending-references.use-case.ts";
import { env } from "../../config/env.ts";

/**
 * Worker agendado de PENDING_REFERENCE (seção 7.1): roda uma "volta" do use case,
 * espera, roda de novo. Cada instância da API tem o seu; a lease + SKIP LOCKED
 * fazem eles dividirem o trabalho. Mesmo desenho do worker da outbox.
 */
@Injectable()
export class PendingReferenceWorker implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(PendingReferenceWorker.name);
  private running = false;
  private timer?: ReturnType<typeof setTimeout>;
  private currentRun?: Promise<void>;

  constructor(private readonly resolvePending: ResolvePendingReferencesUseCase) {}

  onApplicationBootstrap(): void {
    if (!env.pendingReference.workerEnabled) return;
    this.running = true;
    this.schedule(0);
  }

  /** SIGTERM: para de agendar e espera a volta atual (antes de o ORM fechar). */
  async beforeApplicationShutdown(): Promise<void> {
    this.running = false;
    clearTimeout(this.timer);
    await this.currentRun;
  }

  private schedule(delayMs: number): void {
    if (!this.running) return;
    this.timer = setTimeout(() => {
      this.currentRun = this.tick();
    }, delayMs);
  }

  private async tick(): Promise<void> {
    let delay = env.pendingReference.pollIntervalMs;
    try {
      const result = await this.resolvePending.execute(env.pendingReference.batchSize, env.pendingReference.leaseMs);
      if (result.failed > 0) {
        this.logger.warn(`${result.failed} PENDING_REFERENCE falharam nesta volta (a lease vence e outra volta tenta)`);
      }
      // Lote cheio: provavelmente tem mais esperando, então não dorme.
      if (result.claimed === env.pendingReference.batchSize) delay = 0;
    } catch (error) {
      // Banco fora do ar etc.: loga e tenta na próxima volta. O loop nunca morre.
      this.logger.error(`Falha na volta do worker de PENDING_REFERENCE: ${error}`);
    } finally {
      this.schedule(delay);
    }
  }
}
