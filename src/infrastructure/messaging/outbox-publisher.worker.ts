import { Injectable, Logger, type OnApplicationBootstrap, type OnApplicationShutdown } from "@nestjs/common";
import { PublishOutboxBatchUseCase } from "../../application/outbox/publish-outbox-batch.use-case.ts";
import { env } from "../../config/env.ts";

/**
 * Loop do publisher: roda uma "volta" (o use case), espera um pouco, roda de novo.
 * Cada instância da API tem o seu; o SKIP LOCKED faz eles dividirem o trabalho.
 */
@Injectable()
export class OutboxPublisherWorker implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(OutboxPublisherWorker.name);
  private running = false;
  private timer?: ReturnType<typeof setTimeout>;
  private currentRun?: Promise<void>;

  constructor(private readonly publishBatch: PublishOutboxBatchUseCase) {}

  onApplicationBootstrap(): void {
    if (!env.outbox.publisherEnabled) return;
    this.running = true;
    this.schedule(0);
  }

  /** SIGTERM: para de agendar e ESPERA a volta atual terminar (commit ou rollback). */
  async onApplicationShutdown(): Promise<void> {
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
    let delay = env.outbox.pollIntervalMs;
    try {
      const result = await this.publishBatch.execute(env.outbox.batchSize);
      if (result.failed > 0) {
        this.logger.warn(`Falharam ${result.failed} mensagens`)
      }

      if (result.claimed === env.outbox.batchSize) delay = 0
    } catch (error) {
      this.logger.error(`Falha na volta do publisher da outbox: ${error}`)
    } finally {
      this.schedule(delay);
    }
  }
}
