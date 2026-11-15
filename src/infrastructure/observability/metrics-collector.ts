import { GetQueueAttributesCommand, GetQueueUrlCommand, SQSClient } from "@aws-sdk/client-sqs";
import { EntityManager } from "@mikro-orm/postgresql";
import { Injectable, Logger, type OnApplicationShutdown } from "@nestjs/common";
import { env } from "../../config/env.ts";
import { Metrics } from "./metrics.ts";

const COLLECT_TIMEOUT_MS = 2_000;

/**
 * Os gauges que olham o ESTADO GLOBAL (banco e fila), lidos na hora do scrape:
 * outbox lag, eventos pendentes, PENDING_REFERENCE esperando, profundidade da DLQ.
 *
 * Lidos no scrape (e não mantidos em memória) porque são compartilhados entre as
 * instâncias: qualquer instância responde o número certo. Se uma fonte falhar,
 * o /metrics continua respondendo o resto (e conta a falha).
 */
@Injectable()
export class MetricsCollector implements OnApplicationShutdown {
  private readonly logger = new Logger(MetricsCollector.name);
  private readonly sqs = new SQSClient({
    region: env.aws.region,
    endpoint: env.aws.sqsEndpoint,
    maxAttempts: 1,
    requestHandler: { connectionTimeout: 1_000, requestTimeout: COLLECT_TIMEOUT_MS },
  });

  constructor(
    private readonly em: EntityManager,
    private readonly metrics: Metrics,
  ) {}

  onApplicationShutdown(): void {
    this.sqs.destroy();
  }

  async collect(): Promise<void> {
    await Promise.all([
      this.safely("database", () => this.collectDatabase()),
      this.safely("sqs", () => this.collectDeadLetterQueue()),
    ]);
  }

  private async collectDatabase(): Promise<void> {
    // Um SELECT só para as duas tabelas. Os dois filtros usam os índices parciais
    // (outbox_pending_due_idx e wager_tx_pending_reference_due_idx): baratos.
    const [row] = await this.em.fork().getConnection().execute<
      { outbox_pending: number; outbox_oldest_age: number | null; pending_reference: number }[]
    >(
      `select
         (select count(*)::int from outbox_messages where published_at is null) as outbox_pending,
         (select extract(epoch from now() - min(occurred_at))::float8
            from outbox_messages where published_at is null)                    as outbox_oldest_age,
         (select count(*)::int from wager_transactions where status = 'PENDING_REFERENCE') as pending_reference`,
    );
    this.metrics.outboxPendingMessages.set(row!.outbox_pending);
    this.metrics.outboxOldestPendingAgeSeconds.set(Math.max(0, row!.outbox_oldest_age ?? 0));
    this.metrics.pendingReferenceTransactions.set(row!.pending_reference);
  }

  private async collectDeadLetterQueue(): Promise<void> {
    const { QueueUrl } = await this.sqs.send(new GetQueueUrlCommand({ QueueName: env.aws.dlqName }));
    const { Attributes } = await this.sqs.send(
      new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ["ApproximateNumberOfMessages"] }),
    );
    this.metrics.sqsDeadLetterQueueMessages.set(Number(Attributes?.ApproximateNumberOfMessages ?? 0), {
      queue: env.aws.dlqName,
    });
  }

  private async safely(source: string, collect: () => Promise<void>): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        collect(),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error(`timeout de ${COLLECT_TIMEOUT_MS}ms`)), COLLECT_TIMEOUT_MS);
        }),
      ]);
    } catch (error) {
      this.metrics.metricsCollectionErrors.inc({ source });
      this.logger.warn({ msg: "falha ao coletar métricas", source, error: String(error) });
    } finally {
      clearTimeout(timer);
    }
  }
}
