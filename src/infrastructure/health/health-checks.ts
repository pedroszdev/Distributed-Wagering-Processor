import { GetQueueUrlCommand, SQSClient } from "@aws-sdk/client-sqs";
import { EntityManager } from "@mikro-orm/postgresql";
import { Injectable, Logger, type OnApplicationShutdown } from "@nestjs/common";
import { env } from "../../config/env.ts";

/** Quanto um check pode demorar antes de contar como "fora". */
export const HEALTH_CHECK_TIMEOUT_MS = 2_000;

export type DependencyStatus =
  | { status: "up"; latencyMs: number }
  | { status: "down"; latencyMs: number; reason: "timeout" | "unreachable" };

/**
 * As dependências que a instância precisa para atender: Postgres e SQS.
 * Cada check tem timeout: um banco "pendurado" (rede caindo pacotes) não pode
 * deixar o /health/ready pendurado também — o orquestrador precisa da resposta.
 */
@Injectable()
export class HealthChecks implements OnApplicationShutdown {
  private readonly logger = new Logger(HealthChecks.name);

  private readonly sqs = new SQSClient({
    region: env.aws.region,
    endpoint: env.aws.sqsEndpoint,
    maxAttempts: 1, // health check não faz retry: responde o estado de AGORA
    requestHandler: { connectionTimeout: 1_000, requestTimeout: HEALTH_CHECK_TIMEOUT_MS },
  });

  constructor(private readonly em: EntityManager) {}

  onApplicationShutdown(): void {
    this.sqs.destroy();
  }

  /** O Postgres responde a uma consulta? */
  database(): Promise<DependencyStatus> {
    return this.run("database", async () => {
      await this.em.fork().getConnection().execute("select 1");
    });
  }

  /** As filas que esta instância usa existem e respondem? */
  queues(): Promise<DependencyStatus> {
    return this.run("sqs", async () => {
      await Promise.all(
        [env.aws.queueName, env.aws.eventsQueueName].map((QueueName) =>
          this.sqs.send(new GetQueueUrlCommand({ QueueName })),
        ),
      );
    });
  }

  private async run(name: string, check: () => Promise<void>): Promise<DependencyStatus> {
    const startedAt = performance.now();
    const latency = () => Math.round(performance.now() - startedAt);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), HEALTH_CHECK_TIMEOUT_MS);
    });
    try {
      const outcome = await Promise.race([check().then(() => "ok" as const), timeout]);
      if (outcome === "timeout") {
        this.logger.warn(`health check ${name}: sem resposta em ${HEALTH_CHECK_TIMEOUT_MS}ms`);
        return { status: "down", latencyMs: latency(), reason: "timeout" };
      }
      return { status: "up", latencyMs: latency() };
    } catch (error) {
      // O detalhe (host, porta) vai só para o log: o endpoint é aberto, sem autenticação.
      this.logger.warn(`health check ${name} falhou: ${error}`);
      return { status: "down", latencyMs: latency(), reason: "unreachable" };
    } finally {
      clearTimeout(timer);
    }
  }
}
