import { Injectable } from "@nestjs/common";
import { MetricsRegistry } from "./metrics-registry.ts";

/** Faixas de latência em segundos: de 5ms a 5s. */
const LATENCY_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5];
/** Faixas do lag da outbox (commit → publicado): de 50ms a 5 min. */
const LAG_BUCKETS = [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300];

/**
 * Todas as métricas da aplicação, num lugar só: este arquivo É o catálogo do
 * GET /metrics. Mínimo da seção 12, e onde cada uma é medida:
 *
 *   transações por status   → wager_transactions_total{channel,kind,status}
 *   duplicatas detectadas   → wager_duplicates_total{mechanism}
 *   retries                 → sqs_messages_total{outcome="retried"},
 *                             outbox_publish_failures_total, pending_reference_resolutions_total{outcome="rescheduled"}
 *   mensagens em DLQ        → sqs_dead_letters_total{reason} + sqs_dead_letter_queue_messages (profundidade real)
 *   conflitos de lock       → wallet_lock_conflicts_total{type} + wallet_lock_wait_seconds
 *   outbox lag              → outbox_oldest_pending_age_seconds, outbox_pending_messages, outbox_publish_lag_seconds
 *   latência                → wager_transaction_processing_seconds, sqs_message_processing_seconds,
 *                             http_request_duration_seconds
 *
 * Os contadores são POR INSTÂNCIA (o Prometheus soma as instâncias). Os gauges
 * que dependem do banco/fila (outbox, DLQ, pendentes) são lidos na hora do
 * scrape (metrics-collector.ts), então mostram o estado GLOBAL.
 */
@Injectable()
export class Metrics {
  readonly registry = new MetricsRegistry();

  // ------------------------------------------------------------------ transações
  readonly wagerTransactions = this.registry.counter(
    "wager_transactions_total",
    "Transações novas processadas, por canal (http | sqs), kind e status final.",
  );
  readonly wagerDuplicates = this.registry.counter(
    "wager_duplicates_total",
    "Duplicatas detectadas e absorvidas sem efeito, por mecanismo (idempotency_replay | inbox).",
  );
  readonly wagerProcessingSeconds = this.registry.histogram(
    "wager_transaction_processing_seconds",
    "Tempo do use case de processamento (inclui espera de lock e o retry interno), por canal.",
    LATENCY_BUCKETS,
  );

  // ------------------------------------------------------------------ concorrência
  readonly walletLockWaitSeconds = this.registry.histogram(
    "wallet_lock_wait_seconds",
    "Quanto tempo se esperou pelo lock da wallet (SELECT ... FOR UPDATE). Sobe numa hot wallet.",
    LATENCY_BUCKETS,
  );
  readonly walletLockConflicts = this.registry.counter(
    "wallet_lock_conflicts_total",
    "Conflitos de concorrência, por tipo (idempotency_race | version_conflict | deadlock | serialization | lock_timeout).",
  );

  // ------------------------------------------------------------------ fila de entrada
  readonly sqsMessages = this.registry.counter(
    "sqs_messages_total",
    "Mensagens da fila de entrada, por desfecho (processed | duplicate | business_error | retried | dead_lettered | released).",
  );
  readonly sqsDeadLetters = this.registry.counter(
    "sqs_dead_letters_total",
    "Mensagens enviadas para a DLQ pelo consumidor (malformadas), por motivo. As que o redrive move estão no gauge da DLQ.",
  );
  readonly sqsProcessingSeconds = this.registry.histogram(
    "sqs_message_processing_seconds",
    "Tempo para processar uma mensagem da fila (parse + use case), por desfecho.",
    LATENCY_BUCKETS,
  );
  readonly sqsDeadLetterQueueMessages = this.registry.gauge(
    "sqs_dead_letter_queue_messages",
    "Mensagens paradas na DLQ agora (ApproximateNumberOfMessages). Lido no scrape.",
  );

  // ------------------------------------------------------------------ outbox
  readonly outboxPublished = this.registry.counter(
    "outbox_messages_published_total",
    "Eventos publicados no SQS pela outbox, por tipo de evento.",
  );
  readonly outboxPublishFailures = this.registry.counter(
    "outbox_publish_failures_total",
    "Falhas ao publicar (cada uma vira um retry com backoff).",
  );
  readonly outboxPublishLagSeconds = this.registry.histogram(
    "outbox_publish_lag_seconds",
    "Tempo entre o evento acontecer (commit) e ser publicado.",
    LAG_BUCKETS,
  );
  readonly outboxPendingMessages = this.registry.gauge(
    "outbox_pending_messages",
    "Eventos ainda não publicados. Lido no scrape.",
  );
  readonly outboxOldestPendingAgeSeconds = this.registry.gauge(
    "outbox_oldest_pending_age_seconds",
    "Idade do evento pendente mais antigo (o outbox lag). 0 = nada pendente. Lido no scrape.",
  );

  // ------------------------------------------------------------------ PENDING_REFERENCE
  readonly pendingReferenceResolutions = this.registry.counter(
    "pending_reference_resolutions_total",
    "Tentativas do worker de PENDING_REFERENCE, por desfecho (processed | rejected | rescheduled | skipped | failed).",
  );
  readonly pendingReferenceTransactions = this.registry.gauge(
    "pending_reference_transactions",
    "Transações esperando a referência agora. Lido no scrape.",
  );

  // ------------------------------------------------------------------ operação
  readonly walletReconciliations = this.registry.counter(
    "wallet_reconciliations_total",
    "Reconciliações de wallet executadas, por resultado (consistent | divergent).",
  );
  readonly transientErrors = this.registry.counter(
    "transient_errors_total",
    "Falhas transitórias de infraestrutura que viraram 503 (HTTP), por origem.",
  );
  readonly httpRequestDurationSeconds = this.registry.histogram(
    "http_request_duration_seconds",
    "Duração das requisições HTTP, por método, rota (o padrão, não a URL) e status.",
    LATENCY_BUCKETS,
  );
  readonly metricsCollectionErrors = this.registry.counter(
    "metrics_collection_errors_total",
    "Falhas ao ler os gauges do banco/fila no scrape, por fonte.",
  );

  render(): string {
    return this.registry.render();
  }
}

/** Classifica um erro de concorrência do Postgres (ou undefined se não for um). */
export function lockConflictType(error: unknown): string | undefined {
  const code = (error as { code?: unknown } | null)?.code;
  switch (code) {
    case "40P01":
      return "deadlock";
    case "40001":
      return "serialization";
    case "55P03":
      return "lock_timeout";
    default:
      return undefined;
  }
}
