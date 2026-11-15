/**
 * Lê e valida as variáveis de ambiente uma única vez.
 * Se faltar algo obrigatório, a aplicação nem sobe (falha rápida).
 */
function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(`Variável de ambiente obrigatória ausente: ${name}`);
  }
  return value;
}

function optional(name: string, fallback: string): string {
  const value = process.env[name];
  return value === undefined || value === "" ? fallback : value;
}

const LOG_LEVELS = ["verbose", "debug", "log", "warn", "error", "fatal"] as const;
type LogLevelName = (typeof LOG_LEVELS)[number];

function logLevel(): LogLevelName {
  const value = optional("LOG_LEVEL", "log");
  if (!(LOG_LEVELS as readonly string[]).includes(value)) {
    throw new Error(`LOG_LEVEL inválido: ${value} (use ${LOG_LEVELS.join(", ")})`);
  }
  return value as LogLevelName;
}

export const env = {
  port: Number(optional("PORT", "3000")),
  log: {
    format: optional("LOG_FORMAT", "json") as "json" | "pretty",
    level: logLevel(),
  },
  databaseUrl: required("DATABASE_URL"),
  aws: {
    region: optional("AWS_REGION", "us-east-1"),
    sqsEndpoint: optional("SQS_ENDPOINT", "http://localhost:4566"),
    queueName: optional("SQS_QUEUE_NAME", "wager-transactions.fifo"),
    dlqName: optional("SQS_DLQ_NAME", "wager-transactions-dlq.fifo"),
    eventsQueueName: optional("SQS_EVENTS_QUEUE_NAME", "wagering-events.fifo"), 
  },
  outbox: {
    publisherEnabled: optional("OUTBOX_PUBLISHER_ENABLED", "true") === "true",
    pollIntervalMs: Number(optional("OUTBOX_POLL_INTERVAL_MS", "500")),
    batchSize: Number(optional("OUTBOX_BATCH_SIZE", "20")),
  },
  sqsConsumer: {
    enabled: optional("SQS_CONSUMER_ENABLED", "true") === "true",
    // Vai para a inbox: (consumer_name, message_id). Trocar o nome = "outro consumidor".
    name: optional("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
    batchSize: Number(optional("SQS_CONSUMER_BATCH_SIZE", "10")), // máximo do SQS
    waitTimeSeconds: Number(optional("SQS_CONSUMER_WAIT_SECONDS", "10")), // long polling
    // Backoff do retry (base · 2^(entrega − 1), com teto). Os testes usam base 1.
    retryBaseDelaySeconds: Number(optional("SQS_RETRY_BASE_DELAY_SECONDS", "5")),
    retryMaxDelaySeconds: Number(optional("SQS_RETRY_MAX_DELAY_SECONDS", "300")),
  },
  pendingReference: {
    workerEnabled: optional("PENDING_REFERENCE_WORKER_ENABLED", "true") === "true",
    pollIntervalMs: Number(optional("PENDING_REFERENCE_POLL_INTERVAL_MS", "1000")),
    batchSize: Number(optional("PENDING_REFERENCE_BATCH_SIZE", "20")),
    // Quanto tempo um item fica reservado para um worker; se ele morrer, outro pega depois disso.
    leaseMs: Number(optional("PENDING_REFERENCE_LEASE_MS", "30000")),
    // Prazo para a referência aparecer; depois, REJECTED com REFERENCE_NOT_FOUND.
    ttlMs: Number(optional("PENDING_REFERENCE_TTL_MS", "300000")),
    retryBaseDelayMs: Number(optional("PENDING_REFERENCE_RETRY_BASE_MS", "1000")),
    retryMaxDelayMs: Number(optional("PENDING_REFERENCE_RETRY_MAX_MS", "60000")),
  },
} as const;
