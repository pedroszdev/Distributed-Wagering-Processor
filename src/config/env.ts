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

export const env = {
  port: Number(optional("PORT", "3000")),
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
} as const;
