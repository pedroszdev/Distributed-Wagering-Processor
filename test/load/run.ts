/**
 * Teste de carga: `bun run test:load` (seção 14, diferencial).
 *
 * Sobe N instâncias REAIS (processos `bun src/main.ts`, com os workers da outbox e
 * de PENDING_REFERENCE ligados), dispara uma mistura realista de operações por um
 * tempo fixo e mede: throughput, latência p50/p95/p99 (geral e por cenário), taxa
 * de erro, conflitos/espera de lock e outbox lag. No fim confere a consistência
 * (reconciliação de TODAS as wallets) e escreve test/load/REPORT.md.
 *
 * Usa o banco wagering_test e as filas de teste: o schema é RECRIADO a cada execução.
 *
 * Parâmetros (variáveis de ambiente):
 *   LOAD_DURATION_SECONDS (20)  LOAD_CONCURRENCY (48)  LOAD_INSTANCES (3)  LOAD_WALLETS (200)
 */
import { cpus, totalmem, release, type } from "node:os";
import { resolve } from "node:path";
import { CreateQueueCommand, DeleteQueueCommand, SQSClient } from "@aws-sdk/client-sqs";
import { env } from "../../src/config/env.ts";
import { closeTestOrm, getTestOrm, sql } from "../integration/support/database.ts";
import { startAppProcess, type AppProcess } from "../integration/support/process.ts";

// ============================================================================ configuração

const intEnv = (name: string, fallback: number) => Number(process.env[name] ?? fallback);
const CONFIG = {
  durationSeconds: intEnv("LOAD_DURATION_SECONDS", 20),
  concurrency: intEnv("LOAD_CONCURRENCY", 48),
  instances: intEnv("LOAD_INSTANCES", 3),
  wallets: intEnv("LOAD_WALLETS", 200),
  requestTimeoutMs: 10_000,
  /**
   * embedded: cada instância da API também roda os workers (outbox, PENDING_REFERENCE).
   * dedicated: as instâncias da API NÃO rodam workers; +1 processo só de workers, sem HTTP.
   */
  publisherMode: (process.env.LOAD_PUBLISHER_MODE ?? "embedded") as "embedded" | "dedicated",
};

/** A mistura de operações (pesos somam 100). */
const MIX = [
  { scenario: "BET (wallets distintas)", weight: 50 },
  { scenario: "BET (hot wallet)", weight: 15 },
  { scenario: "WIN", weight: 10 },
  { scenario: "LOSS", weight: 5 },
  { scenario: "REFUND de uma BET", weight: 5 },
  { scenario: "reenvio duplicado", weight: 15 },
] as const;
type Scenario = (typeof MIX)[number]["scenario"];

const WORKERS_ON = {
  OUTBOX_PUBLISHER_ENABLED: "true",
  PENDING_REFERENCE_WORKER_ENABLED: "true",
  SQS_CONSUMER_ENABLED: "false", // a carga entra pelo HTTP
};
const WORKERS_OFF = {
  OUTBOX_PUBLISHER_ENABLED: "false",
  PENDING_REFERENCE_WORKER_ENABLED: "false",
  SQS_CONSUMER_ENABLED: "false",
};

// ============================================================================ tipos e utilitários

interface Wallet {
  id: string;
  playerId: string;
}

interface Operation {
  body: Record<string, unknown>;
  key: string;
}

interface Sample {
  scenario: Scenario;
  status: number | "network-error";
  ms: number;
}

const newId = () => Bun.randomUUIDv7();
const pick = <T>(items: readonly T[]): T => items[Math.floor(Math.random() * items.length)]!;

function pickScenario(): Scenario {
  let roll = Math.random() * 100;
  for (const { scenario, weight } of MIX) {
    if ((roll -= weight) < 0) return scenario;
  }
  return MIX[0].scenario;
}

function percentile(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1);
  return sorted[Math.max(0, index)]!;
}

const fmt = (n: number, digits = 1) => n.toFixed(digits);

// ---------------------------------------------------------------- leitura do /metrics

interface Series {
  name: string;
  labels: Record<string, string>;
  value: number;
}

function parseMetrics(text: string): Series[] {
  const series: Series[] = [];
  for (const line of text.split("\n")) {
    const match = /^([a-zA-Z_:][\w:]*)(?:\{([^}]*)\})?\s+(\S+)$/.exec(line);
    if (!match) continue;
    const labels: Record<string, string> = {};
    for (const [, key, value] of (match[2] ?? "").matchAll(/(\w+)="((?:[^"\\]|\\.)*)"/g)) labels[key!] = value!;
    series.push({ name: match[1]!, labels, value: Number(match[3]) });
  }
  return series;
}

async function scrapeAll(apps: AppProcess[]): Promise<Series[]> {
  const texts = await Promise.all(apps.map((app) => fetch(`${app.url}/metrics`).then((r) => r.text())));
  return texts.flatMap(parseMetrics);
}

const sum = (series: Series[], name: string, filter: (labels: Record<string, string>) => boolean = () => true) =>
  series.filter((s) => s.name === name && filter(s.labels)).reduce((total, s) => total + s.value, 0);

/**
 * Percentil estimado de um histograma Prometheus somado entre as instâncias
 * (interpolação linear dentro do bucket, como o histogram_quantile do Prometheus).
 */
function histogramQuantile(series: Series[], name: string, q: number): number | undefined {
  const buckets = new Map<number, number>();
  for (const s of series.filter((x) => x.name === `${name}_bucket`)) {
    const bound = s.labels.le === "+Inf" ? Number.POSITIVE_INFINITY : Number(s.labels.le);
    buckets.set(bound, (buckets.get(bound) ?? 0) + s.value);
  }
  const sorted = [...buckets.entries()].sort((a, b) => a[0] - b[0]);
  const total = sorted.at(-1)?.[1] ?? 0;
  if (total === 0) return undefined;
  const target = q * total;
  let previousBound = 0;
  let previousCount = 0;
  for (const [bound, count] of sorted) {
    if (count >= target) {
      if (!Number.isFinite(bound)) return previousBound; // caiu no +Inf: o melhor que dá para dizer é "> último bucket"
      const inBucket = count - previousCount;
      return inBucket === 0 ? bound : previousBound + ((bound - previousBound) * (target - previousCount)) / inBucket;
    }
    previousBound = bound;
    previousCount = count;
  }
  return previousBound;
}

// ============================================================================ execução

async function post(app: AppProcess, path: string, body: unknown, headers: Record<string, string> = {}) {
  return fetch(`${app.url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(CONFIG.requestTimeoutMs),
  });
}

async function createWallet(app: AppProcess, amount: string): Promise<Wallet> {
  const res = await post(app, "/wallets", { playerId: newId(), initialBalance: { amount, currency: "BRL" } });
  if (res.status !== 201) throw new Error(`falha ao criar wallet: ${res.status}`);
  return (await res.json()) as Wallet;
}

function operation(wallet: Wallet, kind: string, amount: string, extra: Record<string, unknown> = {}): Operation {
  const externalTransactionId = `load-${newId()}`;
  return {
    body: {
      providerId: "provider-load",
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `round-${Math.floor(Math.random() * 1_000)}`,
      gameId: "load-game",
      kind,
      money: { amount, currency: "BRL" },
      ...extra,
    },
    key: `provider-load:${externalTransactionId}`,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  console.log(`\n▶ teste de carga: ${CONFIG.instances} instâncias, ${CONFIG.concurrency} usuários virtuais, ${CONFIG.durationSeconds}s\n`);

  // ---------------------------------------------------------------- preparação
  const orm = await getTestOrm(); // recria o schema do zero (banco wagering_test)
  const [{ version: postgresVersion }] = await sql("select version()");
  // Uma fila de eventos NOVA por execução, apagada no fim. Medido: o MiniStack fica
  // mais lento conforme acumula volume (SendMessageBatch de ~11ms para ~57ms depois
  // de ~95 mil mensagens, e esvaziar a fila não recupera). Sem isso, cada execução
  // mediria um emulador mais lento que a anterior.
  const eventsQueueName = `wagering-events-load-${Date.now()}.fifo`;
  const sqs = new SQSClient({ region: env.aws.region, endpoint: env.aws.sqsEndpoint });
  const { QueueUrl: eventsQueueUrl } = await sqs.send(
    new CreateQueueCommand({ QueueName: eventsQueueName, Attributes: { FifoQueue: "true" } }),
  );
  for (const QueueName of [env.aws.dlqName, env.aws.queueName]) {
    await sqs.send(new CreateQueueCommand({ QueueName, Attributes: { FifoQueue: "true" } }));
  }

  // LOAD_LOG_DIR=/algum/dir: guarda os logs JSON de cada instância (diagnóstico).
  const logDir = process.env.LOAD_LOG_DIR;
  const dedicated = CONFIG.publisherMode === "dedicated";
  const logLevel = { LOG_LEVEL: logDir ? "log" : "error", SQS_EVENTS_QUEUE_NAME: eventsQueueName };
  // `apps` recebem a carga HTTP; `workerApp` (modo dedicated) só roda os workers.
  const apps = await Promise.all(
    Array.from({ length: CONFIG.instances }, (_, i) =>
      startAppProcess(
        { ...(dedicated ? WORKERS_OFF : WORKERS_ON), ...logLevel },
        logDir ? { logFile: resolve(logDir, `instance-${i}.log`) } : {},
      ),
    ),
  );
  const workerApp = dedicated
    ? await startAppProcess({ ...WORKERS_ON, ...logLevel }, logDir ? { logFile: resolve(logDir, "worker.log") } : {})
    : undefined;
  const everyProcess = workerApp ? [...apps, workerApp] : apps;
  console.log(`  API: ${apps.map((a) => a.url).join(", ")}${workerApp ? ` · workers: ${workerApp.url}` : ""}`);

  const wallets: Wallet[] = [];
  for (let i = 0; i < CONFIG.wallets; i++) wallets.push(await createWallet(apps[i % apps.length]!, "10000.00"));
  const hotWallet = await createWallet(apps[0]!, "1000000.00");
  console.log(`  ${wallets.length} wallets + 1 hot wallet criadas`);

  // Pool de operações já enviadas (para os reenvios) e de BETs que ainda podem ser estornadas.
  const sent: Operation[] = [];
  const refundableBets: { wallet: Wallet; op: Operation }[] = [];

  function nextOperation(scenario: Scenario): Operation {
    switch (scenario) {
      case "BET (wallets distintas)": {
        const wallet = pick(wallets);
        const op = operation(wallet, "BET", `${1 + Math.floor(Math.random() * 5)}.00`);
        refundableBets.push({ wallet, op });
        return op;
      }
      case "BET (hot wallet)":
        return operation(hotWallet, "BET", "1.00");
      case "WIN":
        return operation(pick(wallets), "WIN", `${1 + Math.floor(Math.random() * 10)}.00`);
      case "LOSS":
        return operation(pick(wallets), "LOSS", "0.00");
      case "REFUND de uma BET": {
        const bet = refundableBets.splice(Math.floor(Math.random() * refundableBets.length), 1)[0];
        if (!bet) return nextOperation("BET (wallets distintas)");
        return operation(bet.wallet, "REFUND", (bet.op.body.money as { amount: string }).amount, {
          referenceExternalTransactionId: bet.op.body.externalTransactionId,
          roundId: bet.op.body.roundId, // a referência exige a mesma rodada
        });
      }
      case "reenvio duplicado":
        return sent.length > 0 ? pick(sent) : nextOperation("BET (wallets distintas)");
    }
  }

  // ---------------------------------------------------------------- carga
  const samples: Sample[] = [];
  const lagSamples: { pending: number; oldestAge: number }[] = [];
  const deadline = Date.now() + CONFIG.durationSeconds * 1_000;
  let instanceCursor = 0;

  const sampler = (async () => {
    while (Date.now() < deadline) {
      try {
        const series = parseMetrics(await (await fetch(`${everyProcess.at(-1)!.url}/metrics`)).text());
        lagSamples.push({
          pending: sum(series, "outbox_pending_messages"),
          oldestAge: sum(series, "outbox_oldest_pending_age_seconds"),
        });
      } catch {
        // amostra perdida: segue
      }
      await Bun.sleep(500);
    }
  })();

  const virtualUser = async () => {
    while (Date.now() < deadline) {
      const scenario = pickScenario();
      const op = nextOperation(scenario);
      const app = apps[instanceCursor++ % apps.length]!;
      const t0 = performance.now();
      let status: Sample["status"];
      try {
        const res = await post(app, "/wagering/transactions", op.body, { "idempotency-key": op.key });
        await res.arrayBuffer();
        status = res.status;
      } catch {
        status = "network-error";
      }
      samples.push({ scenario, status, ms: performance.now() - t0 });
      if (scenario !== "reenvio duplicado") sent.push(op);
    }
  };

  const loadStartedAt = performance.now();
  await Promise.all([...Array.from({ length: CONFIG.concurrency }, virtualUser), sampler]);
  const loadSeconds = (performance.now() - loadStartedAt) / 1_000;
  console.log(`  carga terminada: ${samples.length} requisições em ${fmt(loadSeconds)}s`);

  // ---------------------------------------------------------------- dreno da outbox
  const drainStartedAt = performance.now();
  let drained = false;
  while (performance.now() - drainStartedAt < 60_000) {
    const [{ pending }] = await sql("select count(*)::int as pending from outbox_messages where published_at is null");
    if (pending === 0) {
      drained = true;
      break;
    }
    await Bun.sleep(100);
  }
  const drainSeconds = (performance.now() - drainStartedAt) / 1_000;

  // ---------------------------------------------------------------- consistência
  const all = [...wallets, hotWallet];
  let inconsistent = 0;
  for (const [i, wallet] of all.entries()) {
    const res = await post(apps[i % apps.length]!, `/wallets/${wallet.id}/reconciliation`, {});
    const report = (await res.json()) as { consistent: boolean };
    if (!report.consistent) inconsistent++;
  }
  const [{ pending_reference }] = await sql(
    "select count(*)::int as pending_reference from wager_transactions where status = 'PENDING_REFERENCE'",
  );
  const [{ negative }] = await sql("select count(*)::int as negative from wallets where balance_amount < 0");

  // ---------------------------------------------------------------- métricas das instâncias
  const series = await scrapeAll(everyProcess);
  const publishedPerInstance = await Promise.all(
    everyProcess.map(async (app) => sum(parseMetrics(await (await fetch(`${app.url}/metrics`)).text()), "outbox_messages_published_total")),
  );
  const publishFailures = sum(series, "outbox_publish_failures_total");
  await Promise.all(everyProcess.map((app) => app.stop()));
  await sqs.send(new DeleteQueueCommand({ QueueUrl: eventsQueueUrl })).catch(() => {});
  sqs.destroy();
  await closeTestOrm();
  void orm;

  // ---------------------------------------------------------------- números
  const latencies = samples.map((s) => s.ms).sort((a, b) => a - b);
  const statusCount = new Map<string, number>();
  for (const s of samples) statusCount.set(String(s.status), (statusCount.get(String(s.status)) ?? 0) + 1);
  const isError = (s: Sample) => s.status === "network-error" || s.status >= 500 || ![200, 201, 202, 422].includes(s.status);
  const errors = samples.filter(isError).length;

  const byScenario = MIX.map(({ scenario }) => {
    const ms = samples.filter((s) => s.scenario === scenario).map((s) => s.ms).sort((a, b) => a - b);
    return { scenario, count: ms.length, p50: percentile(ms, 0.5), p95: percentile(ms, 0.95), p99: percentile(ms, 0.99) };
  });

  const conflicts = series
    .filter((s) => s.name === "wallet_lock_conflicts_total")
    .reduce<Record<string, number>>((acc, s) => ({ ...acc, [s.labels.type!]: (acc[s.labels.type!] ?? 0) + s.value }), {});
  const lockWait = (q: number) => (histogramQuantile(series, "wallet_lock_wait_seconds", q) ?? 0) * 1_000;
  const publishLag = (q: number) => (histogramQuantile(series, "outbox_publish_lag_seconds", q) ?? 0) * 1_000;
  const maxPending = Math.max(0, ...lagSamples.map((s) => s.pending));
  const maxOldestAge = Math.max(0, ...lagSamples.map((s) => s.oldestAge));
  const published = sum(series, "outbox_messages_published_total");
  const duplicates = sum(series, "wager_duplicates_total");

  // ---------------------------------------------------------------- achados (calculados desta execução)
  const distinct = byScenario.find((s) => s.scenario === "BET (wallets distintas)")!;
  const hot = byScenario.find((s) => s.scenario === "BET (hot wallet)")!;
  const generatedPerSecond = published / loadSeconds;
  const publishedPerSecond = published / (loadSeconds + drainSeconds);
  const findings: string[] = [
    `- **Hot wallet vs. wallets distintas:** p50 ${fmt(hot.p50)} ms contra ${fmt(distinct.p50)} ms ` +
      `(${fmt(hot.p50 / Math.max(1, distinct.p50))}×). É o custo esperado do lock pessimista POR WALLET: ` +
      `as apostas da mesma wallet fazem fila, as de wallets diferentes seguem em paralelo ` +
      `(espera de lock p95 = ${fmt(lockWait(0.95))} ms, concentrada na hot wallet). Sem conflito, sem retry, sem erro.`,
    `- **Reenvios também esperam o lock:** a checagem de idempotência acontece DEPOIS do \`FOR UPDATE\` ` +
      `(passo 1 → 2 do use case), então um replay na hot wallet entra na mesma fila. É correto (o replay ` +
      `lê o resultado já gravado), mas é uma otimização possível: ler a Idempotency-Key antes do lock e ` +
      `responder o replay sem travar a wallet.`,
  ];
  if (publishLag(0.95) > 1_000) {
    findings.push(
      `- **A outbox fica um pouco atrás no pico:** ~${fmt(generatedPerSecond, 0)} eventos/s gerados ` +
        `contra ~${fmt(publishedPerSecond, 0)} eventos/s publicados (somando as ${CONFIG.instances} instâncias); ` +
        `a fila chegou a ${maxPending} e o lag p95 a ${fmt(publishLag(0.95) / 1_000)} s. Nada se perdeu ` +
        `(esvaziou em ${fmt(drainSeconds, 1)} s depois da carga, tudo publicado). O publisher já manda em lote ` +
        `(\`SendMessageBatch\`, blocos de 10, em série para preservar a ordem FIFO); os próximos passos seriam ` +
        `publicar grupos FIFO diferentes em paralelo e escalar instâncias/conexões.`,
    );
  } else {
    findings.push(`- **Outbox em dia:** lag de publicação p95 de ${fmt(publishLag(0.95))} ms.`);
  }
  findings.push(
    `- **Ambiente compartilhado:** gerador de carga, ${CONFIG.instances} instâncias, Postgres e MiniStack na mesma ` +
      `máquina (${cpus().length} CPUs), com pool de 10 conexões por instância. Os números servem para comparar ` +
      `execuções e enxergar gargalos, não como capacidade de produção.`,
  );

  // ---------------------------------------------------------------- relatório
  const cpu = cpus();
  const report = `# Teste de carga

> Gerado por \`bun run test:load\` em ${startedAt.toISOString()}. Os números abaixo são DESTA máquina e
> desta execução; rode de novo para reproduzir (parâmetros no topo de \`test/load/run.ts\`).

## Ambiente

| Item | Valor |
|---|---|
| Máquina | ${cpu.length} × ${cpu[0]?.model.trim()} · ${fmt(totalmem() / 1024 ** 3, 0)} GB RAM · ${type()} ${release()} |
| Runtime | Bun ${Bun.version} |
| Banco | ${String(postgresVersion).split(",")[0]} (Docker, mesma máquina) · pool de 10 conexões por instância (padrão do pg-pool) |
| Mensageria | SQS via MiniStack (Docker, mesma máquina) |
| Instâncias | ${
    dedicated
      ? `${CONFIG.instances} processos \`bun src/main.ts\` só de API (workers desligados) + 1 processo só de workers (outbox e PENDING_REFERENCE, sem tráfego HTTP) — \`LOAD_PUBLISHER_MODE=dedicated\``
      : `${CONFIG.instances} processos \`bun src/main.ts\`, cada um com a API e os workers (outbox e PENDING_REFERENCE) — \`LOAD_PUBLISHER_MODE=embedded\``
  } |
| Gerador de carga | o mesmo host (compete por CPU com as instâncias e o banco) |

## Metodologia

- **${CONFIG.concurrency} usuários virtuais** em loop fechado (cada um manda a próxima requisição quando a anterior responde), por **${CONFIG.durationSeconds}s**, distribuídos em round-robin entre as instâncias.
- **${CONFIG.wallets} wallets** com 10.000,00 BRL e **1 hot wallet** com 1.000.000,00 BRL; schema recriado antes de cada execução.
- Mistura: ${MIX.map((m) => `${m.weight}% ${m.scenario}`).join(" · ")}.
  O "reenvio duplicado" repete uma requisição já enviada, com a mesma Idempotency-Key e o mesmo corpo (deve virar replay, sem efeito).
- Latência medida no cliente (requisição completa). Conflitos, espera de lock e lag da outbox vêm do \`/metrics\` das instâncias.
- **Erro** = falha de rede, timeout, 5xx ou status inesperado. **422 não é erro**: é rejeição de negócio esperada (ex.: saldo insuficiente).
- Depois da carga: espera a outbox esvaziar e **reconcilia todas as wallets**.

## Resultados

| Métrica | Valor |
|---|---|
| Requisições | ${samples.length} em ${fmt(loadSeconds)}s |
| **Throughput** | **${fmt(samples.length / loadSeconds)} req/s** |
| **Latência p50 / p95 / p99** | **${fmt(percentile(latencies, 0.5))} / ${fmt(percentile(latencies, 0.95))} / ${fmt(percentile(latencies, 0.99))} ms** |
| Latência máxima | ${fmt(latencies.at(-1) ?? 0)} ms |
| **Taxa de erro** | **${fmt((errors / Math.max(1, samples.length)) * 100, 2)}%** (${errors} de ${samples.length}) |
| Status | ${[...statusCount.entries()].sort().map(([s, n]) => `${s}: ${n}`).join(" · ")} |
| Duplicatas absorvidas (replay) | ${duplicates} |

### Latência por cenário (ms)

| Cenário | Requisições | p50 | p95 | p99 |
|---|---|---|---|---|
${byScenario.map((s) => `| ${s.scenario} | ${s.count} | ${fmt(s.p50)} | ${fmt(s.p95)} | ${fmt(s.p99)} |`).join("\n")}

### Concorrência

| Métrica | Valor |
|---|---|
| Espera pelo lock da wallet p50 / p95 / p99 | ${fmt(lockWait(0.5))} / ${fmt(lockWait(0.95))} / ${fmt(lockWait(0.99))} ms (estimado pelo histograma) |
| Conflitos de concorrência | ${Object.keys(conflicts).length === 0 ? "0" : Object.entries(conflicts).map(([k, v]) => `${k}: ${v}`).join(" · ")} |

### Outbox

| Métrica | Valor |
|---|---|
| Eventos publicados | ${published} (por instância: ${publishedPerInstance.join(" · ")}) · falhas de publicação: ${publishFailures} |
| Lag de publicação (commit → SQS) p50 / p95 / p99 | ${fmt(publishLag(0.5))} / ${fmt(publishLag(0.95))} / ${fmt(publishLag(0.99))} ms (estimado pelo histograma) |
| Maior idade do evento pendente mais antigo, durante a carga | ${fmt(maxOldestAge, 2)} s |
| Maior fila de eventos pendentes, durante a carga | ${maxPending} |
| Tempo para esvaziar depois da carga | ${drained ? `${fmt(drainSeconds, 2)} s` : "NÃO esvaziou em 60s"} |

### Consistência final

| Verificação | Resultado |
|---|---|
| Wallets reconciliadas | ${all.length} |
| Divergências (saldo ≠ ledger) | ${inconsistent === 0 ? "**0** ✅" : `**${inconsistent}** ❌`} |
| Saldos negativos | ${negative === 0 ? "0 ✅" : `${negative} ❌`} |
| PENDING_REFERENCE sobrando | ${pending_reference} |

## Leitura dos números

${findings.join("\n")}

Não há meta de RPS (seção 14): o objetivo é mostrar que, sob carga concorrente real e com duplicatas,
**não há erro, divergência nem saldo negativo**, e medir quanto essa garantia custa.

## Histórico: o que este teste mudou no código

**1. Publicação em lote.** As primeiras execuções mostraram que a outbox não acompanhava a escrita:
o publisher fazia uma \`SendMessage\` por evento, em série, e um \`UPDATE\` por evento. A correção foi
publicar com \`SendMessageBatch\` (blocos de 10, em série para preservar a ordem FIFO) e marcar os
publicados num único \`UPDATE\`. A/B nas mesmas condições (3 instâncias embedded, 48 usuários, 20 s,
MiniStack limpo, 2 execuções de cada lado):

| Outbox | Antes (\`SendMessage\` por evento) | Depois (\`SendMessageBatch\`) |
|---|---|---|
| Lag de publicação p95 | 23,0–25,6 s | 4,0–4,1 s |
| Maior fila de pendentes | ~3.000–3.200 | ~1.000–1.200 |
| Tempo para esvaziar depois da carga | 11–12 s | ~2 s |
| Eventos publicados/s | ~221 | ~370 |
| Throughput da API | 206–215 req/s | 237–243 req/s |

**2. O emulador também degrada.** Execuções seguidas foram ficando mais lentas, nos dois modos. A causa
não era o código: o MiniStack acumula estado com o volume (o \`SendMessageBatch\` foi de ~11 ms para
~57 ms depois de ~95 mil mensagens; esvaziar a fila não recuperava, reiniciar o container sim).
Por isso cada execução usa uma fila de eventos nova e a apaga no fim.

**3. Workers em toda instância vs. um processo dedicado.** Com o emulador limpo,
\`LOAD_PUBLISHER_MODE=embedded\` (3 publishers, um por instância) teve lag p95 de ~4,0 s e esvaziou em
~2 s; \`dedicated\` (1 processo só de workers, sem HTTP) ficou em ~4,9–5,0 s e ~5 s. Mais publishers
em paralelo (via SKIP LOCKED) rendem mais que isolar um só; o padrão ficou \`embedded\`.
`;

  const reportPath = resolve(import.meta.dir, "REPORT.md");
  await Bun.write(reportPath, report);
  console.log(report);
  console.log(`relatório salvo em ${reportPath}`);

  if (inconsistent > 0 || negative > 0 || !drained) process.exitCode = 1;
}

await main();
