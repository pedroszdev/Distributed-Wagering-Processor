# Distributed Wagering Processor

Serviço financeiro que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`)
vindas de provedores de jogos, por **HTTP** e por **SQS**, e continua correto com mensagens
duplicadas, fora de ordem, processadas ao mesmo tempo e com várias instâncias rodando juntas.

**Stack:** Bun 1.4 · TypeScript (strict) · NestJS 12 · MikroORM 7 · PostgreSQL 17 · SQS (MiniStack) · Docker Compose

> **Decisões, trade-offs e limitações:** [ARCHITECTURE.md](./ARCHITECTURE.md).
> **Teste de carga (último resultado):** [test/load/REPORT.md](./test/load/REPORT.md).

---

## Em resumo

- **Dinheiro** é `decimal.js` no domínio, string decimal na API e `numeric(20,2)` no banco. Nunca `number`.
- **Concorrência:** um lock pessimista **por wallet** (`SELECT … FOR UPDATE`) mais a checagem de versão. Não existe lock global.
- **Idempotência** persistente: `Idempotency-Key` + `payloadHash` (JSON canônico, SHA-256) no Postgres, e uma **inbox** para a fila.
- **Ledger imutável** (um trigger recusa `UPDATE`, `DELETE` e `TRUNCATE`). As invariantes de unicidade, não-negatividade e coerência são **constraints do schema**.
- **Transactional outbox:** os eventos são gravados na mesma transação da aposta e publicados depois do commit (`FOR UPDATE SKIP LOCKED`, `SendMessageBatch`).
- **Consumidor SQS:** usa o **mesmo use case** do HTTP. Tem inbox, ack só depois do commit, retry com backoff, DLQ e shutdown limpo.
- **Referência fora de ordem:** a transação fica `PENDING_REFERENCE`, e um worker agendado tenta de novo com backoff e TTL.
- **Operação:** reconciliação, `/health/live`, `/health/ready`, logs JSON com `correlationId` e métricas Prometheus.
- **Testes:** 738 testes contra **Postgres e SQS reais**, incluindo races, 3 instâncias, `SIGKILL` no meio da carga e um teste de carga.

---

## Pré-requisitos

- [Bun](https://bun.sh) 1.4+
- Docker e Docker Compose

> **Por que MiniStack, e não LocalStack?** Desde março de 2026 a imagem do LocalStack exige conta
> e auth token. O MiniStack expõe a mesma API do SQS na porta 4566, sem cadastro.

## Setup

```bash
bun install
cp .env.example .env
bun run infra:up        # sobe Postgres + SQS e cria as filas FIFO
bun run migration:up    # aplica as migrations no banco "wagering"
bun run dev             # API em http://localhost:3000 (com reload automático)
```

Para conferir que subiu:

```bash
curl -s localhost:3000/health/ready    # {"status":"ok","checks":{"database":{"status":"up",…},"sqs":{"status":"up",…}}}
```

## Comandos

| Comando | O que faz |
|---|---|
| `bun run dev` | API com reload automático |
| `bun run start` | API sem reload |
| `bun run typecheck` | checagem de tipos (`tsc --noEmit`) |
| `bun test` | **todos** os testes (unidade + integração; precisam do `infra:up`) |
| `bun run test:unit` / `bun run test:integration` | só unidade (`src/`) / só integração (`test/integration/`) |
| `bun run test:load` | teste de carga, que escreve `test/load/REPORT.md` (ver abaixo) |
| `bun run infra:up` / `infra:down` | sobe / derruba Postgres e SQS |
| `bun run infra:reset` | derruba e **apaga os dados** (volumes) |
| `bun run migration:up` / `migration:down` | aplica / reverte as migrations |
| `bun run migration:list` | lista as migrations aplicadas |

## Várias instâncias (3+)

A mesma imagem roda como API **e** como worker. Os workers são ligados por variável de ambiente e
dividem o trabalho entre as instâncias (`SKIP LOCKED` e lease no banco), então dá para escalar sem
configurar nada:

```bash
bun run infra:up && bun run migration:up
docker compose --profile app up --build --scale app=3
docker compose port --index 1 app 3000    # porta de cada instância (--index 1, 2, 3)
```

## Infraestrutura local

| Serviço | Endereço |
|---|---|
| PostgreSQL | `localhost:5432`, usuário/senha `wagering`, bancos `wagering` (dev) e `wagering_test` (testes) |
| SQS (MiniStack) | `http://localhost:4566` |
| Fila de entrada | `wager-transactions.fifo` (DLQ: `wager-transactions-dlq.fifo`, `maxReceiveCount` = 5) |
| Fila de eventos (saída da outbox) | `wagering-events.fifo` |

---

## API

| Método e rota | O que faz |
|---|---|
| `POST /wallets` | cria uma wallet. Se o saldo inicial for > 0, gera uma transação `OPENING` e o lançamento de crédito na mesma transação SQL |
| `GET /wallets/:walletId` | saldo e versão |
| `GET /wallets/:walletId/ledger?cursor=…&limit=50` | extrato com cursor opaco e estável (keyset); `limit` máximo de 200 |
| `POST /wallets/:walletId/reconciliation` | saldo gravado × saldo reconstruído pelo ledger |
| `POST /wagering/transactions` | submete uma transação (header `Idempotency-Key` obrigatório) |
| `GET /wagering/transactions/:transactionId` | transação pelo id interno |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | transação pelo id do provedor |
| `GET /health/live` · `GET /health/ready` | liveness (o processo) · readiness (Postgres e SQS) |
| `GET /metrics` | métricas no formato do Prometheus |

O header `x-correlation-id` é opcional. Quando vem, ele é devolvido na resposta e acompanha os logs
e os eventos daquela requisição. Quando não vem, o serviço gera um.

### Exemplo

```bash
# 1. cria a wallet
curl -s localhost:3000/wallets -H 'content-type: application/json' -d '{
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "initialBalance": { "amount": "1000.00", "currency": "BRL" }
}'

# 2. aposta (use o "id" devolvido acima como walletId)
curl -s -i localhost:3000/wagering/transactions \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{
    "providerId": "provider-a", "externalTransactionId": "transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", "walletId": "<walletId>",
    "roundId": "round-987", "gameId": "fortune-chimp",
    "kind": "BET", "money": { "amount": "25.00", "currency": "BRL" }
  }'
# → 201 {"transactionId":"…","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
# o mesmo comando de novo → 200 com "idempotentReplay": true (sem debitar outra vez)
```

### Status HTTP

| Status | Quando | O provedor deve… |
|---|---|---|
| `201` | transação nova aplicada (`PROCESSED`) | seguir |
| `200` | **replay**: a mesma key com o mesmo payload devolve o resultado original | seguir (não houve efeito novo) |
| `202` | aceita, esperando a transação referenciada (`PENDING_REFERENCE`) | consultar depois pelo GET |
| `422` | rejeitada por regra de negócio (`failureCode` no corpo); é um resultado final e auditável | não reenviar igual |
| `400` | payload inválido | corrigir antes de reenviar |
| `404` | wallet ou transação inexistente | corrigir |
| `409` | conflito de idempotência (a mesma key com outro payload, ou o mesmo `externalTransactionId` com outra key) | não reenviar |
| `503` + `Retry-After` | falha transitória de infraestrutura (banco fora, deadlock) | **reenviar com a mesma key** |

A tabela completa, os `failureCode` e as decisões por trás deles estão no [ARCHITECTURE.md](./ARCHITECTURE.md#9-api-http).

### Mandar uma transação pela fila

A mensagem segue o formato da seção 10 do desafio: o `data` é o corpo do POST mais o `idempotencyKey`.

```bash
docker run --rm --network wagering_default \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli --endpoint-url http://sqs:4566 sqs send-message \
  --queue-url http://sqs:4566/000000000000/wager-transactions.fifo \
  --message-group-id <walletId> --message-deduplication-id msg-123 \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-07-29T15:00:00.000Z",
    "data":{"providerId":"provider-a","externalTransactionId":"transaction-124","idempotencyKey":"provider-a:transaction-124",
    "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"<walletId>","roundId":"round-987","gameId":"fortune-chimp",
    "kind":"BET","money":{"amount":"10.00","currency":"BRL"}}}'
```

---

## Testes

```bash
bun run infra:up   # os testes de integração usam o Postgres e o SQS de verdade
bun test           # ~80 s
```

- Os testes usam um banco próprio (`wagering_test`, recriado do zero a cada execução **pelas
  migrations**) e filas próprias (`*-test.fifo`). Duas travas de segurança impedem que eles rodem
  contra o banco ou as filas de dev.
- **Nada de mock de Postgres ou SQS.** Os spies só servem para **injetar falhas** em pontos exatos (o
  banco cai no meio, o processo morre antes do ack, o SQS recusa uma entrada do lote).
- A invariante da seção 13 (`saldo da wallet == saldo reconstruído pelo ledger`) é verificada no
  fim dos testes que mexem em saldo. O helper confere também a **corrente** de cada lançamento
  (`balance_before[n] == balance_after[n-1]`).

| Cobertura | Onde |
|---|---|
| `Money`, `Wallet`, `WagerTransaction`, ledger, regras de reversão, hash e idempotência, políticas de retry | `src/**/*.test.ts` (unidade) |
| migrations, constraints, triggers | `test/integration/schema.test.ts` |
| regras de BET/WIN/LOSS/REFUND/ROLLBACK, atomicidade, outbox na mesma transação | `process-wager-transaction`, `wagering-outbox` |
| contrato HTTP, 503, ledger paginado | `wagering.http`, `wallets.http` |
| inbox e redelivery, retry, DLQ, ordem FIFO, SIGTERM | `inbox`, `sqs-consumer` |
| publishers concorrentes, crash antes do commit, lote | `outbox-publisher` |
| PENDING_REFERENCE, backoff, TTL, workers concorrentes, deadlock | `pending-reference` |
| reconciliação, health, métricas e logs, autenticação | `reconciliation`, `health`, `observability`, `auth` |

**Os cenários de concorrência da seção 13:**

| # | Cenário | Teste |
|---|---|---|
| 1 | a mesma aposta 50× em paralelo → 1 débito | `process-wager-transaction` (50×), `wagering.http` (30× pelo HTTP) |
| 2 | saldo 100, duas BETs de 80 ao mesmo tempo | `process-wager-transaction`, `wagering.http` (80 BETs disputando 100.00) |
| 3 | wallets distintas em paralelo | `distinct-wallets` (o lock de A não segura B; 30 wallets × 10 apostas) |
| 4 | ≥ 3 instâncias | `wagering.http`, `sqs-consumer`, `outbox-publisher`, `pending-reference` (3 apps reais) |
| 5 | worker morto depois do commit e antes do ack | `sqs-consumer` (outra instância recebe de novo; vira duplicata) |
| 6 | dois publishers sobre a mesma outbox | `outbox-publisher` (3 instâncias × 2 publishers sobre 100 eventos) |
| 7 | `ROLLBACK`/`REFUND` antes da referência | `pending-reference`, `sqs-consumer` |
| 8 | reinício com consistência final | `restart` (processo real morto com **`SIGKILL`** no meio da rajada) |

## Teste de carga

```bash
bun run infra:up
bun run test:load        # ~75 s; escreve test/load/REPORT.md
```

Sobe 3 instâncias reais (processos separados, com os workers ligados) e dispara 48 usuários
virtuais por 20 s. A mistura tem wallets distintas, uma **hot wallet**, WIN, LOSS, REFUND e 15% de
**reenvios duplicados**. O relatório traz ambiente, metodologia, throughput, p50/p95/p99 por cenário,
taxa de erro, espera e conflitos de lock, outbox lag e a reconciliação de todas as wallets no fim.

Parâmetros: `LOAD_DURATION_SECONDS`, `LOAD_CONCURRENCY`, `LOAD_INSTANCES`, `LOAD_WALLETS` e
`LOAD_PUBLISHER_MODE` (`embedded` ou `dedicated`). Ele **recria o schema do `wagering_test`**.

Último resultado nesta máquina (8 CPUs, tudo no mesmo host): **~240 req/s, 0% de erro, 0 divergências**.
O relatório inclui a análise e o A/B que levou a outbox a publicar em lote.

## Observabilidade

- **Logs** em JSON, uma linha por evento, no stdout. Cada linha leva `correlationId`, `messageId`,
  `transactionId`, `walletId` e `providerId` (propagados com `AsyncLocalStorage`) e nunca o valor da
  aposta nem o corpo da requisição. Para ler no terminal: `LOG_FORMAT=pretty bun run dev`.
- **Métricas** em `GET /metrics` (Prometheus): transações por status, duplicatas, retries, DLQ,
  espera e conflitos de lock, outbox lag e latências. O catálogo está em
  [`src/infrastructure/observability/metrics.ts`](./src/infrastructure/observability/metrics.ts).

## Configuração

O [`.env.example`](./.env.example) traz as do dia a dia. A lista completa, com os padrões, está em
[`src/config/env.ts`](./src/config/env.ts), lido uma vez na subida. A aplicação não sobe se faltar o
`DATABASE_URL` ou se o `LOG_LEVEL` for inválido. As principais:

| Variável | Padrão | Para quê |
|---|---|---|
| `DATABASE_URL` | (obrigatória) | Postgres |
| `SQS_ENDPOINT` · `SQS_QUEUE_NAME` · `SQS_DLQ_NAME` · `SQS_EVENTS_QUEUE_NAME` | MiniStack / filas acima | SQS |
| `OUTBOX_PUBLISHER_ENABLED` · `SQS_CONSUMER_ENABLED` · `PENDING_REFERENCE_WORKER_ENABLED` | `true` | liga e desliga cada worker nesta instância |
| `OUTBOX_BATCH_SIZE` · `OUTBOX_POLL_INTERVAL_MS` | `100` · `500` | publisher da outbox |
| `SQS_RETRY_BASE_DELAY_SECONDS` · `SQS_RETRY_MAX_DELAY_SECONDS` | `5` · `300` | backoff do consumidor |
| `PENDING_REFERENCE_TTL_MS` · `PENDING_REFERENCE_RETRY_BASE_MS` · `PENDING_REFERENCE_RETRY_MAX_MS` | 5 min · 1 s · 60 s | worker de `PENDING_REFERENCE` |
| `LOG_FORMAT` · `LOG_LEVEL` | `json` · `log` | logs |

## Estrutura

```
src/
  domain/            regras e invariantes, sem framework: Money, Wallet, WagerTransaction,
                     WalletLedgerEntry, OutboxMessage, eventos de integração
  application/       use cases (orquestração e transação) e portas (EventPublisher)
  http/              controllers, parse e validação dos requests, respostas, filtro de erros, auth
  infrastructure/
    database/        entities, mappers, repositórios, migrations
    messaging/       consumidor SQS, publisher da outbox (SQS), políticas de retry
    scheduling/      worker de PENDING_REFERENCE
    observability/   logger JSON, contexto de log, métricas
    health/          checks de Postgres e SQS
test/
  integration/       Postgres e SQS reais; support/ tem os helpers (app, banco, ledger, processos)
  load/              teste de carga e o último REPORT.md
```
