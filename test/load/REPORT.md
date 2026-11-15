# Teste de carga

> Gerado por `bun run test:load` em 2026-11-15T17:49:52.074Z. Os números abaixo são DESTA máquina e
> desta execução; rode de novo para reproduzir (parâmetros no topo de `test/load/run.ts`).

## Ambiente

| Item | Valor |
|---|---|
| Máquina | 8 × 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz · 15 GB RAM · Linux 7.0.0-43-generic |
| Runtime | Bun 1.4.4 |
| Banco | PostgreSQL 17.11 on x86_64-pc-linux-musl (Docker, mesma máquina) · pool de 10 conexões por instância (padrão do pg-pool) |
| Mensageria | SQS via MiniStack (Docker, mesma máquina) |
| Instâncias | 3 processos `bun src/main.ts` com outbox publisher e worker de PENDING_REFERENCE ligados |
| Gerador de carga | o mesmo host (compete por CPU com as instâncias e o banco) |

## Metodologia

- **48 usuários virtuais** em loop fechado (cada um manda a próxima requisição quando a anterior responde), por **20s**, distribuídos em round-robin entre as instâncias.
- **200 wallets** com 10.000,00 BRL e **1 hot wallet** com 1.000.000,00 BRL; schema recriado antes de cada execução.
- Mistura: 50% BET (wallets distintas) · 15% BET (hot wallet) · 10% WIN · 5% LOSS · 5% REFUND de uma BET · 15% reenvio duplicado.
  O "reenvio duplicado" repete uma requisição já enviada, com a mesma Idempotency-Key e o mesmo corpo (deve virar replay, sem efeito).
- Latência medida no cliente (requisição completa). Conflitos, espera de lock e lag da outbox vêm do `/metrics` das instâncias.
- **Erro** = falha de rede, timeout, 5xx ou status inesperado. **422 não é erro**: é rejeição de negócio esperada (ex.: saldo insuficiente).
- Depois da carga: espera a outbox esvaziar e **reconcilia todas as wallets**.

## Resultados

| Métrica | Valor |
|---|---|
| Requisições | 4335 em 20.4s |
| **Throughput** | **212.5 req/s** |
| **Latência p50 / p95 / p99** | **137.3 / 883.1 / 1192.6 ms** |
| Latência máxima | 1931.3 ms |
| **Taxa de erro** | **0.00%** (0 de 4335) |
| Status | 200: 648 · 201: 3680 · 202: 7 |
| Duplicatas absorvidas (replay) | 648 |

### Latência por cenário (ms)

| Cenário | Requisições | p50 | p95 | p99 |
|---|---|---|---|---|
| BET (wallets distintas) | 2156 | 113.1 | 365.6 | 400.8 |
| BET (hot wallet) | 647 | 623.6 | 1211.4 | 1559.7 |
| WIN | 447 | 111.3 | 358.3 | 405.7 |
| LOSS | 203 | 105.7 | 363.0 | 401.9 |
| REFUND de uma BET | 226 | 119.4 | 384.7 | 472.6 |
| reenvio duplicado | 656 | 103.4 | 857.0 | 1176.9 |

### Concorrência

| Métrica | Valor |
|---|---|
| Espera pelo lock da wallet p50 / p95 / p99 | 3.6 / 743.0 / 999.5 ms (estimado pelo histograma) |
| Conflitos de concorrência | 0 |

### Outbox

| Métrica | Valor |
|---|---|
| Eventos publicados | 7178 |
| Lag de publicação (commit → SQS) p50 / p95 / p99 | 15769.2 / 28576.9 / 29715.4 ms (estimado pelo histograma) |
| Maior idade do evento pendente mais antigo, durante a carga | 12.18 s |
| Maior fila de eventos pendentes, durante a carga | 4563 |
| Tempo para esvaziar depois da carga | 27.15 s |

### Consistência final

| Verificação | Resultado |
|---|---|
| Wallets reconciliadas | 201 |
| Divergências (saldo ≠ ledger) | **0** ✅ |
| Saldos negativos | 0 ✅ |
| PENDING_REFERENCE sobrando | 0 |

## Leitura dos números

- **Hot wallet vs. wallets distintas:** p50 623.6 ms contra 113.1 ms (5.5×). É o custo esperado do lock pessimista POR WALLET: as apostas da mesma wallet fazem fila, as de wallets diferentes seguem em paralelo (espera de lock p95 = 743.0 ms, concentrada na hot wallet). Sem conflito, sem retry, sem erro.
- **Reenvios também esperam o lock:** a checagem de idempotência acontece DEPOIS do `FOR UPDATE` (passo 1 → 2 do use case), então um replay na hot wallet entra na mesma fila. É correto (o replay lê o resultado já gravado), mas é uma otimização possível: ler a Idempotency-Key antes do lock e responder o replay sem travar a wallet.
- **A outbox não acompanhou o ritmo de escrita:** ~352 eventos/s gerados contra ~151 eventos/s publicados (somando as 3 instâncias), então a fila cresceu até 4563 e o lag p95 chegou a 28.6 s. Nada se perdeu (esvaziou em 27.2 s depois da carga, tudo publicado), mas o atraso existe. Causa: uma `SendMessage` por evento, em série, dentro da transação do lote. Melhorias, em ordem de impacto: `SendMessageBatch` (até 10 eventos por chamada), publicar grupos FIFO diferentes em paralelo, e um `OUTBOX_BATCH_SIZE` maior.
- **Ambiente compartilhado:** gerador de carga, 3 instâncias, Postgres e MiniStack na mesma máquina (8 CPUs), com pool de 10 conexões por instância. Os números servem para comparar execuções e enxergar gargalos, não como capacidade de produção.

Não há meta de RPS (seção 14): o objetivo é mostrar que, sob carga concorrente real e com duplicatas,
**não há erro, divergência nem saldo negativo**, e medir quanto essa garantia custa.
