# Teste de carga

> Gerado por `bun run test:load` em 2026-11-15T19:16:49.714Z. Os números abaixo são DESTA máquina e
> desta execução; rode de novo para reproduzir (parâmetros no topo de `test/load/run.ts`).

## Ambiente

| Item | Valor |
|---|---|
| Máquina | 8 × 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz · 15 GB RAM · Linux 7.0.0-43-generic |
| Runtime | Bun 1.4.4 |
| Banco | PostgreSQL 17.11 on x86_64-pc-linux-musl (Docker, mesma máquina) · pool de 10 conexões por instância (padrão do pg-pool) |
| Mensageria | SQS via MiniStack (Docker, mesma máquina) |
| Instâncias | 3 processos `bun src/main.ts`, cada um com a API e os workers (outbox e PENDING_REFERENCE) — `LOAD_PUBLISHER_MODE=embedded` |
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
| Requisições | 4881 em 20.4s |
| **Throughput** | **239.3 req/s** |
| **Latência p50 / p95 / p99** | **106.2 / 791.4 / 1127.5 ms** |
| Latência máxima | 1823.1 ms |
| **Taxa de erro** | **0.00%** (0 de 4881) |
| Status | 200: 706 · 201: 4171 · 202: 4 |
| Duplicatas absorvidas (replay) | 706 |

### Latência por cenário (ms)

| Cenário | Requisições | p50 | p95 | p99 |
|---|---|---|---|---|
| BET (wallets distintas) | 2452 | 88.3 | 334.6 | 401.0 |
| BET (hot wallet) | 703 | 603.8 | 1142.3 | 1424.8 |
| WIN | 468 | 87.9 | 327.0 | 373.9 |
| LOSS | 279 | 65.7 | 324.0 | 400.7 |
| REFUND de uma BET | 265 | 102.2 | 346.7 | 443.7 |
| reenvio duplicado | 714 | 76.0 | 661.9 | 1025.7 |

### Concorrência

| Métrica | Valor |
|---|---|
| Espera pelo lock da wallet p50 / p95 / p99 | 3.5 / 704.7 / 994.3 ms (estimado pelo histograma) |
| Conflitos de concorrência | 0 |

### Outbox

| Métrica | Valor |
|---|---|
| Eventos publicados | 8075 (por instância: 3252 · 2342 · 2481) · falhas de publicação: 0 |
| Lag de publicação (commit → SQS) p50 / p95 / p99 | 838.7 / 4394.1 / 4878.8 ms (estimado pelo histograma) |
| Maior idade do evento pendente mais antigo, durante a carga | 2.99 s |
| Maior fila de eventos pendentes, durante a carga | 1234 |
| Tempo para esvaziar depois da carga | 2.30 s |

### Consistência final

| Verificação | Resultado |
|---|---|
| Wallets reconciliadas | 201 |
| Divergências (saldo ≠ ledger) | **0** ✅ |
| Saldos negativos | 0 ✅ |
| PENDING_REFERENCE sobrando | 0 |

## Leitura dos números

- **Hot wallet vs. wallets distintas:** p50 603.8 ms contra 88.3 ms (6.8×). É o custo esperado do lock pessimista POR WALLET: as apostas da mesma wallet fazem fila, as de wallets diferentes seguem em paralelo (espera de lock p95 = 704.7 ms, concentrada na hot wallet). Sem conflito, sem retry, sem erro.
- **Reenvios também esperam o lock:** a checagem de idempotência acontece DEPOIS do `FOR UPDATE` (passo 1 → 2 do use case), então um replay na hot wallet entra na mesma fila. É correto (o replay lê o resultado já gravado), mas é uma otimização possível: ler a Idempotency-Key antes do lock e responder o replay sem travar a wallet.
- **A outbox fica um pouco atrás no pico:** ~396 eventos/s gerados contra ~356 eventos/s publicados (somando as 3 instâncias); a fila chegou a 1234 e o lag p95 a 4.4 s. Nada se perdeu (esvaziou em 2.3 s depois da carga, tudo publicado). O publisher já manda em lote (`SendMessageBatch`, blocos de 10, em série para preservar a ordem FIFO); os próximos passos seriam publicar grupos FIFO diferentes em paralelo e escalar instâncias/conexões.
- **Ambiente compartilhado:** gerador de carga, 3 instâncias, Postgres e MiniStack na mesma máquina (8 CPUs), com pool de 10 conexões por instância. Os números servem para comparar execuções e enxergar gargalos, não como capacidade de produção.

Não há meta de RPS (seção 14): o objetivo é mostrar que, sob carga concorrente real e com duplicatas,
**não há erro, divergência nem saldo negativo**, e medir quanto essa garantia custa.

## Histórico: o que este teste mudou no código

**1. Publicação em lote.** As primeiras execuções mostraram que a outbox não acompanhava a escrita:
o publisher fazia uma `SendMessage` por evento, em série, e um `UPDATE` por evento. A correção foi
publicar com `SendMessageBatch` (blocos de 10, em série para preservar a ordem FIFO) e marcar os
publicados num único `UPDATE`. A/B nas mesmas condições (3 instâncias embedded, 48 usuários, 20 s,
MiniStack limpo, 2 execuções de cada lado):

| Outbox | Antes (`SendMessage` por evento) | Depois (`SendMessageBatch`) |
|---|---|---|
| Lag de publicação p95 | 23,0–25,6 s | 4,0–4,1 s |
| Maior fila de pendentes | ~3.000–3.200 | ~1.000–1.200 |
| Tempo para esvaziar depois da carga | 11–12 s | ~2 s |
| Eventos publicados/s | ~221 | ~370 |
| Throughput da API | 206–215 req/s | 237–243 req/s |

**2. O emulador também degrada.** Execuções seguidas foram ficando mais lentas, nos dois modos. A causa
não era o código: o MiniStack acumula estado com o volume (o `SendMessageBatch` foi de ~11 ms para
~57 ms depois de ~95 mil mensagens; esvaziar a fila não recuperava, reiniciar o container sim).
Por isso cada execução usa uma fila de eventos nova e a apaga no fim.

**3. Workers em toda instância vs. um processo dedicado.** Com o emulador limpo,
`LOAD_PUBLISHER_MODE=embedded` (3 publishers, um por instância) teve lag p95 de ~4,0 s e esvaziou em
~2 s; `dedicated` (1 processo só de workers, sem HTTP) ficou em ~4,9–5,0 s e ~5 s. Mais publishers
em paralelo (via SKIP LOCKED) rendem mais que isolar um só; o padrão ficou `embedded`.
