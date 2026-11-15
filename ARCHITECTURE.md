# Arquitetura

Este documento explica **o que foi decidido, por quê, o que custa e o que fica de fora**. Cada
decisão traz o teste que a comprova. Setup e comandos estão no [README](./README.md).

## Sumário

1. [Visão geral](#1-visão-geral)
2. [Camadas e fronteiras](#2-camadas-e-fronteiras)
3. [Modelo de domínio](#3-modelo-de-domínio)
4. [Dinheiro](#4-dinheiro)
5. [ORM e estratégia transacional](#5-orm-e-estratégia-transacional)
6. [Concorrência](#6-concorrência)
7. [Garantias no schema](#7-garantias-no-schema)
8. [Idempotência](#8-idempotência)
9. [API HTTP](#9-api-http)
10. [Regras de negócio e interpretações](#10-regras-de-negócio-e-interpretações)
11. [Transactional outbox](#11-transactional-outbox)
12. [Consumidor SQS](#12-consumidor-sqs)
13. [Referências fora de ordem (PENDING_REFERENCE)](#13-referências-fora-de-ordem-pending_reference)
14. [Reconciliação](#14-reconciliação)
15. [Observabilidade](#15-observabilidade)
16. [Autenticação](#16-autenticação)
17. [Estratégia de testes](#17-estratégia-de-testes)
18. [Teste de carga](#18-teste-de-carga)
19. [Limitações e próximos passos](#19-limitações-e-próximos-passos)

---

## 1. Visão geral

```
                    ┌──────────────────────── instância (×N, todas iguais) ───────────────────────┐
 provedor ─HTTP──▶  │ controller ─┐                                                               │
                    │             ├─▶ ProcessWagerTransactionUseCase ──┐  (1 transação SQL)       │
 provedor ─SQS───▶  │ consumidor ─┘   (inbox, quando vem da fila)      │                          │
 wager-transactions │                                                  ▼                          │
      .fifo         │                     wallet (FOR UPDATE) · wager_transactions · ledger ·     │
                    │                     inbox · outbox_messages ─────────────── PostgreSQL      │
                    │                                                  │                          │
                    │ outbox publisher (SKIP LOCKED) ◀────────────────┘ depois do commit          │
                    │ worker de PENDING_REFERENCE (lease + backoff)                               │
                    └───────────┬──────────────────────────────────────────────────────────────────┘
                                ▼
                       wagering-events.fifo  (eventos de integração, at-least-once)
```

- **Uma única imagem, uma única aplicação.** Toda instância atende HTTP, consome a fila e roda os
  workers. Cada papel liga e desliga por variável de ambiente. A coordenação entre instâncias é
  **toda feita no Postgres**: locks de linha, `SKIP LOCKED`, leases e constraints. Não existe estado
  compartilhado em memória nem lock global.
- **O Postgres é a fonte da verdade** de saldo, ledger, idempotência, inbox e outbox. O SQS é
  transporte. O FIFO e a deduplicação do SQS são **otimizações**: a correção não depende deles
  (seção 8 do desafio).

## 2. Camadas e fronteiras

| Camada | Conteúdo | Depende de |
|---|---|---|
| `domain/` | `Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry`, `OutboxMessage`, eventos de integração, erros de domínio | nada (só `decimal.js`) |
| `application/` | use cases (orquestração e fronteira transacional), regras de reversão, idempotência, políticas de retry, **portas** (`EventPublisher`) | domínio + repositórios |
| `http/` | controllers, parse e validação dos requests, mapeamento de status, filtro de erros, guard de auth | application |
| `infrastructure/` | entities e mappers do MikroORM, repositórios, migrations, adaptador SQS, consumidor, workers, logger, métricas, health | application, domain |

Decisões:

- **O domínio não conhece o ORM.** As entities do MikroORM (`defineEntity`) descrevem as **linhas**
  das tabelas, e os **mappers** convertem linha ↔ domínio. Na volta, o mapper usa a factory
  `rehydrate`, que reconstrói o estado persistido **sem revalidar transições** (seção 6.0).
- **O repositório recebe o `EntityManager` de quem chama.** É assim que o use case passa o `em` **da
  transação**, e todas as escritas de uma operação (wallet, transação, ledger, inbox, outbox) caem
  na mesma transação SQL. O repositório nunca abre transação por conta própria.
- **Portas onde a troca é real:**
  - `EventPublisher`: SQS hoje, um fake nos testes;
  - `ProviderAuthenticator`: no-op hoje, OIDC amanhã (ver [§16](#16-autenticação)).

  Não criei porta para os repositórios. Eles já recebem o `em` e são testados contra o Postgres de
  verdade, então uma interface a mais só adicionaria indireção.
- **Controllers magros:** fazem só parse → use case → status. Por isso o consumidor SQS reaproveita
  o **mesmo** `ProcessWagerTransactionUseCase` do HTTP (seção 10 do desafio).
- **As regras de BET/WIN/LOSS/REFUND/ROLLBACK ficam num lugar só**
  ([`WagerRules`](./src/application/wagering/wager-rules.ts)), usado pela entrada normal e pelo
  worker de `PENDING_REFERENCE`. As duas entradas não têm como divergir.

## 3. Modelo de domínio

Construtor `private`, factories estáticas (`create`, `from`, `rehydrate`) e estado encapsulado,
com transições explícitas que validam tudo **antes** de mudar qualquer campo.

| Classe | Invariantes que ela mesma protege |
|---|---|
| `Money` | decimal exato, escala 2, moeda ISO-4217, operações só entre a mesma moeda (`CurrencyMismatchError`) |
| `Wallet` | `debit` nunca deixa o saldo negativo (`InsufficientFundsError`); cada movimento incrementa `version` e devolve `{ before, after }` para o ledger |
| `WagerTransaction` | nasce `PENDING`; `REFUND`/`ROLLBACK` exigem referência e `BET` não aceita; valor > 0 (zero só para `LOSS`); estados terminais (`PROCESSED`, `REJECTED`, `FAILED`) não mudam mais (`InvalidTransactionStateError`) |
| `WalletLedgerEntry` | `balanceAfter = balanceBefore ± amount` (o `isBalanced`), valor positivo |
| `InboxMessage` | identidade `(consumerName, messageId)` obrigatória (até 255 caracteres) e `payloadHash`; `markProcessed` uma vez só e nunca antes de `receivedAt`. Na entrada pela fila, ela nasce e é marcada processada **dentro** da transação da aposta, então só existe no banco se a aposta também existir |
| `OutboxMessage` | `markPublished` uma vez só; `scheduleRetry` com backoff exponencial |
| `IntegrationEvent<T>` | classe abstrata; `eventType` e `version` ficam **no tipo** de cada subclasse; o `data` usa `MoneyProps` (string), nunca a instância |

Estados da transação: `PENDING → PROCESSED | REJECTED | PENDING_REFERENCE`, e depois
`PENDING_REFERENCE → PROCESSED | REJECTED`. `FAILED` existe no modelo e no schema para falhas
permanentes de infraestrutura, mas nenhum fluxo o produz hoje: o que é permanente vira `REJECTED`
com um `failureCode`, e o que é transitório não grava nada (503 ou retry).

**Eventos** (a tabela mínima da seção 11):

| Evento | Quando | `aggregateId` |
|---|---|---|
| `WagerTransactionProcessed` | qualquer transação aplicada, **inclusive `LOSS`** | transação |
| `WagerTransactionRejected` | rejeitada por regra (inclusive o TTL de `PENDING_REFERENCE`) | transação |
| `WalletBalanceChanged` | **somente** quando o saldo muda (`LOSS` não emite) | wallet |
| `WagerTransactionPendingReference` | referência ausente | transação |

O envelope segue a seção 11: `eventId` (é também o id da linha na outbox), `eventType`,
`aggregateId`, `correlationId`, `causationId?`, `occurredAt` em ISO-8601, `version` e `data`.

- O **`correlationId`** vem do header `x-correlation-id` (ou é gerado) no HTTP, e é o `messageId`
  na fila.
- O **`causationId`** é o `messageId` da mensagem que causou o evento. Na resolução de uma
  `PENDING_REFERENCE`, ele aponta para o evento `WagerTransactionPendingReference` original, e o
  evento herda o `correlationId` do pedido original.
- A **abertura da wallet** (`OPENING`) é uma transação interna e não emite eventos de integração.

## 4. Dinheiro

- **Domínio:** `Money` com `decimal.js` (precisão de 40 dígitos, `ROUND_HALF_EVEN`), escala fixa de 2.
- **API e eventos:** `{ "amount": "25.00", "currency": "BRL" }`, sempre como string decimal.
- **Banco:** `numeric(20,2)`, que o MikroORM lê e grava como **string** (`p.decimal()`). A string
  vai direto para o `Money.from`, sem passar por `number` em nenhum momento.
- **Entrada fora do formato é recusada, nunca arredondada.** `"10.123"`, `"1e5"`, `"-5.00"`, `5`
  (number), `"abc"`, `"1,50"` e `" 5"` dão erro (`INVALID_MONEY`). Arredondar em silêncio o valor
  de uma aposta esconderia um bug do provedor. O `ROUND_HALF_EVEN` existe só por segurança
  em operações internas, porque somar e subtrair na mesma escala não gera casas a mais.
- **As somas da reconciliação são feitas no Postgres, em `numeric`.** Créditos e débitos vêm
  separados porque o `Money.from` só aceita valores não negativos. A diferença sai do domínio
  (`stored.subtract(calculated)`) e pode ser negativa.
- **Limitação:** a escala é fixa em 2. Moedas com 0 ou 3 casas (JPY, KWD) precisariam de escala por
  moeda ([§19](#19-limitações-e-próximos-passos)).

## 5. ORM e estratégia transacional

**MikroORM 7**, o preferido pelo desafio. Uso dele:

- `em.transactional(...)`: uma transação SQL por operação, com rollback automático em qualquer erro;
- `em.fork()` em cada use case: um Unit of Work isolado por chamada, sem identity map vazando
  entre requisições;
- `LockMode.PESSIMISTIC_WRITE` (`FOR UPDATE`) na wallet e `PESSIMISTIC_PARTIAL_WRITE`
  (`FOR UPDATE SKIP LOCKED`) na outbox;
- `nativeUpdate` / `insert` explícitos, em vez de deixar o flush do Unit of Work decidir. Os
  UPDATEs que importam levam a condição no `WHERE` (`version = esperada`, `status =
  'PENDING_REFERENCE'`) e conferem o número de linhas afetadas.

**SQL direto onde o ORM atrapalharia,** e só nesses lugares:

| Onde | Por quê |
|---|---|
| inbox: `INSERT … ON CONFLICT DO NOTHING RETURNING` | a duplicata não pode **quebrar** a transação (no Postgres, um erro aborta a transação inteira) |
| claim da `PENDING_REFERENCE`: `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING` | a reserva (lease) acontece num comando só, atômico |
| reconciliação: `SELECT` com `sum(...) FILTER (...)` | o saldo e o ledger no **mesmo snapshot** ([§14](#14-reconciliação)) |
| migrations | escritas à mão, em SQL, e **reversíveis** (`up` e `down` na ordem inversa das FKs). O `migrations.test.ts` roda up → down → up e compara uma foto do catálogo do Postgres (colunas, constraints, índices, triggers, funções): o schema recriado é **idêntico**, e o `down` não deixa nada para trás |

**A transação de uma aposta grava, atomicamente:**
- a inbox (quando vem da fila);
- a transação (`wager_transactions`);
- o novo saldo da wallet (condicionado à versão);
- o lançamento no ledger;
- os eventos na outbox.

Ou tudo isso é confirmado junto, ou nada é. Os testes de atomicidade derrubam o banco **no meio**
(por exemplo, um erro injetado no insert do ledger) e conferem que não sobra nada: nem aposta, nem
saldo, nem inbox, nem evento. Depois, a redelivery processa normalmente.

## 6. Concorrência

**A estratégia é o lock pessimista por wallet,** com a versão como segunda barreira.

```
BEGIN
  SELECT … FROM wallets WHERE id = $1 FOR UPDATE      -- a fila se forma AQUI, por wallet
  (idempotência, regras, débito/crédito em memória)
  UPDATE wallets SET balance_amount = …, version = v+1 WHERE id = $1 AND version = v
  INSERT wager_transactions · INSERT wallet_ledger_entries · INSERT outbox_messages
COMMIT
```

- **Por que pessimista.** A wallet é exatamente a unidade de concorrência (seção 8). Com o
  `FOR UPDATE`, duas apostas na mesma wallet **esperam** uma pela outra: a segunda lê o saldo já
  debitado e é rejeitada por saldo insuficiente. Não há read-modify-write sem controle, não há
  lost update e não há retry. É a política mais simples de **provar** correta, e é previsível sob
  contenção.
- **Por que também a versão.** O `UPDATE … AND version = v` é uma rede de segurança contra bugs,
  por exemplo um caminho futuro que esqueça o lock. Se afetar 0 linhas, dá
  `ConcurrentWalletUpdateError`, que vira 503 no HTTP e retry na fila. Com o lock, isso nunca
  aconteceu nos testes, nem no de carga (o contador `version_conflict` ficou em 0).
- **A última barreira é o banco:** `CHECK (balance_amount >= 0)`. Mesmo com um bug no código, o
  saldo negativo vira erro e rollback, nunca dado gravado.
- **Ordem dos locks, sempre wallet → transações.** O use case principal e o worker de
  `PENDING_REFERENCE` travam nessa ordem. Com a ordem invertida no worker, o teste de concorrência
  falha com **deadlock de verdade** no Postgres. Esse teste foi rodado com o código estragado de
  propósito (uma mutação).
- **Não existe lock global.** O `distinct-wallets.test.ts` mantém a wallet A travada e mostra que
  uma aposta na wallet B termina em menos de 1 s. Com 150 ms de trabalho dentro do lock, 10
  wallets distintas levam ~150 ms no total, e 10 apostas na mesma wallet levam ≥ 1,5 s.

**Alternativas consideradas:**

| Alternativa | Por que não |
|---|---|
| Otimista puro (versão + retry) | sob contenção (hot wallet), os retries viram tempestade e a latência fica imprevisível; ainda exige limite de tentativas e um erro para quando estoura |
| UPDATE atômico condicionado (`SET balance = balance - x WHERE balance >= x`) | ótimo para o saldo isolado, mas o ledger precisa do `balance_before/after` exato e a idempotência precisa ler antes de decidir; tudo isso também precisaria de lock |
| Lock distribuído (Redis etc.) | outra peça para falhar, e o desafio proíbe confiar em algo fora do banco para a garantia |

**O custo, medido:** no teste de carga, a hot wallet tem p50 ~5–6× maior que as wallets distintas.
É a fila no lock, por desenho. Ver [§18](#18-teste-de-carga).

## 7. Garantias no schema

A restrição 9 da seção 5 pede que unicidade, imutabilidade e não-negatividade valham **no banco**.
O que está na migration ([`Migration20261113120000_initial-schema.ts`](./src/infrastructure/database/migrations/Migration20261113120000_initial-schema.ts)),
com testes em `schema.test.ts`:

| Garantia | Como |
|---|---|
| uma wallet por player + moeda | `UNIQUE (player_id, currency)` |
| saldo nunca negativo | `CHECK (balance_amount >= 0)` em `wallets`; `balance_after_amount >= 0` em transações e ledger |
| idempotência por provedor | `UNIQUE (provider_id, idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)` |
| uma reversão por referência **e por tipo** | índice único parcial `(reference_transaction_id, kind) WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')` |
| coerência da transação | `REFUND`/`ROLLBACK` exigem `reference_external_transaction_id`; `BET` não tem; `PROCESSED` tem `processed_at` e `balance_after`; reversão `PROCESSED` tem `reference_transaction_id`; **`failure_code` existe se e somente se** o status é `REJECTED`/`FAILED`; valor > 0, exceto `LOSS` |
| transação e lançamento pertencem à wallet certa | **FKs compostas**: `(wallet_id, player_id, currency) → wallets` e `(transaction_id, wallet_id) → wager_transactions`; o lançamento tem a moeda da wallet |
| um lançamento por transação | `UNIQUE (transaction_id, wallet_id)` no ledger |
| lançamento balanceado | `CHECK` com `after = before ± amount` conforme a direção |
| **ledger imutável** | triggers que recusam `UPDATE`, `DELETE` e `TRUNCATE` em `wallet_ledger_entries` |
| inbox | `PRIMARY KEY (consumer_name, message_id)` |

**Índices pensados pelas consultas que existem** (os parciais ficam pequenos para sempre, mesmo com
as tabelas crescendo):

- `ledger_wallet_cursor_idx (wallet_id, id)`: a paginação keyset do extrato;
- `outbox_pending_due_idx (next_attempt_at) WHERE published_at IS NULL`: o publisher;
- `wager_tx_pending_reference_due_idx (next_attempt_at) WHERE status = 'PENDING_REFERENCE'`: o worker;
- `wager_tx_waiting_for_reference_idx (provider_id, reference_external_transaction_id) WHERE status = 'PENDING_REFERENCE'`:
  quando a referência chega, achar quem esperava por ela;
- `wager_tx_wallet_idx (wallet_id)`.

**Fica fora do schema, de propósito:** "saldo == soma do ledger". Um trigger para isso seria caro e
frágil. Quem cobre essa regra é a [reconciliação](#14-reconciliação), junto com o `CHECK` de
balanceamento de cada lançamento e a corrente conferida nos testes.

## 8. Idempotência

- **A fonte da verdade é o header `Idempotency-Key`**, obrigatório, escopado **por provedor** (dois
  provedores podem usar a mesma key). O formato recomendado é `{providerId}:{externalTransactionId}`.
  O máximo é de 255 caracteres.
- **O `payloadHash`** é o SHA-256 (hex) do **JSON canônico** dos campos de negócio, com as chaves
  ordenadas recursivamente e os campos `undefined` omitidos:
  `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}, referenceExternalTransactionId`.
  O `money` entra **já normalizado** pelo `Money` (`"25"` vira `"25.00"`). Header e metadados de
  transporte (`messageId`, `occurredAt`, `idempotencyKey`, `x-correlation-id`) **não** entram.
  Implementação: [`payload-hash.ts`](./src/application/wagering/payload-hash.ts).
- **A decisão acontece dentro da transação, com a wallet travada:**

  | Situação | Resultado |
  |---|---|
  | key nova | processa |
  | mesma key, mesmo hash | **replay**: o resultado original, incluindo o **saldo daquele momento** (`balance_after` gravado na transação) e `idempotentReplay: true` |
  | mesma key, hash diferente | **conflito** (409), sem replay |
  | key nova, mas o mesmo `externalTransactionId` já existe | conflito (409): o id do provedor também é único |

- **Corrida entre pedidos iguais em wallets diferentes:** os dois passam pela checagem ao mesmo
  tempo, e a constraint `UNIQUE` barra o segundo no commit. O use case refaz a operação **uma vez**,
  e agora ela vira replay ou conflito. É contada como `wallet_lock_conflicts_total{type="idempotency_race"}`.
- **Um replay de uma rejeitada continua rejeitado,** devolvendo 422 com o mesmo `failureCode`,
  mesmo que agora houvesse saldo. Repetir uma operação devolve o resultado **original**
  (regra 7 da seção 7).
- **Pela fila há duas camadas.** A **inbox** `(consumerName, messageId)` vem primeiro, na mesma
  transação ([§12](#12-consumidor-sqs)). A idempotency key vem depois, então um provedor que
  reenvia a mesma operação com **outro** `messageId` também cai em replay.
- **Nada de idempotência em memória.** Tudo é linha no Postgres, válida entre instâncias e entre
  reinícios (provado no `restart.test.ts`).

## 9. API HTTP

A tabela de status e o que o provedor deve fazer em cada caso estão no [README](./README.md#status-http).
O ponto central é que **cada situação tem um status próprio**. O provedor decide se reenvia pelo
status, sem precisar interpretar texto:

| Situação | Status | Corpo |
|---|---|---|
| processada (nova) | 201 | o resultado |
| replay | 200 | o resultado original + `idempotentReplay: true` |
| aceita, aguardando a referência | 202 | `status: "PENDING_REFERENCE"` |
| rejeição de negócio | 422 | `status: "REJECTED"`, `failureCode` |
| payload inválido | 400 | `{ code, message }` (`INVALID_PAYLOAD`, `INVALID_MONEY`, `CURRENCY_MISMATCH`, `MISSING_REFERENCE`, `WALLET_PLAYER_MISMATCH`, …) |
| não encontrado | 404 | `WALLET_NOT_FOUND`, `WAGER_TRANSACTION_NOT_FOUND` |
| conflito de idempotência / wallet duplicada | 409 | `IDEMPOTENCY_CONFLICT`, `WALLET_ALREADY_EXISTS` |
| falha transitória | 503 + `Retry-After: 1` | `SERVICE_UNAVAILABLE`, sem detalhes internos |
| auth (ponto de extensão) | 401 / 403 | `UNAUTHENTICATED`, `PROVIDER_MISMATCH` |

Detalhes:

- **Rejeição de negócio é 422 com o corpo normal, não um erro.** A transação existe, foi gravada e é
  auditável (GET devolve 200 com o `failureCode`).
- **Moeda diferente da wallet é 400, e não 422.** O pedido é incompatível com a wallet, não é uma
  regra de saldo: nada é gravado e o provedor precisa corrigir.
- **"Transitório" é reconhecido pelo erro do driver:** os códigos de rede (`ECONNREFUSED`,
  `ECONNRESET`…), a classe 08 do SQLSTATE, `57P01-03`, `53300`, `40001` (serialização), `40P01`
  (deadlock), `55P03`, e as mensagens do `pg` sem código. Um bug continua sendo 500. A resposta
  503 não vaza host nem porta.
- **Formatos de erro:** os erros nossos usam `{ code, message }`. Os erros nativos do Nest (por
  exemplo, rota inexistente) mantêm o formato do Nest (`{ statusCode, message, error }`). Unificar
  seria trivial, mas não mudaria nenhuma decisão do provedor.
- **O extrato usa cursor keyset** (`id > cursor ORDER BY id LIMIT n+1`) sobre o índice
  `(wallet_id, id)`. O id é UUID v7, ordenado pelo tempo. O cursor é o id em base64url, opaco para o
  cliente, e continua estável com inserções durante a paginação (há teste para isso).

## 10. Regras de negócio e interpretações

| Operação | Saldo | Ledger | Regra |
|---|---|---|---|
| `BET` | débito | 1 `DEBIT` | sem saldo → `REJECTED` `INSUFFICIENT_FUNDS` |
| `WIN` | crédito | 1 `CREDIT` | |
| `LOSS` | — | nenhum | `PROCESSED` com o saldo atual; valor **pode ser 0** |
| `REFUND` | crédito | 1 `CREDIT` | só referencia `BET` `PROCESSED` |
| `ROLLBACK` | o inverso da referência | 1 lançamento invertido | referencia `BET`, `WIN` ou `REFUND` `PROCESSED` |

**Taxonomia de `failureCode`** (seção 7.2):

| `failureCode` | Quando | O provedor deve… |
|---|---|---|
| `INSUFFICIENT_FUNDS` | BET sem saldo | não reenviar a mesma; é a resposta final |
| `REVERSAL_INSUFFICIENT_FUNDS` | a reversão deixaria o saldo negativo (ex.: ROLLBACK de um WIN já gasto) | tratar manualmente; é uma situação operacional diferente de uma aposta sem saldo (regra 9) |
| `REFERENCE_NOT_FOUND` | a referência não chegou dentro do TTL (5 min) | conferir o id; reenviar com **outra** key se a referência existir |
| `REFERENCE_MISMATCH` | a referência é de outro player, wallet, moeda ou rodada | corrigir o payload |
| `INVALID_REFERENCE_KIND` | REFUND de algo que não é BET; ROLLBACK de LOSS ou de ROLLBACK | corrigir o payload |
| `REFERENCE_NOT_PROCESSED` | a referência existe, mas não está `PROCESSED` (rejeitada ou pendente) | não reenviar igual |
| `ALREADY_REVERSED` | a referência já foi revertida (por REFUND **ou** ROLLBACK) | nada a fazer |
| `AMOUNT_MISMATCH` | o valor é diferente do da referência (reversão parcial está fora do escopo) | corrigir o payload |

Os códigos `CURRENCY_MISMATCH`, `WALLET_NOT_FOUND` e `PERMANENT_FAILURE` existem no enum, mas hoje
**não são emitidos como `failureCode`**: os dois primeiros são erros de requisição (400/404), sem
nada gravado, e o terceiro está reservado para o status `FAILED`.

**Interpretações adotadas** (o desafio pede que sejam documentadas):

1. **A referência é resolvida por `(providerId, referenceExternalTransactionId)`** e precisa ter o
   mesmo player, wallet, moeda e **rodada** (regra 2).
2. **"Não pode ser revertida duas vezes"** conta **qualquer** reversão processada, e não só a do mesmo
   tipo. Senão um REFUND seguido de um ROLLBACK da mesma BET devolveria o dinheiro duas vezes. O
   índice único parcial no banco cobre o caso "mesmo tipo", e a regra de aplicação cobre o caso
   "tipo diferente", com a wallet travada.
3. **`ROLLBACK` de `REFUND`** é permitido: desfaz o estorno, debitando.
4. **`LOSS` com valor 0** é aceito (é o único kind que aceita 0). `LOSS` não gera lançamento nem
   `WalletBalanceChanged`, mas gera `WagerTransactionProcessed`.
5. **Uma `REJECTED` não altera saldo nem gera ledger, mas é gravada** (auditável e consultável) e gera
   `WagerTransactionRejected`.
6. **Reversão cuja referência ainda está `PENDING_REFERENCE`** é rejeitada com
   `REFERENCE_NOT_PROCESSED`, igual a uma referência rejeitada. Uma alternativa seria esperar
   também, mas cadeias de pendências mascarariam provedores com problema.
7. **`OPENING`** é uma transação interna, gerada na criação da wallet quando o saldo inicial é > 0,
   com o crédito correspondente na mesma transação SQL. Não é aceita pela API.

## 11. Transactional outbox

- **Os eventos entram na `outbox_messages` na mesma transação da aposta.** Um rollback leva os
  eventos junto, então nunca existe evento de algo que não aconteceu (restrição 4 da seção 5).
- **O publisher é um worker por instância:**

  ```
  BEGIN
    SELECT … FROM outbox_messages WHERE published_at IS NULL AND next_attempt_at <= now()
      ORDER BY next_attempt_at LIMIT 100 FOR UPDATE SKIP LOCKED
    SendMessageBatch (blocos de 10, em série)
    UPDATE … SET published_at = now() WHERE id = ANY(publicados)      -- um UPDATE só
    UPDATE das falhas: attempts + 1, next_attempt_at com backoff, last_error
  COMMIT
  ```

- **`SKIP LOCKED`:** vários publishers dividem a outbox sem esperar uns pelos outros e sem pegar a
  mesma linha. No teste, 3 instâncias × 2 publishers sobre 100 eventos publicam **cada evento
  exatamente uma vez**. O teste conta as **chamadas ao publisher**, e não as mensagens na fila,
  porque a deduplicação do SQS esconderia uma duplicata.
- **O cenário obrigatório (seção 11)**, provado no `outbox-publisher.test.ts`: o commit acontece, o
  processo publica e **morre antes do seu próprio commit** (simulado com falha no `UPDATE`); o
  rollback devolve o lock; outra instância assume e publica de novo, com o **mesmo `eventId`**.
- **A duplicata é segura** em duas camadas. O `MessageDeduplicationId = eventId` faz o SQS FIFO
  descartar a cópia em 5 minutos. Depois disso, o consumidor deduplica pelo `eventId`.
  **At-least-once, nunca at-most-once.**
- **Fila de saída separada (`wagering-events.fifo`).** O desafio só define a fila de **entrada**.
  Publicar os eventos nela faria o nosso consumidor consumir os próprios eventos.
- **A ordem é por agregado:** `MessageGroupId = aggregateId`. Os blocos do `SendMessageBatch` vão em
  série justamente para não inverter eventos do mesmo agregado. Um retry ainda pode reordenar (um
  evento que falhou sai depois do seguinte), então **a ordem é otimização, não garantia**: o
  consumidor deve usar o `eventId` para deduplicar e o `walletVersion` (no `WalletBalanceChanged`)
  para ordenar.
- **O backoff vai de 1 s, 2 s, 4 s… até 5 min, sem limite de tentativas.** Um evento confirmado
  **nunca** é descartado. Se o SQS ficar fora por horas, o evento espera. Quem avisa é a métrica
  `outbox_oldest_pending_age_seconds`, junto com `attempts` e `last_error` na linha.
- **Trade-off: o lock fica preso durante a chamada ao SQS.** É simples e seguro (o commit só
  acontece se o publish terminou), mas segura conexões. Por isso o SQS client tem timeout de 5 s e
  `maxAttempts: 1` (quem faz retry é a outbox). A alternativa seria uma lease, como no worker de
  `PENDING_REFERENCE`.
- **O lote veio de uma medição.** No teste de carga, uma `SendMessage` por evento deixava a outbox
  para trás: lag p95 de 23–26 s. Com `SendMessageBatch` e o `UPDATE` único, caiu para ~4 s, num A/B
  nas mesmas condições ([REPORT.md](./test/load/REPORT.md#histórico-o-que-este-teste-mudou-no-código)).

## 12. Consumidor SQS

Para cada mensagem de `wager-transactions.fifo`:

1. **Parse e validação do envelope** (`messageId`, `type = WagerTransactionRequested`, `data`). O
   `data` passa pela **mesma** validação do HTTP.
2. **O mesmo use case do HTTP,** com a **inbox na mesma transação**:
   `INSERT inbox_messages (consumer_name, message_id) … ON CONFLICT DO NOTHING`. Se não inseriu, é
   `DuplicateMessageError`, e nada é processado. Duas entregas simultâneas da mesma mensagem se
   resolvem sozinhas: a segunda **espera** a primeira (pela PK). Se a primeira fizer commit, a
   segunda vira duplicata; se fizer rollback, a segunda processa.
3. **Ack (DeleteMessage) só depois do commit.**

**A decisão por tipo de erro** ([`message-failure-policy.ts`](./src/infrastructure/messaging/message-failure-policy.ts),
uma função pura com testes de unidade):

| Aconteceu | Ação |
|---|---|
| processada (PROCESSED, REJECTED ou PENDING_REFERENCE) | ack |
| duplicata (inbox) | ack, sem efeito |
| erro de negócio (conflito de idempotência, wallet inexistente) | ack e log: tentar de novo daria o mesmo erro |
| malformada (JSON, envelope, payload, moeda incompatível) | **DLQ imediata**, com o atributo `deadLetterReason` |
| transitória (banco, deadlock, conflito de versão) | **não dá ack**; `ChangeMessageVisibility` com backoff (5 s, 10 s, 20 s… até 5 min) |
| desconhecida (bug) | retry, como a transitória. Uma aposta **não pode sumir** por um erro que não entendemos |
| 5 entregas sem sucesso | o **redrive do SQS** move para a DLQ (`maxReceiveCount` = 5) |

**Decisões e trade-offs:**

- **A ordem dos `if` importa.** `DuplicateMessageError` e `ConcurrentWalletUpdateError` também são
  `DomainError`. Se o caso genérico viesse antes, um conflito de lock levaria **ack sem processar**.
  Há um teste específico para isso.
- **A inbox usa o `messageId` do corpo, e não o `MessageId` do SQS.** O SQS gera um id novo a cada
  envio, e um reenvio do provedor teria outro id. O id do corpo é estável entre os reenvios.
- **A ordem do grupo FIFO é preservada:** se uma mensagem de um grupo vai para retry, as seguintes
  **do mesmo grupo** naquele lote voltam para a fila sem processar (visibilidade 0). Os outros grupos
  seguem. O custo é que o `receiveCount` delas também sobe.
- **Os retries transitórios contam para o `maxReceiveCount`.** Com o backoff de 5+10+20+40 s, uma
  queda do banco de mais de ~75 s manda as mensagens para a DLQ. Elas não se perdem, mas precisam de
  um **redrive manual**. A alternativa (mais entregas ou backoff maior) atrasaria a detecção de
  mensagens que nunca vão funcionar.
- **Um erro de negócio dá ack sem gravar a inbox** (o rollback leva a inbox junto). Se a mesma
  mensagem voltar, dá o mesmo erro e o mesmo ack: é inofensivo.
- **SIGTERM:** o `beforeApplicationShutdown` (que roda **antes** de o MikroORM fechar as conexões)
  aborta o long polling na hora, deixa a mensagem em andamento terminar (commit + ack) e devolve as
  que ainda não começaram. No teste, o shutdown leva < 2 s. Se o processo morrer de verdade, a
  visibilidade vence e outra instância recebe a mensagem, e aí a inbox resolve.

## 13. Referências fora de ordem (PENDING_REFERENCE)

Um `REFUND` ou `ROLLBACK` cuja referência ainda não existe é gravado como `PENDING_REFERENCE` (202).
A primeira nova tentativa fica agendada para 1 s depois, e o evento `WagerTransactionPendingReference`
é emitido.

- **Dois caminhos para acordar a pendência:**
  1. **O worker agendado** (seção 7.1) reserva as pendências vencidas e tenta de novo, com backoff de
     1 s, 2 s, 4 s… até 60 s.
  2. **Quando a referência chega**, a transação que acabou de terminar faz
     `UPDATE … SET next_attempt_at = now()` em quem estava esperando por ela. O worker resolve na
     volta seguinte, sem esperar o backoff. Essa busca usa o índice
     `wager_tx_waiting_for_reference_idx`. No teste de ponta a ponta (REFUND pela fila, BET pelo HTTP),
     a resolução levou ~1 s.
- **Lease em vez de lock longo:** a reserva empurra o `next_attempt_at` para 30 s no futuro
  (`UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`). Cada item é resolvido numa
  transação própria: **wallet primeiro, depois a transação**, a mesma ordem do use case principal.
  Se o worker morrer, a lease vence e outra instância assume. Se dois workers pegarem o mesmo item
  (lease vencida no meio), o segundo vê que ele já não está pendente e pula.
- **A resolução usa as mesmas regras** (`WagerRules`): PROCESSED (com saldo e ledger) ou REJECTED
  com o código da regra (`AMOUNT_MISMATCH`, `ALREADY_REVERSED`…). Uma resolução também acorda quem
  esperava **por ela**, então uma cadeia como ROLLBACK de um REFUND de uma BET se resolve sozinha.
- **TTL de 5 min, contado desde a chegada.** Esgotado o prazo sem referência, a transação vira
  `REJECTED` com `REFERENCE_NOT_FOUND` e o evento é publicado. Por que 5 min: um provedor que demora
  mais que isso para mandar a BET de um REFUND está com problema, e segurar mais tempo só atrasa o
  sinal. O TTL só rejeita quem **continua** sem referência: se a referência aparecer depois do
  prazo, mas antes da próxima tentativa, a transação ainda é processada.
- **Um replay depois da resolução devolve o resultado final:** 200 com PROCESSED, ou 422 com
  `REFERENCE_NOT_FOUND`.

## 14. Reconciliação

`POST /wallets/:walletId/reconciliation` devolve o formato da seção 9:
`storedBalance`, `calculatedBalance` (créditos − débitos do ledger), `difference`, `consistent` e
`checkedEntries`.

- **Um único `SELECT`, um único snapshot.** Com duas consultas separadas, uma aposta que fizesse
  commit entre elas daria uma divergência **falsa**. O comando único enxerga um snapshot MVCC, **sem
  travar a wallet**, então reconciliar não atrapalha a hot wallet. Há um teste com 20 reconciliações
  no meio de 40 apostas concorrentes: todas consistentes.
- **POST, e não GET,** porque é uma ação com efeito: incrementa a métrica e, se divergir, escreve um
  log de erro. A resposta é **200 nos dois casos**: a divergência é um resultado, não uma falha da
  requisição.
- **Divergência nunca é corrigida.** Corrigir esconderia o bug que a causou. Ela é logada como
  `error` (com walletId e os valores), contada em `wallet_reconciliations_total{result="divergent"}`
  e sinalizada com `consistent: false`. O teste corrompe o saldo direto no banco e confere que ele
  **continua** errado depois.

## 15. Observabilidade

**Logs** (seção 12):
- Um `JsonLogger` do Nest escreve uma linha JSON por evento no stdout.
- Os campos `correlationId`, `messageId`, `sqsMessageId`, `transactionId`, `walletId` e `providerId`
  entram **sozinhos** em toda linha, pelo `AsyncLocalStorage`. O contexto é aberto pelo middleware
  HTTP, pelo consumidor (por mensagem) e pelo worker (por item), e é completado pelo use case. Um
  log de **qualquer** camada (um filtro de erro, um repositório) sai com o `correlationId` do pedido
  que o causou.
- **Sem payload financeiro:** os campos `money`, `payload`, `body`, `input`, `authorization`,
  `password` e `token` são trocados por `[redacted]` como rede de segurança. Um teste faz apostas de
  `777.77` e `999.99` e confirma que nenhuma linha contém esses valores. A exceção consciente é o log
  de **divergência da reconciliação**, que traz os saldos, porque é o que alguém precisa para
  investigar.
- Há um access log por requisição, com o nível pelo status (5xx `error`, 4xx `warn`). Com
  `LOG_FORMAT=pretty`, o formato é o do Nest, para ler no terminal.

**Métricas** (`GET /metrics`, formato Prometheus, sem dependência externa):

| Exigência da seção 12 | Métrica |
|---|---|
| transações por status | `wager_transactions_total{channel,kind,status}` |
| duplicatas | `wager_duplicates_total{mechanism="idempotency_replay"\|"inbox"}` |
| retries | `sqs_messages_total{outcome="retried"}`, `outbox_publish_failures_total`, `pending_reference_resolutions_total{outcome="rescheduled"}` |
| DLQ | `sqs_dead_letters_total{reason}` + `sqs_dead_letter_queue_messages` (a profundidade real, inclusive o que o redrive moveu) |
| conflitos de lock | `wallet_lock_conflicts_total{type}` + `wallet_lock_wait_seconds` (a espera no `FOR UPDATE`: é o sinal de hot wallet) |
| outbox lag | `outbox_oldest_pending_age_seconds`, `outbox_pending_messages`, `outbox_publish_lag_seconds` |
| latência | `wager_transaction_processing_seconds`, `sqs_message_processing_seconds`, `http_request_duration_seconds{route}` |

- **Contadores só sobem depois do commit:** um rollback não conta como processado, e um evento que
  vai ser republicado não conta duas vezes.
- **Os gauges de estado global** (outbox, pendências, DLQ) **são lidos do banco e do SQS na hora do
  scrape.** Qualquer instância responde o número certo, e uma fonte fora não derruba o `/metrics`
  (a falha conta em `metrics_collection_errors_total`).
- **A label da duração HTTP é a rota** (`/wallets/:walletId`), **nunca a URL**. Um id por série
  explodiria a cardinalidade.

**Health** (seção 9):
- `/health/live` diz só "o processo está de pé". Ele **não** olha o banco: se o Postgres cair,
  reiniciar a instância não resolve, e um liveness dependente faria o orquestrador reiniciar tudo
  em cascata.
- `/health/ready` testa o Postgres (`select 1`) e as filas (`GetQueueUrl`) em paralelo, com
  **timeout de 2 s** por dependência. Responde 503 dizendo **qual** caiu, e o detalhe vai só para o
  log.
- Os dois são abertos (seção 2), assim como o `/metrics`. Em produção, este último ficaria restrito
  à rede interna.

## 16. Autenticação

**Decisão: não foi implementada.** A autenticação não vale pontos (seção 2), e o tempo foi para
correção, concorrência e mensageria. Mas o **ponto de extensão já existe e funciona**:

- [`ProviderAuthenticator`](./src/http/auth/provider-authenticator.ts) é a porta, recebe headers,
  método e rota, e devolve `{ authenticated, principal? }`. A implementação atual é
  `NoopProviderAuthenticator`: tudo passa e não há principal.
- [`ProviderAuthGuard`](./src/http/auth/provider-auth.guard.ts) é **global**. As rotas `@Public()`
  (health e métricas) não passam por ele. Sem credencial válida, a resposta é **401**.
- **Com um principal, o provedor só opera em nome de si mesmo.** Outro `providerId` no corpo ou na
  rota dá **403** `PROVIDER_MISMATCH`. A transação de outro provedor buscada por id dá **404**, para
  não revelar que ela existe.
- Os testes (`auth.test.ts`) trocam o comportamento da porta e provam o guard, as rotas públicas e
  o 403/404 sem nenhuma mudança no código.

**O desenho que eu adotaria:**
- Um IdP externo (**Keycloak** ou **Zitadel**) no Compose, com **OAuth2 client credentials**: cada
  provedor de jogos é um *client*.
- O `OidcProviderAuthenticator` valida o JWT (assinatura via JWKS, `iss`, `aud`, `exp`) e lê o
  `provider_id` de um claim. É **uma classe**, e o `useClass` muda no `AppModule`.
- As rotas de wallet (criar, reconciliar) ficariam com outro escopo (*operator*), separado de
  *provider*.
- **Mensagens da fila não passam por autenticação:** são canal interno confiável (seção 2). O
  `providerId` delas continua sujeito às mesmas regras de domínio (referência do mesmo provedor,
  idempotência por provedor).

## 17. Estratégia de testes

- **760 testes**, `bun test`, ~85 s. Integração **real**: Postgres 17 e MiniStack em containers.
  **Nenhum mock de Postgres ou SQS.** Os spies só **injetam falhas** em pontos exatos: o banco cai
  no insert do ledger, o processo morre antes do ack, o SQS recusa uma entrada do lote.
- **Isolamento:** banco `wagering_test`, recriado **pelas migrations** a cada execução (então toda
  execução testa as migrations), e filas `*-test.fifo` criadas e configuradas pelos testes. Duas
  travas impedem que eles rodem contra o banco ou as filas de dev.
- **Os workers ficam desligados nos testes** (`*_ENABLED=false`). Cada teste chama a "volta"
  (`execute()`, `pollOnce()`) na mão, o que dá determinismo. Só os testes de loop e shutdown ligam o
  loop de verdade.
- **Paralelismo real:** `Promise.all` de requisições HTTP contra 3 apps Nest **no mesmo processo**,
  cada uma com o seu pool. No `restart.test.ts`, contra **processos separados** (`bun src/main.ts`),
  com um deles morto por `SIGKILL` no meio da rajada.
- **A invariante final:** o helper `assertLedgerMatchesBalance` confere o saldo e a **corrente** de
  lançamentos.
- **Testes que pegam bugs:** as garantias principais foram validadas estragando o código de
  propósito e conferindo que o teste certo falha:

  | Estrago | Pego por |
  |---|---|
  | `FOR UPDATE` sem `SKIP LOCKED` na outbox | o teste "a outra instância pula" |
  | outbox sem lock | publishers concorrentes (duplicaram eventos) |
  | inbox sem checagem de duplicata | 3 testes de inbox |
  | `down()` da migration esquecendo o `DROP FUNCTION` | o teste de reversibilidade (sobra no catálogo, e o `up` seguinte quebra) |
  | consumidor sem o bloqueio do grupo FIFO | o teste de ordem |
  | erro transitório tratado como ack | 3 testes (as apostas sumiriam) |
  | a BET não acorda a pendência | 11 testes de `PENDING_REFERENCE` |
  | lock transação → wallet (ordem invertida) | o teste de concorrência (**deadlock** real) |

## 18. Teste de carga

`bun run test:load`. A metodologia completa e os números estão no [REPORT.md](./test/load/REPORT.md).

- 3 instâncias reais (processos separados, com os workers), 48 usuários virtuais por 20 s, 200
  wallets + 1 hot wallet, 15% de reenvios duplicados, tudo na mesma máquina (8 CPUs).
- **Resultado típico:** ~240 req/s, p50/p95/p99 ≈ 110/800/1130 ms, **0% de erro, 0 divergências em
  201 wallets reconciliadas, 0 saldos negativos, 0 conflitos de versão.**

**O que a carga ensinou:**
1. **O custo da hot wallet** é ~5–6× no p50. É a fila no lock, e é o preço da garantia escolhida
   ([§6](#6-concorrência)).
2. **A outbox foi o gargalo:** uma `SendMessage` por evento. Publicar em lote baixou o lag p95 de
   ~24 s para ~4 s (A/B nas mesmas condições).
3. **A primeira hipótese para a variação estava errada.** Achei que fosse disputa de event loop; era
   o **MiniStack degradando** com o volume acumulado (de ~11 ms para ~57 ms por `SendMessageBatch`).
   O teste passou a usar uma fila nova por execução.
4. **Workers em toda instância** (3 publishers) renderam mais que **um processo dedicado** (lag p95
   de ~4,0 s contra ~5 s), então o padrão ficou `embedded`.

## 19. Limitações e próximos passos

| Limitação | Impacto | Próximo passo |
|---|---|---|
| **A ordem dos eventos é por agregado, mas um retry pode reordenar** | o consumidor precisa tolerar | documentar no contrato: deduplicar por `eventId`, ordenar por `walletVersion` |
| **O publisher segura o lock durante o SQS** | conexões presas se o SQS ficar lento (há timeout de 5 s) | lease, como no worker de `PENDING_REFERENCE` |
| **O replay também espera o lock da wallet** | numa hot wallet, um reenvio entra na fila | ler a `Idempotency-Key` **antes** do `FOR UPDATE` e responder o replay sem travar |
| **O retry transitório conta para o `maxReceiveCount`** | uma queda > ~75 s manda mensagens para a DLQ | backoff maior ou um contador próprio; tooling de redrive da DLQ |
| **Não há limpeza da `outbox_messages` e da `inbox_messages`** | as tabelas crescem para sempre (as consultas usam índices parciais, então continuam baratas) | job de retenção/arquivamento (ex.: publicados há mais de 7 dias) |
| **As métricas ficam em memória, por instância** | precisam de um Prometheus fazendo scrape de cada instância | OpenTelemetry (métricas + tracing distribuído) |
| **Escala de dinheiro fixa em 2** | não suporta JPY (0 casas) nem KWD (3) | escala por moeda no `Money` e no schema |
| **Autenticação desligada** | não pode ser exposto assim | `OidcProviderAuthenticator` ([§16](#16-autenticação)) |
| **A ordem por UUID v7 depende do relógio de cada instância** | com clock skew, o extrato pode trocar a ordem de lançamentos muito próximos (a paginação continua estável) | sequência por wallet (`walletVersion`) como ordem do extrato |
| **MiniStack ≠ AWS** | o desempenho e alguns comportamentos diferem (ex.: a degradação com volume) | validar os números de carga em SQS de verdade |
| **Pool padrão de 10 conexões por instância** | limita a vazão por instância | dimensionar o pool junto com o `max_connections` do Postgres |
| **Um Postgres único** | ponto único de falha | réplica com failover; leituras (extrato, GETs) na réplica |
