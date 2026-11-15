import { Inject, Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import type { MoneyProps } from "../../domain/money/money.ts";
import { CurrencyMismatchError } from "../../domain/money/money.errors.ts";
import type { FailureCode } from "../../domain/wager-transaction/failure-code.ts";
import { WagerTransaction } from "../../domain/wager-transaction/wager-transaction.ts";
import { WagerTransactionStatus } from "../../domain/wager-transaction/wager-transaction.enums.ts";
import type { Wallet } from "../../domain/wallet/wallet.ts";
import type { WagerTransactionInput } from "../../http/wager-transaction.request.ts";
import { isUniqueViolation } from "../../infrastructure/database/constraint-violation.ts";
import { LedgerEntryRepository } from "../../infrastructure/database/repositories/ledger-entry.repository.ts";
import { WagerTransactionRepository } from "../../infrastructure/database/repositories/wager-transaction.repository.ts";
import { WalletRepository } from "../../infrastructure/database/repositories/wallet.repository.ts";
import { WalletNotFoundError } from "../wallet/wallet.errors.ts";
import { decideIdempotency } from "./idempotency.ts";
import { computePayloadHash } from "./payload-hash.ts";
import { DuplicateMessageError, IdempotencyConflictError, WalletPlayerMismatchError } from "./wagering.errors.ts";
import { OutboxMessageRepository } from "../../infrastructure/database/repositories/outbox-message.repository.ts";
import { InboxMessageRepository } from "../../infrastructure/database/repositories/inbox-message.repository.ts";
import { OutboxMessage } from "../../domain/outbox/outbox-message.ts";
import type { EventContext } from "../../domain/shared/integration-events.ts";
import { WagerRules } from "./wager-rules.ts";
import { nextPendingAttemptAt, PENDING_REFERENCE_POLICY, type PendingReferencePolicy } from "./pending-reference-policy.ts";

export interface WagerTransactionResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: FailureCode;
}

/** Quando a entrada vem da fila: quem está consumindo e o id da mensagem. */
export interface InboxContext {
  consumerName: string;
  messageId: string;
}

@Injectable()
export class ProcessWagerTransactionUseCase {
  constructor(
    private readonly em: EntityManager,
    private readonly wallets: WalletRepository,
    private readonly transactions: WagerTransactionRepository,
    private readonly ledger: LedgerEntryRepository,
    private readonly outbox: OutboxMessageRepository,
    private readonly inbox: InboxMessageRepository,
    private readonly rules: WagerRules,
    @Inject(PENDING_REFERENCE_POLICY) private readonly pendingPolicy: PendingReferencePolicy,
  ) {}

  /**
   * @param context quem chamou (HTTP ou SQS) diz o correlationId dos eventos.
   *                Sem contexto, gera um id novo (a cada chamada).
   * @param inbox   só a entrada pela FILA passa isto: o registro na inbox entra
   *                na MESMA transação da aposta (ou os dois existem, ou nenhum).
   * @throws DuplicateMessageError se a mensagem já tinha sido processada.
   */
  async execute(
    input: WagerTransactionInput,
    context: EventContext = { correlationId: Bun.randomUUIDv7() },
    inbox?: InboxContext,
  ): Promise<WagerTransactionResult> {
    const payloadHash = computePayloadHash(input);
    try {
      return await this.em.transactional((tx) => this.process(tx, input, payloadHash, context, inbox));
    } catch (error) {
      if (
        isUniqueViolation(error, "wager_tx_provider_idempotency_key_uq") ||
        isUniqueViolation(error, "wager_tx_provider_external_id_uq")
      ) {
        return await this.em.transactional((tx) => this.process(tx, input, payloadHash, context, inbox));
      }
      throw error;
    }
  }

  private async process(
    tx: EntityManager,
    input: WagerTransactionInput,
    payloadHash: string,
    context: EventContext,
    inbox: InboxContext | undefined,
  ): Promise<WagerTransactionResult> {
    // 0. Inbox (só quando veio da fila): a 2ª entrega da mesma mensagem para aqui,
    //    antes de disputar o lock da wallet com as apostas de verdade.
    //    `throw` (e não `return`): não há nada a gravar, e o consumidor recebe um
    //    tipo de erro claro, que ele trata como "dá ack".
    if (inbox) {
      const isNew = await this.inbox.tryInsert(tx, { ...inbox, payloadHash });
      if (!isNew) throw new DuplicateMessageError(inbox.consumerName, inbox.messageId);
    }

    // 1. TRAVA a wallet. Daqui até o commit, ninguém mais mexe nela.
    const wallet = await this.wallets.findByIdForUpdate(tx, input.walletId);
    if (!wallet) throw new WalletNotFoundError(input.walletId);

    // 2. Idempotência pela key.
    const existing = await this.transactions.findByIdempotencyKey(tx, input.providerId, input.idempotencyKey);
    const decision = decideIdempotency(existing, payloadHash);
    if (decision.type === "replay") return this.toResult(decision.transaction, wallet, true);
    if (decision.type === "conflict") throw new IdempotencyConflictError(input.idempotencyKey, decision.transaction.id);

    // 3. Mesmo externalTransactionId com OUTRA key também é conflito.
    const sameExternalId = await this.transactions.findByExternalId(tx, input.providerId, input.externalTransactionId);
    if (sameExternalId) throw new IdempotencyConflictError(input.idempotencyKey, sameExternalId.id);

    // 4. O pedido é compatível com a wallet?
    if (wallet.playerId !== input.playerId) throw new WalletPlayerMismatchError(input.walletId, input.playerId);
    if (wallet.currency !== input.money.currency) throw new CurrencyMismatchError(wallet.currency, input.money.currency);

    // 5. Cria a transação (o domínio valida a referência e o valor).
    const transaction = WagerTransaction.create({ ...input, id: Bun.randomUUIDv7(), payloadHash });
    const expectedVersion = wallet.version; // ANTES de qualquer debit/credit

    // 6. Aplica a regra do kind (wager-rules.ts, as mesmas regras do worker de PENDING_REFERENCE).
    const outcome = await this.rules.apply(tx, transaction, wallet);
    const now = new Date();

    // 7. Grava tudo. A ordem importa por causa das FKs: transação antes do lançamento.
    //    PENDING_REFERENCE nasce agendada para a 1ª nova tentativa do worker.
    const nextAttemptAt =
      transaction.status === WagerTransactionStatus.PendingReference
        ? nextPendingAttemptAt(now, 0, this.pendingPolicy)
        : undefined;
    await this.transactions.insert(tx, transaction, { nextAttemptAt });
    if (outcome.entry) {
      await this.wallets.update(tx, wallet, expectedVersion);
      await this.ledger.insert(tx, outcome.entry);
    }

    // 7b. Alguém estava esperando por ESTA transação (um REFUND/ROLLBACK que chegou
    //     antes)? Avisa: o worker resolve na próxima volta, sem esperar o backoff.
    if (transaction.isTerminal()) {
      await this.transactions.wakeUpWaitingFor(tx, transaction.providerId, transaction.externalTransactionId, now);
    }

    // 8. Os eventos vão para a OUTBOX, na MESMA transação SQL. Se algo acima
    //    falhar, o rollback leva os eventos junto: nunca existe evento de algo
    //    que não aconteceu. Quem publica no SQS é o worker, DEPOIS do commit.
    //    Replay e conflito saíram lá no passo 2/3, então não geram evento de novo.
    for (const event of this.rules.eventsFor(transaction, wallet, outcome, context)) {
      await this.outbox.insert(tx, OutboxMessage.enqueue(event));
    }

    return this.toResult(transaction, wallet, false);
  }

  private toResult(transaction: WagerTransaction, wallet: Wallet, idempotentReplay: boolean): WagerTransactionResult {
    return {
      transactionId: transaction.id,
      status: transaction.status,
      balance: (transaction.balanceAfter ?? wallet.balance).toJSON(),
      idempotentReplay,
      ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
    };
  }
}
