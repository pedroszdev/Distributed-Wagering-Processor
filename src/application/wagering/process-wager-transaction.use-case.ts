import { Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { LedgerDirection } from "../../domain/ledger/ledger-direction.ts";
import { WalletLedgerEntry } from "../../domain/ledger/ledger-entry.ts";
import type { MoneyProps } from "../../domain/money/money.ts";
import { CurrencyMismatchError } from "../../domain/money/money.errors.ts";
import { FailureCode } from "../../domain/wager-transaction/failure-code.ts";
import { WagerTransaction } from "../../domain/wager-transaction/wager-transaction.ts";
import { WagerTransactionKind, WagerTransactionStatus } from "../../domain/wager-transaction/wager-transaction.enums.ts";
import type { Wallet } from "../../domain/wallet/wallet.ts";
import { InsufficientFundsError } from "../../domain/wallet/wallet.errors.ts";
import type { WagerTransactionInput } from "../../http/wager-transaction.request.ts";
import { isUniqueViolation } from "../../infrastructure/database/constraint-violation.ts";
import { LedgerEntryRepository } from "../../infrastructure/database/repositories/ledger-entry.repository.ts";
import { WagerTransactionRepository } from "../../infrastructure/database/repositories/wager-transaction.repository.ts";
import { WalletRepository } from "../../infrastructure/database/repositories/wallet.repository.ts";
import { WalletNotFoundError } from "../wallet/wallet.errors.ts";
import { decideIdempotency } from "./idempotency.ts";
import { computePayloadHash } from "./payload-hash.ts";
import { checkReversal } from "./reversal-rules.ts";
import { DuplicateMessageError, IdempotencyConflictError, WalletPlayerMismatchError } from "./wagering.errors.ts";
import { OutboxMessageRepository } from "../../infrastructure/database/repositories/outbox-message.repository.ts";
import { InboxMessageRepository } from "../../infrastructure/database/repositories/inbox-message.repository.ts";
import { OutboxMessage } from "../../domain/outbox/outbox-message.ts";
import type { EventContext, IntegrationEvent } from "../../domain/shared/integration-events.ts";
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from "../../domain/wager-transaction/wager-transaction.events.ts";
import { WalletBalanceChanged } from "../../domain/wallet/wallet.events.ts";

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

interface RulesOutcome {
  entry?: WalletLedgerEntry; // existe quando o saldo mudou
  nextAttemptAt?: Date; // existe quando ficou PENDING_REFERENCE
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

    // 6. Aplica a regra do kind.
    const outcome = await this.applyRules(tx, transaction, wallet);

    // 7. Grava tudo. A ordem importa por causa das FKs: transação antes do lançamento.
    await this.transactions.insert(tx, transaction, { nextAttemptAt: outcome.nextAttemptAt });
    if (outcome.entry) {
      await this.wallets.update(tx, wallet, expectedVersion);
      await this.ledger.insert(tx, outcome.entry);
    }

    // 8. Os eventos vão para a OUTBOX, na MESMA transação SQL. Se algo acima
    //    falhar, o rollback leva os eventos junto: nunca existe evento de algo
    //    que não aconteceu. Quem publica no SQS é o worker (item 7), DEPOIS do commit.
    //    Replay e conflito saíram lá no passo 2/3, então não geram evento de novo.
    for (const event of this.eventsFor(transaction, wallet, outcome, context)) {
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

  /** Decide o que acontece com cada kind. Muda a transaction e a wallet em memória. */
  private async applyRules(tx: EntityManager, transaction: WagerTransaction, wallet: Wallet): Promise<RulesOutcome> {
    switch(transaction.kind) {
        case(WagerTransactionKind.Loss):{
            const data = new Date()
            transaction.markProcessed(undefined, data, wallet.balance)
            return {}
        }
        case(WagerTransactionKind.Bet):
        case(WagerTransactionKind.Win):{
            const direction = transaction.ledgerDirectionFor()
            return this.move(transaction,wallet, direction, undefined, FailureCode.InsufficientFunds)
        }
        case(WagerTransactionKind.Refund):
        case(WagerTransactionKind.Rollback):{
            const reference = await this.transactions.findByExternalId(tx, transaction.providerId, transaction.referenceExternalTransactionId!)

            if (!reference) {
                transaction.markPendingReference()
                const umSegundo: Date = new Date(Date.now() + 1000);
                return {nextAttemptAt: umSegundo}
            }
            const alreadyReversed = await this.transactions.hasProcessedReversal(tx, reference.id)
            const failure = checkReversal(transaction, reference, alreadyReversed)
            if (failure) {
                transaction.reject(failure)
                return {}
            }
            const direction = transaction.ledgerDirectionFor(reference)
            return this.move(transaction, wallet, direction, reference.id, FailureCode.ReversalInsufficientFunds)
        }   
        case(WagerTransactionKind.Opening):
            throw new Error('Kind não pode ser Opening')
    }
  }

  /** Debita ou credita, marca como processada e monta o lançamento. Sem saldo → rejeita. */
  private move(
    transaction: WagerTransaction,
    wallet: Wallet,
    direction: LedgerDirection,
    referenceTransactionId: string | undefined,
    insufficientFundsCode: FailureCode,
  ): RulesOutcome {
    try {
      const {before, after} = direction === LedgerDirection.Debit ? wallet.debit(transaction.money) : wallet.credit(transaction.money)
      transaction.markProcessed(referenceTransactionId, new Date(), after)
      const id = Bun.randomUUIDv7()
      const entry = WalletLedgerEntry.create({
        id,
        walletId: wallet.id,
        transactionId: transaction.id,
        direction,
        money: transaction.money,
        balanceBefore: before,
        balanceAfter: after


      })
      return { entry }
    } catch (error) {
      if (error instanceof InsufficientFundsError) {
        transaction.reject(insufficientFundsCode);
        return {};
      }
      throw error;
    }
  }

  /** Quais eventos este resultado gera (tabela da seção 11 do README). */
  private eventsFor(
    transaction: WagerTransaction,
    wallet: Wallet,
    outcome: RulesOutcome,
    context: EventContext,
  ): IntegrationEvent<unknown>[] {
    switch (transaction.status) {
      case WagerTransactionStatus.Processed: {
        const events: IntegrationEvent<unknown>[] = [WagerTransactionProcessed.from(transaction, context)];
        // "Somente quando o saldo muda": o entry só existe se houve débito/crédito.
        // LOSS é PROCESSED mas não tem entry → só o evento acima.
        if (outcome.entry) {
          events.push(WalletBalanceChanged.from(wallet, outcome.entry, context));
        }
        return events;
      }
      case WagerTransactionStatus.Rejected:
        return [WagerTransactionRejected.from(transaction, context)];

      case WagerTransactionStatus.PendingReference:
        return [WagerTransactionPendingReference.from(transaction, context)];

      default:
        // PENDING ou FAILED aqui = bug no applyRules (toda regra termina num status final).
        throw new Error(`Transação ${transaction.id} terminou o processamento em ${transaction.status}`);
    }
  }

}
