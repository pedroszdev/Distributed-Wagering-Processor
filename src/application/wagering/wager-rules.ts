import type { EntityManager } from "@mikro-orm/postgresql";
import { Injectable } from "@nestjs/common";
import { LedgerDirection } from "../../domain/ledger/ledger-direction.ts";
import { WalletLedgerEntry } from "../../domain/ledger/ledger-entry.ts";
import type { EventContext, IntegrationEvent } from "../../domain/shared/integration-events.ts";
import { FailureCode } from "../../domain/wager-transaction/failure-code.ts";
import { WagerTransactionKind, WagerTransactionStatus } from "../../domain/wager-transaction/wager-transaction.enums.ts";
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from "../../domain/wager-transaction/wager-transaction.events.ts";
import type { WagerTransaction } from "../../domain/wager-transaction/wager-transaction.ts";
import { InsufficientFundsError } from "../../domain/wallet/wallet.errors.ts";
import { WalletBalanceChanged } from "../../domain/wallet/wallet.events.ts";
import type { Wallet } from "../../domain/wallet/wallet.ts";
import { WagerTransactionRepository } from "../../infrastructure/database/repositories/wager-transaction.repository.ts";
import { checkReversal } from "./reversal-rules.ts";

export interface RulesOutcome {
  entry?: WalletLedgerEntry; // existe quando o saldo mudou
}

/**
 * As regras de BET, WIN, LOSS, REFUND e ROLLBACK (seção 7), num lugar só.
 *
 * Usadas por quem processa uma transação NOVA (ProcessWagerTransactionUseCase) e
 * por quem RETOMA uma que estava esperando a referência
 * (ResolvePendingReferencesUseCase). Assim as duas entradas nunca divergem.
 *
 * Quem chama já travou a wallet: aqui só se decide e se muda a transação e a
 * wallet EM MEMÓRIA. Gravar é com quem chama.
 */
@Injectable()
export class WagerRules {
  constructor(private readonly transactions: WagerTransactionRepository) {}

  /**
   * Aplica a regra do kind. Ao final, a transação está PROCESSED, REJECTED ou
   * PENDING_REFERENCE (referência ainda não chegou).
   */
  async apply(tx: EntityManager, transaction: WagerTransaction, wallet: Wallet): Promise<RulesOutcome> {
    switch (transaction.kind) {
      case WagerTransactionKind.Loss: {
        transaction.markProcessed(undefined, new Date(), wallet.balance);
        return {};
      }
      case WagerTransactionKind.Bet:
      case WagerTransactionKind.Win: {
        const direction = transaction.ledgerDirectionFor();
        return this.move(transaction, wallet, direction, undefined, FailureCode.InsufficientFunds);
      }
      case WagerTransactionKind.Refund:
      case WagerTransactionKind.Rollback: {
        const reference = await this.transactions.findByExternalId(
          tx,
          transaction.providerId,
          transaction.referenceExternalTransactionId!,
        );
        if (!reference) {
          transaction.markPendingReference();
          return {};
        }
        const alreadyReversed = await this.transactions.hasProcessedReversal(tx, reference.id);
        const failure = checkReversal(transaction, reference, alreadyReversed);
        if (failure) {
          transaction.reject(failure);
          return {};
        }
        const direction = transaction.ledgerDirectionFor(reference);
        return this.move(transaction, wallet, direction, reference.id, FailureCode.ReversalInsufficientFunds);
      }
      case WagerTransactionKind.Opening:
        throw new Error("Kind não pode ser Opening");
    }
  }

  /** Quais eventos este resultado gera (tabela da seção 11 do README). */
  eventsFor(
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
        // PENDING ou FAILED aqui = bug no apply (toda regra termina num status final).
        throw new Error(`Transação ${transaction.id} terminou o processamento em ${transaction.status}`);
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
      const { before, after } =
        direction === LedgerDirection.Debit ? wallet.debit(transaction.money) : wallet.credit(transaction.money);
      transaction.markProcessed(referenceTransactionId, new Date(), after);
      const entry = WalletLedgerEntry.create({
        id: Bun.randomUUIDv7(),
        walletId: wallet.id,
        transactionId: transaction.id,
        direction,
        money: transaction.money,
        balanceBefore: before,
        balanceAfter: after,
      });
      return { entry };
    } catch (error) {
      if (error instanceof InsufficientFundsError) {
        transaction.reject(insufficientFundsCode);
        return {};
      }
      throw error;
    }
  }
}
