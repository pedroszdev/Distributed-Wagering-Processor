import { EntityManager } from "@mikro-orm/postgresql";
import { Inject, Injectable, Logger } from "@nestjs/common";
import { OutboxMessage } from "../../domain/outbox/outbox-message.ts";
import type { EventContext } from "../../domain/shared/integration-events.ts";
import { FailureCode } from "../../domain/wager-transaction/failure-code.ts";
import { WagerTransactionStatus } from "../../domain/wager-transaction/wager-transaction.enums.ts";
import type { WagerTransaction } from "../../domain/wager-transaction/wager-transaction.ts";
import { LedgerEntryRepository } from "../../infrastructure/database/repositories/ledger-entry.repository.ts";
import { OutboxMessageRepository } from "../../infrastructure/database/repositories/outbox-message.repository.ts";
import {
  WagerTransactionRepository,
  type PendingReferenceTicket,
} from "../../infrastructure/database/repositories/wager-transaction.repository.ts";
import { WalletRepository } from "../../infrastructure/database/repositories/wallet.repository.ts";
import { WalletNotFoundError } from "../wallet/wallet.errors.ts";
import {
  isPendingReferenceExpired,
  nextPendingAttemptAt,
  PENDING_REFERENCE_POLICY,
  type PendingReferencePolicy,
} from "./pending-reference-policy.ts";
import { WagerRules } from "./wager-rules.ts";
import { logContext } from "../../infrastructure/observability/log-context.ts";
import { lockConflictType, Metrics } from "../../infrastructure/observability/metrics.ts";

export type PendingReferenceOutcome = "processed" | "rejected" | "rescheduled" | "skipped";

export interface PendingReferenceBatchResult {
  claimed: number;
  processed: number; // a referência chegou e a reversão foi aplicada
  rejected: number; // a referência chegou mas a regra recusou, OU o TTL venceu (REFERENCE_NOT_FOUND)
  rescheduled: number; // a referência ainda não chegou: próxima tentativa com backoff
  skipped: number; // outro worker resolveu antes
  failed: number; // erro (banco etc.): a lease vence e alguém tenta de novo
}

/**
 * Retoma REFUND/ROLLBACK que chegaram antes da transação referenciada (seção 7.1).
 *
 * Uma "volta": reserva um lote (lease) e resolve cada item numa transação SQL
 * própria. Para cada um:
 *   - a referência chegou → as MESMAS regras da entrada normal (WagerRules):
 *     PROCESSED (move saldo + ledger) ou REJECTED (ex.: AMOUNT_MISMATCH);
 *   - não chegou e ainda está no prazo → conta a tentativa, agenda com backoff;
 *   - não chegou e o TTL venceu → REJECTED com REFERENCE_NOT_FOUND.
 * Os eventos saem pela outbox, na mesma transação (como em toda escrita).
 */
@Injectable()
export class ResolvePendingReferencesUseCase {
  private readonly logger = new Logger(ResolvePendingReferencesUseCase.name);

  constructor(
    private readonly em: EntityManager,
    private readonly wallets: WalletRepository,
    private readonly transactions: WagerTransactionRepository,
    private readonly ledger: LedgerEntryRepository,
    private readonly outbox: OutboxMessageRepository,
    private readonly rules: WagerRules,
    @Inject(PENDING_REFERENCE_POLICY) private readonly policy: PendingReferencePolicy,
    private readonly metrics: Metrics,
  ) {}

  async execute(batchSize: number, leaseMs: number): Promise<PendingReferenceBatchResult> {
    const now = new Date();
    const tickets = await this.transactions.claimDuePendingReferences(
      this.em.fork(),
      now,
      batchSize,
      new Date(now.getTime() + leaseMs),
    );

    const result: PendingReferenceBatchResult = {
      claimed: tickets.length,
      processed: 0,
      rejected: 0,
      rescheduled: 0,
      skipped: 0,
      failed: 0,
    };
    // Um item por vez, cada um na sua transação: um erro num não desfaz os outros.
    // Cada item tem o seu contexto de log (transactionId/walletId em toda linha).
    for (const ticket of tickets) {
      await logContext.run({ transactionId: ticket.transactionId, walletId: ticket.walletId }, async () => {
        try {
          const outcome = await this.resolve(ticket);
          result[outcome]++;
          this.metrics.pendingReferenceResolutions.inc({ outcome });
        } catch (error) {
          result.failed++;
          this.metrics.pendingReferenceResolutions.inc({ outcome: "failed" });
          const conflict = lockConflictType(error);
          if (conflict) this.metrics.walletLockConflicts.inc({ type: conflict });
          this.logger.warn({ msg: "falha ao resolver PENDING_REFERENCE; a lease vence e outro tenta", error: String(error) });
        }
      });
    }
    return result;
  }

  /** Resolve UMA transação pendente, numa transação SQL própria. */
  async resolve(ticket: PendingReferenceTicket): Promise<PendingReferenceOutcome> {
    return this.em.fork().transactional(async (tx) => {
      // 1. A WALLET primeiro: a mesma ordem de lock do use case principal
      //    (wallet → transações). Ordem igual em todo lugar = sem deadlock.
      const wallet = await this.wallets.findByIdForUpdate(tx, ticket.walletId);
      if (!wallet) throw new WalletNotFoundError(ticket.walletId); // impossível pela FK

      // 2. A transação, travada. Se outro worker já resolveu, não há o que fazer.
      const pending = await this.transactions.findPendingReferenceForUpdate(tx, ticket.transactionId);
      if (!pending) return "skipped";
      const { transaction, attempts } = pending;
      const expectedVersion = wallet.version;
      const now = new Date();

      // 3. As mesmas regras da entrada normal.
      const outcome = await this.rules.apply(tx, transaction, wallet);

      if (transaction.status === WagerTransactionStatus.PendingReference) {
        // 4a. A referência ainda não chegou.
        if (!isPendingReferenceExpired(transaction.createdAt, now, this.policy)) {
          await this.transactions.reschedulePendingReference(
            tx,
            transaction.id,
            attempts + 1,
            nextPendingAttemptAt(now, attempts + 1, this.policy),
          );
          return "rescheduled";
        }
        // 4b. ...e o prazo acabou: desiste, de forma auditável.
        transaction.reject(FailureCode.ReferenceNotFound);
      }

      // 5. Grava o desfecho (o UPDATE exige que a linha AINDA esteja pendente).
      await this.transactions.saveResolution(tx, transaction, attempts + 1);
      if (outcome.entry) {
        await this.wallets.update(tx, wallet, expectedVersion);
        await this.ledger.insert(tx, outcome.entry);
      }
      // Alguém esperava por ESTA (ex.: um ROLLBACK de um REFUND que estava pendente)?
      await this.transactions.wakeUpWaitingFor(tx, transaction.providerId, transaction.externalTransactionId, now);

      // 6. Eventos na outbox, na mesma transação.
      const context = await this.contextFor(tx, transaction);
      logContext.assign({ correlationId: context.correlationId, providerId: transaction.providerId });
      for (const event of this.rules.eventsFor(transaction, wallet, outcome, context)) {
        await this.outbox.insert(tx, OutboxMessage.enqueue(event));
      }

      this.logger.log({
        msg: "PENDING_REFERENCE resolvida",
        transactionId: transaction.id,
        walletId: transaction.walletId,
        providerId: transaction.providerId,
        status: transaction.status,
        failureCode: transaction.failureCode,
        attempts: attempts + 1,
      });
      return transaction.status === WagerTransactionStatus.Processed ? "processed" : "rejected";
    });
  }

  /**
   * Os eventos da resolução continuam a história do pedido ORIGINAL: mesmo
   * correlationId, e a causa é o evento WagerTransactionPendingReference.
   */
  private async contextFor(tx: EntityManager, transaction: WagerTransaction): Promise<EventContext> {
    const original = await this.outbox.findPendingReferenceEvent(tx, transaction.id);
    return original
      ? { correlationId: original.correlationId, causationId: original.eventId }
      : { correlationId: transaction.id };
  }
}
