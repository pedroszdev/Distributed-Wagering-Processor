import { Injectable } from "@nestjs/common";
import { LockMode, type EntityManager } from "@mikro-orm/postgresql";
import type { WagerTransaction } from "../../../domain/wager-transaction/wager-transaction.ts";
import { WagerTransactionEntity, type WagerTransactionRecord } from "../entities/wager-transaction.entity.ts";
import { wagerTransactionToDomain, wagerTransactionToRecord } from "../mappers/wager-transaction.mapper.ts";
import { WagerTransactionKind, WagerTransactionStatus } from "../../../domain/wager-transaction/wager-transaction.enums.ts";

@Injectable()
export class WagerTransactionRepository {
  async insert(em: EntityManager, transaction: WagerTransaction, schedule?: { nextAttemptAt?: Date }): Promise<void> {
    await em.insert(WagerTransactionEntity, wagerTransactionToRecord(transaction, schedule));
  }
  // "Essa key já foi usada por este provider?" → alimenta o decideIdempotency(). */
  async findByIdempotencyKey(em: EntityManager, providerId: string, idempotencyKey: string): Promise<WagerTransaction | null> {
    const record = await em.findOne(WagerTransactionEntity, { providerId, idempotencyKey });
    return record ? wagerTransactionToDomain(record) : null;
  }

  //achar a BET que um REFUND quer reverter
  async findByExternalId(em: EntityManager, providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    const record = await em.findOne(WagerTransactionEntity, {providerId, externalTransactionId})
    return record ? wagerTransactionToDomain(record) : null;
  }
  
  async findById(em: EntityManager, id: string): Promise<WagerTransaction | null> {
    const record = await em.findOne(WagerTransactionEntity, { id });
    return record ? wagerTransactionToDomain(record) : null;
  }

  /**
   * Essa transação já foi revertida? Conta QUALQUER reversão processada (REFUND ou
   * ROLLBACK), não só a do mesmo tipo: senão um REFUND seguido de um ROLLBACK da
   * mesma BET devolveria o dinheiro duas vezes.
   */
  async hasProcessedReversal(em: EntityManager, referenceTransactionId: string): Promise<boolean> {
    const count = await em.count(WagerTransactionEntity, {
      referenceTransactionId,
      kind: { $in: [WagerTransactionKind.Refund, WagerTransactionKind.Rollback] },
      status: WagerTransactionStatus.Processed,
    });
    return count > 0;
  }

  // ------------------------------------------------------------ PENDING_REFERENCE (seção 7.1)

  /**
   * Reserva um lote de PENDING_REFERENCE vencidas para ESTE worker.
   *
   * A reserva é uma "lease": empurra o next_attempt_at para `leaseUntil`, então os
   * outros workers não pegam as mesmas linhas. Se este worker morrer antes de
   * resolver, a lease vence e outro pega. SKIP LOCKED: linhas que outro worker
   * está reservando neste instante são puladas, não esperadas.
   *
   * Roda num comando só (não segura lock depois): quem resolve cada item trava
   * a WALLET primeiro, na mesma ordem do use case principal (sem deadlock).
   */
  async claimDuePendingReferences(
    em: EntityManager,
    now: Date,
    limit: number,
    leaseUntil: Date,
  ): Promise<PendingReferenceTicket[]> {
    const rows = await em.execute<{ id: string; wallet_id: string }[]>(
      `update wager_transactions
          set next_attempt_at = ?
        where id in (
          select id from wager_transactions
           where status = 'PENDING_REFERENCE' and next_attempt_at <= ?
           order by next_attempt_at
           limit ?
           for update skip locked)
      returning id, wallet_id`,
      [leaseUntil, now, limit],
    );
    return rows.map((row) => ({ transactionId: row.id, walletId: row.wallet_id }));
  }

  /**
   * A transação, travada, SE ainda estiver PENDING_REFERENCE (outro worker pode
   * ter resolvido no meio do caminho). Junto vem o número de tentativas já feitas.
   */
  async findPendingReferenceForUpdate(
    em: EntityManager,
    id: string,
  ): Promise<{ transaction: WagerTransaction; attempts: number } | null> {
    const record = await em.findOne(
      WagerTransactionEntity,
      { id, status: WagerTransactionStatus.PendingReference },
      { lockMode: LockMode.PESSIMISTIC_WRITE },
    );
    return record ? { transaction: wagerTransactionToDomain(record), attempts: record.attempts } : null;
  }

  /** A referência ainda não chegou: conta a tentativa e agenda a próxima. */
  async reschedulePendingReference(em: EntityManager, id: string, attempts: number, nextAttemptAt: Date): Promise<void> {
    await this.updatePending(em, id, { attempts, nextAttemptAt });
  }

  /** Grava o desfecho (PROCESSED ou REJECTED) de uma transação que estava pendente. */
  async saveResolution(em: EntityManager, transaction: WagerTransaction, attempts: number): Promise<void> {
    await this.updatePending(em, transaction.id, {
      status: transaction.status,
      referenceTransactionId: transaction.referenceTransactionId ?? null,
      failureCode: transaction.failureCode ?? null,
      balanceAfterAmount: transaction.balanceAfter?.toJSON().amount ?? null,
      processedAt: transaction.processedAt ?? null,
      attempts,
      nextAttemptAt: null,
    });
  }

  /**
   * Uma transação acabou de chegar a um estado final: acorda quem estava esperando
   * por ela (REFUND/ROLLBACK com essa referência). O worker resolve na próxima
   * volta, sem esperar o backoff. Usa o índice wager_tx_waiting_for_reference_idx.
   */
  async wakeUpWaitingFor(em: EntityManager, providerId: string, externalTransactionId: string, now: Date): Promise<void> {
    await em.nativeUpdate(
      WagerTransactionEntity,
      { providerId, referenceExternalTransactionId: externalTransactionId, status: WagerTransactionStatus.PendingReference },
      { nextAttemptAt: now },
    );
  }

  /** UPDATE que só vale se a linha AINDA está pendente (senão é bug: quem chamou devia ter o lock). */
  private async updatePending(em: EntityManager, id: string, changes: Partial<WagerTransactionRecord>): Promise<void> {
    const affected = await em.nativeUpdate(
      WagerTransactionEntity,
      { id, status: WagerTransactionStatus.PendingReference },
      changes,
    );
    if (affected !== 1) {
      throw new Error(`Transação ${id} não estava mais PENDING_REFERENCE ao gravar`);
    }
  }
}

/** Uma PENDING_REFERENCE reservada para um worker resolver. */
export interface PendingReferenceTicket {
  transactionId: string;
  walletId: string;
}
