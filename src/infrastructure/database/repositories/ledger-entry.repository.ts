import { Injectable } from "@nestjs/common";
import { QueryOrder, type EntityManager } from "@mikro-orm/postgresql";
import type { WalletLedgerEntry } from "../../../domain/ledger/ledger-entry.ts";
import { LedgerEntryEntity } from "../entities/ledger-entry.entity.ts";
import { ledgerEntryToDomain, ledgerEntryToRecord } from "../mappers/ledger-entry.mapper.ts";

/** Só INSERT: o ledger é imutável (o trigger no banco recusa UPDATE/DELETE). */
@Injectable()
export class LedgerEntryRepository {
  async insert(em: EntityManager, entry: WalletLedgerEntry): Promise<void> {
    await em.insert(LedgerEntryEntity, ledgerEntryToRecord(entry));
  }

  async listByWallet(
  em: EntityManager,
  walletId: string,
  page: { afterId?: string; limit: number },
): Promise<WalletLedgerEntry[]> {
  const records = await em.find(
    LedgerEntryEntity,
    {
      walletId,
      ...(page.afterId ? { id: { $gt: page.afterId } } : {}),
    },
    { orderBy: { id: QueryOrder.ASC }, limit: page.limit },
  );
  const recordsOrd = records.map((record): WalletLedgerEntry => ledgerEntryToDomain(record))
  return recordsOrd
}

  /**
   * O saldo gravado na wallet e as somas do ledger, NUM ÚNICO comando SQL.
   *
   * Por que um comando só: com duas consultas separadas, uma BET que fizesse
   * commit entre elas daria uma "divergência" falsa (saldo de depois, ledger de
   * antes). Um único SELECT enxerga um snapshot só do banco (MVCC), sem travar
   * a wallet — a reconciliação não atrapalha as apostas de uma hot wallet.
   *
   * As somas são feitas em `numeric` (decimal exato) no Postgres; créditos e
   * débitos separados porque o Money do domínio não aceita valor negativo.
   */
  async reconciliationSnapshot(em: EntityManager, walletId: string): Promise<ReconciliationSnapshot | null> {
    const [row] = await em.execute<
      { currency: string; stored: string; credits: string; debits: string; entries: number }[]
    >(
      `select w.currency,
              w.balance_amount                                                              as stored,
              coalesce(sum(e.amount) filter (where e.direction = 'CREDIT'), 0)::numeric(20,2) as credits,
              coalesce(sum(e.amount) filter (where e.direction = 'DEBIT'), 0)::numeric(20,2)  as debits,
              count(e.id)::int                                                              as entries
         from wallets w
         left join wallet_ledger_entries e on e.wallet_id = w.id
        where w.id = ?
        group by w.id`,
      [walletId],
    );
    if (!row) return null;
    return {
      currency: row.currency,
      storedAmount: row.stored,
      creditsAmount: row.credits,
      debitsAmount: row.debits,
      entries: row.entries,
    };
  }
}

export interface ReconciliationSnapshot {
  currency: string;
  storedAmount: string;
  creditsAmount: string;
  debitsAmount: string;
  entries: number;
}
