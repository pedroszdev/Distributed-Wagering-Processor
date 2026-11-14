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

}
