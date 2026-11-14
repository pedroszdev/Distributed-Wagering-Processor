import { Injectable } from "@nestjs/common";
import type { WalletLedgerEntry } from "../../domain/ledger/ledger-entry.ts";
import { EntityManager } from "@mikro-orm/postgresql";
import { WalletRepository } from "../../infrastructure/database/repositories/wallet.repository.ts";
import { LedgerEntryRepository } from "../../infrastructure/database/repositories/ledger-entry.repository.ts";
import { WalletNotFoundError } from "./wallet.errors.ts";

export interface LedgerPage {
  entries: WalletLedgerEntry[];
  nextAfterId: string | undefined; // undefined = acabou
}

@Injectable()
export class ListWalletLedgerUseCase {
  constructor(
    private readonly em: EntityManager,
    private readonly wallets: WalletRepository,
    private readonly ledger: LedgerEntryRepository,
  ) {}

  async execute(walletId: string, page: { afterId?: string; limit: number }): Promise<LedgerPage> {
    const em = this.em.fork()
    const wallet = await this.wallets.findById(em, walletId)
    if (!wallet) {
        throw new WalletNotFoundError(walletId)
    }
    
    const list = await this.ledger.listByWallet(em,walletId,  { ...page, limit: page.limit + 1 })
    const hasMore = list.length > page.limit;
    const entities: WalletLedgerEntry[] = list.slice(0, page.limit)
    const nextAfterId = hasMore ? entities.at(-1)!.id : undefined
    return {entries: entities, nextAfterId}
  }
}
